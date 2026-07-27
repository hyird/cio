#include "cio/config.hpp"

#if defined(CIO_REACTOR_EPOLL)

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>

#include "cio/detail/metrics.hpp"
#include "cio/detail/reactor.hpp"
#include "cio/detail/scheduler.hpp"

namespace cio::detail {
namespace {

constexpr int kMaxEvents = 256;
// data.u64 value reserved for the wakeup eventfd. Real tokens pack a slab index
// in the low half, which can never reach UINT32_MAX entries.
constexpr std::uint64_t kWakeToken = ~std::uint64_t{0};

inline std::uint64_t make_token(const IoDesc& desc) noexcept {
    return (static_cast<std::uint64_t>(desc.generation.load(std::memory_order_acquire)) << 32) |
           desc.index;
}

// epoll_pwait2 takes a timespec, so a sub-millisecond sleep does not get
// rounded up to a full millisecond. Falls back to epoll_wait on older kernels.
int wait_for_events(int epoll_fd, epoll_event* events, int max_events,
                    std::int64_t timeout_ns) noexcept {
#if defined(__NR_epoll_pwait2)
    static std::atomic<bool> has_pwait2{true};
    if (has_pwait2.load(std::memory_order_relaxed)) {
        timespec ts{};
        timespec* tsp = nullptr;
        if (timeout_ns >= 0) {
            ts.tv_sec = static_cast<time_t>(timeout_ns / 1'000'000'000);
            ts.tv_nsec = static_cast<long>(timeout_ns % 1'000'000'000);
            tsp = &ts;
        }
        const long n = ::syscall(__NR_epoll_pwait2, epoll_fd, events, max_events, tsp, nullptr,
                                 static_cast<std::size_t>(0));
        if (n >= 0 || errno != ENOSYS) return static_cast<int>(n);
        has_pwait2.store(false, std::memory_order_relaxed);
    }
#endif
    int timeout_ms;
    if (timeout_ns < 0) {
        timeout_ms = -1;
    } else if (timeout_ns == 0) {
        timeout_ms = 0;
    } else {
        const std::int64_t ms = (timeout_ns + 999'999) / 1'000'000;
        timeout_ms = ms > INT_MAX ? INT_MAX : static_cast<int>(ms);
    }
    return ::epoll_wait(epoll_fd, events, max_events, timeout_ms);
}

[[noreturn]] void fatal(const char* what) {
    std::fprintf(stderr, "cio: %s failed: %s\n", what, std::strerror(errno));
    std::abort();
}

}  // namespace

Reactor::Reactor(Scheduler& sched) : sched_(sched) {
    backend_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (backend_fd_ < 0) fatal("epoll_create1");

    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) fatal("eventfd");
    wake_write_fd_ = wake_fd_;

    // Level-triggered on purpose: a wake() issued just before the poller enters
    // epoll_wait must still make it return, and a level-triggered eventfd stays
    // readable until we drain it.
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.u64 = kWakeToken;
    if (::epoll_ctl(backend_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) != 0) fatal("epoll_ctl(wakefd)");
}

Reactor::~Reactor() {
    if (backend_fd_ >= 0) ::close(backend_fd_);
    if (wake_fd_ >= 0) ::close(wake_fd_);
    const std::uint32_t chunks = chunk_count_.load(std::memory_order_acquire);
    for (std::uint32_t i = 0; i < chunks; ++i) {
        delete[] chunks_[i].load(std::memory_order_acquire);
    }
}

Result<IoDesc*> Reactor::attach(int fd) {
    IoDesc* desc = alloc_desc();
    if (desc == nullptr) return Error{ENOMEM};

    desc->fd = fd;
    desc->owner = this;
    desc->closing.store(false, std::memory_order_relaxed);
    desc->slot[0].store(nullptr, std::memory_order_relaxed);
    desc->slot[1].store(nullptr, std::memory_order_relaxed);
    for (unsigned i = 0; i < kDirCount; ++i) {
        desc->deadline_seq[i].store(1, std::memory_order_relaxed);
        desc->expired_seq[i].store(0, std::memory_order_relaxed);
        desc->deadline_timer[i].state.store(Timer::kIdle, std::memory_order_relaxed);
        desc->deadline_timer[i].heap_index = ~0u;
        // Nothing is known about a fresh descriptor, so let the first operation
        // try the syscall.
        desc->ready_hint[i].store(true, std::memory_order_relaxed);
    }

    // Register both directions once, edge-triggered, and never touch epoll_ctl
    // again for the life of the fd. Rearming per operation (the one-shot model)
    // costs a syscall per I/O; the readiness state machine makes it unnecessary.
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    ev.data.u64 = make_token(*desc);
    if (::epoll_ctl(backend_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        const Error err = Error::from_errno();
        free_desc(desc);
        return err;
    }

    registered_.fetch_add(1, std::memory_order_relaxed);
    return desc;
}

void Reactor::detach(IoDesc* desc) {
    desc->closing.store(true, std::memory_order_release);

    for (unsigned i = 0; i < kDirCount; ++i) {
        // Unconditionally, and before free_desc() below: this is what
        // guarantees no deadline callback is still running on this descriptor
        // when it goes back on the free list to be handed to another socket.
        sched_.timers().disarm(&desc->deadline_timer[i]);
        // Invalidate any in-flight firing of the deadline timer.
        desc->deadline_seq[i].fetch_add(1, std::memory_order_acq_rel);
    }

    ::epoll_ctl(backend_fd_, EPOLL_CTL_DEL, desc->fd, nullptr);
    registered_.fetch_sub(1, std::memory_order_relaxed);

    unblock(desc, Dir::kRead, Error{Errc::closed});
    unblock(desc, Dir::kWrite, Error{Errc::closed});

    free_desc(desc);
}

int Reactor::poll(std::int64_t timeout_ns) {
    epoll_event events[kMaxEvents];

    const int n = wait_for_events(backend_fd_, events, kMaxEvents, timeout_ns);
    if (n <= 0) return 0;  // 0 = timeout, <0 = EINTR or error; either way, retry later
    CIO_METRIC(poll_events, static_cast<std::uint64_t>(n));

    // One syscall can make hundreds of tasks runnable. Queue them all, then
    // issue a single wake sized to the burst — see Scheduler::notify_batch.
    std::uint32_t made_runnable = 0;

    for (int i = 0; i < n; ++i) {
        const std::uint64_t token = events[i].data.u64;

        if (token == kWakeToken) {
            std::uint64_t value = 0;
            while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
            }
            // Clear *after* draining. A wake() that lands in between re-arms the
            // eventfd, so the next poll returns immediately — a spurious extra
            // search, never a missed wakeup.
            wake_pending_.store(false, std::memory_order_release);
            continue;
        }

        const std::uint32_t mask = events[i].events;
        unsigned dirs = 0;
        // HUP/ERR wake both directions: a peer reset must unblock a parked
        // writer, not just a reader.
        if (mask & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) dirs |= 1u;
        if (mask & (EPOLLOUT | EPOLLHUP | EPOLLERR)) dirs |= 2u;
        if (dirs != 0) dispatch(token, dirs, &made_runnable);
    }

    CIO_METRIC(poll_wakeups, made_runnable);

    // One of these is the poller's own next task, so it needs one fewer peer
    // awake than it made runnable.
    //
    // Every caller of poll() that is a worker returns straight to its run loop
    // and takes an item — park() falls through to Worker::run's next_local(),
    // find_work() calls next_local() itself. Counting that one as somebody
    // else's work means a poll that wakes a single task issues a futex to a
    // parked worker which arrives, finds the queue already emptied by the
    // poller, and parks again. At one connection that round trip is on the
    // critical path of every request: 93us per round trip against asio's 79.
    //
    // Go does the same thing for the same reason — findRunnable pops one
    // goroutine off the netpoll list to run on the current P and injects only
    // the remainder.
    //
    // The monitor thread also polls, and it is not a worker and will not run
    // anything, so it still has to wake somebody for every task.
    const std::uint32_t taken_by_poller = current_worker() != nullptr ? 1u : 0u;
    sched_.notify_batch(made_runnable > taken_by_poller ? made_runnable - taken_by_poller : 0);
    return n;
}

void Reactor::wake() noexcept {
    // Collapse redundant wakes: many schedule() calls can race to nudge one
    // parked poller, and each write is a syscall.
    if (wake_pending_.exchange(true, std::memory_order_acq_rel)) return;
    CIO_METRIC(reactor_wakes, 1);

    const std::uint64_t one = 1;
    for (;;) {
        const ssize_t written = ::write(wake_write_fd_, &one, sizeof(one));
        if (written == static_cast<ssize_t>(sizeof(one))) return;
        if (written < 0 && errno == EINTR) continue;
        // EAGAIN means the counter is already saturated, so a token is pending
        // and the poll will return regardless.
        if (written < 0 && errno == EAGAIN) return;

        // Anything else published no token. Dropping the flag matters more than
        // the failed write: leaving it set would silently swallow every future
        // wake, including the one that ends a blocking poll at shutdown.
        wake_pending_.store(false, std::memory_order_release);
        return;
    }
}

}  // namespace cio::detail

#endif  // CIO_REACTOR_EPOLL
