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

Reactor::Reactor(Scheduler& sched, WorkerId shard_id)
    : sched_(sched), shard_id_(shard_id) {
    // Allocate the reusable control frame before acquiring raw kernel
    // resources. If allocation throws, construction unwinds without leaking
    // an epoll/eventfd pair from an object whose destructor cannot run.
    initialize_driver();

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
    last_poll_ns_.store(now_ns(), std::memory_order_relaxed);
}

Reactor::~Reactor() {
    destroy_driver();
    if (backend_fd_ >= 0) ::close(backend_fd_);
    if (wake_fd_ >= 0) ::close(wake_fd_);
    const std::uint32_t chunks = chunk_count_.load(std::memory_order_acquire);
    for (std::uint32_t i = 0; i < chunks; ++i) {
        delete[] chunks_[i].load(std::memory_order_acquire);
    }
}

Result<IoDesc*> Reactor::attach(int fd) {
    if (sched_.stopping()) return Error{Errc::shutdown};

    IoDesc* desc = alloc_desc();
    if (desc == nullptr) return Error{ENOMEM};

    desc->lock_lifecycle();
    desc->fd = fd;
    if (desc->owner == nullptr) {
        desc->owner = this;
        desc->home_worker = shard_id_;
        desc->runtime_stop = &sched_.stop_;
    }
    desc->closing.store(false, std::memory_order_relaxed);
    desc->refs.store(1, std::memory_order_relaxed);
    desc->slot[0].store(nullptr, std::memory_order_relaxed);
    desc->slot[1].store(nullptr, std::memory_order_relaxed);
    for (unsigned i = 0; i < kDirCount; ++i) {
        desc->syscall_active[i].store(false, std::memory_order_relaxed);
        desc->deadline_seq[i].store(1, std::memory_order_relaxed);
        desc->expired_seq[i].store(0, std::memory_order_relaxed);
        desc->absolute_deadline_ns[i].store(0, std::memory_order_relaxed);
        desc->deadline_timer[i].state.store(Timer::kIdle, std::memory_order_relaxed);
        desc->deadline_timer[i].heap_index = ~0u;
        // Nothing is known about a fresh descriptor, so let the first operation
        // try the syscall.
        desc->ready_hint[i].store(true, std::memory_order_relaxed);
    }
    desc->unlock_lifecycle();

    // Register both directions once, edge-triggered, and never touch epoll_ctl
    // again for the life of the fd. Rearming per operation (the one-shot model)
    // costs a syscall per I/O; the readiness state machine makes it unnecessary.
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    ev.data.u64 = make_token(*desc);
    if (::epoll_ctl(backend_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        const Error err = Error::from_errno();
        desc->closing.store(true, std::memory_order_release);
        release_desc(desc);
        return err;
    }

    registered_.fetch_add(1, std::memory_order_relaxed);
    // Give the owner one prompt poll ticket for a newly attached descriptor.
    // Later tickets come from stale monitor passes.
    (void)request_owner_poll();
    return desc;
}

void Reactor::detach(IoDesc* desc) {
    desc->lock_lifecycle();
    desc->closing.store(true, std::memory_order_release);
    desc->unlock_lifecycle();

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

    // Never wait while holding lifecycle_lock: the syscall that won the
    // closing race must be able to drop its direction lease. Because closing
    // was published under that same lock, no new lease can start now. Once
    // both flags are clear, Socket::close() may physically close the fd
    // without any operation reaching a reused fd number.
    desc->wait_for_syscalls();
    release_desc(desc);
}

int Reactor::poll(std::int64_t timeout_ns) {
    bool expected = false;
    if (!polling_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
        return -1;
    }
    // The monitor may opportunistically drain this shard's I/O, but a wake
    // token belongs to the worker that owns the shard. In particular, that
    // worker can have published itself idle and completed its final work check
    // while the monitor owns polling_. A producer then publishes into the
    // worker's inbox and writes this token. If the monitor consumed it, the
    // worker could acquire polling_ afterwards and sleep with runnable work
    // already queued.
    const bool shard_owner =
        sched_.current_worker_id() == shard_id_;
    if (timeout_ns == 0) {
        CIO_METRIC(polls_nonblocking, 1);
    } else {
        CIO_METRIC(polls_blocking, 1);
    }

    const std::int64_t started = now_ns();
    poller_deadline_ns_.store(
        timeout_ns < 0 ? INT64_MAX : started + timeout_ns,
        std::memory_order_release);

    epoll_event events[kMaxEvents];

    const int n = wait_for_events(backend_fd_, events, kMaxEvents, timeout_ns);
    // A shard-owning worker published itself idle before entering a blocking
    // poll. It is active again as soon as epoll_wait returns, not after this
    // entire readiness batch has been dispatched. Clear that publication now
    // so batch wakeups cannot claim the poller itself. For a monitor poll there
    // is no current owning worker, and poller_returned() deliberately does
    // nothing.
    sched_.poller_returned(shard_id_);
    poller_deadline_ns_.store(INT64_MAX, std::memory_order_release);
    // Freshness starts when the kernel wait completes, not when it starts. A
    // blocking poll that just returned must not look stale merely because it
    // slept longer than the monitor interval.
    last_poll_ns_.store(now_ns(), std::memory_order_relaxed);
    if (n <= 0) {
        polling_.store(false, std::memory_order_release);
        return 0;  // timeout/EINTR/error: the worker retries its state checks
    }
    CIO_METRIC(poll_events, static_cast<std::uint64_t>(n));

    ReadyBatch batch;

    for (int i = 0; i < n; ++i) {
        const std::uint64_t token = events[i].data.u64;

        if (token == kWakeToken) {
            // Keep the level-triggered token latched for the shard owner. A
            // monitor poll still returns and releases polling_, but it must
            // not steal the directed wake that makes the owner's next poll
            // return.
            if (!shard_owner) continue;

            std::uint64_t value = 0;
            while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
            }
            // Only the owner clears this publication. A producer that races
            // the drain and observes true has published work to the owner that
            // is already awake; the owner's run loop rechecks every queue and
            // timer before it can park again. Foreign pollers leave both the
            // flag and the kernel token untouched.
            wake_pending_.store(false, std::memory_order_release);
            continue;
        }

        const std::uint32_t mask = events[i].events;
        unsigned dirs = 0;
        // HUP/ERR wake both directions: a peer reset must unblock a parked
        // writer, not just a reader.
        if (mask & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) dirs |= 1u;
        if (mask & (EPOLLOUT | EPOLLHUP | EPOLLERR)) dirs |= 2u;
        if (dirs != 0) dispatch(token, dirs, &batch);
    }

    CIO_METRIC(poll_wakeups, batch.total);
    if (batch.unpublished_local_fifo != 0) {
        sched_.finish_io_batch(batch.unpublished_local_fifo);
    }
    polling_.store(false, std::memory_order_release);
    return n;
}

void Reactor::nudge(std::int64_t deadline_ns) noexcept {
    if (!polling_.load(std::memory_order_acquire)) return;
    if (poller_deadline_ns_.load(std::memory_order_acquire) <= deadline_ns) return;
    wake();
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
