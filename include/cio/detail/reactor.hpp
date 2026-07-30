// The reactor: readiness notification for the runtime.
//
// epoll, edge-triggered. The backend-independent half of the reactor lives in
// src/reactor_common.cpp and the epoll-specific half in src/reactor_epoll.cpp,
// which is where another backend would go — but none is claimed.
//
// Per-fd, per-direction state follows Go's netpoll protocol. Each direction is
// a single atomic word with three states:
//
//   nullptr    idle
//   kIoReady   an edge arrived while nobody was waiting; the next waiter
//              consumes it instead of parking
//   IoWait*    exactly one task is parked on this direction
//
// The three-state machine is what makes edge-triggered polling safe: the race
// between "syscall returned EAGAIN, about to park" and "readiness arrived" is
// resolved by the CAS, never by a lock.
//
// Deadlines live on the descriptor, not on the awaiter — Go's SetReadDeadline
// model. That is not just API mimicry: a timer that can fire concurrently with
// the task it is timing out must outlive the coroutine frame, and a descriptor
// in the reactor's slab does while an awaiter in a frame does not.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <mutex>

#include "cio/config.hpp"
#include "cio/detail/completion.hpp"
#include "cio/detail/timer.hpp"
#include "cio/detail/worker_id.hpp"
#include "cio/result.hpp"

namespace cio::detail {

class Scheduler;
class Reactor;
struct SchedulerTestAccess;

enum class Dir : unsigned { kRead = 0, kWrite = 1 };
inline constexpr unsigned kDirCount = 2;

// Sentinel meaning "readiness arrived, no waiter". Never dereferenced.
inline void* const kIoReady = reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));

// The parked-task record stored in an IoDesc slot. Lives in the awaiter, which
// lives in the coroutine frame: parking on I/O never allocates.
//
// Exactly one party ever takes an IoWait out of its slot (the CAS decides), and
// the rule for whoever takes it is: touch the frame only until you schedule it,
// and schedule it last. That is what makes a refcount unnecessary here.
struct IoWait {
    std::coroutine_handle<> handle{};
    SchedulerTarget sched;
    WorkerId preferred_worker = kInvalidWorkerId;
    Error err{};  // written by the waker before scheduling
};

struct IoDesc;

// A deadline timer owned by a descriptor. Derives from Timer so it can sit in
// the timer heaps while carrying the back-pointers its callback needs.
struct IoTimer : Timer {
    IoDesc* desc = nullptr;
    Reactor* reactor = nullptr;
    Dir dir = Dir::kRead;
    std::uint32_t seq = 0;
};

// Per-file-descriptor registration. Allocated from a slab that never frees, so
// a stale event dequeued by another thread can always dereference it safely;
// the generation tag tells us whether the contents are still ours.
struct CIO_CACHE_ALIGNED IoDesc {
    std::atomic<void*> slot[kDirCount]{{nullptr}, {nullptr}};
    int fd = -1;
    std::uint32_t index = 0;
    std::atomic<std::uint32_t> generation{0};
    // One owner reference while attached plus one for each live IoAwaiter.
    // detach drops the owner but recycling waits until a scheduled waiter has
    // observed closing and released its reference.
    std::atomic<std::uint32_t> refs{0};
    std::atomic<bool> closing{false};
    // A non-blocking syscall owns one direction from the final lifecycle
    // validation until it has finished using the native fd. detach() first
    // publishes closing under lifecycle_lock, then waits for these leases
    // without holding that lock before Socket::close() may close the fd.
    //
    // The public contract permits at most one operation per direction, so a
    // bool is sufficient and also detects accidental same-direction overlap.
    std::atomic<bool> syscall_active[kDirCount]{{false}, {false}};
    // Serializes the tiny "validate incarnation + publish waiter" window with
    // descriptor teardown/reuse and syscall-lease acquisition. Readiness
    // dispatch remains lock-free.
    std::atomic_flag lifecycle_lock = ATOMIC_FLAG_INIT;
    Reactor* owner = nullptr;
    WorkerId home_worker = kInvalidWorkerId;
    // Points into the owning Scheduler, whose lifetime is bounded by the
    // Socket object and every live IoAwaiter. It lets an escaped Socket reject
    // new async work after cio::run() has stopped that scheduler, instead of
    // parking behind an epoll shard that no longer has a worker.
    const std::atomic<bool>* runtime_stop = nullptr;
    // Optional cancellation bound to this descriptor.
    //
    // Cancellation rides the same syscall-admission check as the deadline
    // rather than being threaded through every operation's signature: a
    // descriptor-scoped signal is what makes read/write/accept cancellable
    // without changing a single public signature, and it matches the existing
    // rule that deadlines live on the connection, not on the call.
    //
    // The pointee is owned by a CancelToken the Socket holds, so it outlives
    // every binding; the pointer is cleared under the lifecycle lock.
    std::atomic<const std::atomic<bool>*> cancel_flag{nullptr};
    IoDesc* free_next = nullptr;

    IoTimer deadline_timer[kDirCount];
    // A direction is "timed out" while expired_seq == deadline_seq. Stamping
    // the state with the sequence instead of using a bool is what makes
    // rearming a deadline race-free against the previous timer firing.
    std::atomic<std::uint32_t> deadline_seq[kDirCount]{{1}, {1}};
    std::atomic<std::uint32_t> expired_seq[kDirCount]{{0}, {0}};
    // Absolute deadline is the authoritative operation-state check. A timer
    // callback may be delayed behind CPU work, so syscall admission cannot
    // depend on the callback having stamped expired_seq already. Zero means
    // that the deadline is cleared.
    std::atomic<std::int64_t> absolute_deadline_ns[kDirCount]{{0}, {0}};
    // "A syscall on this direction might succeed."
    //
    // Edge-triggered readiness means an operation is only complete once it has
    // seen EAGAIN, so the naive loop costs two syscalls per message: one that
    // returns data and one that returns EAGAIN to prove the edge is consumed.
    // Measured against an echo server: 1.98 recv per request, and throughput
    // that tracks 1/syscalls almost exactly.
    //
    // A short read removes the need for the second one. If recv returns fewer
    // bytes than asked for, the receive queue was empty when it returned, so the
    // next call would EAGAIN — and any data arriving later re-arms the epoll
    // edge, which sets this back to true. So the task can skip straight to
    // parking. Starts true because nothing is known about a fresh descriptor.
    // Keep this hot state beside the absolute deadline and before the cold
    // update mutexes so no-deadline I/O does not acquire an extra cache line.
    std::atomic<bool> ready_hint[kDirCount]{{true}, {true}};
    // Serializes the cold set/clear transaction for one direction. The
    // lifecycle lock must be dropped while disarm waits for a firing callback,
    // so it cannot by itself prevent two setters from both disarming the old
    // node and then trying to arm the same IoTimer.
    std::mutex deadline_update_mutex[kDirCount];

    std::atomic<void*>& dir_slot(Dir d) noexcept { return slot[static_cast<unsigned>(d)]; }

    void lock_lifecycle() noexcept {
        while (lifecycle_lock.test_and_set(std::memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#endif
        }
    }
    void unlock_lifecycle() noexcept {
        lifecycle_lock.clear(std::memory_order_release);
    }

    bool may_be_ready(Dir d) const noexcept {
        return ready_hint[static_cast<unsigned>(d)].load(std::memory_order_acquire);
    }
    // Called by the owning task when a syscall proved the direction is drained.
    void note_would_block(Dir d) noexcept {
        ready_hint[static_cast<unsigned>(d)].store(false, std::memory_order_release);
    }
    // Called by the reactor when an edge arrives.
    void note_readable(Dir d) noexcept {
        ready_hint[static_cast<unsigned>(d)].store(true, std::memory_order_release);
    }

    bool timed_out(Dir d) const noexcept {
        const unsigned i = static_cast<unsigned>(d);
        const std::int64_t deadline =
            absolute_deadline_ns[i].load(std::memory_order_acquire);
        if (deadline == 0) return false;
        if (deadline <= now_ns()) return true;
        return expired_seq[i].load(std::memory_order_acquire) ==
               deadline_seq[i].load(std::memory_order_acquire);
    }

    // On the syscall-admission path, so it must cost one load and a
    // predicted-not-taken branch when nothing is bound — which is the common
    // case, since a socket only has a pointer here if set_cancel() was called.
    //
    // The pointer load is relaxed on purpose. Acquire would order it against
    // the publication in set_cancel(), but missing a binding published in the
    // same instant only defers the cancellation to the next admission check,
    // and a parked operation is woken by the hook regardless. That is the same
    // latitude set_deadline() already has against an in-flight syscall. The
    // flag's own load stays acquire, so a pointer we do observe is never read
    // against a stale value.
    bool cancelled() const noexcept {
        const std::atomic<bool>* const flag =
            cancel_flag.load(std::memory_order_relaxed);
        if (flag == nullptr) [[likely]] return false;
        return flag->load(std::memory_order_acquire);
    }

    bool runtime_stopping() const noexcept {
        return runtime_stop != nullptr &&
               runtime_stop->load(std::memory_order_acquire);
    }

    // Persistent operation state for an explicitly captured fd incarnation.
    // Callers that still own the Socket's descriptor reference may use this
    // before a syscall; IoAwaiter also uses it while holding its own reference.
    //
    // Re-checking the incarnation after the deadline loads avoids returning a
    // timeout assembled from two different recycled incarnations. Complete
    // operations use FdUseGuard below for the stronger final-check-through-
    // syscall close boundary; awaiters use this state-only check.
    Error io_error(Dir d, std::uint32_t expected_generation) const noexcept {
        if (generation.load(std::memory_order_acquire) != expected_generation ||
            closing.load(std::memory_order_acquire)) {
            return Error{Errc::closed};
        }
        if (runtime_stopping()) return Error{Errc::shutdown};
        if (cancelled()) return Error{Errc::cancelled};

        const bool expired = timed_out(d);
        if (generation.load(std::memory_order_acquire) != expected_generation ||
            closing.load(std::memory_order_acquire)) {
            return Error{Errc::closed};
        }
        if (runtime_stopping()) return Error{Errc::shutdown};
        return expired ? Error{Errc::timed_out} : Error{};
    }

    Error begin_syscall(Dir d, std::uint32_t expected_generation,
                        int& fd_snapshot) noexcept {
        const unsigned i = static_cast<unsigned>(d);
        Error state;

        lock_lifecycle();
        if (generation.load(std::memory_order_acquire) !=
                expected_generation ||
            closing.load(std::memory_order_acquire) || fd < 0) {
            state = Error{Errc::closed};
        } else if (runtime_stopping()) {
            state = Error{Errc::shutdown};
        } else if (cancelled()) {
            state = Error{Errc::cancelled};
        } else if (timed_out(d)) {
            state = Error{Errc::timed_out};
        } else if (syscall_active[i].load(std::memory_order_relaxed)) {
            state = Error{Errc::broken};
        } else {
            // fd and active are published as one lifecycle transaction. The
            // relaxed store is made visible to detach by this critical
            // section's release unlock and detach's acquire lock. Once it
            // succeeds, detach either has not started yet and must wait for
            // end_syscall(), or already published closing and made this branch
            // unreachable.
            fd_snapshot = fd;
            syscall_active[i].store(true, std::memory_order_relaxed);
        }
        unlock_lifecycle();
        return state;
    }

    void end_syscall(Dir d) noexcept {
        syscall_active[static_cast<unsigned>(d)].store(
            false, std::memory_order_release);
    }

    void wait_for_syscalls() const noexcept {
        for (unsigned i = 0; i < kDirCount; ++i) {
            while (syscall_active[i].load(std::memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                __asm__ __volatile__("yield");
#endif
            }
        }
    }
};

// Linearizes one native-fd syscall with descriptor teardown. The guard also
// keeps the IoDesc incarnation from being recycled: detach does not drop its
// owner reference until every active guard has released.
class FdUseGuard {
public:
    FdUseGuard(IoDesc* desc, Dir dir,
               std::uint32_t generation) noexcept
        : desc_(desc), dir_(dir) {
        if (desc_ == nullptr) {
            error_ = Error{EBADF};
            return;
        }
        error_ = desc_->begin_syscall(dir_, generation, fd_);
        if (error_) desc_ = nullptr;
    }

    ~FdUseGuard() { reset(); }

    FdUseGuard(const FdUseGuard&) = delete;
    FdUseGuard& operator=(const FdUseGuard&) = delete;
    FdUseGuard(FdUseGuard&&) = delete;
    FdUseGuard& operator=(FdUseGuard&&) = delete;

    explicit operator bool() const noexcept { return desc_ != nullptr; }
    Error error() const noexcept { return error_; }
    int fd() const noexcept { return fd_; }

    void reset() noexcept {
        if (desc_ == nullptr) return;
        IoDesc* const desc = desc_;
        desc_ = nullptr;
        desc->end_syscall(dir_);
    }

private:
    IoDesc* desc_ = nullptr;
    Dir dir_;
    int fd_ = -1;
    Error error_{};
};

// Awaits readiness in one direction.
//
// Always used inside the readiness loop; a single await is never a complete
// operation, because edge-triggered readiness can be spurious:
//
//     for (;;) {
//         n = ::read(fd, ...);
//         if (n >= 0) return n;
//         if (errno != EAGAIN) return Error::from_errno();
//         if (auto r = co_await IoAwaiter(desc, Dir::kRead); !r) return r.error();
//     }
class IoAwaiter {
public:
    IoAwaiter(IoDesc* desc, Dir dir, std::uint32_t generation) noexcept;

    // Detail-level convenience for callers that do not retain a descriptor
    // handle. Socket operations use the explicit incarnation-token overload.
    IoAwaiter(IoDesc* desc, Dir dir) noexcept
        : IoAwaiter(desc, dir,
                    desc == nullptr
                        ? 0
                        : desc->generation.load(std::memory_order_acquire)) {}
    ~IoAwaiter();

    IoAwaiter(const IoAwaiter&) = delete;
    IoAwaiter& operator=(const IoAwaiter&) = delete;

    bool await_ready() noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    Result<void> await_resume() noexcept;

private:
    IoDesc* desc_;
    Dir dir_;
    std::uint32_t generation_;
    bool retained_ = false;
    // An outstanding awaiter may outlive close() and a subsequent Socket move
    // assignment. Keep the source Reactor slab alive until the destructor has
    // returned from release_desc(); member destruction happens afterwards.
    std::shared_ptr<Scheduler> source_lifetime_;
    IoWait wait_{};
};

class Reactor {
public:
    explicit Reactor(Scheduler& sched, WorkerId shard_id = 0);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    // Registers `fd` for edge-triggered read+write notification. The fd must
    // already be non-blocking. The descriptor is released with detach().
    Result<IoDesc*> attach(int fd);

    // Unregisters the fd, waking any parked task with Errc::closed. Must be
    // called before the fd itself is closed.
    void detach(IoDesc* desc);

    // Waits for events and schedules the tasks they unblock. Returns the number
    // of events the kernel handed back, which is what lets a caller notice that
    // a non-blocking drain came up empty and stop repeating it.
    //   timeout_ns < 0  block until an event or wake()
    //   timeout_ns == 0 non-blocking drain
    // Returns -1 when another worker/monitor currently owns this shard's poll.
    int poll(std::int64_t timeout_ns);

    // Makes a blocked poll() return promptly. Safe from any thread.
    void wake() noexcept;

    // Arms (or with deadline_ns == 0, clears) the deadline for one direction.
    // Concurrent setters are serialized per direction.
    void set_deadline(IoDesc* desc, Dir dir,
                      std::uint32_t expected_generation,
                      std::int64_t deadline_ns);

    std::size_t registered() const noexcept {
        return registered_.load(std::memory_order_relaxed);
    }
    bool polling() const noexcept {
        return polling_.load(std::memory_order_acquire);
    }
    std::int64_t last_poll_ns() const noexcept {
        return last_poll_ns_.load(std::memory_order_relaxed);
    }
    // Monitor-to-owner ticket latch. The monitor writes its already-sampled
    // timestamp; the owner consumes the coalesced request at its bounded
    // service checkpoint.
    bool request_owner_poll_at(std::int64_t requested_ns) noexcept {
        std::int64_t expected = 0;
        return owner_poll_requested_ns_.compare_exchange_strong(
            expected, requested_ns, std::memory_order_release,
            std::memory_order_relaxed);
    }
    bool request_owner_poll() noexcept {
        return request_owner_poll_at(now_ns());
    }
    std::int64_t take_owner_poll_request_ns() noexcept {
        return owner_poll_requested_ns_.load(std::memory_order_acquire) == 0
                   ? 0
                   : owner_poll_requested_ns_.exchange(
                         0, std::memory_order_acquire);
    }
    bool take_owner_poll_request() noexcept {
        return take_owner_poll_request_ns() != 0;
    }
    std::int64_t owner_poll_request_ns() const noexcept {
        return owner_poll_requested_ns_.load(std::memory_order_acquire);
    }
    void nudge(std::int64_t deadline_ns) noexcept;

    // Wakes the task parked on (desc, dir), or records readiness if none.
    // A default-constructed `err` means "ready".
    // Fails both directions of a live descriptor with Errc::cancelled, waking
    // anything parked on it. Same shape as the deadline callback: taken under
    // the lifecycle lock and gated on the incarnation, so a hook that fires
    // after the descriptor was closed or recycled is a no-op.
    void cancel_waiters(IoDesc* desc, std::uint32_t expected_generation) noexcept;

    void unblock(IoDesc* desc, Dir dir, Error err) noexcept {
        unblock_impl(desc, dir, err, nullptr);
    }

    Scheduler& scheduler() noexcept { return sched_; }
    WorkerId shard_id() const noexcept { return shard_id_; }

private:
    friend class IoAwaiter;
    friend class Scheduler;
    friend struct SchedulerTestAccess;

    struct ReadyBatch {
        std::uint32_t total = 0;
        std::uint32_t unpublished_local_fifo = 0;
    };

    // One scheduler-owned control frame per shard. The monitor may queue this
    // frame globally when an owner poll ticket remains unserviced; whichever
    // ordinary worker resumes it drives poll(0), so same-runtime completions
    // retain the existing worker-local batch placement.
    struct DriverCoroutine;
    struct DriverSuspend;
    enum class DriverPhase : std::uint8_t {
        kUnavailable,
        kSuspended,
        kQueued,
        kRunning,
    };

    void initialize_driver();
    void destroy_driver() noexcept;
    DriverCoroutine driver_loop();
    void run_driver_once() noexcept;
    void observe_driver_coverage() noexcept;
    bool request_driver_at(std::int64_t requested_ns) noexcept;
    bool queue_driver() noexcept;
    void cover_driver_epoch(std::uint64_t epoch) noexcept;

    static std::coroutine_handle<> on_deadline(Timer* timer) noexcept;

    IoDesc* alloc_desc();
    void release_desc(IoDesc* desc) noexcept;
    void free_desc(IoDesc* desc) noexcept;
    IoDesc* desc_at(std::uint32_t index) noexcept;

    // When `batch` is non-null, completions use shard-aware I/O placement and
    // the poller keeps one local continuation without waking a thief.
    void unblock_impl(IoDesc* desc, Dir dir, Error err, ReadyBatch* batch) noexcept;
    void dispatch(std::uint64_t token, unsigned dirs, ReadyBatch* batch) noexcept;

    // Slab: fixed-size chunks, never freed, so pointers stay stable and a stale
    // event is always safe to dereference. Recycling is detected by generation,
    // not by pointer validity.
    static constexpr std::uint32_t kChunkShift = 9;
    static constexpr std::uint32_t kChunkSize = 1u << kChunkShift;
    static constexpr std::uint32_t kChunkMask = kChunkSize - 1;
    static constexpr std::uint32_t kMaxChunks = 8192;  // ~4M descriptors

    Scheduler& sched_;
    WorkerId shard_id_;
    int backend_fd_ = -1;  // epoll fd / kqueue fd
    int wake_fd_ = -1;     // eventfd (Linux) or pipe read end (BSD)
    int wake_write_fd_ = -1;
    std::atomic<bool> wake_pending_{false};
    CIO_CACHE_ALIGNED std::atomic<bool> polling_{false};
    CIO_CACHE_ALIGNED std::atomic<std::int64_t>
        owner_poll_requested_ns_{0};
    std::atomic<std::int64_t> last_poll_ns_{0};
    std::atomic<std::int64_t> poller_deadline_ns_{INT64_MAX};

    // Cold stale-reactor control state. requested_at is written before the
    // release publication of requested_epoch; readers double-check the epoch
    // when they need a coherent pair. covered/attempted are monotonic so a
    // late control frame can never erase a newer request.
    CIO_CACHE_ALIGNED std::coroutine_handle<> driver_handle_{};
    std::atomic<DriverPhase> driver_phase_{
        DriverPhase::kUnavailable};
    std::atomic<std::uint64_t> driver_requested_epoch_{0};
    std::atomic<std::uint64_t> driver_attempted_epoch_{0};
    std::atomic<std::uint64_t> driver_covered_epoch_{0};
    std::atomic<std::int64_t> driver_requested_at_ns_{0};

    // Keep monitor-written driver generations away from the descriptor chunk
    // pointers read for every delivered epoll event.
    CIO_CACHE_ALIGNED std::atomic<IoDesc*> chunks_[kMaxChunks]{};
    std::atomic<std::uint32_t> chunk_count_{0};
    std::atomic<std::uint32_t> registered_{0};
    std::mutex slab_mutex_;
    IoDesc* free_list_ = nullptr;
};

}  // namespace cio::detail
