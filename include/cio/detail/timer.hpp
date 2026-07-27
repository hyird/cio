// Timers.
//
// Sharded 4-ary min-heaps, one shard per worker. A task arms its timer on the
// shard belonging to the worker it is currently running on, so the common case
// (every I/O op carrying a deadline) never touches a shared lock.
//
// Each shard publishes its earliest deadline in an atomic so the worker that is
// blocked in the reactor can compute its poll timeout by reading N atomics
// instead of locking N heaps.
#pragma once

#include <atomic>
#include <cstdint>
#include <coroutine>
#include <memory>
#include <mutex>
#include <vector>

#include "cio/clock.hpp"
#include "cio/config.hpp"

namespace cio::detail {

class Scheduler;

// A timer node. Lives inside the awaiter, which lives inside the coroutine
// frame, so arming a timer never allocates.
struct Timer {
    enum State : std::uint32_t {
        kIdle = 0,       // not in any heap
        kArmed = 1,      // in a heap, waiting for its deadline
        kFiring = 2,     // on_fire is running and still touching this node
        kFired = 3,      // done; nobody will touch this node again
        kCancelled = 4   // removed before firing
    };

    // Invoked when the deadline is reached, instead of simply scheduling
    // `waiter`. Used where the timer races another waker (a select case) and
    // the winner has to be decided by a claim.
    //
    // It returns the task to resume rather than resuming it itself, and that is
    // load-bearing: resuming a task lets its frame — which is where this Timer
    // node lives — be destroyed. The timer service publishes kFired *before*
    // scheduling, so a concurrent disarm() knows exactly when the node has
    // stopped being touched.
    using FireFn = std::coroutine_handle<> (*)(Timer*) noexcept;

    std::int64_t deadline_ns = 0;
    std::coroutine_handle<> waiter{};
    FireFn on_fire = nullptr;

    // Set when the timer fires; the awaiter reads it after being resumed.
    std::atomic<std::uint32_t> state{kIdle};

    // Filled in by TimerService::arm().
    std::uint32_t shard = 0;
    std::uint32_t heap_index = ~0u;  // position in the shard heap, for O(log n) removal
};

class TimerService {
public:
    TimerService(Scheduler& sched, std::size_t shard_count);
    ~TimerService();

    TimerService(const TimerService&) = delete;
    TimerService& operator=(const TimerService&) = delete;

    // Arms `t` on the calling worker's shard (shard 0 for non-worker threads).
    // The caller must have set t->deadline_ns and t->waiter.
    void arm(Timer* t);

    // Attempts to remove an armed timer. Returns true if it was removed before
    // firing (the caller then owns the waiter and must resume it); false if the
    // timer already fired, in which case the timer thread owns the waiter.
    //
    // On the false path it does not return until the firing callback has
    // finished touching the node, so the caller may then destroy it.
    bool disarm(Timer* t);

    // Nanoseconds until the earliest deadline across all shards, clamped to
    // >= 0. Returns -1 when no timer is armed (poll indefinitely). Nanoseconds
    // rather than milliseconds because the reactor can wait with that
    // resolution (epoll_pwait2), and rounding every sleep up to a millisecond
    // is a large relative error for short timeouts.
    std::int64_t next_timeout_ns() const noexcept;

    // Nanoseconds until the earliest deadline, or INT64_MAX if none.
    std::int64_t next_deadline_ns() const noexcept;

    // Fires every timer whose deadline has passed, scheduling its waiter.
    // Returns the number fired. Safe to call from any thread.
    std::size_t run_expired();

    // Fires every armed timer with kCancelled, used during shutdown so parked
    // tasks unwind instead of leaking.
    void drain_all();

    bool empty() const noexcept;

private:
    struct CIO_CACHE_ALIGNED Shard {
        mutable std::mutex mutex;
        std::vector<Timer*> heap;  // 4-ary min-heap on deadline_ns
        // Earliest deadline in `heap`, or INT64_MAX. Written under `mutex`,
        // read without it by the poller.
        std::atomic<std::int64_t> earliest{INT64_MAX};
    };

    static void sift_up(std::vector<Timer*>& heap, std::size_t i) noexcept;
    static void sift_down(std::vector<Timer*>& heap, std::size_t i) noexcept;
    static void heap_remove(std::vector<Timer*>& heap, std::size_t i) noexcept;
    void republish(Shard& s) noexcept;
    std::size_t run_expired_shard(Shard& s, std::int64_t now);

    Scheduler& sched_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace cio::detail
