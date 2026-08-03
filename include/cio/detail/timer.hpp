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
#include <coroutine>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cio/clock.hpp"
#include "cio/config.hpp"
#include "cio/detail/worker_id.hpp"

namespace cio::detail {

class Scheduler;

// A timer node. Lives inside the awaiter, which lives inside the coroutine
// frame, so arming a timer never allocates.
struct Timer {
    enum State : std::uint32_t {
        kIdle = 0,      // not in any heap
        kArmed = 1,     // in a heap, waiting for its deadline
        kFiring = 2,    // on_fire is running and still touching this node
        kFired = 3,     // done; nobody will touch this node again
        kCancelled = 4  // removed before firing
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

    Timer() noexcept = default;

    struct ArmTag {
        explicit ArmTag() = default;
    };

    // Lazy sleep awaiters construct the node only when they actually suspend.
    // arm() overwrites state and shard, so initialize just the fields it reads
    // plus heap_index, whose sentinel is asserted before insertion.
    Timer(ArmTag, std::int64_t deadline, std::coroutine_handle<> coroutine,
          FireFn fire) noexcept
        : deadline_ns(deadline),
          waiter(coroutine),
          on_fire(fire),
          state(kIdle),
          heap_index(~0u) {}

    std::int64_t deadline_ns = 0;
    std::coroutine_handle<> waiter{};
    FireFn on_fire = nullptr;

    // Set when the timer fires; the awaiter reads it after being resumed.
    std::atomic<std::uint32_t> state{kIdle};

    // Filled in by TimerService::arm().
    std::uint32_t shard = 0;
    WorkerId preferred_worker = kInvalidWorkerId;
    std::uint32_t heap_index =
        ~0u;  // position in the shard heap, for O(log n) removal
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
    bool disarm(Timer* t) {
        const std::uint32_t state = t->state.load(std::memory_order_acquire);
        if (state == Timer::kIdle || state == Timer::kCancelled ||
            state == Timer::kFired) {
            return false;
        }
        return disarm_slow(t, state);
    }

    // Nanoseconds until the earliest deadline across all shards, clamped to
    // >= 0. Returns -1 when no timer is armed (poll indefinitely). Nanoseconds
    // rather than milliseconds because the reactor can wait with that
    // resolution (epoll_pwait2), and rounding every sleep up to a millisecond
    // is a large relative error for short timeouts.
    std::int64_t next_timeout_ns() const noexcept;
    std::int64_t next_timeout_ns(WorkerId worker) const noexcept;

    // Nanoseconds until the earliest deadline, or INT64_MAX if none.
    std::int64_t next_deadline_ns() const noexcept;
    std::int64_t next_deadline_ns(WorkerId worker) const noexcept;

    // Fires every timer whose deadline has passed, scheduling its waiter.
    // Returns the number fired. Safe to call from any thread.
    std::size_t run_expired();
    std::size_t run_expired(WorkerId worker);
    // Scheduler checkpoints that already sampled the clock pass that same
    // timestamp through, avoiding a second clock read on the firing path.
    std::size_t run_expired(WorkerId worker, std::int64_t now);

    // Fires every armed timer with kCancelled, used during shutdown so parked
    // tasks unwind instead of leaking.
    void drain_all();

    bool empty() const noexcept;

private:
    class ShardMutex {
    public:
        bool try_lock() noexcept {
            return !locked_.test_and_set(std::memory_order_acquire);
        }

        void lock() noexcept {
            if (try_lock()) return;
            unsigned spins = 0;
            for (;;) {
                while (locked_.test(std::memory_order_relaxed)) {
                    cio::cpu_relax();
                    if (++spins == 64) {
                        spins = 0;
                        std::this_thread::yield();
                    }
                }
                if (try_lock()) return;
            }
        }

        void unlock() noexcept { locked_.clear(std::memory_order_release); }

    private:
        std::atomic_flag locked_ = ATOMIC_FLAG_INIT;
    };

    struct CIO_CACHE_ALIGNED Shard {
        mutable ShardMutex mutex;
        std::vector<Timer*> heap;  // 4-ary min-heap on deadline_ns
        // Earliest deadline in `heap`, or INT64_MAX. Written under `mutex`,
        // read without it by the poller. Keep monitor scans off the cache line
        // whose lock word and heap metadata the owner mutates on every arm.
        CIO_CACHE_ALIGNED std::atomic<std::int64_t> earliest{INT64_MAX};
    };

    static void sift_up(std::vector<Timer*>& heap, std::size_t i) noexcept;
    static void sift_down(std::vector<Timer*>& heap, std::size_t i) noexcept;
    static void heap_pop_root(std::vector<Timer*>& heap) noexcept;
    static void heap_remove(std::vector<Timer*>& heap, std::size_t i) noexcept;
    void republish(Shard& s) noexcept;
    bool disarm_slow(Timer* t, std::uint32_t state);
    std::size_t run_expired_shard(Shard& s, std::int64_t now);

    Scheduler& sched_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace cio::detail
