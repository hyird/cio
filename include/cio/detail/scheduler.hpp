// The M:N scheduler.
//
// One OS thread per worker, each owning a run queue. Work moves between them by
// stealing, never by a central dispatcher. The pieces:
//
//   runnext      one slot, LIFO. A task woken by a channel hand-off goes here,
//                so a producer/consumer pair ping-pongs on one core with its
//                data still in L1.
//   local queue  bounded ring, FIFO, stealable in halves. FIFO here is what
//                stops a hot pair from starving the rest of the worker's work.
//   global queue mutex-guarded overflow and cross-thread submission point.
//                Checked every 61 iterations so it cannot starve.
//
// Idle workers follow Go's spinning protocol: a worker announces itself as a
// searcher before scanning, and un-announces itself before parking with a
// recheck in between. That ordering is what closes the lost-wakeup race without
// putting a lock on the scheduling fast path — schedule() only pays one atomic
// load when a searcher is already awake.
//
// One worker at a time volunteers as the poller and blocks in the reactor with
// a timeout derived from the earliest armed timer, so an idle runtime consumes
// no CPU and a timer fires without anybody spinning on the clock.
#pragma once

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cio/config.hpp"
#include "cio/detail/blocking_pool.hpp"
#include "cio/detail/queue.hpp"
#include "cio/detail/reactor.hpp"
#include "cio/detail/timer.hpp"

namespace cio::detail {

class Scheduler;

class CIO_CACHE_ALIGNED Worker {
public:
    std::uint32_t index() const noexcept { return index_; }
    Scheduler& scheduler() const noexcept { return *sched_; }

private:
    friend class Scheduler;

    void run();

    // Fast path: runnext, then the local ring, with a periodic global-queue
    // check for fairness.
    void* next_local() noexcept;
    // Slow path: global queue, then steal from a random rotation of peers,
    // then a non-blocking reactor drain.
    void* find_work() noexcept;
    void* steal_from_peers() noexcept;

    void push(void* item) noexcept;
    void push_next(void* item) noexcept;
    std::uint32_t rand_up_to(std::uint32_t n) noexcept;

    Scheduler* sched_ = nullptr;
    std::uint32_t index_ = 0;
    std::uint32_t tick_ = 0;
    std::uint64_t rng_ = 0;

    // The LIFO hand-off slot. Deliberately NOT stealable: it exists so that a
    // producer/consumer pair stays pinned to one core, and a peer stealing it
    // on every hop destroys exactly the locality it was added for. Safe to
    // withhold because only the owning worker ever writes it (a remote wake
    // falls back to the global queue), so it can never strand a task under a
    // parked worker.
    std::atomic<void*> runnext_{nullptr};
    LocalRunQueue queue_;
    std::thread thread_;
};

class Scheduler {
public:
    Scheduler(std::size_t worker_count, std::size_t max_blocking_threads);
    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    void start();
    // Requests stop, wakes everything parked, joins all threads. Idempotent.
    void shutdown();
    bool stopping() const noexcept { return stop_.load(std::memory_order_acquire); }

    // Makes a suspended task runnable. Safe from any thread, including threads
    // the runtime does not own.
    void schedule(std::coroutine_handle<> h) noexcept { schedule_frame(h.address()); }
    void schedule_frame(void* frame) noexcept;

    // Re-queues a task that is voluntarily giving up the worker. Skips the
    // wake path: the worker doing this is about to loop straight back into its
    // own queue, so waking a peer only buys a thread round-trip and a stolen
    // task's worth of cache misses.
    void reschedule_self(std::coroutine_handle<> h) noexcept;

    // Hand-off: run this task next on the current worker. Use when the caller
    // just produced the exact data the target is waiting for.
    void schedule_next(std::coroutine_handle<> h) noexcept { schedule_next_frame(h.address()); }
    void schedule_next_frame(void* frame) noexcept;

    void schedule_batch(void* const* frames, std::uint32_t n) noexcept;

    Reactor& reactor() noexcept { return *reactor_; }
    TimerService& timers() noexcept { return *timers_; }
    BlockingPool& blocking() noexcept { return *blocking_; }

    std::size_t worker_count() const noexcept { return workers_.size(); }

    // Wakes one parked worker if one exists and no searcher is already awake.
    void notify() noexcept;

    // Pushes a task without waking anybody. The caller must follow up with
    // notify_batch(). Only for callers that make many tasks runnable at once
    // and know the count.
    void schedule_deferred(std::coroutine_handle<> h) noexcept;

    // Wakes up to `count` parked workers at once.
    //
    // notify() deliberately wakes exactly one, because the common case is a
    // single task becoming runnable and any more would be wasted futex traffic.
    // But a reactor poll can make two hundred tasks runnable in one syscall,
    // and with single wakes the workers ramp up one at a time — each waking the
    // next only after it has found work — which serialises a futex round trip
    // per worker onto the critical path of every burst.
    void notify_batch(std::uint32_t count) noexcept;

    // Called when a timer is armed with a deadline earlier than whatever the
    // parked poller is currently waiting for. Avoids an eventfd write for the
    // overwhelmingly common case of arming a timer that is not the earliest.
    void nudge_poller(std::int64_t deadline_ns) noexcept;

private:
    friend class Worker;

    // Returns true if the worker woke holding a searcher credit handed to it by
    // notify(), in which case it must not increment spinning_ again.
    bool park(Worker& w);
    bool any_work_available() const noexcept;
    void monitor_main();

    std::vector<std::unique_ptr<Worker>> workers_;
    GlobalRunQueue global_;

    std::unique_ptr<Reactor> reactor_;
    std::unique_ptr<TimerService> timers_;
    std::unique_ptr<BlockingPool> blocking_;

    // Idle/searcher bookkeeping. `spinning_` is the load-bearing one: a
    // non-zero value lets schedule() skip the wake path entirely.
    CIO_CACHE_ALIGNED std::atomic<std::uint32_t> spinning_{0};
    CIO_CACHE_ALIGNED std::atomic<std::uint32_t> idle_{0};
    CIO_CACHE_ALIGNED std::atomic<bool> polling_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> started_{false};
    std::atomic<std::int64_t> last_poll_ns_{0};
    // Deadline the parked poller is currently blocked until, INT64_MAX when
    // nobody is polling or the poll is untimed.
    std::atomic<std::int64_t> poller_deadline_ns_{INT64_MAX};

    std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
    std::uint32_t wake_tokens_ = 0;  // guarded by idle_mutex_
    std::uint32_t waiters_ = 0;      // workers actually blocked on idle_cv_

    std::thread monitor_;
};

// The worker running on this thread, or nullptr on a foreign thread.
Worker* current_worker() noexcept;

// The scheduler this thread belongs to: the current worker's, or the process
// default set by the most recently constructed Runtime.
Scheduler* current_scheduler() noexcept;
void set_default_scheduler(Scheduler* sched) noexcept;

}  // namespace cio::detail
