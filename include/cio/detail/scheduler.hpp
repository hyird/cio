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
//   inbox       bounded MPSC queue for targeted remote submissions. Only its
//               worker consumes it.
//   global      mutex-guarded exceptional overflow/shutdown and external or
//               non-local completion fallback queue.
//
// Every worker owns an epoll/eventfd shard. It publishes idle before a final
// queue/inbox/stealable recheck; remote producers publish work, atomically
// claim that concrete idle worker, then write only its eventfd. Stealing scans
// published victims rather than every peer.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "cio/config.hpp"
#include "cio/detail/bitmap.hpp"
#include "cio/detail/blocking_pool.hpp"
#include "cio/detail/completion.hpp"
#include "cio/detail/queue.hpp"
#include "cio/detail/reactor.hpp"
#include "cio/detail/timer.hpp"
#include "cio/detail/worker_id.hpp"

namespace cio::detail {

class Scheduler;
struct SchedulerTestAccess;

class CIO_CACHE_ALIGNED Worker {
public:
    WorkerId index() const noexcept { return index_; }
    Scheduler& scheduler() const noexcept { return *sched_; }

private:
    friend class Scheduler;
    friend struct SchedulerTarget;
    friend struct SchedulerTestAccess;

    void run();

    // Fast path: runnext and local FIFO, with a periodic remote/overflow
    // fairness check.
    void* next_local() noexcept;
    void* take_local() noexcept;
    void* service_fairness() noexcept;
    CIO_NOINLINE void* service_global_fairness() noexcept;
    void* find_work() noexcept;
    void* steal_from_peers() noexcept;
    void* consume_searcher_credit() noexcept;
    void* drain_inbox() noexcept;
    void stage_fairness_item(void* item) noexcept;
    void repair_stealable() noexcept;

    bool push(void* item, bool publish = true) noexcept;
    CIO_NOINLINE void push_overflow(void* item) noexcept;
    void push_next(void* item) noexcept;
    std::uint32_t rand_up_to(std::uint32_t n) noexcept;

    Scheduler* sched_ = nullptr;
    // Cached stable completion identity. SchedulerTarget hot paths compare
    // this directly instead of chasing Worker -> Scheduler -> endpoint.
    CompletionEndpoint* completion_endpoint_ = nullptr;
    WorkerId index_ = 0;
    std::uint32_t tick_ = 0;
    std::uint32_t fair_cursor_ = 0;
    std::uint64_t rng_ = 0;
    // A victim-publication wake carries an obligation to search published
    // FIFOs before unrelated local/inbox/global work can consume that wake.
    // A victim producer grants it before attempting to claim this worker
    // idle; only this worker consumes it.
    std::atomic<bool> searcher_credit_{false};
    // Owner-only. A worker whose FIFO has never been published cannot have a
    // stealable bitmap bit to repair; private yield/handoff chains skip that
    // shared-line load entirely.
    bool has_published_stealable_ = false;

    struct CIO_CACHE_ALIGNED StealablePublication {
        // The owner writes publish_epoch before consulting clear_epoch.
        // A successful bitmap clearer increments clear_epoch before reading
        // publish_epoch. This SC handshake lets the common producer path skip
        // the shared bitmap entirely while still closing the clear race.
        std::atomic<std::uint64_t> publish_epoch{0};
        std::atomic<std::uint64_t> clear_epoch{0};
        std::uint64_t next_publish_epoch = 0;
        std::uint64_t seen_clear_epoch = 0;
        char padding[kCacheLine -
                     2 * sizeof(std::atomic<std::uint64_t>) -
                     2 * sizeof(std::uint64_t)]{};
    };
    static_assert(sizeof(StealablePublication) == kCacheLine);

    // The LIFO hand-off slot. Deliberately NOT stealable: it exists so that a
    // producer/consumer pair retains same-worker cache locality, and a peer
    // stealing it on every hop destroys exactly the locality it was added for.
    // Safe to withhold because only the owning worker ever writes it. Remote
    // producers use the target inbox or the shared fallback, so runnext can
    // never strand a task under a parked worker.
    std::atomic<void*> runnext_{nullptr};
    LocalRunQueue queue_;
    RemoteInbox inbox_;
    std::unique_ptr<Reactor> reactor_;
    std::thread thread_;
    StealablePublication stealable_publication_;
};

class Scheduler : public std::enable_shared_from_this<Scheduler> {
public:
    using IoCompletionRoute =
        ::cio::detail::IoCompletionRoute;

    Scheduler(std::size_t worker_count, std::size_t max_blocking_threads);
    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    void start();
    // Requests stop, wakes everything parked, joins all threads. Idempotent.
    void shutdown();
    bool stopping() const noexcept { return stop_.load(std::memory_order_acquire); }
    std::shared_ptr<Scheduler> shared_handle() noexcept {
        return weak_from_this().lock();
    }
    SchedulerTarget completion_target() const noexcept {
        return completion_target_;
    }

    // Makes a suspended task runnable. Safe from any thread, including threads
    // the runtime does not own.
    void schedule(std::coroutine_handle<> h) noexcept { schedule_frame(h.address()); }
    void schedule_frame(void* frame) noexcept;

    // Directed internal scheduling prefers a valid target's inbox; overflow
    // may degrade to shared execution, and invalid targets schedule normally.
    void schedule_to(std::coroutine_handle<> h, WorkerId worker) noexcept {
        schedule_to_frame(h.address(), worker);
    }
    void schedule_to_frame(void* frame, WorkerId worker) noexcept;

    // Reactor batch placement. Local FIFO entries are left unpublished until
    // finish_io_batch() so a readiness burst needs only one bitmap transition.
    // Runnext is private by construction; shared/overflow paths publish
    // themselves immediately.
    IoCompletionRoute schedule_io_completion(
        std::coroutine_handle<> h,
        WorkerId preferred_worker) noexcept;
    // Soft-affinity completion wakeups (I/O deadline/close, blocking-pool and
    // cross-runtime join). A local preferred worker may retain the
    // continuation; a monitor, foreign runtime/thread or different worker
    // publishes it globally so an idle worker can make progress even when the
    // preferred worker is executing a CPU-bound task.
    void schedule_completion_wake(
        std::coroutine_handle<> h,
        WorkerId preferred_worker) noexcept;
    void finish_io_batch(
        std::uint32_t unpublished_local_fifo) noexcept;

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
    // Soft-affinity batch scheduling for timer completions. The target owner
    // keeps a local batch; any foreign/different-worker producer publishes to
    // the shared queue so a CPU-bound preferred worker cannot strand it.
    void schedule_batch_to(void* const* frames, std::uint32_t n,
                           WorkerId target) noexcept;

    Reactor& reactor() noexcept;
    Reactor& reactor_for(WorkerId worker) noexcept;
    TimerService& timers() noexcept { return *timers_; }
    BlockingPool& blocking() noexcept { return *blocking_; }

    std::size_t worker_count() const noexcept { return workers_.size(); }
    WorkerId current_worker_id() const noexcept;
    bool valid_worker_id(WorkerId worker) const noexcept {
        return worker < workers_.size();
    }
    WorkerId choose_worker() noexcept;

    // Wakes arbitrary idle workers. Targeted remote submission bypasses these
    // and claims its concrete destination.
    void notify() noexcept;

    // Compatibility facade for old detail callers. Scheduling is immediate;
    // directed eventfd wake coalescing removes the old deferred-notify
    // obligation.
    void schedule_deferred(std::coroutine_handle<> h) noexcept;

    // Claims and directs eventfd wakes to at most `count` idle workers. Stops
    // after a full idle-bitmap scan finds none; a worker entering park observes
    // already-published shared work in its final recheck.
    void notify_batch(std::uint32_t count) noexcept;

    void nudge_poller(WorkerId worker, std::int64_t deadline_ns) noexcept;
    void nudge_poller(std::int64_t deadline_ns) noexcept {
        nudge_poller(current_worker_id(), deadline_ns);
    }

private:
    friend class Worker;
    friend class Reactor;
    friend struct SchedulerTarget;
    friend struct SchedulerTestAccess;

    void park(Worker& worker);
    void leave_park(Worker& worker) noexcept;
    bool worker_has_work(Worker& worker) noexcept;
    void enqueue_remote(WorkerId target, void* frame, bool wake) noexcept;
    CIO_NOINLINE void schedule_completion_fallback(
        void* frame, WorkerId preferred_worker) noexcept;
    void wake_worker(WorkerId worker) noexcept;
    bool wake_one_idle(WorkerId start = 0) noexcept;
    bool wake_one_searcher(WorkerId start = 0) noexcept;
    void publish_stealable(Worker& worker, std::uint32_t wake_count = 1) noexcept;
    bool repair_stealable(Worker& worker) noexcept;
    void poller_returned(WorkerId shard) noexcept;
    void monitor_main();

    std::vector<std::unique_ptr<Worker>> workers_;
    GlobalRunQueue global_;
    std::atomic<WorkerId> placement_next_{0};
    std::atomic<WorkerId> wake_cursor_{0};

    std::unique_ptr<TimerService> timers_;
    std::unique_ptr<BlockingPool> blocking_;

    std::unique_ptr<AtomicWorkerBitmap> idle_workers_;
    std::unique_ptr<AtomicWorkerBitmap> stealable_workers_;

    std::atomic<bool> stop_{false};
    std::atomic<bool> started_{false};
    std::thread monitor_;
    SchedulerTarget completion_target_;
};

// The worker running on this thread, or nullptr on a foreign thread.
Worker* current_worker() noexcept;
WorkerId current_worker_id(const Scheduler* sched = nullptr) noexcept;

// The scheduler this thread belongs to: the current worker's, or the process
// default set by the most recently constructed Runtime.
Scheduler* current_scheduler() noexcept;
void set_default_scheduler(Scheduler* sched) noexcept;

}  // namespace cio::detail
