#include "cio/detail/scheduler.hpp"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cio/clock.hpp"
#include "cio/detail/metrics.hpp"

namespace cio::detail {

struct CompletionEndpoint {
    static constexpr std::uint64_t kClosed = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kCountMask = kClosed - 1;

    std::atomic<std::uint64_t> state{kClosed};
    std::atomic<Scheduler*> scheduler{nullptr};
};

namespace {

struct CompletionRegistry {
    std::mutex mutex;
    std::vector<std::unique_ptr<CompletionEndpoint>> storage;
};

CompletionRegistry& completion_registry() {
    // SchedulerTarget tokens are allowed to outlive arbitrary global objects.
    // Keeping the arena reachable until process teardown makes every endpoint
    // address permanently valid, including during static destruction.
    static CompletionRegistry* const registry = new CompletionRegistry;
    return *registry;
}

SchedulerTarget acquire_completion_target(Scheduler* scheduler) {
    CompletionRegistry& registry = completion_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);

    auto owned = std::make_unique<CompletionEndpoint>();
    CompletionEndpoint* const endpoint = owned.get();
    endpoint->scheduler.store(scheduler, std::memory_order_relaxed);
    registry.storage.push_back(std::move(owned));
    // Opening is the commit point for the scheduler pointer. The endpoint is
    // never reused, so a closed token can never name another Scheduler.
    endpoint->state.store(0, std::memory_order_release);
    return SchedulerTarget{endpoint};
}

void release_completion_endpoint(CompletionEndpoint* endpoint) noexcept {
    const std::uint64_t previous =
        endpoint->state.fetch_sub(1, std::memory_order_acq_rel);
    assert((previous & CompletionEndpoint::kCountMask) != 0);
    if ((previous & CompletionEndpoint::kClosed) != 0 &&
        (previous & CompletionEndpoint::kCountMask) == 1) {
        endpoint->state.notify_all();
    }
}

void retire_completion_target(SchedulerTarget target) noexcept {
    CompletionEndpoint* const endpoint = target.endpoint;
    if (endpoint == nullptr) return;

    std::uint64_t observed =
        endpoint->state.fetch_or(CompletionEndpoint::kClosed,
                                 std::memory_order_acq_rel) |
        CompletionEndpoint::kClosed;
    while ((observed & CompletionEndpoint::kCountMask) != 0) {
        endpoint->state.wait(observed, std::memory_order_acquire);
        observed = endpoint->state.load(std::memory_order_acquire);
    }
    endpoint->scheduler.store(nullptr, std::memory_order_release);
}

thread_local Worker* t_worker = nullptr;
std::atomic<Scheduler*> g_default_scheduler{nullptr};

constexpr std::int64_t kMonitorPollStaleNs = 200'000;    // 200us
constexpr std::int64_t kMonitorDriverGraceNs = 200'000;  // 200us
constexpr std::uint32_t kInboxDrainBatch = 32;
// While a worker has an indefinitely non-empty local queue, this bounds how
// many coroutine resumptions may pass before it services targeted submissions,
// its reactor shard and its timer shard. This periodic checkpoint is one of
// two bounded monitor-ticket acknowledgement sites; the other is the cold
// I/O-completion quota checkpoint. Neither adds a shared load to ordinary
// I/O completions or task selections.
constexpr std::uint32_t kLocalServiceInterval = 32;

}  // namespace

thread_local constinit std::uint64_t t_cooperative_io_budget = 0;
#if defined(__GNUC__) || defined(__clang__)
extern thread_local constinit std::uint64_t t_cooperative_io_budget_local
    __attribute__((alias("_ZN3cio6detail23t_cooperative_io_budgetE"),
                   visibility("hidden")));
#endif

// A stable scheduler control frame, allocated once per Reactor. It never
// completes by itself: each resume performs at most one non-blocking shard
// poll and suspends again. Reactor owns and destroys the frame only after the
// monitor and all workers have joined, so a queued-but-never-run control item
// cannot leak or outlive its shard during shutdown.
struct Reactor::DriverCoroutine {
    struct promise_type {
        DriverCoroutine get_return_object() noexcept {
            return DriverCoroutine{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() const noexcept { return {}; }
        std::suspend_always final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}
        [[noreturn]] void unhandled_exception() const noexcept {
            std::terminate();
        }
    };

    using Handle = std::coroutine_handle<promise_type>;

    explicit DriverCoroutine(Handle handle) noexcept : handle_(handle) {}
    DriverCoroutine(DriverCoroutine&& other) noexcept
        : handle_(std::exchange(other.handle_, {})) {}
    DriverCoroutine(const DriverCoroutine&) = delete;
    DriverCoroutine& operator=(const DriverCoroutine&) = delete;
    ~DriverCoroutine() {
        if (handle_) handle_.destroy();
    }

    Handle release() noexcept { return std::exchange(handle_, {}); }

private:
    Handle handle_{};
};

struct Reactor::DriverSuspend {
    Reactor* reactor;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) const noexcept {
        // This release publication is deliberately the final operation in
        // await_suspend. Once visible, another worker may queue and resume the
        // same stable frame immediately.
        reactor->driver_phase_.store(DriverPhase::kSuspended,
                                     std::memory_order_release);
    }
    void await_resume() const noexcept {}
};

Reactor::DriverCoroutine Reactor::driver_loop() {
    for (;;) {
        run_driver_once();
        co_await DriverSuspend{this};
    }
}

void Reactor::initialize_driver() {
    DriverCoroutine driver = driver_loop();
    driver_handle_ = driver.release();
    driver_phase_.store(DriverPhase::kSuspended, std::memory_order_release);
}

void Reactor::destroy_driver() noexcept {
    driver_phase_.store(DriverPhase::kUnavailable, std::memory_order_release);
    const std::coroutine_handle<> handle = std::exchange(driver_handle_, {});
    if (handle) handle.destroy();
}

void Reactor::cover_driver_epoch(std::uint64_t epoch) noexcept {
    std::uint64_t covered =
        driver_covered_epoch_.load(std::memory_order_relaxed);
    while (covered < epoch && !driver_covered_epoch_.compare_exchange_weak(
                                  covered, epoch, std::memory_order_release,
                                  std::memory_order_relaxed)) {
    }
}

void Reactor::observe_driver_coverage() noexcept {
    for (;;) {
        const std::uint64_t requested =
            driver_requested_epoch_.load(std::memory_order_acquire);
        if (driver_covered_epoch_.load(std::memory_order_acquire) >=
            requested) {
            return;
        }
        const std::int64_t requested_at =
            driver_requested_at_ns_.load(std::memory_order_relaxed);
        if (driver_requested_epoch_.load(std::memory_order_acquire) !=
            requested) {
            continue;
        }
        if (last_poll_ns_.load(std::memory_order_acquire) >= requested_at) {
            cover_driver_epoch(requested);
        }
        return;
    }
}

bool Reactor::request_driver_at(std::int64_t requested_ns) noexcept {
    observe_driver_coverage();
    const std::uint64_t requested =
        driver_requested_epoch_.load(std::memory_order_acquire);
    if (driver_covered_epoch_.load(std::memory_order_acquire) < requested) {
        return false;
    }

    // monitor_main is the sole producer. Publish the timestamp before its
    // generation so a worker's acquire snapshot cannot pair a new epoch with
    // the preceding request's deadline.
    driver_requested_at_ns_.store(requested_ns, std::memory_order_relaxed);
    driver_requested_epoch_.store(requested + 1, std::memory_order_release);
    return true;
}

bool Reactor::queue_driver() noexcept {
    const std::uint64_t requested =
        driver_requested_epoch_.load(std::memory_order_acquire);
    if (driver_covered_epoch_.load(std::memory_order_acquire) >= requested ||
        driver_attempted_epoch_.load(std::memory_order_acquire) >= requested) {
        return false;
    }

    DriverPhase expected = DriverPhase::kSuspended;
    if (!driver_phase_.compare_exchange_strong(expected, DriverPhase::kQueued,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        return false;
    }

    // This is intentionally global, not a fixed owner's MPSC inbox. The
    // control item has no hard affinity, and any idle worker can provide the
    // worker context that keeps completions local.
    sched_.global_.push(driver_handle_.address());
    sched_.wake_one_idle(shard_id_ + 1);
    return true;
}

void Reactor::run_driver_once() noexcept {
    driver_phase_.store(DriverPhase::kRunning, std::memory_order_relaxed);

    std::uint64_t requested = 0;
    std::int64_t requested_at = 0;
    for (;;) {
        requested = driver_requested_epoch_.load(std::memory_order_acquire);
        requested_at = driver_requested_at_ns_.load(std::memory_order_relaxed);
        if (driver_requested_epoch_.load(std::memory_order_acquire) ==
            requested) {
            break;
        }
    }

    std::uint64_t attempted =
        driver_attempted_epoch_.load(std::memory_order_relaxed);
    while (attempted < requested &&
           !driver_attempted_epoch_.compare_exchange_weak(
               attempted, requested, std::memory_order_release,
               std::memory_order_relaxed)) {
    }

    if (driver_covered_epoch_.load(std::memory_order_acquire) >= requested ||
        last_poll_ns_.load(std::memory_order_acquire) >= requested_at) {
        cover_driver_epoch(requested);
        return;
    }

    if (sched_.stopping() || registered() == 0) {
        cover_driver_epoch(requested);
        return;
    }

    const int result = poll(0);
    if (result >= 0) {
        // Cover only the generation captured before poll(). A newer request
        // published during this run remains outstanding and will requeue the
        // frame after DriverSuspend makes it safely resumable.
        cover_driver_epoch(requested);
    } else {
        // Another owner already has polling_. Its eventual freshness
        // publication, or the monitor's absolute grace backstop, provides
        // coverage; a failed claim is not completion by itself.
        observe_driver_coverage();
    }
}

SchedulerLease::SchedulerLease(SchedulerLease&& other) noexcept
    : scheduler_(std::exchange(other.scheduler_, nullptr)),
      endpoint_(std::exchange(other.endpoint_, nullptr)) {}

SchedulerLease& SchedulerLease::operator=(SchedulerLease&& other) noexcept {
    if (this == &other) return *this;
    reset();
    scheduler_ = std::exchange(other.scheduler_, nullptr);
    endpoint_ = std::exchange(other.endpoint_, nullptr);
    return *this;
}

SchedulerLease::~SchedulerLease() {
    reset();
}

void SchedulerLease::reset() noexcept {
    CompletionEndpoint* const endpoint = std::exchange(endpoint_, nullptr);
    scheduler_ = nullptr;
    if (endpoint != nullptr) {
        release_completion_endpoint(endpoint);
    }
}

SchedulerLease SchedulerTarget::lock() const noexcept {
    if (endpoint == nullptr) return {};

    std::uint64_t observed = endpoint->state.load(std::memory_order_acquire);
    for (;;) {
        if ((observed & CompletionEndpoint::kClosed) != 0 ||
            (observed & CompletionEndpoint::kCountMask) ==
                CompletionEndpoint::kCountMask) {
            return {};
        }
        if (endpoint->state.compare_exchange_weak(observed, observed + 1,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            break;
        }
    }

    Scheduler* const scheduler =
        endpoint->scheduler.load(std::memory_order_acquire);
    if (scheduler == nullptr) {
        release_completion_endpoint(endpoint);
        return {};
    }
    return SchedulerLease{scheduler, endpoint};
}

namespace {

CIO_NOINLINE void schedule_target_slow(SchedulerTarget target,
                                       std::coroutine_handle<> handle,
                                       WorkerId preferred_worker) noexcept {
    SchedulerLease scheduler = target.lock();
    if (scheduler && !scheduler->stopping()) {
        scheduler->schedule_completion_wake(handle, preferred_worker);
    }
}

CIO_NOINLINE void schedule_target_next_slow(
    SchedulerTarget target, std::coroutine_handle<> handle) noexcept {
    SchedulerLease scheduler = target.lock();
    if (scheduler && !scheduler->stopping()) {
        scheduler->schedule_next(handle);
    }
}

CIO_NOINLINE bool schedule_target_io_slow(SchedulerTarget target,
                                          std::coroutine_handle<> handle,
                                          WorkerId preferred_worker,
                                          IoCompletionRoute& route) noexcept {
    SchedulerLease scheduler = target.lock();
    if (!scheduler || scheduler->stopping()) {
        return false;
    }
    route = scheduler->schedule_io_completion(handle, preferred_worker);
    return true;
}

}  // namespace

void SchedulerTarget::dispatch(SchedulerTarget target,
                               std::coroutine_handle<> handle,
                               WorkerId preferred_worker) noexcept {
    Worker* const worker = t_worker;
    if (CIO_LIKELY(worker != nullptr &&
                   worker->completion_endpoint_ == target.endpoint)) {
        // This worker cannot outlive its Scheduler: shutdown joins it before
        // releasing any owned storage. A concurrent stop may abandon this
        // frame either way, so the hot same-runtime path needs no stop load.
        worker->push(handle.address());
        return;
    }
    schedule_target_slow(target, handle, preferred_worker);
}

void SchedulerTarget::dispatch_next(SchedulerTarget target,
                                    std::coroutine_handle<> handle) noexcept {
    Worker* const worker = t_worker;
    if (CIO_LIKELY(worker != nullptr &&
                   worker->completion_endpoint_ == target.endpoint)) {
        worker->push_next(handle.address());
        return;
    }
    schedule_target_next_slow(target, handle);
}

void SchedulerTarget::dispatch_completion(SchedulerTarget target,
                                          std::coroutine_handle<> handle,
                                          WorkerId preferred_worker) noexcept {
    Worker* const worker = t_worker;
    if (CIO_LIKELY(worker != nullptr &&
                   worker->completion_endpoint_ == target.endpoint)) {
        Scheduler* const scheduler = worker->sched_;
        if (!scheduler->valid_worker_id(preferred_worker) ||
            worker->index_ == preferred_worker) {
            worker->push(handle.address());
        } else {
            scheduler->schedule_completion_fallback(handle.address(),
                                                    preferred_worker);
        }
        return;
    }
    schedule_target_slow(target, handle, preferred_worker);
}

bool SchedulerTarget::dispatch_io(SchedulerTarget target,
                                  std::coroutine_handle<> handle,
                                  WorkerId preferred_worker,
                                  IoCompletionRoute& route) noexcept {
    Worker* const worker = t_worker;
    if (CIO_LIKELY(worker != nullptr &&
                   worker->completion_endpoint_ == target.endpoint)) {
        if (worker->runnext_.load(std::memory_order_relaxed) == nullptr) {
            worker->runnext_.store(handle.address(), std::memory_order_release);
            route = IoCompletionRoute::kRunnext;
        } else {
            route = worker->push(handle.address(), false)
                        ? IoCompletionRoute::kLocalFifo
                        : IoCompletionRoute::kSharedFallback;
        }
        return true;
    }
    return schedule_target_io_slow(target, handle, preferred_worker, route);
}

Worker* current_worker() noexcept {
    return t_worker;
}

WorkerId current_worker_id(const Scheduler* sched) noexcept {
    if (t_worker == nullptr) return kInvalidWorkerId;
    if (sched != nullptr && &t_worker->scheduler() != sched) {
        return kInvalidWorkerId;
    }
    return t_worker->index();
}

Scheduler* current_scheduler() noexcept {
    if (t_worker != nullptr) return &t_worker->scheduler();
    return g_default_scheduler.load(std::memory_order_acquire);
}

void set_default_scheduler(Scheduler* sched) noexcept {
    g_default_scheduler.store(sched, std::memory_order_release);
}

// ---------------------------------------------------------------- Worker ---

std::uint32_t Worker::rand_up_to(std::uint32_t n) noexcept {
    rng_ ^= rng_ << 13;
    rng_ ^= rng_ >> 7;
    rng_ ^= rng_ << 17;
    if (n == 0) return 0;
    const std::uint32_t random = static_cast<std::uint32_t>(rng_ >> 32);
    return static_cast<std::uint32_t>(
        (static_cast<std::uint64_t>(random) * n) >> 32);
}

bool Worker::push(void* item, bool publish, std::uint8_t* spawn_mode) noexcept {
    if (CIO_LIKELY(queue_.push(item, spawn_mode))) {
        if (publish) sched_->publish_stealable(*this);
        return true;
    }

    push_overflow(item);
    return false;
}

void Worker::push_overflow(void* item) noexcept {
    CIO_METRIC(local_overflow, 1);
    void* batch[kLocalQueueCapacity / 2 + 1];
    const std::uint32_t n = queue_.pop_half(batch);
    batch[n] = item;
    sched_->global_.push_batch(batch, n + 1);

    // The ring may still contain the half retained for this owner. Overflow is
    // exceptional, so waking an arbitrary idle worker for the global batch is
    // preferable to complicating the local producer contract.
    if (queue_.maybe_nonempty()) sched_->publish_stealable(*this);
    sched_->wake_one_idle(index_ + 1);
}

void Worker::push_next(void* item) noexcept {
    void* previous = runnext_.load(std::memory_order_relaxed);
    runnext_.store(item, std::memory_order_release);
    if (previous != nullptr) {
        // A displaced direct handoff is still part of the same cache-affine
        // synchronization chain. Keep it private with runnext; publishing each
        // displacement made idle workers steal one lock/channel waiter at a
        // time, turning serialized work into an eventfd/park round trip per
        // operation.
        push(previous, false);
    }
}

void Worker::repair_stealable() noexcept {
    if (!has_published_stealable_) return;
    has_published_stealable_ = sched_->repair_stealable(*this);
}

void* Worker::drain_inbox() noexcept {
    void* first = inbox_.pop();
    if (first == nullptr) return nullptr;

    std::uint32_t drained = 1;
    std::uint32_t promoted = 0;
    while (drained < kInboxDrainBatch) {
        void* frame = inbox_.pop();
        if (frame == nullptr) break;
        ++drained;
        if (push(frame, false)) ++promoted;
    }
    if (promoted > 0) sched_->publish_stealable(*this, promoted);
    return first;
}

void* Worker::take_local() noexcept {
    void* item = runnext_.load(std::memory_order_acquire);
    if (item != nullptr) {
        runnext_.store(nullptr, std::memory_order_relaxed);
        return item;
    }

    item = queue_.pop();
    if (item != nullptr) {
        repair_stealable();
    }
    return item;
}

void Worker::stage_fairness_item(void* item) noexcept {
    // A fairness-selected item may not suspend again. Put it in runnext so
    // synchronous I/O/timer service sees the slot as occupied, and expose
    // every local runnable it is about to bypass before committing to it.
    //
    // runnext is owner-only, so the ordinary load/store pair has the same
    // publication contract as push_next() without a locked RMW.
    void* const displaced = runnext_.load(std::memory_order_relaxed);
    runnext_.store(item, std::memory_order_release);
    if (displaced != nullptr) {
        // Publishing this push exposes both the displaced handoff and any FIFO
        // items that were already private.
        push(displaced, true);
    } else if (queue_.maybe_nonempty()) {
        sched_->publish_stealable(*this);
    }
}

void* Worker::service_fairness() noexcept {
    // Service each owner-local concern before selecting the next task. A
    // permanently non-empty inbox must not hide reactor service or timers,
    // and vice versa.
    void* const inbox_item = drain_inbox();
    if (inbox_item != nullptr) {
        // Reserve runnext for the already-selected inbox task before polling.
        // Otherwise the first I/O/timer completion below could occupy this
        // private slot, after which a non-suspending inbox task would hide the
        // completion from every idle thief indefinitely. Any handoff that was
        // already in runnext must likewise become stealable before we commit
        // to running unrelated inbox work.
        stage_fairness_item(inbox_item);
    }

    // If no inbox item reserved runnext, remember whether a private FIFO
    // continuation is at risk of being bypassed by the first completion below.
    // Do not publish an ordinary singleton yield merely because the periodic
    // service checkpoint ran.
    const bool protect_private_fifo =
        inbox_item == nullptr &&
        runnext_.load(std::memory_order_relaxed) == nullptr &&
        queue_.maybe_nonempty();

    // This is a busy-worker poll: local FIFO/runnext work may already exist.
    // If inbox work was selected above, runnext is reserved and every new
    // completion enters the published FIFO. Otherwise at most one newly-ready
    // continuation gets runnext priority; the rest of the batch enters FIFO.
    // The fixed owner checkpoint preserves prompt I/O service; the monitor
    // ticket only keeps the stale-reactor fallback owner-first.
    if (reactor_->registered() > 0) {
        // A failed claim means the foreign fallback already owns the shard.
        // The ticket is still acknowledged: there is no retry state.
        (void)reactor_->poll(0);
    }

    const std::int64_t timer_deadline =
        sched_->timers_->next_deadline_ns(index_);
    if (timer_deadline != INT64_MAX && timer_deadline <= now_ns()) {
        sched_->timers_->run_expired(index_);
    }

    if (protect_private_fifo &&
        runnext_.load(std::memory_order_acquire) != nullptr) {
        sched_->publish_stealable(*this);
    }
    return take_local();
}

void* Worker::service_global_fairness() noexcept {
    for (std::uint32_t offset = 0; offset < 2; ++offset) {
        const std::uint32_t source = (fair_cursor_ + offset) % 2;
        void* item = nullptr;
        if (source == 0) {
            item = sched_->global_.pop();
        } else {
            item = queue_.pop();
            if (item != nullptr) {
                repair_stealable();
            }
        }
        if (item != nullptr) {
            fair_cursor_ = (source + 1) % 2;
            // This checkpoint deliberately selected work ahead of runnext or
            // the rest of the FIFO. Publish everything it bypasses before a
            // possibly non-suspending selected task takes the worker.
            stage_fairness_item(item);
            return take_local();
        }
    }
    fair_cursor_ = (fair_cursor_ + 1) % 2;
    return nullptr;
}

void* Worker::next_local() noexcept {
    const std::uint32_t tick = ++tick_;

    if (tick % kLocalServiceInterval == 0) {
        // Fold the ticket load into the fairness checkpoint that already
        // exists, avoiding another shared load on every task selection.
        (void)reactor_->take_owner_poll_request_ns();
        if (void* item = service_fairness()) return item;
    }

    // A hot runnext pair must not starve exceptional overflow or the ordinary
    // FIFO. Inbox/I/O/timer service has its own independent budget above.
    if (tick % kGlobalQueueInterval == 0) {
        if (void* item = service_global_fairness()) return item;
    }

    return take_local();
}

void* Worker::steal_from_peers() noexcept {
    const auto worker_count =
        static_cast<std::uint32_t>(sched_->workers_.size());
    if (worker_count <= 1) return nullptr;

    WorkerId start = rand_up_to(worker_count);
    void* batch[kLocalQueueCapacity / 2];
    const auto find_other = [this](const AtomicWorkerBitmap& bitmap,
                                   WorkerId from) noexcept -> WorkerId {
        WorkerId candidate = bitmap.find_from(from);
        if (candidate != index_) return candidate;
        candidate = bitmap.find_from(index_ + 1);
        return candidate == index_ ? kInvalidWorkerId : candidate;
    };

    // Search only queues that owners have published as stealable.
    for (std::uint32_t attempt = 0; attempt < worker_count; ++attempt) {
        const WorkerId victim_id =
            find_other(*sched_->stealable_workers_, start);
        if (victim_id == kInvalidWorkerId) return nullptr;
        start = victim_id + 1;

        Worker& victim = *sched_->workers_[victim_id];
        CIO_METRIC(steal_attempts, 1);
        const std::uint32_t got =
            victim.queue_.grab(batch, kLocalQueueCapacity / 2);
        // We are a thief, not `victim`'s owner, so bypass its owner-only
        // never-published fast-path flag.
        const bool victim_still_stealable = sched_->repair_stealable(victim);
        if (got == 0) continue;

        CIO_METRIC(steal_hits, 1);
        std::uint32_t retained = 0;
        for (std::uint32_t i = 1; i < got; ++i) {
            if (push(batch[i], false)) ++retained;
        }
        if (retained > 0) sched_->publish_stealable(*this, retained);
        // A retained batch and the original victim tail are two independent
        // published sources. Publishing the retained items accounts only for
        // this worker's FIFO; if the original victim remains non-empty, give
        // it a separate searcher before non-suspending stolen work can occupy
        // every currently active worker.
        if (victim_still_stealable) {
            sched_->wake_one_searcher(victim_id + 1);
        }
        return batch[0];
    }
    return nullptr;
}

void* Worker::consume_searcher_credit() noexcept {
    // Most park departures carry only a generic wake. Keep that case to one
    // read-only load and reserve the locked RMW for an armed credit.
    if (!searcher_credit_.load(std::memory_order_acquire) ||
        !searcher_credit_.exchange(false, std::memory_order_acquire)) {
        return nullptr;
    }

    if (void* item = steal_from_peers()) return item;

    // The credited victim may have been drained or repaired while we searched.
    // If another published victim remains, pass the obligation to a concrete
    // idle worker before unrelated work can occupy this one indefinitely.
    if (sched_->stealable_workers_->any_seq_cst()) {
        sched_->wake_one_searcher(index_ + 1);
    }
    return nullptr;
}

void* Worker::find_work() noexcept {
    if (void* item = drain_inbox()) return item;

    {
        void* batch[kLocalQueueCapacity / 2];
        const std::uint32_t n = sched_->global_.pop_batch(
            batch, kLocalQueueCapacity / 2,
            static_cast<std::uint32_t>(sched_->workers_.size()));
        if (n > 0) {
            CIO_METRIC(global_batch_pops, 1);
            CIO_METRIC(global_batch_items, n);
            std::uint32_t retained = 0;
            for (std::uint32_t i = 1; i < n; ++i) {
                if (push(batch[i], false)) ++retained;
            }
            if (retained > 0) sched_->publish_stealable(*this, retained);
            return batch[0];
        }
    }

    // A shard owner must give its own readiness priority over migration. With
    // a shared reactor an empty-poll backoff could be covered by another
    // poller; with sharded reactors it lets a worker steal while its own fd is
    // ready, pushing that connection onto the remote-inbox path thereafter.
    // Therefore every actual steal search is preceded by one local poll
    // attempt. A concurrent monitor poll returns -1 and already provides the
    // equivalent readiness service.
    if (reactor_->registered() > 0 && sched_->stealable_workers_->any()) {
        (void)reactor_->poll(0);
        if (void* item = take_local()) return item;
        if (void* item = drain_inbox()) return item;
    }

    if (sched_->timers_->next_deadline_ns(index_) <= now_ns() &&
        sched_->timers_->run_expired(index_) > 0) {
        if (void* item = take_local()) return item;
        if (void* item = drain_inbox()) return item;
    }

    return steal_from_peers();
}

void Worker::run() {
    t_worker = this;
    rng_ = 0x9E3779B97F4A7C15ull ^
           (static_cast<std::uint64_t>(index_ + 1) * 0xBF58476D1CE4E5B9ull);

    bool returned_from_park = false;
    while (!sched_->stopping()) {
        // A wake sent for published FIFO work is a search entitlement, not a
        // generic eventfd token. Credits are only granted to a worker whose
        // idle publication is being claimed (or whose final park departure
        // adopts that claim), so inspect the atomic exactly once after park
        // returns—not after every ordinary coroutine resumption.
        void* item = nullptr;
        if (returned_from_park) {
            returned_from_park = false;
            item = consume_searcher_credit();
        }
        if (item == nullptr) item = next_local();
        if (item == nullptr) item = find_work();

        if (item != nullptr) {
            CIO_METRIC(tasks_run, 1);
            // A top-level scheduler return is already a fairness boundary.
            // The extra count lets one full quota of terminal leaf completions
            // remain in one symmetric-transfer chain; the following completion
            // performs the cold demand check.
            t_cooperative_io_budget = kCooperativeIoBudget + 1;
            cooperative_io_local_grace_ = false;
            std::coroutine_handle<>::from_address(item).resume();
            continue;
        }
        sched_->park(*this);
        returned_from_park = true;
    }

    sched_->idle_workers_->clear(index_);
    t_cooperative_io_budget = 0;
    t_worker = nullptr;
}

// ------------------------------------------------------------- Scheduler ---

Scheduler::Scheduler(std::size_t worker_count, std::size_t max_blocking_threads,
                     std::size_t max_blocking_queue)
    : Scheduler(worker_count, BlockingLimits{max_blocking_threads,
                                             max_blocking_queue, 0, 0}) {}

Scheduler::Scheduler(std::size_t worker_count, BlockingLimits blocking_limits) {
    if (worker_count == 0) {
        worker_count =
            std::max<std::size_t>(1, std::thread::hardware_concurrency());
    }
    if (worker_count >
        static_cast<std::size_t>(std::numeric_limits<WorkerId>::max() - 1)) {
        throw std::length_error("cio: too many runtime workers");
    }

    workers_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        auto worker = std::make_unique<Worker>();
        worker->sched_ = this;
        worker->index_ = static_cast<WorkerId>(i);
        workers_.push_back(std::move(worker));
    }

    idle_workers_ = std::make_unique<AtomicWorkerBitmap>(worker_count);
    stealable_workers_ = std::make_unique<AtomicWorkerBitmap>(worker_count);

    timers_ = std::make_unique<TimerService>(*this, worker_count);
    blocking_ = std::make_unique<BlockingPool>(blocking_limits);
    for (auto& worker : workers_) {
        worker->reactor_ = std::make_unique<Reactor>(*this, worker->index_);
    }
    completion_target_ = acquire_completion_target(this);
    for (auto& worker : workers_) {
        worker->completion_endpoint_ = completion_target_.endpoint;
    }
}

Scheduler::~Scheduler() {
    shutdown();
}

void Scheduler::start() {
    if (started_.exchange(true, std::memory_order_acq_rel)) return;
    for (auto& worker : workers_) {
        worker->thread_ = std::thread([w = worker.get()] { w->run(); });
    }
    monitor_ = std::thread([this] { monitor_main(); });
}

void Scheduler::shutdown() {
    if (t_worker != nullptr && t_worker->sched_ == this) {
        throw std::logic_error(
            "cio: Runtime::shutdown() cannot run on its own worker");
    }
    if (stop_.exchange(true, std::memory_order_acq_rel)) return;

    // Close completion admission before any owned storage can disappear.
    // Foreign wakers that already hold a lease finish their short scheduling
    // call; later or stale tokens fail without dereferencing this Scheduler.
    retire_completion_target(completion_target_);

    // Wake every shard regardless of its idle bit. A monitor may own one poll,
    // and a worker may be entering epoll after the stop publication.
    for (auto& worker : workers_) worker->reactor_->wake();

    for (auto& worker : workers_) {
        if (worker->thread_.joinable()) worker->thread_.join();
    }
    if (monitor_.joinable()) monitor_.join();

    blocking_->shutdown();
    timers_->drain_all();

    if (g_default_scheduler.load(std::memory_order_acquire) == this) {
        g_default_scheduler.store(nullptr, std::memory_order_release);
    }
}

WorkerId Scheduler::current_worker_id() const noexcept {
    return detail::current_worker_id(this);
}

WorkerId Scheduler::choose_worker() noexcept {
    const auto count = static_cast<WorkerId>(workers_.size());
    if (count <= 1) return 0;
    return placement_next_.fetch_add(1, std::memory_order_relaxed) % count;
}

Reactor& Scheduler::reactor() noexcept {
    WorkerId worker = current_worker_id();
    if (!valid_worker_id(worker)) worker = choose_worker();
    return reactor_for(worker);
}

Reactor& Scheduler::reactor_for(WorkerId worker) noexcept {
    if (!valid_worker_id(worker)) worker = 0;
    return *workers_[worker]->reactor_;
}

void Scheduler::wake_worker(WorkerId worker) noexcept {
    if (!valid_worker_id(worker)) return;
    if (idle_workers_->clear(worker)) {
        workers_[worker]->reactor_->wake();
    }
}

bool Scheduler::wake_one_idle(WorkerId start) noexcept {
    if (workers_.empty()) return false;
    start %= static_cast<WorkerId>(workers_.size());
    const WorkerId worker = idle_workers_->claim_from(start);
    if (worker == kInvalidWorkerId) return false;
    workers_[worker]->reactor_->wake();
    return true;
}

bool Scheduler::wake_one_searcher(WorkerId start) noexcept {
    if (workers_.empty()) return false;
    const WorkerId count = static_cast<WorkerId>(workers_.size());
    start %= count;

    // A victim-publication wake is an entitlement to search, not a generic
    // eventfd token. Arm the concrete candidate before attempting to remove
    // its idle publication. If that exact claim loses a race, leave the credit
    // sticky: the worker either consumes it on this departure from park or
    // after a later park departure, and at worst performs one harmless extra
    // search.
    for (WorkerId tries = 0; tries < count; ++tries) {
        const WorkerId worker = idle_workers_->find_from_seq_cst(start);
        if (worker == kInvalidWorkerId) return false;

        workers_[worker]->searcher_credit_.store(true,
                                                 std::memory_order_release);
        if (idle_workers_->clear(worker)) {
            workers_[worker]->reactor_->wake();
            return true;
        }
        start = static_cast<WorkerId>((worker + 1) % count);
    }
    return false;
}

void Scheduler::publish_stealable(Worker& worker,
                                  std::uint32_t wake_count) noexcept {
    // With no peer there is nobody that can consume this hint. The owner
    // checks its queue directly before parking, so publication is unnecessary.
    if (workers_.size() <= 1) return;

    auto& publication = worker.stealable_publication_;
    const bool known_published = worker.has_published_stealable_;
    worker.has_published_stealable_ = true;

    // The queue tail was release-published before this call. Keep the
    // publication handshake on a per-worker cache line. The first publication
    // and the first publication after any successful clear touch the shared
    // bitmap; the common burst path observes an unchanged clear epoch and
    // avoids even loading that contended cache line.
    publication.publish_epoch.store(++publication.next_publish_epoch,
                                    std::memory_order_seq_cst);
    const std::uint64_t clear_epoch =
        publication.clear_epoch.load(std::memory_order_seq_cst);

    bool became_stealable = false;
    if (!known_published || clear_epoch != publication.seen_clear_epoch) {
        publication.seen_clear_epoch = clear_epoch;
        became_stealable = stealable_workers_->set(worker.index_);
    }

    // A previous clear->set transition already sent a searcher toward this
    // queue. Repeating the idle-bitmap scan for every item in a burst adds
    // shared-cache traffic without increasing useful parallelism. Explicit
    // batches may still request additional workers for their exposed tail.
    if (!became_stealable && wake_count <= 1) return;

    for (std::uint32_t i = 0; i < wake_count; ++i) {
        const WorkerId start =
            wake_cursor_.fetch_add(1, std::memory_order_relaxed);
        if (!wake_one_searcher(start)) break;
    }
}

bool Scheduler::repair_stealable(Worker& worker) noexcept {
    // A clear bit means either that this queue was deliberately kept private
    // (yield/direct hand-off), or that another clearer currently owns the
    // epoch/recheck protocol.  In both cases that thread is responsible for
    // any later re-publication.  Testing the bit first keeps private FIFO work
    // from paying three queue-counter loads after every pop.
    if (!stealable_workers_->test(worker.index_)) return false;

    if (worker.queue_.maybe_nonempty()) {
        return true;
    }

    // Multiple thieves may observe the old bit, but only the thread whose RMW
    // changed it from set to clear owns the epoch handshake and final recheck.
    if (!stealable_workers_->clear(worker.index_)) return false;

    auto& publication = worker.stealable_publication_;
    publication.clear_epoch.fetch_add(1, std::memory_order_seq_cst);

    // A producer that still observed the old clear epoch may have skipped its
    // bitmap RMW. In the SC order its publish store precedes that observation,
    // which precedes our clear-epoch increment and this load. Reading that
    // store (or a later one) makes its earlier queue-tail release
    // happen-before the final recheck. Conversely, a producer that observes
    // this increment restores the bit itself. A complete 2^64-clear wrap
    // between two owner observations is outside the executable lifetime of a
    // process; a single numeric wrap is still detected normally.
    (void)publication.publish_epoch.load(std::memory_order_seq_cst);

    if (worker.queue_.maybe_nonempty()) {
        stealable_workers_->set(worker.index_);
        wake_one_searcher(worker.index_ + 1);
        return true;
    }
    return false;
}

void Scheduler::poller_returned(WorkerId shard) noexcept {
    Worker* const worker = t_worker;
    if (worker != nullptr && worker->sched_ == this &&
        worker->index_ == shard) {
        // A worker parked in its shard published idle before epoll_wait. Once
        // the syscall returns it is active again, even though event dispatch is
        // still in progress. Clearing here prevents local readiness batches
        // from claiming/waking their own poller (and prevents simultaneous
        // shard batches from waking workers the kernel already woke).
        idle_workers_->clear(shard);
    }
}

void Scheduler::enqueue_remote(WorkerId target, void* frame,
                               bool wake) noexcept {
    if (!valid_worker_id(target)) target = choose_worker();
    Worker& worker = *workers_[target];
    if (CIO_LIKELY(worker.inbox_.try_push(frame))) {
        if (wake) wake_worker(target);
        return;
    }

    // Full inbox is the exceptional MPMC path. The frame is never dropped and
    // any idle worker may drain it.
    global_.push(frame);
    wake_one_idle(target + 1);
}

void Scheduler::schedule_frame(void* frame) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(frame);
        return;
    }
    // An ordinary foreign submission has no ownership target. Publishing it
    // to an arbitrarily chosen owner-only inbox can strand it behind a
    // non-suspending task while another worker is idle.
    schedule_completion_fallback(frame, kInvalidWorkerId);
}

void Scheduler::schedule_spawn_frame(void* frame,
                                     std::uint8_t* spawn_mode) noexcept {
    *spawn_mode = 1;
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(frame, true, spawn_mode);
        return;
    }
    schedule_completion_fallback(frame, kInvalidWorkerId);
}

void Scheduler::schedule_to_frame(void* frame, WorkerId target) noexcept {
    if (!valid_worker_id(target)) {
        schedule_frame(frame);
        return;
    }

    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        if (target == worker->index_) {
            worker->push(frame);
        } else {
            enqueue_remote(target, frame, true);
        }
        return;
    }
    enqueue_remote(target, frame, true);
}

Scheduler::IoCompletionRoute Scheduler::schedule_io_completion(
    std::coroutine_handle<> handle, WorkerId preferred_worker) noexcept {
    Worker* worker = t_worker;
    if (CIO_LIKELY(worker != nullptr && worker->sched_ == this)) {
        // A worker poller is guaranteed to return to this scheduler loop, so
        // it may keep the completion even when the old preferred worker is
        // different. Worker affinity is a hint, and returning a migrated
        // connection to its active home-shard poller is cheaper than a remote
        // wake while also avoiding a busy preferred worker.
        //
        // A bounded fairness poll may run while FIFO work exists. It gives at
        // most one completion runnext priority; the remainder enters FIFO and
        // is published once in finish_io_batch().
        if (worker->runnext_.load(std::memory_order_relaxed) == nullptr) {
            worker->runnext_.store(handle.address(), std::memory_order_release);
            return IoCompletionRoute::kRunnext;
        }
        return worker->push(handle.address(), false)
                   ? IoCompletionRoute::kLocalFifo
                   : IoCompletionRoute::kSharedFallback;
    }

    schedule_completion_fallback(handle.address(), preferred_worker);
    return IoCompletionRoute::kSharedFallback;
}

void Scheduler::schedule_completion_fallback(
    void* frame, WorkerId preferred_worker) noexcept {
    if (!valid_worker_id(preferred_worker)) {
        preferred_worker = choose_worker();
    }
    // A monitor, pool thread or foreign-runtime waker will not return through
    // this scheduler's local queue. Publishing globally lets any idle worker
    // take the completion; targeting an owner-only inbox could otherwise
    // strand it indefinitely behind one non-suspending CPU coroutine.
    global_.push(frame);
    wake_one_idle(preferred_worker);
}

void Scheduler::schedule_completion_wake(std::coroutine_handle<> handle,
                                         WorkerId preferred_worker) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this &&
        (!valid_worker_id(preferred_worker) ||
         worker->index_ == preferred_worker)) {
        // Preserve the established local wake behaviour. Unlike a poll batch,
        // an arbitrary waker may keep work private only when it is already the
        // preferred worker; otherwise another idle worker must be allowed to
        // make progress.
        worker->push(handle.address());
        return;
    }

    schedule_completion_fallback(handle.address(), preferred_worker);
}

void Scheduler::finish_io_batch(std::uint32_t unpublished_local_fifo) noexcept {
    Worker* worker = t_worker;
    if (worker == nullptr || worker->sched_ != this ||
        unpublished_local_fifo == 0) {
        return;
    }
    if (worker->queue_.maybe_nonempty()) {
        publish_stealable(*worker, unpublished_local_fifo);
    }
}

void Scheduler::reschedule_self(std::coroutine_handle<> handle) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(handle.address(), false);
        return;
    }
    schedule_frame(handle.address());
}

std::uint8_t cooperative_io_return_debt_slow() noexcept {
    t_cooperative_io_budget = 0;
    Scheduler* const scheduler = current_scheduler();
    if (scheduler == nullptr) {
        return kCooperativeIoDebtNone;
    }

    const std::uint8_t debt = scheduler->prepare_cooperative_io_checkpoint();
    if (debt == kCooperativeIoDebtNone) {
        t_cooperative_io_budget = kCooperativeIoBudget + 1;
    }
    return debt;
}

std::uint8_t Scheduler::prepare_cooperative_io_checkpoint() noexcept {
    Worker* const worker = t_worker;
    if (worker == nullptr || worker->sched_ != this) {
        return kCooperativeIoDebtNone;
    }

    CIO_METRIC(cooperative_io_exhaustions, 1);

    const auto runnable_debt = [this, worker]() noexcept {
        std::uint8_t debt = kCooperativeIoDebtNone;
        if (worker->runnext_.load(std::memory_order_acquire) != nullptr ||
            worker->queue_.maybe_nonempty()) {
            debt |= kCooperativeIoDebtLocal;
        }
        if (!worker->inbox_.empty()) {
            debt |= kCooperativeIoDebtInbox;
        }
        if (!global_.empty()) {
            debt |= kCooperativeIoDebtGlobal;
        }
        return debt;
    };

    const auto record_yield = [worker](std::uint8_t debt) noexcept {
        worker->cooperative_io_local_grace_ = false;
        CIO_METRIC(cooperative_io_forced_yields, 1);
        const std::uint8_t runnable =
            debt & static_cast<std::uint8_t>(kCooperativeIoDebtLocal |
                                             kCooperativeIoDebtInbox |
                                             kCooperativeIoDebtGlobal);
        if (runnable == kCooperativeIoDebtLocal) {
            CIO_METRIC(cooperative_io_yield_local_only, 1);
        }
        if ((runnable & kCooperativeIoDebtInbox) != 0) {
            CIO_METRIC(cooperative_io_yield_inbox, 1);
        }
        if ((runnable & kCooperativeIoDebtGlobal) != 0) {
            CIO_METRIC(cooperative_io_yield_global, 1);
        }
    };

    if (stopping()) {
        record_yield(kCooperativeIoDebtStop);
        return kCooperativeIoDebtStop;
    }

    // Existing runnable work is already sufficient reason to yield. Avoid an
    // opportunistic kernel call in that case; the normal owner checkpoint
    // retains the ticket and will service it.
    if (std::uint8_t debt = runnable_debt(); debt != kCooperativeIoDebtNone) {
        if (debt == kCooperativeIoDebtLocal &&
            !worker->cooperative_io_local_grace_) {
            worker->cooperative_io_local_grace_ = true;
            CIO_METRIC(cooperative_io_deferred_local_only, 1);
            return kCooperativeIoDebtNone;
        }
        record_yield(debt);
        return debt;
    }

    Reactor& reactor = *worker->reactor_;
    if (reactor.take_owner_poll_request_ns() != 0) {
        if (reactor.registered() > 0) {
            CIO_METRIC(cooperative_io_ticket_polls, 1);
            (void)reactor.poll(0);
            if (std::uint8_t debt = runnable_debt();
                debt != kCooperativeIoDebtNone) {
                CIO_METRIC(cooperative_io_ticket_polls_productive, 1);
                record_yield(debt);
                return debt;
            }
            CIO_METRIC(cooperative_io_ticket_polls_empty, 1);
        }
    }

    // Avoid a clock read when this shard has no timer at all. An expired
    // callback may complete inline without making a task runnable, so decide
    // whether to yield from the queues after firing, not from the deadline.
    const std::int64_t timer_deadline =
        timers_->next_deadline_ns(worker->index_);
    if (timer_deadline != INT64_MAX) {
        CIO_METRIC(cooperative_io_timer_checks, 1);
        if (timer_deadline <= now_ns()) {
            timers_->run_expired(worker->index_);
            if (std::uint8_t debt = runnable_debt();
                debt != kCooperativeIoDebtNone) {
                CIO_METRIC(cooperative_io_timer_productive, 1);
                record_yield(debt);
                return debt;
            }
        }
    }

    if (stopping()) {
        record_yield(kCooperativeIoDebtStop);
        return kCooperativeIoDebtStop;
    }

    worker->cooperative_io_local_grace_ = false;
    CIO_METRIC(cooperative_io_renew_no_demand, 1);
    return kCooperativeIoDebtNone;
}

void Scheduler::reschedule_self_for_cooperative_io(
    std::coroutine_handle<> handle, std::uint8_t debt) noexcept {
    Worker* const worker = t_worker;
    if (worker == nullptr || worker->sched_ != this) {
        schedule_frame(handle.address());
        return;
    }

    // Convert one shared item, then one owner-directed item, into concrete
    // local ordering before publishing the current continuation. Staging the
    // inbox item last gives owner-only work runnext priority; any displaced
    // global/local item is published for idle thieves.
    bool staged_nonlocal = false;
    if ((debt & kCooperativeIoDebtGlobal) != 0) {
        if (void* item = global_.pop(); item != nullptr) {
            worker->fair_cursor_ = 1;
            worker->stage_fairness_item(item);
            staged_nonlocal = true;
        }
    }
    if ((debt & kCooperativeIoDebtInbox) != 0) {
        if (void* item = worker->drain_inbox(); item != nullptr) {
            worker->stage_fairness_item(item);
            staged_nonlocal = true;
        }
    }

    // With spare capacity, expose a local-only parent as well: the selected
    // local task may never suspend, and an idle peer can then steal either the
    // parent or enough older debt for the owner to reach it. Under saturation,
    // keep ordinary local yields private to preserve connection locality.
    const bool publish_parent =
        staged_nonlocal ||
        (((debt & kCooperativeIoDebtLocal) != 0) && idle_workers_->any());

    // Final action: publication can let a peer resume and destroy this
    // coroutine before await_suspend returns.
    worker->push(handle.address(), publish_parent);
}

std::coroutine_handle<> defer_cooperative_io_continuation(
    std::coroutine_handle<> continuation, std::uint8_t debt) noexcept {
    Worker* const worker = current_worker();
    if (worker == nullptr) {
        return continuation;
    }

    // Final action: publishing the parent may let it destroy the completed
    // child frame whose FinalAwaiter called us.
    worker->scheduler().reschedule_self_for_cooperative_io(continuation, debt);
    return std::noop_coroutine();
}

void Scheduler::schedule_next_frame(void* frame) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push_next(frame);
        return;
    }
    // Direct handoff is meaningful only inside this scheduler's active worker.
    // A foreign or cross-runtime waker must not turn it into a hard-targeted
    // owner-only inbox submission: the arbitrarily chosen owner may be stuck
    // in a non-suspending task while another worker is idle.
    schedule_completion_fallback(frame, kInvalidWorkerId);
}

void Scheduler::schedule_batch(void* const* frames, std::uint32_t n) noexcept {
    if (n == 0) return;
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        std::uint32_t retained = 0;
        for (std::uint32_t i = 0; i < n; ++i) {
            if (worker->push(frames[i], false)) ++retained;
        }
        // Generic batches do not reserve a runnext item for the current owner.
        // Even a singleton FIFO item must therefore be visible to an idle
        // peer before the caller can enter a non-suspending task.
        if (retained > 0) publish_stealable(*worker, retained);
        return;
    }

    schedule_batch_to(frames, n, kInvalidWorkerId);
}

void Scheduler::schedule_batch_to(void* const* frames, std::uint32_t n,
                                  WorkerId target) noexcept {
    if (n == 0) return;
    if (!valid_worker_id(target)) target = choose_worker();

    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this &&
        worker->index_ == target) {
        std::uint32_t first = 0;
        if (worker->runnext_.load(std::memory_order_relaxed) == nullptr) {
            worker->runnext_.store(frames[0], std::memory_order_release);
            first = 1;
        }

        std::uint32_t retained = 0;
        for (std::uint32_t i = first; i < n; ++i) {
            if (worker->push(frames[i], false)) ++retained;
        }
        if (retained > 0) publish_stealable(*worker, retained);
        return;
    }

    // `target` is affinity, not ownership. A monitor or a different shard
    // firing this timer batch will not return through the target worker's
    // loop. Putting the batch in its owner-only inbox could therefore strand
    // every completion behind one non-suspending task while peers are idle.
    global_.push_batch(frames, n);
    const std::uint32_t wake_count =
        std::min<std::uint32_t>(n, static_cast<std::uint32_t>(workers_.size()));
    if (wake_count > 0 && wake_one_idle(target)) {
        notify_batch(wake_count - 1);
    }
}

void Scheduler::schedule_deferred(std::coroutine_handle<> handle) noexcept {
    // Retained as a compatibility facade for detail callers. Directed eventfd
    // wake coalescing makes an immediate targeted schedule cheap and avoids a
    // hidden "notify the right owner later" obligation.
    schedule_frame(handle.address());
}

void Scheduler::notify() noexcept {
    const WorkerId start = wake_cursor_.fetch_add(1, std::memory_order_relaxed);
    wake_one_idle(start);
}

void Scheduler::notify_batch(std::uint32_t count) noexcept {
    for (std::uint32_t i = 0; i < count; ++i) {
        const WorkerId start =
            wake_cursor_.fetch_add(1, std::memory_order_relaxed);
        // Once a complete bitmap scan finds no idle worker, further scans in
        // this batch can only add shared-line traffic. A worker entering park
        // observes the already-published global batch in its final recheck.
        if (!wake_one_idle(start)) break;
    }
}

void Scheduler::nudge_poller(WorkerId worker,
                             std::int64_t deadline_ns) noexcept {
    if (!valid_worker_id(worker)) worker = 0;
    Worker* const caller = current_worker();
    Reactor& reactor = *workers_[worker]->reactor_;

    // A timer armed by its shard owner is sequenced before that same worker's
    // next timeout snapshot, so the ordinary deadline-aware nudge is enough.
    if (caller != nullptr && caller->sched_ == this &&
        caller->index_ == worker) {
        reactor.nudge(deadline_ns);
        return;
    }

    // Foreign and cross-runtime callers can arm after the owner has published
    // idle and taken its final timeout snapshot, but before Reactor::poll()
    // publishes polling=true. Claiming the idle bit closes that window:
    // either this writes a level-triggered token, or the owner's later idle
    // publication/final acquire check observes the already-published timer.
    if (idle_workers_->clear(worker)) {
        reactor.wake();
        return;
    }
    reactor.nudge(deadline_ns);
}

bool Scheduler::worker_has_work(Worker& worker) noexcept {
    if (worker.runnext_.load(std::memory_order_acquire) != nullptr) return true;
    if (worker.queue_.maybe_nonempty()) return true;
    if (!worker.inbox_.empty()) return true;
    if (!global_.empty()) return true;
    if (timers_->next_deadline_ns(worker.index_) <= now_ns()) return true;

    // The owner's queue bits are repaired before this check. Any remaining bit
    // is a published victim worth another pass through steal_from_peers().
    return stealable_workers_->any_seq_cst();
}

void Scheduler::leave_park(Worker& worker) noexcept {
    idle_workers_->clear(worker.index_);
    // Clear first, then participate in the same SC publication order as
    // victim producers. If a producer lost the idle claim because this
    // worker was already leaving, the worker adopts the search obligation
    // itself before any local/inbox/global task can run.
    if (stealable_workers_->any_seq_cst()) {
        worker.searcher_credit_.store(true, std::memory_order_release);
    }
}

void Scheduler::park(Worker& worker) {
    CIO_METRIC(parks, 1);

    worker.has_published_stealable_ = repair_stealable(worker);
    idle_workers_->set(worker.index_);

    // Publish idle before this acquire recheck. A producer either claims and
    // wakes our bit, or published before it and is observed here.
    if (stopping() || worker_has_work(worker)) {
        leave_park(worker);
        return;
    }

    const std::int64_t timeout_ns = timers_->next_timeout_ns(worker.index_);
    worker.reactor_->poll(timeout_ns);
    leave_park(worker);
    timers_->run_expired(worker.index_);
}

void Scheduler::monitor_pass(std::int64_t now) noexcept {
    for (auto& worker : workers_) {
        Reactor& reactor = *worker->reactor_;
        if (!reactor.polling() && reactor.registered() > 0 &&
            now - reactor.last_poll_ns() > kMonitorPollStaleNs) {
            // Give the shard owner one bounded checkpoint before using the
            // worker-context driver. With one worker there is no independent
            // executor for that control item, so retain the direct fallback.
            if (!reactor.request_owner_poll_at(now)) {
                if (workers_.size() == 1) {
                    (void)reactor.poll(0);
                } else {
                    (void)reactor.request_driver_at(now);
                    (void)reactor.queue_driver();

                    std::uint64_t requested = 0;
                    std::int64_t requested_at = 0;
                    for (;;) {
                        requested = reactor.driver_requested_epoch_.load(
                            std::memory_order_acquire);
                        requested_at = reactor.driver_requested_at_ns_.load(
                            std::memory_order_relaxed);
                        if (reactor.driver_requested_epoch_.load(
                                std::memory_order_acquire) == requested) {
                            break;
                        }
                    }

                    if (reactor.driver_covered_epoch_.load(
                            std::memory_order_acquire) < requested &&
                        now - requested_at >= kMonitorDriverGraceNs) {
                        const int result = reactor.poll(0);
                        if (result >= 0) {
                            reactor.cover_driver_epoch(requested);
                        } else {
                            reactor.observe_driver_coverage();
                        }
                    }
                }
            }
        }

        // Timer firing is serialized by the shard heap and is safe even while
        // the owner is blocked in its reactor. Checking due timers
        // unconditionally is a defensive backstop for any stale poll timeout
        // or missed kernel wake.
        if (timers_->next_deadline_ns(worker->index_) <= now) {
            timers_->run_expired(worker->index_);
        }
    }
}

bool Scheduler::should_use_batch_monitor_policy(int inherited_policy) noexcept {
    return inherited_policy == SCHED_OTHER;
}

void Scheduler::monitor_main() {
    (void)::pthread_setname_np(::pthread_self(), "cio-monitor");

    // The watchdog must remain runnable when every worker executes
    // non-suspending user code, so SCHED_IDLE is too weak. SCHED_BATCH remains
    // in the normal fair class while reducing this short-sleeping background
    // thread's wakeup-preemption advantage. Preserve any policy explicitly
    // configured by the creator and inherited by std::thread.
    int inherited_policy = SCHED_OTHER;
    sched_param inherited_param{};
    if (::pthread_getschedparam(::pthread_self(), &inherited_policy,
                                &inherited_param) == 0 &&
        should_use_batch_monitor_policy(inherited_policy)) {
#if defined(SCHED_BATCH)
        sched_param batch_param{};
        (void)::pthread_setschedparam(::pthread_self(), SCHED_BATCH,
                                      &batch_param);
#endif
    }

    std::int64_t delay_us = 50;

    while (!stopping()) {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
        if (stopping()) break;

        monitor_pass(now_ns());

        if (idle_workers_->all()) {
            delay_us = std::min<std::int64_t>(delay_us * 2, 10'000);
        } else {
            delay_us = 50;
        }
    }
}

}  // namespace cio::detail
