#include "cio/detail/scheduler.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "cio/clock.hpp"
#include "cio/detail/metrics.hpp"

namespace cio::detail {
namespace {

thread_local Worker* t_worker = nullptr;
std::atomic<Scheduler*> g_default_scheduler{nullptr};

// How stale the last reactor poll may get before the monitor thread steps in.
constexpr std::int64_t kMonitorPollStaleNs = 200'000;  // 200us

}  // namespace

Worker* current_worker() noexcept { return t_worker; }

Scheduler* current_scheduler() noexcept {
    if (t_worker != nullptr) return &t_worker->scheduler();
    return g_default_scheduler.load(std::memory_order_acquire);
}

void set_default_scheduler(Scheduler* sched) noexcept {
    g_default_scheduler.store(sched, std::memory_order_release);
}

// ---------------------------------------------------------------- Worker ---

std::uint32_t Worker::rand_up_to(std::uint32_t n) noexcept {
    // xorshift64*: cheap, and the quality only has to be good enough to keep
    // thieves from converging on the same victim.
    rng_ ^= rng_ << 13;
    rng_ ^= rng_ >> 7;
    rng_ ^= rng_ << 17;
    return n == 0 ? 0 : static_cast<std::uint32_t>((rng_ >> 33) % n);
}

void Worker::push(void* item) noexcept {
    if (CIO_LIKELY(queue_.push(item))) return;
    CIO_METRIC(local_overflow, 1);
    // Ring full: spill half of it plus the new item to the global queue in one
    // lock acquisition, so a burst of spawns degrades gracefully instead of
    // hammering the mutex once per task.
    void* batch[kLocalQueueCapacity / 2 + 1];
    const std::uint32_t n = queue_.pop_half(batch);
    batch[n] = item;
    sched_->global_.push_batch(batch, n + 1);
}

void Worker::push_next(void* item) noexcept {
    void* previous = runnext_.exchange(item, std::memory_order_acq_rel);
    if (previous != nullptr) push(previous);
}

void* Worker::next_local() noexcept {
    // Fairness valve: a producer/consumer pair that keeps re-arming runnext
    // would otherwise starve everything in the global queue forever.
    if (++tick_ % kGlobalQueueInterval == 0) {
        if (void* item = sched_->global_.pop()) return item;
    }
    if (void* item = runnext_.exchange(nullptr, std::memory_order_acquire)) return item;
    return queue_.pop();
}

void* Worker::steal_from_peers() noexcept {
    const auto worker_count = static_cast<std::uint32_t>(sched_->workers_.size());
    if (worker_count <= 1) return nullptr;

    const std::uint32_t start = rand_up_to(worker_count);
    void* batch[kLocalQueueCapacity / 2];

    // Random start offset so thieves do not all converge on worker 0.
    // runnext is intentionally off limits — see the comment on the member.
    for (std::uint32_t i = 0; i < worker_count; ++i) {
        Worker& victim = *sched_->workers_[(start + i) % worker_count];
        if (&victim == this) continue;

        CIO_METRIC(steal_attempts, 1);
        const std::uint32_t got = victim.queue_.grab(batch, kLocalQueueCapacity / 2);
        if (got > 0) {
            CIO_METRIC(steal_hits, 1);
            for (std::uint32_t k = 1; k < got; ++k) push(batch[k]);
            return batch[0];
        }
    }
    return nullptr;
}

void* Worker::find_work() noexcept {
    Scheduler& sched = *sched_;

    // Take a share of the global queue rather than a single item: N workers
    // draining concurrently should not each pay a lock round-trip per task.
    {
        void* batch[kLocalQueueCapacity / 2];
        const std::uint32_t n = sched.global_.pop_batch(
            batch, kLocalQueueCapacity / 2, static_cast<std::uint32_t>(sched.workers_.size()));
        if (n > 0) {
            CIO_METRIC(global_batch_pops, 1);
            CIO_METRIC(global_batch_items, n);
            for (std::uint32_t i = 1; i < n; ++i) push(batch[i]);
            return batch[0];
        }
    }

    // Non-blocking reactor drain, and it comes *before* stealing.
    //
    // In an I/O-bound server essentially every runnable task originates in the
    // reactor, so a searcher that steals first pays a full scan of every peer —
    // two cache lines each, on lines those peers are concurrently writing — to
    // discover what one epoll_wait was about to hand it in bulk. Measured on
    // the 8-thread echo workload at 256 connections: 3.33M steal attempts
    // against 701k hits, a 21% hit rate, with one epoll event per request.
    // Go's findRunnable polls the network before it resorts to stealing for
    // exactly this reason.
    //
    // The claim below is what keeps this from being a syscall per searcher: at
    // most one worker is inside the reactor at a time, so the others fall
    // straight through to the steal scan, and the one that polls is the one
    // publishing the work they are about to steal.
    //
    // Claim the reactor, do not merely test for it. polling_ says "exactly one
    // thread is inside the reactor", and a load-then-store cannot enforce that:
    // the parked poller sets the flag under idle_mutex_ and this path reads it
    // without, so a worker can slip in after the flag is set but before the
    // poller has entered epoll_wait. Two threads on one epoll fd then race for
    // the wakeup eventfd, and a non-blocking drain that swallows the token
    // meant for the blocking poller leaves it in epoll_wait forever — at
    // shutdown, that is a hung join().
    bool unclaimed = false;
    if (sched.reactor_->registered() > 0 &&
        sched.polling_.compare_exchange_strong(unclaimed, true, std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        sched.last_poll_ns_.store(now_ns(), std::memory_order_relaxed);
        CIO_METRIC(polls_nonblocking, 1);
        sched.reactor_->poll(0);
        sched.polling_.store(false, std::memory_order_release);
        if (void* item = next_local()) return item;
        if (void* item = sched.global_.pop()) return item;
    }

    if (void* stolen = steal_from_peers()) return stolen;

    if (sched.timers_->next_deadline_ns() <= now_ns() && sched.timers_->run_expired() > 0) {
        if (void* item = next_local()) return item;
        if (void* item = sched.global_.pop()) return item;
    }

    return nullptr;
}

void Worker::run() {
    t_worker = this;
    rng_ = 0x9E3779B97F4A7C15ull ^ (static_cast<std::uint64_t>(index_ + 1) * 0xBF58476D1CE4E5B9ull);

    bool spinning = false;
    while (!sched_->stopping()) {
        void* item = next_local();
        if (item == nullptr) {
            if (!spinning) {
                spinning = true;
                sched_->spinning_.fetch_add(1, std::memory_order_seq_cst);
            }
            item = find_work();
        }

        if (item != nullptr) {
            if (spinning) {
                spinning = false;
                // If we were the last searcher, wake another one: there may be
                // more work than the item we just claimed.
                if (sched_->spinning_.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                    sched_->notify();
                }
            }
            CIO_METRIC(tasks_run, 1);
            std::coroutine_handle<>::from_address(item).resume();
            continue;
        }

        // park() consumes our searcher credit, and hands one back if it was
        // woken by notify() — inheriting it is what stops every wake from
        // re-entering the notify path.
        spinning = sched_->park(*this);
    }

    if (spinning) sched_->spinning_.fetch_sub(1, std::memory_order_seq_cst);
    t_worker = nullptr;
}

// ------------------------------------------------------------- Scheduler ---

Scheduler::Scheduler(std::size_t worker_count, std::size_t max_blocking_threads) {
    if (worker_count == 0) {
        worker_count = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    }
    reactor_ = std::make_unique<Reactor>(*this);
    timers_ = std::make_unique<TimerService>(*this, worker_count);
    blocking_ = std::make_unique<BlockingPool>(max_blocking_threads);

    workers_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        auto worker = std::make_unique<Worker>();
        worker->sched_ = this;
        worker->index_ = static_cast<std::uint32_t>(i);
        workers_.push_back(std::move(worker));
    }
}

Scheduler::~Scheduler() { shutdown(); }

void Scheduler::start() {
    if (started_.exchange(true, std::memory_order_acq_rel)) return;
    last_poll_ns_.store(now_ns(), std::memory_order_relaxed);
    for (auto& worker : workers_) {
        worker->thread_ = std::thread([w = worker.get()] { w->run(); });
    }
    monitor_ = std::thread([this] { monitor_main(); });
}

void Scheduler::shutdown() {
    if (stop_.exchange(true, std::memory_order_acq_rel)) return;

    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        wake_tokens_ = static_cast<std::uint32_t>(workers_.size());
    }
    idle_cv_.notify_all();
    reactor_->wake();

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

void Scheduler::schedule_frame(void* frame) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(frame);
    } else {
        global_.push(frame);
    }
    notify();
}

void Scheduler::reschedule_self(std::coroutine_handle<> h) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(h.address());
        return;  // deliberately no notify(): we are about to look at this queue
    }
    schedule_frame(h.address());
}

void Scheduler::schedule_next_frame(void* frame) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push_next(frame);
        // Only worth waking a peer if there is something it could actually
        // take. runnext is not stealable, so waking someone when the ring is
        // empty buys a guaranteed-wasted futex round trip — and on a hot
        // channel that is one wasted round trip per message.
        if (!worker->queue_.empty()) notify();
        return;
    }
    schedule_frame(frame);
}

void Scheduler::schedule_batch(void* const* frames, std::uint32_t n) noexcept {
    if (n == 0) return;
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        for (std::uint32_t i = 0; i < n; ++i) worker->push(frames[i]);
    } else {
        global_.push_batch(frames, n);
    }
    notify();
}

void Scheduler::schedule_deferred(std::coroutine_handle<> h) noexcept {
    Worker* worker = t_worker;
    if (worker != nullptr && worker->sched_ == this) {
        worker->push(h.address());
    } else {
        global_.push(h.address());
    }
    // No notify(): the caller batches it.
}

void Scheduler::notify_batch(std::uint32_t count) noexcept {
    if (count == 0) return;
    if (idle_.load(std::memory_order_seq_cst) == 0) return;
    if (count == 1) {
        notify();
        return;
    }

    std::uint32_t granted = 0;
    bool wake_everyone = false;
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        const std::uint32_t claimable = waiters_ > wake_tokens_ ? waiters_ - wake_tokens_ : 0;
        granted = count < claimable ? count : claimable;
        if (granted > 0) {
            // Both under the lock, and spinning_ first: a woken worker inherits
            // one of these searcher credits, and it needs this same lock to
            // claim its token, so it cannot decrement before we have added.
            spinning_.fetch_add(granted, std::memory_order_seq_cst);
            wake_tokens_ += granted;
            // There is now a token for every waiter, so one broadcast delivers
            // them all. Below, the loop of notify_one() is `granted` separate
            // FUTEX_WAKE syscalls issued back to back, which means the last
            // worker to be told starts that whole sequence late — and it is on
            // the critical path of whichever request its task belongs to.
            wake_everyone = wake_tokens_ >= waiters_;
        }
    }

    if (granted > 0) {
        CIO_METRIC(wake_batch_calls, 1);
        CIO_METRIC(wake_batch_workers, granted);
    }

    if (granted == 0) {
        // Nobody on the condition variable — fall back to the single-wake path,
        // which also knows how to nudge a worker parked inside the reactor.
        notify();
        return;
    }
    if (wake_everyone) {
        idle_cv_.notify_all();
        return;
    }
    for (std::uint32_t i = 0; i < granted; ++i) idle_cv_.notify_one();
}

void Scheduler::notify() noexcept {
    // Nobody is parked or on their way to parking; every worker is running a
    // task and will loop back through its queues when that task suspends.
    if (idle_.load(std::memory_order_seq_cst) == 0) return;

    // A searcher is already awake and will find whatever was just published —
    // see park() for why the ordering makes this safe.
    if (spinning_.load(std::memory_order_seq_cst) != 0) return;

    // Claim the right to create *the* searcher. Everything above this point is
    // two atomic loads, which is what schedule() pays in steady state; without
    // this gate a busy channel wakes and re-parks a thread on every single hop,
    // and the futex round-trip dwarfs the work being scheduled.
    std::uint32_t expected = 0;
    if (!spinning_.compare_exchange_strong(expected, 1, std::memory_order_seq_cst,
                                           std::memory_order_relaxed)) {
        return;
    }

    bool handed_off = false;
    bool wake_poller = false;
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        if (waiters_ > 0) {
            ++wake_tokens_;  // the token carries the searcher credit we just took
            handed_off = true;
        } else if (polling_.load(std::memory_order_relaxed)) {
            wake_poller = true;
        }
    }

    if (handed_off) {
        CIO_METRIC(wake_single, 1);
        idle_cv_.notify_one();
        return;
    }

    // Nobody took the credit, so give it back. This is safe against a worker
    // that is mid-park: it has not yet reached the condition variable, which
    // means it has not yet run its final work check, and that check happens
    // under the same mutex we just released.
    spinning_.fetch_sub(1, std::memory_order_seq_cst);
    if (wake_poller) reactor_->wake();
}

void Scheduler::nudge_poller(std::int64_t deadline_ns) noexcept {
    if (!polling_.load(std::memory_order_acquire)) return;
    // The poller already plans to wake up at or before this deadline.
    if (poller_deadline_ns_.load(std::memory_order_acquire) <= deadline_ns) return;
    reactor_->wake();
}

bool Scheduler::any_work_available() const noexcept {
    if (!global_.empty()) return true;
    for (const auto& worker : workers_) {
        if (worker->runnext_.load(std::memory_order_acquire) != nullptr) return true;
        if (!worker->queue_.empty()) return true;
    }
    return timers_->next_deadline_ns() <= now_ns();
}

bool Scheduler::park(Worker& /*worker*/) {
    CIO_METRIC(parks, 1);
    // Announce "idle" *before* dropping the searcher credit. notify() reads
    // idle_ then spinning_, both seq_cst; if it observes spinning_ == 0 then
    // our decrement already happened, so our increment did too and it is
    // guaranteed to observe idle_ > 0. That is what lets notify() skip the
    // mutex entirely on the fast path without ever losing a wakeup.
    idle_.fetch_add(1, std::memory_order_seq_cst);
    spinning_.fetch_sub(1, std::memory_order_seq_cst);

    std::unique_lock<std::mutex> lock(idle_mutex_);

    auto leave = [this] { idle_.fetch_sub(1, std::memory_order_seq_cst); };

    if (stop_.load(std::memory_order_acquire)) {
        leave();
        return false;
    }
    if (wake_tokens_ > 0) {
        --wake_tokens_;
        leave();
        return true;  // inherit the searcher credit that came with the token
    }
    if (any_work_available()) {
        leave();
        return false;
    }

    // Volunteer to be the one thread blocked in the reactor. polling_ is set
    // while holding the lock so notify() cannot decide "nobody to wake" in the
    // window between the decision and the syscall.
    bool unclaimed = false;
    if (polling_.compare_exchange_strong(unclaimed, true, std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        lock.unlock();

        const std::int64_t timeout_ns = timers_->next_timeout_ns();
        // Publish what we are waiting until, so a task arming an earlier timer
        // knows whether it needs to interrupt us.
        poller_deadline_ns_.store(timeout_ns < 0 ? INT64_MAX : now_ns() + timeout_ns,
                                  std::memory_order_release);
        last_poll_ns_.store(now_ns(), std::memory_order_relaxed);
        CIO_METRIC(polls_blocking, 1);
        reactor_->poll(timeout_ns);
        poller_deadline_ns_.store(INT64_MAX, std::memory_order_release);

        {
            std::lock_guard<std::mutex> relock(idle_mutex_);
            polling_.store(false, std::memory_order_release);
        }
        timers_->run_expired();
        leave();
        return false;
    }

    ++waiters_;
    CIO_METRIC(park_cv_waits, 1);
    idle_cv_.wait(lock, [this] {
        return wake_tokens_ > 0 || stop_.load(std::memory_order_acquire);
    });
    --waiters_;
    const bool inherited = wake_tokens_ > 0;
    if (inherited) --wake_tokens_;
    leave();
    return inherited;
}

void Scheduler::monitor_main() {
    // A sysmon-style watchdog. Its job is narrow but important: when every
    // worker is busy running CPU-bound tasks, nobody is inside the reactor and
    // nobody is checking the timer heaps, so I/O completions and timeouts would
    // sit unnoticed until a worker happened to go idle.
    std::int64_t delay_us = 50;

    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
        if (stop_.load(std::memory_order_acquire)) break;

        const std::int64_t now = now_ns();

        if (reactor_->registered() > 0 &&
            now - last_poll_ns_.load(std::memory_order_relaxed) > kMonitorPollStaleNs) {
            bool unclaimed = false;
            if (polling_.compare_exchange_strong(unclaimed, true, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                last_poll_ns_.store(now, std::memory_order_relaxed);
                CIO_METRIC(polls_nonblocking, 1);
                reactor_->poll(0);
                polling_.store(false, std::memory_order_release);
            }
        }

        if (timers_->next_deadline_ns() <= now) timers_->run_expired();

        // Back off hard once the runtime goes quiet; a fully idle process
        // should not burn a core on watchdog wakeups.
        if (idle_.load(std::memory_order_relaxed) >= workers_.size()) {
            delay_us = std::min<std::int64_t>(delay_us * 2, 10'000);
        } else {
            delay_us = 50;
        }
    }
}

}  // namespace cio::detail
