#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace cio::detail {

struct SchedulerTestAccess {
    static bool any_idle(Scheduler& scheduler) {
        return scheduler.idle_workers_->any();
    }

    static void push_next_private(Scheduler& scheduler, WorkerId worker,
                                  void* frame) {
        scheduler.workers_[worker]->push_next(frame);
    }

    static void stage_fairness_preselection(Scheduler& scheduler,
                                            WorkerId worker_id,
                                            void* inbox_frame,
                                            void* runnext_frame) {
        Worker& worker = *scheduler.workers_[worker_id];
        CIO_CHECK(worker.inbox_.try_push(inbox_frame));
        worker.runnext_.store(runnext_frame, std::memory_order_release);
    }

    static void stage_fairness_with_private_fifo(Scheduler& scheduler,
                                                 WorkerId worker_id,
                                                 void* inbox_frame,
                                                 void* fifo_frame) {
        Worker& worker = *scheduler.workers_[worker_id];
        CIO_CHECK(worker.inbox_.try_push(inbox_frame));
        CIO_CHECK(worker.queue_.push(fifo_frame));
        worker.runnext_.store(nullptr, std::memory_order_release);
    }

    static void stage_global_fairness(Scheduler& scheduler, WorkerId worker_id,
                                      void* global_frame, void* runnext_frame,
                                      void* fifo_frame) {
        Worker& worker = *scheduler.workers_[worker_id];
        scheduler.global_.push(global_frame);
        worker.runnext_.store(runnext_frame, std::memory_order_release);
        CIO_CHECK(worker.queue_.push(fifo_frame));
        worker.fair_cursor_ = 0;
    }

    static void* service_fairness(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->service_fairness();
    }

    static void* service_global_fairness(Scheduler& scheduler,
                                         WorkerId worker_id) {
        return scheduler.workers_[worker_id]->service_global_fairness();
    }

    static std::uint32_t steal_fifo(Scheduler& scheduler, WorkerId victim_id,
                                    void** frames, std::uint32_t capacity) {
        return scheduler.workers_[victim_id]->queue_.grab(frames, capacity);
    }

    static bool stealable(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.stealable_workers_->test(worker_id);
    }

    static void publish_fifo(Scheduler& scheduler, WorkerId worker_id,
                             void* frame) {
        Worker& worker = *scheduler.workers_[worker_id];
        CIO_CHECK(worker.queue_.push(frame));
        scheduler.publish_stealable(worker);
    }

    static void* pop_fifo(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->queue_.pop();
    }

    // Deliberately bypass Worker::repair_stealable(): this models a thief
    // clearing another worker's bit, so the owner-only fast-path flag remains
    // set and the next publication must notice clear_epoch.
    static bool repair_fifo_as_thief(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.repair_stealable(*scheduler.workers_[worker_id]);
    }

    static std::uint64_t clear_epoch(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]
            ->stealable_publication_.clear_epoch.load(
                std::memory_order_seq_cst);
    }

    static void publish_idle(Scheduler& scheduler, WorkerId worker_id) {
        scheduler.idle_workers_->set(worker_id);
    }

    static void clear_idle(Scheduler& scheduler, WorkerId worker_id) {
        scheduler.idle_workers_->clear(worker_id);
    }

    static void park_once(Scheduler& scheduler, WorkerId worker_id) {
        scheduler.park(*scheduler.workers_[worker_id]);
    }

    static void leave_park(Scheduler& scheduler, WorkerId worker_id) {
        scheduler.leave_park(*scheduler.workers_[worker_id]);
    }

    static bool searcher_credit(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->searcher_credit_.load(
            std::memory_order_acquire);
    }

    static void* consume_searcher_credit(Scheduler& scheduler,
                                         WorkerId worker_id) {
        return scheduler.workers_[worker_id]->consume_searcher_credit();
    }

    static void* pop_inbox(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->inbox_.pop();
    }

    static bool inbox_empty(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->inbox_.empty();
    }

    static void* pop_global(Scheduler& scheduler) {
        return scheduler.global_.pop();
    }

    static bool global_empty(Scheduler& scheduler) {
        return scheduler.global_.empty();
    }

    static void* runnext(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->runnext_.load(
            std::memory_order_acquire);
    }

    static void stage_runnext(Scheduler& scheduler, WorkerId worker_id,
                              void* runnext_frame) {
        scheduler.workers_[worker_id]->runnext_.store(
            runnext_frame, std::memory_order_release);
    }

    static void set_tick(Scheduler& scheduler, WorkerId worker_id,
                         std::uint32_t tick) {
        scheduler.workers_[worker_id]->tick_ = tick;
    }

    static std::uint32_t tick(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->tick_;
    }

    static void stage_inbox(Scheduler& scheduler, WorkerId worker_id,
                            void* frame) {
        CIO_CHECK(scheduler.workers_[worker_id]->inbox_.try_push(frame));
    }

    static void stage_global(Scheduler& scheduler, void* frame) {
        scheduler.global_.push(frame);
    }

    static void* select_round(Scheduler& scheduler, WorkerId worker_id) {
        Worker& worker = *scheduler.workers_[worker_id];
        void* item = worker.next_local();
        if (item == nullptr) item = worker.find_work();
        return item;
    }

    static void monitor_pass(Scheduler& scheduler, std::int64_t now) {
        scheduler.monitor_pass(now);
    }

    static void start_workers_without_monitor(Scheduler& scheduler) {
        const bool already_started =
            scheduler.started_.exchange(true, std::memory_order_acq_rel);
        CIO_CHECK(!already_started);
        if (already_started) return;
        for (auto& worker : scheduler.workers_) {
            worker->thread_ = std::thread([w = worker.get()] { w->run(); });
        }
    }

    static std::uint64_t driver_requested_epoch(Scheduler& scheduler,
                                                WorkerId worker_id) {
        return scheduler.workers_[worker_id]
            ->reactor_->driver_requested_epoch_.load(std::memory_order_acquire);
    }

    static std::uint64_t driver_attempted_epoch(Scheduler& scheduler,
                                                WorkerId worker_id) {
        return scheduler.workers_[worker_id]
            ->reactor_->driver_attempted_epoch_.load(std::memory_order_acquire);
    }

    static std::uint64_t driver_covered_epoch(Scheduler& scheduler,
                                              WorkerId worker_id) {
        return scheduler.workers_[worker_id]
            ->reactor_->driver_covered_epoch_.load(std::memory_order_acquire);
    }

    static std::int64_t driver_requested_at(Scheduler& scheduler,
                                            WorkerId worker_id) {
        return scheduler.workers_[worker_id]
            ->reactor_->driver_requested_at_ns_.load(std::memory_order_acquire);
    }

    static int driver_phase(Scheduler& scheduler, WorkerId worker_id) {
        using Phase = Reactor::DriverPhase;
        const Phase phase =
            scheduler.workers_[worker_id]->reactor_->driver_phase_.load(
                std::memory_order_acquire);
        switch (phase) {
            case Phase::kUnavailable:
                return 0;
            case Phase::kSuspended:
                return 1;
            case Phase::kQueued:
                return 2;
            case Phase::kRunning:
                return 3;
        }
        return -1;
    }

    static bool request_driver(Scheduler& scheduler, WorkerId worker_id,
                               std::int64_t requested_at) {
        return scheduler.workers_[worker_id]->reactor_->request_driver_at(
            requested_at);
    }

    static bool queue_driver(Scheduler& scheduler, WorkerId worker_id) {
        return scheduler.workers_[worker_id]->reactor_->queue_driver();
    }

    static bool should_use_batch_monitor_policy(int inherited_policy) {
        return Scheduler::should_use_batch_monitor_policy(inherited_policy);
    }
};

}  // namespace cio::detail

namespace {

struct SwitchToSchedulerWorker {
    cio::detail::Scheduler* scheduler = nullptr;
    cio::detail::WorkerId target = cio::detail::kInvalidWorkerId;

    bool await_ready() const noexcept {
        return cio::detail::current_worker_id(scheduler) == target;
    }
    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        scheduler->schedule_to(handle, target);
    }
    void await_resume() const noexcept {}
};

struct PublishSuspendedFrame {
    std::atomic<void*>* frame = nullptr;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        frame->store(handle.address(), std::memory_order_release);
    }
    void await_resume() const noexcept {}
};

void schedule_detached_to(cio::detail::Scheduler* scheduler, cio::Task<> task,
                          cio::detail::WorkerId worker);

cio::Task<int> add(int a, int b) {
    co_return a + b;
}

cio::Task<int> nested(int depth) {
    if (depth == 0) co_return 0;
    const int below = co_await nested(depth - 1);
    co_return below + 1;
}

void test_child_task_composition() {
    const int result = cio::run(add(2, 40));
    CIO_CHECK_EQ(result, 42);

    const int depth = cio::run(nested(1000));
    CIO_CHECK_EQ(depth, 1000);
}

void test_exception_propagates_through_await() {
    struct Boom : std::runtime_error {
        Boom() : std::runtime_error("boom") {}
    };

    auto thrower = []() -> cio::Task<int> {
        throw Boom{};
        co_return 1;
    };
    auto caller = [&]() -> cio::Task<int> {
        try {
            co_return co_await thrower();
        } catch (const Boom&) {
            co_return -1;
        }
    };

    CIO_CHECK_EQ(cio::run(caller()), -1);
}

void test_spawn_and_join() {
    auto body = []() -> cio::Task<int> {
        std::vector<cio::JoinHandle<int>> handles;
        handles.reserve(64);
        for (int i = 0; i < 64; ++i) {
            handles.push_back(cio::spawn(add(i, i)));
        }
        int total = 0;
        for (auto& handle : handles) total += co_await handle;
        co_return total;
    };

    int expected = 0;
    for (int i = 0; i < 64; ++i) expected += i + i;
    CIO_CHECK_EQ(cio::run(body()), expected);
}

cio::Task<int> finish_on_release(std::atomic<bool>* release) {
    while (!release->load(std::memory_order_acquire)) co_await cio::yield();
    co_return 42;
}

cio::Task<> release_cross_runtime_join(std::atomic<bool>* release) {
    release->store(true, std::memory_order_release);
    co_return;
}

struct CrossRuntimeJoinObservation {
    int value = 0;
    cio::detail::Scheduler* before = nullptr;
    cio::detail::Scheduler* after = nullptr;
};

cio::Task<CrossRuntimeJoinObservation> await_foreign_join(
    cio::JoinHandle<int> handle, std::atomic<bool>* release) {
    // With one B worker this task cannot run until the coroutine below has
    // suspended in JoinHandle::await_suspend(). That makes completion on A
    // race-free with waiter publication instead of relying on a sleep.
    auto releaser = cio::spawn(release_cross_runtime_join(release));

    CrossRuntimeJoinObservation observation;
    observation.before = cio::detail::current_scheduler();
    observation.value = co_await handle;
    observation.after = cio::detail::current_scheduler();
    co_await releaser;
    co_return observation;
}

void test_cross_runtime_join_resumes_on_awaiting_runtime() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime_a(options);
    cio::Runtime runtime_b(options);

    std::atomic<bool> release{false};
    auto handle = runtime_a.spawn(finish_on_release(&release));
    const auto observation =
        runtime_b.block_on(await_foreign_join(std::move(handle), &release));

    CIO_CHECK_EQ(observation.value, 42);
    CIO_CHECK(observation.before == &runtime_b.scheduler());
    CIO_CHECK(observation.after == &runtime_b.scheduler());
}

void test_join_handle_can_detach_before_completion() {
    static std::atomic<bool> completed{false};
    completed.store(false, std::memory_order_relaxed);

    auto body = []() -> cio::Task<> {
        auto handle = cio::spawn([]() -> cio::Task<> {
            co_await cio::sleep(2ms);
            completed.store(true, std::memory_order_release);
        }());
        handle.detach();
        while (!completed.load(std::memory_order_acquire))
            co_await cio::yield();
    };

    cio::run(body());
    CIO_CHECK(completed.load(std::memory_order_acquire));
}

void test_spawn_propagates_child_exception() {
    struct Boom : std::runtime_error {
        Boom() : std::runtime_error("spawn failed") {}
    };

    auto body = []() -> cio::Task<bool> {
        auto handle = cio::spawn([]() -> cio::Task<int> {
            throw Boom{};
            co_return 0;
        }());
        try {
            (void)co_await handle;
        } catch (const Boom&) {
            co_return true;
        }
        co_return false;
    };
    CIO_CHECK(cio::run(body()));
}

void test_spawn_preserves_invalid_and_completed_task_semantics() {
    auto body = []() -> cio::Task<bool> {
        cio::Task<int> invalid;
        auto invalid_join = cio::spawn(std::move(invalid));
        bool invalid_rethrown = false;
        try {
            (void)co_await invalid_join;
        } catch (const std::logic_error& error) {
            invalid_rethrown = std::string_view(error.what()) ==
                               "cio: awaited an invalid Task";
        }

        auto completed = []() -> cio::Task<int> { co_return 42; }();
        CIO_CHECK_EQ(co_await completed, 42);
        // Resuming a coroutine already parked at final_suspend is undefined.
        // spawn() retains its historical cold wrapper for this valid but
        // already-completed Task instead of taking the direct completion path.
        auto completed_join = cio::spawn(std::move(completed));
        co_return invalid_rethrown&& co_await completed_join == 42;
    };

    CIO_CHECK(cio::run(body()));
}

struct SpawnMoveFailure {};

struct ThrowOnSecondMove {
    int* moves = nullptr;

    explicit ThrowOnSecondMove(int* count) noexcept : moves(count) {}
    ThrowOnSecondMove(const ThrowOnSecondMove&) = delete;
    ThrowOnSecondMove& operator=(const ThrowOnSecondMove&) = delete;

    ThrowOnSecondMove(ThrowOnSecondMove&& other) : moves(other.moves) {
        if (++*moves == 2) throw SpawnMoveFailure{};
    }
};

void test_spawn_captures_result_move_failure() {
    auto body = []() -> cio::Task<bool> {
        int moves = 0;
        auto joined =
            cio::spawn([](int* count) -> cio::Task<ThrowOnSecondMove> {
                co_return ThrowOnSecondMove{count};
            }(&moves));
        try {
            (void)co_await joined;
        } catch (const SpawnMoveFailure&) {
            co_return moves == 2;
        }
        co_return false;
    };

    CIO_CHECK(cio::run(body()));
}

void test_completed_join_handle_can_be_awaited_again() {
    auto body = []() -> cio::Task<bool> {
        auto handle = cio::spawn([]() -> cio::Task<int> { co_return 42; }());

        CIO_CHECK_EQ(co_await handle, 42);
        auto saved_awaiter = handle.operator co_await();
        handle.detach();
        co_return co_await saved_awaiter == 42;
    };

    CIO_CHECK(cio::run(body()));
}

void test_concurrent_join_waiter_is_rejected() {
    auto body = []() -> cio::Task<bool> {
        std::atomic<bool> release{false};
        std::atomic<bool> first_entered{false};
        auto handle = cio::spawn([](std::atomic<bool>* gate) -> cio::Task<int> {
            while (!gate->load(std::memory_order_acquire)) {
                co_await cio::yield();
            }
            co_return 7;
        }(&release));

        auto first =
            cio::spawn([](cio::JoinHandle<int>* shared,
                          std::atomic<bool>* entered) -> cio::Task<int> {
                entered->store(true, std::memory_order_release);
                co_return co_await *shared;
            }(&handle, &first_entered));

        while (!first_entered.load(std::memory_order_acquire)) {
            co_await cio::yield();
        }
        // Let the first joiner publish its wait node.
        co_await cio::yield();

        bool rejected = false;
        try {
            (void)co_await handle;
        } catch (const std::logic_error& e) {
            rejected = std::string_view(e.what()) ==
                       "cio: JoinHandle already has a waiter";
        }

        release.store(true, std::memory_order_release);
        const int value = co_await first;
        co_return rejected&& value == 7;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

void test_saved_join_awaiter_survives_handle_detach() {
    auto body = []() -> cio::Task<bool> {
        std::atomic<bool> release{false};
        auto incomplete =
            cio::spawn([](std::atomic<bool>* gate) -> cio::Task<int> {
                while (!gate->load(std::memory_order_acquire)) {
                    co_await cio::yield();
                }
                co_return 21;
            }(&release));
        auto incomplete_awaiter = incomplete.operator co_await();
        incomplete.detach();
        release.store(true, std::memory_order_release);
        const int first = co_await incomplete_awaiter;

        auto completed = cio::spawn([]() -> cio::Task<int> { co_return 21; }());
        while (!completed.done()) co_await cio::yield();
        auto completed_awaiter = completed.operator co_await();
        completed.detach();
        const int second = co_await completed_awaiter;

        co_return first + second == 42;
    };

    CIO_CHECK(cio::run(body()));
}

void test_completed_void_join_snapshot_survives_detach() {
    auto body = []() -> cio::Task<bool> {
        auto completed = cio::spawn([]() -> cio::Task<> { co_return; }());
        while (!completed.done()) co_await cio::yield();

        auto first = completed.operator co_await();
        co_await first;
        auto saved = completed.operator co_await();
        completed.detach();
        co_await saved;
        co_return true;
    };

    CIO_CHECK(cio::run(body()));
}

void test_completed_void_exception_snapshot_keeps_state() {
    struct VoidBoom {};

    auto body = []() -> cio::Task<bool> {
        auto failed = cio::spawn([]() -> cio::Task<> {
            throw VoidBoom{};
            co_return;
        }());
        while (!failed.done()) co_await cio::yield();

        auto saved = failed.operator co_await();
        failed.detach();
        try {
            co_await saved;
        } catch (const VoidBoom&) {
            co_return true;
        }
        co_return false;
    };

    CIO_CHECK(cio::run(body()));
}

void test_spawn_join_handoff_respects_local_batch() {
    auto body = []() -> cio::Task<bool> {
        std::vector<int> order;
        const auto record = [](std::vector<int>* target,
                               int value) -> cio::Task<> {
            target->push_back(value);
            co_return;
        };

        // The first child entered an empty local FIFO, so its completion may
        // hand directly back to the joiner ahead of a later sibling.
        auto lone = cio::spawn(record(&order, 1));
        auto later = cio::spawn(record(&order, 3));
        co_await lone;
        order.push_back(2);
        co_await later;
        CIO_CHECK(order == std::vector<int>({1, 2, 3}));

        order.clear();
        // Only the first enqueue in a batch receives the direct-handoff hint.
        // Awaiting the second must leave its continuation behind the sibling
        // that was already queued, preserving batch completion.
        auto first = cio::spawn(record(&order, 4));
        auto batched = cio::spawn(record(&order, 5));
        auto sibling = cio::spawn(record(&order, 6));
        co_await batched;
        order.push_back(7);
        co_await first;
        co_await sibling;
        co_return order == std::vector<int>({4, 5, 6, 7});
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

struct RunnextFairnessState {
    std::atomic<bool> victim_enqueued{false};
    std::atomic<bool> victim_ran{false};
    std::atomic<bool> watchdog_fired{false};
};

cio::Task<> runnext_fairness_victim(cio::Chan<> left, cio::Chan<> right,
                                    cio::Chan<> done,
                                    RunnextFairnessState* state) {
    state->victim_ran.store(true, std::memory_order_release);
    left.close();
    right.close();
    done.close();
    co_return;
}

cio::Task<> runnext_ping(cio::Chan<> left, cio::Chan<> right, cio::Chan<> done,
                         RunnextFairnessState* state) {
    bool launched_victim = false;
    while (co_await left.recv()) {
        if (!launched_victim) {
            launched_victim = true;
            cio::go(runnext_fairness_victim(left, right, done, state));
            // Publish only after go() has placed the victim in this worker's
            // ordinary FIFO.
            state->victim_enqueued.store(true, std::memory_order_release);
        }
        if (!(co_await right.send(cio::Unit{}))) co_return;
    }
}

cio::Task<> runnext_pong(cio::Chan<> left, cio::Chan<> right) {
    while (co_await right.recv()) {
        if (!(co_await left.send(cio::Unit{}))) co_return;
    }
}

void test_runnext_handoff_does_not_starve_local_fifo() {
    auto left = cio::make_chan<>();
    auto right = cio::make_chan<>();
    auto done = cio::make_chan<>();
    RunnextFairnessState state;

    // The watchdog only makes the pre-fix infinite handoff chain terminate.
    // It is deliberately an OS thread so it cannot itself be starved in the
    // worker's local FIFO.
    std::thread watchdog([&] {
        const auto enqueue_deadline = cio::Clock::now() + 1s;
        while (!state.victim_enqueued.load(std::memory_order_acquire) &&
               cio::Clock::now() < enqueue_deadline) {
            std::this_thread::yield();
        }
        if (state.victim_enqueued.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(100ms);
        }
        if (state.victim_ran.load(std::memory_order_acquire)) return;

        state.watchdog_fired.store(true, std::memory_order_release);
        left.close();
        right.close();
        done.close();
    });

    auto body = [&]() -> cio::Task<bool> {
        cio::TaskGroup pair;
        pair.spawn(runnext_ping(left, right, done, &state));
        pair.spawn(runnext_pong(left, right));

        co_await left.send(cio::Unit{});
        (void)co_await done.recv();
        co_await pair.join();
        co_return state.victim_ran.load(std::memory_order_acquire) &&
            !state.watchdog_fired.load(std::memory_order_acquire);
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    const bool fair = cio::run(body(), options);
    watchdog.join();
    CIO_CHECK(fair);
}

struct RemoteInboxFairnessState {
    std::atomic<bool> chain_started{false};
    std::atomic<bool> remote_enqueued{false};
    std::atomic<bool> remote_ran{false};
    std::atomic<bool> watchdog_fired{false};
};

cio::Task<> remote_inbox_ping(cio::Chan<> left, cio::Chan<> right,
                              RemoteInboxFairnessState* state) {
    state->chain_started.store(true, std::memory_order_release);
    while (co_await left.recv()) {
        if (!(co_await right.send(cio::Unit{}))) co_return;
    }
}

cio::Task<> remote_inbox_fairness_victim(cio::Chan<> left, cio::Chan<> right,
                                         cio::Chan<> done,
                                         RemoteInboxFairnessState* state) {
    state->remote_ran.store(true, std::memory_order_release);
    left.close();
    right.close();
    done.close();
    co_return;
}

void test_runnext_handoff_does_not_starve_remote_inbox() {
    auto left = cio::make_chan<>();
    auto right = cio::make_chan<>();
    auto done = cio::make_chan<>();
    RemoteInboxFairnessState state;

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);

    // Submit from a foreign OS thread after the direct-handoff chain is live.
    // With one worker this has exactly one destination: its MPSC inbox.
    std::thread producer([&] {
        const auto start_deadline = cio::Clock::now() + 1s;
        while (!state.chain_started.load(std::memory_order_acquire) &&
               cio::Clock::now() < start_deadline) {
            std::this_thread::yield();
        }
        if (!state.chain_started.load(std::memory_order_acquire)) {
            state.watchdog_fired.store(true, std::memory_order_release);
            left.close();
            right.close();
            done.close();
            return;
        }

        schedule_detached_to(
            &runtime.scheduler(),
            remote_inbox_fairness_victim(left, right, done, &state), 0);
        state.remote_enqueued.store(true, std::memory_order_release);

        const auto run_deadline = cio::Clock::now() + 250ms;
        while (!state.remote_ran.load(std::memory_order_acquire) &&
               cio::Clock::now() < run_deadline) {
            std::this_thread::yield();
        }
        if (!state.remote_ran.load(std::memory_order_acquire)) {
            state.watchdog_fired.store(true, std::memory_order_release);
            left.close();
            right.close();
            done.close();
        }
    });

    auto body = [&]() -> cio::Task<bool> {
        cio::TaskGroup pair;
        pair.spawn(remote_inbox_ping(left, right, &state));
        pair.spawn(runnext_pong(left, right));

        co_await left.send(cio::Unit{});
        (void)co_await done.recv();
        co_await pair.join();
        co_return state.remote_enqueued.load(std::memory_order_acquire) &&
            state.remote_ran.load(std::memory_order_acquire) &&
            !state.watchdog_fired.load(std::memory_order_acquire);
    };

    const bool fair = runtime.block_on(body());
    producer.join();
    CIO_CHECK(fair);
}

void test_foreign_poller_preserves_directed_wake_for_owner() {
    // No worker is started, so both calls model the scheduler monitor rather
    // than the shard owner. The first poll may observe the wake, but it must
    // leave the level-triggered eventfd token latched for the owner. Before the
    // fix it drained the token and the second poll returned zero.
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    reactor.wake();

    CIO_CHECK(reactor.poll(0) > 0);
    CIO_CHECK(reactor.poll(0) > 0);
}

cio::Task<> suspend_io_target(cio::detail::Scheduler* scheduler,
                              std::atomic<void*>* frame,
                              std::atomic<cio::detail::WorkerId>* resumed_on,
                              std::atomic<bool>* done) {
    co_await SwitchToSchedulerWorker{scheduler, 1};
    co_await PublishSuspendedFrame{frame};
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    done->store(true, std::memory_order_release);
}

struct IoCompletionPlacement {
    cio::detail::Scheduler::IoCompletionRoute route =
        cio::detail::Scheduler::IoCompletionRoute::kSharedFallback;
    cio::detail::WorkerId poller = cio::detail::kInvalidWorkerId;
};

cio::Task<IoCompletionPlacement> inject_io_completion_from_worker_zero(
    cio::detail::Scheduler* scheduler, void* frame, std::atomic<bool>* done) {
    co_await SwitchToSchedulerWorker{scheduler, 0};

    IoCompletionPlacement placement;
    placement.poller = cio::detail::current_worker_id(scheduler);
    placement.route = scheduler->schedule_io_completion(
        std::coroutine_handle<>::from_address(frame), 1);
    scheduler->finish_io_batch(
        placement.route == cio::detail::Scheduler::IoCompletionRoute::kLocalFifo
            ? 1u
            : 0u);

    while (!done->load(std::memory_order_acquire)) {
        co_await cio::yield();
    }
    co_return placement;
}

void test_same_runtime_io_completion_follows_active_poller() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    std::atomic<void*> frame{nullptr};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    std::atomic<bool> done{false};
    runtime.go(suspend_io_target(scheduler, &frame, &resumed_on, &done));

    const auto publish_deadline = cio::Clock::now() + 1s;
    while (frame.load(std::memory_order_acquire) == nullptr &&
           cio::Clock::now() < publish_deadline) {
        std::this_thread::yield();
    }
    void* const suspended = frame.load(std::memory_order_acquire);
    CIO_CHECK(suspended != nullptr);
    if (suspended == nullptr) return;

    const IoCompletionPlacement placement = runtime.block_on(
        inject_io_completion_from_worker_zero(scheduler, suspended, &done));
    CIO_CHECK(placement.route ==
              cio::detail::Scheduler::IoCompletionRoute::kRunnext);
    CIO_CHECK_EQ(placement.poller, 0u);
    CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 0u);
}

cio::Task<> io_completion_cpu_hog(std::atomic<bool>* started,
                                  std::atomic<bool>* release,
                                  std::atomic<bool>* finished) {
    started->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    finished->store(true, std::memory_order_release);
    co_return;
}

void schedule_detached_to(cio::detail::Scheduler* scheduler, cio::Task<> task,
                          cio::detail::WorkerId worker) {
    auto handle = task.release();
    CIO_CHECK(static_cast<bool>(handle));
    if (!handle) return;
    handle.promise().detached = true;
    scheduler->schedule_to(handle, worker);
}

void* release_detached_frame(cio::Task<> task) {
    auto handle = task.release();
    CIO_CHECK(static_cast<bool>(handle));
    if (!handle) return nullptr;
    handle.promise().continuation_or_completion = nullptr;
    handle.promise().detached = true;
    return handle.address();
}

struct SuspendWithCooperativeIoDebt {
    cio::detail::Scheduler* scheduler = nullptr;
    std::uint8_t debt = cio::detail::kCooperativeIoDebtNone;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        cio::detail::Scheduler* const selected = scheduler;
        const std::uint8_t selected_debt = debt;
        selected->reschedule_self_for_cooperative_io(handle, selected_debt);
    }
    void await_resume() const noexcept {}
};

cio::Task<> record_cooperative_order(std::atomic<int>* sequence,
                                     std::atomic<int>* observed) {
    observed->store(sequence->fetch_add(1, std::memory_order_acq_rel) + 1,
                    std::memory_order_release);
    co_return;
}

cio::Task<bool> cooperative_no_demand_renews_inline(
    cio::detail::Scheduler* scheduler) {
    const cio::detail::WorkerId worker =
        cio::detail::current_worker_id(scheduler);
    cio::detail::SchedulerTestAccess::set_tick(*scheduler, worker, 7);

    const std::uint32_t before =
        cio::detail::SchedulerTestAccess::tick(*scheduler, worker);
    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    const bool first_inline =
        cio::detail::SchedulerTestAccess::tick(*scheduler, worker) == before &&
        cio::detail::t_cooperative_io_budget ==
            cio::detail::kCooperativeIoBudget + 1;

    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    const bool second_inline =
        cio::detail::SchedulerTestAccess::tick(*scheduler, worker) == before &&
        cio::detail::t_cooperative_io_budget ==
            cio::detail::kCooperativeIoBudget + 1;
    co_return first_inline&& second_inline;
}

void test_cooperative_io_no_demand_renews_inline() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(
        cooperative_no_demand_renews_inline(&runtime.scheduler())));

    // A checkpoint is inactive on a foreign thread even though the process
    // default scheduler exists.
    cio::detail::t_cooperative_io_budget = 0;
    cio::detail::CooperativeIoCheckpoint foreign;
    CIO_CHECK(foreign.await_ready());
    CIO_CHECK_EQ(cio::detail::t_cooperative_io_budget, UINT64_MAX);
}

cio::Task<bool> cooperative_local_demand_grace_body(
    cio::detail::Scheduler* scheduler) {
    const cio::detail::WorkerId worker =
        cio::detail::current_worker_id(scheduler);
    auto& reactor = scheduler->reactor_for(worker);
    (void)reactor.take_owner_poll_request_ns();
    CIO_CHECK(reactor.request_owner_poll_at(cio::now_ns()));

    std::atomic<int> sequence{0};
    std::atomic<int> observer_order{0};
    cio::go(record_cooperative_order(&sequence, &observer_order));

    cio::detail::SchedulerTestAccess::set_tick(*scheduler, worker, 0);
    const std::int64_t poll_before = reactor.last_poll_ns();
    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    const bool first_inline =
        observer_order.load(std::memory_order_acquire) == 0 &&
        reactor.owner_poll_request_ns() != 0 &&
        reactor.last_poll_ns() == poll_before &&
        cio::detail::t_cooperative_io_budget ==
            cio::detail::kCooperativeIoBudget + 1;

    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    const int current_order =
        sequence.fetch_add(1, std::memory_order_acq_rel) + 1;

    const bool result =
        first_inline && observer_order.load(std::memory_order_acquire) == 1 &&
        current_order == 2 && reactor.owner_poll_request_ns() != 0 &&
        reactor.last_poll_ns() == poll_before;
    (void)reactor.take_owner_poll_request_ns();
    co_return result;
}

void test_cooperative_io_local_demand_gets_one_grace_budget() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(
        cooperative_local_demand_grace_body(&runtime.scheduler())));
}

cio::Task<> hold_local_checkpoint_owner(std::atomic<bool>* parent_resumed,
                                        std::atomic<bool>* parent_escaped,
                                        std::atomic<bool>* done) {
    const auto deadline = cio::Clock::now() + 1s;
    while (!parent_resumed->load(std::memory_order_acquire) &&
           cio::Clock::now() < deadline) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }
    parent_escaped->store(parent_resumed->load(std::memory_order_acquire),
                          std::memory_order_release);
    done->store(true, std::memory_order_release);
    co_return;
}

cio::Task<bool> cooperative_local_hog_parent_escape_body(
    cio::detail::Scheduler* scheduler) {
    const auto idle_deadline = cio::Clock::now() + 1s;
    while (!cio::detail::SchedulerTestAccess::any_idle(*scheduler) &&
           cio::Clock::now() < idle_deadline) {
        co_await cio::yield();
    }
    if (!cio::detail::SchedulerTestAccess::any_idle(*scheduler)) {
        co_return false;
    }

    std::atomic<bool> parent_resumed{false};
    std::atomic<bool> parent_escaped{false};
    std::atomic<bool> hog_done{false};
    const cio::detail::WorkerId before =
        cio::detail::current_worker_id(scheduler);
    // Keep the next owner selection away from the periodic global-fairness
    // checkpoint. Otherwise that checkpoint may legally select the published
    // parent first and expose the displaced hog instead, which preserves
    // progress but does not exercise the parent-steal path below.
    cio::detail::SchedulerTestAccess::set_tick(*scheduler, before, 0);

    void* const hog = release_detached_frame(hold_local_checkpoint_owner(
        &parent_resumed, &parent_escaped, &hog_done));
    if (hog == nullptr) co_return false;
    cio::detail::SchedulerTestAccess::push_next_private(*scheduler, before,
                                                        hog);

    // The first local-only boundary consumes the throughput grace. The second
    // must publish this parent because the other worker is idle.
    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};

    const cio::detail::WorkerId after =
        cio::detail::current_worker_id(scheduler);
    parent_resumed.store(true, std::memory_order_release);

    const auto done_deadline = cio::Clock::now() + 1s;
    while (!hog_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < done_deadline) {
        co_await cio::yield();
    }
    co_return hog_done.load(std::memory_order_acquire) &&
        parent_escaped.load(std::memory_order_acquire) && before != after;
}

void test_cooperative_io_local_hog_publishes_parent() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(
        cooperative_local_hog_parent_escape_body(&runtime.scheduler())));
}

cio::Task<bool> cooperative_hard_debt_order_body(
    cio::detail::Scheduler* scheduler) {
    const cio::detail::WorkerId worker =
        cio::detail::current_worker_id(scheduler);
    std::atomic<int> sequence{0};
    std::atomic<int> local_order{0};
    std::atomic<int> inbox_order{0};
    std::atomic<int> global_order{0};

    void* const local = release_detached_frame(
        record_cooperative_order(&sequence, &local_order));
    void* const global = release_detached_frame(
        record_cooperative_order(&sequence, &global_order));
    void* const inbox = release_detached_frame(
        record_cooperative_order(&sequence, &inbox_order));
    if (local == nullptr || global == nullptr || inbox == nullptr) {
        co_return false;
    }

    cio::detail::SchedulerTestAccess::stage_runnext(*scheduler, worker, local);
    cio::detail::SchedulerTestAccess::stage_global(*scheduler, global);
    cio::detail::SchedulerTestAccess::stage_inbox(*scheduler, worker, inbox);
    cio::detail::SchedulerTestAccess::set_tick(*scheduler, worker, 0);

    const std::uint8_t expected = cio::detail::kCooperativeIoDebtLocal |
                                  cio::detail::kCooperativeIoDebtInbox |
                                  cio::detail::kCooperativeIoDebtGlobal;
    const std::uint8_t debt = scheduler->prepare_cooperative_io_checkpoint();
    const bool exact_debt = debt == expected;

    // OR the expected mask for failure cleanup: every detached marker must be
    // drained before this coroutine's stack state goes out of scope.
    co_await SuspendWithCooperativeIoDebt{
        scheduler, static_cast<std::uint8_t>(debt | expected)};
    const int current_order =
        sequence.fetch_add(1, std::memory_order_acq_rel) + 1;

    co_return exact_debt&& inbox_order.load(std::memory_order_acquire) == 1 &&
        local_order.load(std::memory_order_acquire) == 2 &&
        global_order.load(std::memory_order_acquire) == 3 && current_order == 4;
}

void test_cooperative_io_stages_hard_debt_before_self() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(
        cooperative_hard_debt_order_body(&runtime.scheduler())));
}

cio::Task<> cooperative_nonlocal_hog(std::atomic<bool>* started,
                                     std::atomic<bool>* release,
                                     std::atomic<bool>* finished) {
    started->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
        cio::cpu_relax();
    }
    finished->store(true, std::memory_order_release);
    co_return;
}

cio::Task<bool> cooperative_nonlocal_publication_body(
    cio::detail::Scheduler* scheduler, std::atomic<bool>* hog_started,
    std::atomic<bool>* release_hog, std::atomic<bool>* hog_finished,
    std::atomic<bool>* continuation_resumed) {
    co_await SwitchToSchedulerWorker{scheduler, 0};

    void* const hog = release_detached_frame(
        cooperative_nonlocal_hog(hog_started, release_hog, hog_finished));
    if (hog == nullptr) co_return false;
    cio::detail::SchedulerTestAccess::stage_inbox(*scheduler, 0, hog);
    const std::uint8_t debt = scheduler->prepare_cooperative_io_checkpoint();

    co_await SuspendWithCooperativeIoDebt{
        scheduler,
        static_cast<std::uint8_t>(debt | cio::detail::kCooperativeIoDebtInbox)};
    const cio::detail::WorkerId resumed_on =
        cio::detail::current_worker_id(scheduler);
    continuation_resumed->store(true, std::memory_order_release);
    release_hog->store(true, std::memory_order_release);

    while (!hog_finished->load(std::memory_order_acquire)) {
        co_await cio::yield();
    }
    co_return (debt & cio::detail::kCooperativeIoDebtInbox) != 0 &&
        resumed_on == 1;
}

void test_cooperative_io_nonlocal_stage_publishes_self() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    std::atomic<bool> continuation_resumed{false};
    std::atomic<bool> watchdog_fired{false};
    std::thread watchdog([&] {
        const auto start_deadline = cio::Clock::now() + 1s;
        while (!hog_started.load(std::memory_order_acquire) &&
               cio::Clock::now() < start_deadline) {
            std::this_thread::yield();
        }
        const auto escape_deadline = cio::Clock::now() + 250ms;
        while (!continuation_resumed.load(std::memory_order_acquire) &&
               cio::Clock::now() < escape_deadline) {
            std::this_thread::yield();
        }
        if (!continuation_resumed.load(std::memory_order_acquire)) {
            watchdog_fired.store(true, std::memory_order_release);
            release_hog.store(true, std::memory_order_release);
        }
    });

    const bool escaped = runtime.block_on(cooperative_nonlocal_publication_body(
        &runtime.scheduler(), &hog_started, &release_hog, &hog_finished,
        &continuation_resumed));
    release_hog.store(true, std::memory_order_release);
    watchdog.join();

    CIO_CHECK(escaped);
    CIO_CHECK(!watchdog_fired.load(std::memory_order_acquire));
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
}

struct CooperativeTicketResult {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{false};
};

cio::Task<> cooperative_empty_ticket_controller(
    cio::detail::Scheduler* scheduler, int fd,
    CooperativeTicketResult* result) {
    auto& reactor = scheduler->reactor_for(0);
    auto attached = reactor.attach(fd);
    if (!attached) {
        result->done.store(true, std::memory_order_release);
        co_return;
    }

    cio::detail::IoDesc* const desc = *attached;
    (void)reactor.take_owner_poll_request_ns();
    const bool requested = reactor.request_owner_poll_at(cio::now_ns());
    const std::uint32_t tick_before =
        cio::detail::SchedulerTestAccess::tick(*scheduler, 0);
    const std::int64_t poll_before = reactor.last_poll_ns();

    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};

    const bool ok =
        requested && reactor.owner_poll_request_ns() == 0 &&
        reactor.last_poll_ns() > poll_before &&
        cio::detail::SchedulerTestAccess::tick(*scheduler, 0) == tick_before &&
        cio::detail::t_cooperative_io_budget ==
            cio::detail::kCooperativeIoBudget + 1;
    reactor.detach(desc);
    result->ok.store(ok, std::memory_order_release);
    result->done.store(true, std::memory_order_release);
}

void test_cooperative_io_empty_ticket_poll_renews_inline() {
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    cio::detail::Scheduler scheduler(1, 1);
    cio::detail::SchedulerTestAccess::start_workers_without_monitor(scheduler);
    CooperativeTicketResult result;
    schedule_detached_to(
        &scheduler,
        cooperative_empty_ticket_controller(&scheduler, pipe_fds[0], &result),
        0);

    const auto deadline = cio::Clock::now() + 1s;
    while (!result.done.load(std::memory_order_acquire) &&
           cio::Clock::now() < deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(result.done.load(std::memory_order_acquire));
    CIO_CHECK(result.ok.load(std::memory_order_acquire));
    scheduler.shutdown();
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

cio::Task<> cooperative_ticket_waiter(cio::detail::IoDesc* desc,
                                      std::atomic<int>* sequence,
                                      std::atomic<int>* waiter_order,
                                      std::atomic<bool>* done) {
    const auto ready = co_await cio::detail::IoAwaiter{
        desc, cio::detail::Dir::kRead,
        desc->generation.load(std::memory_order_acquire)};
    if (ready) {
        waiter_order->store(
            sequence->fetch_add(1, std::memory_order_acq_rel) + 1,
            std::memory_order_release);
    }
    done->store(true, std::memory_order_release);
}

cio::Task<> cooperative_productive_ticket_controller(
    cio::detail::Scheduler* scheduler, int read_fd, int write_fd,
    CooperativeTicketResult* result) {
    auto& reactor = scheduler->reactor_for(0);
    auto attached = reactor.attach(read_fd);
    if (!attached) {
        result->done.store(true, std::memory_order_release);
        co_return;
    }
    cio::detail::IoDesc* const desc = *attached;
    (void)reactor.take_owner_poll_request_ns();

    std::atomic<int> sequence{0};
    std::atomic<int> waiter_order{0};
    std::atomic<bool> waiter_done{false};
    schedule_detached_to(
        scheduler,
        cooperative_ticket_waiter(desc, &sequence, &waiter_order, &waiter_done),
        0);
    co_await cio::yield();

    const char byte = 'C';
    const bool wrote = ::write(write_fd, &byte, sizeof(byte)) ==
                       static_cast<ssize_t>(sizeof(byte));
    const bool requested = reactor.request_owner_poll_at(cio::now_ns());
    cio::detail::t_cooperative_io_budget = 1;
    co_await cio::detail::CooperativeIoCheckpoint{};
    const int controller_order =
        sequence.fetch_add(1, std::memory_order_acq_rel) + 1;

    const bool ok =
        wrote && requested && waiter_done.load(std::memory_order_acquire) &&
        waiter_order.load(std::memory_order_acquire) == 1 &&
        controller_order == 2 && reactor.owner_poll_request_ns() == 0;
    reactor.detach(desc);
    result->ok.store(ok, std::memory_order_release);
    result->done.store(true, std::memory_order_release);
}

void test_cooperative_io_productive_ticket_yields() {
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    cio::detail::Scheduler scheduler(1, 1);
    cio::detail::SchedulerTestAccess::start_workers_without_monitor(scheduler);
    CooperativeTicketResult result;
    schedule_detached_to(&scheduler,
                         cooperative_productive_ticket_controller(
                             &scheduler, pipe_fds[0], pipe_fds[1], &result),
                         0);

    const auto deadline = cio::Clock::now() + 1s;
    while (!result.done.load(std::memory_order_acquire) &&
           cio::Clock::now() < deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(result.done.load(std::memory_order_acquire));
    CIO_CHECK(result.ok.load(std::memory_order_acquire));
    scheduler.shutdown();
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

cio::Task<bool> cooperative_budget_reset_body() {
    cio::detail::t_cooperative_io_budget = 7;
    co_await cio::yield();
    co_return cio::detail::t_cooperative_io_budget ==
        cio::detail::kCooperativeIoBudget + 1;
}

void test_cooperative_io_top_level_resume_resets_budget() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(cooperative_budget_reset_body(), options));
}

cio::Task<> await_driver_polled_pipe(
    cio::detail::Scheduler* scheduler, cio::detail::IoDesc* desc,
    std::atomic<bool>* done, std::atomic<bool>* result_ok,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    const auto ready = co_await cio::detail::IoAwaiter{
        desc, cio::detail::Dir::kRead,
        desc->generation.load(std::memory_order_acquire)};
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    result_ok->store(ready.has_value(), std::memory_order_release);
    done->store(true, std::memory_order_release);
}

cio::Task<> run_queued_driver_from_worker_one(
    cio::detail::Scheduler* scheduler, std::atomic<bool>* started,
    std::atomic<bool>* run, void* expected_waiter,
    std::atomic<bool>* resumed_driver,
    std::atomic<bool>* completion_was_local) {
    co_await SwitchToSchedulerWorker{scheduler, 1};
    started->store(true, std::memory_order_release);
    while (!run->load(std::memory_order_acquire)) {
        cio::cpu_relax();
    }

    void* const frame =
        cio::detail::SchedulerTestAccess::pop_global(*scheduler);
    if (frame != nullptr) {
        std::coroutine_handle<>::from_address(frame).resume();
        resumed_driver->store(true, std::memory_order_release);
        completion_was_local->store(
            cio::detail::SchedulerTestAccess::runnext(*scheduler, 1) ==
                    expected_waiter &&
                cio::detail::SchedulerTestAccess::global_empty(*scheduler),
            std::memory_order_release);
    }
}

void test_worker_driver_polls_and_places_completion_locally() {
    cio::detail::Scheduler scheduler(2, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }

    cio::detail::SchedulerTestAccess::start_workers_without_monitor(scheduler);

    std::atomic<bool> waiter_done{false};
    std::atomic<bool> waiter_ok{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    schedule_detached_to(
        &scheduler,
        await_driver_polled_pipe(&scheduler, *attached, &waiter_done,
                                 &waiter_ok, &resumed_on),
        0);

    const auto setup_deadline = cio::Clock::now() + 2s;
    const auto waiter_parked = [&] {
        void* const slot =
            (*attached)
                ->slot[static_cast<unsigned>(cio::detail::Dir::kRead)]
                .load(std::memory_order_acquire);
        return slot != nullptr && slot != cio::detail::kIoReady;
    };
    while (!waiter_parked() && cio::Clock::now() < setup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(waiter_parked());
    void* expected_waiter = nullptr;
    if (waiter_parked()) {
        void* const slot =
            (*attached)
                ->slot[static_cast<unsigned>(cio::detail::Dir::kRead)]
                .load(std::memory_order_acquire);
        expected_waiter =
            static_cast<cio::detail::IoWait*>(slot)->handle.address();
    }
    CIO_CHECK(expected_waiter != nullptr);

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        &scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < setup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    std::atomic<bool> coordinator_started{false};
    std::atomic<bool> run_driver{false};
    std::atomic<bool> resumed_driver{false};
    std::atomic<bool> completion_was_local{false};
    schedule_detached_to(
        &scheduler,
        run_queued_driver_from_worker_one(
            &scheduler, &coordinator_started, &run_driver, expected_waiter,
            &resumed_driver, &completion_was_local),
        1);
    while (!coordinator_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < setup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(coordinator_started.load(std::memory_order_acquire));

    (void)reactor.take_owner_poll_request_ns();
    const std::int64_t before_poll = reactor.last_poll_ns();
    const char byte = 'd';
    CIO_CHECK_EQ(::write(pipe_fds[1], &byte, 1), 1);
    const std::int64_t stale_now = before_poll + 1'000'000;
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 50'000);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 2);

    run_driver.store(true, std::memory_order_release);
    const auto completion_deadline = cio::Clock::now() + 2s;
    while (!waiter_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < completion_deadline) {
        std::this_thread::yield();
    }

    const bool completed_before_backstop =
        waiter_done.load(std::memory_order_acquire);
    release_hog.store(true, std::memory_order_release);
    while (!hog_finished.load(std::memory_order_acquire) &&
           cio::Clock::now() < completion_deadline) {
        std::this_thread::yield();
    }

    if (!waiter_done.load(std::memory_order_acquire)) {
        reactor.detach(*attached);
        const auto close_deadline = cio::Clock::now() + 1s;
        while (!waiter_done.load(std::memory_order_acquire) &&
               cio::Clock::now() < close_deadline) {
            std::this_thread::yield();
        }
    } else {
        reactor.detach(*attached);
    }
    scheduler.shutdown();
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);

    CIO_CHECK(completed_before_backstop);
    CIO_CHECK(resumed_driver.load(std::memory_order_acquire));
    CIO_CHECK(completion_was_local.load(std::memory_order_acquire));
    CIO_CHECK(waiter_ok.load(std::memory_order_acquire));
    CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire),
                 cio::detail::WorkerId{1});
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        1u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        1u);
}

cio::Task<> mark_foreign_submission_ran(
    cio::detail::Scheduler* scheduler, std::atomic<bool>* done,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    done->store(true, std::memory_order_release);
    co_return;
}

void test_foreign_submission_escapes_arbitrary_busy_worker() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);
    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    std::atomic<bool> done{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    // Called by this foreign test thread. It has no real affinity target and
    // must therefore remain available to worker 1 rather than being assigned
    // to worker 0's owner-only inbox.
    runtime.go(mark_foreign_submission_ran(scheduler, &done, &resumed_on));

    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = done.load(std::memory_order_acquire);
    CIO_CHECK(escaped);
    if (escaped) {
        CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 1u);
    }
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(done.load(std::memory_order_acquire));
}

cio::Task<int> gated_blocking_job(std::atomic<bool>* started,
                                  std::atomic<bool>* release) {
    co_return co_await cio::blocking([started, release] {
        started->store(true, std::memory_order_release);
        const auto deadline = cio::Clock::now() + 2s;
        while (!release->load(std::memory_order_acquire) &&
               cio::Clock::now() < deadline) {
            std::this_thread::yield();
        }
        return 1;
    });
}

cio::Task<int> observable_blocking_job(std::atomic<bool>* executed) {
    co_return co_await cio::blocking([executed] {
        executed->store(true, std::memory_order_release);
        return 2;
    });
}

cio::Task<bool> exercise_blocking_queue_limit() {
    std::atomic<bool> first_started{false};
    std::atomic<bool> release_first{false};
    std::atomic<bool> second_executed{false};
    std::atomic<bool> rejected_callable_executed{false};

    auto first = cio::spawn(gated_blocking_job(&first_started, &release_first));

    const auto start_deadline = cio::Clock::now() + 1s;
    while (!first_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < start_deadline) {
        co_await cio::yield();
    }
    if (!first_started.load(std::memory_order_acquire)) {
        release_first.store(true, std::memory_order_release);
        (void)co_await first;
        co_return false;
    }

    auto second = cio::spawn(observable_blocking_job(&second_executed));

    // yield() returns behind the newly spawned task, so its blocking job is
    // deterministically queued behind `first` before the third submission.
    co_await cio::yield();

    bool overloaded = false;
    try {
        (void)co_await cio::blocking([&] {
            rejected_callable_executed.store(true, std::memory_order_release);
            return 3;
        });
    } catch (const cio::SystemError& error) {
        overloaded = error.error().is(cio::Errc::overloaded);
    }

    const bool second_was_queued =
        !second_executed.load(std::memory_order_acquire);
    release_first.store(true, std::memory_order_release);
    const int first_value = co_await first;
    const int second_value = co_await second;

    co_return overloaded&& second_was_queued &&
        !rejected_callable_executed.load(std::memory_order_acquire) &&
        first_value == 1 && second_value == 2;
}

void test_blocking_queue_rejects_overload() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    options.max_blocking_threads = 1;
    options.max_blocking_queue = 1;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(exercise_blocking_queue_limit()));
}

struct BlockingRunProbe : cio::detail::BlockingJob {
    std::atomic<bool>* executed = nullptr;
};

void run_blocking_probe(cio::detail::BlockingJob* base) noexcept {
    auto* probe = static_cast<BlockingRunProbe*>(base);
    probe->executed->store(true, std::memory_order_release);
}

bool reject_blocking_thread_launch(cio::detail::BlockingPool*) noexcept {
    return false;
}

void test_blocking_pool_rejects_failed_first_thread_launch() {
    cio::detail::BlockingPool pool(1, 1, &reject_blocking_thread_launch);

    std::atomic<bool> executed{false};
    BlockingRunProbe probe;
    probe.run = &run_blocking_probe;
    probe.executed = &executed;

    const auto result = pool.submit(&probe);
    CIO_CHECK(result == cio::detail::BlockingSubmitResult::overloaded);
    CIO_CHECK_EQ(pool.thread_count(), std::size_t{0});
    CIO_CHECK(!executed.load(std::memory_order_acquire));
}

void test_stopped_blocking_pool_rejects_without_running_inline() {
    cio::detail::BlockingPool pool(1, 1);
    pool.shutdown();

    std::atomic<bool> executed{false};
    BlockingRunProbe probe;
    probe.run = &run_blocking_probe;
    probe.executed = &executed;

    const auto result = pool.submit(&probe);
    CIO_CHECK(result == cio::detail::BlockingSubmitResult::shutdown);
    CIO_CHECK(!executed.load(std::memory_order_acquire));
}

cio::Task<> blocking_completion_waiter(
    cio::detail::Scheduler* scheduler, std::atomic<bool>* job_started,
    std::atomic<bool>* release_job, std::atomic<bool>* completion_done,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    const int value = co_await cio::blocking([job_started, release_job] {
        job_started->store(true, std::memory_order_release);
        while (!release_job->load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        return 42;
    });
    CIO_CHECK_EQ(value, 42);
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    completion_done->store(true, std::memory_order_release);
}

void test_foreign_blocking_completion_escapes_busy_preference() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    std::atomic<bool> job_started{false};
    std::atomic<bool> release_job{false};
    std::atomic<bool> completion_done{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    runtime.go(blocking_completion_waiter(scheduler, &job_started, &release_job,
                                          &completion_done, &resumed_on));

    const auto job_deadline = cio::Clock::now() + 1s;
    while (!job_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < job_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(job_started.load(std::memory_order_acquire));

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);

    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    release_job.store(true, std::memory_order_release);
    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!completion_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = completion_done.load(std::memory_order_acquire);
    CIO_CHECK(escaped);
    if (escaped) {
        CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 1u);
    }
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

cio::Task<int> cross_runtime_join_child(std::atomic<bool>* release) {
    while (!release->load(std::memory_order_acquire)) {
        co_await cio::yield();
    }
    co_return 42;
}

cio::Task<> cross_runtime_join_waiter(
    cio::detail::Scheduler* scheduler, cio::JoinHandle<int> handle,
    std::atomic<bool>* about_to_wait, std::atomic<bool>* completion_done,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    // The worker cannot execute the subsequently enqueued hog until this
    // coroutine reaches await_suspend(), so publishing this flag is enough to
    // let the test arrange a parked joiner deterministically.
    about_to_wait->store(true, std::memory_order_release);
    const int value = co_await handle;
    CIO_CHECK_EQ(value, 42);
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    completion_done->store(true, std::memory_order_release);
}

void test_cross_runtime_join_completion_escapes_busy_preference() {
    cio::RuntimeOptions options_a;
    options_a.worker_threads = 1;
    cio::RuntimeOptions options_b;
    options_b.worker_threads = 2;
    cio::Runtime runtime_a(options_a);
    cio::Runtime runtime_b(options_b);

    std::atomic<bool> release_child{false};
    auto handle = runtime_a.spawn(cross_runtime_join_child(&release_child));

    std::atomic<bool> about_to_wait{false};
    std::atomic<bool> completion_done{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    runtime_b.go(cross_runtime_join_waiter(&runtime_b.scheduler(),
                                           std::move(handle), &about_to_wait,
                                           &completion_done, &resumed_on));

    const auto wait_deadline = cio::Clock::now() + 1s;
    while (!about_to_wait.load(std::memory_order_acquire) &&
           cio::Clock::now() < wait_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(about_to_wait.load(std::memory_order_acquire));

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        &runtime_b.scheduler(),
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);

    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    release_child.store(true, std::memory_order_release);
    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!completion_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = completion_done.load(std::memory_order_acquire);
    CIO_CHECK(escaped);
    if (escaped) {
        CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 1u);
    }
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

cio::Task<> wait_group_completion_waiter(
    cio::detail::Scheduler* scheduler, cio::WaitGroup* group,
    std::atomic<bool>* about_to_wait, std::atomic<bool>* completion_done,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    about_to_wait->store(true, std::memory_order_release);
    co_await group->wait();
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    completion_done->store(true, std::memory_order_release);
}

void test_foreign_wait_group_wake_escapes_busy_preference() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    cio::WaitGroup group;
    group.add();
    std::atomic<bool> about_to_wait{false};
    std::atomic<bool> completion_done{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    runtime.go(wait_group_completion_waiter(scheduler, &group, &about_to_wait,
                                            &completion_done, &resumed_on));

    const auto wait_deadline = cio::Clock::now() + 1s;
    while (!about_to_wait.load(std::memory_order_acquire) &&
           cio::Clock::now() < wait_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(about_to_wait.load(std::memory_order_acquire));

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);
    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    group.done();
    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!completion_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = completion_done.load(std::memory_order_acquire);
    CIO_CHECK(escaped);
    if (escaped) {
        CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 1u);
    }
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

cio::Task<> closed_channel_completion_waiter(
    cio::detail::Scheduler* scheduler, cio::Chan<> channel,
    std::atomic<bool>* about_to_wait, std::atomic<bool>* completion_done,
    std::atomic<cio::detail::WorkerId>* resumed_on) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    about_to_wait->store(true, std::memory_order_release);
    CIO_CHECK(!(co_await channel.recv()));
    resumed_on->store(cio::detail::current_worker_id(scheduler),
                      std::memory_order_release);
    completion_done->store(true, std::memory_order_release);
}

void test_foreign_channel_close_escapes_busy_preference() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    auto channel = cio::make_chan<>();
    std::atomic<bool> about_to_wait{false};
    std::atomic<bool> completion_done{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    runtime.go(closed_channel_completion_waiter(
        scheduler, channel, &about_to_wait, &completion_done, &resumed_on));

    const auto wait_deadline = cio::Clock::now() + 1s;
    while (!about_to_wait.load(std::memory_order_acquire) &&
           cio::Clock::now() < wait_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(about_to_wait.load(std::memory_order_acquire));

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);
    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    channel.close();
    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!completion_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = completion_done.load(std::memory_order_acquire);
    CIO_CHECK(escaped);
    if (escaped) {
        CIO_CHECK_EQ(resumed_on.load(std::memory_order_acquire), 1u);
    }
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

cio::Task<cio::detail::Scheduler::IoCompletionRoute>
inject_io_completion_behind_runnext_hog(cio::detail::Scheduler* scheduler,
                                        void* frame,
                                        std::atomic<bool>* hog_started,
                                        std::atomic<bool>* release_hog,
                                        std::atomic<bool>* hog_finished) {
    co_await SwitchToSchedulerWorker{scheduler, 0};

    auto hog = io_completion_cpu_hog(hog_started, release_hog, hog_finished);
    auto hog_handle = hog.release();
    hog_handle.promise().detached = true;
    scheduler->schedule_next(hog_handle);

    const auto route = scheduler->schedule_io_completion(
        std::coroutine_handle<>::from_address(frame), 1);
    scheduler->finish_io_batch(
        route == cio::detail::Scheduler::IoCompletionRoute::kLocalFifo ? 1u
                                                                       : 0u);
    co_return route;
}

void test_io_completion_behind_runnext_is_published() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    std::atomic<void*> frame{nullptr};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    std::atomic<bool> completion_done{false};
    runtime.go(
        suspend_io_target(scheduler, &frame, &resumed_on, &completion_done));

    const auto publish_deadline = cio::Clock::now() + 1s;
    while (frame.load(std::memory_order_acquire) == nullptr &&
           cio::Clock::now() < publish_deadline) {
        std::this_thread::yield();
    }
    void* const suspended = frame.load(std::memory_order_acquire);
    CIO_CHECK(suspended != nullptr);
    if (suspended == nullptr) return;

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    const auto route = runtime.block_on(inject_io_completion_behind_runnext_hog(
        scheduler, suspended, &hog_started, &release_hog, &hog_finished));
    CIO_CHECK(route == cio::detail::Scheduler::IoCompletionRoute::kLocalFifo);

    const auto escape_deadline = cio::Clock::now() + 250ms;
    while (!completion_done.load(std::memory_order_acquire) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    const bool escaped = completion_done.load(std::memory_order_acquire);
    CIO_CHECK(hog_started.load(std::memory_order_acquire));
    CIO_CHECK(escaped);

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

cio::Task<> inject_singleton_batch_then_hog(cio::detail::Scheduler* scheduler,
                                            void* frame,
                                            std::atomic<bool>* hog_started,
                                            std::atomic<bool>* release_hog,
                                            std::atomic<bool>* hog_finished) {
    co_await SwitchToSchedulerWorker{scheduler, 0};
    void* frames[1] = {frame};
    scheduler->schedule_batch(frames, 1);

    hog_started->store(true, std::memory_order_release);
    while (!release_hog->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    hog_finished->store(true, std::memory_order_release);
}

void test_singleton_generic_batch_is_published() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    std::atomic<void*> frame{nullptr};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
    std::atomic<bool> completion_done{false};
    runtime.go(
        suspend_io_target(scheduler, &frame, &resumed_on, &completion_done));

    const auto publish_deadline = cio::Clock::now() + 1s;
    while (frame.load(std::memory_order_acquire) == nullptr &&
           cio::Clock::now() < publish_deadline) {
        std::this_thread::yield();
    }
    void* const suspended = frame.load(std::memory_order_acquire);
    CIO_CHECK(suspended != nullptr);
    if (suspended == nullptr) return;

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    runtime.go(inject_singleton_batch_then_hog(
        scheduler, suspended, &hog_started, &release_hog, &hog_finished));

    const auto escape_deadline = cio::Clock::now() + 250ms;
    while ((!hog_started.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < escape_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
    CIO_CHECK(!hog_finished.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !completion_done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK(completion_done.load(std::memory_order_acquire));
}

void test_fairness_preselection_publishes_displaced_runnext() {
    // Models the exact state produced when fairness selects a remote CPU hog
    // and the same checkpoint makes an I/O/timer continuation runnable. The
    // selected inbox task may run forever, so every other runnable must be in
    // a queue an idle peer can steal before service_fairness() returns it.
    cio::detail::Scheduler scheduler(2, 1);
    int inbox_frame = 1;
    int completion_frame = 2;
    cio::detail::SchedulerTestAccess::stage_fairness_preselection(
        scheduler, 0, &inbox_frame, &completion_frame);

    CIO_CHECK(cio::detail::SchedulerTestAccess::service_fairness(
                  scheduler, 0) == &inbox_frame);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));

    void* stolen[1] = {nullptr};
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::steal_fifo(scheduler, 0, stolen, 1),
        1u);
    CIO_CHECK(stolen[0] == &completion_frame);
}

void test_fairness_preselection_publishes_private_fifo() {
    cio::detail::Scheduler scheduler(2, 1);
    int inbox_frame = 1;
    int private_frame = 2;
    cio::detail::SchedulerTestAccess::stage_fairness_with_private_fifo(
        scheduler, 0, &inbox_frame, &private_frame);

    CIO_CHECK(cio::detail::SchedulerTestAccess::service_fairness(
                  scheduler, 0) == &inbox_frame);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));

    void* stolen[1] = {nullptr};
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::steal_fifo(scheduler, 0, stolen, 1),
        1u);
    CIO_CHECK(stolen[0] == &private_frame);
}

void test_global_fairness_publishes_bypassed_local_work() {
    cio::detail::Scheduler scheduler(2, 1);
    int global_frame = 1;
    int runnext_frame = 2;
    int fifo_frame = 3;
    cio::detail::SchedulerTestAccess::stage_global_fairness(
        scheduler, 0, &global_frame, &runnext_frame, &fifo_frame);

    CIO_CHECK(cio::detail::SchedulerTestAccess::service_global_fairness(
                  scheduler, 0) == &global_frame);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));

    void* stolen[2] = {nullptr, nullptr};
    std::uint32_t count =
        cio::detail::SchedulerTestAccess::steal_fifo(scheduler, 0, stolen, 2);
    count += cio::detail::SchedulerTestAccess::steal_fifo(
        scheduler, 0, stolen + count, 2 - count);
    CIO_CHECK_EQ(count, 2u);
    CIO_CHECK((stolen[0] == &runnext_frame && stolen[1] == &fifo_frame) ||
              (stolen[0] == &fifo_frame && stolen[1] == &runnext_frame));
}

void test_attach_arms_owner_poll_ticket() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 0);
    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (attached) {
        CIO_CHECK(reactor.take_owner_poll_request_ns() > 0);
        CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 0);
        reactor.detach(*attached);
    }
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_owner_poll_ticket_runs_at_fairness_checkpoint() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }

    const std::int64_t before_poll = reactor.last_poll_ns();
    while (cio::now_ns() <= before_poll) {
        std::this_thread::yield();
    }

    int runnext_frame = 1;
    cio::detail::SchedulerTestAccess::stage_runnext(scheduler, 0,
                                                    &runnext_frame);
    cio::detail::SchedulerTestAccess::set_tick(scheduler, 0, 31);
    CIO_CHECK(cio::detail::SchedulerTestAccess::select_round(scheduler, 0) ==
              &runnext_frame);
    CIO_CHECK(reactor.last_poll_ns() > before_poll);
    CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 0);

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_owner_checkpoint_polls_without_ticket() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }

    CIO_CHECK(reactor.take_owner_poll_request());
    const std::int64_t before_poll = reactor.last_poll_ns();
    while (cio::now_ns() <= before_poll) {
        std::this_thread::yield();
    }

    int runnext_frame = 1;
    cio::detail::SchedulerTestAccess::stage_runnext(scheduler, 0,
                                                    &runnext_frame);
    cio::detail::SchedulerTestAccess::set_tick(scheduler, 0, 31);
    CIO_CHECK(cio::detail::SchedulerTestAccess::select_round(scheduler, 0) ==
              &runnext_frame);
    CIO_CHECK(reactor.last_poll_ns() > before_poll);
    CIO_CHECK_EQ(reactor.owner_poll_request_ns(), 0);

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_stale_monitor_arms_owner_before_foreign_poll() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }
    (void)reactor.take_owner_poll_request_ns();

    const char byte = 'm';
    CIO_CHECK_EQ(::write(pipe_fds[1], &byte, 1), 1);
    const std::int64_t before_poll = reactor.last_poll_ns();
    const std::int64_t stale_now = before_poll + 1'000'000;

    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
    CIO_CHECK(reactor.owner_poll_request_ns() != 0);
    CIO_CHECK_EQ(reactor.last_poll_ns(), before_poll);

    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 100'000);
    CIO_CHECK(reactor.last_poll_ns() > before_poll);
    CIO_CHECK(reactor.owner_poll_request_ns() != 0);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        0u);
    CIO_CHECK(cio::detail::SchedulerTestAccess::global_empty(scheduler));

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_stale_monitor_queues_one_reusable_worker_driver() {
    cio::detail::Scheduler scheduler(2, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }
    (void)reactor.take_owner_poll_request_ns();

    const std::int64_t before_poll = reactor.last_poll_ns();
    const std::int64_t stale_now = before_poll + 1'000'000;

    // The first stale pass gives the shard owner its existing bounded ticket.
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
    CIO_CHECK(reactor.owner_poll_request_ns() != 0);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        0u);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 1);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::pop_global(scheduler),
                 nullptr);

    // The second pass queues one stable control frame globally. Repeated
    // monitor passes before it runs coalesce onto that same generation.
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 50'000);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        1u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_at(scheduler, 0),
        stale_now + 50'000);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 2);

    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 100'000);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        1u);

    void* const first_driver =
        cio::detail::SchedulerTestAccess::pop_global(scheduler);
    CIO_CHECK(first_driver != nullptr);
    if (first_driver != nullptr) {
        std::coroutine_handle<>::from_address(first_driver).resume();
    }
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 1);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        1u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        1u);
    CIO_CHECK(reactor.last_poll_ns() > before_poll);

    // A later stale generation reuses the exact same suspended frame.
    const std::int64_t later_stale = reactor.last_poll_ns() + 1'000'000;
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, later_stale);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        2u);
    void* const second_driver =
        cio::detail::SchedulerTestAccess::pop_global(scheduler);
    CIO_CHECK_EQ(second_driver, first_driver);
    if (second_driver != nullptr) {
        std::coroutine_handle<>::from_address(second_driver).resume();
    }
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        2u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        2u);

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_worker_driver_has_absolute_direct_backstop() {
    cio::detail::Scheduler scheduler(2, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }
    (void)reactor.take_owner_poll_request_ns();

    const std::int64_t before_poll = reactor.last_poll_ns();
    const std::int64_t stale_now = before_poll + 1'000'000;
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 50'000);

    const std::int64_t requested_at =
        cio::detail::SchedulerTestAccess::driver_requested_at(scheduler, 0);
    CIO_CHECK(requested_at != 0);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 2);

    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   requested_at + 199'999);
    CIO_CHECK_EQ(reactor.last_poll_ns(), before_poll);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        0u);

    // A queued control frame cannot postpone liveness indefinitely. At the
    // absolute grace deadline, the monitor competes for polling_ directly.
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   requested_at + 200'000);
    CIO_CHECK(reactor.last_poll_ns() > before_poll);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        1u);
    const std::int64_t after_backstop = reactor.last_poll_ns();

    // The already-queued frame remains safe: it observes coverage, performs a
    // no-op, and returns to its reusable suspended state.
    void* const late_driver =
        cio::detail::SchedulerTestAccess::pop_global(scheduler);
    CIO_CHECK(late_driver != nullptr);
    if (late_driver != nullptr) {
        std::coroutine_handle<>::from_address(late_driver).resume();
    }
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 1);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        1u);
    CIO_CHECK_EQ(reactor.last_poll_ns(), after_backstop);

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_late_worker_driver_adopts_newer_generation() {
    cio::detail::Scheduler scheduler(2, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(pipe_fds[0]);
        ::close(pipe_fds[1]);
        return;
    }
    (void)reactor.take_owner_poll_request_ns();

    const std::int64_t stale_now = reactor.last_poll_ns() + 1'000'000;
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
    cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                   stale_now + 50'000);
    const std::int64_t first_requested_at =
        cio::detail::SchedulerTestAccess::driver_requested_at(scheduler, 0);
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 2);

    // Cover generation 1 through the hard backstop, but leave its one carrier
    // in global_. A later stale episode may advance the generation while that
    // same stable frame is still queued.
    cio::detail::SchedulerTestAccess::monitor_pass(
        scheduler, first_requested_at + 200'000);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        1u);
    const bool requested_second =
        cio::detail::SchedulerTestAccess::request_driver(
            scheduler, 0, reactor.last_poll_ns() + 1);
    CIO_CHECK(requested_second);
    CIO_CHECK(!cio::detail::SchedulerTestAccess::queue_driver(scheduler, 0));

    void* const only_driver =
        cio::detail::SchedulerTestAccess::pop_global(scheduler);
    CIO_CHECK(only_driver != nullptr);
    CIO_CHECK(cio::detail::SchedulerTestAccess::global_empty(scheduler));
    if (only_driver != nullptr) {
        std::coroutine_handle<>::from_address(only_driver).resume();
    }

    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0),
                 1);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_requested_epoch(scheduler, 0),
        2u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        2u);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        2u);

    reactor.detach(*attached);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_queued_worker_driver_is_owned_through_shutdown() {
    cio::detail::Scheduler scheduler(2, 1);
    auto& reactor = scheduler.reactor_for(0);
    int pipe_fds[2] = {-1, -1};
    CIO_CHECK_EQ(::pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK), 0);
    if (pipe_fds[0] < 0) return;

    auto attached = reactor.attach(pipe_fds[0]);
    CIO_CHECK(attached.has_value());
    if (attached) {
        (void)reactor.take_owner_poll_request_ns();
        const std::int64_t stale_now = reactor.last_poll_ns() + 1'000'000;
        cio::detail::SchedulerTestAccess::monitor_pass(scheduler, stale_now);
        cio::detail::SchedulerTestAccess::monitor_pass(scheduler,
                                                       stale_now + 50'000);
        CIO_CHECK_EQ(
            cio::detail::SchedulerTestAccess::driver_phase(scheduler, 0), 2);
        CIO_CHECK(!cio::detail::SchedulerTestAccess::global_empty(scheduler));
        // Deliberately leave the frame in global_. Reactor owns it and destroys
        // it after the raw queue has lost all consumers during teardown.
        reactor.detach(*attached);
    }
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

void test_reusable_worker_driver_rearm_race() {
    constexpr std::uint64_t kIterations = 2'000;

    cio::detail::Scheduler scheduler(2, 1);
    cio::detail::SchedulerTestAccess::start_workers_without_monitor(scheduler);
    auto& reactor = scheduler.reactor_for(0);

    // Keep shard 0's owner out of its blocking reactor poll. Every queued
    // control frame is then consumed by worker 1, making rapid
    // SUSPENDED->QUEUED rearm deterministic instead of depending on which idle
    // worker happened to leave park first.
    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    schedule_detached_to(
        &scheduler,
        io_completion_cpu_hog(&hog_started, &release_hog, &hog_finished), 0);
    const auto setup_deadline = cio::Clock::now() + 2s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < setup_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    const std::int64_t requested_base = reactor.last_poll_ns() + 60'000'000'000;
    const auto deadline = cio::Clock::now() + 5s;

    std::uint64_t completed = 0;
    for (std::uint64_t i = 0; i < kIterations; ++i) {
        const bool requested = cio::detail::SchedulerTestAccess::request_driver(
            scheduler, 0, requested_base + static_cast<std::int64_t>(i));
        bool queued = false;
        while (requested && !queued && cio::Clock::now() < deadline) {
            queued =
                cio::detail::SchedulerTestAccess::queue_driver(scheduler, 0);
            if (!queued) std::this_thread::yield();
        }
        if (!queued) break;

        const std::uint64_t epoch = i + 1;
        // Coverage is published inside run_driver_once(), before
        // DriverSuspend publishes SUSPENDED. Start the next generation as soon
        // as coverage appears so queue_driver's CAS races that final release
        // instead of serializing every reuse after suspension.
        while (cio::detail::SchedulerTestAccess::driver_covered_epoch(
                   scheduler, 0) < epoch &&
               cio::Clock::now() < deadline) {
            std::this_thread::yield();
        }
        if (cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler,
                                                                   0) < epoch) {
            break;
        }
        completed = epoch;
    }

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 2s;
    while (!hog_finished.load(std::memory_order_acquire) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }
    scheduler.shutdown();
    CIO_CHECK(hog_finished.load(std::memory_order_acquire));
    CIO_CHECK_EQ(completed, kIterations);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_attempted_epoch(scheduler, 0),
        kIterations);
    CIO_CHECK_EQ(
        cio::detail::SchedulerTestAccess::driver_covered_epoch(scheduler, 0),
        kIterations);
}

void test_monitor_batch_only_replaces_default_policy() {
    CIO_CHECK(cio::detail::SchedulerTestAccess::should_use_batch_monitor_policy(
        SCHED_OTHER));
    CIO_CHECK(
        !cio::detail::SchedulerTestAccess::should_use_batch_monitor_policy(
            SCHED_FIFO));
    CIO_CHECK(
        !cio::detail::SchedulerTestAccess::should_use_batch_monitor_policy(
            SCHED_RR));
#if defined(SCHED_BATCH)
    CIO_CHECK(
        !cio::detail::SchedulerTestAccess::should_use_batch_monitor_policy(
            SCHED_BATCH));
#endif
#if defined(SCHED_IDLE)
    CIO_CHECK(
        !cio::detail::SchedulerTestAccess::should_use_batch_monitor_policy(
            SCHED_IDLE));
#endif
}

void test_owner_poll_ticket_timestamp_coalesces() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    CIO_CHECK(reactor.request_owner_poll_at(100));
    CIO_CHECK(!reactor.request_owner_poll_at(200));
    CIO_CHECK_EQ(reactor.owner_poll_request_ns(), 100);
    CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 100);
    CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 0);
}

void test_reactor_poll_does_not_consume_owner_ticket() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    CIO_CHECK(reactor.request_owner_poll_at(100));
    CIO_CHECK_EQ(reactor.poll(0), 0);
    CIO_CHECK_EQ(reactor.take_owner_poll_request_ns(), 100);
}

void test_completed_poll_refreshes_last_poll_after_wait() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);
    std::atomic<int> poll_result{-2};
    std::thread poller([&] {
        poll_result.store(reactor.poll(cio::to_ns(1s)),
                          std::memory_order_release);
    });

    const auto polling_deadline = cio::Clock::now() + 1s;
    while (!reactor.polling() && cio::Clock::now() < polling_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(reactor.polling());
    std::this_thread::sleep_for(1ms);
    const std::int64_t wake_ns = cio::now_ns();
    reactor.wake();
    poller.join();

    CIO_CHECK(poll_result.load(std::memory_order_acquire) >= 0);
    CIO_CHECK(reactor.last_poll_ns() >= wake_ns);
}

void test_foreign_timer_batch_uses_shared_fallback() {
    // run_expired() is also called by the foreign monitor thread. Its target
    // worker is only a soft affinity hint: an owner-only inbox would strand
    // this already-due waiter if that worker entered a non-suspending task.
    cio::detail::Scheduler scheduler(2, 1);
    const auto waiter = std::noop_coroutine();
    cio::detail::Timer timer{cio::detail::Timer::ArmTag{}, cio::now_ns() - 1,
                             waiter, nullptr};
    scheduler.timers().arm(&timer);

    CIO_CHECK_EQ(scheduler.timers().run_expired(0), 1u);
    CIO_CHECK(cio::detail::SchedulerTestAccess::inbox_empty(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_global(scheduler) ==
              waiter.address());
}

void test_foreign_direct_handoff_uses_shared_fallback() {
    // schedule_next is a true runnext handoff only while the waker is already
    // executing on this scheduler. Cross-runtime/foreign callers must not
    // choose an owner-only inbox that a CPU-bound owner can strand.
    cio::detail::Scheduler scheduler(2, 1);
    const auto waiter = std::noop_coroutine();
    scheduler.schedule_next(waiter);

    CIO_CHECK(cio::detail::SchedulerTestAccess::inbox_empty(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::inbox_empty(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_global(scheduler) ==
              waiter.address());

    scheduler.schedule_to(waiter, cio::detail::kInvalidWorkerId);
    CIO_CHECK(cio::detail::SchedulerTestAccess::inbox_empty(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::inbox_empty(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_global(scheduler) ==
              waiter.address());
}

// The real test of the stealing/parking protocol: many more tasks than
// workers, spawned from many workers, all of which must run exactly once.
void test_many_tasks_across_workers() {
    static constexpr int kFanOut = 64;
    static constexpr int kPerFanOut = 500;
    static std::atomic<int> counter{0};
    counter.store(0);

    auto leaf = []() -> cio::Task<> {
        counter.fetch_add(1, std::memory_order_relaxed);
        co_return;
    };
    auto branch = [&]() -> cio::Task<> {
        std::vector<cio::JoinHandle<>> handles;
        handles.reserve(kPerFanOut);
        for (int i = 0; i < kPerFanOut; ++i)
            handles.push_back(cio::spawn(leaf()));
        for (auto& handle : handles) co_await handle;
    };
    auto root = [&]() -> cio::Task<> {
        std::vector<cio::JoinHandle<>> handles;
        handles.reserve(kFanOut);
        for (int i = 0; i < kFanOut; ++i)
            handles.push_back(cio::spawn(branch()));
        for (auto& handle : handles) co_await handle;
    };

    cio::run(root());
    CIO_CHECK_EQ(counter.load(), kFanOut * kPerFanOut);
}

// Repeatedly force the stealable FIFO through empty -> one item -> empty.
// The root never suspends while waiting, so its own worker cannot run the
// child: progress requires the peer to observe the publication and steal it.
// A missed clear/publication handshake therefore fails instead of being
// masked by the owner eventually draining its queue.
void test_single_item_publication_survives_clear_race() {
#if defined(__SANITIZE_THREAD__)
    static constexpr int kRounds = 500;
#else
    static constexpr int kRounds = 10'000;
#endif

    auto root = []() -> cio::Task<bool> {
        for (int i = 0; i < kRounds; ++i) {
            auto child = cio::spawn([]() -> cio::Task<> { co_return; }());
            const auto deadline = cio::Clock::now() + 1s;
            while (!child.done() && cio::Clock::now() < deadline) {
                std::this_thread::yield();
            }

            const bool stolen = child.done();
            // If publication was lost, suspending here lets the owner drain
            // and destroy the child cleanly before the test reports failure.
            co_await child;
            if (!stolen) co_return false;
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 2;
    CIO_CHECK(cio::run(root(), options));
}

// A thief cannot update the victim owner's owner-only publication flag. Verify
// that a successful foreign clear advances clear_epoch and that the next owner
// publication restores the shared victim bit despite that stale local flag.
void test_publication_observes_foreign_clear_epoch() {
    cio::detail::Scheduler scheduler(2, 1);
    int first = 1;
    int second = 2;

    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &first);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_fifo(scheduler, 0) ==
              &first);

    const std::uint64_t before =
        cio::detail::SchedulerTestAccess::clear_epoch(scheduler, 0);
    CIO_CHECK(
        !cio::detail::SchedulerTestAccess::repair_fifo_as_thief(scheduler, 0));
    CIO_CHECK(!cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK_EQ(cio::detail::SchedulerTestAccess::clear_epoch(scheduler, 0),
                 before + 1);

    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &second);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_fifo(scheduler, 0) ==
              &second);
}

// A victim-publication wake is not a generic eventfd token. If targeted inbox
// work arrives after that wake claimed its worker, the worker must search the
// published victim before the inbox task can intercept the only searcher.
void test_searcher_credit_precedes_intercepting_inbox() {
    cio::detail::Scheduler scheduler(3, 1);
    int victim = 1;
    int intercept = 2;

    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 1);
    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 2);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &victim);

    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(!cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 2));

    scheduler.schedule_to_frame(&intercept, 1);
    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 1) == &victim);
    CIO_CHECK(!cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_inbox(scheduler, 1) ==
              &intercept);
}

// Stealing half of a two-item FIFO takes one item and leaves one. Because the
// victim bit stays set, there is no second publication transition to wake a
// peer; the successful thief must transfer its search entitlement before it
// runs a potentially non-suspending stolen task.
void test_singleton_steal_transfers_remaining_search() {
    cio::detail::Scheduler scheduler(3, 1);
    int first = 1;
    int second = 2;

    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 1);
    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 2);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &first);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &second);

    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(!cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 2));

    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 1) == &first);
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 2));
    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 2) == &second);
    CIO_CHECK(!cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
}

// A steal from three items creates two published sources: one retained item on
// the thief and one tail item on the original victim. The retained-batch wake
// cannot cover both, so two other idle workers must receive search credits.
void test_batch_steal_accounts_for_original_victim_tail() {
    cio::detail::Scheduler scheduler(4, 1);
    int first = 1;
    int second = 2;
    int third = 3;

    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 1);
    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 2);
    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 3);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &first);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &second);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &third);

    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 1) == &first);

    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK(cio::detail::SchedulerTestAccess::stealable(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 2));
    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 3));

    void* const next =
        cio::detail::SchedulerTestAccess::consume_searcher_credit(scheduler, 2);
    void* const last =
        cio::detail::SchedulerTestAccess::consume_searcher_credit(scheduler, 3);
    CIO_CHECK((next == &second && last == &third) ||
              (next == &third && last == &second));
    CIO_CHECK(!cio::detail::SchedulerTestAccess::stealable(scheduler, 0));
    CIO_CHECK(!cio::detail::SchedulerTestAccess::stealable(scheduler, 1));
}

void test_park_final_check_adopts_searcher_credit() {
    cio::detail::Scheduler scheduler(2, 1);
    int victim = 1;
    int intercept = 2;

    // Publish with no idle worker, so no producer can pre-arm a credit.
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &victim);
    CIO_CHECK(!cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    scheduler.schedule_to_frame(&intercept, 1);

    // W1 publishes idle, sees its inbox in the final check, and leaves park.
    // Its clear-then-SC-victim check must adopt the otherwise-unclaimed search.
    cio::detail::SchedulerTestAccess::park_once(scheduler, 1);
    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 1) == &victim);
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_inbox(scheduler, 1) ==
              &intercept);
}

void test_poll_return_leave_adopts_searcher_credit() {
    cio::detail::Scheduler scheduler(2, 1);
    int victim = 1;
    int intercept = 2;

    // Model poller_returned(): the worker's idle bit is already clear before
    // the readiness batch finishes and park executes its common leave step.
    cio::detail::SchedulerTestAccess::publish_idle(scheduler, 1);
    cio::detail::SchedulerTestAccess::clear_idle(scheduler, 1);
    cio::detail::SchedulerTestAccess::publish_fifo(scheduler, 0, &victim);
    CIO_CHECK(!cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    scheduler.schedule_to_frame(&intercept, 1);

    cio::detail::SchedulerTestAccess::leave_park(scheduler, 1);
    CIO_CHECK(cio::detail::SchedulerTestAccess::searcher_credit(scheduler, 1));
    CIO_CHECK(cio::detail::SchedulerTestAccess::consume_searcher_credit(
                  scheduler, 1) == &victim);
    CIO_CHECK(cio::detail::SchedulerTestAccess::pop_inbox(scheduler, 1) ==
              &intercept);
}

cio::Task<> occupy_only_worker(std::atomic<bool>* started,
                               std::atomic<bool>* release) {
    started->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
        // Deliberately do not suspend the coroutine: foreign producers must
        // fill the targeted inbox and exercise its global overflow path.
        std::this_thread::yield();
    }
    co_return;
}

cio::Task<> mark_external_submission(std::atomic<bool>* seen,
                                     std::atomic<int>* completed,
                                     std::atomic<int>* duplicates) {
    if (seen->exchange(true, std::memory_order_acq_rel)) {
        duplicates->fetch_add(1, std::memory_order_relaxed);
    }
    completed->fetch_add(1, std::memory_order_release);
    co_return;
}

void test_foreign_mpsc_burst_overflows_without_loss() {
    constexpr int kProducers = 8;
    constexpr int kPerProducer = 512;
    constexpr int kTotal = kProducers * kPerProducer;

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);

    std::atomic<bool> started{false};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> duplicates{0};
    auto seen = std::make_unique<std::atomic<bool>[]>(kTotal);
    for (int i = 0; i < kTotal; ++i) {
        seen[i].store(false, std::memory_order_relaxed);
    }

    runtime.go(occupy_only_worker(&started, &release));
    while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            const int base = producer * kPerProducer;
            for (int i = 0; i < kPerProducer; ++i) {
                schedule_detached_to(
                    &runtime.scheduler(),
                    mark_external_submission(&seen[base + i], &completed,
                                             &duplicates),
                    0);
            }
        });
    }
    for (auto& producer : producers) producer.join();

    release.store(true, std::memory_order_release);
    const bool drained = runtime.block_on([&]() -> cio::Task<bool> {
        const auto deadline = cio::Clock::now() + 5s;
        while (completed.load(std::memory_order_acquire) != kTotal &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        co_return completed.load(std::memory_order_acquire) == kTotal;
    }());

    CIO_CHECK(drained);
    CIO_CHECK_EQ(completed.load(std::memory_order_acquire), kTotal);
    CIO_CHECK_EQ(duplicates.load(std::memory_order_acquire), 0);
    for (int i = 0; i < kTotal; ++i) {
        CIO_CHECK(seen[i].load(std::memory_order_acquire));
    }
}

void test_yield_round_trips() {
    auto body = []() -> cio::Task<int> {
        for (int i = 0; i < 10000; ++i) co_await cio::yield();
        co_return 7;
    };
    CIO_CHECK_EQ(cio::run(body()), 7);
}

void test_detached_task_runs() {
    static std::atomic<int> seen{0};
    seen.store(0);

    auto body = []() -> cio::Task<> {
        for (int i = 0; i < 100; ++i) {
            cio::go([]() -> cio::Task<> {
                seen.fetch_add(1, std::memory_order_relaxed);
                co_return;
            }());
        }
        // Detached tasks have no join point, so drain by polling — this is
        // exactly why TaskGroup exists.
        while (seen.load(std::memory_order_relaxed) < 100)
            co_await cio::sleep(1ms);
    };

    cio::run(body());
    CIO_CHECK_EQ(seen.load(), 100);
}

void test_join_completion_ignores_destroyed_target_runtime() {
    auto* state = new cio::detail::JoinState<int>(1);
    std::unique_ptr<cio::detail::JoinWait> waiter;

    {
        cio::RuntimeOptions options;
        options.worker_threads = 1;
        cio::Runtime target(options);
        waiter = std::make_unique<cio::detail::JoinWait>(std::noop_coroutine(),
                                                         &target.scheduler());
        const auto parked = state->try_park(waiter.get());
        CIO_CHECK(parked == cio::detail::JoinState<int>::ParkResult::kParked);
    }
    CIO_CHECK(!waiter->sched.lock());

    // Completion may originate on another runtime or an arbitrary thread long
    // after the awaiting runtime was torn down. It must publish the result
    // without dereferencing the dead target Scheduler.
    state->set_value(42);
    CIO_CHECK_EQ(state->take(), 42);
    state->release();
}

void test_completion_endpoint_identity_is_never_recycled() {
    cio::detail::SchedulerTarget stale;
    {
        cio::RuntimeOptions options;
        options.worker_threads = 1;
        cio::Runtime first(options);
        stale = first.scheduler().completion_target();
        auto live = stale.lock();
        CIO_CHECK(static_cast<bool>(live));
    }
    CIO_CHECK(!stale.lock());

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime replacement(options);
    const auto current = replacement.scheduler().completion_target();
    CIO_CHECK(current.endpoint != stale.endpoint);
    CIO_CHECK(!stale.lock());
    CIO_CHECK(static_cast<bool>(current.lock()));
}

void test_completion_endpoint_shutdown_waits_out_foreign_leases() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    auto runtime = std::make_unique<cio::Runtime>(options);
    const auto target = runtime->scheduler().completion_target();

    std::atomic<bool> started{false};
    std::atomic<bool> stop{false};
    std::atomic<int> acquired{0};
    std::thread waker([&] {
        started.store(true, std::memory_order_release);
        while (!stop.load(std::memory_order_acquire)) {
            auto lease = target.lock();
            if (lease) {
                (void)lease->worker_count();
                acquired.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    const auto acquisition_deadline = cio::Clock::now() + 1s;
    while (acquired.load(std::memory_order_acquire) == 0 &&
           cio::Clock::now() < acquisition_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(acquired.load(std::memory_order_acquire) > 0);

    runtime.reset();
    stop.store(true, std::memory_order_release);
    waker.join();
    CIO_CHECK(!target.lock());
}

// Task, JoinHandle and go() are all publicly reachable with nothing behind
// them; each used to dereference the null and crash.
void test_invalid_async_handles_report_errors() {
    auto body = []() -> cio::Task<bool> {
        bool task_rejected = false;
        try {
            cio::Task<int> invalid_task;
            (void)co_await invalid_task;
        } catch (const std::logic_error&) {
            task_rejected = true;
        }

        bool join_rejected = false;
        try {
            cio::JoinHandle<int> invalid_join;
            (void)co_await invalid_join;
        } catch (const std::logic_error&) {
            join_rejected = true;
        }

        bool go_rejected = false;
        try {
            cio::go(cio::Task<>{});
        } catch (const std::invalid_argument&) {
            go_rejected = true;
        }

        co_return task_rejected&& join_rejected&& go_rejected;
    };
    CIO_CHECK(cio::run(body()));
}

void test_shutdown_from_own_worker_is_rejected() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);

    const bool rejected = runtime.block_on([&runtime]() -> cio::Task<bool> {
        try {
            runtime.shutdown();
        } catch (const std::logic_error&) {
            co_return true;
        }
        co_return false;
    }());
    CIO_CHECK(rejected);

    // The failed call must not half-close the scheduler; an external caller
    // can still use it and perform the real shutdown.
    CIO_CHECK_EQ(runtime.block_on([]() -> cio::Task<int> { co_return 42; }()),
                 42);
    runtime.shutdown();
}

cio::Task<> graceful_shutdown_root(cio::Runtime* runtime,
                                   cio::CancelToken shutdown,
                                   std::atomic<bool>* started,
                                   std::atomic<bool>* child_finished,
                                   std::atomic<bool>* root_finished) {
    started->store(true, std::memory_order_release);
    (void)co_await shutdown.done().recv();

    // Admission is already closed to foreign callers, but a live root may
    // still submit cleanup work from its own worker. The root deliberately
    // does not join it: graceful shutdown must track both submissions.
    runtime->go([](std::atomic<bool>* finished) -> cio::Task<> {
        co_await cio::sleep(2ms);
        finished->store(true, std::memory_order_release);
    }(child_finished));
    root_finished->store(true, std::memory_order_release);
}

void test_graceful_shutdown_cancels_and_joins_runtime_roots() {
    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);

    std::atomic<bool> started{false};
    std::atomic<bool> child_finished{false};
    std::atomic<bool> root_finished{false};
    runtime.go(graceful_shutdown_root(&runtime, runtime.shutdown_token(),
                                      &started, &child_finished,
                                      &root_finished));

    const auto deadline = cio::Clock::now() + 2s;
    while (!started.load(std::memory_order_acquire) &&
           cio::Clock::now() < deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(started.load(std::memory_order_acquire));

    runtime.graceful_shutdown();
    CIO_CHECK(child_finished.load(std::memory_order_acquire));
    CIO_CHECK(root_finished.load(std::memory_order_acquire));

    bool rejected = false;
    try {
        runtime.go([]() -> cio::Task<> { co_return; }());
    } catch (const cio::SystemError& error) {
        rejected = error.error().is(cio::Errc::shutdown);
    }
    CIO_CHECK(rejected);

    // Both graceful shutdown and a graceful call after immediate shutdown are
    // idempotent and must not wait on work an immediate stop abandoned.
    runtime.graceful_shutdown();

    cio::Runtime stopped(options);
    stopped.shutdown();
    stopped.graceful_shutdown();
}

void test_graceful_shutdown_from_own_worker_is_rejected() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime(options);
    const cio::CancelToken shutdown = runtime.shutdown_token();

    const bool rejected = runtime.block_on([&runtime]() -> cio::Task<bool> {
        try {
            runtime.graceful_shutdown();
        } catch (const std::logic_error&) {
            co_return true;
        }
        co_return false;
    }());
    CIO_CHECK(rejected);
    CIO_CHECK(!shutdown.cancelled());
    CIO_CHECK_EQ(runtime.block_on([]() -> cio::Task<int> { co_return 7; }()),
                 7);

    runtime.graceful_shutdown();
    CIO_CHECK(shutdown.cancelled());
}

}  // namespace

int main() {
    RUN_TEST(test_child_task_composition);
    RUN_TEST(test_exception_propagates_through_await);
    RUN_TEST(test_spawn_and_join);
    RUN_TEST(test_cross_runtime_join_resumes_on_awaiting_runtime);
    RUN_TEST(test_join_handle_can_detach_before_completion);
    RUN_TEST(test_spawn_propagates_child_exception);
    RUN_TEST(test_spawn_preserves_invalid_and_completed_task_semantics);
    RUN_TEST(test_spawn_captures_result_move_failure);
    RUN_TEST(test_completed_join_handle_can_be_awaited_again);
    RUN_TEST(test_concurrent_join_waiter_is_rejected);
    RUN_TEST(test_saved_join_awaiter_survives_handle_detach);
    RUN_TEST(test_completed_void_join_snapshot_survives_detach);
    RUN_TEST(test_completed_void_exception_snapshot_keeps_state);
    RUN_TEST(test_spawn_join_handoff_respects_local_batch);
    RUN_TEST(test_runnext_handoff_does_not_starve_local_fifo);
    RUN_TEST(test_runnext_handoff_does_not_starve_remote_inbox);
    RUN_TEST(test_foreign_poller_preserves_directed_wake_for_owner);
    RUN_TEST(test_same_runtime_io_completion_follows_active_poller);
    RUN_TEST(test_worker_driver_polls_and_places_completion_locally);
    RUN_TEST(test_foreign_submission_escapes_arbitrary_busy_worker);
    RUN_TEST(test_blocking_queue_rejects_overload);
    RUN_TEST(test_blocking_pool_rejects_failed_first_thread_launch);
    RUN_TEST(test_stopped_blocking_pool_rejects_without_running_inline);
    RUN_TEST(test_foreign_blocking_completion_escapes_busy_preference);
    RUN_TEST(test_cross_runtime_join_completion_escapes_busy_preference);
    RUN_TEST(test_foreign_wait_group_wake_escapes_busy_preference);
    RUN_TEST(test_foreign_channel_close_escapes_busy_preference);
    RUN_TEST(test_cooperative_io_no_demand_renews_inline);
    RUN_TEST(test_cooperative_io_local_demand_gets_one_grace_budget);
    RUN_TEST(test_cooperative_io_local_hog_publishes_parent);
    RUN_TEST(test_cooperative_io_stages_hard_debt_before_self);
    RUN_TEST(test_cooperative_io_nonlocal_stage_publishes_self);
    RUN_TEST(test_cooperative_io_empty_ticket_poll_renews_inline);
    RUN_TEST(test_cooperative_io_productive_ticket_yields);
    RUN_TEST(test_cooperative_io_top_level_resume_resets_budget);
    RUN_TEST(test_io_completion_behind_runnext_is_published);
    RUN_TEST(test_singleton_generic_batch_is_published);
    RUN_TEST(test_fairness_preselection_publishes_displaced_runnext);
    RUN_TEST(test_fairness_preselection_publishes_private_fifo);
    RUN_TEST(test_global_fairness_publishes_bypassed_local_work);
    RUN_TEST(test_attach_arms_owner_poll_ticket);
    RUN_TEST(test_owner_poll_ticket_runs_at_fairness_checkpoint);
    RUN_TEST(test_owner_checkpoint_polls_without_ticket);
    RUN_TEST(test_stale_monitor_arms_owner_before_foreign_poll);
    RUN_TEST(test_stale_monitor_queues_one_reusable_worker_driver);
    RUN_TEST(test_worker_driver_has_absolute_direct_backstop);
    RUN_TEST(test_late_worker_driver_adopts_newer_generation);
    RUN_TEST(test_queued_worker_driver_is_owned_through_shutdown);
    RUN_TEST(test_reusable_worker_driver_rearm_race);
    RUN_TEST(test_monitor_batch_only_replaces_default_policy);
    RUN_TEST(test_owner_poll_ticket_timestamp_coalesces);
    RUN_TEST(test_reactor_poll_does_not_consume_owner_ticket);
    RUN_TEST(test_completed_poll_refreshes_last_poll_after_wait);
    RUN_TEST(test_foreign_timer_batch_uses_shared_fallback);
    RUN_TEST(test_foreign_direct_handoff_uses_shared_fallback);
    RUN_TEST(test_many_tasks_across_workers);
    RUN_TEST(test_single_item_publication_survives_clear_race);
    RUN_TEST(test_publication_observes_foreign_clear_epoch);
    RUN_TEST(test_searcher_credit_precedes_intercepting_inbox);
    RUN_TEST(test_singleton_steal_transfers_remaining_search);
    RUN_TEST(test_batch_steal_accounts_for_original_victim_tail);
    RUN_TEST(test_park_final_check_adopts_searcher_credit);
    RUN_TEST(test_poll_return_leave_adopts_searcher_credit);
    RUN_TEST(test_foreign_mpsc_burst_overflows_without_loss);
    RUN_TEST(test_yield_round_trips);
    RUN_TEST(test_detached_task_runs);
    RUN_TEST(test_join_completion_ignores_destroyed_target_runtime);
    RUN_TEST(test_completion_endpoint_identity_is_never_recycled);
    RUN_TEST(test_completion_endpoint_shutdown_waits_out_foreign_leases);
    RUN_TEST(test_invalid_async_handles_report_errors);
    RUN_TEST(test_shutdown_from_own_worker_is_rejected);
    RUN_TEST(test_graceful_shutdown_cancels_and_joins_runtime_roots);
    RUN_TEST(test_graceful_shutdown_from_own_worker_is_rejected);
    return cio_test::summary();
}
