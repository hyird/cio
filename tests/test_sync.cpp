#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

void test_waitgroup() {
    static std::atomic<int> done{0};
    done.store(0);

    auto body = []() -> cio::Task<int> {
        cio::WaitGroup group;
        group.add(500);
        for (int i = 0; i < 500; ++i) {
            cio::go([](cio::WaitGroup& wg) -> cio::Task<> {
                co_await cio::sleep(std::chrono::microseconds(100));
                done.fetch_add(1, std::memory_order_relaxed);
                wg.done();
            }(group));
        }
        co_await group.wait();
        co_return done.load(std::memory_order_relaxed);
    };
    CIO_CHECK_EQ(cio::run(body()), 500);
}

void test_waitgroup_single_waiter_gets_zero_handoff() {
    auto body = []() -> cio::Task<bool> {
        std::vector<int> order;
        cio::WaitGroup group;
        group.add(1);

        cio::go([](cio::WaitGroup* target,
                   std::vector<int>* observed) -> cio::Task<> {
            observed->push_back(1);
            target->done();
            co_return;
        }(&group, &order));
        auto later = cio::spawn([](std::vector<int>* observed) -> cio::Task<> {
            observed->push_back(3);
            co_return;
        }(&order));

        co_await group.wait();
        order.push_back(2);
        co_await later;
        co_return order == std::vector<int>({1, 2, 3});
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

void test_waitgroup_reuse_waits_for_zero_transition() {
    auto body = []() -> cio::Task<bool> {
        for (int round = 0; round < 50'000; ++round) {
            cio::WaitGroup group;
            group.add(1);
            cio::go([](cio::WaitGroup* target) -> cio::Task<> {
                target->done();
                co_return;
            }(&group));
            co_await group.wait();
        }

        // Reuse one object across generations as Go's WaitGroup permits. On
        // alternating rounds let done() finish first, leaving no real waiter
        // in that generation's list; the prior generation's drained marker
        // must never be dispatched as though it were a coroutine.
        cio::WaitGroup reused;
        for (int round = 0; round < 20'000; ++round) {
            reused.add(1);
            cio::go([](cio::WaitGroup* target) -> cio::Task<> {
                target->done();
                co_return;
            }(&reused));
            if ((round & 1) == 0) co_await cio::yield();
            co_await reused.wait();
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 8;
    CIO_CHECK(cio::run(body(), options));
}

// If the mutex were not actually exclusive, the non-atomic counter would lose
// increments across 24 workers.
void test_async_mutex_is_exclusive() {
    static constexpr int kTasks = 64;
    static constexpr int kIncrements = 500;

    auto body = []() -> cio::Task<long> {
        cio::Mutex mutex;
        long counter = 0;

        cio::TaskGroup group;
        for (int i = 0; i < kTasks; ++i) {
            group.spawn([](cio::Mutex& m, long& value) -> cio::Task<> {
                for (int k = 0; k < kIncrements; ++k) {
                    auto guard = co_await m.lock();
                    const long observed = value;
                    co_await cio::yield();  // force a suspension inside the
                                            // section
                    value = observed + 1;
                }
            }(mutex, counter));
        }
        co_await group.join();
        co_return counter;
    };
    CIO_CHECK_EQ(cio::run(body()), static_cast<long>(kTasks) * kIncrements);
}

void test_async_mutex_try_lock_and_guard_release() {
    auto body = []() -> cio::Task<bool> {
        cio::Mutex mutex;
        CIO_CHECK(mutex.try_lock());
        CIO_CHECK(!mutex.try_lock());
        mutex.unlock();

        auto guard = co_await mutex.lock();
        CIO_CHECK(!mutex.try_lock());
        guard.release();
        CIO_CHECK(mutex.try_lock());
        mutex.unlock();
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_mutex_unlock_races_waiter_publication() {
    auto body = []() -> cio::Task<bool> {
        for (int round = 0; round < 20'000; ++round) {
            cio::Mutex mutex;
            auto owner = co_await mutex.lock();
            auto ready = cio::make_chan(1);
            std::atomic<int> inside{1};
            std::atomic<bool> overlap{false};

            auto waiter =
                cio::spawn([](cio::Mutex& target, cio::Chan<cio::Unit> started,
                              std::atomic<int>& active,
                              std::atomic<bool>& raced) -> cio::Task<bool> {
                    co_await started.send(cio::Unit{});
                    auto guard = co_await target.lock();
                    if (active.fetch_add(1, std::memory_order_relaxed) != 0) {
                        raced.store(true, std::memory_order_relaxed);
                    }
                    co_await cio::yield();
                    active.fetch_sub(1, std::memory_order_relaxed);
                    co_return true;
                }(mutex, ready, inside, overlap));

            // The buffered signal lets the waiter continue into lock() while
            // this task resumes on another worker. Releasing here races the
            // waiter's intent publication and queue insertion; either side may
            // win, but the waiter must never be stranded between them.
            (void)co_await ready.recv();
            inside.fetch_sub(1, std::memory_order_relaxed);
            owner.release();

            // A try_lock racing the transient contended-only state must not
            // treat it as unlocked; that state belongs to the first waiter
            // while it holds the queue lock and claims ownership.
            for (int attempt = 0; attempt < 2; ++attempt) {
                if (mutex.try_lock()) {
                    if (inside.fetch_add(1, std::memory_order_relaxed) != 0) {
                        overlap.store(true, std::memory_order_relaxed);
                    }
                    co_await cio::yield();
                    inside.fetch_sub(1, std::memory_order_relaxed);
                    mutex.unlock();
                }
                co_await cio::yield();
            }
            CIO_CHECK(co_await waiter);
            CIO_CHECK(!overlap.load(std::memory_order_relaxed));

            // Once the handoff queue drains, the mutex must return to its
            // ordinary lock-free state rather than retaining a sticky
            // contended mode.
            CIO_CHECK(mutex.try_lock());
            mutex.unlock();
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 8;
    CIO_CHECK(cio::run(body(), options));
}

void test_taskgroup_joins_every_child() {
    static std::atomic<int> finished{0};
    finished.store(0);

    auto body = []() -> cio::Task<int> {
        cio::TaskGroup group;
        for (int i = 0; i < 200; ++i) {
            group.spawn([]() -> cio::Task<> {
                co_await cio::sleep(std::chrono::microseconds(200));
                finished.fetch_add(1, std::memory_order_relaxed);
            }());
        }
        co_await group.join();
        // join() must not return before every child has finished.
        co_return finished.load(std::memory_order_relaxed);
    };
    CIO_CHECK_EQ(cio::run(body()), 200);
}

void test_taskgroup_zero_join_is_reusable() {
    auto body = []() -> cio::Task<bool> {
        cio::TaskGroup group;
        for (int i = 0; i < 10'000; ++i) co_await group.join();
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_taskgroup_single_child_completion_is_reusable() {
    auto body = []() -> cio::Task<bool> {
        cio::TaskGroup group;
        int completed = 0;
        for (int i = 0; i < 10'000; ++i) {
            group.spawn([](int* count) -> cio::Task<> {
                ++*count;
                co_return;
            }(&completed));
            co_await group.join();
        }
        co_return completed == 10'000;
    };
    CIO_CHECK(cio::run(body()));
}

void test_taskgroup_parallel_spawns_are_joined() {
    static constexpr int kSpawners = 8;
    static constexpr int kChildrenPerSpawner = 500;

    auto body = []() -> cio::Task<int> {
        cio::TaskGroup group;
        std::atomic<int> finished{0};

        for (int i = 0; i < kSpawners; ++i) {
            group.spawn([](cio::TaskGroup& target,
                           std::atomic<int>& completed) -> cio::Task<> {
                for (int child = 0; child < kChildrenPerSpawner; ++child) {
                    target.spawn([](std::atomic<int>& count) -> cio::Task<> {
                        count.fetch_add(1, std::memory_order_relaxed);
                        co_return;
                    }(completed));
                    if ((child & 7) == 0) co_await cio::yield();
                }
            }(group, finished));
        }

        co_await group.join();
        co_return finished.load(std::memory_order_relaxed);
    };
    CIO_CHECK_EQ(cio::run(body()), kSpawners * kChildrenPerSpawner);
}

void test_taskgroup_direct_and_cold_completion_paths() {
    auto body = []() -> cio::Task<bool> {
        // A live, non-void child takes the direct final-suspend completion
        // path.
        cio::TaskGroup direct;
        direct.spawn([]() -> cio::Task<int> { co_return 42; }());
        co_await direct.join();

        // A task already at final suspend must keep using the cold wrapper: it
        // is valid, but resuming it directly would be undefined behaviour.
        cio::Task<> completed = []() -> cio::Task<> { co_return; }();
        co_await completed;
        CIO_CHECK(completed.done());
        cio::TaskGroup cold;
        cold.spawn(std::move(completed));
        co_await cold.join();

        // The historical invalid-task behaviour is asynchronous group failure,
        // not a synchronous exception from spawn().
        cio::TaskGroup invalid;
        invalid.spawn(cio::Task<>{});
        try {
            co_await invalid.join();
        } catch (const std::logic_error&) {
            co_return true;
        }
        co_return false;
    };
    CIO_CHECK(cio::run(body()));
}

void test_taskgroup_single_joiner_gets_final_handoff() {
    auto body = []() -> cio::Task<bool> {
        std::vector<int> order;
        const auto record = [](std::vector<int>* target,
                               int value) -> cio::Task<> {
            target->push_back(value);
            co_return;
        };

        cio::TaskGroup group;
        group.spawn(record(&order, 1));
        auto later = cio::spawn(record(&order, 3));
        co_await group.join();
        order.push_back(2);
        co_await later;
        co_return order == std::vector<int>({1, 2, 3});
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

void test_taskgroup_unjoined_children_keep_state_alive() {
    auto body = []() -> cio::Task<bool> {
        auto observed = cio::make_chan<bool>(1);
        auto release = cio::make_chan();
        {
            cio::TaskGroup group;
            group.spawn([](cio::Chan<cio::Unit> gate, cio::CancelToken token,
                           cio::Chan<bool> result) -> cio::Task<> {
                (void)co_await gate.recv();
                co_await result.send(token.cancelled());
            }(release, group.token(), observed));
        }
        release.close();

        // Destruction cancels the child but does not own its frame. The direct
        // completion record must keep GroupState alive until this task exits.
        co_return *co_await observed.recv();
    };
    CIO_CHECK(cio::run(body()));
}

void test_taskgroup_token_outlives_group() {
    cio::CancelToken token;
    {
        cio::TaskGroup group;
        token = group.token();
        CIO_CHECK(!token.cancelled());
        CIO_CHECK(!token.done().is_closed());
    }

    // TaskGroup's token aliases the group's shared control block. The token
    // must keep the embedded CancelState alive after the group destroys its
    // own reference and fires cancellation.
    CIO_CHECK(token.cancelled());
    CIO_CHECK(token.done().is_closed());
    const cio::CancelToken copy = token;
    token = cio::CancelToken{};
    CIO_CHECK(copy.cancelled());
    CIO_CHECK(copy.done().is_closed());
}

void test_taskgroup_start_races_last_completion() {
    auto body = []() -> cio::Task<bool> {
        for (int round = 0; round < 5'000; ++round) {
            cio::TaskGroup group;
            auto ready = cio::make_chan(1);
            auto release = cio::make_chan();

            group.spawn([](cio::Chan<cio::Unit> started,
                           cio::Chan<cio::Unit> gate) -> cio::Task<> {
                co_await started.send(cio::Unit{});
                (void)co_await gate.recv();
            }(ready, release));
            (void)co_await ready.recv();

            // Releasing the only child and starting another one deliberately
            // races 1 -> finishing against finishing -> 1. join() must cover
            // whichever side wins that generation boundary.
            release.close();
            group.spawn([]() -> cio::Task<> { co_return; }());
            co_await group.join();
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 8;
    CIO_CHECK(cio::run(body(), options));
}

void test_taskgroup_propagates_first_failure() {
    struct Boom : std::runtime_error {
        Boom() : std::runtime_error("child failed") {}
    };

    auto body = []() -> cio::Task<bool> {
        cio::TaskGroup group;
        group.spawn([]() -> cio::Task<> {
            co_await cio::sleep(5ms);
            throw Boom{};
        }());
        group.spawn([]() -> cio::Task<> { co_await cio::sleep(20ms); }());

        try {
            co_await group.join();
        } catch (const Boom&) {
            co_return true;
        }
        co_return false;
    };
    CIO_CHECK(cio::run(body()));
}

// A failing child cancels its siblings, so a long-running sibling that watches
// the token stops early instead of running to completion.
void test_taskgroup_failure_cancels_siblings() {
    auto body = []() -> cio::Task<bool> {
        cio::TaskGroup group;
        auto stopped_early = cio::make_chan<bool>(1);

        group.spawn(
            [](cio::CancelToken token, cio::Chan<bool> out) -> cio::Task<> {
                auto sel = cio::select(cio::recv(token.done()), cio::after(5s));
                co_await out.send(co_await sel == 0);
            }(group.token(), stopped_early));

        group.spawn([]() -> cio::Task<> {
            co_await cio::sleep(10ms);
            throw std::runtime_error("boom");
        }());

        try {
            co_await group.join();
        } catch (const std::runtime_error&) {
        }
        co_return *co_await stopped_early.recv();
    };
    CIO_CHECK(cio::run(body()));
}

void test_cancel_token_is_observable_both_ways() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource source;
        auto token = source.token();
        CIO_CHECK(!token.cancelled());

        cio::go([](cio::CancelSource s) -> cio::Task<> {
            co_await cio::sleep(10ms);
            s.cancel();
        }(source));

        // Blocking form: the done() channel closes on cancel.
        co_await token.done().recv();
        CIO_CHECK(token.cancelled());
        co_return token.cancelled();
    };
    CIO_CHECK(cio::run(body()));
}

void test_cancel_races_first_done_channel_creation() {
    {
        cio::CancelSource source;
        const auto token = source.token();
        source.cancel();

        std::atomic<bool> start{false};
        std::atomic<bool> observed_open{false};
        std::vector<std::thread> readers;
        for (int i = 0; i < 8; ++i) {
            readers.emplace_back([token, &start, &observed_open] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                if (!token.done().is_closed()) {
                    observed_open.store(true, std::memory_order_relaxed);
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& reader : readers) reader.join();
        CIO_CHECK(!observed_open.load(std::memory_order_relaxed));
    }

    for (int round = 0; round < 2'000; ++round) {
        cio::CancelSource source;
        const auto token = source.token();
        std::atomic<bool> start{false};
        std::thread canceller([source, &start] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            source.cancel();
        });

        start.store(true, std::memory_order_release);
        auto done = token.done();
        canceller.join();
        CIO_CHECK(done.is_closed());
    }
}

}  // namespace

int main() {
    RUN_TEST(test_waitgroup);
    RUN_TEST(test_waitgroup_single_waiter_gets_zero_handoff);
    RUN_TEST(test_waitgroup_reuse_waits_for_zero_transition);
    RUN_TEST(test_async_mutex_is_exclusive);
    RUN_TEST(test_async_mutex_try_lock_and_guard_release);
    RUN_TEST(test_mutex_unlock_races_waiter_publication);
    RUN_TEST(test_taskgroup_joins_every_child);
    RUN_TEST(test_taskgroup_zero_join_is_reusable);
    RUN_TEST(test_taskgroup_single_child_completion_is_reusable);
    RUN_TEST(test_taskgroup_parallel_spawns_are_joined);
    RUN_TEST(test_taskgroup_direct_and_cold_completion_paths);
    RUN_TEST(test_taskgroup_single_joiner_gets_final_handoff);
    RUN_TEST(test_taskgroup_unjoined_children_keep_state_alive);
    RUN_TEST(test_taskgroup_token_outlives_group);
    RUN_TEST(test_taskgroup_start_races_last_completion);
    RUN_TEST(test_taskgroup_propagates_first_failure);
    RUN_TEST(test_taskgroup_failure_cancels_siblings);
    RUN_TEST(test_cancel_token_is_observable_both_ways);
    RUN_TEST(test_cancel_races_first_done_channel_creation);
    return cio_test::summary();
}
