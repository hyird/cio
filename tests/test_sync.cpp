#include <atomic>
#include <chrono>
#include <stdexcept>

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
                    co_await cio::yield();  // force a suspension inside the section
                    value = observed + 1;
                }
            }(mutex, counter));
        }
        co_await group.join();
        co_return counter;
    };
    CIO_CHECK_EQ(cio::run(body()), static_cast<long>(kTasks) * kIncrements);
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

        group.spawn([](cio::CancelToken token, cio::Chan<bool> out) -> cio::Task<> {
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

}  // namespace

int main() {
    RUN_TEST(test_waitgroup);
    RUN_TEST(test_async_mutex_is_exclusive);
    RUN_TEST(test_taskgroup_joins_every_child);
    RUN_TEST(test_taskgroup_propagates_first_failure);
    RUN_TEST(test_taskgroup_failure_cancels_siblings);
    RUN_TEST(test_cancel_token_is_observable_both_ways);
    return cio_test::summary();
}
