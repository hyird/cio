#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

cio::Task<int> add(int a, int b) { co_return a + b; }

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
        for (int i = 0; i < kPerFanOut; ++i) handles.push_back(cio::spawn(leaf()));
        for (auto& handle : handles) co_await handle;
    };
    auto root = [&]() -> cio::Task<> {
        std::vector<cio::JoinHandle<>> handles;
        handles.reserve(kFanOut);
        for (int i = 0; i < kFanOut; ++i) handles.push_back(cio::spawn(branch()));
        for (auto& handle : handles) co_await handle;
    };

    cio::run(root());
    CIO_CHECK_EQ(counter.load(), kFanOut * kPerFanOut);
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
        while (seen.load(std::memory_order_relaxed) < 100) co_await cio::sleep(1ms);
    };

    cio::run(body());
    CIO_CHECK_EQ(seen.load(), 100);
}

}  // namespace

int main() {
    RUN_TEST(test_child_task_composition);
    RUN_TEST(test_exception_propagates_through_await);
    RUN_TEST(test_spawn_and_join);
    RUN_TEST(test_many_tasks_across_workers);
    RUN_TEST(test_yield_round_trips);
    RUN_TEST(test_detached_task_runs);
    return cio_test::summary();
}
