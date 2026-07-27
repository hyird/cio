#include <algorithm>
#include <atomic>
#include <chrono>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

void test_sleep_waits_at_least_the_duration() {
    auto body = []() -> cio::Task<long> {
        const auto started = cio::Clock::now();
        co_await cio::sleep(50ms);
        co_return std::chrono::duration_cast<std::chrono::milliseconds>(cio::Clock::now() -
                                                                       started)
            .count();
    };
    const long elapsed = cio::run(body());
    CIO_CHECK(elapsed >= 49);
    CIO_CHECK(elapsed < 500);
}

// Sub-millisecond sleeps: the reactor waits with nanosecond resolution
// (epoll_pwait2), so 200us must not round up to a full millisecond.
void test_sub_millisecond_sleep_is_not_rounded_up() {
    auto body = []() -> cio::Task<long> {
        const auto started = cio::Clock::now();
        for (int i = 0; i < 50; ++i) co_await cio::sleep(200us);
        co_return std::chrono::duration_cast<std::chrono::microseconds>(cio::Clock::now() -
                                                                       started)
            .count();
    };
    const long elapsed_us = cio::run(body());
    CIO_CHECK(elapsed_us >= 50 * 200);
    // 50 * 1ms would be 50000us; anything near that means we lost the precision.
    CIO_CHECK(elapsed_us < 40000);
}

// Timers fire in deadline order even when armed out of order and spread across
// per-worker shards.
void test_timers_fire_in_order() {
    auto body = []() -> cio::Task<bool> {
        auto order = cio::make_chan<int>(16);
        const std::array<int, 8> delays_ms{70, 10, 50, 20, 80, 30, 60, 40};

        for (std::size_t i = 0; i < delays_ms.size(); ++i) {
            cio::go([](cio::Chan<int> out, int delay) -> cio::Task<> {
                co_await cio::sleep(std::chrono::milliseconds(delay));
                co_await out.send(delay);
            }(order, delays_ms[i]));
        }

        std::vector<int> observed;
        for (std::size_t i = 0; i < delays_ms.size(); ++i) {
            observed.push_back(*co_await order.recv());
        }
        CIO_CHECK(std::is_sorted(observed.begin(), observed.end()));
        co_return std::is_sorted(observed.begin(), observed.end());
    };
    CIO_CHECK(cio::run(body()));
}

// Many timers at once: exercises heap growth, sharding and the poller's
// earliest-deadline computation.
void test_many_concurrent_timers() {
    static constexpr int kTimers = 5000;
    static std::atomic<int> fired{0};
    fired.store(0);

    auto body = []() -> cio::Task<> {
        cio::WaitGroup group;
        group.add(kTimers);
        for (int i = 0; i < kTimers; ++i) {
            cio::go([](cio::WaitGroup& wg, int index) -> cio::Task<> {
                co_await cio::sleep(std::chrono::microseconds(500 + (index % 50) * 100));
                fired.fetch_add(1, std::memory_order_relaxed);
                wg.done();
            }(group, i));
        }
        co_await group.wait();
    };

    cio::run(body());
    CIO_CHECK_EQ(fired.load(), kTimers);
}

// An idle runtime must not spin: with nothing to do, the poller parks in the
// reactor until the timer's deadline instead of polling in a loop.
void test_idle_runtime_does_not_spin() {
    auto body = []() -> cio::Task<> { co_await cio::sleep(300ms); };

    const auto wall_start = cio::Clock::now();
    const std::clock_t cpu_start = std::clock();
    cio::run(body());
    const double cpu_seconds = static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
    const auto wall = std::chrono::duration_cast<std::chrono::milliseconds>(cio::Clock::now() -
                                                                           wall_start);

    CIO_CHECK(wall.count() >= 290);
    // 24 workers spinning for 300ms would be ~7 CPU-seconds. Anything under
    // half a second means everyone actually parked.
    CIO_CHECK(cpu_seconds < 0.5);
    if (cpu_seconds >= 0.5) {
        std::fprintf(stderr, "  (cpu seconds while idle: %.3f)\n", cpu_seconds);
    }
}

}  // namespace

int main() {
    RUN_TEST(test_sleep_waits_at_least_the_duration);
    RUN_TEST(test_sub_millisecond_sleep_is_not_rounded_up);
    RUN_TEST(test_timers_fire_in_order);
    RUN_TEST(test_many_concurrent_timers);
    RUN_TEST(test_idle_runtime_does_not_spin);
    return cio_test::summary();
}
