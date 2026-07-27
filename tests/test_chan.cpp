#include <atomic>
#include <chrono>
#include <numeric>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

void test_unbuffered_rendezvous() {
    auto body = []() -> cio::Task<int> {
        auto ch = cio::make_chan<int>();  // capacity 0
        cio::go([](cio::Chan<int> out) -> cio::Task<> {
            for (int i = 1; i <= 100; ++i) co_await out.send(i);
            out.close();
        }(ch));

        int total = 0;
        while (auto value = co_await ch.recv()) total += *value;
        co_return total;
    };
    CIO_CHECK_EQ(cio::run(body()), 5050);
}

void test_buffered_capacity() {
    auto body = []() -> cio::Task<bool> {
        auto ch = cio::make_chan<int>(4);
        // Four fit without a peer; the fifth would block.
        for (int i = 0; i < 4; ++i) CIO_CHECK(ch.try_send(i));
        CIO_CHECK(!ch.try_send(99));
        CIO_CHECK_EQ(ch.size(), std::size_t{4});

        for (int i = 0; i < 4; ++i) {
            auto value = co_await ch.recv();
            CIO_CHECK(value.has_value());
            CIO_CHECK_EQ(*value, i);
        }
        CIO_CHECK(!ch.try_recv().has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_close_drains_then_reports() {
    auto body = []() -> cio::Task<bool> {
        auto ch = cio::make_chan<int>(8);
        co_await ch.send(1);
        co_await ch.send(2);
        ch.close();

        // Buffered values survive close; only an empty closed channel is done.
        CIO_CHECK_EQ(*co_await ch.recv(), 1);
        CIO_CHECK_EQ(*co_await ch.recv(), 2);
        CIO_CHECK(!(co_await ch.recv()).has_value());

        // Sending to a closed channel fails rather than delivering.
        CIO_CHECK(!(co_await ch.send(3)));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_close_wakes_blocked_receivers() {
    auto body = []() -> cio::Task<int> {
        auto ch = cio::make_chan<int>();
        auto done = cio::make_chan<int>(16);

        for (int i = 0; i < 16; ++i) {
            cio::go([](cio::Chan<int> in, cio::Chan<int> out) -> cio::Task<> {
                auto value = co_await in.recv();
                co_await out.send(value.has_value() ? 1 : 0);
            }(ch, done));
        }

        // Give the receivers time to actually park before closing.
        co_await cio::sleep(20ms);
        ch.close();

        int woken_with_nullopt = 0;
        for (int i = 0; i < 16; ++i) woken_with_nullopt += *co_await done.recv() == 0 ? 1 : 0;
        co_return woken_with_nullopt;
    };
    CIO_CHECK_EQ(cio::run(body()), 16);
}

// Many producers and consumers on one channel: exercises the waiter queues,
// the direct hand-off path and the buffer wraparound at the same time.
void test_mpmc_throughput() {
    static constexpr int kProducers = 8;
    static constexpr int kConsumers = 8;
    static constexpr int kPerProducer = 2000;

    auto body = []() -> cio::Task<long> {
        auto jobs = cio::make_chan<int>(64);
        auto sums = cio::make_chan<long>(kConsumers);

        cio::WaitGroup producers;
        producers.add(kProducers);
        for (int p = 0; p < kProducers; ++p) {
            cio::go([](cio::Chan<int> out, cio::WaitGroup& wg, int base) -> cio::Task<> {
                for (int i = 0; i < kPerProducer; ++i) co_await out.send(base + i);
                wg.done();
            }(jobs, producers, p * kPerProducer));
        }

        for (int c = 0; c < kConsumers; ++c) {
            cio::go([](cio::Chan<int> in, cio::Chan<long> out) -> cio::Task<> {
                long total = 0;
                while (auto value = co_await in.recv()) total += *value;
                co_await out.send(total);
            }(jobs, sums));
        }

        co_await producers.wait();
        jobs.close();

        long total = 0;
        for (int c = 0; c < kConsumers; ++c) total += *co_await sums.recv();
        co_return total;
    };

    const long n = static_cast<long>(kProducers) * kPerProducer;
    CIO_CHECK_EQ(cio::run(body()), n * (n - 1) / 2);
}

void test_move_only_payload() {
    auto body = []() -> cio::Task<std::size_t> {
        auto ch = cio::make_chan<std::unique_ptr<int>>(2);
        cio::go([](cio::Chan<std::unique_ptr<int>> out) -> cio::Task<> {
            for (int i = 0; i < 10; ++i) co_await out.send(std::make_unique<int>(i));
            out.close();
        }(ch));

        std::size_t count = 0;
        while (auto value = co_await ch.recv()) {
            CIO_CHECK_EQ(**value, static_cast<int>(count));
            ++count;
        }
        co_return count;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{10});
}

void test_string_payload_ordering() {
    auto body = []() -> cio::Task<std::string> {
        auto ch = cio::make_chan<std::string>(3);
        cio::go([](cio::Chan<std::string> out) -> cio::Task<> {
            for (char c = 'a'; c <= 'z'; ++c) co_await out.send(std::string(1, c));
            out.close();
        }(ch));

        std::string joined;
        while (auto piece = co_await ch.recv()) joined += *piece;
        co_return joined;
    };
    CIO_CHECK_EQ(cio::run(body()), std::string("abcdefghijklmnopqrstuvwxyz"));
}

}  // namespace

int main() {
    RUN_TEST(test_unbuffered_rendezvous);
    RUN_TEST(test_buffered_capacity);
    RUN_TEST(test_close_drains_then_reports);
    RUN_TEST(test_close_wakes_blocked_receivers);
    RUN_TEST(test_mpmc_throughput);
    RUN_TEST(test_move_only_payload);
    RUN_TEST(test_string_payload_ordering);
    return cio_test::summary();
}
