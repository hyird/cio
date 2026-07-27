#include <array>
#include <chrono>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

void test_picks_the_ready_case() {
    auto body = []() -> cio::Task<std::size_t> {
        auto a = cio::make_chan<int>(1);
        auto b = cio::make_chan<int>(1);
        CIO_CHECK(b.try_send(7));

        auto sel = cio::select(cio::recv(a), cio::recv(b));
        const std::size_t which = co_await sel;
        CIO_CHECK_EQ(which, std::size_t{1});
        CIO_CHECK_EQ(*sel.get<1>(), 7);
        co_return which;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

void test_blocks_until_a_case_fires() {
    auto body = []() -> cio::Task<std::size_t> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();

        cio::go([](cio::Chan<int> target) -> cio::Task<> {
            co_await cio::sleep(20ms);
            co_await target.send(42);
        }(b));

        auto sel = cio::select(cio::recv(a), cio::recv(b));
        const std::size_t which = co_await sel;
        CIO_CHECK_EQ(which, std::size_t{1});
        CIO_CHECK_EQ(*sel.get<1>(), 42);
        co_return which;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

void test_timeout_case_fires() {
    auto body = []() -> cio::Task<std::size_t> {
        auto never = cio::make_chan<int>();
        const auto started = cio::Clock::now();

        auto sel = cio::select(cio::recv(never), cio::after(30ms));
        const std::size_t which = co_await sel;

        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(elapsed >= 25ms);
        CIO_CHECK(elapsed < 2s);
        co_return which;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

void test_timeout_loses_to_a_ready_channel() {
    auto body = []() -> cio::Task<std::size_t> {
        auto ch = cio::make_chan<int>();
        cio::go([](cio::Chan<int> target) -> cio::Task<> {
            co_await cio::sleep(5ms);
            co_await target.send(1);
        }(ch));

        auto sel = cio::select(cio::recv(ch), cio::after(500ms));
        co_return co_await sel;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{0});
}

void test_default_makes_it_non_blocking() {
    auto body = []() -> cio::Task<std::size_t> {
        auto empty = cio::make_chan<int>(1);
        auto sel = cio::select(cio::recv(empty), cio::otherwise());
        co_return co_await sel;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

void test_send_case() {
    auto body = []() -> cio::Task<bool> {
        auto ch = cio::make_chan<int>(1);
        auto sel = cio::select(cio::send(ch, 5), cio::after(1s));
        const std::size_t which = co_await sel;
        CIO_CHECK_EQ(which, std::size_t{0});
        CIO_CHECK(sel.get<0>());
        CIO_CHECK_EQ(*co_await ch.recv(), 5);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A nil channel disables its case, exactly as in Go. This is the idiom for
// turning a case off without restructuring the select.
void test_nil_channel_case_is_never_ready() {
    auto body = []() -> cio::Task<std::size_t> {
        cio::Chan<int> disabled;  // nil
        auto live = cio::make_chan<int>(1);
        CIO_CHECK(live.try_send(3));

        auto sel = cio::select(cio::recv(disabled), cio::recv(live));
        co_return co_await sel;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

// Both cases permanently ready: the choice must not be a fixed order, or a hot
// first case starves everything after it.
void test_ready_cases_are_chosen_randomly() {
    auto body = []() -> cio::Task<int> {
        auto a = cio::make_chan<int>(64);
        auto b = cio::make_chan<int>(64);
        int picked_b = 0;

        for (int i = 0; i < 200; ++i) {
            CIO_CHECK(a.try_send(1));
            CIO_CHECK(b.try_send(2));
            auto sel = cio::select(cio::recv(a), cio::recv(b));
            if (co_await sel == 1) ++picked_b;
            // Drain whichever we did not take so the next round starts clean.
            a.try_recv();
            b.try_recv();
        }
        co_return picked_b;
    };
    const int picked_b = cio::run(body());
    CIO_CHECK(picked_b > 40);
    CIO_CHECK(picked_b < 160);
}

// The pattern this whole design exists to support: a worker loop that is
// cancellable because cancellation is just another channel.
void test_cancellation_through_select() {
    auto body = []() -> cio::Task<int> {
        auto jobs = cio::make_chan<int>(4);
        cio::CancelSource source;
        auto processed = cio::make_chan<int>(1);

        cio::go([](cio::Chan<int> in, cio::CancelToken token,
                   cio::Chan<int> out) -> cio::Task<> {
            int count = 0;
            for (;;) {
                auto sel = cio::select(cio::recv(in), cio::recv(token.done()));
                if (co_await sel == 1) break;  // cancelled
                if (!sel.get<0>()) break;      // channel closed
                ++count;
            }
            co_await out.send(count);
        }(jobs, source.token(), processed));

        for (int i = 0; i < 5; ++i) co_await jobs.send(i);
        co_await cio::sleep(20ms);
        source.cancel();

        co_return *co_await processed.recv();
    };
    CIO_CHECK_EQ(cio::run(body()), 5);
}

// Hammer the setup/wake race: many selects parking and being woken while
// other selects are still registering their cases.
void test_concurrent_selects() {
    static constexpr int kWorkers = 32;
    static constexpr int kRounds = 300;

    auto body = []() -> cio::Task<int> {
        auto left = cio::make_chan<int>(8);
        auto right = cio::make_chan<int>(8);
        auto results = cio::make_chan<int>(kWorkers);

        for (int w = 0; w < kWorkers; ++w) {
            cio::go([](cio::Chan<int> a, cio::Chan<int> b,
                       cio::Chan<int> out) -> cio::Task<> {
                int seen = 0;
                for (;;) {
                    auto sel = cio::select(cio::recv(a), cio::recv(b));
                    const std::size_t which = co_await sel;
                    const bool alive = which == 0 ? sel.get<0>().has_value()
                                                  : sel.get<1>().has_value();
                    if (!alive) break;
                    ++seen;
                }
                co_await out.send(seen);
            }(left, right, results));
        }

        for (int i = 0; i < kRounds; ++i) {
            co_await left.send(i);
            co_await right.send(i);
        }
        left.close();
        right.close();

        int total = 0;
        for (int w = 0; w < kWorkers; ++w) total += *co_await results.recv();
        co_return total;
    };
    CIO_CHECK_EQ(cio::run(body()), kRounds * 2);
}

}  // namespace

int main() {
    RUN_TEST(test_picks_the_ready_case);
    RUN_TEST(test_blocks_until_a_case_fires);
    RUN_TEST(test_timeout_case_fires);
    RUN_TEST(test_timeout_loses_to_a_ready_channel);
    RUN_TEST(test_default_makes_it_non_blocking);
    RUN_TEST(test_send_case);
    RUN_TEST(test_nil_channel_case_is_never_ready);
    RUN_TEST(test_ready_cases_are_chosen_randomly);
    RUN_TEST(test_cancellation_through_select);
    RUN_TEST(test_concurrent_selects);
    return cio_test::summary();
}
