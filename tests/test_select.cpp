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

void test_timeout_does_not_bias_ready_cases() {
    auto body = []() -> cio::Task<std::array<int, 3>> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();
        a.close();
        b.close();
        const auto deadline = cio::Clock::now() + 24h;

        std::array<int, 3> picked{};
        for (int i = 0; i < 6000; ++i) {
            auto sel = cio::select(cio::recv(a), cio::after_deadline(deadline),
                                   cio::recv(b));
            ++picked[co_await sel];
        }
        co_return picked;
    };

    const auto picked = cio::run(body());
    CIO_CHECK(picked[0] > 2500);
    CIO_CHECK(picked[0] < 3500);
    CIO_CHECK_EQ(picked[1], 0);
    CIO_CHECK(picked[2] > 2500);
    CIO_CHECK(picked[2] < 3500);
}

void test_default_makes_it_non_blocking() {
    auto body = []() -> cio::Task<std::size_t> {
        auto empty = cio::make_chan<int>(1);
        auto sel = cio::select(cio::recv(empty), cio::otherwise());
        co_return co_await sel;
    };
    CIO_CHECK_EQ(cio::run(body()), std::size_t{1});
}

void test_default_does_not_compete_with_ready_cases() {
    auto body = []() -> cio::Task<std::array<int, 3>> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();
        a.close();
        b.close();

        std::array<int, 3> picked{};
        for (int i = 0; i < 6000; ++i) {
            auto sel =
                cio::select(cio::recv(a), cio::otherwise(), cio::recv(b));
            ++picked[co_await sel];
        }
        co_return picked;
    };

    const auto picked = cio::run(body());
    CIO_CHECK_EQ(picked[1], 0);
    CIO_CHECK(picked[0] > 2500);
    CIO_CHECK(picked[0] < 3500);
    CIO_CHECK(picked[2] > 2500);
    CIO_CHECK(picked[2] < 3500);
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

void test_copied_and_moved_cases_park_safely() {
    auto body = []() -> cio::Task<bool> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();
        auto first = cio::recv(a);
        auto second = cio::recv(b);
        auto copied = first;

        auto sender = cio::spawn([](cio::Chan<int> target) -> cio::Task<> {
            co_await cio::sleep(20ms);
            CIO_CHECK(co_await target.send(42));
        }(b));

        auto recv_select = cio::select(std::move(copied), std::move(second));
        CIO_CHECK_EQ(co_await recv_select, std::size_t{1});
        CIO_CHECK_EQ(*recv_select.get<1>(), 42);
        co_await sender;

        auto output = cio::make_chan<int>();
        auto send_case = cio::send(output, 9);
        auto receiver = cio::spawn([](cio::Chan<int> source) -> cio::Task<int> {
            co_await cio::sleep(20ms);
            co_return *co_await source.recv();
        }(output));

        auto send_select = cio::select(std::move(send_case), cio::after(1s));
        CIO_CHECK_EQ(co_await send_select, std::size_t{0});
        CIO_CHECK(send_select.get<0>());
        CIO_CHECK_EQ(co_await receiver, 9);
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

// A rotation randomises only where the scan starts, so a case that is never
// ready hands its rotations to whichever case follows it. With two ready cases
// and one disabled, that used to produce a 2:1 split.
void test_ready_cases_are_uniform_with_a_disabled_case() {
    auto body = [](std::size_t disabled) -> cio::Task<std::array<int, 3>> {
        std::array<cio::Chan<int>, 3> channels;
        for (std::size_t i = 0; i < channels.size(); ++i) {
            if (i == disabled) continue;
            channels[i] = cio::make_chan<int>();
            channels[i].close();
        }

        std::array<int, 3> picked{};
        for (int i = 0; i < 6000; ++i) {
            auto sel =
                cio::select(cio::recv(channels[0]), cio::recv(channels[1]),
                            cio::recv(channels[2]));
            const std::size_t which = co_await sel;
            CIO_CHECK(which < picked.size());
            if (which < picked.size()) ++picked[which];
        }

        co_return picked;
    };

    for (std::size_t disabled = 0; disabled < 3; ++disabled) {
        const auto picked = cio::run(body(disabled));
        CIO_CHECK_EQ(picked[disabled], 0);
        for (std::size_t i = 0; i < picked.size(); ++i) {
            if (i == disabled) continue;
            CIO_CHECK(picked[i] > 2500);
            CIO_CHECK(picked[i] < 3500);
        }
        if (picked[disabled] != 0) {
            std::fprintf(stderr, "  (disabled %zu was selected %d times)\n",
                         disabled, picked[disabled]);
        }
    }
}

void test_three_ready_cases_are_uniform() {
    auto body = []() -> cio::Task<std::array<int, 3>> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();
        auto c = cio::make_chan<int>();
        a.close();
        b.close();
        c.close();

        std::array<int, 3> picked{};
        for (int i = 0; i < 9000; ++i) {
            auto sel = cio::select(cio::recv(a), cio::recv(b), cio::recv(c));
            const std::size_t which = co_await sel;
            CIO_CHECK(which < picked.size());
            if (which < picked.size()) ++picked[which];
        }
        co_return picked;
    };

    const auto picked = cio::run(body());
    for (const int count : picked) {
        CIO_CHECK(count > 2500);
        CIO_CHECK(count < 3500);
    }
    if (picked[0] <= 2500 || picked[0] >= 3500 || picked[1] <= 2500 ||
        picked[1] >= 3500 || picked[2] <= 2500 || picked[2] >= 3500) {
        std::fprintf(stderr, "  (split was %d / %d / %d)\n", picked[0],
                     picked[1], picked[2]);
    }
}

// The poll order is built by a different branch for each case count — one, two
// and three cases each pick a permutation directly instead of shuffling — so
// uniformity has to be checked at more than one count. This is the two-case
// branch, where the permutation is a single random bit.
void test_two_ready_cases_are_uniform() {
    auto body = []() -> cio::Task<std::array<int, 2>> {
        auto a = cio::make_chan<int>();
        auto b = cio::make_chan<int>();
        a.close();
        b.close();

        std::array<int, 2> picked{};
        for (int i = 0; i < 6000; ++i) {
            auto sel = cio::select(cio::recv(a), cio::recv(b));
            const std::size_t which = co_await sel;
            CIO_CHECK(which < 2);
            if (which < 2) ++picked[which];
        }
        co_return picked;
    };

    const auto picked = cio::run(body());
    CIO_CHECK(picked[0] > 2500);
    CIO_CHECK(picked[0] < 3500);
    CIO_CHECK(picked[1] > 2500);
    CIO_CHECK(picked[1] < 3500);
    if (picked[0] <= 2500 || picked[0] >= 3500) {
        std::fprintf(stderr, "  (split was %d / %d)\n", picked[0], picked[1]);
    }
}

// Four cases fall through to the general Fisher-Yates branch. Checking that
// every ready case is reachable and roughly even keeps the generic path honest
// once the specialised ones exist to be preferred.
void test_four_ready_cases_are_uniform() {
    auto body = []() -> cio::Task<std::array<int, 4>> {
        std::array<cio::Chan<int>, 4> channels{
            cio::make_chan<int>(), cio::make_chan<int>(), cio::make_chan<int>(),
            cio::make_chan<int>()};
        for (auto& channel : channels) channel.close();

        std::array<int, 4> picked{};
        for (int i = 0; i < 8000; ++i) {
            auto sel =
                cio::select(cio::recv(channels[0]), cio::recv(channels[1]),
                            cio::recv(channels[2]), cio::recv(channels[3]));
            const std::size_t which = co_await sel;
            CIO_CHECK(which < 4);
            if (which < 4) ++picked[which];
        }
        co_return picked;
    };

    const auto picked = cio::run(body());
    for (const int count : picked) {
        CIO_CHECK(count > 1500);
        CIO_CHECK(count < 2500);
    }
    if (picked[0] <= 1500 || picked[0] >= 2500) {
        std::fprintf(stderr, "  (split was %d / %d / %d / %d)\n", picked[0],
                     picked[1], picked[2], picked[3]);
    }
}

}  // namespace

int main() {
    RUN_TEST(test_picks_the_ready_case);
    RUN_TEST(test_blocks_until_a_case_fires);
    RUN_TEST(test_timeout_case_fires);
    RUN_TEST(test_timeout_loses_to_a_ready_channel);
    RUN_TEST(test_timeout_does_not_bias_ready_cases);
    RUN_TEST(test_default_makes_it_non_blocking);
    RUN_TEST(test_default_does_not_compete_with_ready_cases);
    RUN_TEST(test_send_case);
    RUN_TEST(test_copied_and_moved_cases_park_safely);
    RUN_TEST(test_nil_channel_case_is_never_ready);
    RUN_TEST(test_ready_cases_are_chosen_randomly);
    RUN_TEST(test_cancellation_through_select);
    RUN_TEST(test_ready_cases_are_uniform_with_a_disabled_case);
    RUN_TEST(test_two_ready_cases_are_uniform);
    RUN_TEST(test_three_ready_cases_are_uniform);
    RUN_TEST(test_four_ready_cases_are_uniform);
    RUN_TEST(test_concurrent_selects);
    return cio_test::summary();
}
