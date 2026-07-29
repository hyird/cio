#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace sig = cio::signal;

namespace {

// SIGUSR1/SIGUSR2 are used rather than SIGINT/SIGTERM so a failing test cannot
// take down the harness through a default disposition.
void test_subscribe_requires_a_blocked_signal() {
    // SIGUSR2 is deliberately left unblocked here.
    auto unblocked = sig::SignalSet::subscribe({SIGUSR2});
    CIO_CHECK(!unblocked.has_value());
    CIO_CHECK(unblocked.error().is(cio::Errc::broken));

    auto empty = sig::SignalSet::subscribe(std::vector<int>{});
    CIO_CHECK(!empty.has_value());
}

void test_receives_a_raised_signal() {
    auto body = []() -> cio::Task<bool> {
        auto signals = sig::SignalSet::subscribe({SIGUSR1});
        CIO_CHECK(signals.has_value());
        CIO_CHECK(signals->valid());

        // Raised from a pool thread so the awaiting task is genuinely parked
        // on the reactor rather than finding the descriptor already readable.
        auto raiser = cio::spawn([]() -> cio::Task<> {
            co_await cio::blocking([] {
                std::this_thread::sleep_for(20ms);
                ::kill(::getpid(), SIGUSR1);
            });
        }());

        auto received = co_await signals->recv();
        co_await raiser;

        CIO_CHECK(received.has_value());
        CIO_CHECK_EQ(*received, SIGUSR1);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_deadline_and_close_wake_a_parked_receiver() {
    auto body = []() -> cio::Task<bool> {
        auto signals = sig::SignalSet::subscribe({SIGUSR1});
        CIO_CHECK(signals.has_value());

        // No signal is pending, so the deadline is what ends the wait.
        signals->set_deadline(cio::Clock::now() + 30ms);
        const auto started = cio::Clock::now();
        auto timed_out = co_await signals->recv();
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
        CIO_CHECK(elapsed >= 25ms);

        signals->clear_deadline();

        // close() wakes a receiver that is already parked.
        auto closer = cio::spawn([](sig::SignalSet& target) -> cio::Task<> {
            co_await cio::sleep(20ms);
            target.close();
        }(*signals));

        auto closed = co_await signals->recv();
        co_await closer;
        CIO_CHECK(!closed.has_value());

        // A closed set reports rather than touching a reused descriptor.
        auto after_close = co_await signals->recv();
        CIO_CHECK(!after_close.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_multiple_signals_in_one_set() {
    auto body = []() -> cio::Task<bool> {
        auto signals = sig::SignalSet::subscribe({SIGUSR1, SIGCHLD});
        CIO_CHECK(signals.has_value());

        auto raiser = cio::spawn([]() -> cio::Task<> {
            co_await cio::blocking([] {
                std::this_thread::sleep_for(20ms);
                ::kill(::getpid(), SIGCHLD);
            });
        }());

        auto received = co_await signals->recv();
        co_await raiser;
        CIO_CHECK(received.has_value());
        CIO_CHECK_EQ(*received, SIGCHLD);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    // The startup contract: block before any runtime thread exists.
    const auto blocked = sig::block({SIGUSR1, SIGCHLD});
    if (!blocked) {
        std::fprintf(stderr, "cannot block signals\n");
        return 1;
    }

    RUN_TEST(test_subscribe_requires_a_blocked_signal);
    RUN_TEST(test_receives_a_raised_signal);
    RUN_TEST(test_deadline_and_close_wake_a_parked_receiver);
    RUN_TEST(test_multiple_signals_in_one_set);
    return cio_test::summary();
}
