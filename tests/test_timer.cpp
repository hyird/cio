#include <atomic>
#include <chrono>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

// ------------------------------------------------------------- Timer ---

void test_timer_fires_once() {
    auto body = []() -> cio::Task<bool> {
        cio::Timer timer(30ms);
        CIO_CHECK(timer.valid());

        const auto started = cio::Clock::now();
        auto tick = co_await timer.chan().recv();
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(tick.has_value());
        CIO_CHECK(elapsed >= 25ms);

        // One-shot: the channel closes after firing rather than leaving a
        // receiver waiting forever.
        auto again = co_await timer.chan().recv();
        CIO_CHECK(!again.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Stop must report whether it won the race, as time.Timer.Stop does.
void test_timer_stop_reports_whether_it_won() {
    auto body = []() -> cio::Task<bool> {
        cio::Timer pending(10s);
        CIO_CHECK(pending.stop());   // stopped before firing
        CIO_CHECK(!pending.stop());  // already stopped

        cio::Timer quick(10ms);
        auto tick = co_await quick.chan().recv();
        CIO_CHECK(tick.has_value());
        // It had already fired, so this call did not stop anything.
        CIO_CHECK(!quick.stop());

        // A stopped timer's channel closes, so a receiver is released.
        cio::Timer stopped(10s);
        auto waiter = cio::spawn([](cio::Chan<cio::Unit> c) -> cio::Task<bool> {
            auto v = co_await c.recv();
            co_return !v.has_value();
        }(stopped.chan()));
        co_await cio::sleep(10ms);
        stopped.stop();
        CIO_CHECK(co_await waiter);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Reset must take effect while the timer is still pending — extending a
// deadline is the case the handle exists for.
void test_timer_reset_extends_and_shortens() {
    auto body = []() -> cio::Task<bool> {
        // Extend: armed for 30ms, pushed out to 120ms before it fires.
        cio::Timer timer(30ms);
        co_await cio::sleep(10ms);
        CIO_CHECK(timer.reset(120ms));

        const auto started = cio::Clock::now();
        auto tick = co_await timer.chan().recv();
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(tick.has_value());
        // It must not have fired at the original 30ms.
        CIO_CHECK(elapsed >= 100ms);

        // Shorten: armed far out, pulled in.
        cio::Timer far(10s);
        CIO_CHECK(far.reset(20ms));
        const auto second_start = cio::Clock::now();
        auto soon = co_await far.chan().recv();
        CIO_CHECK(soon.has_value());
        CIO_CHECK(cio::Clock::now() - second_start < 5s);

        // Reset after firing reports that it was too late.
        CIO_CHECK(!far.reset(1s));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_timer_destructor_stops() {
    auto body = []() -> cio::Task<bool> {
        cio::Chan<cio::Unit> channel;
        {
            cio::Timer timer(10s);
            channel = timer.chan();
        }  // destructor stops it
        // The channel is closed, so this returns rather than waiting 10s.
        auto tick = co_await channel.recv();
        CIO_CHECK(!tick.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_timer_selectable() {
    auto body = []() -> cio::Task<bool> {
        auto work = cio::make_chan<int>(1);
        cio::Timer deadline(30ms);

        // The timer loses: work arrives first.
        co_await work.send(7);
        auto first = cio::select(cio::recv(work), cio::recv(deadline.chan()));
        CIO_CHECK_EQ(co_await first, std::size_t{0});
        CIO_CHECK_EQ(*first.get<0>(), 7);

        // The timer wins: nothing else arrives.
        auto second = cio::select(cio::recv(work), cio::recv(deadline.chan()));
        CIO_CHECK_EQ(co_await second, std::size_t{1});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// ------------------------------------------------------------ Ticker ---

void test_ticker_delivers_repeatedly() {
    auto body = []() -> cio::Task<bool> {
        cio::Ticker ticker(15ms);
        CIO_CHECK(ticker.valid());

        const auto started = cio::Clock::now();
        for (int i = 0; i < 4; ++i) {
            auto tick = co_await ticker.chan().recv();
            CIO_CHECK(tick.has_value());
        }
        const auto elapsed = cio::Clock::now() - started;
        // Four ticks at 15ms cannot arrive faster than three intervals.
        CIO_CHECK(elapsed >= 45ms);

        ticker.stop();
        auto after_stop = co_await ticker.chan().recv();
        CIO_CHECK(!after_stop.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A slow handler must not make the period drift: the next deadline advances
// from the previous deadline, not from when the tick was handled.
void test_ticker_does_not_drift() {
    auto body = []() -> cio::Task<bool> {
        cio::Ticker ticker(20ms);
        const auto started = cio::Clock::now();

        for (int i = 0; i < 5; ++i) {
            auto tick = co_await ticker.chan().recv();
            CIO_CHECK(tick.has_value());
            // Burn most of an interval handling it.
            co_await cio::sleep(12ms);
        }
        const auto elapsed = cio::Clock::now() - started;
        ticker.stop();

        // Drifting by the handler cost would take 5*(20+12) = 160ms. Advancing
        // from the previous deadline keeps five ticks near 100ms; allow slack for
        // the final handler and scheduling.
        CIO_CHECK(elapsed >= 100ms);
        CIO_CHECK(elapsed < 150ms);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_ticker_reset_and_invalid_period() {
    auto body = []() -> cio::Task<bool> {
        cio::Ticker ticker(200ms);
        ticker.reset(15ms);

        const auto started = cio::Clock::now();
        auto tick = co_await ticker.chan().recv();
        CIO_CHECK(tick.has_value());
        // The new interval applies, not the original 200ms.
        CIO_CHECK(cio::Clock::now() - started < 150ms);
        ticker.stop();

        // A non-positive period would spin, so the handle refuses it.
        cio::Ticker zero(0ms);
        CIO_CHECK(!zero.valid());
        cio::Ticker negative(-5ms);
        CIO_CHECK(!negative.valid());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// ---------------------------------------------------------- after_func ---

void test_after_func_runs_and_can_be_stopped() {
    auto body = []() -> cio::Task<bool> {
        std::atomic<int> ran{0};
        auto timer = cio::after_func(15ms, [&ran] {
            ran.fetch_add(1, std::memory_order_relaxed);
        });
        co_await cio::sleep(60ms);
        CIO_CHECK_EQ(ran.load(), 1);
        // It already ran, so stop() reports it did not win.
        CIO_CHECK(!timer.stop());

        std::atomic<int> never{0};
        auto cancelled = cio::after_func(10s, [&never] {
            never.fetch_add(1, std::memory_order_relaxed);
        });
        CIO_CHECK(cancelled.stop());
        co_await cio::sleep(30ms);
        CIO_CHECK_EQ(never.load(), 0);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// ------------------------------------------------------------- pools ---

void test_buffer_pool_reuses_storage() {
    cio::BufferPool pool;

    std::byte* first = nullptr;
    {
        auto buffer = pool.get(4096);
        CIO_CHECK(buffer.valid());
        // Rounded up to a power-of-two class, never smaller than requested.
        CIO_CHECK(buffer.size() >= std::size_t{4096});
        first = buffer.bytes().data();
        buffer.bytes()[0] = std::byte{0xAB};
    }

    // The same storage comes back rather than a fresh allocation.
    {
        auto again = pool.get(4096);
        CIO_CHECK_EQ(again.bytes().data(), first);
    }

    // A size class is shared across a range of requests.
    {
        auto small = pool.get(600);
        auto also_small = pool.get(1000);
        CIO_CHECK(small.size() == also_small.size());
    }

    // Above the pooled range: served, but not retained.
    {
        auto huge = pool.get(cio::BufferPool::kMaxPooledBytes * 2);
        CIO_CHECK(huge.valid());
        CIO_CHECK(huge.size() >= cio::BufferPool::kMaxPooledBytes * 2);
    }

    // Zero is served as a usable buffer rather than an empty span.
    {
        auto tiny = pool.get(0);
        CIO_CHECK(tiny.valid());
        CIO_CHECK(tiny.size() > std::size_t{0});
    }
}

void test_buffer_pool_move_semantics() {
    cio::BufferPool pool;
    auto first = pool.get(1024);
    std::byte* const data = first.bytes().data();

    auto moved = std::move(first);
    CIO_CHECK(!first.valid());
    CIO_CHECK(moved.valid());
    CIO_CHECK_EQ(moved.bytes().data(), data);

    // Move-assignment must return the overwritten buffer, not leak it.
    auto second = pool.get(1024);
    second = std::move(moved);
    CIO_CHECK_EQ(second.bytes().data(), data);
    CIO_CHECK(!moved.valid());
}

struct Poolable {
    int uses = 0;
};

void test_object_pool_reuses_objects() {
    cio::Pool<Poolable> pool;
    Poolable* first = nullptr;
    {
        auto handle = pool.get();
        CIO_CHECK(handle.valid());
        handle->uses = 5;
        first = &*handle;
    }
    CIO_CHECK_EQ(pool.retained(), std::size_t{1});
    {
        auto handle = pool.get();
        CIO_CHECK_EQ(&*handle, first);
        // State is the caller's to reset; the pool does not pretend otherwise.
        CIO_CHECK_EQ(handle->uses, 5);
    }
}

// The convenience copy() overload must borrow rather than allocate per call.
void test_copy_uses_the_pool() {
    struct Source {
        std::string data;
        std::size_t at = 0;
        cio::Task<cio::Result<std::size_t>> read(std::span<std::byte> out) {
            const std::size_t n = std::min(out.size(), data.size() - at);
            for (std::size_t i = 0; i < n; ++i) {
                out[i] = static_cast<std::byte>(data[at + i]);
            }
            at += n;
            co_return n;
        }
    };
    struct Sink {
        std::string seen;
        cio::Task<cio::Result<std::size_t>> write(
            std::span<const std::byte> in) {
            for (std::byte b : in) seen += static_cast<char>(b);
            co_return in.size();
        }
    };

    auto body = []() -> cio::Task<bool> {
        const std::size_t before = cio::buffer_pool().retained();
        for (int i = 0; i < 3; ++i) {
            Source src{std::string(4096, 'x')};
            Sink dst;
            auto copied = co_await cio::io::copy(dst, src);
            CIO_CHECK(copied.has_value());
            CIO_CHECK_EQ(*copied, std::uint64_t{4096});
            CIO_CHECK_EQ(dst.seen.size(), std::size_t{4096});
        }
        // Three copies must not have grown the retained set by three buffers;
        // the same one is handed back and reused.
        const std::size_t after = cio::buffer_pool().retained();
        CIO_CHECK(after <= before + 1);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_timer_fires_once);
    RUN_TEST(test_timer_stop_reports_whether_it_won);
    RUN_TEST(test_timer_reset_extends_and_shortens);
    RUN_TEST(test_timer_destructor_stops);
    RUN_TEST(test_timer_selectable);
    RUN_TEST(test_ticker_delivers_repeatedly);
    RUN_TEST(test_ticker_does_not_drift);
    RUN_TEST(test_ticker_reset_and_invalid_period);
    RUN_TEST(test_after_func_runs_and_can_be_stopped);
    RUN_TEST(test_buffer_pool_reuses_storage);
    RUN_TEST(test_buffer_pool_move_semantics);
    RUN_TEST(test_object_pool_reuses_objects);
    RUN_TEST(test_copy_uses_the_pool);
    return cio_test::summary();
}
