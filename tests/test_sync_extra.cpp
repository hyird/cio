#include <atomic>
#include <chrono>
#include <span>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

// ----------------------------------------------------------- RWMutex ---

void test_readers_share_writers_exclude() {
    auto body = []() -> cio::Task<bool> {
        cio::RWMutex lock;
        std::atomic<int> concurrent_readers{0};
        std::atomic<int> peak_readers{0};

        cio::TaskGroup readers;
        for (int i = 0; i < 8; ++i) {
            readers.spawn([](cio::RWMutex& m, std::atomic<int>& live,
                             std::atomic<int>& peak) -> cio::Task<> {
                auto guard = co_await m.lock_read();
                const int now = live.fetch_add(1) + 1;
                int seen = peak.load();
                while (now > seen && !peak.compare_exchange_weak(seen, now)) {
                }
                co_await cio::sleep(20ms);
                live.fetch_sub(1);
            }(lock, concurrent_readers, peak_readers));
        }
        co_await readers.join();

        // Readers must actually overlap, or this is just a Mutex.
        CIO_CHECK(peak_readers.load() > 1);

        // A writer excludes everything.
        std::atomic<int> inside_writer{0};
        std::atomic<bool> overlapped{false};
        cio::TaskGroup mixed;
        for (int i = 0; i < 4; ++i) {
            mixed.spawn([](cio::RWMutex& m, std::atomic<int>& inside,
                           std::atomic<bool>& bad) -> cio::Task<> {
                auto guard = co_await m.lock();
                if (inside.fetch_add(1) != 0) bad.store(true);
                co_await cio::sleep(5ms);
                inside.fetch_sub(1);
            }(lock, inside_writer, overlapped));
            mixed.spawn([](cio::RWMutex& m, std::atomic<int>& inside,
                           std::atomic<bool>& bad) -> cio::Task<> {
                auto guard = co_await m.lock_read();
                if (inside.load() != 0) bad.store(true);
                co_await cio::sleep(2ms);
            }(lock, inside_writer, overlapped));
        }
        co_await mixed.join();
        CIO_CHECK(!overlapped.load());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Writer-preferring: a steady stream of readers must not starve a waiting
// writer. Without that, a read-heavy workload never lets a writer run.
void test_writer_is_not_starved() {
    auto body = []() -> cio::Task<bool> {
        cio::RWMutex lock;
        std::atomic<bool> writer_ran{false};
        std::atomic<bool> stop{false};

        // Hold a read lock so the writer has to queue.
        auto first = co_await lock.lock_read();

        auto writer = cio::spawn([](cio::RWMutex& m,
                                    std::atomic<bool>& ran) -> cio::Task<> {
            auto guard = co_await m.lock();
            ran.store(true);
        }(lock, writer_ran));

        co_await cio::sleep(10ms);
        // With a writer queued, a fresh reader must not be admitted.
        CIO_CHECK(!lock.try_lock_read());

        cio::TaskGroup latecomers;
        for (int i = 0; i < 4; ++i) {
            latecomers.spawn([](cio::RWMutex& m) -> cio::Task<> {
                auto guard = co_await m.lock_read();
                co_await cio::sleep(2ms);
            }(lock));
        }

        first = cio::RWMutex::ReadGuard{};  // release the initial read lock
        co_await writer;
        CIO_CHECK(writer_ran.load());
        co_await latecomers.join();
        (void)stop;
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_try_lock_variants() {
    auto body = []() -> cio::Task<bool> {
        cio::RWMutex lock;
        CIO_CHECK(lock.try_lock_read());
        // Another reader may join.
        CIO_CHECK(lock.try_lock_read());
        // A writer may not.
        CIO_CHECK(!lock.try_lock());
        lock.unlock_read();
        lock.unlock_read();

        CIO_CHECK(lock.try_lock());
        CIO_CHECK(!lock.try_lock());
        CIO_CHECK(!lock.try_lock_read());
        lock.unlock();
        CIO_CHECK(lock.try_lock_read());
        lock.unlock_read();
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// -------------------------------------------------------------- Once ---

void test_once_runs_exactly_once() {
    auto body = []() -> cio::Task<bool> {
        cio::Once once;
        std::atomic<int> calls{0};

        cio::TaskGroup group;
        for (int i = 0; i < 16; ++i) {
            group.spawn([](cio::Once& o, std::atomic<int>& n) -> cio::Task<> {
                co_await o.call([&n]() -> cio::Task<void> {
                    // Suspends inside the initialiser, which sync.Once cannot
                    // express: every other caller must wait rather than race
                    // past a half-built value.
                    co_await cio::sleep(20ms);
                    n.fetch_add(1);
                });
            }(once, calls));
        }
        co_await group.join();

        CIO_CHECK_EQ(calls.load(), 1);
        CIO_CHECK(once.done());

        // A later call does nothing.
        co_await once.call([&calls]() -> cio::Task<void> {
            calls.fetch_add(1);
            co_return;
        });
        CIO_CHECK_EQ(calls.load(), 1);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Every waiter must observe the initialisation, not just the first caller.
void test_once_waiters_see_the_result() {
    auto body = []() -> cio::Task<bool> {
        cio::Once once;
        std::atomic<int> value{0};
        std::atomic<int> observed_zero{0};

        cio::TaskGroup group;
        for (int i = 0; i < 8; ++i) {
            group.spawn([](cio::Once& o, std::atomic<int>& v,
                           std::atomic<int>& bad) -> cio::Task<> {
                co_await o.call([&v]() -> cio::Task<void> {
                    co_await cio::sleep(15ms);
                    v.store(42);
                });
                if (v.load() != 42) bad.fetch_add(1);
            }(once, value, observed_zero));
        }
        co_await group.join();
        CIO_CHECK_EQ(observed_zero.load(), 0);
        CIO_CHECK_EQ(value.load(), 42);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// -------------------------------------------------------------- Cond ---

void test_cond_wait_and_notify() {
    auto body = []() -> cio::Task<bool> {
        cio::Mutex mutex;
        cio::Cond cond(mutex);
        int ready = 0;
        std::atomic<int> woke{0};

        cio::TaskGroup waiters;
        for (int i = 0; i < 3; ++i) {
            waiters.spawn([](cio::Mutex& m, cio::Cond& c, int& state,
                             std::atomic<int>& count) -> cio::Task<> {
                auto guard = co_await m.lock();
                // Re-check in a loop: a condition variable permits spurious
                // wakeups, here as in Go and the standard library.
                while (state == 0) co_await c.wait(guard);
                count.fetch_add(1);
            }(mutex, cond, ready, woke));
        }

        co_await cio::sleep(20ms);
        CIO_CHECK_EQ(woke.load(), 0);
        {
            auto guard = co_await mutex.lock();
            ready = 1;
        }
        cond.notify_all();
        co_await waiters.join();
        CIO_CHECK_EQ(woke.load(), 3);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_cond_notify_one() {
    auto body = []() -> cio::Task<bool> {
        cio::Mutex mutex;
        cio::Cond cond(mutex);
        std::atomic<int> served{0};
        int tickets = 0;

        cio::TaskGroup waiters;
        for (int i = 0; i < 3; ++i) {
            waiters.spawn([](cio::Mutex& m, cio::Cond& c, int& t,
                             std::atomic<int>& n) -> cio::Task<> {
                auto guard = co_await m.lock();
                while (t == 0) co_await c.wait(guard);
                --t;
                n.fetch_add(1);
            }(mutex, cond, tickets, served));
        }
        co_await cio::sleep(20ms);

        // One ticket, one wakeup: the others stay waiting.
        {
            auto guard = co_await mutex.lock();
            tickets = 1;
        }
        cond.notify_one();
        co_await cio::sleep(30ms);
        CIO_CHECK_EQ(served.load(), 1);

        // Release the rest so the group can join.
        {
            auto guard = co_await mutex.lock();
            tickets = 2;
        }
        cond.notify_all();
        co_await waiters.join();
        CIO_CHECK_EQ(served.load(), 3);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_readers_share_writers_exclude);
    RUN_TEST(test_writer_is_not_starved);
    RUN_TEST(test_try_lock_variants);
    RUN_TEST(test_once_runs_exactly_once);
    RUN_TEST(test_once_waiters_see_the_result);
    RUN_TEST(test_cond_wait_and_notify);
    RUN_TEST(test_cond_notify_one);
    return cio_test::summary();
}
