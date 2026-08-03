#include <atomic>
#include <chrono>
#include <memory>
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
                auto guard = co_await m.rlock();
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
                auto guard = co_await m.rlock();
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
        auto first = co_await lock.rlock();

        auto writer = cio::spawn(
            [](cio::RWMutex& m, std::atomic<bool>& ran) -> cio::Task<> {
                auto guard = co_await m.lock();
                ran.store(true);
            }(lock, writer_ran));

        co_await cio::sleep(10ms);
        // With a writer queued, a fresh reader must not be admitted.
        CIO_CHECK(!lock.try_rlock());

        cio::TaskGroup latecomers;
        for (int i = 0; i < 4; ++i) {
            latecomers.spawn([](cio::RWMutex& m) -> cio::Task<> {
                auto guard = co_await m.rlock();
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

// The writer publishes its pending bit while the final reader is leaving.
// Missing either side of that handshake parks the writer forever: the reader
// observes no writer to wake, while later readers observe the pending bit and
// cannot make progress either.
void test_last_reader_hands_off_to_queued_writer() {
    auto body = []() -> cio::Task<bool> {
        for (int round = 0; round < 2'000; ++round) {
            cio::RWMutex lock;
            auto reader = co_await lock.rlock();
            std::atomic<bool> writer_started{false};
            bool writer_ran = false;
            auto writer =
                cio::spawn([](cio::RWMutex& mutex, std::atomic<bool>& started,
                              bool& ran) -> cio::Task<> {
                    started.store(true, std::memory_order_release);
                    auto guard = co_await mutex.lock();
                    ran = true;
                }(lock, writer_started, writer_ran));

            while (!writer_started.load(std::memory_order_acquire)) {
                co_await cio::yield();
            }

            bool queued = false;
            const auto queue_deadline = cio::Clock::now() + 1s;
            while (cio::Clock::now() < queue_deadline) {
                if (!lock.try_rlock()) {
                    queued = true;
                    break;
                }
                lock.runlock();
                co_await cio::yield();
            }
            // A yield count is not a scheduling deadline: another worker may
            // be descheduled after publishing writer_started but before it
            // reaches lock(). Bound the actual publication check by wall time.
            CIO_CHECK(queued);

            reader = cio::RWMutex::ReadGuard{};
            co_await writer;
            CIO_CHECK(writer_ran);
        }
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The uncontended writer fast path is valid only while no waiter is being
// published. Releasing immediately after the reader announces its attempt
// repeatedly hits both sides of that boundary: unlock either wins first and
// the reader acquires zero, or the reader publishes pending and unlock joins
// the queue protocol. Neither outcome may lose the reader or overlap owners.
void test_rwmutex_unlock_races_first_reader_publication() {
    auto body = []() -> cio::Task<bool> {
        for (int round = 0; round < 20'000; ++round) {
            cio::RWMutex lock;
            auto writer = co_await lock.lock();
            auto ready = cio::make_chan(1);
            std::atomic<int> inside{1};
            std::atomic<bool> overlap{false};
            std::atomic<bool> reader_acquired{false};

            auto reader = cio::spawn(
                [](cio::RWMutex& mutex, cio::Chan<cio::Unit> started,
                   std::atomic<int>& active, std::atomic<bool>& raced,
                   std::atomic<bool>& acquired) -> cio::Task<bool> {
                    co_await started.send(cio::Unit{});
                    auto guard = co_await mutex.rlock();
                    acquired.store(true, std::memory_order_relaxed);
                    if (active.fetch_add(1, std::memory_order_relaxed) != 0) {
                        raced.store(true, std::memory_order_relaxed);
                    }
                    co_await cio::yield();
                    active.fetch_sub(1, std::memory_order_relaxed);
                    co_return true;
                }(lock, ready, inside, overlap, reader_acquired));

            (void)co_await ready.recv();
            if ((round & 15) == 0) {
                // Periodically give the waiter enough time to publish while
                // the writer remains held, also checking basic exclusion.
                co_await cio::yield();
                CIO_CHECK(!reader_acquired.load(std::memory_order_relaxed));
            }

            inside.fetch_sub(1, std::memory_order_relaxed);
            writer = cio::RWMutex::WriteGuard{};

            // A fresh writer may legitimately win before the reader publishes,
            // but once either side owns the lock they must remain exclusive.
            for (int attempt = 0; attempt < 2; ++attempt) {
                if (lock.try_lock()) {
                    if (inside.fetch_add(1, std::memory_order_relaxed) != 0) {
                        overlap.store(true, std::memory_order_relaxed);
                    }
                    co_await cio::yield();
                    inside.fetch_sub(1, std::memory_order_relaxed);
                    lock.unlock();
                }
                co_await cio::yield();
            }

            CIO_CHECK(co_await reader);
            CIO_CHECK(reader_acquired.load(std::memory_order_relaxed));
            CIO_CHECK(!overlap.load(std::memory_order_relaxed));
            CIO_CHECK(lock.try_lock());
            lock.unlock();
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 8;
    CIO_CHECK(cio::run(body(), options));
}

// Regression for the zero-to-pending race: one writer repeatedly queues while
// readers leave in parallel. The broken two-step "retry, then set pending"
// protocol strands every task in this exact shape with all workers parked.
void test_rwmutex_parallel_handoff_makes_progress() {
    auto body = []() -> cio::Task<bool> {
        cio::RWMutex lock;
        cio::TaskGroup tasks;
        std::atomic<int> completed{0};

        tasks.spawn(
            [](cio::RWMutex& mutex, std::atomic<int>& done) -> cio::Task<> {
                for (int i = 0; i < 5'000; ++i) {
                    auto guard = co_await mutex.lock();
                    co_await cio::yield();
                }
                done.fetch_add(1, std::memory_order_relaxed);
            }(lock, completed));

        for (int reader = 0; reader < 7; ++reader) {
            tasks.spawn(
                [](cio::RWMutex& mutex, std::atomic<int>& done) -> cio::Task<> {
                    for (int i = 0; i < 5'000; ++i) {
                        auto guard = co_await mutex.rlock();
                        co_await cio::yield();
                    }
                    done.fetch_add(1, std::memory_order_relaxed);
                }(lock, completed));
        }

        co_await tasks.join();
        CIO_CHECK_EQ(completed.load(std::memory_order_relaxed), 8);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_try_lock_variants() {
    auto body = []() -> cio::Task<bool> {
        cio::RWMutex lock;
        CIO_CHECK(lock.try_rlock());
        // Another reader may join.
        CIO_CHECK(lock.try_rlock());
        // A writer may not.
        CIO_CHECK(!lock.try_lock());
        lock.runlock();
        lock.runlock();

        CIO_CHECK(lock.try_lock());
        CIO_CHECK(!lock.try_lock());
        CIO_CHECK(!lock.try_rlock());
        lock.unlock();
        CIO_CHECK(lock.try_rlock());
        lock.runlock();
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

void test_once_preserves_lazy_move_only_calls() {
    auto body = []() -> cio::Task<bool> {
        cio::Once once;
        int value = 0;

        auto delayed = once.call(
            [increment = std::make_unique<int>(1), &value]() -> cio::Task<> {
                value += *increment;
                co_return;
            });
        co_await once.call(
            [increment = std::make_unique<int>(10), &value]() -> cio::Task<> {
                value += *increment;
                co_return;
            });
        co_await std::move(delayed);
        co_return value == 10;
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
        cond.broadcast();
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
        cond.signal();
        co_await cio::sleep(30ms);
        CIO_CHECK_EQ(served.load(), 1);

        // Release the rest so the group can join.
        {
            auto guard = co_await mutex.lock();
            tickets = 2;
        }
        cond.broadcast();
        co_await waiters.join();
        CIO_CHECK_EQ(served.load(), 3);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// ------------------------------------------------- cancellation scopes ---

// Cancelling a parent must reach every descendant, which is the whole point of
// nesting: a request deadline has to arrive at sub-operations without being
// threaded through by hand.
void test_cancel_propagates_down_the_chain() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource root;
        auto middle = cio::with_cancel(root.token());
        auto leaf = cio::with_cancel(middle.token());

        CIO_CHECK(!middle.cancelled());
        CIO_CHECK(!leaf.cancelled());

        root.cancel();
        // The done channel is what a parked operation selects on, so wait on it
        // rather than polling.
        (void)co_await leaf.token().done().recv();
        CIO_CHECK(middle.cancelled());
        CIO_CHECK(leaf.cancelled());
        CIO_CHECK(leaf.token().err().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A child cancel must not travel upward.
void test_cancel_does_not_propagate_up() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource root;
        auto child = cio::with_cancel(root.token());
        child.cancel();
        CIO_CHECK(child.cancelled());
        CIO_CHECK(!root.cancelled());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Attaching to an already-cancelled parent must fire immediately: no callback
// is coming, so waiting for one would hang.
void test_child_of_cancelled_parent_starts_cancelled() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource root;
        root.cancel();
        auto child = cio::with_cancel(root.token());
        CIO_CHECK(child.cancelled());
        auto grandchild = cio::with_timeout(child.token(), 10s);
        CIO_CHECK(grandchild.cancelled());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_timeout_scope_fires_and_reports_deadline() {
    auto body = []() -> cio::Task<bool> {
        auto scope = cio::with_timeout(30ms);
        const auto deadline = scope.token().deadline();
        CIO_CHECK(deadline.has_value());

        const auto started = cio::Clock::now();
        (void)co_await scope.token().done().recv();
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(elapsed >= 25ms);
        CIO_CHECK(scope.cancelled());

        // An elapsed deadline reports timed_out, not cancelled: they are
        // different outcomes and a caller decides differently on each.
        CIO_CHECK(scope.token().err().is(cio::Errc::timed_out));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A child must not be able to grant itself more time than its parent has.
void test_child_deadline_is_clamped_to_the_parent() {
    auto body = []() -> cio::Task<bool> {
        auto parent = cio::with_timeout(40ms);
        auto child = cio::with_timeout(parent.token(), 10s);

        const auto parent_deadline = parent.token().deadline();
        const auto child_deadline = child.token().deadline();
        CIO_CHECK(parent_deadline.has_value());
        CIO_CHECK(child_deadline.has_value());
        // Clamped, not honoured as asked.
        CIO_CHECK(*child_deadline <= *parent_deadline);

        const auto started = cio::Clock::now();
        (void)co_await child.token().done().recv();
        CIO_CHECK(cio::Clock::now() - started < 5s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// An explicit cancel before the deadline still reports cancellation.
void test_explicit_cancel_beats_the_deadline() {
    auto body = []() -> cio::Task<bool> {
        auto scope = cio::with_timeout(10s);
        scope.cancel();
        CIO_CHECK(scope.cancelled());
        CIO_CHECK(scope.token().err().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A scope must integrate with the operations that take a token, which is what
// it exists for.
void test_scope_cancels_a_socket_operation() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            cio::net::TcpListener::listen(cio::net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted = cio::spawn(
            [](cio::net::TcpListener l) -> cio::Task<cio::net::TcpConn> {
                auto conn = co_await l.accept();
                co_return conn ? std::move(*conn) : cio::net::TcpConn{};
            }(std::move(*listener)));

        auto client = co_await cio::net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto server = co_await accepted;

        // A 30ms request budget, applied to the socket.
        auto request = cio::with_timeout(30ms);
        client->set_cancel(request.token());

        std::byte buffer[8];
        auto read = co_await client->read(buffer);
        CIO_CHECK(!read.has_value());
        CIO_CHECK(read.error().is_cancelled());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Many finished children must not accumulate hooks on a long-lived parent.
void test_finished_children_deregister() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource root;
        for (int i = 0; i < 500; ++i) {
            auto child = cio::with_cancel(root.token());
            CIO_CHECK(!child.cancelled());
        }
        // The parent still works; if the hooks had leaked this would be the
        // point where ASan or the run time showed it.
        auto last = cio::with_cancel(root.token());
        root.cancel();
        (void)co_await last.token().done().recv();
        CIO_CHECK(last.cancelled());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_cancel_propagates_down_the_chain);
    RUN_TEST(test_cancel_does_not_propagate_up);
    RUN_TEST(test_child_of_cancelled_parent_starts_cancelled);
    RUN_TEST(test_timeout_scope_fires_and_reports_deadline);
    RUN_TEST(test_child_deadline_is_clamped_to_the_parent);
    RUN_TEST(test_explicit_cancel_beats_the_deadline);
    RUN_TEST(test_scope_cancels_a_socket_operation);
    RUN_TEST(test_finished_children_deregister);
    RUN_TEST(test_readers_share_writers_exclude);
    RUN_TEST(test_writer_is_not_starved);
    RUN_TEST(test_last_reader_hands_off_to_queued_writer);
    RUN_TEST(test_rwmutex_unlock_races_first_reader_publication);
    RUN_TEST(test_rwmutex_parallel_handoff_makes_progress);
    RUN_TEST(test_try_lock_variants);
    RUN_TEST(test_once_runs_exactly_once);
    RUN_TEST(test_once_waiters_see_the_result);
    RUN_TEST(test_once_preserves_lazy_move_only_calls);
    RUN_TEST(test_cond_wait_and_notify);
    RUN_TEST(test_cond_notify_one);
    return cio_test::summary();
}
