// Core scheduler and channel microbenchmarks.
//
//     ./bench_core [workers] [name-filter] [scale]
//
// Numbers to compare against, on the same machine, for Go 1.2x:
//   goroutine spawn+join   ~300-400 ns
//   unbuffered chan hop    ~250-350 ns
//   buffered chan op       ~60-90 ns
#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "cio/cio.hpp"

using namespace std::chrono_literals;

namespace {

struct Timing {
    const char* name;
    double ns_per_op;
    double ops_per_sec;
};

std::vector<Timing> results;
std::string_view benchmark_filter;
double benchmark_scale = 1.0;

long scaled(long count) {
    return std::max(1L, static_cast<long>(std::llround(
                            static_cast<double>(count) * benchmark_scale)));
}

template<typename F>
void measure(const char* name, long operations, F&& body) {
    if (!benchmark_filter.empty() &&
        std::string_view{name}.find(benchmark_filter) ==
            std::string_view::npos) {
        return;
    }
    const auto start = cio::Clock::now();
    body();
    const auto elapsed = cio::Clock::now() - start;
    const double ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    results.push_back({name, ns / static_cast<double>(operations),
                       static_cast<double>(operations) / (ns / 1e9)});
}

// --- spawn rate -------------------------------------------------------------

cio::Task<> empty_task() {
    co_return;
}

cio::Task<> spawn_detached(long count, cio::WaitGroup& group) {
    for (long i = 0; i < count; ++i) {
        cio::go([](cio::WaitGroup& wg) -> cio::Task<> {
            wg.done();
            co_return;
        }(group));
    }
    co_await group.wait();
}

cio::Task<> spawn_and_join(long count) {
    std::vector<cio::JoinHandle<>> handles;
    handles.reserve(static_cast<std::size_t>(count));
    for (long i = 0; i < count; ++i)
        handles.push_back(cio::spawn(empty_task()));
    for (auto& handle : handles) co_await handle;
}

cio::Task<> spawn_single_child(long count) {
    for (long i = 0; i < count; ++i) {
        auto handle = cio::spawn(empty_task());
        co_await handle;
    }
}

cio::Task<> await_completed_join_handle(long count) {
    auto handle = cio::spawn(empty_task());
    co_await handle;
    for (long i = 0; i < count; ++i) co_await handle;
}

cio::Task<> wait_group_single_child(long count) {
    for (long i = 0; i < count; ++i) {
        cio::WaitGroup group;
        group.add(1);
        cio::go([](cio::WaitGroup* target) -> cio::Task<> {
            target->done();
            co_return;
        }(&group));
        co_await group.wait();
    }
}

cio::Task<> task_group_spawn_and_join(long count) {
    cio::TaskGroup group;
    for (long i = 0; i < count; ++i) group.spawn(empty_task());
    co_await group.join();
}

cio::Task<> task_group_single_child(long count) {
    for (long i = 0; i < count; ++i) {
        cio::TaskGroup group;
        group.spawn(empty_task());
        co_await group.join();
    }
}

cio::Task<> task_group_reused_single_child(long count) {
    cio::TaskGroup group;
    for (long i = 0; i < count; ++i) {
        group.spawn(empty_task());
        co_await group.join();
    }
}

cio::Task<> task_group_zero_join(long count) {
    cio::TaskGroup group;
    for (long i = 0; i < count; ++i) co_await group.join();
}

// --- channel hop ------------------------------------------------------------

cio::Task<> ping_pong(long rounds, std::size_t capacity) {
    auto to_pong = cio::make_chan<int>(capacity);
    auto to_ping = cio::make_chan<int>(capacity);

    cio::go([](cio::Chan<int> in, cio::Chan<int> out, long n) -> cio::Task<> {
        for (long i = 0; i < n; ++i) {
            auto value = co_await in.recv();
            if (!value) break;
            co_await out.send(*value);
        }
    }(to_pong, to_ping, rounds));

    for (long i = 0; i < rounds; ++i) {
        co_await to_pong.send(1);
        co_await to_ping.recv();
    }
    to_pong.close();
}

// --- channel throughput -----------------------------------------------------

cio::Task<> chan_throughput(long per_producer, int producers, int consumers) {
    auto jobs = cio::make_chan<int>(1024);
    cio::WaitGroup producing;
    producing.add(producers);

    for (int p = 0; p < producers; ++p) {
        cio::go(
            [](cio::Chan<int> out, cio::WaitGroup& wg, long n) -> cio::Task<> {
                for (long i = 0; i < n; ++i) co_await out.send(1);
                wg.done();
            }(jobs, producing, per_producer));
    }

    cio::WaitGroup consuming;
    consuming.add(consumers);
    for (int c = 0; c < consumers; ++c) {
        cio::go([](cio::Chan<int> in, cio::WaitGroup& wg) -> cio::Task<> {
            while (co_await in.recv()) {
            }
            wg.done();
        }(jobs, consuming));
    }

    co_await producing.wait();
    jobs.close();
    co_await consuming.wait();
}

cio::Task<> chan_buffered_try_ops(long rounds) {
    auto channel = cio::make_chan<int>(1);
    long sum = 0;
    for (long i = 0; i < rounds; ++i) {
        if (!channel.try_send(static_cast<int>(i))) std::terminate();
        auto value = channel.try_recv();
        if (!value) std::terminate();
        sum += *value;
    }
    if (sum == -1) std::terminate();
    co_return;
}

cio::Task<> chan_buffered_try_recv_miss(long rounds) {
    auto channel = cio::make_chan<int>(1);
    for (long i = 0; i < rounds; ++i) {
        if (channel.try_recv().has_value()) std::terminate();
    }
    co_return;
}

cio::Task<> chan_buffered_await_ops(long rounds) {
    auto channel = cio::make_chan<int>(1);
    long sum = 0;
    for (long i = 0; i < rounds; ++i) {
        if (!(co_await channel.send(static_cast<int>(i)))) std::terminate();
        auto value = co_await channel.recv();
        if (!value) std::terminate();
        sum += *value;
    }
    if (sum == -1) std::terminate();
    co_return;
}

cio::Task<> wait_group_nonzero_counter(long rounds) {
    cio::WaitGroup group;
    group.add(1);
    for (long i = 0; i < rounds; ++i) {
        group.add(1);
        group.done();
    }
    group.done();
    co_return;
}

cio::Task<> wait_group_zero_wait(long rounds) {
    cio::WaitGroup group;
    for (long i = 0; i < rounds; ++i) co_await group.wait();
}

cio::Task<> cancellation_poll(long count) {
    cio::CancelSource source;
    const cio::CancelToken token = source.token();
    long cancelled = 0;
    for (long i = 0; i < count; ++i) cancelled += token.cancelled();
    if (cancelled != 0) std::terminate();
    co_return;
}

// --- suspension cost --------------------------------------------------------

cio::Task<> yield_loop(long count) {
    for (long i = 0; i < count; ++i) co_await cio::yield();
}

// Measures the *fast path* only: the deadline has already passed by the time
// await_ready runs, so this never suspends. It is the cost of a sleep that does
// not need to sleep, which is what a deadline check in a hot loop costs.
cio::Task<> expired_sleep_loop(long count) {
    for (long i = 0; i < count; ++i)
        co_await cio::sleep(std::chrono::nanoseconds(0));
}

// Real timer traffic: arm, park, fire, resume. Concurrency hides the wall time
// so what is measured is the per-timer runtime cost, not the delay itself.
cio::Task<> concurrent_timers(long count) {
    cio::WaitGroup group;
    group.add(count);
    for (long i = 0; i < count; ++i) {
        cio::go([](cio::WaitGroup& wg) -> cio::Task<> {
            co_await cio::sleep(std::chrono::milliseconds(1));
            wg.done();
        }(group));
    }
    co_await group.wait();
}

cio::Task<> timer_arm_disarm(long operations) {
    constexpr long kBatch = 1024;
    auto timers = std::make_unique<cio::detail::Timer[]>(kBatch);
    auto& service = cio::detail::current_scheduler()->timers();
    const long cycles = std::max(1L, operations / (kBatch * 2));

    for (long cycle = 0; cycle < cycles; ++cycle) {
        const std::int64_t base = cio::now_ns() + 60'000'000'000LL;
        for (long i = 0; i < kBatch; ++i) {
            auto& timer = timers[static_cast<std::size_t>(i)];
            timer.deadline_ns = base + ((i * 683) & (kBatch - 1));
            timer.waiter = std::noop_coroutine();
            timer.on_fire = nullptr;
            service.arm(&timer);
        }
        for (long i = 0; i < kBatch; ++i) {
            if (!service.disarm(&timers[static_cast<std::size_t>(i)])) {
                std::terminate();
            }
        }
    }
    co_return;
}

// Foreign threads have no worker-local shard, so they all fall back to shard
// zero. This is the adversarial timer-lock shape: unlike ordinary sleeps and
// descriptor deadlines, every operation genuinely contends on one heap.
cio::Task<> timer_foreign_contention(long per_thread, int thread_count) {
    auto& service = cio::detail::current_scheduler()->timers();
    std::barrier start(thread_count);
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(thread_count));

    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i] {
            cio::detail::Timer timer;
            timer.waiter = std::noop_coroutine();
            timer.on_fire = nullptr;
            start.arrive_and_wait();
            try {
                for (long round = 0; round < per_thread; ++round) {
                    timer.deadline_ns = cio::now_ns() + 60'000'000'000LL + i;
                    service.arm(&timer);
                    if (!service.disarm(&timer)) {
                        failed.store(true, std::memory_order_relaxed);
                        return;
                    }
                }
            } catch (...) {
                failed.store(true, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    if (failed.load(std::memory_order_relaxed)) std::terminate();
    co_return;
}

// --- mutex ------------------------------------------------------------------

cio::Task<> mutex_uncontended(long count) {
    cio::Mutex mutex;
    for (long i = 0; i < count; ++i) {
        auto guard = co_await mutex.lock();
    }
}

// Force ownership to move between tasks. The yield happens while the guard is
// held, giving peers a chance to queue before unlock hands the mutex directly
// to the oldest waiter.
cio::Task<> mutex_contended(long per_task, int tasks) {
    cio::Mutex mutex;
    cio::WaitGroup group;
    group.add(tasks);
    long total = 0;

    for (int i = 0; i < tasks; ++i) {
        cio::go([](cio::Mutex& m, cio::WaitGroup& wg, long& value,
                   long count) -> cio::Task<> {
            for (long k = 0; k < count; ++k) {
                auto guard = co_await m.lock();
                ++value;
                co_await cio::yield();
            }
            wg.done();
        }(mutex, group, total, per_task));
    }
    co_await group.wait();
}

cio::Task<> rwmutex_read_uncontended(long count) {
    cio::RWMutex mutex;
    for (long i = 0; i < count; ++i) {
        auto guard = co_await mutex.rlock();
    }
}

cio::Task<> rwmutex_write_uncontended(long count) {
    cio::RWMutex mutex;
    for (long i = 0; i < count; ++i) {
        auto guard = co_await mutex.lock();
    }
}

cio::Task<> rwmutex_contended(long per_task) {
    cio::RWMutex mutex;
    cio::WaitGroup group;
    group.add(8);

    cio::go([](cio::RWMutex& m, cio::WaitGroup& wg, long count) -> cio::Task<> {
        for (long i = 0; i < count; ++i) {
            auto guard = co_await m.lock();
            co_await cio::yield();
        }
        wg.done();
    }(mutex, group, per_task));

    for (int task = 0; task < 7; ++task) {
        cio::go(
            [](cio::RWMutex& m, cio::WaitGroup& wg, long count) -> cio::Task<> {
                for (long i = 0; i < count; ++i) {
                    auto guard = co_await m.rlock();
                    co_await cio::yield();
                }
                wg.done();
            }(mutex, group, per_task));
    }
    co_await group.wait();
}

cio::Task<> once_already_done(long count) {
    cio::Once once;
    co_await once.call(empty_task);
    for (long i = 0; i < count; ++i) {
        co_await once.call(empty_task);
    }
}

cio::Task<> once_first_call(long count) {
    for (long i = 0; i < count; ++i) {
        cio::Once once;
        co_await once.call(empty_task);
    }
}

cio::Task<> once_first_call_parallel(long rounds) {
    for (long round = 0; round < rounds; ++round) {
        cio::Once once;
        cio::TaskGroup group;
        for (int task = 0; task < 8; ++task) {
            group.spawn([](cio::Once& target) -> cio::Task<> {
                co_await target.call(empty_task);
            }(once));
        }
        co_await group.join();
    }
}

cio::Task<> once_completed_parallel(long per_task) {
    cio::Once once;
    co_await once.call(empty_task);

    cio::WaitGroup group;
    group.add(8);
    for (int i = 0; i < 8; ++i) {
        cio::go([](cio::Once& value, cio::WaitGroup& done,
                   long count) -> cio::Task<> {
            for (long call = 0; call < count; ++call) {
                co_await value.call(empty_task);
            }
            done.done();
        }(once, group, per_task));
    }
    co_await group.wait();
}

// --- select -----------------------------------------------------------------

cio::Task<> select_loop(long rounds) {
    auto a = cio::make_chan<int>(1);
    auto b = cio::make_chan<int>(1);
    auto idle = cio::make_chan<int>();  // never ready

    for (long i = 0; i < rounds; ++i) {
        a.try_send(1);
        auto sel = cio::select(cio::recv(a), cio::recv(b), cio::recv(idle));
        co_await sel;
    }
}

// A select that actually has to park on every iteration, then be woken by a
// peer — the expensive shape, and the one a cancellable worker loop uses.
cio::Task<> select_blocking_loop(long rounds) {
    auto data = cio::make_chan<int>();
    auto quit = cio::make_chan<int>();

    cio::go([](cio::Chan<int> out, long n) -> cio::Task<> {
        for (long i = 0; i < n; ++i) co_await out.send(1);
    }(data, rounds));

    for (long i = 0; i < rounds; ++i) {
        auto sel = cio::select(cio::recv(data), cio::recv(quit));
        co_await sel;
    }
}

cio::Task<> select_default_loop(long rounds) {
    auto a = cio::make_chan<int>();
    auto b = cio::make_chan<int>();

    for (long i = 0; i < rounds; ++i) {
        auto sel = cio::select(cio::recv(a), cio::recv(b), cio::otherwise());
        co_await sel;
    }
}

cio::Task<> select_ready_beats_timeout_loop(long rounds) {
    auto ready = cio::make_chan<int>(1);
    const auto deadline = cio::Clock::now() + 24h;

    for (long i = 0; i < rounds; ++i) {
        if (!ready.try_send(1)) std::terminate();
        auto sel = cio::select(cio::recv(ready), cio::after_deadline(deadline));
        if (co_await sel != 0) std::terminate();
    }
}

cio::Task<> select_timeout_loop(long rounds) {
    cio::Chan<int> never;
    for (long i = 0; i < rounds; ++i) {
        auto sel = cio::select(cio::recv(never), cio::after(0ns));
        if (co_await sel != 1) std::terminate();
    }
}

// --- child task composition (no scheduler involvement) ----------------------

cio::Task<long> leaf(long v) {
    co_return v;
}

cio::Task<long> await_children(long count) {
    long total = 0;
    for (long i = 0; i < count; ++i) total += co_await leaf(i);
    co_return total;
}

cio::Task<> frame_pool_roundtrip(long count) {
    constexpr std::size_t kRepresentativeFrameSize = 128;
    for (long i = 0; i < count; ++i) {
        void* frame =
            cio::detail::FramePool::allocate(kRepresentativeFrameSize);
        cio::detail::FramePool::deallocate(frame, kRepresentativeFrameSize);
    }
    co_return;
}

cio::Task<> buffer_pool_roundtrip(long count, std::size_t buffer_size) {
    cio::BufferPool pool;
    for (long i = 0; i < count; ++i) {
        auto buffer = pool.get(buffer_size);
        if (buffer.size() != buffer_size) std::terminate();
    }
    co_return;
}

}  // namespace

int main(int argc, char** argv) {
    cio::RuntimeOptions options;
    if (argc > 1)
        options.worker_threads = static_cast<std::size_t>(std::atoi(argv[1]));
    if (argc > 2) benchmark_filter = argv[2];
    if (argc > 3) {
        benchmark_scale = std::strtod(argv[3], nullptr);
        if (!std::isfinite(benchmark_scale) || benchmark_scale <= 0.0) {
            std::fprintf(stderr,
                         "scale must be a finite number greater than zero\n");
            return 2;
        }
    }

    cio::Runtime runtime(options);
    std::printf("cio benchmarks — %zu workers, scale %.3g\n\n",
                runtime.worker_count(), benchmark_scale);

    const long kSpawns = scaled(1'000'000);
    const long kHops = scaled(200'000);
    const long kYields = scaled(2'000'000);
    const long kTimers = scaled(200'000);
    const long kAwaits = scaled(5'000'000);
    const long kThroughput = ((scaled(500'000) + 7) / 8) * 8;
    const long kMutexOps = scaled(1'000'000);

    {
        cio::WaitGroup group;
        group.add(kSpawns);
        measure("go() detached spawn", kSpawns,
                [&] { runtime.block_on(spawn_detached(kSpawns, group)); });
    }
    measure("spawn() + co_await join", kSpawns / 4,
            [&] { runtime.block_on(spawn_and_join(kSpawns / 4)); });
    measure("spawn(), one child", kSpawns / 4,
            [&] { runtime.block_on(spawn_single_child(kSpawns / 4)); });
    measure("completed JoinHandle await", kAwaits,
            [&] { runtime.block_on(await_completed_join_handle(kAwaits)); });
    measure("wait group, one child", kSpawns / 4,
            [&] { runtime.block_on(wait_group_single_child(kSpawns / 4)); });
    measure("task group spawn + join", kSpawns,
            [&] { runtime.block_on(task_group_spawn_and_join(kSpawns)); });
    measure("task group, one child", kSpawns,
            [&] { runtime.block_on(task_group_single_child(kSpawns)); });
    measure("task group reused, one child", kSpawns,
            [&] { runtime.block_on(task_group_reused_single_child(kSpawns)); });
    measure("task group, zero join", kMutexOps,
            [&] { runtime.block_on(task_group_zero_join(kMutexOps)); });
    measure("co_await child task", kAwaits,
            [&] { runtime.block_on(await_children(kAwaits)); });
    measure("frame pool alloc+free", kAwaits,
            [&] { runtime.block_on(frame_pool_roundtrip(kAwaits)); });
    measure("buffer pool get+put, 512 B", kAwaits,
            [&] { runtime.block_on(buffer_pool_roundtrip(kAwaits, 512)); });
    measure("buffer pool get+put, 4 KiB", kAwaits, [&] {
        runtime.block_on(buffer_pool_roundtrip(kAwaits, 4 * 1024));
    });
    measure("buffer pool get+put, 64 KiB", kAwaits, [&] {
        runtime.block_on(buffer_pool_roundtrip(kAwaits, 64 * 1024));
    });
    measure("buffer pool get+put, 1 MiB", kAwaits, [&] {
        runtime.block_on(buffer_pool_roundtrip(kAwaits, 1024 * 1024));
    });
    measure("co_await yield()", kYields,
            [&] { runtime.block_on(yield_loop(kYields)); });
    measure("sleep() already-expired path", kYields / 4,
            [&] { runtime.block_on(expired_sleep_loop(kYields / 4)); });
    measure("timer arm+fire (concurrent)", kTimers,
            [&] { runtime.block_on(concurrent_timers(kTimers)); });
    const long kTimerArmDisarm = ((kTimers + 2047) / 2048) * 2048;
    measure("timer arm+disarm batch", kTimerArmDisarm,
            [&] { runtime.block_on(timer_arm_disarm(kTimerArmDisarm)); });
    constexpr int kTimerForeignThreads = 8;
    const long kTimerForeignPerThread = scaled(100'000);
    measure("timer foreign contention, 8 threads",
            kTimerForeignPerThread * kTimerForeignThreads * 2, [&] {
                runtime.block_on(timer_foreign_contention(
                    kTimerForeignPerThread, kTimerForeignThreads));
            });
    measure("mutex uncontended", kMutexOps,
            [&] { runtime.block_on(mutex_uncontended(kMutexOps)); });
    measure("mutex contended, 8 tasks", kMutexOps / 5,
            [&] { runtime.block_on(mutex_contended(kMutexOps / 40, 8)); });
    measure("rwmutex read uncontended", kMutexOps,
            [&] { runtime.block_on(rwmutex_read_uncontended(kMutexOps)); });
    measure("rwmutex write uncontended", kMutexOps,
            [&] { runtime.block_on(rwmutex_write_uncontended(kMutexOps)); });
    measure("rwmutex contended, 7r1w", kMutexOps / 5,
            [&] { runtime.block_on(rwmutex_contended(kMutexOps / 40)); });
    measure("once already done", kMutexOps,
            [&] { runtime.block_on(once_already_done(kMutexOps)); });
    measure("once first call", kMutexOps,
            [&] { runtime.block_on(once_first_call(kMutexOps)); });
    const long kOnceFirstParallel = ((scaled(100'000) + 7) / 8) * 8;
    measure("once first call, 8 tasks", kOnceFirstParallel, [&] {
        runtime.block_on(once_first_call_parallel(kOnceFirstParallel / 8));
    });
    const long kOnceParallel = ((kMutexOps + 7) / 8) * 8;
    measure("once completed, 8 tasks", kOnceParallel, [&] {
        runtime.block_on(once_completed_parallel(kOnceParallel / 8));
    });
    measure("select, a case ready", kHops,
            [&] { runtime.block_on(select_loop(kHops)); });
    measure("select, parks every round", kHops,
            [&] { runtime.block_on(select_blocking_loop(kHops)); });
    measure("select, takes default", kHops,
            [&] { runtime.block_on(select_default_loop(kHops)); });
    measure("select, ready beats timeout", kHops,
            [&] { runtime.block_on(select_ready_beats_timeout_loop(kHops)); });
    measure("select, timeout fires", kHops,
            [&] { runtime.block_on(select_timeout_loop(kHops)); });

    // Each round is a send + a recv on each side: 2 channel hops.
    measure("unbuffered chan round trip", kHops,
            [&] { runtime.block_on(ping_pong(kHops, 0)); });
    measure("buffered chan round trip", kHops,
            [&] { runtime.block_on(ping_pong(kHops, 64)); });

    measure("chan 1p1c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput, 1, 1)); });
    measure("chan 8p1c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput / 8, 8, 1)); });
    measure("chan 1p8c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput, 1, 8)); });
    measure("chan 8p8c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput / 8, 8, 8)); });
    measure("buffered chan try send+recv", kThroughput * 2,
            [&] { runtime.block_on(chan_buffered_try_ops(kThroughput)); });
    measure("buffered chan try recv miss", kThroughput, [&] {
        runtime.block_on(chan_buffered_try_recv_miss(kThroughput));
    });
    measure("buffered chan await send+recv", kThroughput * 2,
            [&] { runtime.block_on(chan_buffered_await_ops(kThroughput)); });
    measure("wait group nonzero add+done", kMutexOps * 2,
            [&] { runtime.block_on(wait_group_nonzero_counter(kMutexOps)); });
    measure("wait group, zero wait", kMutexOps,
            [&] { runtime.block_on(wait_group_zero_wait(kMutexOps)); });
    measure("cancellation poll", kMutexOps,
            [&] { runtime.block_on(cancellation_poll(kMutexOps)); });

    std::printf("%-30s %14s %16s\n", "benchmark", "ns/op", "ops/sec");
    std::printf("%-30s %14s %16s\n", "------------------------------",
                "--------------", "----------------");
    for (const auto& r : results) {
        std::printf("%-30s %14.1f %16.0f\n", r.name, r.ns_per_op,
                    r.ops_per_sec);
    }
    const auto metrics = cio::runtime_metrics();
    if (metrics.tasks_run != 0) {
        std::printf(
            "\nmetrics: tasks=%llu parks=%llu cv_waits=%llu wake_single=%llu "
            "steals=%llu/%llu\n",
            static_cast<unsigned long long>(metrics.tasks_run),
            static_cast<unsigned long long>(metrics.parks),
            static_cast<unsigned long long>(metrics.park_cv_waits),
            static_cast<unsigned long long>(metrics.wake_single),
            static_cast<unsigned long long>(metrics.steal_hits),
            static_cast<unsigned long long>(metrics.steal_attempts));
    }
    return 0;
}
