// Core scheduler and channel microbenchmarks.
//
//     ./bench_core [workers]
//
// Numbers to compare against, on the same machine, for Go 1.2x:
//   goroutine spawn+join   ~300-400 ns
//   unbuffered chan hop    ~250-350 ns
//   buffered chan op       ~60-90 ns
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
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

template <typename F>
void measure(const char* name, long operations, F&& body) {
    const auto start = cio::Clock::now();
    body();
    const auto elapsed = cio::Clock::now() - start;
    const double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    results.push_back({name, ns / static_cast<double>(operations),
                       static_cast<double>(operations) / (ns / 1e9)});
}

// --- spawn rate -------------------------------------------------------------

cio::Task<> empty_task() { co_return; }

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
    for (long i = 0; i < count; ++i) handles.push_back(cio::spawn(empty_task()));
    for (auto& handle : handles) co_await handle;
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
        cio::go([](cio::Chan<int> out, cio::WaitGroup& wg, long n) -> cio::Task<> {
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

// --- suspension cost --------------------------------------------------------

cio::Task<> yield_loop(long count) {
    for (long i = 0; i < count; ++i) co_await cio::yield();
}

// Measures the *fast path* only: the deadline has already passed by the time
// await_ready runs, so this never suspends. It is the cost of a sleep that does
// not need to sleep, which is what a deadline check in a hot loop costs.
cio::Task<> expired_sleep_loop(long count) {
    for (long i = 0; i < count; ++i) co_await cio::sleep(std::chrono::nanoseconds(0));
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

// --- child task composition (no scheduler involvement) ----------------------

cio::Task<long> leaf(long v) { co_return v; }

cio::Task<long> await_children(long count) {
    long total = 0;
    for (long i = 0; i < count; ++i) total += co_await leaf(i);
    co_return total;
}

}  // namespace

int main(int argc, char** argv) {
    cio::RuntimeOptions options;
    if (argc > 1) options.worker_threads = static_cast<std::size_t>(std::atoi(argv[1]));

    cio::Runtime runtime(options);
    std::printf("cio benchmarks — %zu workers\n\n", runtime.worker_count());

    constexpr long kSpawns = 1'000'000;
    constexpr long kHops = 200'000;
    constexpr long kYields = 2'000'000;
    constexpr long kTimers = 200'000;
    constexpr long kAwaits = 5'000'000;
    constexpr long kThroughput = 500'000;

    {
        cio::WaitGroup group;
        group.add(kSpawns);
        measure("go() detached spawn", kSpawns,
                [&] { runtime.block_on(spawn_detached(kSpawns, group)); });
    }
    measure("spawn() + co_await join", kSpawns / 4,
            [&] { runtime.block_on(spawn_and_join(kSpawns / 4)); });

    measure("co_await child task", kAwaits,
            [&] { runtime.block_on(await_children(kAwaits)); });
    measure("co_await yield()", kYields, [&] { runtime.block_on(yield_loop(kYields)); });
    measure("sleep() already-expired path", kYields / 4,
            [&] { runtime.block_on(expired_sleep_loop(kYields / 4)); });
    measure("timer arm+fire (concurrent)", kTimers,
            [&] { runtime.block_on(concurrent_timers(kTimers)); });

    measure("select, a case ready", kHops,
            [&] { runtime.block_on(select_loop(kHops)); });
    measure("select, parks every round", kHops,
            [&] { runtime.block_on(select_blocking_loop(kHops)); });

    // Each round is a send + a recv on each side: 2 channel hops.
    measure("unbuffered chan round trip", kHops,
            [&] { runtime.block_on(ping_pong(kHops, 0)); });
    measure("buffered chan round trip", kHops,
            [&] { runtime.block_on(ping_pong(kHops, 64)); });

    measure("chan 1p1c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput, 1, 1)); });
    measure("chan 8p8c throughput", kThroughput,
            [&] { runtime.block_on(chan_throughput(kThroughput / 8, 8, 8)); });

    std::printf("%-30s %14s %16s\n", "benchmark", "ns/op", "ops/sec");
    std::printf("%-30s %14s %16s\n", "------------------------------", "--------------",
                "----------------");
    for (const auto& r : results) {
        std::printf("%-30s %14.1f %16.0f\n", r.name, r.ns_per_op, r.ops_per_sec);
    }
    return 0;
}
