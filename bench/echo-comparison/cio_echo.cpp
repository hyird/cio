// cio echo server — one task per connection.
//
// cio uses a work-stealing M:N scheduler with one epoll/eventfd shard per
// worker. There is one public acceptor; accepted descriptors are distributed
// across private worker shards and published FIFO backlog remains stealable.
//
//     ./cio_echo <port> <workers>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <vector>

#include "cio/cio.hpp"

namespace net = cio::net;

namespace {

// Connection-to-shard mapping, recorded so a skew result can say whether heavy
// connections landed unevenly instead of leaving placement an untestable
// explanation. A task starts on the shard accept() selected for it, so reading
// the worker id at entry — before the first suspension — is that placement.
//
// Work stealing may move the task afterwards; this deliberately measures where
// it was *placed*, which is what the skew hypothesis is about.
struct ShardStats {
    std::atomic<std::uint64_t> connections{0};
    std::atomic<std::uint64_t> heavy_connections{0};
    std::atomic<std::uint64_t> requests{0};
    std::atomic<std::uint64_t> burn_us{0};
};

std::vector<ShardStats> g_shards;

// A connection is "heavy" when its per-request CPU cost is at the high end the
// skew sweep uses; the sweep sends 0 for light and 50 for heavy.
constexpr unsigned kHeavyThresholdUs = 10;

unsigned current_shard() noexcept {
    auto* scheduler = cio::detail::current_scheduler();
    if (scheduler == nullptr) return 0;
    const auto id = scheduler->current_worker_id();
    return id == cio::detail::kInvalidWorkerId ? 0 : static_cast<unsigned>(id);
}

void report_shard_mapping() {
    std::printf("\n# connection-to-shard mapping\n");
    std::printf("%-8s %12s %12s %12s %14s\n", "shard", "conns", "heavy",
                "requests", "burn_us");
    std::uint64_t total_conns = 0;
    std::uint64_t total_heavy = 0;
    for (std::size_t i = 0; i < g_shards.size(); ++i) {
        const auto conns = g_shards[i].connections.load();
        const auto heavy = g_shards[i].heavy_connections.load();
        total_conns += conns;
        total_heavy += heavy;
        std::printf("%-8zu %12llu %12llu %12llu %14llu\n", i,
                    static_cast<unsigned long long>(conns),
                    static_cast<unsigned long long>(heavy),
                    static_cast<unsigned long long>(g_shards[i].requests.load()),
                    static_cast<unsigned long long>(g_shards[i].burn_us.load()));
    }
    std::printf("%-8s %12llu %12llu\n", "total",
                static_cast<unsigned long long>(total_conns),
                static_cast<unsigned long long>(total_heavy));
    std::fflush(stdout);
}

// Per-request CPU work, in microseconds, taken from the first request byte.
//
// A busy-wait on the clock rather than a counted loop: a loop compiles to
// different amounts of work under gcc and the Go compiler, which would make the
// servers incomparable. Spinning until a deadline is identical by construction.
inline void burn_microseconds(unsigned us) {
    if (us == 0) return;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::microseconds(us);
    while (std::chrono::steady_clock::now() < deadline) {
    }
}

cio::Task<> serve(net::TcpConn stream) {
    // Read before the first suspension: this is the shard accept() placed us on.
    const unsigned shard = current_shard();
    ShardStats* const stats =
        shard < g_shards.size() ? &g_shards[shard] : nullptr;
    if (stats != nullptr) stats->connections.fetch_add(1, std::memory_order_relaxed);
    bool counted_weight = false;

    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        const auto burn = static_cast<unsigned>(buffer[0]);
        if (stats != nullptr) {
            if (!counted_weight) {
                counted_weight = true;
                if (burn >= kHeavyThresholdUs) {
                    stats->heavy_connections.fetch_add(1, std::memory_order_relaxed);
                }
            }
            stats->requests.fetch_add(1, std::memory_order_relaxed);
            stats->burn_us.fetch_add(burn, std::memory_order_relaxed);
        }
        burn_microseconds(burn);
        if (auto written = co_await stream.write(std::span(buffer, *n)); !written) break;
    }
}

cio::Task<> accept_loop(net::TcpListener listener) {
    for (;;) {
        auto conn = co_await listener.accept();
        if (!conn) co_return;
        conn->set_nodelay(true);
        cio::go(serve(std::move(*conn)));
    }
}

cio::Task<int> run(std::uint16_t port) {
    auto listener = net::TcpListener::listen(net::SocketAddr::any_v4(port));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 1;
    }
    std::printf("cio echo server on %s\n",
                listener->addr().value().to_string().c_str());
    std::fflush(stdout);

    cio::go(accept_loop(std::move(*listener)));

    // The harness stops the server with a signal; dump the mapping on the way
    // out so a skew run produces placement evidence rather than a hypothesis.
    auto signals = cio::signal::SignalSet::subscribe({SIGINT, SIGTERM});
    if (!signals) {
        // Signals were not blocked before the runtime started; keep serving.
        for (;;) co_await cio::sleep(std::chrono::hours(1));
    }
    const auto received = co_await signals->recv();
    report_shard_mapping();
    co_return received ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9100);
    cio::RuntimeOptions options;
    options.worker_threads = static_cast<std::size_t>(argc > 2 ? std::atoi(argv[2]) : 8);

    // Must happen before the runtime creates any thread.
    (void)cio::signal::block({SIGINT, SIGTERM});

    cio::Runtime runtime(options);
    g_shards = std::vector<ShardStats>(runtime.worker_count());
    std::printf("cio echo server — %zu workers\n", runtime.worker_count());
    std::fflush(stdout);
    return runtime.block_on(run(port));
}
