// Closed-loop TCP echo load generator, shared by every server under test.
//
// Methodology notes, because a benchmark's credibility lives in these:
//
//  * Closed loop. Each connection sends one request and waits for its echo
//    before sending the next, so offered load is bounded by the server's
//    response and nothing queues up in kernel buffers pretending to be
//    throughput.
//  * Warm-up first. Connections are established and traffic runs for a few
//    seconds before any measurement starts, so connection setup, page faults
//    and allocator warm-up stay out of the numbers.
//  * Per-connection histograms, merged at the end. No atomics on the hot path,
//    so measuring does not perturb what is measured.
//  * It reports its own CPU usage. If the generator saturates its cores, the
//    result says more about the generator than the server, and the run script
//    prints both so that is visible rather than assumed.
//
//     ./loadgen <host> <port> <connections> <warmup_s> <duration_s> [payload] [workers]
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <sys/resource.h>

#include "cio/cio.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

// Two-tier linear histogram: 1 us resolution to 2 ms, 64 us to 133 ms. Fine
// enough for tail percentiles without the machinery of a real HDR histogram.
constexpr std::size_t kFineBuckets = 2048;    // 1 us each
constexpr std::size_t kCoarseBuckets = 2048;  // 64 us each
constexpr std::size_t kBuckets = kFineBuckets + kCoarseBuckets;

struct Histogram {
    std::vector<std::uint64_t> buckets = std::vector<std::uint64_t>(kBuckets, 0);
    std::uint64_t overflow = 0;
    std::uint64_t count = 0;
    std::uint64_t total_ns = 0;
    std::uint64_t max_ns = 0;

    void record(std::int64_t ns) {
        ++count;
        total_ns += static_cast<std::uint64_t>(ns);
        if (static_cast<std::uint64_t>(ns) > max_ns) max_ns = static_cast<std::uint64_t>(ns);

        const std::int64_t us = ns / 1000;
        if (us < static_cast<std::int64_t>(kFineBuckets)) {
            ++buckets[static_cast<std::size_t>(us)];
        } else if (us < static_cast<std::int64_t>(kFineBuckets + kCoarseBuckets * 64)) {
            ++buckets[kFineBuckets +
                      static_cast<std::size_t>((us - static_cast<std::int64_t>(kFineBuckets)) / 64)];
        } else {
            ++overflow;
        }
    }

    void merge(const Histogram& other) {
        for (std::size_t i = 0; i < kBuckets; ++i) buckets[i] += other.buckets[i];
        overflow += other.overflow;
        count += other.count;
        total_ns += other.total_ns;
        if (other.max_ns > max_ns) max_ns = other.max_ns;
    }

    // Upper edge of the bucket holding the requested quantile, in microseconds.
    double percentile(double q) const {
        if (count == 0) return 0;
        const std::uint64_t target = static_cast<std::uint64_t>(q * static_cast<double>(count));
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < kBuckets; ++i) {
            seen += buckets[i];
            if (seen >= target) {
                return i < kFineBuckets
                           ? static_cast<double>(i) + 1.0
                           : static_cast<double>(kFineBuckets + (i - kFineBuckets) * 64 + 64);
            }
        }
        return static_cast<double>(max_ns) / 1000.0;
    }
};

std::atomic<bool> g_measuring{false};
std::atomic<bool> g_stop{false};
std::atomic<long> g_connected{0};
std::atomic<long> g_failed{0};

double process_cpu_seconds() {
    rusage usage{};
    ::getrusage(RUSAGE_SELF, &usage);
    return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
           static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}

cio::Task<> connection(net::SocketAddr target, std::size_t payload, Histogram* histogram) {
    auto stream = co_await net::TcpStream::connect(target);
    if (!stream) {
        g_failed.fetch_add(1, std::memory_order_relaxed);
        co_return;
    }
    stream->set_nodelay(true);
    g_connected.fetch_add(1, std::memory_order_relaxed);

    std::vector<std::byte> request(payload, std::byte{0x5A});
    std::vector<std::byte> response(payload);

    while (!g_stop.load(std::memory_order_relaxed)) {
        const auto started = cio::Clock::now();

        if (!(co_await stream->write_all(request))) break;

        std::size_t got = 0;
        bool broken = false;
        while (got < payload) {
            auto n = co_await stream->read(std::span(response).subspan(got));
            if (!n || *n == 0) {
                broken = true;
                break;
            }
            got += *n;
        }
        if (broken) break;

        if (g_measuring.load(std::memory_order_relaxed)) {
            histogram->record(cio::to_ns(cio::Clock::now() - started));
        }
    }
}

cio::Task<int> run(std::string host, std::uint16_t port, int connections, int warmup_s,
                   int duration_s, std::size_t payload) {
    auto target = net::SocketAddr::parse(host, port);
    if (!target) {
        std::fprintf(stderr, "bad host %s\n", host.c_str());
        co_return 1;
    }

    std::vector<std::unique_ptr<Histogram>> histograms;
    histograms.reserve(static_cast<std::size_t>(connections));
    for (int i = 0; i < connections; ++i) histograms.push_back(std::make_unique<Histogram>());

    cio::TaskGroup clients;
    for (int i = 0; i < connections; ++i) {
        clients.spawn(connection(*target, payload, histograms[static_cast<std::size_t>(i)].get()));
    }

    co_await cio::sleep(std::chrono::seconds(warmup_s));
    if (g_connected.load() == 0) {
        std::fprintf(stderr, "no connections established\n");
        g_stop.store(true, std::memory_order_relaxed);
        co_await clients.join();
        co_return 1;
    }

    const double cpu_before = process_cpu_seconds();
    const auto measure_start = cio::Clock::now();
    g_measuring.store(true, std::memory_order_relaxed);

    co_await cio::sleep(std::chrono::seconds(duration_s));

    g_measuring.store(false, std::memory_order_relaxed);
    const double elapsed = std::chrono::duration<double>(cio::Clock::now() - measure_start).count();
    const double cpu_after = process_cpu_seconds();

    g_stop.store(true, std::memory_order_relaxed);
    co_await clients.join();

    Histogram merged;
    for (const auto& h : histograms) merged.merge(*h);

    const double throughput = static_cast<double>(merged.count) / elapsed;
    const double client_cpu = cpu_after - cpu_before;

    // Machine-readable line for the run script, then the human summary.
    std::printf("RESULT rps=%.0f count=%llu elapsed=%.3f p50=%.1f p90=%.1f p99=%.1f "
                "p999=%.1f max=%.1f mean=%.1f client_cpu=%.2f conns=%ld failed=%ld\n",
                throughput, static_cast<unsigned long long>(merged.count), elapsed,
                merged.percentile(0.50), merged.percentile(0.90), merged.percentile(0.99),
                merged.percentile(0.999), static_cast<double>(merged.max_ns) / 1000.0,
                merged.count ? static_cast<double>(merged.total_ns) / static_cast<double>(merged.count) / 1000.0 : 0.0,
                client_cpu, g_connected.load(), g_failed.load());
    std::fflush(stdout);
    co_return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port = static_cast<std::uint16_t>(argc > 2 ? std::atoi(argv[2]) : 9100);
    const int connections = argc > 3 ? std::atoi(argv[3]) : 256;
    const int warmup_s = argc > 4 ? std::atoi(argv[4]) : 3;
    const int duration_s = argc > 5 ? std::atoi(argv[5]) : 10;
    const auto payload = static_cast<std::size_t>(argc > 6 ? std::atoi(argv[6]) : 128);

    cio::RuntimeOptions options;
    options.worker_threads = static_cast<std::size_t>(argc > 7 ? std::atoi(argv[7]) : 16);

    cio::Runtime runtime(options);
    return runtime.block_on(run(host, port, connections, warmup_s, duration_s, payload));
}
