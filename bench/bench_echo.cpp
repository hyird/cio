// End-to-end TCP throughput: request/response round trips per second, which is
// what a "one task per connection" server actually costs.
//
// Three modes, because the convenient one is also the least honest:
//
//     ./bench_echo                                  in-process (default)
//     ./bench_echo server <port> [workers]          server only
//     ./bench_echo client <host> <port> <conns> <reqs> [workers]
//
// In-process is convenient but the client tasks and the server tasks compete
// for the same 24 cores and the same runtime, so the number it prints is a
// lower bound that says as much about the load generator as about the server.
// Split the two across processes (or machines) to measure the server.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>
#include <string>

#include "cio/cio.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

constexpr std::size_t kPayload = 128;

cio::Task<> serve(net::TcpConn stream) {
    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        if (auto written = co_await stream.write(std::span(buffer, *n)); !written) break;
    }
}

cio::Task<> accept_loop(net::TcpListener listener, cio::CancelToken token) {
    while (!token.cancelled()) {
        auto conn = co_await listener.accept();
        if (!conn) break;
        conn->set_nodelay(true);
        cio::go(serve(std::move(*conn)));
    }
}

cio::Task<long> client(net::SocketAddr target, long requests) {
    auto stream = co_await net::TcpConn::dial(target);
    if (!stream) co_return 0;
    stream->set_nodelay(true);

    std::byte request[kPayload];
    std::memset(request, 0x5A, sizeof(request));
    std::byte response[kPayload];

    long completed = 0;
    for (long i = 0; i < requests; ++i) {
        if (!(co_await stream->write(request))) break;

        std::size_t got = 0;
        while (got < kPayload) {
            auto n = co_await stream->read(std::span(response + got, kPayload - got));
            if (!n || *n == 0) break;
            got += *n;
        }
        if (got != kPayload) break;
        ++completed;
    }
    co_return completed;
}

cio::Task<long> run_benchmark(int connections, long requests) {
    auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 0;
    }
    const auto addr = listener->local_addr().value();

    cio::CancelSource stop;
    cio::go(accept_loop(std::move(*listener), stop.token()));

    std::vector<cio::JoinHandle<long>> clients;
    clients.reserve(static_cast<std::size_t>(connections));

    const auto started = cio::Clock::now();
    for (int i = 0; i < connections; ++i) clients.push_back(cio::spawn(client(addr, requests)));

    long total = 0;
    for (auto& handle : clients) total += co_await handle;
    const auto elapsed = cio::Clock::now() - started;

    stop.cancel();

    const double seconds = std::chrono::duration<double>(elapsed).count();
    std::printf("connections      %d\n", connections);
    std::printf("requests/conn    %ld\n", requests);
    std::printf("completed        %ld\n", total);
    std::printf("elapsed          %.3f s\n", seconds);
    std::printf("round trips/sec  %.0f\n", static_cast<double>(total) / seconds);
    std::printf("latency (avg)    %.1f us\n",
                seconds * 1e6 / (static_cast<double>(total) / connections));
    co_return total;
}

cio::Task<int> run_server(std::uint16_t port) {
    auto listener = net::TcpListener::listen(net::SocketAddr::any_v4(port));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 1;
    }
    std::printf("echo server on %s — ctrl-c to stop\n",
                listener->local_addr().value().to_string().c_str());
    // stdout is block-buffered when redirected, and a driver script waiting for
    // this line to know the server is up would otherwise wait forever.
    std::fflush(stdout);

    for (;;) {
        auto conn = co_await listener->accept();
        if (!conn) co_return 1;
        conn->set_nodelay(true);
        cio::go(serve(std::move(*conn)));
    }
}

cio::Task<int> run_client(std::string host, std::uint16_t port, int connections,
                          long requests) {
    auto target = net::SocketAddr::parse(host, port);
    if (!target) {
        auto resolved = co_await net::resolve(host, port);
        if (!resolved || resolved->empty()) {
            std::fprintf(stderr, "cannot resolve %s\n", host.c_str());
            co_return 1;
        }
        target = resolved->front();
    }

    std::vector<cio::JoinHandle<long>> clients;
    clients.reserve(static_cast<std::size_t>(connections));

    const auto started = cio::Clock::now();
    for (int i = 0; i < connections; ++i) {
        clients.push_back(cio::spawn(client(*target, requests)));
    }
    long total = 0;
    for (auto& handle : clients) total += co_await handle;
    const double seconds = std::chrono::duration<double>(cio::Clock::now() - started).count();

    std::printf("target           %s\n", target->to_string().c_str());
    std::printf("connections      %d\n", connections);
    std::printf("completed        %ld\n", total);
    std::printf("elapsed          %.3f s\n", seconds);
    std::printf("round trips/sec  %.0f\n", static_cast<double>(total) / seconds);
    std::printf("latency (avg)    %.1f us\n",
                seconds * 1e6 / (static_cast<double>(total) / connections));
    co_return total > 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "inproc";

    if (mode == "server") {
        const auto port = static_cast<std::uint16_t>(argc > 2 ? std::atoi(argv[2]) : 9100);
        cio::RuntimeOptions options;
        if (argc > 3) options.worker_threads = static_cast<std::size_t>(std::atoi(argv[3]));
        cio::Runtime runtime(options);
        std::printf("cio echo server — %zu workers\n", runtime.worker_count());
        return runtime.block_on(run_server(port));
    }

    if (mode == "client") {
        const std::string host = argc > 2 ? argv[2] : "127.0.0.1";
        const auto port = static_cast<std::uint16_t>(argc > 3 ? std::atoi(argv[3]) : 9100);
        const int connections = argc > 4 ? std::atoi(argv[4]) : 256;
        const long requests = argc > 5 ? std::atol(argv[5]) : 2000;
        cio::RuntimeOptions options;
        if (argc > 6) options.worker_threads = static_cast<std::size_t>(std::atoi(argv[6]));
        cio::Runtime runtime(options);
        std::printf("cio echo client — %zu workers\n\n", runtime.worker_count());
        return runtime.block_on(run_client(host, port, connections, requests));
    }

    const int connections = argc > 1 ? std::atoi(argv[1]) : 256;
    const long requests = argc > 2 ? std::atol(argv[2]) : 2000;
    cio::RuntimeOptions options;
    if (argc > 3) options.worker_threads = static_cast<std::size_t>(std::atoi(argv[3]));

    cio::Runtime runtime(options);
    std::printf("cio echo benchmark, in-process — %zu workers\n"
                "(client and server share these workers; see the header comment)\n\n",
                runtime.worker_count());
    runtime.block_on(run_benchmark(connections, requests));
    const auto metrics = cio::runtime_metrics();
    if (metrics.tasks_run != 0) {
        std::printf(
            "metrics          tasks=%llu parks=%llu poll=%llu/%llu "
            "events=%llu io_wakeups=%llu eventfd=%llu steals=%llu/%llu\n",
            static_cast<unsigned long long>(metrics.tasks_run),
            static_cast<unsigned long long>(metrics.parks),
            static_cast<unsigned long long>(metrics.polls_blocking),
            static_cast<unsigned long long>(metrics.polls_nonblocking),
            static_cast<unsigned long long>(metrics.poll_events),
            static_cast<unsigned long long>(metrics.poll_wakeups),
            static_cast<unsigned long long>(metrics.reactor_wakes),
            static_cast<unsigned long long>(metrics.steal_hits),
            static_cast<unsigned long long>(metrics.steal_attempts));
    }
    return 0;
}
