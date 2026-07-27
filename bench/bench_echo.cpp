// End-to-end TCP throughput: an echo server and its clients in one process,
// all on the runtime. Measures request/response round trips per second over
// loopback, which is what a "one task per connection" server actually costs.
//
//     ./bench_echo [connections] [requests_per_connection] [workers]
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

cio::Task<> serve(net::TcpStream stream) {
    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        if (auto written = co_await stream.write_all(std::span(buffer, *n)); !written) break;
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
    auto stream = co_await net::TcpStream::connect(target);
    if (!stream) co_return 0;
    stream->set_nodelay(true);

    std::byte request[kPayload];
    std::memset(request, 0x5A, sizeof(request));
    std::byte response[kPayload];

    long completed = 0;
    for (long i = 0; i < requests; ++i) {
        if (!(co_await stream->write_all(request))) break;

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
    auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
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

}  // namespace

int main(int argc, char** argv) {
    const int connections = argc > 1 ? std::atoi(argv[1]) : 256;
    const long requests = argc > 2 ? std::atol(argv[2]) : 2000;

    cio::RuntimeOptions options;
    if (argc > 3) options.worker_threads = static_cast<std::size_t>(std::atoi(argv[3]));

    cio::Runtime runtime(options);
    std::printf("cio echo benchmark — %zu workers\n\n", runtime.worker_count());
    runtime.block_on(run_benchmark(connections, requests));
    return 0;
}
