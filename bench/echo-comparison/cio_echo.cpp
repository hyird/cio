// cio echo server — one task per connection.
//
// Unlike the asio server next to it, this is not shared-nothing: cio is a
// work-stealing M:N scheduler with one shared reactor, the same architecture Go
// uses. There is a single acceptor and connections land on whichever worker has
// capacity. That is the design being measured.
//
//     ./cio_echo <port> <workers>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <span>

#include "cio/cio.hpp"

namespace net = cio::net;

namespace {

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

cio::Task<> serve(net::TcpStream stream) {
    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        burn_microseconds(static_cast<unsigned>(buffer[0]));
        if (auto written = co_await stream.write_all(std::span(buffer, *n)); !written) break;
    }
}

cio::Task<int> run(std::uint16_t port) {
    auto listener = net::TcpListener::bind(net::SocketAddr::any_v4(port));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 1;
    }
    std::printf("cio echo server on %s\n",
                listener->local_addr().value().to_string().c_str());
    std::fflush(stdout);

    for (;;) {
        auto conn = co_await listener->accept();
        if (!conn) co_return 1;
        conn->set_nodelay(true);
        cio::go(serve(std::move(*conn)));
    }
}

}  // namespace

int main(int argc, char** argv) {
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9100);
    cio::RuntimeOptions options;
    options.worker_threads = static_cast<std::size_t>(argc > 2 ? std::atoi(argv[2]) : 8);

    cio::Runtime runtime(options);
    std::printf("cio echo server — %zu workers\n", runtime.worker_count());
    std::fflush(stdout);
    return runtime.block_on(run(port));
}
