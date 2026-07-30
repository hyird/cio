// A TCP echo server. Compare it to the Go version you would write — the shape
// is the same, and there is no thread, no event loop and no callback in sight.
//
//     ./echo_server 9000
//     nc 127.0.0.1 9000
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>

#include "cio/cio.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

cio::Task<> serve(net::TcpConn stream) {
    // One task per connection, exactly like one goroutine per connection. At
    // 100k connections this costs 100k coroutine frames, not 100k stacks.
    const auto peer = stream.remote_addr();
    std::byte buffer[16 * 1024];

    for (;;) {
        stream.set_read_timeout(60s);
        auto n = co_await stream.read(buffer);
        if (!n) {
            if (n.error().is(cio::Errc::timed_out)) {
                std::printf("[%s] idle timeout\n", peer ? peer->to_string().c_str() : "?");
            }
            break;
        }
        if (*n == 0) break;  // clean EOF

        if (auto written = co_await stream.write(std::span(buffer, *n)); !written) break;
    }
    std::printf("[%s] closed\n", peer ? peer->to_string().c_str() : "?");
}

CIO_MAIN_ARGS(argc, argv) {
    const std::uint16_t port =
        argc > 1 ? static_cast<std::uint16_t>(std::atoi(argv[1])) : 9000;

    auto listener = net::TcpListener::listen(net::SocketAddr::any_v4(port));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 1;
    }
    std::printf("listening on %s\n", listener->local_addr().value().to_string().c_str());
    std::fflush(stdout);

    for (;;) {
        auto conn = co_await listener->accept();
        if (!conn) {
            std::fprintf(stderr, "accept failed: %s\n", conn.error().message().c_str());
            co_return 1;
        }
        conn->set_nodelay(true);
        cio::go(serve(std::move(*conn)));
    }
}
