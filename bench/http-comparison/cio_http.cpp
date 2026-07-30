// Minimal HTTP/1.1 server on cio — one task per connection.
//
// Deliberately hand-rolled rather than built on an HTTP library: the point of
// this benchmark is to compare runtimes under a third-party load generator, and
// a library difference would be indistinguishable from a runtime difference.
//
//     ./cio_http <port> <workers>
#include <cstdio>
#include <cstdlib>
#include <span>

#include "cio/cio.hpp"
#include "http_common.hpp"

namespace net = cio::net;

namespace {

cio::Task<> serve(net::TcpConn stream) {
    std::byte buffer[2048];
    bench::RequestSplitter splitter;
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;

        const int requests =
            splitter.feed(reinterpret_cast<const char*>(buffer), *n);
        for (int i = 0; i < requests; ++i) {
            auto written = co_await stream.write(std::span(
                reinterpret_cast<const std::byte*>(bench::kResponse), bench::kResponseLen));
            if (!written) co_return;
        }
    }
}

cio::Task<int> run(std::uint16_t port) {
    auto listener = net::TcpListener::listen(net::SocketAddr::any_v4(port));
    if (!listener) {
        std::fprintf(stderr, "bind failed: %s\n", listener.error().message().c_str());
        co_return 1;
    }
    std::printf("cio http server on %s\n",
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
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9300);
    cio::RuntimeOptions options;
    options.worker_threads = static_cast<std::size_t>(argc > 2 ? std::atoi(argv[2]) : 8);

    cio::Runtime runtime(options);
    std::printf("cio http server — %zu workers\n", runtime.worker_count());
    std::fflush(stdout);
    return runtime.block_on(run(port));
}
