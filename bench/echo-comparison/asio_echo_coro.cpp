// Boost.Asio echo server, shared-nothing, C++20 coroutines.
//
// Same shared-nothing structure as the callback version — one io_context and
// one SO_REUSEPORT acceptor per thread — but written with asio::awaitable, so
// it is the apples-to-apples comparison against cio on programming model as
// well as on throughput. The callback version is the same server without the
// coroutine machinery, so the gap between the two is asio's own coroutine
// overhead.
//
//     ./asio_echo_coro <port> <threads>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using asio::ip::tcp;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;

namespace {

constexpr std::size_t kBufferSize = 4096;
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

awaitable<void> serve(tcp::socket socket) {
    try {
        socket.set_option(tcp::no_delay(true));
        char buffer[kBufferSize];
        for (;;) {
            const std::size_t n =
                co_await socket.async_read_some(asio::buffer(buffer), use_awaitable);
            if (n == 0) break;
            burn_microseconds(static_cast<unsigned char>(buffer[0]));
            co_await asio::async_write(socket, asio::buffer(buffer, n), use_awaitable);
        }
    } catch (const std::exception&) {
        // Peer closed; asio reports that as an exception on this completion token.
    }
}

awaitable<void> accept_loop(tcp::acceptor& acceptor) {
    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
        co_spawn(acceptor.get_executor(), serve(std::move(socket)), detached);
    }
}

struct Shard {
    asio::io_context context{1};
    tcp::acceptor acceptor{context};
    std::thread thread;

    void open(std::uint16_t port) {
        const tcp::endpoint endpoint(tcp::v4(), port);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(asio::socket_base::reuse_address(true));
        int one = 1;
        ::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
        acceptor.bind(endpoint);
        acceptor.listen(asio::socket_base::max_listen_connections);
        co_spawn(context, accept_loop(acceptor), detached);
    }
};

}  // namespace

int main(int argc, char** argv) {
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9100);
    const int threads = argc > 2 ? std::atoi(argv[2]) : 8;

    std::vector<std::unique_ptr<Shard>> shards;
    shards.reserve(static_cast<std::size_t>(threads));
    for (int i = 0; i < threads; ++i) {
        shards.push_back(std::make_unique<Shard>());
        shards.back()->open(port);
    }

    std::printf("asio echo server (coroutines, shared-nothing) on 0.0.0.0:%u — %d shards\n",
                port, threads);
    std::fflush(stdout);

    for (auto& shard : shards) {
        shard->thread = std::thread([s = shard.get()] { s->context.run(); });
    }
    for (auto& shard : shards) shard->thread.join();
    return 0;
}
