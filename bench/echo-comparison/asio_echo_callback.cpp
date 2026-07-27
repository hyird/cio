// Boost.Asio echo server, shared-nothing.
//
// One io_context per thread, each with its own SO_REUSEPORT acceptor on the
// same port. The kernel spreads incoming connections across the listening
// sockets, and from then on a connection is handled entirely by the thread that
// accepted it: no shared queue, no work stealing, no cross-thread wakeups.
//
// io_context{1} tells asio the context is single-threaded so it can drop the
// internal locking. This is asio at its fastest — callbacks, not coroutines —
// which is the version worth measuring cio against.
//
//     ./asio_echo_callback <port> <threads>
#include <boost/asio.hpp>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using asio::ip::tcp;

namespace {

constexpr std::size_t kBufferSize = 4096;

class Session : public std::enable_shared_from_this<Session> {
public:
    explicit Session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        socket_.set_option(tcp::no_delay(true));
        read();
    }

private:
    void read() {
        auto self = shared_from_this();
        socket_.async_read_some(asio::buffer(buffer_),
                                [this, self](boost::system::error_code ec, std::size_t n) {
                                    if (ec || n == 0) return;
                                    write(n);
                                });
    }

    void write(std::size_t n) {
        auto self = shared_from_this();
        asio::async_write(socket_, asio::buffer(buffer_, n),
                          [this, self](boost::system::error_code ec, std::size_t) {
                              if (ec) return;
                              read();
                          });
    }

    tcp::socket socket_;
    char buffer_[kBufferSize];
};

// One shard = one thread + its own reactor + its own acceptor. Nothing here is
// shared with any other shard.
struct Shard {
    asio::io_context context{1};
    tcp::acceptor acceptor{context};
    std::thread thread;

    void open(std::uint16_t port) {
        const tcp::endpoint endpoint(tcp::v4(), port);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(asio::socket_base::reuse_address(true));
        // The load-bearing option: without SO_REUSEPORT the shards cannot each
        // hold their own listening socket, and accept becomes a shared point.
        int one = 1;
        ::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
        acceptor.bind(endpoint);
        acceptor.listen(asio::socket_base::max_listen_connections);
        accept();
    }

    void accept() {
        acceptor.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) std::make_shared<Session>(std::move(socket))->start();
            accept();
        });
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

    std::printf("asio echo server (callbacks, shared-nothing) on 0.0.0.0:%u — %d shards\n",
                port, threads);
    std::fflush(stdout);

    for (auto& shard : shards) {
        shard->thread = std::thread([s = shard.get()] { s->context.run(); });
    }
    for (auto& shard : shards) shard->thread.join();
    return 0;
}
