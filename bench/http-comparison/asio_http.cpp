// Minimal HTTP/1.1 server on Boost.Asio, shared-nothing.
//
// Same architecture as asio_echo_callback.cpp next door: one io_context per
// thread, each with its own SO_REUSEPORT acceptor, so a connection is handled
// entirely by the thread that accepted it — no shared queue, no stealing, no
// cross-thread wakeups. This is the contrasting design cio is measured against,
// and callbacks rather than awaitables because that is asio at its fastest.
//
//     ./asio_http <port> <threads>
#include <boost/asio.hpp>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "http_common.hpp"

namespace asio = boost::asio;
using asio::ip::tcp;

namespace {

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
        socket_.async_read_some(
            asio::buffer(buffer_), [this, self](boost::system::error_code ec, std::size_t n) {
                if (ec || n == 0) return;
                const int requests = splitter_.feed(buffer_, n);
                if (requests <= 0) {
                    read();
                    return;
                }
                write(requests);
            });
    }

    void write(int requests) {
        auto self = shared_from_this();
        // One writev of `requests` copies would diverge from the other servers,
        // which write one response per request; keep the shapes identical.
        pending_ = requests;
        write_one();
    }

    void write_one() {
        auto self = shared_from_this();
        asio::async_write(socket_,
                          asio::buffer(bench::kResponse, bench::kResponseLen),
                          [this, self](boost::system::error_code ec, std::size_t) {
                              if (ec) return;
                              if (--pending_ > 0) {
                                  write_one();
                                  return;
                              }
                              read();
                          });
    }

    tcp::socket socket_;
    char buffer_[2048];
    bench::RequestSplitter splitter_;
    int pending_ = 0;
};

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
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9302);
    const int threads = argc > 2 ? std::atoi(argv[2]) : 8;

    std::vector<std::unique_ptr<Shard>> shards;
    shards.reserve(static_cast<std::size_t>(threads));
    for (int i = 0; i < threads; ++i) {
        shards.push_back(std::make_unique<Shard>());
        shards.back()->open(port);
    }

    std::printf("asio http server (callbacks, shared-nothing) on 0.0.0.0:%u — %d shards\n",
                port, threads);
    std::fflush(stdout);

    for (auto& shard : shards) {
        shard->thread = std::thread([s = shard.get()] { s->context.run(); });
    }
    for (auto& shard : shards) shard->thread.join();
    return 0;
}
