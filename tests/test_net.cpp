#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

cio::Task<> echo_connection(net::TcpStream stream) {
    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        if (auto written = co_await stream.write_all(std::span(buffer, *n)); !written) break;
    }
}

cio::Task<> echo_server(net::TcpListener listener, cio::CancelToken token) {
    for (;;) {
        auto conn = co_await listener.accept();
        if (!conn) break;  // Errc::closed when the listener is torn down
        cio::go(echo_connection(std::move(*conn)));
        if (token.cancelled()) break;
    }
}

void test_echo_round_trip() {
    auto body = []() -> cio::Task<std::string> {
        auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->local_addr().value();

        cio::CancelSource stop;
        cio::go(echo_server(std::move(*listener), stop.token()));

        auto client = co_await net::TcpStream::connect(addr);
        CIO_CHECK(client.has_value());
        if (!client) co_return "";

        const std::string message = "hello cio";
        auto written = co_await client->write_all(bytes_of(message));
        CIO_CHECK(written.has_value());

        std::string received;
        std::byte buffer[64];
        while (received.size() < message.size()) {
            auto n = co_await client->read(buffer);
            if (!n || *n == 0) break;
            received.append(reinterpret_cast<const char*>(buffer), *n);
        }
        stop.cancel();
        co_return received;
    };
    CIO_CHECK_EQ(cio::run(body()), std::string("hello cio"));
}

// The read deadline must interrupt a parked read, and the socket must stay
// usable after the deadline is cleared.
void test_read_deadline() {
    auto body = []() -> cio::Task<bool> {
        auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->local_addr().value();

        // A server that accepts and then stays silent.
        auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpStream> {
            auto conn = co_await l.accept();
            co_return std::move(conn.value());
        }(std::move(*listener)));

        auto client = co_await net::TcpStream::connect(addr);
        CIO_CHECK(client.has_value());
        auto server_side = co_await accepted;

        std::byte buffer[16];
        client->set_read_timeout(30ms);
        const auto started = cio::Clock::now();
        auto timed_out = co_await client->read(buffer);
        const auto elapsed = cio::Clock::now() - started;

        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
        CIO_CHECK(elapsed >= 25ms);

        // Every further read fails immediately until the deadline is reset —
        // Go's semantics.
        auto still_timed_out = co_await client->read(buffer);
        CIO_CHECK(!still_timed_out.has_value());

        client->clear_read_deadline();
        co_await server_side.write_all(bytes_of("ok"));
        auto n = co_await client->read(buffer);
        CIO_CHECK(n.has_value());
        CIO_CHECK_EQ(*n, std::size_t{2});
        co_return n.has_value() && *n == 2;
    };
    CIO_CHECK(cio::run(body()));
}

void test_connect_refused() {
    auto body = []() -> cio::Task<bool> {
        // Bind and immediately drop, so the port is almost certainly closed.
        std::uint16_t port = 0;
        {
            auto probe = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
            CIO_CHECK(probe.has_value());
            port = probe->local_addr().value().port();
        }
        auto result = co_await net::TcpStream::connect(net::SocketAddr::loopback_v4(port));
        CIO_CHECK(!result.has_value());
        co_return !result.has_value();
    };
    CIO_CHECK(cio::run(body()));
}

// Many concurrent connections, each doing several round trips: this is the
// test that actually exercises the edge-triggered readiness state machine.
void test_many_concurrent_connections() {
    static constexpr int kClients = 64;
    static constexpr int kRoundTrips = 20;

    auto body = []() -> cio::Task<int> {
        auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->local_addr().value();

        cio::CancelSource stop;
        cio::go(echo_server(std::move(*listener), stop.token()));

        cio::TaskGroup clients;
        auto successes = cio::make_chan<int>(kClients);

        for (int i = 0; i < kClients; ++i) {
            clients.spawn([](net::SocketAddr target, cio::Chan<int> out) -> cio::Task<> {
                auto stream = co_await net::TcpStream::connect(target);
                if (!stream) {
                    co_await out.send(0);
                    co_return;
                }
                int ok = 0;
                std::byte buffer[32];
                for (int r = 0; r < kRoundTrips; ++r) {
                    const std::string payload = "ping" + std::to_string(r);
                    if (!(co_await stream->write_all(bytes_of(payload)))) break;

                    std::size_t got = 0;
                    while (got < payload.size()) {
                        auto n = co_await stream->read(std::span(buffer + got,
                                                                 sizeof(buffer) - got));
                        if (!n || *n == 0) break;
                        got += *n;
                    }
                    if (got != payload.size()) break;
                    if (std::memcmp(buffer, payload.data(), payload.size()) != 0) break;
                    ++ok;
                }
                co_await out.send(ok);
            }(addr, successes));
        }

        co_await clients.join();

        int total = 0;
        for (int i = 0; i < kClients; ++i) total += *co_await successes.recv();
        stop.cancel();
        co_return total;
    };
    CIO_CHECK_EQ(cio::run(body()), kClients * kRoundTrips);
}

void test_close_wakes_a_parked_reader() {
    auto body = []() -> cio::Task<bool> {
        auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->local_addr().value();

        auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpStream> {
            auto conn = co_await l.accept();
            co_return std::move(conn.value());
        }(std::move(*listener)));

        auto client = co_await net::TcpStream::connect(addr);
        CIO_CHECK(client.has_value());
        auto server_side = co_await accepted;

        // Server hangs up; the parked client read must observe EOF, not hang.
        cio::go([](net::TcpStream s) -> cio::Task<> {
            co_await cio::sleep(20ms);
            s.close();
        }(std::move(server_side)));

        std::byte buffer[16];
        auto n = co_await client->read(buffer);
        const bool eof = n.has_value() && *n == 0;
        CIO_CHECK(eof);
        co_return eof;
    };
    CIO_CHECK(cio::run(body()));
}

void test_udp_round_trip() {
    auto body = []() -> cio::Task<std::string> {
        auto server = net::UdpSocket::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(server.has_value());
        const auto server_addr = server->local_addr().value();

        auto client = net::UdpSocket::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(client.has_value());

        auto echo = cio::spawn([](net::UdpSocket s) -> cio::Task<std::string> {
            std::byte buffer[256];
            net::SocketAddr from;
            auto n = co_await s.recv_from(buffer, from);
            if (!n) co_return "";
            co_await s.send_to(std::span(buffer, *n), from);
            co_return std::string(reinterpret_cast<const char*>(buffer), *n);
        }(std::move(*server)));

        co_await client->send_to(bytes_of("udp hello"), server_addr);

        std::byte buffer[256];
        net::SocketAddr from;
        auto n = co_await client->recv_from(buffer, from);
        CIO_CHECK(n.has_value());
        co_await echo;
        co_return std::string(reinterpret_cast<const char*>(buffer), n.value_or(0));
    };
    CIO_CHECK_EQ(cio::run(body()), std::string("udp hello"));
}

void test_resolve_localhost() {
    auto body = []() -> cio::Task<bool> {
        auto addresses = co_await net::resolve("localhost", 80);
        CIO_CHECK(addresses.has_value());
        co_return addresses.has_value() && !addresses->empty();
    };
    CIO_CHECK(cio::run(body()));
}

void test_blocking_pool_offload() {
    auto body = []() -> cio::Task<int> {
        // 32 tasks that each park a real thread; the runtime's workers must
        // stay free the whole time.
        cio::TaskGroup group;
        auto results = cio::make_chan<int>(32);
        for (int i = 0; i < 32; ++i) {
            group.spawn([](cio::Chan<int> out, int value) -> cio::Task<> {
                auto doubled = co_await cio::blocking([value] {
                    std::this_thread::sleep_for(20ms);
                    return value * 2;
                });
                co_await out.send(doubled);
            }(results, i));
        }
        co_await group.join();

        int total = 0;
        for (int i = 0; i < 32; ++i) total += *co_await results.recv();
        co_return total;
    };
    CIO_CHECK_EQ(cio::run(body()), 31 * 32);
}

}  // namespace

int main() {
    RUN_TEST(test_echo_round_trip);
    RUN_TEST(test_read_deadline);
    RUN_TEST(test_connect_refused);
    RUN_TEST(test_many_concurrent_connections);
    RUN_TEST(test_close_wakes_a_parked_reader);
    RUN_TEST(test_udp_round_trip);
    RUN_TEST(test_resolve_localhost);
    RUN_TEST(test_blocking_pool_offload);
    return cio_test::summary();
}
