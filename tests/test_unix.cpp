#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <span>
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

std::string temp_socket_path() {
    static std::atomic<unsigned> counter{0};
    const char* base = std::getenv("TMPDIR");
    return std::string(base != nullptr ? base : "/tmp") + "/cio_unix_" +
           std::to_string(::getpid()) + "_" +
           std::to_string(counter.fetch_add(1)) + ".sock";
}

bool path_exists(const std::string& path) {
    struct ::stat st {};
    return ::stat(path.c_str(), &st) == 0;
}

// UnixConn must satisfy the same concept TcpConn does, or generic code cannot
// take either.
static_assert(net::Conn<net::UnixConn>);
static_assert(!net::Conn<net::UnixListener>);

void test_addr_parsing() {
    auto filesystem = net::UnixAddr::parse("/tmp/example.sock");
    CIO_CHECK(filesystem.has_value());
    CIO_CHECK(!filesystem->abstract());
    CIO_CHECK_EQ(filesystem->path(), std::string("/tmp/example.sock"));

    // A leading '@' is the Linux abstract namespace, as every Linux tool spells
    // it; the name lives in a namespace rather than the filesystem.
    auto abstract = net::UnixAddr::parse("@cio-test");
    CIO_CHECK(abstract.has_value());
    CIO_CHECK(abstract->abstract());
    CIO_CHECK_EQ(abstract->path(), std::string("cio-test"));

    CIO_CHECK(!net::UnixAddr::parse("").has_value());
    CIO_CHECK(!net::UnixAddr::parse("@").has_value());
    // sun_path is fixed, so an over-long name is rejected up front rather than
    // at bind time.
    CIO_CHECK(!net::UnixAddr::parse(std::string(200, 'x')).has_value());
}

void test_filesystem_round_trip() {
    const std::string path = temp_socket_path();
    auto body = [&path]() -> cio::Task<bool> {
        auto addr = net::UnixAddr::parse(path).value();
        auto listener = net::UnixListener::listen(addr);
        CIO_CHECK(listener.has_value());
        CIO_CHECK(path_exists(path));
        CIO_CHECK_EQ(listener->addr().value().path(), path);

        auto serving = cio::spawn([](net::UnixListener l) -> cio::Task<std::string> {
            auto conn = co_await l.accept();
            if (!conn) co_return std::string{};
            std::vector<std::byte> buffer(5);
            auto filled = co_await cio::io::read_full(
                *conn, std::span<std::byte>{buffer});
            if (!filled) co_return std::string{};
            if (!(co_await conn->write(bytes_of("world")))) {
                co_return std::string{};
            }
            co_return std::string(
                reinterpret_cast<const char*>(buffer.data()), buffer.size());
        }(std::move(*listener)));

        auto client = co_await net::UnixConn::dial(addr);
        CIO_CHECK(client.has_value());
        CIO_CHECK((co_await client->write(bytes_of("hello"))).has_value());

        std::vector<std::byte> reply(5);
        auto read = co_await cio::io::read_full(*client,
                                                std::span<std::byte>{reply});
        CIO_CHECK(read.has_value());
        CIO_CHECK_EQ(std::string(reinterpret_cast<const char*>(reply.data()), 5),
                     std::string("world"));
        CIO_CHECK_EQ(co_await serving, std::string("hello"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
    // close() removed the path it created, which is what lets a service restart.
    CIO_CHECK(!path_exists(path));
}

void test_abstract_namespace() {
    auto body = []() -> cio::Task<bool> {
        const std::string name =
            "@cio-abstract-" + std::to_string(::getpid());
        auto addr = net::UnixAddr::parse(name).value();
        auto listener = net::UnixListener::listen(addr);
        CIO_CHECK(listener.has_value());
        // An abstract address has no filesystem entry to leave behind.
        CIO_CHECK(!path_exists(name.substr(1)));

        auto serving = cio::spawn([](net::UnixListener l) -> cio::Task<bool> {
            auto conn = co_await l.accept();
            if (!conn) co_return false;
            co_return (co_await conn->write(bytes_of("ok"))).has_value();
        }(std::move(*listener)));

        auto client = co_await net::UnixConn::dial(addr);
        CIO_CHECK(client.has_value());
        std::vector<std::byte> reply(2);
        auto read = co_await cio::io::read_full(*client,
                                                std::span<std::byte>{reply});
        CIO_CHECK(read.has_value());
        CIO_CHECK(co_await serving);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A stale path from a previous run is the most common way a Unix-socket service
// fails to restart, so listen() clears it by default.
void test_stale_path_is_replaced() {
    const std::string path = temp_socket_path();
    auto body = [&path]() -> cio::Task<bool> {
        auto addr = net::UnixAddr::parse(path).value();
        {
            auto first = net::UnixListener::listen(addr);
            CIO_CHECK(first.has_value());
            // Leak the path deliberately: unlink it from the listener's
            // bookkeeping so close() leaves it behind, like a killed process.
            first->unlink();
            CIO_CHECK(!path_exists(path));
        }

        // Recreate a stale entry and confirm listen() takes it over.
        auto again = net::UnixListener::listen(addr);
        CIO_CHECK(again.has_value());
        CIO_CHECK(path_exists(path));

        auto third = net::UnixListener::listen(addr);
        CIO_CHECK(third.has_value());  // replaced the stale path

        // Opting out fails against a live path, which is the point of the flag.
        auto refused = net::UnixListener::listen(addr, 1024,
                                                 /*unlink_existing=*/false);
        CIO_CHECK(!refused.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
    ::unlink(path.c_str());
}

void test_dial_missing_and_cancelled() {
    auto body = []() -> cio::Task<bool> {
        auto missing =
            net::UnixAddr::parse(temp_socket_path() + ".absent").value();
        auto failed = co_await net::UnixConn::dial(missing);
        CIO_CHECK(!failed.has_value());
        CIO_CHECK(failed.error().is(ENOENT));

        cio::CancelSource stop;
        stop.cancel();
        auto cancelled = co_await net::UnixConn::dial(missing, stop.token());
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Cancellation binds to the descriptor for a Unix socket too, because it comes
// from the shared Socket base rather than being reimplemented per type.
void test_cancel_and_deadline_apply() {
    const std::string path = temp_socket_path();
    auto body = [&path]() -> cio::Task<bool> {
        auto addr = net::UnixAddr::parse(path).value();
        auto listener = net::UnixListener::listen(addr);
        CIO_CHECK(listener.has_value());

        auto accepted = cio::spawn([](net::UnixListener l)
                                       -> cio::Task<net::UnixConn> {
            auto conn = co_await l.accept();
            co_return conn ? std::move(*conn) : net::UnixConn{};
        }(std::move(*listener)));

        auto client = co_await net::UnixConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto server = co_await accepted;

        // Deadline.
        client->set_read_timeout(30ms);
        std::byte buffer[8];
        auto timed_out = co_await client->read(buffer);
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
        client->clear_read_deadline();

        // Cancellation, including waking an already-parked read.
        cio::CancelSource stop;
        client->set_cancel(stop.token());
        auto canceller = cio::spawn([](cio::CancelSource s) -> cio::Task<> {
            co_await cio::sleep(20ms);
            s.cancel();
        }(stop));
        auto cancelled = co_await client->read(buffer);
        co_await canceller;
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));

        // close_write gives the peer a clean EOF.
        client->clear_cancel();
        CIO_CHECK(client->close_write().has_value());
        std::byte peer_buffer[8];
        auto eof = co_await server.read(peer_buffer);
        CIO_CHECK(eof.has_value());
        CIO_CHECK_EQ(*eof, std::size_t{0});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
    ::unlink(path.c_str());
}

// ------------------------------------------------------- socket options ---

void test_tcp_socket_options() {
    auto body = []() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<bool> {
            auto conn = co_await l.accept();
            co_return conn.has_value();
        }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());

        CIO_CHECK(client->set_nodelay(true).has_value());
        CIO_CHECK(client->set_keepalive(true).has_value());
        CIO_CHECK(client->set_keepalive_period(30s).has_value());
        CIO_CHECK(client->set_linger(-1s).has_value());   // system default
        CIO_CHECK(client->set_linger(0s).has_value());    // discard, send RST
        CIO_CHECK(client->set_linger(5s).has_value());
        CIO_CHECK(client->set_read_buffer(64 * 1024).has_value());
        CIO_CHECK(client->set_write_buffer(64 * 1024).has_value());

        // A non-positive buffer size is rejected rather than passed to the
        // kernel, where it would mean something else.
        CIO_CHECK(!client->set_read_buffer(0).has_value());
        CIO_CHECK(!client->set_write_buffer(-1).has_value());

        CIO_CHECK(co_await accepted);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_addr_parsing);
    RUN_TEST(test_filesystem_round_trip);
    RUN_TEST(test_abstract_namespace);
    RUN_TEST(test_stale_path_is_replaced);
    RUN_TEST(test_dial_missing_and_cancelled);
    RUN_TEST(test_cancel_and_deadline_apply);
    RUN_TEST(test_tcp_socket_options);
    return cio_test::summary();
}
