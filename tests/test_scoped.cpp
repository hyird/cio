#include <sys/eventfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
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

// A connected pair, so a test can park a read that nothing will satisfy.
struct Pair {
    net::TcpStream client;
    net::TcpStream server;
};

cio::Task<cio::Result<Pair>> make_pair() {
    auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
    if (!listener) co_return listener.error();
    const auto addr = listener->local_addr().value();

    auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpStream> {
        auto conn = co_await l.accept();
        co_return conn ? std::move(*conn) : net::TcpStream{};
    }(std::move(*listener)));

    auto client = co_await net::TcpStream::connect(addr);
    if (!client) co_return client.error();
    auto server = co_await accepted;
    co_return Pair{std::move(*client), std::move(server)};
}

// ------------------------------------------------------- unified cancel ---

// An operation that starts after the token fired must refuse immediately,
// without a deadline and without close().
void test_cancel_refuses_new_operations() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        cio::CancelSource stop;
        pair->client.set_cancel(stop.token());
        stop.cancel();

        std::byte buffer[16];
        auto read = co_await pair->client.read(buffer);
        CIO_CHECK(!read.has_value());
        CIO_CHECK(read.error().is(cio::Errc::cancelled));

        // The write direction is covered by the same binding.
        auto written = co_await pair->client.write(bytes_of("x"));
        CIO_CHECK(!written.has_value());
        CIO_CHECK(written.error().is(cio::Errc::cancelled));

        // The peer is unaffected: cancellation is per-socket, not global.
        auto peer_write = co_await pair->server.write(bytes_of("y"));
        CIO_CHECK(peer_write.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The harder half: an operation already parked in epoll must be woken, not
// merely gated at the next admission check.
void test_cancel_wakes_a_parked_operation() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        cio::CancelSource stop;
        pair->client.set_cancel(stop.token());

        auto canceller = cio::spawn([](cio::CancelSource source) -> cio::Task<> {
            co_await cio::sleep(30ms);
            source.cancel();
        }(stop));

        // Nothing will ever arrive; only the cancellation can end this.
        std::byte buffer[16];
        const auto started = cio::Clock::now();
        auto read = co_await pair->client.read(buffer);
        const auto elapsed = cio::Clock::now() - started;
        co_await canceller;

        CIO_CHECK(!read.has_value());
        CIO_CHECK(read.error().is(cio::Errc::cancelled));
        CIO_CHECK(elapsed >= 25ms);
        CIO_CHECK(elapsed < 5s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Binding a fresh token must release the previous one, and clearing must
// restore ordinary behaviour.
void test_cancel_rebind_and_clear() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        cio::CancelSource first;
        cio::CancelSource second;
        pair->client.set_cancel(first.token());
        pair->client.set_cancel(second.token());

        // The replaced token no longer governs this socket.
        first.cancel();
        co_await pair->server.write_all(bytes_of("ab"));
        std::byte buffer[16];
        auto read = co_await pair->client.read(buffer);
        CIO_CHECK(read.has_value());
        CIO_CHECK_EQ(*read, std::size_t{2});

        // The current one does.
        second.cancel();
        auto refused = co_await pair->client.read(buffer);
        CIO_CHECK(!refused.has_value());
        CIO_CHECK(refused.error().is(cio::Errc::cancelled));

        // Clearing restores normal operation even after a cancel.
        pair->client.clear_cancel();
        co_await pair->server.write_all(bytes_of("cd"));
        auto after = co_await pair->client.read(buffer);
        CIO_CHECK(after.has_value());
        CIO_CHECK_EQ(*after, std::size_t{2});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Accept is covered by the same mechanism, with no signature change.
void test_cancel_covers_accept() {
    auto body = []() -> cio::Task<bool> {
        auto listener = net::TcpListener::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());

        cio::CancelSource stop;
        listener->set_cancel(stop.token());
        auto canceller = cio::spawn([](cio::CancelSource source) -> cio::Task<> {
            co_await cio::sleep(30ms);
            source.cancel();
        }(stop));

        auto accepted = co_await listener->accept();
        co_await canceller;
        CIO_CHECK(!accepted.has_value());
        CIO_CHECK(accepted.error().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// ------------------------------------------------------- scoped timeouts ---

void test_timeout_scope_restores_outer_deadline() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        const auto outer = cio::Clock::now() + 10s;
        pair->client.set_read_deadline(outer);

        {
            cio::Timeout inner(pair->client, 30ms);
            std::byte buffer[16];
            auto timed_out = co_await pair->client.read(buffer);
            CIO_CHECK(!timed_out.has_value());
            CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
        }

        // The enclosing deadline is back, and it has not elapsed.
        const auto restored = pair->client.deadline(/*write_direction=*/false);
        CIO_CHECK(restored == outer);

        co_await pair->server.write_all(bytes_of("ok"));
        std::byte buffer[16];
        auto read = co_await pair->client.read(buffer);
        CIO_CHECK(read.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A nested scope must not be able to extend the budget it sits inside.
void test_timeout_scope_never_loosens() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        cio::Timeout overall(pair->client, 40ms);
        {
            // Asks for far longer than the enclosing scope allows.
            cio::Timeout inner(pair->client, 10s);
            std::byte buffer[16];
            const auto started = cio::Clock::now();
            auto timed_out = co_await pair->client.read(buffer);
            const auto elapsed = cio::Clock::now() - started;
            CIO_CHECK(!timed_out.has_value());
            CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
            // Bounded by the outer 40ms, not the inner 10s.
            CIO_CHECK(elapsed < 5s);
        }
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// With no enclosing deadline, the scope must leave none behind.
void test_timeout_scope_clears_when_none_was_set() {
    auto body = []() -> cio::Task<bool> {
        auto pair = co_await make_pair();
        CIO_CHECK(pair.has_value());

        {
            cio::Timeout scoped(pair->client, 20ms);
            std::byte buffer[16];
            auto timed_out = co_await pair->client.read(buffer);
            CIO_CHECK(!timed_out.has_value());
        }
        CIO_CHECK(pair->client.deadline(false) == cio::TimePoint{});
        CIO_CHECK(pair->client.deadline(true) == cio::TimePoint{});

        // A cleared deadline means the socket works again.
        co_await pair->server.write_all(bytes_of("hi"));
        std::byte buffer[16];
        auto read = co_await pair->client.read(buffer);
        CIO_CHECK(read.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// --------------------------------------------------------- PollableFd ---

// The escape hatch: a descriptor the runtime did not create, driven by the
// caller's own syscalls.
void test_pollable_fd_readiness() {
    auto body = []() -> cio::Task<bool> {
        const int raw = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        CIO_CHECK(raw >= 0);

        auto fd = cio::PollableFd::adopt(raw, /*already_nonblocking=*/true);
        CIO_CHECK(fd.has_value());
        CIO_CHECK(fd->valid());

        auto writer = cio::spawn([](int target) -> cio::Task<> {
            co_await cio::sleep(20ms);
            const std::uint64_t one = 1;
            (void)::write(target, &one, sizeof(one));
        }(raw));

        auto ready = co_await fd->readable();
        CIO_CHECK(ready.has_value());

        std::uint64_t value = 0;
        CIO_CHECK_EQ(::read(raw, &value, sizeof(value)),
                     static_cast<ssize_t>(sizeof(value)));
        CIO_CHECK_EQ(value, std::uint64_t{1});
        co_await writer;
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Deadlines and cancellation apply to an adopted descriptor too.
void test_pollable_fd_deadline_and_cancel() {
    auto body = []() -> cio::Task<bool> {
        const int raw = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        CIO_CHECK(raw >= 0);
        auto fd = cio::PollableFd::adopt(raw, true);
        CIO_CHECK(fd.has_value());

        fd->set_timeout(20ms);
        auto timed_out = co_await fd->readable();
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));
        fd->clear_deadline();

        cio::CancelSource stop;
        fd->set_cancel(stop.token());
        auto canceller = cio::spawn([](cio::CancelSource source) -> cio::Task<> {
            co_await cio::sleep(20ms);
            source.cancel();
        }(stop));
        auto cancelled = co_await fd->readable();
        co_await canceller;
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_cancel_refuses_new_operations);
    RUN_TEST(test_cancel_wakes_a_parked_operation);
    RUN_TEST(test_cancel_rebind_and_clear);
    RUN_TEST(test_cancel_covers_accept);
    RUN_TEST(test_timeout_scope_restores_outer_deadline);
    RUN_TEST(test_timeout_scope_never_loosens);
    RUN_TEST(test_timeout_scope_clears_when_none_was_set);
    RUN_TEST(test_pollable_fd_readiness);
    RUN_TEST(test_pollable_fd_deadline_and_cancel);
    return cio_test::summary();
}
