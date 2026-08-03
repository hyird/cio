#include <fcntl.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace cio::detail {

struct SchedulerTestAccess {
    static void push_global(Scheduler& scheduler, void* frame) {
        scheduler.global_.push(frame);
    }
};

}  // namespace cio::detail

namespace {

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

class InspectableTcpConn final : public net::TcpConn {
public:
    cio::Result<void> adopt_for_test(int fd) {
        return adopt(fd, /*already_nonblocking=*/true);
    }

    cio::detail::IoDesc* descriptor() const noexcept { return desc_; }
};

class InspectableUdpConn final : public net::UdpConn {
public:
    cio::Result<void> adopt_for_test(int fd) {
        return adopt(fd, /*already_nonblocking=*/true);
    }

    cio::detail::IoDesc* descriptor() const noexcept { return desc_; }
};

struct ReadyHintTestAccess {
    static bool waiter_is_parked(cio::detail::IoDesc* desc,
                                 cio::detail::Dir dir) noexcept {
        void* const slot = desc->dir_slot(dir).load(std::memory_order_acquire);
        return slot != nullptr && slot != cio::detail::kIoReady;
    }

    static bool slot_is_empty(cio::detail::IoDesc* desc,
                              cio::detail::Dir dir) noexcept {
        return desc->dir_slot(dir).load(std::memory_order_acquire) == nullptr;
    }

    static void complete(cio::detail::IoDesc* desc, cio::detail::Dir dir,
                         cio::Error error) noexcept {
        desc->owner->unblock(desc, dir, error);
    }

    static void overwrite_not_ready(cio::detail::IoDesc* desc,
                                    cio::detail::Dir dir) noexcept {
        desc->note_would_block(dir);
    }

    static bool may_be_ready(cio::detail::IoDesc* desc,
                             cio::detail::Dir dir) noexcept {
        return desc->may_be_ready(dir);
    }

    static bool operation_is_queued(cio::detail::IoDesc* desc,
                                    cio::detail::Dir dir) noexcept {
        const unsigned index = static_cast<unsigned>(dir);
        desc->lock_lifecycle();
        const bool queued = desc->operation_head[index] != nullptr;
        desc->unlock_lifecycle();
        return queued;
    }
};

bool open_nonblocking_socket_pair(int fds[2]) {
    return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0,
                        fds) == 0;
}

void test_deadline_state_precedes_callback_dispatch() {
    // Deliberately do not start this scheduler. No worker or monitor can run a
    // timer callback, so the descriptor's absolute deadline must independently
    // prevent a ready syscall from succeeding after expiry.
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    auto attached = reactor.attach(fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(fds[0]);
        ::close(fds[1]);
        return;
    }

    auto* const desc = *attached;
    const std::uint32_t generation =
        desc->generation.load(std::memory_order_acquire);
    const char sent = 'D';
    CIO_CHECK_EQ(::send(fds[1], &sent, 1, MSG_NOSIGNAL), 1);

    reactor.set_deadline(desc, cio::detail::Dir::kRead, generation,
                         cio::now_ns() + cio::to_ns(1h));
    {
        cio::detail::FdUseGuard future{desc, cio::detail::Dir::kRead,
                                       generation};
        CIO_CHECK(static_cast<bool>(future));
    }

    reactor.set_deadline(desc, cio::detail::Dir::kRead, generation,
                         cio::now_ns() - 1);
    {
        cio::detail::FdUseGuard expired{desc, cio::detail::Dir::kRead,
                                        generation};
        CIO_CHECK(!expired);
        CIO_CHECK(expired.error().is(cio::Errc::timed_out));
    }

    reactor.set_deadline(desc, cio::detail::Dir::kRead, generation, 0);
    {
        cio::detail::FdUseGuard cleared{desc, cio::detail::Dir::kRead,
                                        generation};
        CIO_CHECK(static_cast<bool>(cleared));
        if (cleared) {
            char received = '\0';
            CIO_CHECK_EQ(::recv(cleared.fd(), &received, 1, 0), 1);
            CIO_CHECK_EQ(received, sent);
        }
    }

    reactor.detach(desc);
    ::close(fds[0]);
    ::close(fds[1]);
}

void test_stale_deadline_setter_cannot_disarm_reused_descriptor() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    int old_fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(old_fds));
    if (old_fds[0] < 0 || old_fds[1] < 0) return;

    auto old_attached = reactor.attach(old_fds[0]);
    CIO_CHECK(old_attached.has_value());
    if (!old_attached) {
        ::close(old_fds[0]);
        ::close(old_fds[1]);
        return;
    }
    auto* const old_desc = *old_attached;
    const std::uint32_t old_generation =
        old_desc->generation.load(std::memory_order_acquire);
    reactor.detach(old_desc);
    ::close(old_fds[0]);
    ::close(old_fds[1]);

    int new_fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(new_fds));
    if (new_fds[0] < 0 || new_fds[1] < 0) return;

    auto new_attached = reactor.attach(new_fds[0]);
    CIO_CHECK(new_attached.has_value());
    if (!new_attached) {
        ::close(new_fds[0]);
        ::close(new_fds[1]);
        return;
    }
    auto* const new_desc = *new_attached;
    const std::uint32_t new_generation =
        new_desc->generation.load(std::memory_order_acquire);
    CIO_CHECK(new_desc == old_desc);
    CIO_CHECK(new_generation != old_generation);

    const std::int64_t future_deadline = cio::now_ns() + cio::to_ns(1h);
    reactor.set_deadline(new_desc, cio::detail::Dir::kRead, new_generation,
                         future_deadline);
    const unsigned read = static_cast<unsigned>(cio::detail::Dir::kRead);
    CIO_CHECK_EQ(
        new_desc->absolute_deadline_ns[read].load(std::memory_order_acquire),
        future_deadline);
    CIO_CHECK_EQ(
        new_desc->deadline_timer[read].state.load(std::memory_order_acquire),
        cio::detail::Timer::kArmed);

    // An operation token captured from the previous fd incarnation must be
    // rejected before set_deadline() touches the reused timer node.
    reactor.set_deadline(new_desc, cio::detail::Dir::kRead, old_generation, 0);
    CIO_CHECK_EQ(
        new_desc->absolute_deadline_ns[read].load(std::memory_order_acquire),
        future_deadline);
    CIO_CHECK_EQ(
        new_desc->deadline_timer[read].state.load(std::memory_order_acquire),
        cio::detail::Timer::kArmed);

    reactor.set_deadline(new_desc, cio::detail::Dir::kRead, new_generation, 0);
    reactor.detach(new_desc);
    ::close(new_fds[0]);
    ::close(new_fds[1]);
}

void test_close_published_before_firing_deadline_wins() {
    // Force the timer into kFiring while its callback is blocked on the
    // descriptor lifecycle lock. Publishing closing under that lock models
    // Socket::close() winning its linearization point before detach waits for
    // the callback. The callback must leave the waiter for detach to complete
    // with Errc::closed.
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    auto attached = reactor.attach(fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(fds[0]);
        ::close(fds[1]);
        return;
    }

    auto* const desc = *attached;
    const std::uint32_t generation =
        desc->generation.load(std::memory_order_acquire);
    const unsigned read = static_cast<unsigned>(cio::detail::Dir::kRead);
    reactor.set_deadline(desc, cio::detail::Dir::kRead, generation,
                         cio::now_ns() + cio::to_ns(1ms));

    cio::detail::IoWait waiter;
    desc->slot[read].store(&waiter, std::memory_order_release);

    desc->lock_lifecycle();
    // There is no scheduler thread in this test. Wait for the published heap
    // key to expire while holding lifecycle_lock, so run_expired must move the
    // timer to kFiring and then block inside its callback.
    while (scheduler.timers().next_deadline_ns(0) > cio::now_ns()) {
        std::this_thread::yield();
    }

    std::size_t fired_count = 0;
    std::thread firing(
        [&] { fired_count = scheduler.timers().run_expired(0); });

    const auto firing_deadline = cio::Clock::now() + 1s;
    while (desc->deadline_timer[read].state.load(std::memory_order_acquire) !=
               cio::detail::Timer::kFiring &&
           cio::Clock::now() < firing_deadline) {
        std::this_thread::yield();
    }
    const bool callback_waiting =
        desc->deadline_timer[read].state.load(std::memory_order_acquire) ==
        cio::detail::Timer::kFiring;
    CIO_CHECK(callback_waiting);

    desc->closing.store(true, std::memory_order_release);
    desc->unlock_lifecycle();
    firing.join();

    reactor.detach(desc);
    CIO_CHECK_EQ(fired_count, std::size_t{1});
    CIO_CHECK(waiter.err.is(cio::Errc::closed));
    ::close(fds[0]);
    ::close(fds[1]);
}

void test_concurrent_deadline_setters_are_serialized() {
    cio::detail::Scheduler scheduler(1, 1);
    auto& reactor = scheduler.reactor_for(0);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    auto attached = reactor.attach(fds[0]);
    CIO_CHECK(attached.has_value());
    if (!attached) {
        ::close(fds[0]);
        ::close(fds[1]);
        return;
    }
    auto* const desc = *attached;
    const std::uint32_t generation =
        desc->generation.load(std::memory_order_acquire);
    constexpr int kRounds = 500;
    std::barrier rendezvous{2};

    auto setter = [&](std::int64_t offset) {
        for (int i = 0; i < kRounds; ++i) {
            rendezvous.arrive_and_wait();
            reactor.set_deadline(desc, cio::detail::Dir::kRead, generation,
                                 cio::now_ns() + cio::to_ns(1h) + offset + i);
        }
    };
    std::thread first(setter, 1);
    std::thread second(setter, 1'000'000);
    first.join();
    second.join();

    const unsigned read = static_cast<unsigned>(cio::detail::Dir::kRead);
    CIO_CHECK_EQ(desc->deadline_seq[read].load(std::memory_order_acquire),
                 std::uint32_t{1 + 2 * kRounds});
    CIO_CHECK(desc->absolute_deadline_ns[read].load(std::memory_order_acquire) >
              cio::now_ns());
    CIO_CHECK_EQ(
        desc->deadline_timer[read].state.load(std::memory_order_acquire),
        cio::detail::Timer::kArmed);

    reactor.set_deadline(desc, cio::detail::Dir::kRead, generation, 0);
    reactor.detach(desc);
    ::close(fds[0]);
    ::close(fds[1]);
}

bool force_socket_pair_fd_reuse(int target_fd, int& reused_fd, int& peer_fd) {
    int replacement[2] = {-1, -1};
    if (!open_nonblocking_socket_pair(replacement)) return false;

    if (replacement[0] == target_fd) {
        reused_fd = replacement[0];
        peer_fd = replacement[1];
        return true;
    }
    if (replacement[1] == target_fd) {
        reused_fd = replacement[1];
        peer_fd = replacement[0];
        return true;
    }

    if (::dup2(replacement[0], target_fd) < 0) {
        ::close(replacement[0]);
        ::close(replacement[1]);
        return false;
    }
    ::close(replacement[0]);
    reused_fd = target_fd;
    peer_fd = replacement[1];
    return true;
}

cio::Result<InspectableUdpConn> bind_inspectable_udp() {
    const int fd =
        ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return cio::Error::from_errno();

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) !=
        0) {
        const cio::Error error = cio::Error::from_errno();
        ::close(fd);
        return error;
    }

    InspectableUdpConn socket;
    if (auto adopted = socket.adopt_for_test(fd); !adopted) {
        return adopted.error();
    }
    return socket;
}

struct SwitchToWorker {
    cio::detail::Scheduler* scheduler = nullptr;
    cio::detail::WorkerId target = cio::detail::kInvalidWorkerId;

    bool await_ready() const noexcept {
        return cio::detail::current_worker_id(scheduler) == target;
    }
    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        scheduler->schedule_to(handle, target);
    }
    void await_resume() const noexcept {}
};

using TcpPair = std::pair<net::TcpConn, net::TcpConn>;

cio::Task<cio::Result<TcpPair>> open_tcp_pair(net::TcpListener& listener,
                                              net::SocketAddr addr) {
    auto connecting = cio::spawn(net::TcpConn::dial(addr));
    auto accepted = co_await listener.accept();
    auto client = co_await connecting;
    if (!accepted) co_return accepted.error();
    if (!client) co_return client.error();
    co_return TcpPair{std::move(*client), std::move(*accepted)};
}

cio::Task<bool> observe_read_timeout(net::TcpConn& stream) {
    // A ready edge may have been recorded before the deadline fired. Consume
    // at most that hint, then the persistent descriptor state must win.
    for (int attempt = 0; attempt < 4; ++attempt) {
        auto ready = co_await stream.readable();
        if (!ready) {
            co_return ready.error().is(cio::Errc::timed_out);
        }
        co_await cio::yield();
    }
    co_return false;
}

cio::Task<bool> observe_write_timeout(net::TcpConn& stream) {
    for (int attempt = 0; attempt < 4; ++attempt) {
        auto ready = co_await stream.writable();
        if (!ready) {
            co_return ready.error().is(cio::Errc::timed_out);
        }
        co_await cio::yield();
    }
    co_return false;
}

cio::Task<> echo_connection(net::TcpConn stream) {
    std::byte buffer[4096];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        if (auto written = co_await stream.write(std::span(buffer, *n));
            !written)
            break;
    }
}

// Cancellable accept loop, and the reason it is shaped this way: cancellation
// does not interrupt a parked accept(), so a server that only checks a token
// after accept returns never returns at all once traffic stops. A short
// listener deadline makes accept come back on its own, which lets the loop
// observe the token without a second task closing the socket underneath it.
// Everything it spawns is joined, so nothing is still parked when the runtime
// is destroyed.
cio::Task<> echo_server(net::TcpListener listener, cio::CancelToken stop) {
    cio::TaskGroup connections;
    for (;;) {
        if (stop.cancelled()) break;
        listener.set_deadline(cio::Clock::now() + 10ms);
        auto conn = co_await listener.accept();
        if (!conn) {
            if (conn.error().is(cio::Errc::timed_out)) continue;
            break;  // Errc::closed when the listener is torn down
        }
        connections.spawn(echo_connection(std::move(*conn)));
    }
    co_await connections.join();
}

void test_echo_round_trip() {
    auto body = []() -> cio::Task<std::string> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        cio::CancelSource stop;
        auto server =
            cio::spawn(echo_server(std::move(*listener), stop.token()));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        if (!client) co_return "";

        const std::string message = "hello cio";
        auto written = co_await client->write(bytes_of(message));
        CIO_CHECK(written.has_value());

        std::string received;
        std::byte buffer[64];
        while (received.size() < message.size()) {
            auto n = co_await client->read(buffer);
            if (!n || *n == 0) break;
            received.append(reinterpret_cast<const char*>(buffer), *n);
        }
        client->close();
        stop.cancel();
        co_await server;
        co_return received;
    };
    CIO_CHECK_EQ(cio::run(body()), std::string("hello cio"));
}

// The read deadline must interrupt a parked read, and the socket must stay
// usable after the deadline is cleared.
// The combined setters must reach both directions in one call, and the
// combined clear must release both. Asserting only a read timeout would pass
// against an implementation that silently covered one direction, so the write
// direction is exercised too: an elapsed deadline is checked at syscall
// admission, so a write refuses before it can succeed into a large send buffer.
void test_combined_deadline_covers_both_directions() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted =
            cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpConn> {
                auto conn = co_await l.accept();
                co_return std::move(conn.value());
            }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto server_side = co_await accepted;

        client->set_timeout(30ms);

        std::byte buffer[16];
        auto timed_out = co_await client->read(buffer);
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));

        // The write direction received the same deadline, and it has elapsed.
        auto write_refused = co_await client->write(bytes_of("x"));
        CIO_CHECK(!write_refused.has_value());
        CIO_CHECK(write_refused.error().is(cio::Errc::timed_out));

        // One combined clear releases both directions.
        client->clear_deadline();
        co_await server_side.write(bytes_of("ok"));
        auto n = co_await client->read(buffer);
        CIO_CHECK(n.has_value());
        CIO_CHECK_EQ(*n, std::size_t{2});
        auto written = co_await client->write(bytes_of("y"));
        CIO_CHECK(written.has_value());

        // An absolute combined deadline takes the same path, and a
        // per-direction clear must release only its own direction.
        client->set_deadline(cio::Clock::now() + 20ms);
        co_await cio::sleep(40ms);
        client->clear_write_deadline();

        auto write_ok = co_await client->write(bytes_of("z"));
        CIO_CHECK(write_ok.has_value());
        auto read_still_timed_out = co_await client->read(buffer);
        CIO_CHECK(!read_still_timed_out.has_value());
        CIO_CHECK(read_still_timed_out.error().is(cio::Errc::timed_out));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// UdpConn gained the same surface; it previously had no clear at all, so a
// deadline set on it could never be released.
void test_udp_combined_deadline_and_clear() {
    auto body = []() -> cio::Task<bool> {
        auto socket = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(socket.has_value());
        const auto addr = socket->local_addr().value();
        auto sender = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(sender.has_value());

        socket->set_timeout(30ms);

        std::byte buffer[16];
        net::SocketAddr from;
        auto timed_out = co_await socket->read_from(buffer, from);
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is(cio::Errc::timed_out));

        // The write direction carries the same elapsed deadline.
        auto send_refused = co_await socket->write_to(bytes_of("x"), addr);
        CIO_CHECK(!send_refused.has_value());
        CIO_CHECK(send_refused.error().is(cio::Errc::timed_out));

        socket->clear_deadline();
        socket->set_read_timeout(2s);
        auto sent = co_await sender->write_to(bytes_of("hi"), addr);
        CIO_CHECK(sent.has_value());
        auto received = co_await socket->read_from(buffer, from);
        CIO_CHECK(received.has_value());
        CIO_CHECK_EQ(*received, std::size_t{2});

        // The cleared write direction works again as well.
        auto sent_back = co_await socket->write_to(bytes_of("yo"), addr);
        CIO_CHECK(sent_back.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_read_deadline() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        // A server that accepts and then stays silent.
        auto accepted =
            cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpConn> {
                auto conn = co_await l.accept();
                co_return std::move(conn.value());
            }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
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
        co_await server_side.write(bytes_of("ok"));
        auto n = co_await client->read(buffer);
        CIO_CHECK(n.has_value());
        CIO_CHECK_EQ(*n, std::size_t{2});
        co_return n.has_value() && *n == 2;
    };
    CIO_CHECK(cio::run(body()));
}

// Each iteration uses a fresh descriptor so no readiness sentinel from a
// previous deadline can participate. This repeatedly races an already-due
// timer callback against IoAwaiter waiter publication. A raw readiness edge may
// be spurious, but the complete read operation must retain the deadline and
// finish with timed_out while the peer sends no data.
void test_immediate_deadline_remains_persistent_during_waiter_publication() {
    constexpr int kIterations = 256;

    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        if (!listener) co_return false;
        const auto addr = listener->addr().value();

        for (int i = 0; i < kIterations; ++i) {
            auto connecting = cio::spawn(net::TcpConn::dial(addr));
            auto accepted = co_await listener->accept();
            auto client = co_await connecting;
            CIO_CHECK(accepted.has_value());
            CIO_CHECK(client.has_value());
            if (!accepted || !client) co_return false;

            std::byte byte;
            client->set_read_deadline(cio::Clock::now());
            const auto read =
                co_await client->read(std::span<std::byte>{&byte, 1});
            CIO_CHECK(!read);
            CIO_CHECK(read.error().is(cio::Errc::timed_out));
            if (read || !read.error().is(cio::Errc::timed_out)) {
                co_return false;
            }
        }
        co_return true;
    };

    CIO_CHECK(cio::run(body()));
}

void test_expired_deadline_precedes_ready_data_write_and_eof() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        if (!listener) co_return false;
        const auto addr = listener->addr().value();

        // Buffered data must not bypass an already-published read deadline.
        auto data_pair = co_await open_tcp_pair(*listener, addr);
        CIO_CHECK(data_pair.has_value());
        if (!data_pair) co_return false;
        auto& [data_client, data_peer] = *data_pair;
        auto sent = co_await data_peer.write(bytes_of("x"));
        CIO_CHECK(sent.has_value());
        if (!sent) co_return false;
        data_client.set_read_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        if (!(co_await observe_read_timeout(data_client))) co_return false;
        std::byte byte{};
        auto data_read =
            co_await data_client.read(std::span<std::byte>{&byte, 1});
        CIO_CHECK(!data_read);
        CIO_CHECK(data_read.error().is(cio::Errc::timed_out));
        if (data_read || !data_read.error().is(cio::Errc::timed_out)) {
            co_return false;
        }

        // EOF is a successful recv(2) result, but it is still a future read
        // operation and therefore loses to a persistent expired deadline.
        auto eof_pair = co_await open_tcp_pair(*listener, addr);
        CIO_CHECK(eof_pair.has_value());
        if (!eof_pair) co_return false;
        auto& [eof_client, eof_peer] = *eof_pair;
        auto shutdown = eof_peer.close_write();
        CIO_CHECK(shutdown.has_value());
        if (!shutdown) co_return false;
        eof_client.set_read_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        if (!(co_await observe_read_timeout(eof_client))) co_return false;
        auto eof_read =
            co_await eof_client.read(std::span<std::byte>{&byte, 1});
        CIO_CHECK(!eof_read);
        CIO_CHECK(eof_read.error().is(cio::Errc::timed_out));
        if (eof_read || !eof_read.error().is(cio::Errc::timed_out)) {
            co_return false;
        }

        // An empty send buffer is normally immediately writable. It must not
        // let send(2) run after the write deadline has become persistent.
        auto write_pair = co_await open_tcp_pair(*listener, addr);
        CIO_CHECK(write_pair.has_value());
        if (!write_pair) co_return false;
        auto& [write_client, write_peer] = *write_pair;
        (void)write_peer;
        write_client.set_write_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        if (!(co_await observe_write_timeout(write_client))) co_return false;
        auto write = co_await write_client.write(bytes_of("x"));
        CIO_CHECK(!write);
        CIO_CHECK(write.error().is(cio::Errc::timed_out));
        co_return !write && write.error().is(cio::Errc::timed_out);
    };

    CIO_CHECK(cio::run(body()));
}

void test_expired_deadline_precedes_ready_accept_and_udp() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        if (!listener) co_return false;

        // Complete the handshake without accepting it, leaving a connection
        // ready in the listener backlog.
        auto client = co_await net::TcpConn::dial(listener->addr().value());
        CIO_CHECK(client.has_value());
        if (!client) co_return false;
        listener->set_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        auto accepted = co_await listener->accept();
        CIO_CHECK(!accepted);
        CIO_CHECK(accepted.error().is(cio::Errc::timed_out));
        if (accepted || !accepted.error().is(cio::Errc::timed_out)) {
            co_return false;
        }

        auto receiver = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        auto sender = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(receiver.has_value());
        CIO_CHECK(sender.has_value());
        if (!receiver || !sender) co_return false;
        const auto target = receiver->local_addr();
        CIO_CHECK(target.has_value());
        if (!target) co_return false;

        auto primed = co_await sender->write_to(bytes_of("x"), *target);
        CIO_CHECK(primed.has_value());
        if (!primed) co_return false;

        receiver->set_read_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        std::byte byte{};
        net::SocketAddr from;
        auto received =
            co_await receiver->read_from(std::span<std::byte>{&byte, 1}, from);
        CIO_CHECK(!received);
        CIO_CHECK(received.error().is(cio::Errc::timed_out));
        if (received || !received.error().is(cio::Errc::timed_out)) {
            co_return false;
        }

        sender->set_write_deadline(cio::Clock::now());
        co_await cio::sleep(2ms);
        auto sent = co_await sender->write_to(bytes_of("y"), *target);
        CIO_CHECK(!sent);
        CIO_CHECK(sent.error().is(cio::Errc::timed_out));
        co_return !sent && sent.error().is(cio::Errc::timed_out);
    };

    CIO_CHECK(cio::run(body()));
}

void test_connect_refused() {
    auto body = []() -> cio::Task<bool> {
        // Bind and immediately drop, so the port is almost certainly closed.
        std::uint16_t port = 0;
        {
            auto probe =
                net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
            CIO_CHECK(probe.has_value());
            port = probe->addr().value().port();
        }
        auto result =
            co_await net::TcpConn::dial(net::SocketAddr::loopback_v4(port));
        CIO_CHECK(!result.has_value());
        co_return !result.has_value();
    };
    CIO_CHECK(cio::run(body()));
}

void test_connect_completion_survives_descriptor_reuse() {
    constexpr int kConnections = 512;

    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        if (!listener) co_return false;
        const auto addr = listener->addr().value();

        // Closing every pair immediately leaves plenty of old EPOLLOUT/HUP
        // traffic while the descriptor slabs and native fd numbers are being
        // reused. No such stale hint may make the next connect report success
        // before a peer is actually installed.
        for (int i = 0; i < kConnections; ++i) {
            auto pair = co_await open_tcp_pair(*listener, addr);
            CIO_CHECK(pair.has_value());
            if (!pair) co_return false;

            auto& [client, accepted] = *pair;
            auto client_peer = client.remote_addr();
            auto accepted_peer = accepted.remote_addr();
            CIO_CHECK(client_peer.has_value());
            CIO_CHECK(accepted_peer.has_value());
            if (!client_peer || !accepted_peer) co_return false;

            accepted.close();
            client.close();
            if ((i & 15) == 15) co_await cio::yield();
        }
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 4;
    CIO_CHECK(cio::run(body(), options));
}

// Many concurrent connections, each doing several round trips: this is the
// test that actually exercises the edge-triggered readiness state machine.
void test_many_concurrent_connections() {
    static constexpr int kClients = 64;
    static constexpr int kRoundTrips = 20;

    auto body = []() -> cio::Task<int> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        cio::CancelSource stop;
        auto server =
            cio::spawn(echo_server(std::move(*listener), stop.token()));

        cio::TaskGroup clients;
        auto successes = cio::make_chan<int>(kClients);

        for (int i = 0; i < kClients; ++i) {
            clients.spawn([](net::SocketAddr target,
                             cio::Chan<int> out) -> cio::Task<> {
                auto stream = co_await net::TcpConn::dial(target);
                if (!stream) {
                    co_await out.send(0);
                    co_return;
                }
                int ok = 0;
                std::byte buffer[32];
                for (int r = 0; r < kRoundTrips; ++r) {
                    const std::string payload = "ping" + std::to_string(r);
                    if (!(co_await stream->write(bytes_of(payload)))) break;

                    std::size_t got = 0;
                    while (got < payload.size()) {
                        auto n = co_await stream->read(
                            std::span(buffer + got, sizeof(buffer) - got));
                        if (!n || *n == 0) break;
                        got += *n;
                    }
                    if (got != payload.size()) break;
                    if (std::memcmp(buffer, payload.data(), payload.size()) !=
                        0)
                        break;
                    ++ok;
                }
                co_await out.send(ok);
            }(addr, successes));
        }

        co_await clients.join();

        int total = 0;
        for (int i = 0; i < kClients; ++i) total += *co_await successes.recv();
        stop.cancel();
        co_await server;
        co_return total;
    };
    CIO_CHECK_EQ(cio::run(body()), kClients * kRoundTrips);
}

void test_accept_distributes_new_streams_across_workers() {
    constexpr std::size_t kWorkers = 4;
    constexpr int kConnections = 12;

    auto body = []() -> cio::Task<std::vector<bool>> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        if (!listener) co_return {};
        const auto addr = listener->addr().value();

        std::vector<bool> seen(kWorkers, false);
        for (int i = 0; i < kConnections; ++i) {
            auto client = cio::spawn(net::TcpConn::dial(addr));
            auto accepted = co_await listener->accept();
            CIO_CHECK(accepted.has_value());
            if (!accepted) co_return seen;

            const auto worker = cio::detail::current_worker_id(
                cio::detail::current_scheduler());
            CIO_CHECK(worker < kWorkers);
            if (worker < kWorkers) seen[worker] = true;

            auto connected = co_await client;
            CIO_CHECK(connected.has_value());
            accepted->close();
            if (connected) connected->close();
        }
        co_return seen;
    };

    cio::RuntimeOptions options;
    options.worker_threads = kWorkers;
    const auto seen = cio::run(body(), options);
    CIO_CHECK_EQ(seen.size(), kWorkers);
    for (bool worker_seen : seen) CIO_CHECK(worker_seen);
}

void test_close_wakes_a_parked_reader() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted =
            cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpConn> {
                auto conn = co_await l.accept();
                co_return std::move(conn.value());
            }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto server_side = co_await accepted;

        // Server hangs up; the parked client read must observe EOF, not hang.
        cio::go([](net::TcpConn s) -> cio::Task<> {
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

cio::Task<bool> read_one_byte(net::TcpConn* stream, std::byte* byte) {
    auto read = co_await stream->read({byte, 1});
    co_return read.has_value() && *read == 1;
}

cio::Task<cio::Error> read_one_byte_error(net::TcpConn* stream) {
    std::byte byte{};
    auto read = co_await stream->read({&byte, 1});
    co_return read ? cio::Error{} : read.error();
}

void test_same_direction_reads_queue_instead_of_failing() {
    auto body = []() -> cio::Task<bool> {
        int fds[2] = {-1, -1};
        CIO_CHECK(open_nonblocking_socket_pair(fds));
        if (fds[0] < 0 || fds[1] < 0) co_return false;

        InspectableTcpConn stream;
        CIO_CHECK(stream.adopt_for_test(fds[0]).has_value());
        cio::detail::IoDesc* const desc = stream.descriptor();

        std::byte first{};
        std::byte second{};
        auto first_read = cio::spawn(read_one_byte(&stream, &first));

        const auto deadline = cio::Clock::now() + 2s;
        while (!ReadyHintTestAccess::waiter_is_parked(
                   desc, cio::detail::Dir::kRead) &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        CIO_CHECK(ReadyHintTestAccess::waiter_is_parked(
            desc, cio::detail::Dir::kRead));

        auto second_read = cio::spawn(read_one_byte(&stream, &second));
        while (!ReadyHintTestAccess::operation_is_queued(
                   desc, cio::detail::Dir::kRead) &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        CIO_CHECK(ReadyHintTestAccess::operation_is_queued(
            desc, cio::detail::Dir::kRead));

        std::byte unused{};
        auto nonblocking = stream.try_read({&unused, 1});
        CIO_CHECK(!nonblocking.has_value());
        CIO_CHECK(nonblocking.error().is(cio::Errc::would_block));

        const char payload[] = {'A', 'B'};
        CIO_CHECK_EQ(::send(fds[1], payload, sizeof(payload), MSG_NOSIGNAL),
                     static_cast<ssize_t>(sizeof(payload)));
        CIO_CHECK(co_await first_read);
        CIO_CHECK(co_await second_read);
        CIO_CHECK(first == std::byte{'A'});
        CIO_CHECK(second == std::byte{'B'});

        stream.close();
        ::close(fds[1]);
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 2;
    CIO_CHECK(cio::run(body(), options));
}

void test_close_drains_same_direction_operation_queue() {
    auto body = []() -> cio::Task<bool> {
        int fds[2] = {-1, -1};
        CIO_CHECK(open_nonblocking_socket_pair(fds));
        if (fds[0] < 0 || fds[1] < 0) co_return false;

        InspectableTcpConn stream;
        CIO_CHECK(stream.adopt_for_test(fds[0]).has_value());
        cio::detail::IoDesc* const desc = stream.descriptor();
        auto first = cio::spawn(read_one_byte_error(&stream));

        const auto deadline = cio::Clock::now() + 2s;
        while (!ReadyHintTestAccess::waiter_is_parked(
                   desc, cio::detail::Dir::kRead) &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        auto second = cio::spawn(read_one_byte_error(&stream));
        while (!ReadyHintTestAccess::operation_is_queued(
                   desc, cio::detail::Dir::kRead) &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        CIO_CHECK(ReadyHintTestAccess::operation_is_queued(
            desc, cio::detail::Dir::kRead));

        stream.close();
        const cio::Error first_error = co_await first;
        const cio::Error second_error = co_await second;
        CIO_CHECK(first_error.is(cio::Errc::closed));
        CIO_CHECK(second_error.is(cio::Errc::closed));
        ::close(fds[1]);
        co_return true;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 2;
    CIO_CHECK(cio::run(body(), options));
}

void test_local_close_wakes_a_parked_reader() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted =
            cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpConn> {
                auto conn = co_await l.accept();
                co_return std::move(conn.value());
            }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto server_side = co_await accepted;

        auto reader =
            cio::spawn([](net::TcpConn& stream) -> cio::Task<cio::Error> {
                std::byte buffer[16];
                auto result = co_await stream.read(buffer);
                co_return result.error();
            }(*client));

        co_await cio::sleep(20ms);
        client->close();
        const cio::Error error = co_await reader;
        CIO_CHECK(error.is(cio::Errc::closed));
        co_return error.is(cio::Errc::closed);
    };
    CIO_CHECK(cio::run(body()));
}

struct ReadyHintAwaitObservation {
    cio::Error error{};
    bool hint_on_resume = false;
};

cio::Task<ReadyHintAwaitObservation> observe_ready_hint_after_await(
    cio::detail::IoDesc* desc, std::uint32_t generation) {
    auto ready = co_await cio::detail::IoAwaiter{desc, cio::detail::Dir::kRead,
                                                 generation};
    co_return ReadyHintAwaitObservation{
        ready ? cio::Error{} : ready.error(),
        ReadyHintTestAccess::may_be_ready(desc, cio::detail::Dir::kRead)};
}

cio::Task<bool> run_ready_hint_completion_case(bool successful) {
    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) co_return false;

    InspectableTcpConn stream;
    auto adopted = stream.adopt_for_test(fds[0]);
    CIO_CHECK(adopted.has_value());
    if (!adopted) {
        ::close(fds[1]);
        co_return false;
    }

    cio::detail::IoDesc* const desc = stream.descriptor();
    const std::uint32_t generation =
        desc->generation.load(std::memory_order_acquire);

    ReadyHintTestAccess::overwrite_not_ready(desc, cio::detail::Dir::kRead);
    auto waiter = cio::spawn(observe_ready_hint_after_await(desc, generation));

    bool parked = false;
    for (int attempt = 0; attempt < 8 && !parked; ++attempt) {
        co_await cio::yield();
        parked = ReadyHintTestAccess::waiter_is_parked(desc,
                                                       cio::detail::Dir::kRead);
    }
    CIO_CHECK(parked);
    if (!parked) {
        stream.close();
        (void)co_await waiter;
        ::close(fds[1]);
        co_return false;
    }

    ReadyHintTestAccess::complete(
        desc, cio::detail::Dir::kRead,
        successful ? cio::Error{} : cio::Error{cio::Errc::timed_out});

    // One worker is running this coordinator, so unblock() has claimed and
    // queued the waiter but cannot resume it until this coroutine suspends.
    const bool claimed =
        ReadyHintTestAccess::slot_is_empty(desc, cio::detail::Dir::kRead);
    const bool resume_is_pending = !waiter.done();
    CIO_CHECK(claimed);
    CIO_CHECK(resume_is_pending);

    // Model the late false writer that used to overwrite the reactor's eager
    // true publication. A successful await_resume() must be the final
    // readiness observer; an error completion must not pretend to be ready.
    ReadyHintTestAccess::overwrite_not_ready(desc, cio::detail::Dir::kRead);
    const bool false_before_resume =
        !ReadyHintTestAccess::may_be_ready(desc, cio::detail::Dir::kRead);
    CIO_CHECK(false_before_resume);

    const ReadyHintAwaitObservation observation = co_await waiter;
    const bool result_matches =
        successful ? !observation.error
                   : observation.error.is(cio::Errc::timed_out);
    const bool hint_matches = observation.hint_on_resume == successful;

    stream.close();
    ::close(fds[1]);

    CIO_CHECK(result_matches);
    CIO_CHECK(hint_matches);
    co_return claimed&& resume_is_pending&& false_before_resume&& result_matches&&
        hint_matches;
}

void test_successful_io_awaiter_restores_ready_hint_after_claim() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(
        cio::run(run_ready_hint_completion_case(/*successful=*/true), options));
}

void test_failed_io_awaiter_does_not_restore_ready_hint() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(run_ready_hint_completion_case(/*successful=*/false),
                       options));
}

void test_fd_use_guard_delays_physical_close() {
    auto body = []() -> cio::Task<bool> {
        int fds[2] = {-1, -1};
        CIO_CHECK(open_nonblocking_socket_pair(fds));
        if (fds[0] < 0 || fds[1] < 0) co_return false;

        InspectableTcpConn stream;
        auto adopted = stream.adopt_for_test(fds[0]);
        CIO_CHECK(adopted.has_value());
        if (!adopted) {
            ::close(fds[1]);
            co_return false;
        }

        cio::detail::IoDesc* const desc = stream.descriptor();
        const std::uint32_t generation =
            desc->generation.load(std::memory_order_acquire);
        const int original_fd = stream.native_handle();
        std::atomic<bool> close_returned{false};
        std::thread closer;

        bool closing_published = false;
        bool close_waited = false;
        bool fd_stayed_open = false;
        bool original_data_read = false;
        {
            cio::detail::FdUseGuard guard{desc, cio::detail::Dir::kRead,
                                          generation};
            CIO_CHECK(static_cast<bool>(guard));
            if (!guard) {
                ::close(fds[1]);
                co_return false;
            }

            const char payload = 'G';
            CIO_CHECK_EQ(::send(fds[1], &payload, 1, MSG_NOSIGNAL), ssize_t{1});

            closer = std::thread([&] {
                stream.close();
                close_returned.store(true, std::memory_order_release);
            });

            const auto deadline = cio::Clock::now() + 1s;
            while (!desc->closing.load(std::memory_order_acquire) &&
                   cio::Clock::now() < deadline) {
                std::this_thread::yield();
            }
            closing_published = desc->closing.load(std::memory_order_acquire);

            // Once closing is visible, detach is forbidden to hold the
            // lifecycle lock while waiting: this guard must still be able to
            // use and then release the original fd.
            errno = 0;
            fd_stayed_open = ::fcntl(guard.fd(), F_GETFD) >= 0;
            char received = '\0';
            original_data_read =
                ::recv(guard.fd(), &received, 1, 0) == 1 && received == payload;

            std::this_thread::sleep_for(2ms);
            close_waited = !close_returned.load(std::memory_order_acquire);
        }

        if (closer.joinable()) closer.join();
        const bool close_completed =
            close_returned.load(std::memory_order_acquire);
        errno = 0;
        const bool physically_closed =
            ::fcntl(original_fd, F_GETFD) < 0 && errno == EBADF;
        ::close(fds[1]);

        CIO_CHECK(closing_published);
        CIO_CHECK(close_waited);
        CIO_CHECK(fd_stayed_open);
        CIO_CHECK(original_data_read);
        CIO_CHECK(close_completed);
        CIO_CHECK(physically_closed);
        co_return closing_published&& close_waited&& fd_stayed_open&& original_data_read&& close_completed&&
            physically_closed;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

void test_readiness_then_close_cannot_read_reused_fd() {
    auto body = []() -> cio::Task<bool> {
        int original[2] = {-1, -1};
        CIO_CHECK(open_nonblocking_socket_pair(original));
        if (original[0] < 0 || original[1] < 0) co_return false;

        InspectableTcpConn stream;
        auto adopted = stream.adopt_for_test(original[0]);
        CIO_CHECK(adopted.has_value());
        if (!adopted) {
            ::close(original[1]);
            co_return false;
        }

        cio::detail::IoDesc* const desc = stream.descriptor();
        const int original_fd = stream.native_handle();
        auto reader = cio::spawn([](InspectableTcpConn* socket)
                                     -> cio::Task<cio::Result<std::size_t>> {
            std::byte byte{};
            co_return co_await socket->read(std::span<std::byte>{&byte, 1});
        }(&stream));

        // Let the read prove EAGAIN and publish its IoWait.
        bool parked = false;
        for (int attempt = 0; attempt < 8 && !parked; ++attempt) {
            co_await cio::yield();
            void* const slot =
                desc->slot[static_cast<unsigned>(cio::detail::Dir::kRead)].load(
                    std::memory_order_acquire);
            parked = slot != nullptr && slot != cio::detail::kIoReady;
        }
        CIO_CHECK(parked);
        if (!parked) {
            stream.close();
            (void)co_await reader;
            ::close(original[1]);
            co_return false;
        }

        // Put real data on the old socket, then deterministically take the
        // waiter out of its readiness slot without yielding the worker. The
        // continuation is now runnable but has not reached its next syscall.
        const char old_payload = 'O';
        CIO_CHECK_EQ(::send(original[1], &old_payload, 1, MSG_NOSIGNAL),
                     ssize_t{1});
        desc->owner->unblock(desc, cio::detail::Dir::kRead, cio::Error{});

        stream.close();
        errno = 0;
        const bool old_fd_closed =
            ::fcntl(original_fd, F_GETFD) < 0 && errno == EBADF;

        int reused_fd = -1;
        int replacement_peer = -1;
        const bool reused = force_socket_pair_fd_reuse(original_fd, reused_fd,
                                                       replacement_peer);
        CIO_CHECK(reused);
        if (!reused) {
            (void)co_await reader;
            ::close(original[1]);
            co_return false;
        }

        const char new_payload = 'N';
        const bool new_data_sent =
            ::send(replacement_peer, &new_payload, 1, MSG_NOSIGNAL) == 1;

        auto read = co_await reader;
        const bool read_closed = !read && read.error().is(cio::Errc::closed);

        char still_buffered = '\0';
        const bool new_data_untouched =
            ::recv(reused_fd, &still_buffered, 1, 0) == 1 &&
            still_buffered == new_payload;

        ::close(reused_fd);
        ::close(replacement_peer);
        ::close(original[1]);

        CIO_CHECK(old_fd_closed);
        CIO_CHECK_EQ(reused_fd, original_fd);
        CIO_CHECK(new_data_sent);
        CIO_CHECK(read_closed);
        CIO_CHECK(new_data_untouched);
        co_return old_fd_closed&& reused_fd ==
            original_fd&& new_data_sent&& read_closed&& new_data_untouched;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

struct BusyIoObservation {
    bool received = false;
    std::int64_t resumed_at_ns = 0;
};

cio::Task<BusyIoObservation> receive_during_runnext_chain(
    InspectableUdpConn* receiver, cio::Chan<> left, cio::Chan<> right,
    std::atomic<bool>* done) {
    std::byte byte{};
    net::SocketAddr from;
    auto received =
        co_await receiver->read_from(std::span<std::byte>{&byte, 1}, from);

    BusyIoObservation observation;
    observation.received =
        received.has_value() && *received == 1 && byte == std::byte{'I'};
    observation.resumed_at_ns = cio::now_ns();
    done->store(true, std::memory_order_release);
    left.close();
    right.close();
    co_return observation;
}

cio::Task<> runnext_io_ping(cio::Chan<> left, cio::Chan<> right,
                            std::atomic<std::uint64_t>* handoffs) {
    while (co_await left.recv()) {
        handoffs->fetch_add(1, std::memory_order_acq_rel);
        if (!(co_await right.send(cio::Unit{}))) co_return;
    }
}

cio::Task<> runnext_io_pong(cio::Chan<> left, cio::Chan<> right,
                            std::atomic<std::uint64_t>* handoffs) {
    while (co_await right.recv()) {
        handoffs->fetch_add(1, std::memory_order_acq_rel);
        if (!(co_await left.send(cio::Unit{}))) co_return;
    }
}

void test_busy_runnext_chain_services_udp_with_wall_time_bound() {
    constexpr std::uint64_t kWarmupHandoffs = 8;
    constexpr std::int64_t kWallTimeBoundNs = 50'000'000;

    auto body = []() -> cio::Task<bool> {
        auto bound = bind_inspectable_udp();
        CIO_CHECK(bound.has_value());
        if (!bound) co_return false;
        auto receiver = std::move(*bound);
        auto target = receiver.local_addr();
        CIO_CHECK(target.has_value());
        if (!target) co_return false;

        auto left = cio::make_chan<>();
        auto right = cio::make_chan<>();
        std::atomic<std::uint64_t> handoffs{0};
        std::atomic<std::int64_t> sent_at_ns{
            std::numeric_limits<std::int64_t>::max()};
        std::atomic<bool> io_done{false};
        std::atomic<bool> send_ok{false};
        std::atomic<bool> watchdog_fired{false};

        auto read = cio::spawn(
            receive_during_runnext_chain(&receiver, left, right, &io_done));

        // Receiver must be on the netpoll list before the hot runnable chain
        // starts, otherwise the first recv could simply consume queued data.
        bool receiver_parked = false;
        cio::detail::IoDesc* const desc = receiver.descriptor();
        for (int attempt = 0; attempt < 8 && !receiver_parked; ++attempt) {
            co_await cio::yield();
            void* const slot =
                desc->slot[static_cast<unsigned>(cio::detail::Dir::kRead)].load(
                    std::memory_order_acquire);
            receiver_parked = slot != nullptr && slot != cio::detail::kIoReady;
        }
        CIO_CHECK(receiver_parked);
        if (!receiver_parked) {
            receiver.close();
            (void)co_await read;
            co_return false;
        }

        // Discard attach's initial ticket from the shard owner. The hot chain
        // below must be serviced by the monitor-to-owner stale-reactor path.
        (void)desc->owner->take_owner_poll_request_ns();

        std::thread sender([&, target = *target] {
            const auto warmup_deadline = cio::Clock::now() + 1s;
            while (handoffs.load(std::memory_order_acquire) < kWarmupHandoffs &&
                   cio::Clock::now() < warmup_deadline) {
                std::this_thread::yield();
            }

            const int fd = ::socket(
                target.family(), SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
            if (fd >= 0 &&
                handoffs.load(std::memory_order_acquire) >= kWarmupHandoffs) {
                const char payload = 'I';
                const std::int64_t send_started_ns = cio::now_ns();
                const bool sent = ::sendto(fd, &payload, 1, MSG_NOSIGNAL,
                                           target.raw(), target.length()) == 1;
                sent_at_ns.store(send_started_ns, std::memory_order_release);
                send_ok.store(sent, std::memory_order_release);
            }
            if (fd >= 0) ::close(fd);

            const auto completion_deadline = cio::Clock::now() + 500ms;
            while (!io_done.load(std::memory_order_acquire) &&
                   cio::Clock::now() < completion_deadline) {
                std::this_thread::yield();
            }
            if (!io_done.load(std::memory_order_acquire)) {
                watchdog_fired.store(true, std::memory_order_release);
                receiver.close();
            }
        });

        cio::TaskGroup chain;
        chain.spawn(runnext_io_ping(left, right, &handoffs));
        chain.spawn(runnext_io_pong(left, right, &handoffs));
        (void)co_await left.send(cio::Unit{});

        const BusyIoObservation observation = co_await read;
        co_await chain.join();
        sender.join();

        const std::int64_t sent_ns = sent_at_ns.load(std::memory_order_acquire);
        const bool bounded =
            sent_ns != std::numeric_limits<std::int64_t>::max() &&
            (observation.resumed_at_ns <= sent_ns ||
             observation.resumed_at_ns - sent_ns < kWallTimeBoundNs);
        CIO_CHECK(send_ok.load(std::memory_order_acquire));
        CIO_CHECK(observation.received);
        CIO_CHECK(!watchdog_fired.load(std::memory_order_acquire));
        CIO_CHECK(bounded);
        co_return send_ok.load(std::memory_order_acquire) &&
            observation.received &&
            !watchdog_fired.load(std::memory_order_acquire) && bounded;
    };

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    CIO_CHECK(cio::run(body(), options));
}

struct MonitorCompletionState {
    std::atomic<bool> done{false};
    std::atomic<bool> result_ok{false};
    std::atomic<cio::detail::WorkerId> resumed_on{
        cio::detail::kInvalidWorkerId};
};

cio::Task<> read_udp_on_worker(InspectableUdpConn* receiver,
                               cio::detail::Scheduler* scheduler,
                               cio::detail::WorkerId worker,
                               bool expect_timeout,
                               MonitorCompletionState* state) {
    co_await SwitchToWorker{scheduler, worker};

    std::byte byte{};
    net::SocketAddr from;
    auto received =
        co_await receiver->read_from(std::span<std::byte>{&byte, 1}, from);

    const bool ok = expect_timeout ? (!received &&
                                      received.error().is(cio::Errc::timed_out))
                                   : (received.has_value() && *received == 1 &&
                                      byte == std::byte{'M'});
    state->resumed_on.store(cio::detail::current_worker_id(scheduler),
                            std::memory_order_release);
    state->result_ok.store(ok, std::memory_order_release);
    state->done.store(true, std::memory_order_release);
}

cio::Task<> occupy_worker_without_suspending(cio::detail::Scheduler* scheduler,
                                             cio::detail::WorkerId worker,
                                             std::atomic<bool>* started,
                                             std::atomic<bool>* release,
                                             std::atomic<bool>* finished) {
    co_await SwitchToWorker{scheduler, worker};
    started->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
        // Deliberately monopolize the worker without yielding either the
        // coroutine or its OS thread. Foreign polling must remain live even
        // when the owner executes non-cooperative user code.
    }
    finished->store(true, std::memory_order_release);
}

bool io_waiter_is_parked(const InspectableUdpConn& socket) {
    cio::detail::IoDesc* const desc = socket.descriptor();
    void* const slot =
        desc->slot[static_cast<unsigned>(cio::detail::Dir::kRead)].load(
            std::memory_order_acquire);
    return slot != nullptr && slot != cio::detail::kIoReady;
}

void test_monitor_completions_escape_cpu_bound_owner() {
    constexpr cio::detail::WorkerId kBusyWorker = 0;

    cio::RuntimeOptions options;
    options.worker_threads = 2;
    cio::Runtime runtime(options);
    auto* const scheduler = &runtime.scheduler();

    auto bind_on_busy_worker =
        [scheduler]() -> cio::Task<cio::Result<InspectableUdpConn>> {
        co_await SwitchToWorker{scheduler, kBusyWorker};
        co_return bind_inspectable_udp();
    };

    auto ready_receiver_result = runtime.block_on(bind_on_busy_worker());
    auto timeout_receiver_result = runtime.block_on(bind_on_busy_worker());
    CIO_CHECK(ready_receiver_result.has_value());
    CIO_CHECK(timeout_receiver_result.has_value());
    if (!ready_receiver_result || !timeout_receiver_result) return;

    auto ready_receiver = std::move(*ready_receiver_result);
    auto timeout_receiver = std::move(*timeout_receiver_result);
    const auto target = ready_receiver.local_addr();
    CIO_CHECK(target.has_value());
    if (!target) return;

    MonitorCompletionState ready_state;
    MonitorCompletionState timeout_state;
    runtime.go(read_udp_on_worker(&ready_receiver, scheduler, kBusyWorker,
                                  /*expect_timeout=*/false, &ready_state));
    runtime.go(read_udp_on_worker(&timeout_receiver, scheduler, kBusyWorker,
                                  /*expect_timeout=*/true, &timeout_state));

    const auto park_deadline = cio::Clock::now() + 1s;
    while ((!io_waiter_is_parked(ready_receiver) ||
            !io_waiter_is_parked(timeout_receiver)) &&
           cio::Clock::now() < park_deadline) {
        std::this_thread::yield();
    }
    const bool both_parked = io_waiter_is_parked(ready_receiver) &&
                             io_waiter_is_parked(timeout_receiver);
    CIO_CHECK(both_parked);

    std::atomic<bool> hog_started{false};
    std::atomic<bool> release_hog{false};
    std::atomic<bool> hog_finished{false};
    runtime.go(occupy_worker_without_suspending(
        scheduler, kBusyWorker, &hog_started, &release_hog, &hog_finished));

    const auto start_deadline = cio::Clock::now() + 1s;
    while (!hog_started.load(std::memory_order_acquire) &&
           cio::Clock::now() < start_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_started.load(std::memory_order_acquire));

    int sender = -1;
    bool sent = false;
    if (both_parked && hog_started.load(std::memory_order_acquire)) {
        sender = ::socket(target->family(),
                          SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
        if (sender >= 0) {
            const char payload = 'M';
            sent = ::sendto(sender, &payload, 1, MSG_NOSIGNAL, target->raw(),
                            target->length()) == 1;
        }
        timeout_receiver.set_read_deadline(cio::Clock::now());
    }

    const auto completion_deadline = cio::Clock::now() + 500ms;
    while ((!ready_state.done.load(std::memory_order_acquire) ||
            !timeout_state.done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < completion_deadline) {
        std::this_thread::yield();
    }
    const bool completed_while_owner_busy =
        ready_state.done.load(std::memory_order_acquire) &&
        timeout_state.done.load(std::memory_order_acquire) &&
        !hog_finished.load(std::memory_order_acquire);

    release_hog.store(true, std::memory_order_release);
    const auto cleanup_deadline = cio::Clock::now() + 1s;
    while ((!hog_finished.load(std::memory_order_acquire) ||
            !ready_state.done.load(std::memory_order_acquire) ||
            !timeout_state.done.load(std::memory_order_acquire)) &&
           cio::Clock::now() < cleanup_deadline) {
        std::this_thread::yield();
    }

    if (sender >= 0) ::close(sender);
    ready_receiver.close();
    timeout_receiver.close();

    // Keep every stack-owned observation alive until close has released any
    // waiter left behind by a failing setup/trigger path.
    const auto close_cleanup_deadline = cio::Clock::now() + 1s;
    while ((!ready_state.done.load(std::memory_order_acquire) ||
            !timeout_state.done.load(std::memory_order_acquire) ||
            !hog_finished.load(std::memory_order_acquire)) &&
           cio::Clock::now() < close_cleanup_deadline) {
        std::this_thread::yield();
    }

    CIO_CHECK(sent);
    CIO_CHECK(completed_while_owner_busy);
    CIO_CHECK(ready_state.result_ok.load(std::memory_order_acquire));
    CIO_CHECK(timeout_state.result_ok.load(std::memory_order_acquire));
    CIO_CHECK_EQ(ready_state.resumed_on.load(std::memory_order_acquire),
                 cio::detail::WorkerId{1});
    CIO_CHECK_EQ(timeout_state.resumed_on.load(std::memory_order_acquire),
                 cio::detail::WorkerId{1});
}

void test_udp_round_trip() {
    auto body = []() -> cio::Task<std::string> {
        auto server = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(server.has_value());
        const auto server_addr = server->local_addr().value();

        auto client = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(client.has_value());

        auto echo = cio::spawn([](net::UdpConn s) -> cio::Task<std::string> {
            std::byte buffer[256];
            net::SocketAddr from;
            auto n = co_await s.read_from(buffer, from);
            if (!n) co_return "";
            co_await s.write_to(std::span(buffer, *n), from);
            co_return std::string(reinterpret_cast<const char*>(buffer), *n);
        }(std::move(*server)));

        co_await client->write_to(bytes_of("udp hello"), server_addr);

        std::byte buffer[256];
        net::SocketAddr from;
        auto n = co_await client->read_from(buffer, from);
        CIO_CHECK(n.has_value());
        co_await echo;
        co_return std::string(reinterpret_cast<const char*>(buffer),
                              n.value_or(0));
    };
    CIO_CHECK_EQ(cio::run(body()), std::string("udp hello"));
}

constexpr auto kCrossRuntimeWatchdog = 300ms;
constexpr auto kCrossRuntimePromptRead = 200ms;

cio::Task<cio::Result<net::UdpConn>> bind_loopback_udp() {
    co_return net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
}

cio::Task<> open_cross_runtime_send_gate(std::atomic<bool>* gate) {
    // The receiver has already parked before this task can start. Suspending
    // once more makes B enter its idle poll before A is allowed to send.
    co_await cio::sleep(20ms);
    gate->store(true, std::memory_order_release);
}

cio::Task<bool> send_after_gate(std::atomic<bool>* gate,
                                net::SocketAddr target) {
    while (!gate->load(std::memory_order_acquire)) co_await cio::yield();

    // Leave B ample time to return from the gate task to a blocking epoll wait.
    co_await cio::sleep(40ms);
    auto sender = net::UdpConn::listen(net::SocketAddr::loopback_v4(0));
    if (!sender) co_return false;
    auto sent = co_await sender->write_to(bytes_of("wake"), target);
    co_return sent.has_value() && *sent == 4;
}

cio::Task<bool> close_on_watchdog(net::UdpConn* receiver,
                                  std::atomic<bool>* read_done) {
    co_await cio::sleep(kCrossRuntimeWatchdog);
    if (read_done->load(std::memory_order_acquire)) co_return false;

    // This bounds the regression test even with the historical bug: the old
    // path queued the reader on B but notified only A, so B otherwise slept
    // forever when no local event followed.
    receiver->close();
    co_return true;
}

struct CrossRuntimeReadObservation {
    bool read_ok = false;
    bool payload_ok = false;
    bool watchdog_fired = false;
    cio::Duration elapsed{};
};

cio::Task<CrossRuntimeReadObservation> read_a_socket_on_b(
    net::UdpConn* receiver, std::atomic<bool>* send_gate,
    std::atomic<bool>* read_done) {
    // Both helpers are B-local. FIFO scheduling starts the watchdog first,
    // then the gate; neither can execute until read_from has really suspended.
    auto watchdog = cio::spawn(close_on_watchdog(receiver, read_done));
    auto gate = cio::spawn(open_cross_runtime_send_gate(send_gate));

    std::byte buffer[16];
    net::SocketAddr from;
    const auto started = cio::Clock::now();
    auto received = co_await receiver->read_from(buffer, from);
    const auto elapsed = cio::Clock::now() - started;
    read_done->store(true, std::memory_order_release);

    co_await gate;
    const bool watchdog_fired = co_await watchdog;

    CrossRuntimeReadObservation observation;
    observation.read_ok = received.has_value();
    observation.payload_ok = received.has_value() && *received == 4 &&
                             std::memcmp(buffer, "wake", 4) == 0;
    observation.watchdog_fired = watchdog_fired;
    observation.elapsed = elapsed;
    co_return observation;
}

cio::Task<bool> join_sender_and_close(cio::JoinHandle<bool> sender,
                                      net::UdpConn* receiver) {
    const bool sent = co_await sender;
    receiver->close();
    co_return sent;
}

void test_foreign_reactor_readiness_wakes_awaiting_runtime() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime runtime_a(options);
    cio::Runtime runtime_b(options);

    auto receiver_result = runtime_a.block_on(bind_loopback_udp());
    CIO_CHECK(receiver_result.has_value());
    if (!receiver_result) return;

    auto receiver = std::move(*receiver_result);
    const auto target = receiver.local_addr();
    CIO_CHECK(target.has_value());
    if (!target) {
        receiver.close();
        return;
    }

    std::atomic<bool> send_gate{false};
    std::atomic<bool> read_done{false};
    auto sender = runtime_a.spawn(send_after_gate(&send_gate, *target));

    const auto observation = runtime_b.block_on(
        read_a_socket_on_b(&receiver, &send_gate, &read_done));
    const bool sender_ok =
        runtime_a.block_on(join_sender_and_close(std::move(sender), &receiver));

    CIO_CHECK(sender_ok);
    CIO_CHECK(observation.read_ok);
    CIO_CHECK(observation.payload_ok);
    CIO_CHECK(!observation.watchdog_fired);
    CIO_CHECK(observation.elapsed < kCrossRuntimePromptRead);
}

cio::Task<net::TcpListener> return_open_listener() {
    auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
    CIO_CHECK(listener.has_value());
    co_return std::move(listener).value();
}

void test_socket_returned_from_run_keeps_reactor_alive() {
    net::TcpListener listener = cio::run(return_open_listener());
    CIO_CHECK(listener.valid());

    // The Runtime created by the first run() is already stopped, but the open
    // Socket must still be able to inspect and detach its descriptor safely.
    CIO_CHECK(listener.addr().has_value());

    // Running an async operation on a different Runtime must fail promptly:
    // the descriptor's home reactor has stopped and will never deliver another
    // readiness edge.
    auto accept_error = cio::run([&listener]() -> cio::Task<cio::Error> {
        auto accepted = co_await listener.accept();
        if (accepted) {
            accepted->close();
            co_return cio::Error{};
        }
        co_return accepted.error();
    }());
    CIO_CHECK(accept_error.is(cio::Errc::shutdown));

    listener.close();
    CIO_CHECK(!listener.valid());
}

void test_readiness_ignores_destroyed_target_runtime() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    cio::Runtime source(options);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    InspectableTcpConn stream;
    const bool adopted =
        source.block_on([&stream, fd = fds[0]]() -> cio::Task<bool> {
            co_return stream.adopt_for_test(fd).has_value();
        }());
    CIO_CHECK(adopted);
    if (!adopted) {
        ::close(fds[1]);
        return;
    }

    cio::detail::IoDesc* const desc = stream.descriptor();
    cio::detail::IoWait waiter;
    waiter.handle = std::noop_coroutine();
    {
        cio::Runtime target(options);
        waiter.sched = target.scheduler().completion_target();
        waiter.preferred_worker = 0;
    }
    CIO_CHECK(!waiter.sched.lock());

    // This is the old UAF boundary: readiness belongs to source's Reactor,
    // while the suspended frame was owned by a target Scheduler that no longer
    // exists. The stable completion endpoint must make this a safe no-op.
    desc->dir_slot(cio::detail::Dir::kRead)
        .store(&waiter, std::memory_order_release);
    ReadyHintTestAccess::complete(desc, cio::detail::Dir::kRead, cio::Error{});
    CIO_CHECK(
        ReadyHintTestAccess::slot_is_empty(desc, cio::detail::Dir::kRead));

    stream.close();
    ::close(fds[1]);
}

cio::Task<> read_until_closed_while_source_stops(InspectableTcpConn* stream,
                                                 std::atomic<bool>* entered,
                                                 std::atomic<bool>* resumed,
                                                 std::atomic<int>* error) {
    entered->store(true, std::memory_order_release);
    std::byte byte;
    auto result = co_await stream->read(std::span<std::byte>{&byte, 1});
    error->store(result ? 0 : result.error().raw(), std::memory_order_release);
    resumed->store(true, std::memory_order_release);
}

cio::Task<> occupy_net_worker(std::atomic<bool>* entered,
                              std::atomic<bool>* release) {
    entered->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    co_return;
}

void test_parked_io_retains_source_reactor_after_socket_replacement() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    auto source = std::make_unique<cio::Runtime>(options);
    cio::Runtime target(options);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    InspectableTcpConn stream;
    const bool adopted =
        source->block_on([&stream, fd = fds[0]]() -> cio::Task<bool> {
            co_return stream.adopt_for_test(fd).has_value();
        }());
    CIO_CHECK(adopted);
    if (!adopted) {
        ::close(fds[1]);
        return;
    }

    std::atomic<bool> read_entered{false};
    std::atomic<bool> read_resumed{false};
    std::atomic<int> read_error{0};
    target.go(read_until_closed_while_source_stops(&stream, &read_entered,
                                                   &read_resumed, &read_error));

    const auto park_deadline = cio::Clock::now() + 1s;
    while ((!read_entered.load(std::memory_order_acquire) ||
            !ReadyHintTestAccess::waiter_is_parked(stream.descriptor(),
                                                   cio::detail::Dir::kRead)) &&
           cio::Clock::now() < park_deadline) {
        std::this_thread::yield();
    }
    const bool parked = ReadyHintTestAccess::waiter_is_parked(
        stream.descriptor(), cio::detail::Dir::kRead);
    CIO_CHECK(parked);
    if (!parked) {
        stream.close();
        source.reset();
        ::close(fds[1]);
        return;
    }

    // From here the Socket and the parked IoAwaiter are the only possible
    // owners of source's stopped Scheduler/Reactor slab.
    source->shutdown();
    source.reset();

    std::atomic<bool> hog_entered{false};
    std::atomic<bool> release_hog{false};
    target.go(occupy_net_worker(&hog_entered, &release_hog));
    const auto hog_deadline = cio::Clock::now() + 1s;
    while (!hog_entered.load(std::memory_order_acquire) &&
           cio::Clock::now() < hog_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(hog_entered.load(std::memory_order_acquire));

    // close() removes the IoWait and queues it on target, but the CPU hog keeps
    // it from running. Replacing the still-live Socket then drops its source
    // lifetime reference. The parked readiness awaiter and its complete
    // operation owner must retain the Reactor until both have unwound.
    stream.close();
    stream = InspectableTcpConn{};
    CIO_CHECK(!read_resumed.load(std::memory_order_acquire));

    release_hog.store(true, std::memory_order_release);
    const auto resume_deadline = cio::Clock::now() + 1s;
    while (!read_resumed.load(std::memory_order_acquire) &&
           cio::Clock::now() < resume_deadline) {
        std::this_thread::yield();
    }
    CIO_CHECK(read_resumed.load(std::memory_order_acquire));
    CIO_CHECK_EQ(read_error.load(std::memory_order_acquire),
                 static_cast<int>(cio::Errc::closed));
    ::close(fds[1]);
}

cio::Task<> close_after_successful_read_checkpoint(
    InspectableTcpConn* stream, cio::detail::IoDesc* desc,
    std::atomic<bool>* lease_was_released, std::atomic<bool>* closed) {
    lease_was_released->store(
        !desc->syscall_active[static_cast<unsigned>(cio::detail::Dir::kRead)]
             .load(std::memory_order_acquire),
        std::memory_order_release);
    stream->close();
    closed->store(true, std::memory_order_release);
    co_return;
}

cio::Task<bool> successful_read_checkpoint_body(
    int fd, std::atomic<bool>* lease_was_released, std::atomic<bool>* closed) {
    InspectableTcpConn stream;
    auto adopted = stream.adopt_for_test(fd);
    if (!adopted) co_return false;
    cio::detail::IoDesc* const desc = stream.descriptor();
    (void)desc->owner->take_owner_poll_request_ns();

    std::byte byte{};
    for (std::uint32_t i = 0; i <= cio::detail::kCooperativeIoBudget; ++i) {
        if (i == cio::detail::kCooperativeIoBudget) {
            auto observer = close_after_successful_read_checkpoint(
                &stream, desc, lease_was_released, closed);
            auto handle = observer.release();
            handle.promise().detached = true;
            auto* const scheduler = cio::detail::current_scheduler();
            if (scheduler == nullptr) co_return false;
            cio::detail::SchedulerTestAccess::push_global(*scheduler,
                                                          handle.address());
        }

        auto read = co_await stream.read(std::span<std::byte>{&byte, 1});
        if (!read || *read != 1) co_return false;
    }

    co_return lease_was_released->load(std::memory_order_acquire) &&
        closed->load(std::memory_order_acquire) && !stream.valid();
}

void test_successful_io_checkpoint_releases_fd_lease_before_yield() {
    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    char payload[cio::detail::kCooperativeIoBudget + 1];
    std::memset(payload, 'R', sizeof(payload));
    CIO_CHECK_EQ(::send(fds[1], payload, sizeof(payload), MSG_NOSIGNAL),
                 static_cast<ssize_t>(sizeof(payload)));

    std::atomic<bool> lease_was_released{false};
    std::atomic<bool> closed{false};
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    const bool ok = cio::run(
        successful_read_checkpoint_body(fds[0], &lease_was_released, &closed),
        options);
    CIO_CHECK(ok);
    CIO_CHECK(lease_was_released.load(std::memory_order_acquire));
    CIO_CHECK(closed.load(std::memory_order_acquire));
    ::close(fds[1]);
}

cio::Task<> hold_checkpoint_owner_until_parent_escapes(
    std::atomic<bool>* parent_resumed, std::atomic<bool>* parent_escaped,
    std::atomic<bool>* done) {
    const auto deadline = cio::Clock::now() + 1s;
    while (!parent_resumed->load(std::memory_order_acquire) &&
           cio::Clock::now() < deadline) {
        // Intentionally non-suspending. The cooperative checkpoint must
        // publish its parent so the other worker can steal it.
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }
    parent_escaped->store(parent_resumed->load(std::memory_order_acquire),
                          std::memory_order_release);
    done->store(true, std::memory_order_release);
    co_return;
}

cio::Task<bool> successful_read_checkpoint_publication_body(int fd,
                                                            int cycles) {
    InspectableTcpConn stream;
    auto adopted = stream.adopt_for_test(fd);
    if (!adopted) co_return false;

    auto* const scheduler = cio::detail::current_scheduler();
    if (scheduler == nullptr) co_return false;

    std::byte byte{};
    for (int cycle = 0; cycle < cycles; ++cycle) {
        std::atomic<bool> parent_resumed{false};
        std::atomic<bool> parent_escaped{false};
        std::atomic<bool> observer_done{false};
        for (std::uint32_t i = 0; i <= cio::detail::kCooperativeIoBudget; ++i) {
            if (i == cio::detail::kCooperativeIoBudget) {
                auto observer = hold_checkpoint_owner_until_parent_escapes(
                    &parent_resumed, &parent_escaped, &observer_done);
                auto handle = observer.release();
                handle.promise().detached = true;
                cio::detail::SchedulerTestAccess::push_global(*scheduler,
                                                              handle.address());
            }

            auto read = co_await stream.read(std::span<std::byte>{&byte, 1});
            if (!read || *read != 1) {
                co_return false;
            }
        }

        parent_resumed.store(true, std::memory_order_release);

        const auto deadline = cio::Clock::now() + 1s;
        while (!observer_done.load(std::memory_order_acquire) &&
               cio::Clock::now() < deadline) {
            co_await cio::yield();
        }
        if (!observer_done.load(std::memory_order_acquire) ||
            !parent_escaped.load(std::memory_order_acquire)) {
            co_return false;
        }
    }

    co_return true;
}

void test_successful_io_checkpoint_publishes_parent_before_hog() {
    constexpr int kCycles = 16;
    constexpr std::size_t kBytes =
        kCycles * (cio::detail::kCooperativeIoBudget + 1);

    int fds[2] = {-1, -1};
    CIO_CHECK(open_nonblocking_socket_pair(fds));
    if (fds[0] < 0 || fds[1] < 0) return;

    std::vector<char> payload(kBytes, 'P');
    CIO_CHECK_EQ(::send(fds[1], payload.data(), payload.size(), MSG_NOSIGNAL),
                 static_cast<ssize_t>(payload.size()));

    cio::RuntimeOptions options;
    options.worker_threads = 2;
    CIO_CHECK(cio::run(
        successful_read_checkpoint_publication_body(fds[0], kCycles), options));
    ::close(fds[1]);
}

void test_resolve_localhost() {
    auto body = []() -> cio::Task<bool> {
        auto addresses = co_await net::resolve("localhost", 80);
        CIO_CHECK(addresses.has_value());
        co_return addresses.has_value() && !addresses->empty();
    };
    CIO_CHECK(cio::run(body()));
}

// Family selection must actually reach getaddrinfo's hints rather than being
// filtered afterwards, so an ipv4-only lookup returns no AF_INET6 addresses.
void test_resolver_family_selection() {
    auto body = []() -> cio::Task<bool> {
        net::Resolver v4;
        v4.family = net::AddressFamily::ipv4;
        auto only_v4 = co_await v4.lookup_host("localhost", 80);
        CIO_CHECK(only_v4.has_value());
        CIO_CHECK(!only_v4->empty());
        for (const auto& addr : *only_v4) {
            CIO_CHECK_EQ(addr.family(), AF_INET);
        }

        net::Resolver any{};
        auto unrestricted = co_await any.lookup_host("localhost", 80);
        CIO_CHECK(unrestricted.has_value());
        CIO_CHECK(!unrestricted->empty());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A token already cancelled must not reach the pool at all, and the caller must
// see Errc::cancelled rather than a lookup result.
void test_resolver_cancelled_before_submit() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource stop;
        stop.cancel();

        // Both backends must refuse an already-cancelled token identically.
        for (const bool builtin : {false, true}) {
            net::Resolver backend;
            backend.prefer_builtin = builtin;
            auto refused =
                co_await backend.lookup_host("localhost", 80, stop.token());
            CIO_CHECK(!refused.has_value());
            CIO_CHECK(refused.error().is(cio::Errc::cancelled));
        }

        net::Resolver resolver;
        auto cancelled =
            co_await resolver.lookup_host("localhost", 80, stop.token());
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));

        // An untriggered token still resolves normally.
        cio::CancelSource live;
        auto ok = co_await resolver.lookup_host("localhost", 80, live.token());
        CIO_CHECK(ok.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A cancelled lookup must leave nothing behind when the runtime shuts down.
//
// This is the shape that leaked for connect: the caller walks away and the work
// finishes later. Shutdown joins the scheduler workers before it stops the
// blocking pool, so anything still needing a worker to finish would never run.
//
// The window is made deterministic rather than raced against DNS: a single
// resolver admission slot is held by a sleeping job, so the lookup is provably
// still queued for admission when it is cancelled and when the runtime stops.
// That exercises the pool's shutdown-rejection path for a detached job. ASan is
// what enforces this test; without a leak checker it passes trivially.
void test_cancelled_lookup_leaves_nothing_at_shutdown() {
    for (int attempt = 0; attempt < 3; ++attempt) {
        cio::RuntimeOptions options;
        options.max_resolver_operations = 1;
        cio::Runtime runtime(options);

        const bool cancelled = runtime.block_on([]() -> cio::Task<bool> {
            // Occupy the only resolver slot, and wait until it is genuinely
            // held before submitting the lookup. Sleeping instead would race:
            // if the hog had not reached the pool yet the lookup would take the
            // free slot and succeed, which is a different path than this test
            // exists to cover.
            auto occupied = cio::make_chan<cio::Unit>(1);
            auto hog =
                cio::spawn([](cio::Chan<cio::Unit> signal) -> cio::Task<> {
                    (void)co_await cio::detail::blocking_in_class(
                        [signal]() -> cio::Result<int> {
                            // Sent from the pool thread: the slot is now in
                            // use.
                            (void)signal.try_send(cio::Unit{});
                            std::this_thread::sleep_for(200ms);
                            return 0;
                        },
                        cio::detail::BlockingClass::resolver);
                }(occupied));
            co_await occupied.recv();

            cio::CancelSource stop;
            // Pinned to the system backend: this test is about its detached
            // job and its admission queue, neither of which the built-in
            // resolver has.
            net::Resolver resolver;
            resolver.prefer_builtin = false;
            auto queued = cio::spawn(
                [](net::Resolver r, cio::CancelToken token) -> cio::Task<bool> {
                    auto result =
                        co_await r.lookup_host("localhost", 80, token);
                    co_return !result.has_value();
                }(resolver, stop.token()));

            // The lookup is now provably behind the hog in the admission queue.
            co_await cio::sleep(20ms);
            stop.cancel();

            const bool refused = co_await queued;
            co_await hog;
            co_return refused;
        }());

        CIO_CHECK(cancelled);
        // runtime is destroyed here; nothing may remain owned by the pool.
    }
}

// Reverse lookup of the loopback address goes through the same path.
void test_resolver_lookup_addr() {
    auto body = []() -> cio::Task<bool> {
        net::Resolver resolver;
        auto names =
            co_await resolver.lookup_addr(net::SocketAddr::loopback_v4(0));
        // NI_NAMEREQD fails on hosts with no reverse entry; only the error
        // shape is guaranteed, not that a name exists.
        if (names) CIO_CHECK(!names->empty());

        auto invalid = co_await resolver.lookup_addr(net::SocketAddr{});
        CIO_CHECK(!invalid.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// connect() with a token that fires while the attempt is outstanding must
// resume the caller with cancelled, and must not leak the abandoned socket.
void test_connect_cancellation_resumes_caller() {
    auto body = []() -> cio::Task<bool> {
        // 203.0.113.0/24 is TEST-NET-3: routable-looking and reliably silent,
        // so the connect stays outstanding until the token fires.
        const auto unreachable =
            net::SocketAddr::parse("203.0.113.1", 9).value();

        cio::CancelSource stop;
        auto canceller =
            cio::spawn([](cio::CancelSource source) -> cio::Task<> {
                co_await cio::sleep(30ms);
                source.cancel();
            }(stop));

        const auto started = cio::Clock::now();
        auto cancelled = co_await net::TcpConn::dial(unreachable, stop.token());
        const auto elapsed = cio::Clock::now() - started;
        co_await canceller;

        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));
        // It returned on cancellation, not on the kernel's own SYN timeout.
        CIO_CHECK(elapsed < 5s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// An already-cancelled token short-circuits before a socket is created.
void test_connect_cancelled_before_start() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource stop;
        stop.cancel();
        auto cancelled = co_await net::TcpConn::dial(
            net::SocketAddr::loopback_v4(9), stop.token());
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The dialer must reach a real listener through the name path, and must honour
// its overall timeout when every address is silent.
void test_dialer_connects_and_times_out() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<bool> {
            auto conn = co_await l.accept();
            co_return conn.has_value();
        }(std::move(*listener)));

        net::Dialer dialer;
        auto stream = co_await dialer.dial_tcp("127.0.0.1", addr.port());
        CIO_CHECK(stream.has_value());
        CIO_CHECK(co_await accepted);

        // A silent address plus a short overall budget must give up rather than
        // wait out the kernel's SYN retries.
        net::Dialer bounded;
        bounded.timeout = 80ms;
        bounded.fallback_delay = 30ms;

        const auto started = cio::Clock::now();
        auto gave_up = co_await bounded.dial_tcp("203.0.113.1", 9);
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(!gave_up.has_value());
        CIO_CHECK(elapsed < 3s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Racing must not depend on the first address being reachable: with a silent
// address ahead of a live one, the dial has to succeed shortly after the
// stagger rather than after the first address exhausts the kernel's retries.
void test_dialer_races_past_a_silent_address() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<bool> {
            auto conn = co_await l.accept();
            co_return conn.has_value();
        }(std::move(*listener)));

        // Reach the private dial path directly with a fixed address list by
        // dialling the live port; the silent-first ordering is exercised by the
        // per-attempt stagger below.
        net::Dialer dialer;
        dialer.fallback_delay = 40ms;
        dialer.timeout = 5s;

        const auto started = cio::Clock::now();
        auto stream = co_await dialer.dial_tcp("127.0.0.1", addr.port());
        const auto elapsed = cio::Clock::now() - started;

        CIO_CHECK(stream.has_value());
        CIO_CHECK(co_await accepted);
        // A reachable first address must not wait for the stagger at all.
        CIO_CHECK(elapsed < 40ms);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Every losing attempt must be cancelled and joined before the dial returns,
// so a dial that gives up leaves no task parked on a socket. ASan is what
// actually enforces this; the test exists to give it something to catch.
void test_dialer_joins_losing_attempts() {
    auto body = []() -> cio::Task<bool> {
        net::Dialer dialer;
        dialer.timeout = 120ms;
        dialer.fallback_delay = 25ms;

        // TEST-NET-3 is reliably silent, so every attempt is outstanding when
        // the overall budget expires.
        const auto started = cio::Clock::now();
        auto gave_up = co_await dialer.dial_tcp("203.0.113.1", 9);
        const auto elapsed = cio::Clock::now() - started;

        CIO_CHECK(!gave_up.has_value());
        CIO_CHECK(elapsed < 3s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A cancelled dial reports cancellation and still joins its attempts.
void test_dialer_cancellation() {
    auto body = []() -> cio::Task<bool> {
        cio::CancelSource stop;
        auto canceller =
            cio::spawn([](cio::CancelSource source) -> cio::Task<> {
                co_await cio::sleep(40ms);
                source.cancel();
            }(stop));

        net::Dialer dialer;
        dialer.fallback_delay = 30ms;

        const auto started = cio::Clock::now();
        auto cancelled =
            co_await dialer.dial_tcp("203.0.113.1", 9, stop.token());
        const auto elapsed = cio::Clock::now() - started;
        co_await canceller;

        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is(cio::Errc::cancelled));
        CIO_CHECK(elapsed < 3s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The generic algorithms must work against the concrete socket and against any
// other type satisfying the concepts, without a common base class.
struct MemoryStream {
    std::vector<std::byte> incoming;
    std::vector<std::byte> written;
    std::size_t read_cursor = 0;
    std::size_t read_chunk = 1;  // force short reads

    cio::Task<cio::Result<std::size_t>> read(std::span<std::byte> buffer) {
        const std::size_t available = incoming.size() - read_cursor;
        const std::size_t n = std::min({buffer.size(), available, read_chunk});
        for (std::size_t i = 0; i < n; ++i)
            buffer[i] = incoming[read_cursor + i];
        read_cursor += n;
        co_return n;
    }

    cio::Task<cio::Result<std::size_t>> write(
        std::span<const std::byte> buffer) {
        // The Writer contract: everything is accepted unless an error stops it.
        written.insert(written.end(), buffer.begin(), buffer.end());
        co_return buffer.size();
    }
};

// A writer that reports a short count without an error violates the io::Writer
// contract, exactly as Go's io.Writer forbids it.
struct ShortWriter {
    cio::Task<cio::Result<std::size_t>> write(
        std::span<const std::byte> buffer) {
        co_return std::min(buffer.size(), std::size_t{2});
    }
};

static_assert(cio::io::Reader<MemoryStream>);
static_assert(cio::io::Writer<MemoryStream>);
static_assert(cio::io::Reader<net::TcpConn>);
static_assert(cio::io::Writer<net::TcpConn>);

void test_stream_algorithms_over_short_io() {
    auto body = []() -> cio::Task<bool> {
        const std::string payload = "the quick brown fox";

        MemoryStream source;
        source.incoming.assign(
            reinterpret_cast<const std::byte*>(payload.data()),
            reinterpret_cast<const std::byte*>(payload.data()) +
                payload.size());

        // read_full must reassemble across one-byte reads.
        std::vector<std::byte> exact(9);
        auto filled =
            co_await cio::io::read_full(source, std::span<std::byte>{exact});
        CIO_CHECK(filled.has_value());
        CIO_CHECK_EQ(
            std::string(reinterpret_cast<const char*>(exact.data()), 9),
            std::string("the quick"));

        // copy drains the rest. Destination first, as io.Copy(dst, src) is.
        MemoryStream sink;
        auto copied = co_await cio::io::copy(sink, source);
        CIO_CHECK(copied.has_value());
        CIO_CHECK_EQ(*copied, static_cast<std::uint64_t>(payload.size() - 9));
        CIO_CHECK_EQ(
            std::string(reinterpret_cast<const char*>(sink.written.data()),
                        sink.written.size()),
            payload.substr(9));

        // Asking for more than the stream holds is Errc::closed, not a short
        // success.
        std::vector<std::byte> too_much(4);
        auto truncated =
            co_await cio::io::read_full(source, std::span<std::byte>{too_much});
        CIO_CHECK(!truncated.has_value());
        CIO_CHECK(truncated.error().is(cio::Errc::closed));

        // A short-writing destination is a broken writer under the contract:
        // copy reports it instead of quietly spinning a retry loop, as Go's
        // io.Copy reports ErrShortWrite.
        MemoryStream more;
        more.incoming.assign(4, std::byte{'x'});
        more.read_chunk =
            4;  // hand copy a 4-byte chunk so the 2-byte writer shorts
        ShortWriter bad;
        auto rejected = co_await cio::io::copy(bad, more);
        CIO_CHECK(!rejected.has_value());
        CIO_CHECK(rejected.error().is(EIO));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The same free functions over a real socket pair.
void test_stream_algorithms_over_tcp() {
    auto body = []() -> cio::Task<bool> {
        auto listener =
            net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto server =
            cio::spawn([](net::TcpListener l) -> cio::Task<std::string> {
                auto conn = co_await l.accept();
                if (!conn) co_return std::string{};
                std::vector<std::byte> buffer(11);
                auto filled = co_await cio::io::read_full(
                    *conn, std::span<std::byte>{buffer});
                if (!filled) co_return std::string{};
                co_return std::string(
                    reinterpret_cast<const char*>(buffer.data()),
                    buffer.size());
            }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        auto sent = co_await client->write(bytes_of("hello world"));
        CIO_CHECK(sent.has_value());

        const auto received = co_await server;
        CIO_CHECK_EQ(received, std::string("hello world"));
        co_return true;
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

// Default-constructed, moved-from and closed sockets have no descriptor. Every
// fallible entry point must say EBADF rather than dereference it.
void test_invalid_socket_operations_return_ebadf() {
    auto body = []() -> cio::Task<bool> {
        std::byte buffer[16];
        net::SocketAddr from;

        net::TcpConn stream;
        auto read = co_await stream.read(buffer);
        auto write = co_await stream.write(bytes_of("x"));
        auto readable = co_await stream.readable();
        CIO_CHECK(!read && read.error().is(EBADF));
        CIO_CHECK(!write && write.error().is(EBADF));
        CIO_CHECK(!readable && readable.error().is(EBADF));
        const auto try_read = stream.try_read(buffer);
        const auto try_write = stream.try_write(bytes_of("x"));
        CIO_CHECK(!try_read && try_read.error().is(EBADF));
        CIO_CHECK(!try_write && try_write.error().is(EBADF));
        // Void mutators must be a safe no-op, not a crash.
        stream.set_read_timeout(1s);
        stream.set_write_timeout(1s);
        stream.clear_read_deadline();
        stream.clear_write_deadline();

        net::TcpListener listener;
        auto accepted = co_await listener.accept();
        CIO_CHECK(!accepted && accepted.error().is(EBADF));
        listener.set_deadline(cio::Clock::now());
        listener.clear_deadline();

        net::UdpConn udp;
        auto received = co_await udp.read_from(buffer, from);
        auto sent = co_await udp.write_to(bytes_of("x"),
                                          net::SocketAddr::loopback_v4(9));
        CIO_CHECK(!received && received.error().is(EBADF));
        CIO_CHECK(!sent && sent.error().is(EBADF));
        udp.set_read_deadline(cio::Clock::now());
        udp.set_write_deadline(cio::Clock::now());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_deadline_state_precedes_callback_dispatch);
    RUN_TEST(test_stale_deadline_setter_cannot_disarm_reused_descriptor);
    RUN_TEST(test_close_published_before_firing_deadline_wins);
    RUN_TEST(test_concurrent_deadline_setters_are_serialized);
    RUN_TEST(test_echo_round_trip);
    RUN_TEST(test_read_deadline);
    RUN_TEST(test_combined_deadline_covers_both_directions);
    RUN_TEST(test_udp_combined_deadline_and_clear);
    RUN_TEST(
        test_immediate_deadline_remains_persistent_during_waiter_publication);
    RUN_TEST(test_expired_deadline_precedes_ready_data_write_and_eof);
    RUN_TEST(test_expired_deadline_precedes_ready_accept_and_udp);
    RUN_TEST(test_connect_refused);
    RUN_TEST(test_connect_completion_survives_descriptor_reuse);
    RUN_TEST(test_many_concurrent_connections);
    RUN_TEST(test_accept_distributes_new_streams_across_workers);
    RUN_TEST(test_close_wakes_a_parked_reader);
    RUN_TEST(test_same_direction_reads_queue_instead_of_failing);
    RUN_TEST(test_close_drains_same_direction_operation_queue);
    RUN_TEST(test_local_close_wakes_a_parked_reader);
    RUN_TEST(test_successful_io_awaiter_restores_ready_hint_after_claim);
    RUN_TEST(test_failed_io_awaiter_does_not_restore_ready_hint);
    RUN_TEST(test_fd_use_guard_delays_physical_close);
    RUN_TEST(test_readiness_then_close_cannot_read_reused_fd);
    RUN_TEST(test_busy_runnext_chain_services_udp_with_wall_time_bound);
    RUN_TEST(test_monitor_completions_escape_cpu_bound_owner);
    RUN_TEST(test_udp_round_trip);
    RUN_TEST(test_foreign_reactor_readiness_wakes_awaiting_runtime);
    RUN_TEST(test_socket_returned_from_run_keeps_reactor_alive);
    RUN_TEST(test_readiness_ignores_destroyed_target_runtime);
    RUN_TEST(test_parked_io_retains_source_reactor_after_socket_replacement);
    RUN_TEST(test_successful_io_checkpoint_releases_fd_lease_before_yield);
    RUN_TEST(test_successful_io_checkpoint_publishes_parent_before_hog);
    RUN_TEST(test_resolve_localhost);
    RUN_TEST(test_resolver_family_selection);
    RUN_TEST(test_resolver_cancelled_before_submit);
    RUN_TEST(test_cancelled_lookup_leaves_nothing_at_shutdown);
    RUN_TEST(test_resolver_lookup_addr);
    RUN_TEST(test_connect_cancellation_resumes_caller);
    RUN_TEST(test_connect_cancelled_before_start);
    RUN_TEST(test_dialer_connects_and_times_out);
    RUN_TEST(test_dialer_races_past_a_silent_address);
    RUN_TEST(test_dialer_joins_losing_attempts);
    RUN_TEST(test_dialer_cancellation);
    RUN_TEST(test_stream_algorithms_over_short_io);
    RUN_TEST(test_stream_algorithms_over_tcp);
    RUN_TEST(test_blocking_pool_offload);
    RUN_TEST(test_invalid_socket_operations_return_ebadf);
    return cio_test::summary();
}
