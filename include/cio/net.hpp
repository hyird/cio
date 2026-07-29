// Async TCP/UDP.
//
//     auto listener = cio::net::TcpListener::bind(cio::net::SocketAddr::any_v4(8080)).value();
//     for (;;) {
//         auto conn = co_await listener.accept();
//         if (!conn) break;
//         cio::go(serve(std::move(*conn)));
//     }
//
// Deadlines follow Go: they live on the connection, not on the call, and once
// one passes every operation in that direction fails until it is reset.
//
//     stream.set_read_timeout(5s);
//     auto n = co_await stream.read(buf);      // Errc::timed_out if it elapses
//
// OWNERSHIP: a socket must outlive every task using it. close() is safe to call
// while another task is parked on the socket — it wakes them with Errc::closed
// — but destroying the object out from under a parked task is not.
#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cio/clock.hpp"
#include "cio/detail/reactor.hpp"
#include "cio/group.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

struct sockaddr;

namespace cio::net {

class SocketAddr {
public:
    SocketAddr() = default;

    // Numeric addresses only — no DNS. Use resolve() for names.
    static Result<SocketAddr> parse(std::string_view host, std::uint16_t port);
    static SocketAddr any_v4(std::uint16_t port);
    static SocketAddr loopback_v4(std::uint16_t port);
    static SocketAddr any_v6(std::uint16_t port);
    static SocketAddr loopback_v6(std::uint16_t port);

    // Constructs from a raw sockaddr; `len` must be the true length.
    static SocketAddr from_raw(const void* addr, unsigned len);

    std::string to_string() const;
    std::uint16_t port() const noexcept;
    int family() const noexcept;

    const sockaddr* raw() const noexcept;
    unsigned length() const noexcept { return length_; }
    bool valid() const noexcept { return length_ != 0; }

private:
    // 128 bytes is sockaddr_storage on every platform we target; keeping it as
    // a byte array avoids leaking <sys/socket.h> into every translation unit
    // that touches an address.
    alignas(8) unsigned char storage_[128]{};
    unsigned length_ = 0;
};

enum class AddressFamily {
    any,
    ipv4,
    ipv6,
};

struct LookupOptions {
    AddressFamily family = AddressFamily::any;
    // Use cio's built-in DNS resolver instead of the system one, mirroring
    // Go's Resolver.PreferGo.
    //
    // The built-in resolver speaks DNS over the runtime's own sockets and
    // honours /etc/hosts: a lookup is cancellable mid-flight and occupies no
    // blocking-pool thread. It does not consult NSS, so a machine resolving
    // names through LDAP, NIS or mDNS needs the system resolver.
    //
    // Go defaults this on for Unix; cio defaults it off, because flipping the
    // resolution backend under an existing program is not something a minor
    // release should do silently.
    bool prefer_builtin = false;
};

// Name resolution runs on the blocking pool: getaddrinfo has no async form, and
// the system resolver is the source of truth so /etc/hosts, nsswitch.conf and
// libc policy keep applying. cio implements no DNS protocol and no cache.
//
// CANCELLATION: a lookup cancelled before it starts never reaches the pool. One
// cancelled after getaddrinfo() has begun resumes the caller with
// Errc::cancelled while the lookup finishes in the background; its late result
// is discarded and never resumes the caller a second time. That is why the job
// owns its own strings and result storage rather than borrowing the frame's.
class Resolver {
public:
    Resolver() = default;
    explicit Resolver(LookupOptions options) noexcept : options_(options) {}

    Task<Result<std::vector<SocketAddr>>> lookup_host(
        std::string host, std::uint16_t port, CancelToken cancel = {}) const;

    // Reverse lookup. Returns the names for an address, longest-standing first.
    Task<Result<std::vector<std::string>>> lookup_addr(
        SocketAddr address, CancelToken cancel = {}) const;

    const LookupOptions& options() const noexcept { return options_; }

private:
    LookupOptions options_{};
};

// Delegates to a default-constructed Resolver.
Task<Result<std::vector<SocketAddr>>> resolve(std::string host, std::uint16_t port);

// Base for the socket types: owns the fd and its reactor registration.
class Socket {
public:
    Socket() = default;
    ~Socket() { close(); }

    Socket(Socket&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)),
          desc_(std::exchange(other.desc_, nullptr)),
          scheduler_lifetime_(std::move(other.scheduler_lifetime_)),
          cancel_binding_(std::move(other.cancel_binding_)),
          cancel_token_(std::move(other.cancel_token_)) {}
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = std::exchange(other.fd_, -1);
            desc_ = std::exchange(other.desc_, nullptr);
            scheduler_lifetime_ =
                std::move(other.scheduler_lifetime_);
            cancel_binding_ = std::move(other.cancel_binding_);
            cancel_token_ = std::move(other.cancel_token_);
        }
        return *this;
    }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    bool valid() const noexcept { return fd_ >= 0; }
    int native_handle() const noexcept { return fd_; }

    // Unregisters and closes. Any task parked on this socket wakes with
    // Errc::closed. Idempotent.
    void close();

    Result<SocketAddr> local_addr() const;
    Result<SocketAddr> peer_addr() const;

    // Binds a cancellation signal to this socket.
    //
    // Once the token fires, every operation in either direction fails with
    // Errc::cancelled — including one already parked, which is woken — and
    // every later operation fails immediately, exactly as an elapsed deadline
    // does. This is why read(), write() and accept() take no cancel parameter:
    // like deadlines, cancellation lives on the connection, not on the call.
    //
    // Binding an already-cancelled token cancels the socket at once. Binding a
    // new token replaces the previous binding; a default-constructed token
    // clears it.
    void set_cancel(CancelToken token);
    void clear_cancel() { set_cancel(CancelToken{}); }

    // The absolute deadline currently set on a direction, or a default-
    // constructed TimePoint when none is. Exposed so a scoped deadline can put
    // back exactly what it found.
    TimePoint deadline(bool write_direction) const;

protected:
    // Takes ownership of `fd` and registers it with the reactor.
    //
    // `already_nonblocking` is not an optimisation flag to be guessed at: every
    // socket this library creates comes from socket()/accept4() with
    // SOCK_NONBLOCK already set, and re-deriving that with F_GETFL + F_SETFL
    // costs two syscalls on every single connection. Measured at 0.197 fcntl
    // per request under connection churn, against zero for Go and asio.
    Result<void> adopt(int fd, bool already_nonblocking = false);

    int fd_ = -1;
    detail::IoDesc* desc_ = nullptr;
    // A Socket may escape cio::run(). Keep its stopped scheduler/reactor slab
    // alive through close() and until this handle is destroyed or replaced.
    // An outstanding IoAwaiter independently retains the same lifetime. This
    // is deliberately not stored in IoDesc, which would create Scheduler ->
    // Reactor -> IoDesc -> Scheduler ownership cycles.
    std::shared_ptr<detail::Scheduler> scheduler_lifetime_;
    // Kept alive here and by the CancelState until cancellation fires, so a
    // hook can never reference a destroyed binding.
    std::shared_ptr<detail::CancelHook> cancel_binding_;
    CancelToken cancel_token_;
};

class TcpStream : public Socket {
public:
    TcpStream() = default;

    // Already-resolved connect. With a cancel token, a cancellation resumes the
    // caller with Errc::cancelled; the abandoned socket is closed once its
    // connect settles, so no descriptor is leaked.
    static Task<Result<TcpStream>> connect(SocketAddr addr);
    static Task<Result<TcpStream>> connect(SocketAddr addr, CancelToken cancel);
    // Convenience: resolves through a default Dialer.
    static Task<Result<TcpStream>> connect(std::string host, std::uint16_t port);
    static Task<Result<TcpStream>> connect(std::string host, std::uint16_t port,
                                           CancelToken cancel);

    // Reads whatever is available. 0 means the peer closed cleanly (EOF).
    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    // Writes what it can; may be a partial write, like write(2).
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);
    // Loops until everything is written or an error occurs.
    Task<Result<void>> write_all(std::span<const std::byte> buffer);

    // Non-suspending attempts, for hot paths that want to skip the awaiter
    // entirely when the socket is already ready.
    Result<std::size_t> try_read(std::span<std::byte> buffer);
    Result<std::size_t> try_write(std::span<const std::byte> buffer);
    detail::IoAwaiter readable() noexcept {
        return detail::IoAwaiter{desc_, detail::Dir::kRead};
    }
    detail::IoAwaiter writable() noexcept {
        return detail::IoAwaiter{desc_, detail::Dir::kWrite};
    }

    // Deadlines are per-direction and persist until reset: once one elapses,
    // every operation in that direction fails until it is changed or cleared.
    // The unsuffixed forms apply to both directions at once.
    void set_deadline(TimePoint deadline);
    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void set_timeout(Duration timeout);
    void set_read_timeout(Duration timeout);
    void set_write_timeout(Duration timeout);
    void clear_deadline();
    void clear_read_deadline();
    void clear_write_deadline();

    Result<void> set_nodelay(bool on);
    Result<void> shutdown_write();

private:
    friend class TcpListener;

    // Shared by both connect() overloads. begin_connect() creates, connects and
    // adopts the socket, reporting whether it completed without parking;
    // await_connect() runs the writability/SO_ERROR loop.
    static Result<bool> begin_connect(SocketAddr addr, TcpStream& stream);
    static Task<Result<void>> await_connect(TcpStream& stream);
};

class TcpListener : public Socket {
public:
    TcpListener() = default;

    static Result<TcpListener> bind(SocketAddr addr, int backlog = 1024);
    static Result<TcpListener> bind(std::string_view host, std::uint16_t port,
                                    int backlog = 1024);

    Task<Result<TcpStream>> accept();

    void set_deadline(TimePoint deadline);
    void clear_deadline();
};

struct DialOptions {
    // Covers resolution and every connection attempt. Zero means no overall
    // timeout.
    Duration timeout{};
    // How long one address gets before the next is tried. Zero selects 300ms.
    Duration fallback_delay{};
    bool nodelay = true;
    AddressFamily family = AddressFamily::any;
    // Resolution backend for this dialer, mirroring Go's Dialer.Resolver.
    // See LookupOptions::prefer_builtin.
    bool prefer_builtin_resolver = false;
};

// Name resolution and address selection live here, not on TcpStream.
//
// Addresses are tried with the families interleaved (v6, v4, v6, ...) rather
// than exhausting one family before starting the other, so a host whose IPv6
// route is a blackhole does not have to fail every v6 address first. Attempts
// run one at a time, each bounded by fallback_delay; cio does not yet race
// attempts concurrently the way RFC 8305 does.
class Dialer {
public:
    Dialer() = default;
    explicit Dialer(DialOptions options) noexcept : options_(options) {}

    Task<Result<TcpStream>> dial_tcp(std::string host, std::uint16_t port,
                                     CancelToken cancel = {}) const;

    const DialOptions& options() const noexcept { return options_; }

private:
    DialOptions options_{};
};

// Delegates to a default-constructed Dialer.
Task<Result<TcpStream>> dial_tcp(std::string host, std::uint16_t port,
                                 CancelToken cancel = {});

class UdpSocket : public Socket {
public:
    UdpSocket() = default;

    static Result<UdpSocket> bind(SocketAddr addr);

    Task<Result<std::size_t>> recv_from(std::span<std::byte> buffer, SocketAddr& from);
    Task<Result<std::size_t>> send_to(std::span<const std::byte> buffer,
                                      const SocketAddr& to);

    // Same deadline rules as TcpStream.
    void set_deadline(TimePoint deadline);
    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void set_timeout(Duration timeout);
    void set_read_timeout(Duration timeout);
    void set_write_timeout(Duration timeout);
    void clear_deadline();
    void clear_read_deadline();
    void clear_write_deadline();
};

}  // namespace cio::net
