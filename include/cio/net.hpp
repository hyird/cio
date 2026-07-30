// Async TCP/UDP.
//
//     auto listener = cio::net::TcpListener::listen(cio::net::SocketAddr::any_v4(8080)).value();
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
#include <concepts>
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

    // "1.2.3.4:80" or "[::1]:80", as Go's Addr.String() renders it.
    std::string to_string() const;
    // The address without the port: "1.2.3.4" or "::1". Go exposes TCPAddr.IP.
    std::string ip() const;
    std::uint16_t port() const noexcept;
    int family() const noexcept;
    // "tcp" or "udp" is not knowable from a sockaddr, so Go's Addr.Network()
    // has no counterpart here; the protocol is the type that owns the address.

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

// Splits "host:port" or "[host]:port", as Go's net.SplitHostPort does. The port
// is returned as text because Go does, and because a service name like "http"
// is a legal port field there.
Result<std::pair<std::string, std::string>> split_host_port(
    std::string_view host_port);

// The inverse, bracketing an IPv6 literal, as Go's net.JoinHostPort does.
std::string join_host_port(std::string_view host, std::string_view port);
std::string join_host_port(std::string_view host, std::uint16_t port);

enum class AddressFamily {
    any,
    ipv4,
    ipv6,
};

struct LookupOptions {
    AddressFamily family = AddressFamily::any;
    // Use cio's built-in DNS resolver rather than getaddrinfo(), mirroring
    // Go's Resolver.PreferGo — and defaulting the same way Go does on Unix,
    // for the reason Go gives: a blocked DNS query costs one task, while a
    // blocked C call costs an operating system thread.
    //
    // The built-in resolver speaks DNS over the runtime's own sockets and
    // honours /etc/hosts, so a lookup is cancellable mid-flight and occupies no
    // blocking-pool thread.
    //
    // Set this false on a machine that resolves names through NSS modules the
    // built-in resolver cannot see — LDAP, NIS, mDNS — or whenever answers must
    // agree with `getent hosts`.
    bool prefer_builtin = true;
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
    Result<SocketAddr> remote_addr() const;

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

class TcpConn : public Socket {
public:
    TcpConn() = default;

    // Already-resolved connect. With a cancel token, a cancellation resumes the
    // caller with Errc::cancelled; the abandoned socket is closed once its
    // connect settles, so no descriptor is leaked.
    static Task<Result<TcpConn>> dial(SocketAddr addr);
    static Task<Result<TcpConn>> dial(SocketAddr addr, CancelToken cancel);
    // Convenience: resolves through a default Dialer.
    static Task<Result<TcpConn>> dial(std::string host, std::uint16_t port);
    static Task<Result<TcpConn>> dial(std::string host, std::uint16_t port,
                                           CancelToken cancel);

    // Reads whatever is available. 0 means the peer closed cleanly (EOF).
    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    // Writes the whole span unless an error occurs, per Go's io.Writer
    // contract; the returned count equals buffer.size() on success.
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);

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
    // Go's SetKeepAlive and SetKeepAlivePeriod. The period sets the idle time
    // before the first probe; the count and interval are left to the system.
    Result<void> set_keepalive(bool on);
    Result<void> set_keepalive_period(Duration idle);
    // Go's SetLinger. A negative duration restores the default (close returns
    // immediately and the kernel flushes in the background); zero discards
    // unsent data and sends RST.
    Result<void> set_linger(Duration timeout);
    // Go's SetReadBuffer and SetWriteBuffer.
    Result<void> set_read_buffer(int bytes);
    Result<void> set_write_buffer(int bytes);
    Result<void> close_write();

private:
    friend class TcpListener;

    // Shared by both connect() overloads. begin_connect() creates, connects and
    // adopts the socket, reporting whether it completed without parking;
    // await_connect() runs the writability/SO_ERROR loop.
    static Result<bool> begin_connect(SocketAddr addr, TcpConn& stream);
    static Task<Result<void>> await_connect(TcpConn& stream);
};

class TcpListener : public Socket {
public:
    TcpListener() = default;

    static Result<TcpListener> listen(SocketAddr addr, int backlog = 1024);
    static Result<TcpListener> listen(std::string_view host, std::uint16_t port,
                                    int backlog = 1024);

    Task<Result<TcpConn>> accept();

    // Go's Listener interface spells this Addr(); local_addr() from Socket is
    // the same value and is kept so the socket surface stays uniform.
    Result<SocketAddr> addr() const { return local_addr(); }

    void set_deadline(TimePoint deadline);
    void clear_deadline();
};

// A filesystem or abstract Unix socket address.
//
// Go models this as UnixAddr with a Name and a Net; here the path is the whole
// address and the socket type is the class that owns it. An abstract address —
// Linux's leading NUL, which lives in a namespace rather than the filesystem and
// disappears with the last reference — is written with a leading '@', matching
// the convention every Linux tool uses.
class UnixAddr {
public:
    UnixAddr() = default;

    static Result<UnixAddr> parse(std::string_view path);

    std::string path() const;
    bool abstract() const noexcept { return abstract_; }
    bool valid() const noexcept { return !path_.empty(); }
    std::string to_string() const { return path(); }

private:
    std::string path_;
    bool abstract_ = false;
};

// Go's UnixConn: a stream socket over a Unix domain address.
//
// The same Conn surface as TcpConn, so a generic helper works over either. There
// is no address to report for an unnamed peer, so remote_addr() on an accepted
// connection is usually empty — that is the kernel's behaviour, not an omission.
class UnixConn : public Socket {
public:
    UnixConn() = default;

    static Task<Result<UnixConn>> dial(UnixAddr addr);
    static Task<Result<UnixConn>> dial(UnixAddr addr, CancelToken cancel);

    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    // Full write or error, per Go's io.Writer contract.
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);

    Result<std::size_t> try_read(std::span<std::byte> buffer);
    Result<std::size_t> try_write(std::span<const std::byte> buffer);
    detail::IoAwaiter readable() noexcept {
        return detail::IoAwaiter{desc_, detail::Dir::kRead};
    }
    detail::IoAwaiter writable() noexcept {
        return detail::IoAwaiter{desc_, detail::Dir::kWrite};
    }

    void set_deadline(TimePoint deadline);
    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void set_timeout(Duration timeout);
    void set_read_timeout(Duration timeout);
    void set_write_timeout(Duration timeout);
    void clear_deadline();
    void clear_read_deadline();
    void clear_write_deadline();
    TimePoint deadline(bool write_direction) const {
        return Socket::deadline(write_direction);
    }

    Result<void> close_write();

private:
    friend class UnixListener;
};

// Go's UnixListener.
class UnixListener : public Socket {
public:
    UnixListener() = default;

    // Socket::close() is deliberately non-virtual — a vtable on a socket is not
    // worth one cold path — so destruction alone would never remove the bound
    // path. This destructor is what makes the filesystem entry go away when the
    // listener does, however it is destroyed.
    ~UnixListener() { unlink(); }

    // The moved-from listener must stop owning the path, or its destructor would
    // unlink a socket the new owner is still serving.
    UnixListener(UnixListener&& other) noexcept
        : Socket(std::move(other)),
          bound_(std::move(other.bound_)),
          owns_path_(std::exchange(other.owns_path_, false)) {}
    UnixListener& operator=(UnixListener&& other) noexcept {
        if (this != &other) {
            unlink();
            Socket::operator=(std::move(other));
            bound_ = std::move(other.bound_);
            owns_path_ = std::exchange(other.owns_path_, false);
        }
        return *this;
    }
    UnixListener(const UnixListener&) = delete;
    UnixListener& operator=(const UnixListener&) = delete;

    // Binds and listens. A filesystem socket leaves its path behind on close,
    // exactly as bind(2) does; `unlink_existing` removes a stale path first,
    // which is what a restarting service almost always wants.
    static Result<UnixListener> listen(UnixAddr addr, int backlog = 1024,
                                       bool unlink_existing = true);

    Task<Result<UnixConn>> accept();

    Result<UnixAddr> addr() const;

    void set_deadline(TimePoint deadline);
    void clear_deadline();

    // Removes the filesystem path this listener bound, if any. Called by close()
    // when the listener created the path.
    void unlink();

    void close();

private:
    UnixAddr bound_;
    bool owns_path_ = false;
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
    bool prefer_builtin_resolver = true;
};

// Name resolution and address selection live here, not on TcpConn.
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

    Task<Result<TcpConn>> dial_tcp(std::string host, std::uint16_t port,
                                     CancelToken cancel = {}) const;

    const DialOptions& options() const noexcept { return options_; }

private:
    DialOptions options_{};
};

// Delegates to a default-constructed Dialer.
Task<Result<TcpConn>> dial_tcp(std::string host, std::uint16_t port,
                                 CancelToken cancel = {});

class UdpConn : public Socket {
public:
    UdpConn() = default;

    static Result<UdpConn> listen(SocketAddr addr);

    Task<Result<std::size_t>> read_from(std::span<std::byte> buffer, SocketAddr& from);
    Task<Result<std::size_t>> write_to(std::span<const std::byte> buffer,
                                      const SocketAddr& to);

    // Same deadline rules as TcpConn.
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

// ---------------------------------------------------------------------------
// Go's net package is organised around three interfaces. cio expresses them as
// concepts rather than virtual bases: a protocol library can still be written
// once against "anything that behaves like a connection", but the concrete
// socket keeps its non-virtual fast path.
//
// A TLS stream satisfies Conn as well, exactly as Go's tls.Conn implements
// net.Conn, so a generic helper works over plaintext and TLS unchanged.

// net.Conn
template <typename T>
concept Conn = requires(T& c, std::span<std::byte> in,
                        std::span<const std::byte> out, TimePoint t) {
    { c.read(in) } -> std::same_as<Task<Result<std::size_t>>>;
    { c.write(out) } -> std::same_as<Task<Result<std::size_t>>>;
    c.close();
    { c.local_addr() } -> std::same_as<Result<SocketAddr>>;
    { c.remote_addr() } -> std::same_as<Result<SocketAddr>>;
    c.set_deadline(t);
    c.set_read_deadline(t);
    c.set_write_deadline(t);
};

// net.PacketConn
template <typename T>
concept PacketConn = requires(T& c, std::span<std::byte> in,
                              std::span<const std::byte> out, SocketAddr& from,
                              const SocketAddr& to, TimePoint t) {
    { c.read_from(in, from) } -> std::same_as<Task<Result<std::size_t>>>;
    { c.write_to(out, to) } -> std::same_as<Task<Result<std::size_t>>>;
    c.close();
    { c.local_addr() } -> std::same_as<Result<SocketAddr>>;
    c.set_deadline(t);
    c.set_read_deadline(t);
    c.set_write_deadline(t);
};

// net.Listener
template <typename T>
concept Listener = requires(T& l, TimePoint t) {
    l.accept();
    l.close();
    { l.addr() } -> std::same_as<Result<SocketAddr>>;
    l.set_deadline(t);
};

static_assert(Conn<TcpConn>);
static_assert(Conn<UnixConn>);
static_assert(PacketConn<UdpConn>);
static_assert(Listener<TcpListener>);

}  // namespace cio::net
