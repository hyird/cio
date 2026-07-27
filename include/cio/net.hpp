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

// Name resolution runs on the blocking pool: getaddrinfo has no async form.
Task<Result<std::vector<SocketAddr>>> resolve(std::string host, std::uint16_t port);

// Base for the socket types: owns the fd and its reactor registration.
class Socket {
public:
    Socket() = default;
    ~Socket() { close(); }

    Socket(Socket&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)),
          desc_(std::exchange(other.desc_, nullptr)),
          scheduler_lifetime_(std::move(other.scheduler_lifetime_)) {}
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = std::exchange(other.fd_, -1);
            desc_ = std::exchange(other.desc_, nullptr);
            scheduler_lifetime_ =
                std::move(other.scheduler_lifetime_);
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
};

class TcpStream : public Socket {
public:
    TcpStream() = default;

    static Task<Result<TcpStream>> connect(SocketAddr addr);
    static Task<Result<TcpStream>> connect(std::string host, std::uint16_t port);

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

    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void set_read_timeout(Duration timeout);
    void set_write_timeout(Duration timeout);
    void clear_read_deadline();
    void clear_write_deadline();

    Result<void> set_nodelay(bool on);
    Result<void> shutdown_write();

private:
    friend class TcpListener;
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

class UdpSocket : public Socket {
public:
    UdpSocket() = default;

    static Result<UdpSocket> bind(SocketAddr addr);

    Task<Result<std::size_t>> recv_from(std::span<std::byte> buffer, SocketAddr& from);
    Task<Result<std::size_t>> send_to(std::span<const std::byte> buffer,
                                      const SocketAddr& to);

    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
};

}  // namespace cio::net
