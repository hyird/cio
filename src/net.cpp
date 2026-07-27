#include "cio/net.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "cio/blocking.hpp"
#include "cio/detail/scheduler.hpp"

namespace cio::net {
namespace {

const sockaddr_storage* as_storage(const unsigned char* bytes) {
    return reinterpret_cast<const sockaddr_storage*>(bytes);
}

Result<void> make_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return Error::from_errno();
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return Error::from_errno();
    return ok();
}

detail::Reactor& reactor_for(detail::IoDesc* desc) {
    return *desc->owner;
}

}  // namespace

// ------------------------------------------------------------ SocketAddr ---

SocketAddr SocketAddr::from_raw(const void* addr, unsigned len) {
    SocketAddr out;
    if (len > sizeof(out.storage_)) len = sizeof(out.storage_);
    std::memcpy(out.storage_, addr, len);
    out.length_ = len;
    return out;
}

Result<SocketAddr> SocketAddr::parse(std::string_view host, std::uint16_t port) {
    const std::string host_str(host);
    SocketAddr out;

    sockaddr_in v4{};
    if (::inet_pton(AF_INET, host_str.c_str(), &v4.sin_addr) == 1) {
        v4.sin_family = AF_INET;
        v4.sin_port = ::htons(port);
        std::memcpy(out.storage_, &v4, sizeof(v4));
        out.length_ = sizeof(v4);
        return out;
    }

    sockaddr_in6 v6{};
    if (::inet_pton(AF_INET6, host_str.c_str(), &v6.sin6_addr) == 1) {
        v6.sin6_family = AF_INET6;
        v6.sin6_port = ::htons(port);
        std::memcpy(out.storage_, &v6, sizeof(v6));
        out.length_ = sizeof(v6);
        return out;
    }

    return Error{EINVAL};
}

SocketAddr SocketAddr::any_v4(std::uint16_t port) {
    sockaddr_in v4{};
    v4.sin_family = AF_INET;
    v4.sin_port = ::htons(port);
    v4.sin_addr.s_addr = ::htonl(INADDR_ANY);
    return from_raw(&v4, sizeof(v4));
}

SocketAddr SocketAddr::loopback_v4(std::uint16_t port) {
    sockaddr_in v4{};
    v4.sin_family = AF_INET;
    v4.sin_port = ::htons(port);
    v4.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    return from_raw(&v4, sizeof(v4));
}

SocketAddr SocketAddr::any_v6(std::uint16_t port) {
    sockaddr_in6 v6{};
    v6.sin6_family = AF_INET6;
    v6.sin6_port = ::htons(port);
    v6.sin6_addr = in6addr_any;
    return from_raw(&v6, sizeof(v6));
}

SocketAddr SocketAddr::loopback_v6(std::uint16_t port) {
    sockaddr_in6 v6{};
    v6.sin6_family = AF_INET6;
    v6.sin6_port = ::htons(port);
    v6.sin6_addr = in6addr_loopback;
    return from_raw(&v6, sizeof(v6));
}

const sockaddr* SocketAddr::raw() const noexcept {
    return reinterpret_cast<const sockaddr*>(storage_);
}

int SocketAddr::family() const noexcept {
    return length_ == 0 ? AF_UNSPEC : as_storage(storage_)->ss_family;
}

std::uint16_t SocketAddr::port() const noexcept {
    switch (family()) {
        case AF_INET:
            return ::ntohs(reinterpret_cast<const sockaddr_in*>(storage_)->sin_port);
        case AF_INET6:
            return ::ntohs(reinterpret_cast<const sockaddr_in6*>(storage_)->sin6_port);
        default:
            return 0;
    }
}

std::string SocketAddr::to_string() const {
    char buffer[INET6_ADDRSTRLEN] = {};
    switch (family()) {
        case AF_INET: {
            const auto* v4 = reinterpret_cast<const sockaddr_in*>(storage_);
            ::inet_ntop(AF_INET, &v4->sin_addr, buffer, sizeof(buffer));
            return std::string(buffer) + ":" + std::to_string(port());
        }
        case AF_INET6: {
            const auto* v6 = reinterpret_cast<const sockaddr_in6*>(storage_);
            ::inet_ntop(AF_INET6, &v6->sin6_addr, buffer, sizeof(buffer));
            return "[" + std::string(buffer) + "]:" + std::to_string(port());
        }
        default:
            return "<unspecified>";
    }
}

Task<Result<std::vector<SocketAddr>>> resolve(std::string host, std::uint16_t port) {
    // getaddrinfo has no non-blocking form worth using, so it goes to the pool.
    // This is the canonical example of why the pool exists.
    auto result = co_await blocking(
        [host = std::move(host), port]() -> Result<std::vector<SocketAddr>> {
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;

            const std::string service = std::to_string(port);
            addrinfo* head = nullptr;
            const int rc = ::getaddrinfo(host.c_str(), service.c_str(), &hints, &head);
            if (rc != 0) return Error{rc == EAI_SYSTEM ? errno : EINVAL};

            std::vector<SocketAddr> addresses;
            for (addrinfo* it = head; it != nullptr; it = it->ai_next) {
                addresses.push_back(
                    SocketAddr::from_raw(it->ai_addr, static_cast<unsigned>(it->ai_addrlen)));
            }
            ::freeaddrinfo(head);
            if (addresses.empty()) return Error{ENOENT};
            return addresses;
        });
    co_return result;
}

// ---------------------------------------------------------------- Socket ---

Result<void> Socket::adopt(int fd) {
    if (auto r = make_nonblocking(fd); !r) {
        ::close(fd);
        return r;
    }
    detail::Scheduler* sched = detail::current_scheduler();
    if (sched == nullptr) {
        ::close(fd);
        return Error{Errc::shutdown};
    }
    auto desc = sched->reactor().attach(fd);
    if (!desc) {
        ::close(fd);
        return desc.error();
    }
    fd_ = fd;
    desc_ = *desc;
    return ok();
}

void Socket::close() {
    if (desc_ != nullptr) {
        // Order matters: detach unregisters and wakes parked tasks with
        // Errc::closed *before* the fd number can be reused by another socket.
        reactor_for(desc_).detach(desc_);
        desc_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

Result<SocketAddr> Socket::local_addr() const {
    if (fd_ < 0) return Error{EBADF};
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
        return Error::from_errno();
    }
    return SocketAddr::from_raw(&storage, length);
}

Result<SocketAddr> Socket::peer_addr() const {
    if (fd_ < 0) return Error{EBADF};
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
        return Error::from_errno();
    }
    return SocketAddr::from_raw(&storage, length);
}

// ------------------------------------------------------------- TcpStream ---

Result<std::size_t> TcpStream::try_read(std::span<std::byte> buffer) {
    const ssize_t n = ::recv(fd_, buffer.data(), buffer.size(), 0);
    if (n >= 0) return static_cast<std::size_t>(n);
    return Error::from_errno();
}

Result<std::size_t> TcpStream::try_write(std::span<const std::byte> buffer) {
    // MSG_NOSIGNAL: a write to a closed peer must be an EPIPE return, not a
    // process-wide SIGPIPE.
    const ssize_t n = ::send(fd_, buffer.data(), buffer.size(), MSG_NOSIGNAL);
    if (n >= 0) return static_cast<std::size_t>(n);
    return Error::from_errno();
}

Task<Result<std::size_t>> TcpStream::read(std::span<std::byte> buffer) {
    for (;;) {
        // Try first: on a busy connection the data is already there and this
        // completes without ever suspending.
        const ssize_t n = ::recv(fd_, buffer.data(), buffer.size(), 0);
        if (n >= 0) co_return static_cast<std::size_t>(n);
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) co_return Error::from_errno();

        if (auto ready = co_await detail::IoAwaiter{desc_, detail::Dir::kRead}; !ready) {
            co_return ready.error();
        }
    }
}

Task<Result<std::size_t>> TcpStream::write(std::span<const std::byte> buffer) {
    for (;;) {
        const ssize_t n = ::send(fd_, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        if (n >= 0) co_return static_cast<std::size_t>(n);
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) co_return Error::from_errno();

        if (auto ready = co_await detail::IoAwaiter{desc_, detail::Dir::kWrite}; !ready) {
            co_return ready.error();
        }
    }
}

Task<Result<void>> TcpStream::write_all(std::span<const std::byte> buffer) {
    while (!buffer.empty()) {
        auto written = co_await write(buffer);
        if (!written) co_return written.error();
        if (*written == 0) co_return Error{Errc::closed};
        buffer = buffer.subspan(*written);
    }
    co_return ok();
}

Task<Result<TcpStream>> TcpStream::connect(SocketAddr addr) {
    if (!addr.valid()) co_return Error{EINVAL};

    const int fd = ::socket(addr.family(), SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) co_return Error::from_errno();

    TcpStream stream;
    if (auto adopted = stream.adopt(fd); !adopted) co_return adopted.error();

    if (::connect(stream.fd_, addr.raw(), addr.length()) == 0) co_return std::move(stream);
    if (errno != EINPROGRESS) co_return Error::from_errno();

    // A non-blocking connect reports completion as writability; the actual
    // outcome then has to be read back out of SO_ERROR.
    if (auto ready = co_await detail::IoAwaiter{stream.desc_, detail::Dir::kWrite}; !ready) {
        co_return ready.error();
    }

    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(stream.fd_, SOL_SOCKET, SO_ERROR, &error, &length) != 0) {
        co_return Error::from_errno();
    }
    if (error != 0) co_return Error{error};

    co_return std::move(stream);
}

Task<Result<TcpStream>> TcpStream::connect(std::string host, std::uint16_t port) {
    if (auto literal = SocketAddr::parse(host, port); literal) {
        co_return co_await connect(*literal);
    }
    auto addresses = co_await resolve(std::move(host), port);
    if (!addresses) co_return addresses.error();

    Result<TcpStream> last = Error{EHOSTUNREACH};
    for (const auto& addr : *addresses) {
        last = co_await connect(addr);
        if (last) co_return last;
    }
    co_return last;
}

void TcpStream::set_read_deadline(TimePoint deadline) {
    reactor_for(desc_).set_deadline(
        desc_, detail::Dir::kRead,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpStream::set_write_deadline(TimePoint deadline) {
    reactor_for(desc_).set_deadline(
        desc_, detail::Dir::kWrite,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpStream::set_read_timeout(Duration timeout) {
    reactor_for(desc_).set_deadline(desc_, detail::Dir::kRead, deadline_from_now(timeout));
}

void TcpStream::set_write_timeout(Duration timeout) {
    reactor_for(desc_).set_deadline(desc_, detail::Dir::kWrite, deadline_from_now(timeout));
}

void TcpStream::clear_read_deadline() {
    reactor_for(desc_).set_deadline(desc_, detail::Dir::kRead, 0);
}

void TcpStream::clear_write_deadline() {
    reactor_for(desc_).set_deadline(desc_, detail::Dir::kWrite, 0);
}

Result<void> TcpStream::set_nodelay(bool on) {
    const int value = on ? 1 : 0;
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpStream::shutdown_write() {
    if (::shutdown(fd_, SHUT_WR) != 0) return Error::from_errno();
    return ok();
}

// ----------------------------------------------------------- TcpListener ---

Result<TcpListener> TcpListener::bind(SocketAddr addr, int backlog) {
    if (!addr.valid()) return Error{EINVAL};

    const int fd = ::socket(addr.family(), SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return Error::from_errno();

    const int reuse = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    if (::bind(fd, addr.raw(), addr.length()) != 0) {
        const Error err = Error::from_errno();
        ::close(fd);
        return err;
    }
    if (::listen(fd, backlog) != 0) {
        const Error err = Error::from_errno();
        ::close(fd);
        return err;
    }

    TcpListener listener;
    if (auto adopted = listener.adopt(fd); !adopted) return adopted.error();
    return listener;
}

Result<TcpListener> TcpListener::bind(std::string_view host, std::uint16_t port, int backlog) {
    auto addr = SocketAddr::parse(host, port);
    if (!addr) return addr.error();
    return bind(*addr, backlog);
}

Task<Result<TcpStream>> TcpListener::accept() {
    for (;;) {
        const int fd = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (fd >= 0) {
            TcpStream stream;
            if (auto adopted = stream.adopt(fd); !adopted) co_return adopted.error();
            co_return std::move(stream);
        }
        if (errno == EINTR || errno == ECONNABORTED) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) co_return Error::from_errno();

        if (auto ready = co_await detail::IoAwaiter{desc_, detail::Dir::kRead}; !ready) {
            co_return ready.error();
        }
    }
}

void TcpListener::set_deadline(TimePoint deadline) {
    reactor_for(desc_).set_deadline(
        desc_, detail::Dir::kRead,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpListener::clear_deadline() {
    reactor_for(desc_).set_deadline(desc_, detail::Dir::kRead, 0);
}

// ------------------------------------------------------------- UdpSocket ---

Result<UdpSocket> UdpSocket::bind(SocketAddr addr) {
    if (!addr.valid()) return Error{EINVAL};

    const int fd = ::socket(addr.family(), SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return Error::from_errno();

    if (::bind(fd, addr.raw(), addr.length()) != 0) {
        const Error err = Error::from_errno();
        ::close(fd);
        return err;
    }

    UdpSocket socket;
    if (auto adopted = socket.adopt(fd); !adopted) return adopted.error();
    return socket;
}

Task<Result<std::size_t>> UdpSocket::recv_from(std::span<std::byte> buffer, SocketAddr& from) {
    for (;;) {
        sockaddr_storage storage{};
        socklen_t length = sizeof(storage);
        const ssize_t n = ::recvfrom(fd_, buffer.data(), buffer.size(), 0,
                                     reinterpret_cast<sockaddr*>(&storage), &length);
        if (n >= 0) {
            from = SocketAddr::from_raw(&storage, length);
            co_return static_cast<std::size_t>(n);
        }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) co_return Error::from_errno();

        if (auto ready = co_await detail::IoAwaiter{desc_, detail::Dir::kRead}; !ready) {
            co_return ready.error();
        }
    }
}

Task<Result<std::size_t>> UdpSocket::send_to(std::span<const std::byte> buffer,
                                             const SocketAddr& to) {
    for (;;) {
        const ssize_t n = ::sendto(fd_, buffer.data(), buffer.size(), MSG_NOSIGNAL, to.raw(),
                                   to.length());
        if (n >= 0) co_return static_cast<std::size_t>(n);
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) co_return Error::from_errno();

        if (auto ready = co_await detail::IoAwaiter{desc_, detail::Dir::kWrite}; !ready) {
            co_return ready.error();
        }
    }
}

void UdpSocket::set_read_deadline(TimePoint deadline) {
    reactor_for(desc_).set_deadline(
        desc_, detail::Dir::kRead,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void UdpSocket::set_write_deadline(TimePoint deadline) {
    reactor_for(desc_).set_deadline(
        desc_, detail::Dir::kWrite,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

}  // namespace cio::net
