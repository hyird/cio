#include "cio/net.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
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

class SwitchWorker {
public:
    SwitchWorker(detail::Scheduler& sched, detail::WorkerId target) noexcept
        : sched_(sched), target_(target) {}

    bool await_ready() const noexcept {
        return sched_.current_worker_id() == target_;
    }
    void await_suspend(std::coroutine_handle<> handle) const noexcept {
        sched_.schedule_to(handle, target_);
    }
    void await_resume() const noexcept {}

private:
    detail::Scheduler& sched_;
    detail::WorkerId target_;
};

class AcceptedFd {
public:
    explicit AcceptedFd(int fd) noexcept : fd_(fd) {}
    ~AcceptedFd() {
        if (fd_ >= 0) ::close(fd_);
    }
    AcceptedFd(const AcceptedFd&) = delete;
    AcceptedFd& operator=(const AcceptedFd&) = delete;

    int get() const noexcept { return fd_; }
    int release() noexcept { return std::exchange(fd_, -1); }

private:
    int fd_;
};

struct IoOperation {
    detail::IoDesc* desc = nullptr;
    std::uint32_t generation = 0;
    Error error{EBADF};

    explicit operator bool() const noexcept { return !error; }
};

template <typename T>
class CooperativeIoTask;

template <typename T>
struct CooperativeIoPromise final
    : detail::TaskPromiseBase {
    struct FinalAwaiter {
        CooperativeIoPromise* promise;

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<>) noexcept {
            void* const continuation =
                promise->continuation_or_completion;
            // CooperativeIoTask is internal and every started instance is
            // immediately awaited by its public wrapper. A never-started task
            // can be destroyed, but a completing one always has a parent.
            assert(continuation != nullptr);

            // co_return destroys the syscall scope before entering
            // final_suspend, so no descriptor lease is held here. Count every
            // completed leaf result: errors terminate the hot chain anyway,
            // and avoiding a success flag removes one store, one load and one
            // branch from every successful operation.
            const std::uint8_t debt =
                detail::cooperative_io_return_debt();
            if (debt !=
                detail::kCooperativeIoDebtNone) {
                // The cold call may publish the parent, which can destroy
                // this completed child immediately. It is therefore the
                // final operation that depends on `promise`.
                return detail::
                    defer_cooperative_io_continuation(
                        std::coroutine_handle<>::
                            from_address(continuation),
                        debt);
            }

            return std::coroutine_handle<>::from_address(
                continuation);
        }

        void await_resume() const noexcept {}
    };

    CooperativeIoTask<T>
    get_return_object() noexcept;
    FinalAwaiter final_suspend() noexcept {
        return FinalAwaiter{this};
    }
    void unhandled_exception() noexcept {
        exception = std::current_exception();
    }

    template <typename U = T>
        requires std::convertible_to<U&&, T>
    void return_value(U&& value_to_store) {
        value.emplace(
            std::forward<U>(value_to_store));
    }

    T&& result() {
        rethrow_if_failed();
        return std::move(*value);
    }

    std::optional<T> value;
};

template <typename T>
class [[nodiscard]] CooperativeIoTask {
public:
    using promise_type = CooperativeIoPromise<T>;
    using handle_type =
        std::coroutine_handle<promise_type>;

    explicit CooperativeIoTask(
        handle_type handle) noexcept
        : handle_(handle) {}
    CooperativeIoTask(
        CooperativeIoTask&& other) noexcept
        : handle_(
              std::exchange(other.handle_, {})) {}
    CooperativeIoTask& operator=(
        CooperativeIoTask&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ =
                std::exchange(other.handle_, {});
        }
        return *this;
    }
    CooperativeIoTask(
        const CooperativeIoTask&) = delete;
    CooperativeIoTask& operator=(
        const CooperativeIoTask&) = delete;
    ~CooperativeIoTask() { destroy(); }

    auto operator co_await() const& noexcept {
        return Awaiter{handle_};
    }
    auto operator co_await() const&& noexcept {
        return Awaiter{handle_};
    }

private:
    struct Awaiter {
        handle_type coroutine;

        bool await_ready() const noexcept {
            return !coroutine || coroutine.done();
        }
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<>
                awaiting) noexcept {
            coroutine.promise().
                continuation_or_completion =
                    awaiting.address();
            return coroutine;
        }
        T&& await_resume() {
            if (!coroutine) {
                throw std::logic_error(
                    "cio: awaited an invalid "
                    "cooperative I/O task");
            }
            return coroutine.promise().result();
        }
    };

    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    handle_type handle_{};
};

template <typename T>
CooperativeIoTask<T>
CooperativeIoPromise<T>::
get_return_object() noexcept {
    return CooperativeIoTask<T>{
        std::coroutine_handle<
            CooperativeIoPromise<T>>::
            from_promise(*this)};
}

static_assert(
    sizeof(CooperativeIoPromise<
               Result<std::size_t>>) ==
    sizeof(detail::TaskPromise<
               Result<std::size_t>>));

// Capture one Socket incarnation without changing its public layout. close()
// updates the two fields atomically while holding this descriptor's lifecycle
// lock, so a task either gets the old incarnation token or observes closure;
// it can never assemble a token from a recycled IoDesc.
IoOperation capture_operation(int& socket_fd,
                              detail::IoDesc*& socket_desc) noexcept {
    std::atomic_ref<detail::IoDesc*> desc_field(socket_desc);
    detail::IoDesc* const desc =
        desc_field.load(std::memory_order_acquire);
    if (desc == nullptr) return {};

    std::atomic_ref<int> fd_field(socket_fd);
    IoOperation operation;
    operation.desc = desc;

    desc->lock_lifecycle();
    if (desc_field.load(std::memory_order_relaxed) != desc ||
        fd_field.load(std::memory_order_relaxed) != desc->fd ||
        desc->closing.load(std::memory_order_acquire)) {
        operation.error = Error{Errc::closed};
    } else if (desc->runtime_stopping()) {
        operation.error = Error{Errc::shutdown};
    } else {
        operation.generation =
            desc->generation.load(std::memory_order_relaxed);
        operation.error = Error{};
    }
    desc->unlock_lifecycle();
    return operation;
}

CooperativeIoTask<Result<std::size_t>>
tcp_read_some(
    detail::IoDesc* desc, std::uint32_t generation,
    std::span<std::byte> buffer) {
    for (;;) {
        // Skip the syscall when the last one proved the queue empty and no edge
        // has arrived since. This is what removes the EAGAIN read that
        // edge-triggered polling would otherwise cost on every message.
        if (desc->may_be_ready(detail::Dir::kRead)) {
            detail::FdUseGuard fd_use{
                desc, detail::Dir::kRead, generation};
            if (!fd_use) co_return fd_use.error();

            const ssize_t n =
                ::recv(fd_use.fd(), buffer.data(), buffer.size(), 0);
            const int syscall_error = n < 0 ? errno : 0;
            if (n >= 0) {
                // Short read: the receive queue is empty, so the next recv
                // would EAGAIN until a new edge arrives.
                if (static_cast<std::size_t>(n) < buffer.size()) {
                    desc->note_would_block(detail::Dir::kRead);
                }
                co_return static_cast<std::size_t>(n);
            }
            if (syscall_error == EINTR) continue;
            if (syscall_error != EAGAIN &&
                syscall_error != EWOULDBLOCK) {
                co_return Error{syscall_error};
            }
            desc->note_would_block(detail::Dir::kRead);
        }

        if (auto ready = co_await detail::IoAwaiter{
                desc, detail::Dir::kRead, generation};
            !ready) {
            co_return ready.error();
        }
    }
}

CooperativeIoTask<Result<std::size_t>>
tcp_write_some(
    detail::IoDesc* desc, std::uint32_t generation,
    std::span<const std::byte> buffer) {
    for (;;) {
        if (desc->may_be_ready(detail::Dir::kWrite)) {
            detail::FdUseGuard fd_use{
                desc, detail::Dir::kWrite, generation};
            if (!fd_use) co_return fd_use.error();

            const ssize_t n =
                ::send(fd_use.fd(), buffer.data(), buffer.size(),
                       MSG_NOSIGNAL);
            const int syscall_error = n < 0 ? errno : 0;
            if (n >= 0) {
                // A partial write means the send buffer filled up.
                if (static_cast<std::size_t>(n) < buffer.size()) {
                    desc->note_would_block(detail::Dir::kWrite);
                }
                co_return static_cast<std::size_t>(n);
            }
            if (syscall_error == EINTR) continue;
            if (syscall_error != EAGAIN &&
                syscall_error != EWOULDBLOCK) {
                co_return Error{syscall_error};
            }
            desc->note_would_block(detail::Dir::kWrite);
        }

        if (auto ready = co_await detail::IoAwaiter{
                desc, detail::Dir::kWrite, generation};
            !ready) {
            co_return ready.error();
        }
    }
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

Result<void> Socket::adopt(int fd, bool already_nonblocking) {
    if (!already_nonblocking) {
        if (auto r = make_nonblocking(fd); !r) {
            ::close(fd);
            return r;
        }
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
    // Publish the lifetime before the raw descriptor becomes observable.
    // Runtime-managed schedulers always have a shared handle; a detail-level
    // stack Scheduler remains supported for white-box tests whose own scope
    // already bounds every descriptor.
    scheduler_lifetime_ = sched->shared_handle();
    fd_ = fd;
    desc_ = *desc;
    return ok();
}

void Socket::close() {
    std::atomic_ref<detail::IoDesc*> desc_field(desc_);
    detail::IoDesc* const desc =
        desc_field.load(std::memory_order_acquire);

    if (desc == nullptr) {
        // The normal invalid/moved-from case. A descriptor-owning close clears
        // fd_ before desc_, so a concurrent second close also sees -1 here.
        std::atomic_ref<int> fd_field(fd_);
        const int fd =
            fd_field.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) ::close(fd);
        return;
    }

    int fd = -1;
    bool owns_close = false;
    desc->lock_lifecycle();
    if (desc_field.load(std::memory_order_relaxed) == desc) {
        std::atomic_ref<int> fd_field(fd_);
        fd = fd_field.exchange(-1, std::memory_order_acq_rel);
        desc_field.store(nullptr, std::memory_order_release);
        desc->closing.store(true, std::memory_order_release);
        owns_close = true;
    }
    desc->unlock_lifecycle();

    if (!owns_close) return;

    // Order matters: detach unregisters, wakes parked tasks, and waits for all
    // active syscall leases before this fd number can be reused.
    reactor_for(desc).detach(desc);
    if (fd >= 0) ::close(fd);
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

// The try_* forms always attempt the syscall — a caller asking to "try" wants
// an attempt — but they keep the readiness hint coherent so that mixing them
// with the awaiting forms does not confuse it.
Result<std::size_t> TcpStream::try_read(std::span<std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return operation.error;

    detail::FdUseGuard fd_use{
        operation.desc, detail::Dir::kRead, operation.generation};
    if (!fd_use) return fd_use.error();

    const ssize_t n =
        ::recv(fd_use.fd(), buffer.data(), buffer.size(), 0);
    const int syscall_error = n < 0 ? errno : 0;
    if (n >= 0) {
        if (static_cast<std::size_t>(n) < buffer.size()) {
            operation.desc->note_would_block(detail::Dir::kRead);
        }
        return static_cast<std::size_t>(n);
    }
    if (syscall_error == EAGAIN || syscall_error == EWOULDBLOCK) {
        operation.desc->note_would_block(detail::Dir::kRead);
    }
    return Error{syscall_error};
}

Result<std::size_t> TcpStream::try_write(std::span<const std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return operation.error;

    detail::FdUseGuard fd_use{
        operation.desc, detail::Dir::kWrite, operation.generation};
    if (!fd_use) return fd_use.error();

    // MSG_NOSIGNAL: a write to a closed peer must be an EPIPE return, not a
    // process-wide SIGPIPE.
    const ssize_t n =
        ::send(fd_use.fd(), buffer.data(), buffer.size(), MSG_NOSIGNAL);
    const int syscall_error = n < 0 ? errno : 0;
    if (n >= 0) {
        if (static_cast<std::size_t>(n) < buffer.size()) {
            operation.desc->note_would_block(detail::Dir::kWrite);
        }
        return static_cast<std::size_t>(n);
    }
    if (syscall_error == EAGAIN || syscall_error == EWOULDBLOCK) {
        operation.desc->note_would_block(detail::Dir::kWrite);
    }
    return Error{syscall_error};
}

Task<Result<std::size_t>> TcpStream::read(std::span<std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    co_return co_await tcp_read_some(
        operation.desc, operation.generation, buffer);
}

Task<Result<std::size_t>> TcpStream::write(std::span<const std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    co_return co_await tcp_write_some(
        operation.desc, operation.generation, buffer);
}

Task<Result<void>> TcpStream::write_all(std::span<const std::byte> buffer) {
    // One immutable descriptor incarnation for the whole logical operation:
    // close() may update the Socket fields while this coroutine is parked, so
    // a partial write must not re-read them on its next chunk.
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;

    while (!buffer.empty()) {
        auto written = co_await tcp_write_some(
            operation.desc, operation.generation, buffer);
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

    // Start the state machine before registering EPOLLOUT. A never-connected
    // TCP fd is itself reported writable by epoll; registering it first can
    // leave a stale kIoReady that makes an ensuing EINPROGRESS look complete
    // while SO_ERROR is still zero.
    AcceptedFd pending(fd);
    const int connect_result =
        ::connect(pending.get(), addr.raw(), addr.length());
    const int connect_error = connect_result == 0 ? 0 : errno;
    if (connect_result != 0 && connect_error != EINPROGRESS) {
        co_return Error{connect_error};
    }

    TcpStream stream;
    if (auto adopted =
            stream.adopt(pending.release(), /*already_nonblocking=*/true);
        !adopted) {
        co_return adopted.error();
    }

    if (connect_result == 0) co_return std::move(stream);

    const std::uint32_t generation =
        stream.desc_->generation.load(std::memory_order_acquire);

    // A non-blocking connect reports completion as writability; the actual
    // outcome has to be read back out of SO_ERROR. Readiness is only a hint:
    // SO_ERROR may transiently be zero while the connection is still
    // EINPROGRESS, so getpeername confirms that a peer was actually installed.
    for (;;) {
        if (auto ready = co_await detail::IoAwaiter{
                stream.desc_, detail::Dir::kWrite, generation};
            !ready) {
            co_return ready.error();
        }

        int error = 0;
        socklen_t error_length = sizeof(error);
        if (::getsockopt(stream.fd_, SOL_SOCKET, SO_ERROR, &error,
                         &error_length) != 0) {
            co_return Error::from_errno();
        }
        if (error != 0) co_return Error{error};

        sockaddr_storage peer{};
        socklen_t peer_length = sizeof(peer);
        if (::getpeername(stream.fd_,
                          reinterpret_cast<sockaddr*>(&peer),
                          &peer_length) == 0) {
            co_return std::move(stream);
        }

        const int peer_error = errno;
        if (peer_error != ENOTCONN) co_return Error{peer_error};

        // The edge was spurious or predated actual completion. The slot was
        // consumed by the awaiter above; park again for the transition that
        // installs either a peer or a concrete SO_ERROR.
        stream.desc_->note_would_block(detail::Dir::kWrite);
    }
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
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpStream::set_write_deadline(TimePoint deadline) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kWrite, operation.generation,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpStream::set_read_timeout(Duration timeout) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation,
        deadline_from_now(timeout));
}

void TcpStream::set_write_timeout(Duration timeout) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kWrite, operation.generation,
        deadline_from_now(timeout));
}

void TcpStream::clear_read_deadline() {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation, 0);
}

void TcpStream::clear_write_deadline() {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kWrite, operation.generation, 0);
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
    if (auto adopted = listener.adopt(fd, /*already_nonblocking=*/true); !adopted) {
        return adopted.error();
    }
    return listener;
}

Result<TcpListener> TcpListener::bind(std::string_view host, std::uint16_t port, int backlog) {
    auto addr = SocketAddr::parse(host, port);
    if (!addr) return addr.error();
    return bind(*addr, backlog);
}

Task<Result<TcpStream>> TcpListener::accept() {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;

    for (;;) {
        if (operation.desc->may_be_ready(detail::Dir::kRead)) {
            int accepted_fd = -1;
            {
                detail::FdUseGuard fd_use{
                    operation.desc, detail::Dir::kRead,
                    operation.generation};
                if (!fd_use) co_return fd_use.error();

                accepted_fd =
                    ::accept4(fd_use.fd(), nullptr, nullptr,
                              SOCK_CLOEXEC | SOCK_NONBLOCK);
                const int syscall_error =
                    accepted_fd < 0 ? errno : 0;
                if (accepted_fd < 0) {
                    if (syscall_error == EINTR ||
                        syscall_error == ECONNABORTED) {
                        continue;
                    }
                    if (syscall_error != EAGAIN &&
                        syscall_error != EWOULDBLOCK) {
                        co_return Error{syscall_error};
                    }
                    operation.desc->note_would_block(
                        detail::Dir::kRead);
                }
            }

            if (accepted_fd >= 0) {
                // A success says nothing about whether more are queued, so the
                // hint stays set and the next accept tries again.
                //
                // Route the accepted fd before registering it. Symmetric
                // transfer at this Task's final suspend means the caller of
                // accept() also continues on the selected worker, so the
                // conventional immediate go(serve(stream)) remains local.
                AcceptedFd accepted(accepted_fd);
                co_await detail::CooperativeIoCheckpoint{};
                detail::Scheduler* const sched = detail::current_scheduler();
                if (sched == nullptr) co_return Error{Errc::shutdown};
                const detail::WorkerId target = sched->choose_worker();
                co_await SwitchWorker{*sched, target};

                TcpStream stream;
                if (auto adopted =
                        stream.adopt(accepted.release(),
                                     /*already_nonblocking=*/true);
                    !adopted) {
                    co_return adopted.error();
                }
                co_return std::move(stream);
            }
        }

        if (auto ready =
                co_await detail::IoAwaiter{
                    operation.desc, detail::Dir::kRead,
                    operation.generation};
            !ready) {
            co_return ready.error();
        }
    }
}

void TcpListener::set_deadline(TimePoint deadline) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void TcpListener::clear_deadline() {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation, 0);
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
    if (auto adopted = socket.adopt(fd, /*already_nonblocking=*/true); !adopted) {
        return adopted.error();
    }
    return socket;
}

Task<Result<std::size_t>> UdpSocket::recv_from(std::span<std::byte> buffer, SocketAddr& from) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;

    for (;;) {
        sockaddr_storage storage{};
        socklen_t length = sizeof(storage);
        ssize_t n = -1;
        int syscall_error = 0;
        {
            detail::FdUseGuard fd_use{
                operation.desc, detail::Dir::kRead,
                operation.generation};
            if (!fd_use) co_return fd_use.error();

            n = ::recvfrom(
                fd_use.fd(), buffer.data(), buffer.size(), 0,
                reinterpret_cast<sockaddr*>(&storage), &length);
            syscall_error = n < 0 ? errno : 0;
            if (n < 0 &&
                (syscall_error == EAGAIN ||
                 syscall_error == EWOULDBLOCK)) {
                // Unlike a stream, a short datagram says nothing about whether
                // more are queued, so only EAGAIN can clear the hint here.
                operation.desc->note_would_block(
                    detail::Dir::kRead);
            }
        }

        if (n >= 0) {
            from = SocketAddr::from_raw(&storage, length);
            co_await detail::CooperativeIoCheckpoint{};
            co_return static_cast<std::size_t>(n);
        }
        if (syscall_error == EINTR) continue;
        if (syscall_error != EAGAIN &&
            syscall_error != EWOULDBLOCK) {
            co_return Error{syscall_error};
        }

        if (auto ready =
                co_await detail::IoAwaiter{
                    operation.desc, detail::Dir::kRead,
                    operation.generation};
            !ready) {
            co_return ready.error();
        }
    }
}

Task<Result<std::size_t>> UdpSocket::send_to(std::span<const std::byte> buffer,
                                             const SocketAddr& to) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;

    for (;;) {
        ssize_t n = -1;
        int syscall_error = 0;
        {
            detail::FdUseGuard fd_use{
                operation.desc, detail::Dir::kWrite,
                operation.generation};
            if (!fd_use) co_return fd_use.error();

            n = ::sendto(
                fd_use.fd(), buffer.data(), buffer.size(),
                MSG_NOSIGNAL, to.raw(), to.length());
            syscall_error = n < 0 ? errno : 0;
            if (n < 0 &&
                (syscall_error == EAGAIN ||
                 syscall_error == EWOULDBLOCK)) {
                operation.desc->note_would_block(
                    detail::Dir::kWrite);
            }
        }

        if (n >= 0) {
            co_await detail::CooperativeIoCheckpoint{};
            co_return static_cast<std::size_t>(n);
        }
        if (syscall_error == EINTR) continue;
        if (syscall_error != EAGAIN &&
            syscall_error != EWOULDBLOCK) {
            co_return Error{syscall_error};
        }

        if (auto ready =
                co_await detail::IoAwaiter{
                    operation.desc, detail::Dir::kWrite,
                    operation.generation};
            !ready) {
            co_return ready.error();
        }
    }
}

void UdpSocket::set_read_deadline(TimePoint deadline) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kRead, operation.generation,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

void UdpSocket::set_write_deadline(TimePoint deadline) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return;
    reactor_for(operation.desc).set_deadline(
        operation.desc, detail::Dir::kWrite, operation.generation,
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch())
            .count());
}

}  // namespace cio::net
