#include "cio/net.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <optional>
#include <vector>
#include <type_traits>

#include "cio/blocking.hpp"
#include "cio/dns.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/select.hpp"
#include "cio/spawn.hpp"

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

// Applies one absolute deadline to the selected directions. Zero clears.
//
// Both directions share a single captured incarnation rather than capturing
// twice: set_deadline() revalidates against the captured generation either way,
// so one token cannot outlive the descriptor it names, and a combined
// set_deadline() cannot land its two directions on different incarnations.
// An invalid or closing descriptor is a no-op, matching the per-direction
// setters, which have always been safe to call on a closed socket.
void apply_deadline(int& socket_fd, detail::IoDesc*& socket_desc,
                    bool read, bool write,
                    std::int64_t deadline_ns) noexcept {
    const IoOperation operation = capture_operation(socket_fd, socket_desc);
    if (!operation) return;
    detail::Reactor& reactor = reactor_for(operation.desc);
    if (read) {
        reactor.set_deadline(operation.desc, detail::Dir::kRead,
                             operation.generation, deadline_ns);
    }
    if (write) {
        reactor.set_deadline(operation.desc, detail::Dir::kWrite,
                             operation.generation, deadline_ns);
    }
}

std::int64_t absolute_ns(TimePoint deadline) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               deadline.time_since_epoch())
        .count();
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

namespace {

int af_of(AddressFamily family) noexcept {
    switch (family) {
        case AddressFamily::ipv4: return AF_INET;
        case AddressFamily::ipv6: return AF_INET6;
        case AddressFamily::any: break;
    }
    return AF_UNSPEC;
}

// The synchronous body. Owns its inputs so it can outlive a cancelled caller.
Result<std::vector<SocketAddr>> lookup_host_blocking(const std::string& host,
                                                     std::uint16_t port,
                                                     int family) {
    addrinfo hints{};
    hints.ai_family = family;
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
}

Result<std::vector<std::string>> lookup_addr_blocking(const SocketAddr& address) {
    char host[NI_MAXHOST];
    const int rc = ::getnameinfo(address.raw(), address.length(), host,
                                 sizeof(host), nullptr, 0, NI_NAMEREQD);
    if (rc != 0) return Error{rc == EAI_SYSTEM ? errno : ENOENT};
    return std::vector<std::string>{std::string(host)};
}

// A self-owned lookup job.
//
// Deliberately not a coroutine. A cancelled caller must be able to walk away
// while getaddrinfo() finishes, and a detached coroutine that outlives the
// scheduler workers can never be resumed to completion: Scheduler::shutdown()
// joins the workers before it stops the blocking pool, so a completion
// dispatched afterwards has nothing to run it and its frame leaks. Owning the
// job on the heap and delivering through a non-suspending try_send() removes
// the frame, and with it the leak, on every path.
template <typename Work, typename Value>
struct DetachedLookup final : detail::BlockingJob {
    Work work;
    Chan<Value> out;

    DetachedLookup(Work w, Chan<Value> channel)
        : work(std::move(w)), out(std::move(channel)) {}

    static void deliver(detail::BlockingJob* base, Value value) noexcept {
        auto* self = static_cast<DetachedLookup*>(base);
        // The channel is buffered to one and written once, so this never fails
        // for lack of room. A caller that already gave up simply never reads
        // it, and the value dies with the last channel reference.
        (void)self->out.try_send(std::move(value));
        delete self;
    }

    static void run_job(detail::BlockingJob* base) noexcept {
        auto* self = static_cast<DetachedLookup*>(base);
        Value value{Error{Errc::broken}};
        try {
            value = self->work();
        } catch (...) {
            value = Value{Error{Errc::broken}};
        }
        deliver(base, std::move(value));
    }

    // Still waiting for an admission slot when the pool stopped.
    static void fail_job(detail::BlockingJob* base) noexcept {
        deliver(base, Value{Error{Errc::shutdown}});
    }
};

// Runs `work` on the blocking pool so that a cancellation can resume the caller
// without waiting for the syscall.
//
// Without a token the ordinary awaiter is used, so a plain lookup pays for
// neither a heap job nor a channel.
template <typename Work>
Task<std::invoke_result_t<Work&>> cancellable_blocking(Work work,
                                                       CancelToken cancel) {
    // Work already returns a Result<T>; do not wrap it again.
    using Value = std::invoke_result_t<Work&>;

    if (!cancel) {
        co_return co_await detail::blocking_in_class(
            std::move(work), detail::BlockingClass::resolver);
    }
    if (cancel.cancelled()) co_return Value{Error{Errc::cancelled}};

    detail::Scheduler* const scheduler = detail::current_scheduler();
    if (scheduler == nullptr) co_return Value{Error{Errc::shutdown}};

    auto out = cio::make_chan<Value>(1);
    using Job = DetachedLookup<Work, Value>;
    auto* job = new Job(std::move(work), out);
    job->run = &Job::run_job;
    job->fail = &Job::fail_job;
    job->klass = detail::BlockingClass::resolver;

    switch (scheduler->blocking().submit(job)) {
        case detail::BlockingSubmitResult::accepted:
            break;
        case detail::BlockingSubmitResult::overloaded:
            delete job;
            co_return Value{Error{Errc::overloaded}};
        case detail::BlockingSubmitResult::shutdown:
            delete job;
            co_return Value{Error{Errc::shutdown}};
    }

    auto selected = cio::select(cio::recv(out), cio::recv(cancel.done()));
    if (co_await selected == 1) co_return Value{Error{Errc::cancelled}};

    auto received = selected.template get<0>();
    if (!received) co_return Value{Error{Errc::broken}};
    co_return std::move(*received);
}

}  // namespace

Task<Result<std::vector<SocketAddr>>> Resolver::lookup_host(
    std::string host, std::uint16_t port, CancelToken cancel) const {
    if (options_.prefer_builtin) {
        dns::Config config;
        config.ipv4 = options_.family != AddressFamily::ipv6;
        config.ipv6 = options_.family != AddressFamily::ipv4;
        co_return co_await dns::Resolver{std::move(config)}.lookup(
            std::move(host), port, std::move(cancel));
    }

    const int family = af_of(options_.family);
    co_return co_await cancellable_blocking(
        [host = std::move(host), port, family] {
            return lookup_host_blocking(host, port, family);
        },
        std::move(cancel));
}

Task<Result<std::vector<std::string>>> Resolver::lookup_addr(
    SocketAddr address, CancelToken cancel) const {
    if (!address.valid()) co_return Error{EINVAL};
    co_return co_await cancellable_blocking(
        [address] { return lookup_addr_blocking(address); }, std::move(cancel));
}

std::string SocketAddr::ip() const {
    char buffer[INET6_ADDRSTRLEN] = {};
    switch (family()) {
        case AF_INET: {
            const auto* v4 = reinterpret_cast<const sockaddr_in*>(storage_);
            ::inet_ntop(AF_INET, &v4->sin_addr, buffer, sizeof(buffer));
            return buffer;
        }
        case AF_INET6: {
            const auto* v6 = reinterpret_cast<const sockaddr_in6*>(storage_);
            ::inet_ntop(AF_INET6, &v6->sin6_addr, buffer, sizeof(buffer));
            return buffer;
        }
        default:
            return {};
    }
}

Result<std::pair<std::string, std::string>> split_host_port(
    std::string_view host_port) {
    if (host_port.empty()) return Error{EINVAL};

    if (host_port.front() == '[') {
        // "[::1]:80" — the brackets are what disambiguate an IPv6 literal from
        // a host:port with many colons.
        const std::size_t close = host_port.find(']');
        if (close == std::string_view::npos) return Error{EINVAL};
        if (close + 1 >= host_port.size() || host_port[close + 1] != ':') {
            return Error{EINVAL};
        }
        const std::string_view host = host_port.substr(1, close - 1);
        const std::string_view port = host_port.substr(close + 2);
        if (host.empty() || port.empty()) return Error{EINVAL};
        return std::pair<std::string, std::string>{std::string(host),
                                                   std::string(port)};
    }

    const std::size_t colon = host_port.rfind(':');
    if (colon == std::string_view::npos) return Error{EINVAL};
    // More than one colon without brackets is an unbracketed IPv6 literal,
    // which is ambiguous and which Go rejects too.
    if (host_port.find(':') != colon) return Error{EINVAL};

    const std::string_view host = host_port.substr(0, colon);
    const std::string_view port = host_port.substr(colon + 1);
    if (port.empty()) return Error{EINVAL};
    return std::pair<std::string, std::string>{std::string(host),
                                               std::string(port)};
}

std::string join_host_port(std::string_view host, std::string_view port) {
    // Bracket anything that looks like an IPv6 literal, as Go does.
    if (host.find(':') != std::string_view::npos) {
        return "[" + std::string(host) + "]:" + std::string(port);
    }
    return std::string(host) + ":" + std::string(port);
}

std::string join_host_port(std::string_view host, std::uint16_t port) {
    return join_host_port(host, std::to_string(port));
}

Task<Result<std::vector<SocketAddr>>> resolve(std::string host, std::uint16_t port) {
    co_return co_await Resolver{}.lookup_host(std::move(host), port);
}

// ---------------------------------------------------------------- Socket ---

namespace {

// Wakes a descriptor's parked operations when its bound token fires.
//
// Holds the scheduler lifetime because the hook may outlive the Socket: the
// CancelState keeps a reference until cancellation happens, and the descriptor
// slab must still exist when it does. The generation check inside
// cancel_waiters() makes a hook for a closed or recycled descriptor inert.
class SocketCancelHook final : public detail::CancelHook {
public:
    SocketCancelHook(std::shared_ptr<detail::Scheduler> scheduler,
                     detail::IoDesc* desc, std::uint32_t generation) noexcept
        : scheduler_(std::move(scheduler)),
          desc_(desc),
          generation_(generation) {}

    void on_cancel() noexcept override {
        if (!active_.load(std::memory_order_acquire)) return;
        if (desc_ == nullptr || desc_->owner == nullptr) return;
        desc_->owner->cancel_waiters(desc_, generation_);
    }

    // The binding was replaced or the socket closed. Going inert is safer than
    // unlinking, which would race a cancel already running the hook list.
    void detach() noexcept { active_.store(false, std::memory_order_release); }

private:
    std::shared_ptr<detail::Scheduler> scheduler_;
    detail::IoDesc* desc_ = nullptr;
    std::uint32_t generation_ = 0;
    std::atomic<bool> active_{true};
};

}  // namespace

void Socket::set_cancel(CancelToken token) {
    if (cancel_binding_ != nullptr) {
        static_cast<SocketCancelHook*>(cancel_binding_.get())->detach();
        if (const auto& previous = detail::CancelAccess::state(cancel_token_);
            previous != nullptr) {
            previous->remove_hook(cancel_binding_);
        }
        cancel_binding_.reset();
    }

    const IoOperation operation = capture_operation(fd_, desc_);
    cancel_token_ = std::move(token);
    const auto& state = detail::CancelAccess::state(cancel_token_);

    // Publish the flag first: even without a live descriptor, a later
    // operation on this socket must observe the binding.
    if (operation) {
        operation.desc->cancel_flag.store(
            state != nullptr ? &state->cancelled : nullptr,
            std::memory_order_release);
    }
    if (state == nullptr || !operation) return;

    auto hook = std::make_shared<SocketCancelHook>(
        scheduler_lifetime_, operation.desc, operation.generation);
    if (!state->add_hook(hook)) {
        // Already cancelled; nothing will call back, so wake now.
        hook->on_cancel();
        return;
    }
    cancel_binding_ = std::move(hook);
}

TimePoint Socket::deadline(bool write_direction) const {
    const IoOperation operation =
        capture_operation(const_cast<int&>(fd_),
                          const_cast<detail::IoDesc*&>(desc_));
    if (!operation) return TimePoint{};
    const auto index = static_cast<unsigned>(
        write_direction ? detail::Dir::kWrite : detail::Dir::kRead);
    const std::int64_t deadline_ns =
        operation.desc->absolute_deadline_ns[index].load(
            std::memory_order_acquire);
    if (deadline_ns == 0) return TimePoint{};
    return TimePoint{std::chrono::nanoseconds{deadline_ns}};
}

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
    // Retire any cancellation binding first: the descriptor is about to be
    // unregistered, and a hook that fires afterwards must not touch it. The
    // generation check would already make it inert; going inert here as well
    // keeps a closed socket from holding the token's hook list open.
    if (cancel_binding_ != nullptr) {
        static_cast<SocketCancelHook*>(cancel_binding_.get())->detach();
        if (const auto& state = detail::CancelAccess::state(cancel_token_);
            state != nullptr) {
            state->remove_hook(cancel_binding_);
        }
        cancel_binding_.reset();
    }

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

Result<SocketAddr> Socket::remote_addr() const {
    if (fd_ < 0) return Error{EBADF};
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&storage), &length) != 0) {
        return Error::from_errno();
    }
    return SocketAddr::from_raw(&storage, length);
}

// ------------------------------------------------------------- TcpConn ---

// The try_* forms always attempt the syscall — a caller asking to "try" wants
// an attempt — but they keep the readiness hint coherent so that mixing them
// with the awaiting forms does not confuse it.
Result<std::size_t> TcpConn::try_read(std::span<std::byte> buffer) {
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

Result<std::size_t> TcpConn::try_write(std::span<const std::byte> buffer) {
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

Task<Result<std::size_t>> TcpConn::read(std::span<std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    co_return co_await tcp_read_some(
        operation.desc, operation.generation, buffer);
}

Task<Result<std::size_t>> TcpConn::write(std::span<const std::byte> buffer) {
    // Go's io.Writer contract: the whole span goes out unless an error stops
    // it, so no caller needs a retry loop and none can forget one.
    const std::size_t total = buffer.size();
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    while (!buffer.empty()) {
        auto n = co_await tcp_write_some(operation.desc, operation.generation,
                                         buffer);
        if (!n) co_return n.error();
        buffer = buffer.subspan(*n);
    }
    co_return total;
}

Result<bool> TcpConn::begin_connect(SocketAddr addr, TcpConn& stream) {
    if (!addr.valid()) return Error{EINVAL};

    const int fd = ::socket(addr.family(), SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return Error::from_errno();

    // Start the state machine before registering EPOLLOUT. A never-connected
    // TCP fd is itself reported writable by epoll; registering it first can
    // leave a stale kIoReady that makes an ensuing EINPROGRESS look complete
    // while SO_ERROR is still zero.
    AcceptedFd pending(fd);
    const int connect_result =
        ::connect(pending.get(), addr.raw(), addr.length());
    const int connect_error = connect_result == 0 ? 0 : errno;
    if (connect_result != 0 && connect_error != EINPROGRESS) {
        return Error{connect_error};
    }

    if (auto adopted =
            stream.adopt(pending.release(), /*already_nonblocking=*/true);
        !adopted) {
        return adopted.error();
    }
    return connect_result == 0;
}

Task<Result<void>> TcpConn::await_connect(TcpConn& stream) {
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
            co_return ok();
        }

        const int peer_error = errno;
        if (peer_error != ENOTCONN) co_return Error{peer_error};

        // The edge was spurious or predated actual completion. The slot was
        // consumed by the awaiter above; park again for the transition that
        // installs either a peer or a concrete SO_ERROR.
        stream.desc_->note_would_block(detail::Dir::kWrite);
    }
}

Task<Result<TcpConn>> TcpConn::dial(SocketAddr addr) {
    TcpConn stream;
    auto started = begin_connect(addr, stream);
    if (!started) co_return started.error();
    if (*started) co_return std::move(stream);

    if (auto ready = co_await await_connect(stream); !ready) {
        co_return ready.error();
    }
    co_return std::move(stream);
}

Task<Result<TcpConn>> TcpConn::dial(SocketAddr addr, CancelToken cancel) {
    // Qualified: an unqualified connect() now finds the ::connect syscall.
    if (!cancel) co_return co_await TcpConn::dial(addr);
    if (cancel.cancelled()) co_return Error{Errc::cancelled};

    // Cancellation closes the socket rather than merely abandoning the wait.
    //
    // An earlier version detached the attempt and let the caller stop waiting
    // for it. That resumed the caller correctly but left the connect parked
    // until the kernel gave up on its SYN retries — holding a descriptor for
    // roughly two minutes, and leaking the frame outright if the runtime shut
    // down first, because shutdown does not unwind tasks parked on sockets.
    //
    // Closing the descriptor wakes the parked awaiter immediately, so the
    // watcher and the connect both finish at cancellation time. The stream is
    // shared because the watcher runs concurrently with the connect; close() is
    // documented to be safe against a parked reader or writer.
    auto stream = std::make_shared<TcpConn>();
    auto started = begin_connect(addr, *stream);
    if (!started) co_return started.error();
    if (*started) co_return std::move(*stream);

    // Closed on every exit path to release the watcher when no cancellation
    // arrived; recv() on a closed channel completes rather than parking.
    auto settled = make_chan<Unit>(1);
    auto watcher = spawn([](std::shared_ptr<TcpConn> target, CancelToken token,
                            Chan<Unit> done) -> Task<bool> {
        auto selected = cio::select(cio::recv(token.done()), cio::recv(done));
        const bool cancelled = (co_await selected) == 0;
        if (cancelled) target->close();
        co_return cancelled;
    }(stream, cancel, settled));

    auto ready = co_await await_connect(*stream);

    settled.close();
    // Joined before the stream is moved out, so the watcher cannot touch it
    // afterwards.
    const bool cancelled = co_await watcher;
    if (cancelled) co_return Error{Errc::cancelled};
    if (!ready) co_return ready.error();
    co_return std::move(*stream);
}

Task<Result<TcpConn>> TcpConn::dial(std::string host, std::uint16_t port) {
    co_return co_await Dialer{}.dial_tcp(std::move(host), port);
}

Task<Result<TcpConn>> TcpConn::dial(std::string host, std::uint16_t port,
                                           CancelToken cancel) {
    co_return co_await Dialer{}.dial_tcp(std::move(host), port,
                                         std::move(cancel));
}

// ---------------------------------------------------------------- Dialer ---

namespace {

constexpr auto kDefaultFallbackDelay = std::chrono::milliseconds(300);

// Go's ordering: alternate families so a blackholed IPv6 route costs one
// attempt's delay rather than every v6 address in the list.
std::vector<SocketAddr> interleave_families(
    const std::vector<SocketAddr>& addresses) {
    std::vector<SocketAddr> v6;
    std::vector<SocketAddr> v4;
    for (const auto& addr : addresses) {
        (addr.family() == AF_INET6 ? v6 : v4).push_back(addr);
    }

    // Whichever family the resolver listed first keeps priority, matching the
    // system's own address-selection preference.
    const bool v6_first =
        addresses.empty() || addresses.front().family() == AF_INET6;
    std::vector<SocketAddr>& primary = v6_first ? v6 : v4;
    std::vector<SocketAddr>& secondary = v6_first ? v4 : v6;

    std::vector<SocketAddr> ordered;
    ordered.reserve(addresses.size());
    for (std::size_t i = 0; i < primary.size() || i < secondary.size(); ++i) {
        if (i < primary.size()) ordered.push_back(primary[i]);
        if (i < secondary.size()) ordered.push_back(secondary[i]);
    }
    return ordered;
}

}  // namespace

Task<Result<TcpConn>> Dialer::dial_tcp(std::string host, std::uint16_t port,
                                         CancelToken cancel) const {
    if (cancel && cancel.cancelled()) co_return Error{Errc::cancelled};

    const bool has_overall_timeout = options_.timeout > Duration::zero();
    const TimePoint overall_deadline =
        has_overall_timeout ? Clock::now() + options_.timeout : TimePoint{};

    std::vector<SocketAddr> targets;
    if (auto literal = SocketAddr::parse(host, port); literal) {
        targets.push_back(*literal);
    } else {
        LookupOptions lookup;
        lookup.family = options_.family;
        lookup.prefer_builtin = options_.prefer_builtin_resolver;
        Resolver resolver{lookup};
        auto addresses =
            co_await resolver.lookup_host(std::move(host), port, cancel);
        if (!addresses) co_return addresses.error();
        targets = interleave_families(*addresses);
    }
    if (targets.empty()) co_return Error{ENOENT};

    const Duration fallback_delay = options_.fallback_delay > Duration::zero()
                                        ? options_.fallback_delay
                                        : Duration{kDefaultFallbackDelay};

    // Attempts are raced, not tried one at a time: a new address is started
    // every fallback_delay until one connects. The first success wins and the
    // rest are cancelled, which closes their sockets, and then joined. Nothing
    // is detached — an attempt outliving the dial would leak its frame if the
    // runtime shut down while it was parked on a socket.
    CancelSource stop;
    auto results = make_chan<Result<TcpConn>>(targets.size());

    // One watcher maps the caller's token onto the shared attempt source, so
    // the racing loop never has to special-case an absent token.
    auto settled = make_chan<Unit>(1);
    JoinHandle<void> watcher;
    if (cancel) {
        watcher = spawn([](CancelToken token, CancelSource source,
                           Chan<Unit> done) -> Task<void> {
            auto selected = cio::select(cio::recv(token.done()), cio::recv(done));
            if ((co_await selected) == 0) source.cancel();
        }(cancel, stop, settled));
    }

    std::vector<JoinHandle<void>> attempts;
    attempts.reserve(targets.size());

    Result<TcpConn> winner = Error{EHOSTUNREACH};
    Result<TcpConn> last = Error{EHOSTUNREACH};
    std::size_t started = 0;
    std::size_t finished = 0;

    while (finished < targets.size()) {
        if (started < targets.size()) {
            attempts.push_back(spawn(
                [](SocketAddr target, CancelToken attempt_cancel,
                   Chan<Result<TcpConn>> out) -> Task<void> {
                    auto stream = co_await TcpConn::dial(
                        target, std::move(attempt_cancel));
                    co_await out.send(std::move(stream));
                }(targets[started], stop.token(), results)));
            ++started;
        }

        // While addresses remain, a result and the stagger tick race; once all
        // are running only the overall deadline can end the wait early.
        std::optional<Result<TcpConn>> received;
        if (started < targets.size()) {
            if (has_overall_timeout) {
                auto selected = cio::select(cio::recv(results),
                                            cio::after(fallback_delay),
                                            cio::after_deadline(overall_deadline));
                const std::size_t index = co_await selected;
                if (index == 2) break;
                if (index == 1) continue;
                received = selected.get<0>();
            } else {
                auto selected =
                    cio::select(cio::recv(results), cio::after(fallback_delay));
                if ((co_await selected) == 1) continue;
                received = selected.get<0>();
            }
        } else if (has_overall_timeout) {
            auto selected = cio::select(cio::recv(results),
                                        cio::after_deadline(overall_deadline));
            if ((co_await selected) == 1) break;
            received = selected.get<0>();
        } else {
            received = co_await results.recv();
        }

        if (!received) break;
        ++finished;

        last = std::move(*received);
        if (last) {
            winner = std::move(last);
            last = Error{EHOSTUNREACH};
            break;
        }
        // The caller's token fired; stop racing and report it.
        if (last.error().is(Errc::cancelled)) break;
    }

    // Cancel first so every loser closes its socket, then join them all. The
    // join is prompt precisely because cancellation closes rather than
    // abandons.
    stop.cancel();
    for (auto& attempt : attempts) co_await attempt;
    settled.close();
    if (watcher.valid()) co_await watcher;

    if (winner) {
        if (options_.nodelay) (void)winner->set_nodelay(true);
        co_return winner;
    }
    if (cancel && cancel.cancelled()) co_return Error{Errc::cancelled};
    if (has_overall_timeout && Clock::now() >= overall_deadline &&
        !last.error().is(Errc::cancelled)) {
        co_return Error{Errc::timed_out};
    }
    co_return last;
}

Task<Result<TcpConn>> dial_tcp(std::string host, std::uint16_t port,
                                 CancelToken cancel) {
    co_return co_await Dialer{}.dial_tcp(std::move(host), port,
                                         std::move(cancel));
}

void TcpConn::set_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, true, true, absolute_ns(deadline));
}

void TcpConn::set_read_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, true, false, absolute_ns(deadline));
}

void TcpConn::set_write_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, false, true, absolute_ns(deadline));
}

void TcpConn::set_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, true, true, deadline_from_now(timeout));
}

void TcpConn::set_read_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, true, false, deadline_from_now(timeout));
}

void TcpConn::set_write_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, false, true, deadline_from_now(timeout));
}

void TcpConn::clear_deadline() {
    apply_deadline(fd_, desc_, true, true, 0);
}

void TcpConn::clear_read_deadline() {
    apply_deadline(fd_, desc_, true, false, 0);
}

void TcpConn::clear_write_deadline() {
    apply_deadline(fd_, desc_, false, true, 0);
}

Result<void> TcpConn::set_nodelay(bool on) {
    const int value = on ? 1 : 0;
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpConn::set_keepalive(bool on) {
    const int value = on ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpConn::set_keepalive_period(Duration idle) {
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(idle).count();
    // Go rounds to whole seconds too; the option has no finer resolution.
    const int value = static_cast<int>(seconds > 0 ? seconds : 1);
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_KEEPIDLE, &value, sizeof(value)) != 0) {
        return Error::from_errno();
    }
    // Enabling a period without enabling keepalive itself would silently do
    // nothing, which is not what the call says it does.
    return set_keepalive(true);
}

Result<void> TcpConn::set_linger(Duration timeout) {
    ::linger value{};
    if (timeout < Duration::zero()) {
        value.l_onoff = 0;  // system default: flush in the background
    } else {
        value.l_onoff = 1;
        value.l_linger = static_cast<int>(
            std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
    }
    if (::setsockopt(fd_, SOL_SOCKET, SO_LINGER, &value, sizeof(value)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpConn::set_read_buffer(int bytes) {
    if (bytes <= 0) return Error{EINVAL};
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpConn::set_write_buffer(int bytes) {
    if (bytes <= 0) return Error{EINVAL};
    if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
        return Error::from_errno();
    }
    return ok();
}

Result<void> TcpConn::close_write() {
    if (::shutdown(fd_, SHUT_WR) != 0) return Error::from_errno();
    return ok();
}

// ----------------------------------------------------------- TcpListener ---

Result<TcpListener> TcpListener::listen(SocketAddr addr, int backlog) {
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

Result<TcpListener> TcpListener::listen(std::string_view host, std::uint16_t port, int backlog) {
    auto addr = SocketAddr::parse(host, port);
    if (!addr) return addr.error();
    return TcpListener::listen(*addr, backlog);
}

Task<Result<TcpConn>> TcpListener::accept() {
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

                TcpConn stream;
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

// ------------------------------------------------------------- UdpConn ---

Result<UdpConn> UdpConn::listen(SocketAddr addr) {
    if (!addr.valid()) return Error{EINVAL};

    const int fd = ::socket(addr.family(), SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return Error::from_errno();

    if (::bind(fd, addr.raw(), addr.length()) != 0) {
        const Error err = Error::from_errno();
        ::close(fd);
        return err;
    }

    UdpConn socket;
    if (auto adopted = socket.adopt(fd, /*already_nonblocking=*/true); !adopted) {
        return adopted.error();
    }
    return socket;
}

Task<Result<std::size_t>> UdpConn::read_from(std::span<std::byte> buffer, SocketAddr& from) {
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

Task<Result<std::size_t>> UdpConn::write_to(std::span<const std::byte> buffer,
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

void UdpConn::set_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, true, true, absolute_ns(deadline));
}

void UdpConn::set_read_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, true, false, absolute_ns(deadline));
}

void UdpConn::set_write_deadline(TimePoint deadline) {
    apply_deadline(fd_, desc_, false, true, absolute_ns(deadline));
}

void UdpConn::set_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, true, true, deadline_from_now(timeout));
}

void UdpConn::set_read_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, true, false, deadline_from_now(timeout));
}

void UdpConn::set_write_timeout(Duration timeout) {
    apply_deadline(fd_, desc_, false, true, deadline_from_now(timeout));
}

void UdpConn::clear_deadline() {
    apply_deadline(fd_, desc_, true, true, 0);
}

void UdpConn::clear_read_deadline() {
    apply_deadline(fd_, desc_, true, false, 0);
}

void UdpConn::clear_write_deadline() {
    apply_deadline(fd_, desc_, false, true, 0);
}


// ------------------------------------------------------------- Unix ---

namespace {

// Fills a sockaddr_un. Returns the length to pass to bind/connect, which for an
// abstract address is *not* the whole struct: the trailing NULs would become part
// of the name.
Result<unsigned> fill_unix_addr(const UnixAddr& addr, sockaddr_un& out) {
    const std::string path = addr.path();
    if (path.empty()) return Error{EINVAL};

    out.sun_family = AF_UNIX;
    if (addr.abstract()) {
        // Linux abstract namespace: a leading NUL, then the name, with no
        // terminator.
        if (path.size() + 1 > sizeof(out.sun_path)) return Error{ENAMETOOLONG};
        out.sun_path[0] = '\0';
        std::memcpy(out.sun_path + 1, path.data(), path.size());
        return static_cast<unsigned>(offsetof(sockaddr_un, sun_path) + 1 +
                                     path.size());
    }
    if (path.size() + 1 > sizeof(out.sun_path)) return Error{ENAMETOOLONG};
    std::memcpy(out.sun_path, path.data(), path.size() + 1);
    return static_cast<unsigned>(sizeof(sockaddr_un));
}

}  // namespace

Result<UnixAddr> UnixAddr::parse(std::string_view path) {
    if (path.empty()) return Error{EINVAL};
    UnixAddr addr;
    if (path.front() == '@') {
        addr.abstract_ = true;
        addr.path_ = std::string(path.substr(1));
        if (addr.path_.empty()) return Error{EINVAL};
    } else {
        addr.path_ = std::string(path);
    }
    // sun_path is fixed; reject early rather than at bind time.
    if (addr.path_.size() + 1 > sizeof(sockaddr_un::sun_path)) {
        return Error{ENAMETOOLONG};
    }
    return addr;
}

std::string UnixAddr::path() const {
    return abstract_ ? path_ : path_;
}

Task<Result<UnixConn>> UnixConn::dial(UnixAddr addr) {
    sockaddr_un raw{};
    auto length = fill_unix_addr(addr, raw);
    if (!length) co_return length.error();

    const int fd =
        ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) co_return Error::from_errno();

    AcceptedFd pending(fd);
    const int rc = ::connect(pending.get(),
                             reinterpret_cast<const sockaddr*>(&raw), *length);
    const int connect_error = rc == 0 ? 0 : errno;
    if (rc != 0 && connect_error != EINPROGRESS) co_return Error{connect_error};

    UnixConn conn;
    if (auto adopted = conn.adopt(pending.release(), true); !adopted) {
        co_return adopted.error();
    }
    if (rc == 0) co_return std::move(conn);

    // A Unix connect completes immediately or fails; EINPROGRESS is possible for
    // a full backlog, and completion is reported as writability just as for TCP.
    const std::uint32_t generation =
        conn.desc_->generation.load(std::memory_order_acquire);
    if (auto ready = co_await detail::IoAwaiter{conn.desc_, detail::Dir::kWrite,
                                               generation};
        !ready) {
        co_return ready.error();
    }
    int error = 0;
    socklen_t error_length = sizeof(error);
    if (::getsockopt(conn.fd_, SOL_SOCKET, SO_ERROR, &error, &error_length) != 0) {
        co_return Error::from_errno();
    }
    if (error != 0) co_return Error{error};
    co_return std::move(conn);
}

Task<Result<UnixConn>> UnixConn::dial(UnixAddr addr, CancelToken cancel) {
    if (!cancel) co_return co_await UnixConn::dial(addr);
    if (cancel.cancelled()) co_return Error{Errc::cancelled};

    // Same rule as a cancellable TCP dial: cancellation closes the descriptor
    // rather than abandoning the attempt.
    auto out = make_chan<Result<UnixConn>>(1);
    auto attempt = spawn([](UnixAddr target,
                            Chan<Result<UnixConn>> channel) -> Task<void> {
        auto conn = co_await UnixConn::dial(target);
        co_await channel.send(std::move(conn));
    }(addr, out));

    auto selected = cio::select(cio::recv(out), cio::recv(cancel.done()));
    const bool cancelled = (co_await selected) == 1;
    if (cancelled) {
        co_await attempt;
        co_return Error{Errc::cancelled};
    }
    co_await attempt;
    auto received = selected.get<0>();
    if (!received) co_return Error{Errc::broken};
    co_return std::move(*received);
}

Task<Result<std::size_t>> UnixConn::read(std::span<std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    co_return co_await tcp_read_some(operation.desc, operation.generation, buffer);
}

Task<Result<std::size_t>> UnixConn::write(std::span<const std::byte> buffer) {
    const std::size_t total = buffer.size();
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) co_return operation.error;
    while (!buffer.empty()) {
        auto n = co_await tcp_write_some(operation.desc, operation.generation,
                                         buffer);
        if (!n) co_return n.error();
        buffer = buffer.subspan(*n);
    }
    co_return total;
}

Result<std::size_t> UnixConn::try_read(std::span<std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return operation.error;

    detail::FdUseGuard fd_use{operation.desc, detail::Dir::kRead,
                              operation.generation};
    if (!fd_use) return fd_use.error();

    const ssize_t n = ::recv(fd_use.fd(), buffer.data(), buffer.size(), 0);
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

Result<std::size_t> UnixConn::try_write(std::span<const std::byte> buffer) {
    const IoOperation operation = capture_operation(fd_, desc_);
    if (!operation) return operation.error;

    detail::FdUseGuard fd_use{operation.desc, detail::Dir::kWrite,
                              operation.generation};
    if (!fd_use) return fd_use.error();

    // MSG_NOSIGNAL for the same reason as TCP: a write to a closed peer must be
    // an EPIPE return, not a process-wide SIGPIPE.
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

void UnixConn::set_deadline(TimePoint d) {
    apply_deadline(fd_, desc_, true, true, absolute_ns(d));
}
void UnixConn::set_read_deadline(TimePoint d) {
    apply_deadline(fd_, desc_, true, false, absolute_ns(d));
}
void UnixConn::set_write_deadline(TimePoint d) {
    apply_deadline(fd_, desc_, false, true, absolute_ns(d));
}
void UnixConn::set_timeout(Duration t) {
    apply_deadline(fd_, desc_, true, true, deadline_from_now(t));
}
void UnixConn::set_read_timeout(Duration t) {
    apply_deadline(fd_, desc_, true, false, deadline_from_now(t));
}
void UnixConn::set_write_timeout(Duration t) {
    apply_deadline(fd_, desc_, false, true, deadline_from_now(t));
}
void UnixConn::clear_deadline() { apply_deadline(fd_, desc_, true, true, 0); }
void UnixConn::clear_read_deadline() {
    apply_deadline(fd_, desc_, true, false, 0);
}
void UnixConn::clear_write_deadline() {
    apply_deadline(fd_, desc_, false, true, 0);
}

Result<void> UnixConn::close_write() {
    if (::shutdown(fd_, SHUT_WR) != 0) return Error::from_errno();
    return ok();
}

Result<UnixListener> UnixListener::listen(UnixAddr addr, int backlog,
                                         bool unlink_existing) {
    sockaddr_un raw{};
    auto length = fill_unix_addr(addr, raw);
    if (!length) return length.error();

    // A filesystem socket left behind by a previous run makes bind() fail with
    // EADDRINUSE even when nothing is listening, which is the single most common
    // way a Unix-socket service fails to restart.
    if (unlink_existing && !addr.abstract()) {
        ::unlink(addr.path().c_str());
    }

    const int fd =
        ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return Error::from_errno();

    AcceptedFd pending(fd);
    if (::bind(pending.get(), reinterpret_cast<const sockaddr*>(&raw),
               *length) != 0) {
        return Error::from_errno();
    }
    if (::listen(pending.get(), backlog) != 0) return Error::from_errno();

    UnixListener listener;
    if (auto adopted = listener.adopt(pending.release(), true); !adopted) {
        return adopted.error();
    }
    listener.bound_ = addr;
    listener.owns_path_ = !addr.abstract();
    return listener;
}

Task<Result<UnixConn>> UnixListener::accept() {
    for (;;) {
        const IoOperation operation = capture_operation(fd_, desc_);
        if (!operation) co_return operation.error;

        int accepted = -1;
        {
            detail::FdUseGuard fd_use{operation.desc, detail::Dir::kRead,
                                      operation.generation};
            if (!fd_use) co_return fd_use.error();
            accepted = ::accept4(fd_use.fd(), nullptr, nullptr,
                                 SOCK_CLOEXEC | SOCK_NONBLOCK);
        }
        if (accepted >= 0) {
            AcceptedFd owned(accepted);
            UnixConn conn;
            if (auto adopted = conn.adopt(owned.release(), true); !adopted) {
                co_return adopted.error();
            }
            co_return std::move(conn);
        }
        const int error = errno;
        // ECONNABORTED means a pending connection went away before accept saw
        // it; that is not this listener failing, so retry rather than report.
        if (error == EINTR || error == ECONNABORTED) continue;
        if (error != EAGAIN && error != EWOULDBLOCK) co_return Error{error};

        operation.desc->note_would_block(detail::Dir::kRead);
        if (auto ready = co_await detail::IoAwaiter{
                operation.desc, detail::Dir::kRead, operation.generation};
            !ready) {
            co_return ready.error();
        }
    }
}

Result<UnixAddr> UnixListener::addr() const {
    if (!bound_.valid()) return Error{EBADF};
    return bound_;
}

void UnixListener::set_deadline(TimePoint d) {
    apply_deadline(fd_, desc_, true, false, absolute_ns(d));
}

void UnixListener::clear_deadline() {
    apply_deadline(fd_, desc_, true, false, 0);
}

void UnixListener::unlink() {
    if (!owns_path_ || !bound_.valid()) return;
    ::unlink(bound_.path().c_str());
    owns_path_ = false;
}

void UnixListener::close() {
    // Remove the path this listener created; bind(2) leaves it behind, and a
    // stale path is what blocks the next start.
    unlink();
    Socket::close();
}

}  // namespace cio::net
