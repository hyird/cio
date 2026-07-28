#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;

namespace {

template <typename Awaiter, typename Result>
concept ResumesAs = requires(Awaiter& awaiter) {
    { awaiter.await_ready() } -> std::convertible_to<bool>;
    { awaiter.await_resume() } -> std::same_as<Result>;
};

using IntTaskAwaiter =
    decltype(std::declval<cio::Task<int>&&>().operator co_await());
using VoidTaskAwaiter =
    decltype(std::declval<cio::Task<>&&>().operator co_await());
using IntJoinAwaiter =
    decltype(std::declval<cio::JoinHandle<int>&>().operator co_await());
using VoidJoinAwaiter =
    decltype(std::declval<cio::JoinHandle<>&>().operator co_await());
using IntSendAwaiter =
    decltype(std::declval<const cio::Chan<int>&>().send(1));
using IntRecvAwaiter =
    decltype(std::declval<const cio::Chan<int>&>().recv());
using SelectType = decltype(cio::select(
    cio::recv(std::declval<cio::Chan<int>>()), cio::otherwise()));
using SelectAwaiter =
    decltype(std::declval<SelectType&>().operator co_await());
using GroupJoinAwaiter =
    decltype(std::declval<cio::TaskGroup&>().join());

// Runtime and Task.
static_assert(std::same_as<decltype(cio::RuntimeOptions::worker_threads),
                           std::size_t>);
static_assert(std::same_as<decltype(cio::RuntimeOptions::max_blocking_threads),
                           std::size_t>);
static_assert(std::same_as<decltype(cio::RuntimeOptions::max_blocking_queue),
                           std::size_t>);
static_assert(std::is_constructible_v<cio::Runtime, cio::RuntimeOptions>);
static_assert(!std::is_copy_constructible_v<cio::Runtime>);
static_assert(!std::is_copy_assignable_v<cio::Runtime>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Runtime&>().worker_count()),
              std::size_t>);
static_assert(noexcept(std::declval<const cio::Runtime&>().worker_count()));
static_assert(std::same_as<
              decltype(std::declval<cio::Runtime&>().block_on(
                  std::declval<cio::Task<int>>())),
              int>);
static_assert(std::same_as<
              decltype(std::declval<cio::Runtime&>().spawn(
                  std::declval<cio::Task<int>>())),
              cio::JoinHandle<int>>);
static_assert(std::same_as<
              decltype(std::declval<cio::Runtime&>().go(
                  std::declval<cio::Task<>>())),
              void>);
static_assert(std::same_as<
              decltype(cio::run(std::declval<cio::Task<int>>(),
                                std::declval<cio::RuntimeOptions>())),
              int>);

static_assert(std::same_as<cio::Task<int>::value_type, int>);
static_assert(std::same_as<cio::Task<>::value_type, void>);
static_assert(std::is_nothrow_default_constructible_v<cio::Task<int>>);
static_assert(std::is_nothrow_move_constructible_v<cio::Task<int>>);
static_assert(std::is_nothrow_move_assignable_v<cio::Task<int>>);
static_assert(!std::is_copy_constructible_v<cio::Task<int>>);
static_assert(!std::is_copy_assignable_v<cio::Task<int>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Task<int>&>().valid()), bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Task<int>&>().done()), bool>);
static_assert(noexcept(std::declval<const cio::Task<int>&>().valid()));
static_assert(noexcept(std::declval<const cio::Task<int>&>().done()));
static_assert(ResumesAs<IntTaskAwaiter, int&&>);
static_assert(ResumesAs<VoidTaskAwaiter, void>);

// spawn/go/yield.
static_assert(std::is_nothrow_default_constructible_v<cio::JoinHandle<int>>);
static_assert(std::is_nothrow_move_constructible_v<cio::JoinHandle<int>>);
static_assert(!std::is_copy_constructible_v<cio::JoinHandle<int>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::JoinHandle<int>&>().valid()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::JoinHandle<int>&>().done()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<cio::JoinHandle<int>&>().detach()), void>);
static_assert(ResumesAs<IntJoinAwaiter, int>);
static_assert(ResumesAs<VoidJoinAwaiter, void>);
static_assert(requires(const IntJoinAwaiter& awaiter) {
    { awaiter.await_ready() } -> std::convertible_to<bool>;
});
static_assert(std::same_as<
              decltype(cio::spawn(std::declval<cio::Task<int>>())),
              cio::JoinHandle<int>>);
static_assert(std::same_as<
              decltype(cio::go(std::declval<cio::Task<>>())), void>);
static_assert(noexcept(cio::yield()));

// Channels and select.
static_assert(std::same_as<cio::Chan<int>::value_type, int>);
static_assert(std::same_as<decltype(cio::make_chan<int>(1)), cio::Chan<int>>);
static_assert(std::same_as<decltype(cio::make_chan()), cio::Chan<cio::Unit>>);
static_assert(std::is_nothrow_copy_constructible_v<cio::Chan<int>>);
static_assert(std::is_nothrow_move_constructible_v<cio::Chan<int>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().try_send(1)),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().try_recv()),
              std::optional<int>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().close()), void>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().is_closed()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().size()),
              std::size_t>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Chan<int>&>().capacity()),
              std::size_t>);
static_assert(ResumesAs<IntSendAwaiter, bool>);
static_assert(ResumesAs<IntRecvAwaiter, std::optional<int>>);

static_assert(std::same_as<
              decltype(cio::recv(std::declval<cio::Chan<int>>())),
              cio::RecvCase<int>>);
static_assert(std::same_as<
              decltype(cio::send(std::declval<cio::Chan<int>>(), 1)),
              cio::SendCase<int>>);
static_assert(std::same_as<decltype(cio::after(1ms)), cio::TimeoutCase>);
static_assert(std::same_as<
              decltype(cio::after_deadline(std::declval<cio::TimePoint>())),
              cio::TimeoutCase>);
static_assert(std::same_as<decltype(cio::otherwise()), cio::DefaultCase>);
static_assert(std::same_as<
              decltype(std::declval<const SelectType&>().index()),
              std::size_t>);
static_assert(std::same_as<
              decltype(std::declval<SelectType&>().template get<0>()),
              std::optional<int>>);
static_assert(std::same_as<
              decltype(std::declval<SelectType&>().template get<1>()), void>);
static_assert(ResumesAs<SelectAwaiter, std::size_t>);

// Structured concurrency and cancellation.
static_assert(!std::is_copy_constructible_v<cio::TaskGroup>);
static_assert(std::same_as<
              decltype(std::declval<cio::TaskGroup&>().spawn(
                  std::declval<cio::Task<>>())),
              void>);
static_assert(std::same_as<
              decltype(std::declval<const cio::TaskGroup&>().token()),
              cio::CancelToken>);
static_assert(std::same_as<
              decltype(std::declval<const cio::TaskGroup&>().cancel()), void>);
static_assert(ResumesAs<GroupJoinAwaiter, void>);
using MutexTryLock = bool (cio::Mutex::*)();
static_assert(std::same_as<decltype(&cio::Mutex::try_lock), MutexTryLock>);
static_assert(std::is_copy_constructible_v<cio::CancelToken>);
static_assert(std::same_as<
              decltype(std::declval<const cio::CancelToken&>().cancelled()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::CancelToken&>().done()),
              cio::Chan<cio::Unit>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::CancelSource&>().token()),
              cio::CancelToken>);
static_assert(std::same_as<
              decltype(std::declval<const cio::CancelSource&>().cancel()),
              void>);

// Time.
static_assert(std::same_as<cio::Clock, std::chrono::steady_clock>);
static_assert(std::same_as<cio::Duration, cio::Clock::duration>);
static_assert(std::same_as<cio::TimePoint, cio::Clock::time_point>);
static_assert(std::same_as<decltype(cio::now_ns()), std::int64_t>);
static_assert(std::same_as<decltype(cio::to_ns(1ms)), std::int64_t>);
static_assert(std::same_as<
              decltype(cio::deadline_from_now(std::declval<cio::Duration>())),
              std::int64_t>);
static_assert(std::same_as<
              decltype(cio::sleep(std::declval<cio::Duration>())),
              cio::SleepAwaiter>);
static_assert(std::same_as<
              decltype(cio::sleep_until(std::declval<cio::TimePoint>())),
              cio::SleepAwaiter>);
static_assert(ResumesAs<cio::SleepAwaiter, void>);

// Network surface. These assertions deliberately name only public types even
// where the concrete awaiter returned by a readiness helper is private.
using MutableBytes = std::span<std::byte>;
using ConstBytes = std::span<const std::byte>;
static_assert(std::same_as<
              decltype(cio::net::SocketAddr::parse(
                  std::declval<std::string_view>(), std::uint16_t{})),
              cio::Result<cio::net::SocketAddr>>);
static_assert(std::same_as<
              decltype(cio::net::SocketAddr::loopback_v4(std::uint16_t{})),
              cio::net::SocketAddr>);
static_assert(std::same_as<
              decltype(cio::net::resolve(std::declval<std::string>(),
                                         std::uint16_t{})),
              cio::Task<cio::Result<std::vector<cio::net::SocketAddr>>>>);
static_assert(!std::is_copy_constructible_v<cio::net::Socket>);
static_assert(std::is_nothrow_move_constructible_v<cio::net::Socket>);
static_assert(std::same_as<
              decltype(std::declval<const cio::net::Socket&>().valid()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::net::Socket&>().native_handle()),
              int>);
static_assert(std::same_as<
              decltype(cio::net::TcpStream::connect(
                  std::declval<cio::net::SocketAddr>())),
              cio::Task<cio::Result<cio::net::TcpStream>>>);
static_assert(std::same_as<
              decltype(cio::net::TcpStream::connect(
                  std::declval<std::string>(), std::uint16_t{})),
              cio::Task<cio::Result<cio::net::TcpStream>>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::TcpStream&>().read(
                  std::declval<MutableBytes>())),
              cio::Task<cio::Result<std::size_t>>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::TcpStream&>().write(
                  std::declval<ConstBytes>())),
              cio::Task<cio::Result<std::size_t>>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::TcpStream&>().write_all(
                  std::declval<ConstBytes>())),
              cio::Task<cio::Result<void>>>);
static_assert(std::same_as<
              decltype(cio::net::TcpListener::bind(
                  std::declval<cio::net::SocketAddr>(), 16)),
              cio::Result<cio::net::TcpListener>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::TcpListener&>().accept()),
              cio::Task<cio::Result<cio::net::TcpStream>>>);
static_assert(std::same_as<
              decltype(cio::net::UdpSocket::bind(
                  std::declval<cio::net::SocketAddr>())),
              cio::Result<cio::net::UdpSocket>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::UdpSocket&>().recv_from(
                  std::declval<MutableBytes>(),
                  std::declval<cio::net::SocketAddr&>())),
              cio::Task<cio::Result<std::size_t>>>);
static_assert(std::same_as<
              decltype(std::declval<cio::net::UdpSocket&>().send_to(
                  std::declval<ConstBytes>(),
                  std::declval<const cio::net::SocketAddr&>())),
              cio::Task<cio::Result<std::size_t>>>);

// Result and errors.
static_assert(std::same_as<cio::Result<int>::value_type, int>);
static_assert(std::same_as<cio::Result<void>::value_type, void>);
static_assert(std::is_convertible_v<int, cio::Result<int>>);
static_assert(std::is_convertible_v<cio::Errc, cio::Result<int>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Result<int>&>().has_value()),
              bool>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Result<int>&>().error()),
              cio::Error>);
static_assert(std::same_as<decltype(*std::declval<cio::Result<int>&>()), int&>);
static_assert(std::same_as<
              decltype(*std::declval<const cio::Result<int>&>()), const int&>);
static_assert(std::same_as<
              decltype(*std::declval<cio::Result<int>&&>()), int&&>);
static_assert(std::same_as<
              decltype(std::declval<cio::Result<int>&>().value()), int&>);
static_assert(std::same_as<
              decltype(std::declval<cio::Result<int>&&>().value()), int&&>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Result<int>&>().value_or(0)),
              int>);
static_assert(std::same_as<decltype(cio::ok()), cio::Result<void>>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Result<void>&>().value()),
              void>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Error&>().raw()), int>);
static_assert(std::same_as<
              decltype(std::declval<const cio::Error&>().message()),
              std::string>);
static_assert(std::same_as<
              decltype(std::declval<const cio::SystemError&>().error()),
              cio::Error>);

cio::Task<int> lazy_value(bool* started, int value) {
    *started = true;
    co_return value;
}

cio::Task<int> await_join(cio::JoinHandle<int> handle) {
    co_return co_await handle;
}

cio::Task<void> send_value(cio::Chan<int> channel, int value) {
    CIO_CHECK(co_await channel.send(value));
}

cio::Task<int> recv_value(cio::Chan<int> channel) {
    auto value = co_await channel.recv();
    CIO_CHECK(value.has_value());
    co_return value.value_or(0);
}

cio::Task<int> exercise_concurrency_surface() {
    auto closed = cio::make_chan<int>(2);
    CIO_CHECK(closed.try_send(7));
    closed.close();

    auto receive = cio::select(cio::recv(closed), cio::otherwise());
    CIO_CHECK_EQ(co_await receive, std::size_t{0});
    const auto buffered = receive.get<0>();
    CIO_CHECK(buffered.has_value());
    CIO_CHECK_EQ(buffered.value_or(0), 7);
    CIO_CHECK(!(co_await closed.recv()).has_value());
    CIO_CHECK(!(co_await closed.send(8)));

    auto sent = cio::make_chan<int>(1);
    auto send = cio::select(cio::send(sent, 11), cio::otherwise());
    CIO_CHECK_EQ(co_await send, std::size_t{0});
    CIO_CHECK(send.get<0>());
    CIO_CHECK_EQ(sent.try_recv().value_or(0), 11);

    auto joined = cio::spawn([]() -> cio::Task<int> { co_return 5; }());
    const int joined_value = co_await joined;

    auto detached_value = cio::make_chan<int>(1);
    cio::go(send_value(detached_value, 6));
    const int detached = (co_await detached_value.recv()).value_or(0);

    auto grouped_values = cio::make_chan<int>(2);
    cio::TaskGroup group;
    group.spawn(send_value(grouped_values, 20));
    group.spawn(send_value(grouped_values, 22));
    co_await group.join();
    const int grouped =
        grouped_values.try_recv().value_or(0) +
        grouped_values.try_recv().value_or(0);

    cio::CancelSource cancellation;
    const cio::CancelToken token = cancellation.token();
    CIO_CHECK(static_cast<bool>(token));
    CIO_CHECK(!token.cancelled());
    CIO_CHECK(!token.done().is_closed());
    cancellation.cancel();
    CIO_CHECK(cancellation.cancelled());
    CIO_CHECK(token.cancelled());
    CIO_CHECK(token.done().is_closed());

    auto cancelled = cio::select(cio::recv(token.done()), cio::otherwise());
    CIO_CHECK_EQ(co_await cancelled, std::size_t{0});
    CIO_CHECK(!cancelled.get<0>().has_value());

    co_await cio::sleep(0ns);
    co_await cio::sleep_until(cio::Clock::now());

    co_return joined_value + detached + grouped;
}

// This is intentionally compile-only: it represents downstream socket code
// without performing network I/O in the API fixture.
[[maybe_unused]] cio::Task<void> downstream_net_usage(
    cio::net::TcpStream& stream, cio::net::TcpListener& listener,
    cio::net::UdpSocket& datagram, cio::net::SocketAddr peer,
    MutableBytes input, ConstBytes output) {
    stream.set_read_timeout(1s);
    stream.set_write_deadline(cio::Clock::now() + 1s);
    const cio::Result<void> writable = co_await stream.writable();
    if (writable) {
        const auto written = co_await stream.write(output);
        (void)written;
    }
    const auto read = co_await stream.read(input);
    (void)read;
    stream.clear_read_deadline();
    stream.clear_write_deadline();

    const auto accepted = co_await listener.accept();
    (void)accepted;
    listener.set_deadline(cio::Clock::now() + 1s);
    listener.clear_deadline();

    cio::net::SocketAddr from;
    const auto received = co_await datagram.recv_from(input, from);
    const auto sent = co_await datagram.send_to(output, peer);
    (void)received;
    (void)sent;
}

void test_task_laziness_and_runtime_options() {
    cio::RuntimeOptions defaults;
    CIO_CHECK_EQ(defaults.worker_threads, std::size_t{0});
    CIO_CHECK_EQ(defaults.max_blocking_threads, std::size_t{512});
    CIO_CHECK_EQ(defaults.max_blocking_queue, std::size_t{1024});

    bool started = false;
    auto task = lazy_value(&started, 42);
    CIO_CHECK(!started);
    CIO_CHECK(task.valid());
    CIO_CHECK(!task.done());

    cio::RuntimeOptions options;
    options.worker_threads = 1;
    options.max_blocking_threads = 2;
    options.max_blocking_queue = 8;
    CIO_CHECK_EQ(cio::run(std::move(task), options), 42);
    CIO_CHECK(started);
}

void test_runtime_and_concurrency_usage() {
    cio::RuntimeOptions options;
    options.worker_threads = 1;
    options.max_blocking_threads = 2;
    cio::Runtime runtime(options);
    CIO_CHECK_EQ(runtime.worker_count(), std::size_t{1});

    bool member_spawn_started = false;
    auto member_handle = runtime.spawn(lazy_value(&member_spawn_started, 9));
    CIO_CHECK_EQ(runtime.block_on(await_join(std::move(member_handle))), 9);
    CIO_CHECK(member_spawn_started);

    auto channel = cio::make_chan<int>(1);
    runtime.go(send_value(channel, 8));
    CIO_CHECK_EQ(runtime.block_on(recv_value(channel)), 8);

    CIO_CHECK_EQ(runtime.block_on(exercise_concurrency_surface()), 53);
    runtime.shutdown();
}

void test_net_and_result_construction() {
    const auto parsed = cio::net::SocketAddr::parse("127.0.0.1", 8080);
    CIO_CHECK(parsed.has_value());
    CIO_CHECK(parsed->valid());
    CIO_CHECK_EQ(parsed->port(), std::uint16_t{8080});

    auto connect_task =
        cio::net::TcpStream::connect(cio::net::SocketAddr::loopback_v4(9));
    CIO_CHECK(connect_task.valid());
    CIO_CHECK(!connect_task.done());

    auto resolve_task = cio::net::resolve("localhost", 80);
    CIO_CHECK(resolve_task.valid());
    CIO_CHECK(!resolve_task.done());

    cio::Result<int> value = 17;
    CIO_CHECK(value.has_value());
    CIO_CHECK_EQ(value.value(), 17);
    CIO_CHECK_EQ(value.value_or(3), 17);

    const cio::Result<int> error = cio::Errc::closed;
    CIO_CHECK(!error.has_value());
    CIO_CHECK(error.error().is(cio::Errc::closed));
    CIO_CHECK_EQ(error.value_or(3), 3);

    const cio::Error overloaded = cio::Errc::overloaded;
    CIO_CHECK(overloaded.is(cio::Errc::overloaded));
    CIO_CHECK_EQ(overloaded.message(), std::string{"runtime overloaded"});

    const cio::Result<void> success = cio::ok();
    CIO_CHECK(success.has_value());
    success.value();
}

}  // namespace

int main() {
    RUN_TEST(test_task_laziness_and_runtime_options);
    RUN_TEST(test_runtime_and_concurrency_usage);
    RUN_TEST(test_net_and_result_construction);
    return cio_test::summary();
}
