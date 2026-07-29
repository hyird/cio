// Soak test: run the whole runtime under churn and check that nothing grows.
//
// The unit tests each exercise one mechanism in isolation. This one runs them
// against each other for a while — connections opening and closing under load,
// deadlines firing mid-read, tasks cancelled while parked, timers and channels
// racing — and then asserts the two things that a correct runtime must hold
// over time: file descriptors come back, and memory does not climb.
//
//     ./test_soak [seconds]        (default 3, so ctest stays fast)
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <span>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

// ASan's quarantine holds freed blocks, which inflates RSS by far more than the
// growth this test is looking for. LeakSanitizer still runs, and every other
// assertion here stays active.
#if defined(__SANITIZE_ADDRESS__)
constexpr bool kAddressSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr bool kAddressSanitized = true;
#else
constexpr bool kAddressSanitized = false;
#endif
#else
constexpr bool kAddressSanitized = false;
#endif

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

std::size_t open_fd_count() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) return 0;
    std::size_t count = 0;
    while (::readdir(dir) != nullptr) ++count;
    ::closedir(dir);
    return count;
}

std::size_t resident_kb() {
    std::FILE* file = std::fopen("/proc/self/status", "r");
    if (file == nullptr) return 0;
    char line[256];
    std::size_t kb = 0;
    while (std::fgets(line, sizeof(line), file) != nullptr) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) {
            kb = static_cast<std::size_t>(std::strtoul(line + 6, nullptr, 10));
            break;
        }
    }
    std::fclose(file);
    return kb;
}

std::uint32_t next_random(std::uint64_t& state) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return static_cast<std::uint32_t>(state >> 33);
}

std::atomic<long> g_echoed{0};
std::atomic<long> g_mismatched{0};
std::atomic<long> g_connect_failures{0};

cio::Task<> echo_connection(net::TcpConn stream) {
    std::byte buffer[2048];
    for (;;) {
        auto n = co_await stream.read(buffer);
        if (!n || *n == 0) break;
        if (auto written = co_await stream.write_all(std::span(buffer, *n)); !written) break;
    }
}

cio::Task<> echo_server(net::TcpListener listener, cio::CancelToken stop) {
    cio::TaskGroup connections;
    long accepted = 0;
    for (;;) {
        if (stop.cancelled()) break;
        // See test_net.cpp: a deadline is what makes a parked accept observe
        // cancellation without another task closing the socket under it.
        listener.set_deadline(cio::Clock::now() + 10ms);
        auto conn = co_await listener.accept();
        if (!conn) {
            if (conn.error().is(cio::Errc::timed_out)) continue;
            break;
        }
        connections.spawn(echo_connection(std::move(*conn)));

        // Break the symmetric-transfer chain every so often.
        //
        // A backlog drained without ever suspending is not a loop: awaiting
        // accept() tail-calls into it and it tail-calls back on return, so N
        // back-to-back accepts are N nested resumes. That is constant-stack
        // only because the compiler turns both transfers into jumps, which a
        // sanitizer build cannot do — and it is not, as the README used to
        // say, that ASan disables sibling calls. It does not; ordinary tail
        // calls survive it. What defeats the coroutine tail call is ASan's own
        // stack instrumentation, which has to run after the call returns.
        // Measured with a 100k-iteration `co_await leaf()` chain: 0 bytes of
        // growth at -O3, immediate overflow under ASan.
        //
        // So the depth this loop reaches is the length of an accept burst, and
        // once cio got fast enough to drain a few hundred at once, an 8 MB
        // stack ran out. Yielding suspends for real, which unwinds the chain
        // and bounds the depth at something no build can overflow. Every 16 is
        // far below the limit and far too rare to change what this test
        // measures, which is descriptor and RSS hygiene under churn.
        if ((++accepted & 15) == 0) co_await cio::yield();
    }
    co_await connections.join();
}

// One client's life: connect, do a few round trips of random size, sometimes
// arm a deadline that will expire, sometimes abandon the connection early.
cio::Task<> churn_client(net::SocketAddr target, std::uint64_t seed, cio::CancelToken stop) {
    std::uint64_t rng = seed | 1;

    while (!stop.cancelled()) {
        auto stream = co_await net::TcpConn::dial(target);
        if (!stream) {
            g_connect_failures.fetch_add(1, std::memory_order_relaxed);
            co_await cio::sleep(2ms);
            continue;
        }

        const int round_trips = 1 + static_cast<int>(next_random(rng) % 8);
        for (int i = 0; i < round_trips && !stop.cancelled(); ++i) {
            const std::size_t size = 1 + next_random(rng) % 1024;
            std::vector<std::byte> payload(size, static_cast<std::byte>(next_random(rng) & 0xFF));

            // Every so often, set a deadline short enough to actually fire, so
            // the deadline-vs-readiness race gets exercised rather than assumed.
            const bool tight_deadline = (next_random(rng) % 16) == 0;
            stream->set_read_timeout(tight_deadline ? 1ms : 5s);

            if (!(co_await stream->write_all(payload))) break;

            std::vector<std::byte> received(size);
            std::size_t got = 0;
            bool failed = false;
            while (got < size) {
                auto n = co_await stream->read(std::span(received).subspan(got));
                if (!n) {
                    // A timed-out read is an expected outcome here, not a bug;
                    // what matters is that the connection unwinds cleanly.
                    failed = true;
                    break;
                }
                if (*n == 0) {
                    failed = true;
                    break;
                }
                got += *n;
            }
            if (failed) break;

            if (std::memcmp(received.data(), payload.data(), size) == 0) {
                g_echoed.fetch_add(1, std::memory_order_relaxed);
            } else {
                g_mismatched.fetch_add(1, std::memory_order_relaxed);
            }
        }
        // Close explicitly rather than waiting for the destructor, so the
        // close-while-tasks-are-running path gets hit constantly.
        stream->close();
    }
}

// Background noise on the non-I/O machinery, running concurrently with the
// socket churn so timers, channels and select contend for the same workers.
cio::Task<> channel_noise(cio::CancelToken stop) {
    auto ch = cio::make_chan<int>(4);

    cio::go([](cio::Chan<int> out, cio::CancelToken quit) -> cio::Task<> {
        int i = 0;
        while (!quit.cancelled()) {
            auto sel = cio::select(cio::send(out, i++), cio::recv(quit.done()),
                                   cio::after(1ms));
            if (co_await sel == 1) break;
        }
        out.close();
    }(ch, stop));

    while (!stop.cancelled()) {
        auto sel = cio::select(cio::recv(ch), cio::recv(stop.done()), cio::after(2ms));
        if (co_await sel == 1) break;
    }
}

cio::Task<bool> soak(std::chrono::seconds duration) {
    auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
    CIO_CHECK(listener.has_value());
    if (!listener) co_return false;
    const auto addr = listener->local_addr().value();

    cio::CancelSource stop;
    auto server = cio::spawn(echo_server(std::move(*listener), stop.token()));

    constexpr int kClients = 48;
    constexpr int kNoiseTasks = 8;

    cio::TaskGroup workers;
    for (int i = 0; i < kClients; ++i) {
        workers.spawn(churn_client(addr, 0x9E3779B9ull * (i + 1), stop.token()));
    }
    for (int i = 0; i < kNoiseTasks; ++i) {
        workers.spawn(channel_noise(stop.token()));
    }

    // Let it warm up, then take the baseline: the first seconds allocate the
    // steady-state pools and thread caches, and counting those as growth would
    // make every run look like a leak.
    co_await cio::sleep(1s);
    const std::size_t fds_baseline = open_fd_count();
    const std::size_t rss_baseline = resident_kb();
    const long echoed_baseline = g_echoed.load();

    co_await cio::sleep(duration);

    const std::size_t fds_after = open_fd_count();
    const std::size_t rss_after = resident_kb();

    stop.cancel();
    co_await workers.join();
    co_await server;

    // Give the accept loop and any in-flight connections a moment to unwind.
    co_await cio::sleep(200ms);
    const std::size_t fds_settled = open_fd_count();

    const long echoed = g_echoed.load() - echoed_baseline;
    std::fprintf(stderr,
                 "  round trips: %ld, mismatches: %ld, connect failures: %ld\n"
                 "  fds: %zu -> %zu (settled %zu), rss: %zu KiB -> %zu KiB\n",
                 echoed, g_mismatched.load(), g_connect_failures.load(), fds_baseline,
                 fds_after, fds_settled, rss_baseline, rss_after);

    // Data must never come back wrong.
    CIO_CHECK_EQ(g_mismatched.load(), 0L);
    // The churn has to have actually happened, or the checks below prove nothing.
    CIO_CHECK(echoed > 100);

    // Descriptors are the sharpest leak signal: every connection registers one
    // with the reactor and must give it back. Some slack for connections still
    // in flight when we sampled.
    CIO_CHECK(fds_settled <= fds_baseline + 16);

    // Memory is noisier — allocator arenas and thread caches move around — so
    // this only has to catch unbounded growth, not track it precisely.
    if constexpr (!kAddressSanitized) {
        CIO_CHECK(rss_after <= rss_baseline + 65536);
    }

    co_return g_mismatched.load() == 0;
}

}  // namespace

int main(int argc, char** argv) {
    const int seconds = argc > 1 ? std::atoi(argv[1]) : 3;
    std::fprintf(stderr, "- soak (%d s)\n", seconds);
    cio::run(soak(std::chrono::seconds(seconds)));
    return cio_test::summary();
}
