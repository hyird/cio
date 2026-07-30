// Isolates the per-operation cost of the socket read path.
//
// The question this answers: `co_await stream.read(buf)` returns a
// Task<Result<n>>, which is a coroutine frame per call unless the compiler
// elides it (HALO). This measures the frame against the raw syscall, and counts
// allocations directly rather than guessing.
//
//     ./bench_io [rounds]
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>
#include <vector>

#include "cio/cio.hpp"

namespace {

std::atomic<long> g_allocations{0};

}  // namespace

// Counting allocator. Only valid because this is a benchmark binary; it makes
// "did the frame get elided" a measurement instead of an assumption.
void* operator new(std::size_t size) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
void* operator new[](std::size_t size) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    void* p = std::aligned_alloc(static_cast<std::size_t>(alignment), size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    void* p = std::aligned_alloc(static_cast<std::size_t>(alignment), size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

namespace net = cio::net;

namespace {

constexpr std::size_t kChunk = 128;
constexpr std::size_t kBlobChunks = 256;
constexpr std::size_t kBlob = kChunk * kBlobChunks;  // 32 KiB per round

struct Sample {
    const char* name;
    double ns_per_op;
    double allocs_per_op;
};

std::vector<Sample> samples;

// Refills the reader's socket buffer on request, so the measured loop only ever
// hits the already-has-data path — which is what a busy server actually does.
cio::Task<> feeder(net::TcpConn writer, cio::Chan<int> request, cio::Chan<int> ack) {
    std::vector<std::byte> blob(kBlob, std::byte{0x41});
    while (auto req = co_await request.recv()) {
        if (!(co_await writer.write(blob))) break;
        co_await ack.send(1);
    }
}

cio::Task<> measure_awaited_read(net::TcpConn& reader, cio::Chan<int> request,
                                 cio::Chan<int> ack, long rounds) {
    std::byte buffer[kChunk];
    long operations = 0;
    std::int64_t elapsed_ns = 0;
    const long allocations_before = g_allocations.load(std::memory_order_relaxed);

    for (long r = 0; r < rounds; ++r) {
        co_await request.send(1);
        co_await ack.recv();

        const auto started = cio::Clock::now();
        std::size_t consumed = 0;
        while (consumed < kBlob) {
            auto n = co_await reader.read(buffer);
            if (!n || *n == 0) co_return;
            consumed += *n;
            ++operations;
        }
        elapsed_ns += cio::to_ns(cio::Clock::now() - started);
    }

    const long allocations = g_allocations.load(std::memory_order_relaxed) - allocations_before;
    samples.push_back({"co_await stream.read()",
                       static_cast<double>(elapsed_ns) / static_cast<double>(operations),
                       static_cast<double>(allocations) / static_cast<double>(operations)});
}

cio::Task<> measure_try_read(net::TcpConn& reader, cio::Chan<int> request, cio::Chan<int> ack,
                             long rounds) {
    std::byte buffer[kChunk];
    long operations = 0;
    std::int64_t elapsed_ns = 0;
    const long allocations_before = g_allocations.load(std::memory_order_relaxed);

    for (long r = 0; r < rounds; ++r) {
        co_await request.send(1);
        co_await ack.recv();

        const auto started = cio::Clock::now();
        std::size_t consumed = 0;
        while (consumed < kBlob) {
            auto n = reader.try_read(buffer);
            if (!n) {
                // Should not happen: the feeder guarantees the data is there.
                if (auto ready = co_await reader.readable(); !ready) co_return;
                continue;
            }
            if (*n == 0) co_return;
            consumed += *n;
            ++operations;
        }
        elapsed_ns += cio::to_ns(cio::Clock::now() - started);
    }

    const long allocations = g_allocations.load(std::memory_order_relaxed) - allocations_before;
    samples.push_back({"stream.try_read() (raw syscall)",
                       static_cast<double>(elapsed_ns) / static_cast<double>(operations),
                       static_cast<double>(allocations) / static_cast<double>(operations)});
}

// The allocation pattern a fan-out workload creates: frames are allocated by
// one task and freed by whichever worker ends up running them. A purely
// thread-local free list cannot recycle anything here — the allocating thread
// never frees, so its cache stays empty — which is exactly what the central
// list exists to fix.
cio::Task<> measure_spawn_allocations(long count) {
    cio::WaitGroup group;
    group.add(count);

    const long before = g_allocations.load(std::memory_order_relaxed);
    const auto started = cio::Clock::now();
    for (long i = 0; i < count; ++i) {
        cio::go([](cio::WaitGroup& wg) -> cio::Task<> {
            wg.done();
            co_return;
        }(group));
    }
    co_await group.wait();
    const auto elapsed = cio::Clock::now() - started;
    const long allocations = g_allocations.load(std::memory_order_relaxed) - before;

    samples.push_back({"go() spawn, frames freed cross-thread",
                       static_cast<double>(cio::to_ns(elapsed)) / static_cast<double>(count),
                       static_cast<double>(allocations) / static_cast<double>(count)});
}

// spawn() costs a shared completion state on top of the frames. This checks
// whether that allocation is also being recycled, rather than trusting a timing
// difference that sits inside the run-to-run spread.
cio::Task<> measure_spawn_join_allocations(long count) {
    std::vector<cio::JoinHandle<>> handles;
    handles.reserve(static_cast<std::size_t>(count));

    const long before = g_allocations.load(std::memory_order_relaxed);
    const auto started = cio::Clock::now();
    for (long i = 0; i < count; ++i) {
        handles.push_back(cio::spawn([]() -> cio::Task<> { co_return; }()));
    }
    for (auto& handle : handles) co_await handle;
    const auto elapsed = cio::Clock::now() - started;
    const long allocations = g_allocations.load(std::memory_order_relaxed) - before;

    samples.push_back({"spawn() x N then join all",
                       static_cast<double>(cio::to_ns(elapsed)) / static_cast<double>(count),
                       static_cast<double>(allocations) / static_cast<double>(count)});
}

// The same work with the handle joined before the next spawn. The contrast with
// the batch above is the point: holding N handles live means N join states are
// live, so an allocation per spawn is inherent to that shape, not an allocator
// failure. Recycling only has something to recycle when the state is released.
cio::Task<> measure_sequential_spawn_join(long count) {
    const long before = g_allocations.load(std::memory_order_relaxed);
    const auto started = cio::Clock::now();
    for (long i = 0; i < count; ++i) {
        auto handle = cio::spawn([]() -> cio::Task<> { co_return; }());
        co_await handle;
    }
    const auto elapsed = cio::Clock::now() - started;
    const long allocations = g_allocations.load(std::memory_order_relaxed) - before;

    samples.push_back({"spawn() + join, one at a time",
                       static_cast<double>(cio::to_ns(elapsed)) / static_cast<double>(count),
                       static_cast<double>(allocations) / static_cast<double>(count)});
}

cio::Task<int> run(long rounds) {
    auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
    if (!listener) co_return 1;
    const auto addr = listener->addr().value();

    auto accepted = cio::spawn([](net::TcpListener l) -> cio::Task<net::TcpConn> {
        auto conn = co_await l.accept();
        co_return std::move(conn.value());
    }(std::move(*listener)));

    auto client = co_await net::TcpConn::dial(addr);
    if (!client) co_return 1;
    auto server = co_await accepted;

    auto request = cio::make_chan<int>(1);
    auto ack = cio::make_chan<int>(1);
    cio::go(feeder(std::move(server), request, ack));

    // Two alternating passes. A single ordering conflates the thing being
    // measured with warm-up, page faults and frequency ramp; if the two passes
    // disagree, the ordering is what is being measured.
    for (int pass = 0; pass < 2; ++pass) {
        co_await measure_try_read(*client, request, ack, rounds);
        co_await measure_awaited_read(*client, request, ack, rounds);
    }

    request.close();

    co_await measure_spawn_allocations(500'000);
    co_await measure_spawn_join_allocations(200'000);
    co_await measure_sequential_spawn_join(200'000);

    std::printf("%-38s %12s %14s\n", "path", "ns/op", "allocs/op");
    std::printf("%-38s %12s %14s\n", "--------------------------------------", "------------",
                "--------------");
    for (const auto& s : samples) {
        std::printf("%-38s %12.1f %14.3f\n", s.name, s.ns_per_op, s.allocs_per_op);
    }
    co_return 0;
}

}  // namespace

CIO_MAIN_ARGS(argc, argv) {
    const long rounds = argc > 1 ? std::atol(argv[1]) : 400;
    std::printf("cio read-path benchmark — %zu byte reads, %ld rounds x %zu ops\n\n", kChunk,
                rounds, kBlobChunks);
    co_return co_await run(rounds);
}
