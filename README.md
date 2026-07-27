# cio

Goroutine-style concurrency for C++20 stackless coroutines.

cio provides lazy tasks, detached and joinable spawning, channels, `select`,
structured task groups, cancellation, synchronization primitives, timers,
non-blocking sockets and a blocking pool. Applications use coroutine APIs while
the runtime manages M:N scheduling, worker-local epoll shards, timer shards and
conditional work stealing.

Linux and C++20 are required. The core library has no external dependencies.

## Quick start

```cpp
#include <cio/cio.hpp>

cio::Task<> worker(cio::Chan<int> jobs, cio::Chan<int> out,
                   cio::CancelToken quit) {
    for (;;) {
        auto selected =
            cio::select(cio::recv(jobs), cio::recv(quit.done()));
        if (co_await selected == 1) co_return;

        auto job = selected.get<0>();
        if (!job) co_return;
        if (!(co_await out.send(*job * 2))) co_return;
    }
}

CIO_MAIN {
    auto jobs = cio::make_chan<int>(64);
    auto out = cio::make_chan<int>(64);
    cio::CancelSource stop;

    cio::TaskGroup workers;
    for (int i = 0; i < 4; ++i) {
        workers.spawn(worker(jobs, out, stop.token()));
    }

    for (int value = 1; value <= 100; ++value) {
        co_await jobs.send(value);
    }
    jobs.close();

    int total = 0;
    for (int i = 0; i < 100; ++i) {
        total += *co_await out.recv();
    }
    co_await workers.join();
    co_return total == 10'100 ? 0 : 1;
}
```

`CIO_MAIN` keeps the user-written body asynchronous while emitting the real
non-coroutine `main` required by the C++ standard. Use `cio::Runtime` directly
when worker count or runtime ownership must be configured explicitly.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The project is tested with GCC 13.3 and Clang 19 on Linux 6.12. CMake 3.20 or
newer is required.

Sanitizer configurations are first-class CMake builds:

```sh
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=asan
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=tsan
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

Ten test executables cover the public API, scheduler, worker bitmaps, directed
MPSC inbox, channels, `select`, networking, synchronization, timers and soak
behaviour. For a longer non-sanitized network soak:

```sh
./build/test_soak 90
```

Long coroutine soaks should not run under ASan or TSan; see
[Known limits](#known-limits).

## Public API

| API | Purpose |
|---|---|
| `cio::Task<T>` | Lazy, single-consumer coroutine |
| `cio::go(task)` | Fire-and-forget task |
| `cio::spawn(task)` | Joinable task returning `JoinHandle<T>` |
| `cio::yield()` | Yield the current worker |
| `cio::Chan<T>` / `make_chan<T>(n)` | Mutex-protected MPMC channel; `n == 0` is rendezvous |
| `cio::select(...)` | Receive, send, timeout and default cases |
| `cio::TaskGroup` | Structured child-task scope |
| `CancelSource` / `CancelToken` | Cooperative cancellation |
| `WaitGroup` / `Mutex` | Task-suspending synchronization |
| `cio::sleep(duration)` | Runtime timer |
| `cio::blocking(fn)` | Run blocking work outside scheduler workers |
| `cio::net::TcpListener` / `TcpStream` / `UdpSocket` | Non-blocking sockets with deadlines |
| `cio::Runtime` / `cio::run(task)` / `CIO_MAIN` | Runtime ownership and entry points |

Receiving from a closed and drained channel returns `std::nullopt`. Sending to a
closed channel returns `false`. `select` returns the winning case index and case
values remain available through `selected.get<I>()`.

## Runtime architecture

Each runtime worker owns:

- a single-slot `runnext` handoff;
- an owner-produced local FIFO whose published tail can be stolen;
- a bounded 256-entry MPSC `RemoteInbox` for hard-directed internal work;
- one edge-triggered epoll instance and eventfd;
- one 4-ary timer heap.

The MPSC inbox is deliberately not a general runnable queue. Only submissions
with a concrete internal ownership target enter it, and only the target worker
consumes it. Ordinary foreign submissions, soft-affinity completions and inbox
overflow use a shared mutex-protected fallback so an arbitrary busy worker
cannot strand them. Public `cio::Chan<T>` remains MPMC and is unrelated to the
scheduler inbox.

Workers publish idle and stealable state in scalable bitmaps. Thieves inspect
only advertised victims, while an epoch handshake closes concurrent
publish/clear races. Sticky searcher credit preserves load redistribution when
I/O or another runnable intercepts a newly woken worker.

Accepted sockets receive a stable home reactor shard. Descriptor generations,
lifecycle pins and syscall leases make stale epoll events safe while close,
deadline and cancellation race. Delayed cross-runtime completions use stable
process-lifetime endpoint identities with counted foreign leases.

The detailed invariants, rejected alternatives and frozen release gates live in
[the runtime v2 design](docs/scheduler-v2.md).

## Repository layout

| Path | Contents |
|---|---|
| `include/cio/` | Public headers |
| `include/cio/detail/` | Internal scheduler, reactor, timer and queue contracts |
| `src/` | Runtime implementation |
| `tests/` | Unit, concurrency, API-surface and soak tests |
| `examples/` | Small buildable programs |
| `bench/` | Core, I/O, echo, Go and HTTP/`wrk` benchmarks |
| `docs/scheduler-v2.md` | Implemented runtime-v2 design and final gates |
| `docs/io-infrastructure.md` | Later additive I/O design; not current API |
| `AGENTS.md` | Repository-specific development and verification rules |

Build directories, sanitizer artifacts, benchmark binaries, Python caches and
raw local result directories are generated locally and must remain untracked.

## Benchmarks

Build the C++ microbenchmarks with the normal Release configuration:

```sh
./build/bench_core 24
./build/bench_io
```

The matching Go core workloads are isolated in their own module:

```sh
cd bench/go-core
go test ./...
go build -o go_core .
taskset -c 0-23 ./go_core -gomaxprocs=24 -warmup=1 -repeat=5
```

HTTP before/after measurements use the same third-party `wrk` executable for
both sides, disjoint server/client CPU sets, warm-up, alternating AB/BA pairs,
input SHA-256 checks, error rejection and retained raw logs:

```sh
python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline-server path/to/candidate-server \
  --cells 1:1,8:2,64:8,256:8,1024:8 \
  --pairs 10 --warmup 5 --duration 15
```

Frozen local binaries are generated artifacts and are not committed. Record
their hashes and the `wrk` hash with every result.

### Runtime v2 frozen result

The retained v2 runtime was compared with the exact pre-v2 baseline using
warmed, pinned and alternating pairs:

| Workload | Pre-v2 | Runtime v2 | Paired result |
|---|---:|---:|---:|
| HTTP/`wrk`, 1 connection | 14,209 req/s | 13,870 req/s | -2.39%, neutral |
| HTTP/`wrk`, 64 connections | 634,737 req/s | 783,776 req/s | **+23.48%** |
| Echo, 1024 connections, 128 B | 725,717 req/s | 779,580 req/s | **+7.43%** |
| `spawn()` + join, 8 workers | — | — | **16.86% faster** |
| `spawn()` + join, 24 workers | — | — | **19.76% faster** |
| Unbuffered channel, 24 workers | — | — | neutral, B/A ns/op +0.095% |

Detached `go()` remained 6.52% slower at 8 workers and 6.71% slower at 24.
Saturated network throughput also traded some CPU and p99 latency for higher
capacity. These costs are part of the release record rather than being hidden
behind headline throughput.

An older pre-v2 `wrk` comparison against Go is retained only as historical
context:

| Connections | cio snapshot | Go | cio/Go |
|---:|---:|---:|---:|
| 8 | 75,483 | 72,714 | +3.8% |
| 64 | 636,751 | 629,971 | +1.1% |
| 256 | 770,606 | 716,889 | +7.5% |
| 1024 | 770,628 | 721,637 | +6.8% |

This Go table was not collected in the final v2 A/B run and must not be combined
with it as if all columns were paired. Full methodology and historical results:

- [HTTP comparison driven by wrk](bench/http-comparison/README.md)
- [Echo comparison](bench/echo-comparison/README.md)
- [Go core benchmark](bench/go-core/README.md)

Optional runtime counters are enabled with `-DCIO_METRICS=ON`.
`cio::runtime_metrics()` is always linkable and returns zero-valued counters
when instrumentation is disabled.

## Known limits

- Linux/epoll only; no kqueue, IOCP or io_uring backend is claimed.
- Scheduling is cooperative. A task that never suspends holds its worker.
- Cancellation is cooperative and is observed only where the task checks it.
- Runtime shutdown does not unwind tasks still parked on channels or sockets.
  Calling `Runtime::shutdown()` from one of its own workers throws
  `std::logic_error`.
- A socket object must outlive tasks using it. At most one reader and one writer
  may wait concurrently on a socket.
- Symmetric coroutine transfer relies on tail calls. CMake propagates GCC's
  required `-foptimize-sibling-calls`; sanitizer instrumentation can still turn
  long non-suspending coroutine chains into deep native stacks.

## Development

Read [AGENTS.md](AGENTS.md) before changing runtime ownership, waiter lifetime,
shutdown or benchmark methodology. Public API and observable semantics should
remain stable unless an API change is explicitly requested and documented.
