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

Fourteen test executables cover the public API, scheduler, worker bitmaps,
directed MPSC inbox, channels, `select`, networking, DNS, files, signals,
scoped deadlines and descriptor adoption, synchronization, timers and soak
behaviour. The optional TLS module adds a fifteenth:

```sh
cmake -S . -B build-tls -DCMAKE_BUILD_TYPE=Release -DCIO_TLS=ON
cmake --build build-tls -j
ctest --test-dir build-tls --output-on-failure
```

For a longer non-sanitized network soak:

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
| `cio::net::Resolver` / `resolve()` | System name resolution on the blocking pool |
| `cio::dns::Resolver` | Built-in DNS backend; selected via `prefer_builtin` |
| `cio::Timeout` | Scoped, nestable deadline that restores the enclosing one |
| `cio::PollableFd` | Adopt a foreign fd (eventfd, timerfd, inotify, a C library) |
| `cio::net::Dialer` / `dial_tcp()` | Resolution plus raced address selection and timeouts |
| `cio::fs::File` / `open()` / `stat()` | Regular-file I/O on the blocking pool |
| `cio::signal::SignalSet` | `signalfd`-backed signal delivery |
| `cio::tls::TlsStream` | Optional TLS (`-DCIO_TLS=ON`, links OpenSSL) |
| `cio::read_exact` / `write_all` / `copy` | Generic algorithms over `AsyncReader`/`AsyncWriter` |
| `cio::Runtime` / `cio::run(task)` / `CIO_MAIN` | Runtime ownership and entry points |

Receiving from a closed and drained channel returns `std::nullopt`. Sending to a
closed channel returns `false`. `select` returns the winning case index and case
values remain available through `selected.get<I>()`.

Socket deadlines are per-direction and persist until reset; the unsuffixed
`set_deadline()`, `set_timeout()` and `clear_deadline()` apply to both
directions at once. `cio::Timeout` applies one for a scope and restores the
enclosing deadline afterwards, so timeouts nest without manual save/restore;
an inner scope can only tighten an outer one, never extend it.

Cancellation binds to a socket rather than to a call: `set_cancel(token)` makes
every operation in either direction fail with `Errc::cancelled` once the token
fires, including one already parked, which is woken. That is why `read()`,
`write()` and `accept()` take no cancel parameter — like deadlines,
cancellation lives on the connection. `TcpStream::connect()` and the resolver
and dialer entry points additionally accept a token directly.

`net::Resolver` is the one entry point for name resolution and picks its
backend with `LookupOptions::prefer_builtin`, mirroring Go's
`Resolver.PreferGo`; `Dialer` selects the same way through
`DialOptions::prefer_builtin_resolver`.

The default backend calls `getaddrinfo()` on the blocking pool: it honours
every NSS module, but occupies a pool thread and cannot be interrupted once
started, so a cancelled lookup resumes its caller while the call finishes in
the background. The built-in backend speaks DNS over the runtime's own sockets
and reads `/etc/hosts`: a lookup is cancellable mid-flight and costs no pool
thread, but it does not consult NSS, so LDAP, NIS and mDNS are invisible to it.
Go makes the same split for the same reason, and defaults the other way; cio
keeps the system backend as the default because a minor release should not
silently change how names resolve.

`cio::blocking(fn)` uses a lazily grown thread pool. `RuntimeOptions` bounds
its worker count (`max_blocking_threads`, default 512), its FIFO wait queue
(`max_blocking_queue`, default 1024), and the built-in operation classes
(`max_file_operations`, default 32; `max_resolver_operations`, default 8).
Class limits bound concurrently admitted operations, not threads: a task
waiting for admission is parked and occupies no pool thread, and each class has
its own wait queue so a burst of file work cannot sit in front of name
resolution. A submission to a full queue throws
`cio::SystemError` carrying `Errc::overloaded`. The same error is returned if
the pool has no service thread and the operating system refuses to create its
first one; a rejected callable is never run.

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

The detailed invariants live in [the runtime v2 design](docs/scheduler-v2.md);
the frozen gates and every rejected alternative are in
[the benchmark record](docs/scheduler-results.md).

## Repository layout

| Path | Contents |
|---|---|
| `include/cio/` | Public headers |
| `include/cio/detail/` | Internal scheduler, reactor, timer and queue contracts |
| `src/` | Runtime implementation |
| `tests/` | Unit, concurrency, API-surface and soak tests |
| `examples/` | Small buildable programs |
| `bench/` | Core, I/O, echo, Go and HTTP/`wrk` benchmarks |
| `docs/` | Design, benchmark record and roadmap; start at [docs/README.md](docs/README.md) |
| `include/cio/tls.hpp` | Optional TLS module; built only with `-DCIO_TLS=ON` |
| `CHANGELOG.md` | Released versions and their known limitations |
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
taskset -c 23 python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline-server path/to/candidate-server \
  --cells 1:1,8:4,64:14,256:14,1024:14 \
  --pairs 10 --warmup 5 --duration 15 \
  --server-cores 0-7 --client-cores 8-21 \
  --tail-script bench/http-comparison/wrk_tail.lua \
  --expected-a-sha256 <sha256> \
  --expected-b-sha256 <sha256> \
  --expected-wrk-sha256 <sha256> \
  --expected-tail-script-sha256 <sha256>
```

Frozen local binaries are generated artifacts and are not committed. Record
their hashes and the `wrk` hash with every result.

### Measured throughput

A minimal HTTP/1.1 server on eight workers pinned to CPUs 0-7, driven by
third-party `wrk` on CPUs 8-23. All five cells come from one ten-pair matrix
against the published v0.0.1 runtime, so they are directly comparable:

| Connections | req/s | median p50 | median p99 |
|---:|---:|---:|---:|
| 1 | 14,339 | 68 us | 110 us |
| 8 | 125,598 | 52 us | 644 us |
| 64 | 789,141 | 70 us | 501 us |
| 256 | 771,416 | 306 us | 2765 us |
| 1024 | 781,518 | 1125 us | 4635 us |

A later monitor-balance round measured c1024 at 859,820 req/s with median p99
of 2060 us. That is a separate confirmation against a different baseline and is
not paired with the table above; do not read the two together as one sweep.

Absolute numbers are host-specific and are useful mainly as a shape: throughput
saturates near 64 connections, and tail latency is the axis that moves.

What this scheduler costs, stated rather than omitted: detached `go()` is about
6.5% slower than the shared-reactor design it replaced, p99 at 8 and 256
connections is several times higher than that design's, and 1024-connection
throughput parity for the final work-aware build was not demonstrated. See
[the benchmark record](docs/scheduler-results.md) for baselines, intervals,
CPU data and artifact hashes, and [the roadmap](docs/roadmap.md) for what is
still open.

Comparisons against Boost.Asio and Go, with their methodology and caveats:

- [HTTP comparison driven by wrk](bench/http-comparison/README.md)
- [Echo comparison](bench/echo-comparison/README.md)
- [Go core benchmark](bench/go-core/README.md)

Optional runtime counters are enabled with `-DCIO_METRICS=ON`.
`cio::runtime_metrics()` is always linkable and returns zero-valued counters
when instrumentation is disabled.

## Known limits

Runtime:

- Linux/epoll only; no kqueue, IOCP or io_uring backend is claimed.
- Scheduling is cooperative. A task that never suspends holds its worker.
- Cancellation is cooperative and is observed only where the task checks it.
- A blocking callable that has started cannot be preempted. Runtime shutdown
  waits for started and already-queued blocking work to finish.
- Runtime shutdown does not unwind tasks still parked on channels or sockets.
  Calling `Runtime::shutdown()` from one of its own workers throws
  `std::logic_error`.
- A socket object must outlive tasks using it. At most one reader and one writer
  may wait concurrently on a socket.
- Symmetric coroutine transfer relies on tail calls. CMake propagates GCC's
  required `-foptimize-sibling-calls`; sanitizer instrumentation can still turn
  long non-suspending coroutine chains into deep native stacks.
- `cio::blocking()` itself has no admission limit; only the built-in file and
  resolver classes are bounded. A flood of user blocking work is held only by
  the global queue bound and the thread ceiling.

Performance, measured rather than asserted:

- 64-connection tail latency regressed against the previous release: p99 up
  26-57% and Max up 14-31%, reproducing in both AB and BA order. It is the price
  of the 1024-connection gains.
- The p99.99 and beyond are worse on rare foreign-monitor dispatch. Four designs
  aimed at that path were measured and all four rejected.
- Accepted connections are distributed round-robin, but weight is a property of
  the traffic a connection later carries, so heavy connections cluster by chance
  and skewed workloads land unevenly across reactor shards.

Files:

- No cancellation and no deadlines, by design: a cancelled read would let a pool
  thread keep writing into a caller-owned span after it was destroyed.
- `read()` and `write()` share the file offset and must not run concurrently on
  the same `File`; `read_at()`/`write_at()` may, with distinct buffers.
- `close()` is synchronous. On a filesystem where it can block for an unbounded
  time, call it from `cio::blocking()`.
- No directory traversal, path mutation or whole-file helpers yet.

Name resolution:

- `net::Resolver` uses the system resolver. A cancelled lookup resumes its
  caller immediately, but `getaddrinfo()` cannot be interrupted and runs to
  completion; its late result is discarded.
- The built-in backend reads `/etc/hosts` but not NSS, so LDAP, NIS and mDNS
  are invisible to it. It also has no cache, no DNSSEC validation, no TCP
  fallback — a truncated answer with no usable records is reported rather than
  retried over TCP — and does not implement the `resolv.conf` search list or
  `ndots`, so names are queried as given.

Signals:

- `cio::signal::block()` must be called before the runtime starts any thread.
  `subscribe()` reports `Errc::broken` for a signal that is not blocked, rather
  than returning a set that could never fire.
- Identical signals arriving faster than they are consumed may be coalesced by
  the kernel. Use it for lifecycle events, not as a counter.

TLS (optional):

- Requires `-DCIO_TLS=ON` and links OpenSSL; the core library stays
  dependency-free.
- TLS 1.2 is the floor. No ALPN, no session resumption and no client
  certificates.

Open items that are not inherent — including work blocked on hardware or on
evidence — are tracked in [the roadmap](docs/roadmap.md).

## Development

Read [AGENTS.md](AGENTS.md) before changing runtime ownership, waiter lifetime,
shutdown or benchmark methodology. Public API and observable semantics should
remain stable unless an API change is explicitly requested and documented.

[docs/README.md](docs/README.md) indexes the design, the benchmark record and
the roadmap. Before proposing a scheduler mechanism, check the
[rejected designs](docs/scheduler-results.md#rejected-designs): more than
twenty implemented variants have already been measured and removed.
