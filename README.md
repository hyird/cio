# cio

Goroutine-style concurrency for C++20, on stackless coroutines.

The public API has no thread in it. You write tasks, channels, and `select`, and
the runtime handles M:N scheduling, work stealing, an edge-triggered epoll
reactor, sharded timer heaps and a blocking pool underneath.

```cpp
#include <cio/cio.hpp>

cio::Task<> worker(int id, cio::Chan<int> jobs, cio::Chan<int> out,
                   cio::CancelToken quit) {
    for (;;) {
        auto sel = cio::select(cio::recv(jobs), cio::recv(quit.done()));
        if (co_await sel == 1) co_return;          // cancelled
        auto job = sel.get<0>();
        if (!job) co_return;                       // channel closed
        co_await out.send(*job * 2);
    }
}

CIO_MAIN {
    auto jobs = cio::make_chan<int>(64);
    auto out  = cio::make_chan<int>(64);
    cio::CancelSource stop;

    cio::TaskGroup group;
    for (int i = 0; i < 4; ++i) group.spawn(worker(i, jobs, out, stop.token()));

    for (int i = 1; i <= 100; ++i) co_await jobs.send(i);
    jobs.close();

    int total = 0;
    for (int i = 0; i < 100; ++i) total += *co_await out.recv();
    co_await group.join();
    co_return 0;
}
```

`CIO_MAIN` makes the body of main a coroutine. The standard forbids `main`
itself from being one ([basic.start.main]: "The function main shall not be a
coroutine"), so the macro declares your body as a `cio::Task<int>` and emits the
real `main` that stands up a runtime and blocks on it. Use `cio::Runtime`
directly when you need to configure worker counts.

## Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cd build && ctest
```

Linux only, C++20. Tested with GCC 13.3 and Clang 19 on Linux 6.12. No external
dependencies.

## The public surface

| | |
|---|---|
| `cio::Task<T>` | a lazy coroutine; `co_await`ing one is a tail call, not a scheduling hop |
| `cio::go(task)` | fire and forget — `go f()` |
| `cio::spawn(task)` | spawn and keep a `JoinHandle<T>` you can `co_await` |
| `cio::yield()` | `runtime.Gosched()` |
| `cio::Chan<T>` | a channel; a cheap refcounted handle, passed by value like `chan int` |
| `cio::make_chan<T>(n)` | `n == 0` gives an unbuffered rendezvous channel |
| `cio::select(...)` | with `recv` / `send` / `after` / `otherwise` cases |
| `cio::sleep(d)` | |
| `cio::TaskGroup` | structured scope: join all children, first failure cancels the rest |
| `cio::CancelSource` / `CancelToken` | cancellation as a closable channel, like `context.Done()` |
| `cio::WaitGroup`, `cio::Mutex` | suspend the task, never the worker |
| `cio::blocking(fn)` | run `fn` on a real thread and resume with its result |
| `cio::net::TcpListener/TcpStream/UdpSocket` | with Go-style deadlines |
| `CIO_MAIN`, `cio::Runtime`, `cio::run(task)` | the one place threads are mentioned |

Channel receive returns `std::optional<T>` — `nullopt` means closed and drained,
so the Go range loop transcribes directly:

```cpp
while (auto job = co_await jobs.recv()) { ... }
```

Send returns `bool` rather than panicking on a closed channel. `select` returns
the winning index, and case results come from `sel.get<I>()`, so case bodies are
ordinary code in the enclosing coroutine and can `co_await` freely.

## Architecture

```
   cio::go / chan / select / net          <- no threads here
  ─────────────────────────────────────
   Scheduler        M:N workers, runnext + bounded ring + global queue,
                    steal-half, Go-style spinning/parking protocol
   Reactor          epoll, edge-triggered
   TimerService     per-worker 4-ary heaps, atomic earliest-deadline
   FramePool        size-classed thread caches + central list
   BlockingPool     lazily grown, retires on idle
```

**Scheduler.** One OS thread per worker, each with a single-slot LIFO `runnext`,
a 256-entry lock-free FIFO ring stealable in halves, and a shared mutex-guarded
global queue checked every 61 iterations for fairness. `runnext` is deliberately
*not* stealable — it exists so a producer/consumer pair stays pinned to one core,
and letting peers take it destroys exactly the locality it was added for. This is
safe because only the owning worker ever writes it, so it can never strand a task
under a parked worker.

**Wakeups.** Idle workers follow Go's spinning protocol, and `notify()` is gated
by a CAS that claims the right to create *the* searcher; a worker woken that way
inherits the searcher credit instead of re-entering the notify path. Without that
gate, a hot channel wakes and re-parks a thread on every message and the futex
round trip dwarfs the work being scheduled — measured at 23 µs per channel round
trip before the gate, 140 ns after.

**Reactor.** One edge-triggered epoll registration per fd for the life of the fd,
never rearmed. Per-direction readiness is a single atomic word with three states
(`idle` / `ready` / `waiter*`), following Go's netpoll: the race between "syscall
returned EAGAIN, about to park" and "readiness arrived" is resolved by a CAS,
never a lock. Descriptors come from a slab that never frees, with a generation
tag, so a stale event dequeued by another thread is always safe to dereference.

**Deadlines** live on the descriptor, not on the awaiter — Go's `SetReadDeadline`
model. That is not API mimicry: a timer that can fire concurrently with the
operation it is timing out must outlive the coroutine frame, and a descriptor in
the reactor's slab does while an awaiter in a frame does not.

**Timers.** Per-worker 4-ary min-heaps, so arming a deadline never touches a
shared lock. Each shard publishes its earliest deadline in an atomic, so the
worker parked in the reactor computes its timeout by reading N atomics rather
than locking N heaps. Waits use `epoll_pwait2`, so a 200 µs sleep is not rounded
up to a millisecond.

**Frame pool.** Coroutine frames are the runtime's most frequent allocation — one
per spawned task, one per socket read or write — so they go through a
size-classed allocator instead of malloc. A thread cache serves the common case
(a task allocates and frees its own frames) with no atomics; a central list with
batched transfer serves the fan-out case, where one task allocates every frame
and 24 workers free them, which a purely thread-local cache cannot recycle at
all. Measured: 1.008 → 0.000 allocations per socket read, and 1.000 → 0.008 per
cross-thread spawn.

**Watchdog.** A sysmon-style monitor thread polls the reactor and fires timers
when every worker is busy with CPU-bound work, so I/O latency does not degrade
under load.

### The lifetime rule everything depends on

Waking a task hands its frame to another thread, which may resume and destroy it
immediately. So: **a waker decides whether it owns a waiter while holding the
lock that waiter is queued under, must not touch it after releasing that lock
unless it won, and must schedule it last.**

That single rule is what lets `select` retract its unfired cases with nothing but
the channel lock — no refcounting on the wakeup path. The two places without a
shared lock get explicit handshakes instead: `select`'s timeout publishes through
a phase word so a case that fires mid-registration defers the resume back to the
setup code, and `TimerService::disarm()` does not return until a firing callback
has stopped touching the node.

The corollary is that `disarm()` must be called *unconditionally*. Guarding it
with `if (state == kArmed)` skips the wait for an in-flight callback, which lets
the caller recycle or re-arm the node underneath it — the callback's final state
write then lands on the node's next incarnation, and the same timer ends up
linked into a heap twice with a stale `heap_index`. That was a real crash, found
by the soak test at ~60 seconds and not by any unit test.

## Measurements

AMD EPYC 7402, 24 cores, Linux 6.12, GCC 13.3, `-O3`. `./build/bench_core [workers]`.
These are microbenchmarks on one machine; the multi-worker numbers vary ±20% run
to run because they depend on which workers happen to be parked.

| benchmark | 1 worker | 24 workers |
|---|---:|---:|
| `co_await` child task | 15.8 ns | 16 ns |
| `co_await cio::yield()` | 12.6 ns | 15–24 ns |
| `go()` detached spawn | 144 ns | 288 ns |
| `spawn()` + `co_await` join | 302 ns | 690 ns |
| unbuffered chan round trip | 100 ns | 130–200 ns |
| buffered chan round trip | 102 ns | 115–190 ns |
| chan throughput, 1p/1c | 34.5 ns/op | 36–76 ns/op |
| chan throughput, 8p/8c | — | 330–390 ns/op |
| `select`, a case ready | — | 130–185 ns |
| `select`, parks every round | — | 150–215 ns |
| timer arm + fire | — | 625 ns |

`./build/bench_io` isolates the socket read path against the raw syscall and
counts allocations directly:

| | ns/op | allocs/op |
|---|---:|---:|
| `try_read()` (raw `recv`) | 324 | 0.000 |
| `co_await stream.read()` | 339 | 0.000 |

So an awaited read costs ~15 ns over the syscall, all of it coroutine frame
setup rather than allocation. That is 4.5% of a 128-byte loopback read, which is
why `read`/`write` are still ordinary coroutines: removing the frame entirely
would mean running the retry syscall on the waking thread, and serialising every
completion through the poller is a worse trade than 15 ns.

`./build/bench_echo` — echo round trips over loopback. Run the server and client
as separate processes; the convenient in-process mode measures the load
generator as much as the server:

```
./bench_echo server 9100 12                    # one terminal
./bench_echo client 127.0.0.1 9100 256 2000 12 # another
./bench_echo                                   # in-process, for a quick check
```

| 256 connections × 2000 requests | round trips/sec | avg latency |
|---|---:|---:|
| in-process, 24 workers shared | 497k | 515 µs |
| split processes, 12 workers each | **596k** | 430 µs |

The in-process number is a lower bound on the server: the load generator is
competing for the same cores and the same runtime. The split-process run is the
one to quote, and even that is loopback — put the client on another machine to
measure anything about the network.

For reference, Go on comparable hardware lands around 300–400 ns for goroutine
spawn and 250–350 ns for an unbuffered channel round trip.

### Against other runtimes

`bench/echo-comparison/` runs the same 8-thread echo workload against cio,
Boost.Asio (shared-nothing: one `io_context` and one `SO_REUSEPORT` acceptor per
thread, in both callback and coroutine form) and Go, with the server pinned to
CPUs 0–7 and the load generator to 8–23. Round trips per second, 128-byte
payload:

| connections | cio | asio-callback | asio-coro | go |
|---:|---:|---:|---:|---:|
| 8 | 66,151 | 83,830 | 84,580 | 57,407 |
| 64 | 449,598 | 634,477 | 644,877 | 470,443 |
| 512 | 700,392 | 813,485 | 812,531 | 674,611 |

cio and Go land within a few percent of each other, which is the expected result
for two runtimes with the same architecture. Shared-nothing asio is 16-27%
ahead, and profiling says why: cio retires *fewer* instructions per request than
asio and spends 27% more cycles doing it (IPC 0.46 against 0.61). It is not
doing more work, it is stalling on coherence traffic over shared scheduler
state, which a shared-nothing design does not have. Two earlier explanations —
the wake path, then syscall count — were measured and rejected; see
[bench/echo-comparison/README.md](bench/echo-comparison/README.md) for how, and
for the methodology and caveats, which matter more than the numbers.

## Testing

Seven test binaries (`ctest`) covering the scheduler, channels, `select`, timers,
sync primitives, networking, and a soak test — including 32k tasks across 24
workers, 8×8 MPMC channel traffic, 32 concurrent `select`s racing setup against
wakeup, 64 concurrent TCP connections at 20 round trips each, deadline
interruption, and close-wakes-a-parked-reader.

All pass clean under ThreadSanitizer:

```
cmake -S . -B build-tsan -DCIO_SANITIZE=tsan -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan -j && (cd build-tsan && ctest)
```

There is also an idle-CPU test: 24 workers idling for 300 ms must consume under
0.5 CPU-seconds, which fails loudly if anyone starts spinning instead of parking.

**Run the soak longer than ctest does.** `test_soak` defaults to 3 seconds so
`ctest` stays fast, but the bug it was written to find took 60 seconds to
surface. Run it directly, in an optimized build:

```
./build/test_soak 90
```

It churns 48 clients through connect/round-trip/close against an in-process echo
server, with deadlines short enough to fire mid-read, while eight more tasks
hammer channels, `select` and timers. Then it checks that descriptors came back
and that RSS did not climb. Over 120 s: 17.6M round trips, 0 mismatches, fds
settled back to 8. RSS growth is +2.5 MB at 5 s, +3.4 MB at 90 s, +3.6 MB at
120 s — the shape of a pool warming up, not of a leak.

Do not run long soaks under a sanitizer — see below.

## Known limits

These are real, and worth knowing before you build on it:

- **Symmetric transfer must be a tail call, and sanitizers break it.** A loop
  like `for (;;) co_await subtask()` is constant-stack only if the compiler
  tail-calls the coroutine resume. Clang emits a `musttail` and is always
  correct. GCC needs `-foptimize-sibling-calls`, which `-O2`/`-O3`/`-Og` imply
  but `-O0` and `-O1` do not — the CMake target adds it as a PUBLIC option so
  consumers inherit it, but a hand-rolled build must pass it. Both ASan and TSan
  force sibling calls off and cannot be overridden, so a sanitizer build will
  overflow its stack on a long-running coroutine loop. Keep sanitizer runs short;
  run soaks in a normal build.

  Measured, 100k iterations of `co_await leaf()`: GCC `-O0` and `-O1` overflow,
  `-O0 -foptimize-sibling-calls` and `-Og`/`-O2`/`-O3` all show 0 bytes of stack
  growth.

- **Linux and epoll only, by choice.** A portability layer that is never compiled
  on the other platforms rots silently and invites misplaced trust. The reactor's
  backend-independent half is separated out (`src/reactor_common.cpp`) so a
  kqueue or IOCP backend has a clean place to go, but none is claimed. Note that
  IOCP would also need the net layer's readiness loops reworked into submissions.

- **No preemption.** A task that computes without ever suspending holds its
  worker until it finishes. This is why `cio::blocking()` exists; use it.

- **Shutdown does not unwind parked tasks.** Like Go at process exit, tasks
  blocked on a channel or socket when the runtime stops are simply not resumed,
  and their frames leak. `block_on` returning means your root task finished, not
  that every detached task did — that is what `TaskGroup` is for.

- **A socket must outlive every task using it.** `close()` is safe to call while
  another task is parked on it (they wake with `Errc::closed`), but destroying
  the object out from under a parked task is not.

- **One task per direction per socket.** A second concurrent reader on the same
  socket gets `Errc::broken` rather than silently corrupting the wait slot. Go
  panics in the same situation.

- **Cancellation is cooperative.** A `CancelToken` closes a channel; a task that
  never selects on `token.done()` will not notice. There is no mechanism to
  interrupt a task that is not at a suspension point.
