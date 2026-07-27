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

## Runtime refactor

The [runtime v2 design](docs/scheduler-v2.md) changes implementation only:
worker-local epoll shards and remote inboxes provide a shared-nothing balanced
hot path, while conditional work stealing preserves load redistribution under
skew.
Public names, explicit signatures, options and supported observable semantics
are frozen for this work; private types and public-header implementation details
may change when applications are recompiled.

Resolver, dialer and file additions in the
[I/O infrastructure design](docs/io-infrastructure.md) are a later milestone,
after the runtime refactor is stable. The refactor adds no feature, backend,
executor/completion-token API or io_uring support.

## The public surface

| | |
|---|---|
| `cio::Task<T>` | a lazy coroutine; `co_await`ing one is a tail call, not a scheduling hop |
| `cio::go(task)` | fire and forget — `go f()` |
| `cio::spawn(task)` | spawn and keep a `JoinHandle<T>` you can `co_await` |
| `cio::yield()` | `runtime.Gosched()` |
| `cio::Chan<T>` | a mutex-protected MPMC channel; its cheap refcounted handle is passed by value like `chan int` |
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
   Scheduler        M:N workers, each with runnext + local FIFO + hard-directed
                    MPSC inbox;
                    published-victim stealing and a cold shared fallback
   Reactor          one edge-triggered epoll + eventfd shard per worker
   TimerService     one 4-ary heap per worker, owner-local deadline reads
   FramePool        size-classed thread caches + central list
   BlockingPool     lazily grown, retires on idle
```

**Scheduler.** One OS thread per worker, each with a single-slot LIFO `runnext`,
a 256-entry owner-produced FIFO ring stealable in halves, and a 256-entry
bounded `RemoteInbox` consumed only by that worker. The inbox is MPSC and is
used only by a hard-directed internal submission with a concrete ownership
target. Ordinary foreign submissions, non-local soft-affinity completions and
queue overflow use the shared fallback. It is unrelated to public
`cio::Chan<T>`, whose buffered/rendezvous state and waiter lists remain
mutex-protected MPMC. `runnext` remains owner-only so a direct
producer/consumer handoff keeps same-worker cache locality, while bounded
fairness publishes work that it would otherwise hide. An initial local enqueue
is only a placement choice: FIFO work may be published and stolen immediately,
so it does not guarantee that a task stays on that worker.

**Finding work.** A worker services its local handoff and FIFO first, checks its
inbox, reactor, timer shard and shared fallback with bounded fairness, then
steals only from workers advertised in the scalable `stealable` bitmap. A
per-worker publish/clear epoch handshake closes the set/clear race while
letting a burst skip repeated reads of the shared `stealable` bitmap. This
avoids scanning every peer when balanced while still exposing FIFO backlog
whenever an idle worker can use it.

**Wakeups.** Every worker publishes its idle state in a bitmap and parks in its
own epoll shard. A hard-directed internal producer publishes to the destination
inbox, claims that worker's idle bit and writes only its eventfd; a
`wake_pending` bit coalesces redundant writes. Shared fallback work may wake any
idle worker. After publishing idle, the worker rechecks `runnext`, its local
FIFO and inbox, the shared fallback, due timers and published victims. This
closes the enqueue/park lost-wakeup window.

A FIFO-victim wake additionally pre-arms a sticky searcher credit before
claiming an idle worker. Exactly once, on the first scheduler iteration after
`Scheduler::park()` returns, the worker checks and consumes that credit before
executing any runnable from `runnext`, local FIFO, inbox or shared fallback,
including continuations made ready by timer or I/O service. Ordinary
task-to-task resumptions do not read the credit atomic. Every return from
`Scheduler::park()`—both its pre-poll final recheck and a reactor return—clears
idle and then rechecks published victims, so an unrelated I/O edge cannot
intercept the only searcher. After any successful steal, a still-published
original-victim tail receives another searcher; items retained on the thief and
the original victim tail are separate published sources.

**Reactor.** Each worker owns an edge-triggered epoll instance and eventfd.
An accepted descriptor receives a stable home shard; established readiness is
normally polled and resumed by that shard's worker. Per-direction readiness is
a single atomic word with three states (`idle` / `ready` / `waiter*`), so the
race between "syscall returned EAGAIN, about to park" and "readiness arrived"
is resolved by CAS. Descriptor slots retain stable addresses until reactor
destruction and carry generations, lifecycle pins and syscall leases, making a
stale event safe while `close()` and deadlines race with an operation.

**Deadlines** live on the descriptor, not on the awaiter — Go's `SetReadDeadline`
model. That is not API mimicry: a timer that can fire concurrently with the
operation it is timing out must outlive the coroutine frame, and a descriptor in
the reactor's slab does while an awaiter in a frame does not.

**Timers.** Per-worker 4-ary min-heaps, so arming a deadline never touches a
shared lock. Each worker reads only its own shard's earliest deadline when
computing the timeout for its epoll wait. Foreign or monitor-fired timer batches
use the shared completion fallback so a busy preferred worker cannot strand
them. Waits use `epoll_pwait2`, so a 200 µs sleep is not rounded up to a
millisecond.

**Frame pool.** Coroutine frames are the runtime's most frequent allocation — one
per spawned task, one per socket read or write — so they go through a
size-classed allocator instead of malloc. A thread cache serves the common case
(a task allocates and frees its own frames) with no atomics; a central list with
batched transfer serves the fan-out case, where one task allocates every frame
and 24 workers free them, which a purely thread-local cache cannot recycle at
all. Measured: 1.008 → 0.000 allocations per socket read, and 1.000 → 0.008 per
cross-thread spawn.

**Watchdog.** A sysmon-style monitor is a stale-shard backstop: it can poll a
worker reactor and fire its timers when that owner is occupied by CPU-bound
work. Completions produced there go through the shared fallback and wake the
target runtime.

### The lifetime rule everything depends on

Waking a task hands its frame to another thread, which may resume and destroy it
immediately. So: **a waker decides whether it owns a waiter while holding the
lock that waiter is queued under, must not touch it after releasing that lock
unless it won, and must schedule it last.**

That single rule is what lets `select` retract its unfired cases with nothing but
the channel lock — no waiter or frame refcounting on the channel wakeup path.
The two places without a shared lock get explicit handshakes instead: `select`'s
timeout publishes through a phase word so a case that fires mid-registration
defers the resume back to the setup code, and `TimerService::disarm()` does not
return until a firing callback has stopped touching the node.

The corollary is that `disarm()` must be called *unconditionally*. Guarding it
with `if (state == kArmed)` skips the wait for an in-flight callback, which lets
the caller recycle or re-arm the node underneath it — the callback's final state
write then lands on the node's next incarnation, and the same timer ends up
linked into a heap twice with a stale `heap_index`. That was a real crash, found
by the soak test at ~60 seconds and not by any unit test.

## Measurements

The measurements in this section are the **pre-v2 baseline** retained to explain
why the runtime was refactored. They describe the former shared-reactor,
global-scan scheduler at the commit recorded by the benchmark documentation;
they are not claims about the current worker-sharded implementation. Runtime v2
release measurements use frozen A/B binaries and the gate in
[the design](docs/scheduler-v2.md).

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

There are two comparisons here, and the newer one exists because of a flaw in
the older one.

**`bench/http-comparison/`, driven by wrk.** A minimal HTTP/1.1 server on each
of cio, shared-nothing Boost.Asio and Go — hand-rolled in all three, so what is
compared is the runtime and not `net/http` against an asio HTTP library. Server
pinned to CPUs 0-7, wrk to 8-23, servers interleaved with the order rotating
each repeat. Requests/sec, three repeats, and server CPU out of the 64
core-seconds those 8 cores can supply over the measured window:

| connections | cio | asio | go | cio CPU | go CPU |
|---:|---:|---:|---:|---:|---:|
| 8 | 75,483 | **123,134** | 72,714 | 23.6 | 28.6 |
| 64 | 636,751 | **722,608** | 629,971 | 58.5 | 60.4 |
| 256 | 770,606 | **816,972** | 716,889 | 61.8 | 62.9 |
| 1024 | 770,628 | **835,326** | 721,637 | 61.1 | 63.5 |

cio is ahead of Go at every point — by 1.1% at 64 and 7.5% at 256 — and uses
less CPU than Go at all of them. Shared-nothing asio leads by 5.7-11.9% above 64
connections and by much more below, for the reason the skew sweep below charges
it for.

**Why wrk.** Echo A/B runs the server and load generator as independent pinned
processes and freezes one prebuilt cio load-generator binary across both sides.
That makes an interleaved A/B a server-only comparison, but the generator is
still project code rather than a third-party implementation. Historically,
rebuilding it with the runtime under test changed the answer: the
reactor-ordering change measured +7.9% against the generator as it stood and
+50% after the generator was rebuilt on the improved runtime. Worse, at 8
connections the same harness reported +4.1% for a change that costs 4.6% there
— a real regression hidden by the client speeding up in step with the server.
`wrk` is the third-party generator: it is not built on any runtime under test
and does not change when they do.

Against wrk, the same server source with only the pre-v2 runtime swapped, from
the first commit of that historical series to its last:

| connections | before | after | |
|---:|---:|---:|---:|
| 1 | 12,312 | 14,047 | **+14.1%** |
| 8 | 80,384 | 76,663 | **−4.6%** |
| 64 | 384,256 | 635,822 | **+65.5%** |
| 256 | 544,376 | 758,177 | **+39.3%** |
| 1024 | 596,095 | 766,982 | **+28.7%** |

The 8-connection column was not a rounding error and remained unfixed in that
pre-v2 series. With more workers than in-flight work every worker ran dry
constantly, and draining the shared reactor before stealing cost a syscall each
time it found nothing; a 20 µs backoff recovered about half of it. Runtime v2
replaces that mechanism with per-worker reactor shards and directed wakeups.

**`bench/echo-comparison/`** runs an 8-thread echo workload against cio, Boost.Asio
(shared-nothing: one `io_context` and one `SO_REUSEPORT` acceptor per thread, in
both callback and coroutine form) and Go, with the server pinned to CPUs 0-7 and
the load generator in a separate process pinned to 8-23. Before/after runs keep
that load-generator binary frozen. `run_matrix.sh` sweeps payload, thread count,
connection count, load skew and connection churn; `results.csv` has all 112
cells.

The single headline number is misleading, so here is the shape of it. At 128
bytes with connections held open, shared-nothing asio leads by 14-23% and cio
tracks Go. But:

| workload | winner | margin |
|---|---|---|
| small payloads, many held-open connections | shared-nothing asio | 14-23% |
| payloads >= 16 KiB | tie | 0% |
| below CPU saturation (<= 4 of 8 cores) | tie | 2-4% |
| CPU-heavy requests, evenly spread | tie | 2% |
| **uneven load, few connections** | **cio / Go** | **19-108%** |
| connection churn | unmeasured on this host | — |

asio's advantage is a fixed per-request cost — coherence traffic that cio pays
for sharing run queues and that shared-nothing does not have — so it vanishes as
soon as a request does any real work. Its cost is that a connection is stuck on
the thread that accepted it: with 32 connections and a quarter of them CPU-heavy,
asio uses 26.5 of its 40 core-seconds while cio uses 38.2, because seven idle
threads cannot help the busy one. cio is 2.1x faster there.

The churn row was first written up as a cio weakness and then withdrawn: those
numbers were measuring how much of the ephemeral port space the previous server
in the sequence had left in TIME_WAIT, not the servers. Measuring it needs a
client that avoids TIME_WAIT or a second machine.

Two earlier explanations for the flat-echo gap — the wake path, then syscall
count — were measured and rejected; see
[bench/echo-comparison/README.md](bench/echo-comparison/README.md) for how, and
for the methodology and caveats, which matter more than the numbers.

### Counters

A sampling profiler answers "which symbol has the most cycles", which is not the
question worth asking about a scheduler. How many futex wakes a request costs,
how many events an `epoll_wait` returns, how often a searcher finds nothing —
those are the numbers that decide what to change, and the 21% steal hit rate
that motivated the historical pre-v2 reactor-before-steal reordering was the
first thing they showed.

```
cmake -S . -B build-metrics -DCIO_METRICS=ON -DCMAKE_BUILD_TYPE=Release
```

`cio::runtime_metrics()` returns a snapshot. It links either way and returns
zeroes when the counters are compiled out, so a diagnostic can be written once
and pointed at whichever build is interesting. Off by default: a benchmark and a
production build should not differ.

## Testing

Ten test binaries (`ctest`) cover the scheduler, worker bitmaps and MPSC inbox,
channels, `select`, timers, sync primitives, networking, public API surface, and
a soak test — including 32k tasks across 24 workers, 8×8 MPMC channel traffic,
32 concurrent `select`s racing setup against wakeup, 64 concurrent TCP
connections at 20 round trips each, deadline interruption, and
close-wakes-a-parked-reader.

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
  consumers inherit it, but a hand-rolled build must pass it. A sanitizer build
  will overflow its stack on a long-running coroutine loop either way, for two
  different reasons — keep sanitizer runs short, and run soaks in a normal
  build.

  Measured, 100k iterations of `co_await leaf()`: GCC `-O0` and `-O1` overflow,
  `-O0 -foptimize-sibling-calls` and `-Og`/`-O2`/`-O3` all show 0 bytes of stack
  growth.

  The two reasons are worth separating, because only one of them is the flag.
  TSan does force sibling calls off. ASan does not — compiling a plain tail call
  at `-O1 -foptimize-sibling-calls`, ASan still emits the `jmp` and TSan emits a
  `call`. What ASan defeats is the *coroutine* transfer specifically, because
  its stack instrumentation has to run after the call returns. The outcome is
  the same and the diagnosis is not, so a stack trace full of nested resumes
  under ASan is not evidence that a flag went missing.

  The practical consequence is that a burst drained without suspending — an
  accept loop emptying a backlog, say — is nested resumes rather than a loop,
  and its depth is the length of the burst. That is free in a release build and
  fatal under ASan at a few hundred. `test_soak` yields every sixteenth accept
  for exactly this reason.

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
  `Runtime::shutdown()` is a blocking external-thread operation; calling it
  from one of that Runtime's own worker tasks throws `std::logic_error` before
  stop or join begins.

  Delayed completions carry a process-lifetime-unique endpoint.
  Foreign/cross-runtime wakes acquire a short counted lease. Shutdown closes
  the endpoint to new leases, waits for active leases to drain, and then clears
  its Scheduler pointer; later foreign wakes are dropped safely. Same-runtime
  handoffs compare the cached endpoint without an endpoint RMW. Endpoint
  identities are never recycled—the small tombstone metadata remains reachable
  until process exit, avoiding both ABA and static-destruction UAF. A Socket
  and every parked I/O awaiter retain the stopped home reactor long enough to
  detach and release descriptor state; attempting new async I/O on a Socket
  returned from `cio::run()` reports `Errc::shutdown`.

- **A socket must outlive every task using it.** `close()` is safe to call while
  another task is parked on it (they wake with `Errc::closed`), but destroying
  the object out from under a parked task is not.

- **One task per direction per socket.** A second concurrent reader on the same
  socket gets `Errc::broken` rather than silently corrupting the wait slot. Go
  panics in the same situation.

- **Cancellation is cooperative.** A `CancelToken` closes a channel; a task that
  never selects on `token.done()` will not notice. There is no mechanism to
  interrupt a task that is not at a suspension point.
