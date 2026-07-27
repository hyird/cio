# cio

Goroutine-style concurrency for C++20, on stackless coroutines.

The public API has no thread in it. You write tasks, channels, and `select`, and
the runtime handles M:N scheduling, work stealing, an edge-triggered reactor,
sharded timer heaps and a blocking pool underneath.

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

int main() {
    return cio::run([]() -> cio::Task<int> {
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
        co_return total;
    }());
}
```

## Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cd build && ctest
```

Requires C++20 and Linux. Tested with GCC 13.3 and Clang 19 on Linux 6.12.
No external dependencies.

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
| `cio::Runtime`, `cio::run(task)` | the one place threads are mentioned |

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
   Reactor          epoll ET (kqueue/IOCP behind the same seam)
   TimerService     per-worker 4-ary heaps, atomic earliest-deadline
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
trip before the gate, 200 ns after.

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
setup code, and `Timer::disarm()` does not return until a firing callback has
stopped touching the node.

## Measurements

AMD EPYC 7402, 24 cores, Linux 6.12, GCC 13.3, `-O3`. `./build/bench_core [workers]`.

| benchmark | 1 worker | 24 workers |
|---|---:|---:|
| `co_await` child task | 19.7 ns | 26.1 ns |
| `co_await cio::yield()` | 12.6 ns | 24.3 ns |
| `go()` detached spawn | 144 ns | 567 ns |
| `spawn()` + `co_await` join | 302 ns | 1393 ns |
| unbuffered chan round trip | 100 ns | 137 ns |
| buffered chan round trip | 102 ns | 199 ns |
| chan throughput, 1p/1c | 34.5 ns/op | 63.4 ns/op |
| chan throughput, 8p/8c | — | 352 ns/op |
| `select`, a case ready | — | 177 ns |
| `select`, parks every round | — | 232 ns |
| timer arm + fire | — | 1.3 µs |

`./build/bench_echo 256 2000` — echo server and 256 client connections in one
process over loopback, 24 workers: **497k round trips/sec**, 512k requests in
1.03 s.

For reference, Go on comparable hardware lands around 300–400 ns for goroutine
spawn and 250–350 ns for an unbuffered channel round trip. These numbers are
microbenchmarks on one machine and should be re-measured on yours.

## Testing

Six test binaries (`ctest`) covering the scheduler, channels, `select`, timers,
sync primitives and networking — including 32k tasks across 24 workers, 8×8 MPMC
channel traffic, 32 concurrent `select`s racing setup against wakeup, 64
concurrent TCP connections at 20 round trips each, deadline interruption, and
close-wakes-a-parked-reader.

All six pass clean under ThreadSanitizer:

```
cmake -S . -B build-tsan -DCIO_SANITIZE=tsan -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan -j && (cd build-tsan && ctest)
```

There is also an idle-CPU test: 24 workers idling for 300 ms must consume under
0.5 CPU-seconds, which fails loudly if anyone starts spinning instead of parking.

## Known limits

These are real, and worth knowing before you build on it:

- **Only the epoll backend is built and tested.** `src/reactor_kqueue.cpp` is
  written to the same contract but has never been compiled or run — it is the
  seam with content in it, not a supported backend. IOCP is not written; the
  awaiter shape (park an object living in the frame, hand its address to the
  backend, get it handed back) fits a completion-based backend, but the net
  layer's readiness loops would need a submit-based path.
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
- **`read`/`write` return `Task<Result<n>>`**, so each call is a coroutine frame
  unless the compiler elides it. `try_read`/`try_write` plus `readable()` /
  `writable()` are exposed for hot paths that want the loop without the frame.
- **Cancellation is cooperative.** A `CancelToken` closes a channel; a task that
  never selects on `token.done()` will not notice. There is no mechanism to
  interrupt a task that is not at a suspension point.
