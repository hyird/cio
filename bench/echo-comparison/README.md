# Echo server comparison: cio vs Boost.Asio vs Go

An 8-thread TCP echo server, the same workload against four implementations.

```
export GOROOT=/path/to/go          # optional; the Go server is skipped without it
cmake --build ../../build -j       # the comparison links against libcio.a
./run_comparison.sh [connections] [duration_s] [payload] [repeats]
```

## What is being compared

| | architecture |
|---|---|
| **cio** | work-stealing M:N, one shared reactor, one task per connection |
| **asio-callback** | shared-nothing: one `io_context` + one `SO_REUSEPORT` acceptor per thread, callbacks |
| **asio-coro** | the same shared-nothing server written with `asio::awaitable` |
| **go** | goroutine per connection, `GOMAXPROCS=8` |

Go is the architectural peer: work-stealing M:N over a shared netpoll, the same
shape as cio. The asio servers are the contrasting design — a connection is
pinned to the thread that accepted it and never migrates, so there is no shared
run queue, no stealing, and no cross-thread wakeups. Both asio variants are
included because the gap between them is asio's own coroutine overhead, which
would otherwise be conflated with the architectural difference.

## Methodology

The numbers below are only worth anything because of these:

- **Disjoint cores.** Server pinned to CPUs 0–7, load generator to 8–23. The
  machine is a single-socket 24-core EPYC 7402 with no SMT and one NUMA node, so
  the split is symmetric.
- **Closed loop.** Each connection waits for its echo before sending again, so
  offered load is bounded by the server and nothing piles up in kernel buffers
  pretending to be throughput.
- **Warm-up excluded.** Connections are established and traffic runs for 3 s
  before measurement starts; server CPU is sampled across the measurement window
  only, not the warm-up.
- **Both sides' CPU is reported.** If the load generator saturates its 16 cores
  the result measures the generator, not the server. It does not: at 512
  connections it sits at ~80% while every server is at ~96%.
- **The same generator for all four**, so whatever bias it has is common-mode.
- **Interleaved repeats for A/B.** Run-to-run drift on this machine is larger
  than the effects being measured — two sweeps minutes apart disagreed by 15% on
  every server at once. Any before/after claim here comes from alternating the
  two builds inside one run, never from comparing two sweeps.

Caveats worth stating: this is loopback, so it measures the runtime and the
kernel's TCP path with no network in it; the load generator is built on cio,
which is a conflict of interest mitigated only by it having 2× the cores and not
being the bottleneck; and an echo server is almost pure I/O dispatch, which is
the workload that flatters shared-nothing designs most.

## Results

24-core EPYC 7402, Linux 6.12, GCC 13.3 `-O3 -march=native`, Boost 1.83, Go
1.24. 128-byte payload, 5 s measurement, server on 8 cores.

Round trips per second:

| connections | cio | asio-callback | asio-coro | go |
|---:|---:|---:|---:|---:|
| 8 | 65,338 | 79,764 | 83,767 | 56,106 |
| 64 | 439,374 | 645,816 | 617,365 | 468,098 |
| 512 | 680,753 | 809,941 | 786,083 | 680,915 |

Median / p99 latency (µs) at 512 connections:

| | cio | asio-callback | asio-coro | go |
|---|---:|---:|---:|---:|
| p50 | 683 | 584 | 600 | 644 |
| p99 | 2240 | 1686 | 1745 | 2240 |

Server CPU across the 512-connection window was 38.5 / 38.1 / 38.3 / 38.0
core-seconds out of 40 available — everything is saturated and the differences
are pure efficiency, not headroom.

### Reading them

**cio and Go land in the same place**, which is what you would expect from two
runtimes with the same architecture: dead even at 512 connections (680.8k vs
680.9k), Go ~6% ahead at 64, cio ~16% ahead at 8. Neither difference is
architectural; they are different tunings of the same design.

**Shared-nothing asio is 17–47% faster**, and that is the real finding. Pinning a
connection to one thread for its lifetime removes every cross-thread cost cio and
Go pay: no run queue shared between workers, no stealing, no futex wakeups to
hand a ready connection to an idle thread. What it gives up is load balancing —
one shard with the expensive connections cannot be helped by seven idle ones —
which an echo benchmark, where every connection costs the same, never charges it
for.

**asio's coroutines are close to free** here: the callback and `awaitable`
variants are within 4% of each other, and the coroutine one is actually ahead at
8 connections. Coroutines are not what separates these numbers.

### The batch-wake change

The first run of this comparison had cio at 404k against Go's 469k with *lower*
server CPU (16.9 vs 19.1 core-seconds) — leaving work on the table rather than
running out of CPU. The cause was in the wake path: a reactor poll can make
hundreds of tasks runnable in one syscall, but `notify()` deliberately wakes
exactly one worker, so the workers ramped up one at a time, each waking the next
only after it had found work. That is a futex round trip per worker on the
critical path of every burst.

`Scheduler::notify_batch()` now wakes as many workers as the burst can occupy.
Measured by alternating the two builds inside one run, three repeats each:

| | before | after | |
|---|---:|---:|---|
| 64 connections, rps | 417,433 | 447,160 | **+7.1%** |
| 64 connections, p99 | 392 µs | 319 µs | **−19%** |
| 512 connections, rps | 671,888 | 675,201 | +0.5% (noise) |

Exactly where the diagnosis predicted: it helps at moderate concurrency, where
workers park between bursts and the ramp is on the critical path, and does
nothing at saturation, where nobody parks in the first place.
