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
| 8 | 66,151 | 83,830 | 84,580 | 57,407 |
| 64 | 449,598 | 634,477 | 644,877 | 470,443 |
| 512 | 700,392 | 813,485 | 812,531 | 674,611 |

Median / p99 latency (µs) at 512 connections:

| | cio | asio-callback | asio-coro | go |
|---|---:|---:|---:|---:|
| p50 | 662 | 581 | 587 | 647 |
| p99 | 2176 | 1718 | 1673 | 2304 |

Server CPU across the 512-connection window was 38.9 / 38.2 / 38.5 / 37.7
core-seconds out of 40 available — everything is saturated and the differences
are pure efficiency, not headroom. The load generator sat at 78-83% of its 16
cores, so nothing here is client-bound.

### Reading them

**cio and Go land in the same place**, which is what you would expect from two
runtimes with the same architecture: cio 3.8% ahead at 512 connections, Go 4.6%
ahead at 64, cio 15% ahead at 8. None of those differences is architectural;
they are different tunings of the same design. (Before the two changes described
below, cio was 404k at 64 connections against Go's 469k.)

**Shared-nothing asio is 16–27% faster**, and that is the real finding. Pinning a
connection to one thread for its lifetime removes every cross-thread cost cio and
Go pay: no run queue shared between workers, no stealing, no futex wakeups to
hand a ready connection to an idle thread. What it gives up is load balancing —
one shard with the expensive connections cannot be helped by seven idle ones —
which an echo benchmark, where every connection costs the same, never charges it
for.

**asio's coroutines are close to free** here: the callback and `awaitable`
variants are within 4% of each other, and the coroutine one is actually ahead at
8 connections. Coroutines are not what separates these numbers.

### Where the gap actually is

Three hypotheses, in the order I held them. Two were wrong, and the way they
were wrong is the useful part.

**Hypothesis 1: the scheduler.** cio started 404k against Go's 469k while using
*less* server CPU, which pointed at the wake path. That produced the batch-wake
change below — real, +7% at 64 connections — but it did not close the gap to
asio. Syscall counts then showed why: `futex` costs 0.038 per request and
`epoll_pwait2` 0.016. The scheduler is not where the time goes.

**Hypothesis 2: syscall count.** Counting syscalls per request looked damning:

| | recv/read | send/write | total | rps |
|---|---:|---:|---:|---:|
| cio (before) | 1.982 | 0.992 | 2.974 | 665,083 |
| asio-callback | 1.410 | 0.986 | 2.396 | 804,864 |
| go | 1.980 | 0.990 | 2.970 | 655,950 |

The ratios are almost exactly inverse — syscalls cio/asio = 1.241, throughput
asio/cio = 1.210 — and cio and Go, which issue the same number, land at the same
throughput. The second recv is the EAGAIN that edge-triggered readiness needs in
order to know the edge is consumed.

Removing it (the readiness hint, below) took recv from 1.982 to 0.991 per
request, putting cio *below* asio at 1.982 total syscalls against 2.396. Then:

| | rps | server CPU | µs CPU/req |
|---|---:|---:|---:|
| before hint | 670,815 | 96.9% | 11.55 |
| after hint | 681,537 | 96.7% | 11.35 |

A third of the syscalls, gone, for 1.7% of the CPU. So an EAGAIN recv costs
about 0.2 µs against 11.4 µs for the whole request — it is a kernel fast path
with no data to copy and no TCP stack to run. **Syscalls are not fungible**, and
counting them as if they were is what made the correlation look causal. The
intervention is what settled it; the correlation never could have.

**What it actually is.** With the syscall difference eliminated and cio still
19% behind, `perf` on the same workload:

| | cio | asio-callback |
|---|---:|---:|
| instructions/req | **16,065** | 16,778 |
| cycles/req | **34,923** | 27,615 |
| IPC | 0.46 | 0.61 |
| cache-misses/req | 240 | 208 |
| cache-references/req | 2,934 | 2,900 |

cio retires *fewer* instructions per request and spends 27% more cycles doing
it. It is not doing more work; it is stalling. That is coherence traffic on
shared scheduler state — the run queues, the searcher/idle counters, the CAS on
a victim's queue head during a steal — which a shared-nothing design does not
have at all. It is the price of the architecture, but the mechanism is memory
stalls, not syscalls and not futexes.

(`cpu-migrations` was 762 against asio's 11, which looks like an explanation
until you divide: 0.00025 per request, roughly 0.06% of the time. A large ratio
on a small absolute number is not evidence.)

### The readiness hint

Edge-triggered readiness means an operation is only provably complete once it
has seen EAGAIN, so the naive loop costs two recvs per message. But a short read
— fewer bytes than the buffer holds — means the receive queue was empty when the
call returned, and any later arrival re-arms the epoll edge. So the next read can
skip straight to parking. `IoDesc::ready_hint` records that, and recv per request
drops from 1.982 to 0.991.

Worth 1.6% of throughput here, which the section above explains. It is kept
because it is free and correct, not because it moved this benchmark.

It came with a bug worth recording, since the same shape will recur in anything
that caches kernel readiness in user space: the reactor sets the hint and records
readiness, then an in-flight short read stores `false` over the top, and the task
parks with data in the socket and no further edge coming — a permanent hang for
that connection. The fix is that the hint is set on the path that *observed*
readiness (`IoAwaiter::await_resume`), not on the path that *caused* it, so the
last writer is the one that is right.

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
