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
1.24. Server on 8 cores, load generator on 16, 5 s measurement after 3 s
warm-up. Full data in `results.csv`; `./run_matrix.sh` regenerates it.

The headline case — 128-byte payload, connections held open — is the one every
echo benchmark reports, and on its own it is misleading. Round trips per second:

| connections | cio | asio-callback | asio-coro | go |
|---:|---:|---:|---:|---:|
| 1 | 11,265 | 13,104 | 13,242 | 11,900 |
| 8 | 65,384 | 90,998 | 95,174 | 54,676 |
| 64 | 458,518 | 634,120 | 633,994 | 468,220 |
| 256 | 624,340 | 764,534 | 751,407 | 644,801 |
| 1024 | 700,473 | 823,452 | 816,809 | 682,098 |
| 4096 | 648,711 | 780,519 | 767,909 | 583,976 |

Shared-nothing leads everywhere, cio and Go track each other. Then the other
four sweeps say when that lead is real and when it is an artifact of the
workload.

### Payload: the lead is fixed overhead, and it disappears

512 connections, 8 threads:

| payload | cio | asio-callback | asio-coro | go | asio lead |
|---:|---:|---:|---:|---:|---:|
| 16 B | 675,372 | 813,176 | 807,186 | 674,445 | +20% |
| 128 B | 670,503 | 823,571 | 803,145 | 673,091 | +23% |
| 1 KiB | 611,871 | 711,288 | 708,293 | 604,239 | +16% |
| 4 KiB | 420,079 | 480,154 | 477,837 | 434,802 | +14% |
| 16 KiB | 25,297 | 25,185 | 25,263 | 25,012 | **0%** |

The advantage is a constant per-request cost, not a per-byte one. Once a request
moves 16 KiB, the memcpy and the TCP stack dominate and all four are within 1%.

### Threads: it needs saturation to show up

512 connections, 128 B, everything pinned to the same 8 cores:

| threads | cio | asio-callback | asio lead | cio µs CPU/req | asio µs CPU/req |
|---:|---:|---:|---:|---:|---:|
| 1 | 117,275 | 119,798 | +2.1% | 10.1 | 8.4 |
| 2 | 233,726 | 242,462 | +3.7% | 9.6 | 8.3 |
| 4 | 461,107 | 472,668 | +2.5% | 9.5 | 8.5 |
| 8 | 678,912 | 827,596 | **+21.9%** | 11.4 | 9.3 |

The right-hand columns are the real story. cio costs more CPU per request at
every thread count, and the cost *grows* with thread count (12% at 4 threads,
23% at 8) while asio's stays flat — the signature of coherence traffic on shared
state. Below saturation the spare cores absorb it and throughput matches; at 8
threads on 8 cores it converts directly into lost throughput.

### Skew: where work stealing is worth what it costs

Some connections ask the server to burn CPU per request; the rest ask for none.

With 256 connections the answer is nothing (asio stays ahead, gap narrowing from
26% to 2% as the CPU work grows) — but that is a flaw in the test, not a result.
`SO_REUSEPORT` spreads 256 connections evenly enough that every shard gets its
fair share of heavy ones, so no shard is ever the unlucky one.

Fewer connections, where that averaging does not happen (200 µs of CPU on the
heavy ones):

| | cio | asio-callback | asio-coro | go |
|---|---:|---:|---:|---:|
| 16 conns, 25% heavy | **151,887** | 127,286 | 145,445 | 143,161 |
| 16 conns, 50% heavy | **109,401** | 88,325 | 86,309 | 100,757 |
| 32 conns, 25% heavy | **225,382** | 108,254 | 198,421 | 211,444 |
| 32 conns, 50% heavy | **93,272** | 72,791 | 90,760 | 89,494 |

cio wins all four, by 19% to 108%. The server CPU column says why: at 32
connections and 25% heavy, cio uses 38.2 of its 40 core-seconds while
asio-callback uses 26.5. asio is not slower because it is inefficient — it is
slower because it *cannot reach* the idle cores. The heavy connections are
pinned to the shards that accepted them, and seven idle threads cannot help.

This is the load balancing that a flat echo benchmark never charges shared-
nothing for, and it is worth about as much as shared-nothing's advantage is on
the flat case.

### Churn: cio's real weakness

Reconnect every N requests, 256 connections:

| requests per connection | cio | asio-callback | asio-coro | go |
|---:|---:|---:|---:|---:|
| never (held open) | 616,628 | 767,292 | 770,281 | 651,801 |
| 100 | 605,686 | 735,642 | 720,847 | 625,868 |
| 10 | 325,673 | 506,403 | 322,210 | 428,109 |
| 1 | 33,020 | 32,977 | 32,769 | 64,615 |

At 10 requests per connection cio is 24% behind Go and 36% behind
asio-callback — and its server CPU is 28.2 core-seconds against asio's 34.7. It
is leaving 30% of the machine idle, which means accept is the bottleneck, not
throughput. cio has one acceptor task on one listening socket; asio has a
`SO_REUSEPORT` acceptor per thread and the kernel spreads new connections across
them. This is the one place in the matrix where a concrete, architecture-
preserving fix is indicated: shard the acceptor, which costs cio nothing in load
balancing.

(At 1 request per connection everything collapses into kernel connection setup
and the runtimes stop mattering — except Go, which is 2x everyone else there and
worth a look on its own.)

### Summary

| workload | winner | margin |
|---|---|---|
| small payloads, many held-open connections | shared-nothing asio | 14-23% |
| payloads >= 16 KiB | tie | 0% |
| below CPU saturation (<= 4 of 8 cores) | tie | 2-4% |
| CPU-heavy requests, evenly spread | tie | 2% |
| uneven load, few connections | **cio / Go** | 19-108% |
| connection churn | asio, then Go | cio 24-36% behind |

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
