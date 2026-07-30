# Scheduler benchmark record

Status: evidence archive. This document holds the retained measurement record
for the runtime-v2 refactor and the v0.0.1 retag rounds. The design it justifies
is in [scheduler-v2.md](scheduler-v2.md); open evidence gaps are tracked in
[roadmap.md](roadmap.md).

Nothing here is a specification. A number is retained because it decided
whether a design stayed in the source, and because re-deriving it would cost
another benchmark host-day.

## How to read this record

- Positive throughput deltas mean the B side is faster. Positive latency and
  `ns/op` deltas mean B is worse. Every table states its direction.
- Confidence intervals are per-cell unadjusted paired log-ratio Student-t
  intervals. They do not support cross-cell family-wise claims.
- A result is release evidence only if its binaries map to clean source
  revisions. Frozen dirty-tree artifacts are labelled engineering evidence.
- Raw run directories under `bench/http-comparison/results/` are **local and
  untracked** (see `.gitignore`). Directory names below identify runs on the
  benchmark host; they are not retrievable from this repository.

## How the refactor was sequenced

The ownership rewrite shipped in seven stages, each compared with frozen
baseline artifacts before the next began. The order is recorded because a later
refactor of the same surface should follow it: each stage had to hold behaviour
constant while moving one thing.

0. **Freeze and baseline** — record public declarations and the example compile
   surface; capture correctness, throughput, tail latency, idle CPU and
   scheduler metrics before changing ownership.
1. **Internal ownership seam** — private `WorkerId` and worker/shard lookup
   helpers; the reactor entry point becomes a routing facade; exactly one shard,
   so behaviour and performance stay unchanged.
2. **Targeted inbox** — a preferred worker on internal wait nodes and per-worker
   remote inboxes, with the shared reactor and current stealing retained.
3. **Reactor sharding** — one epoll/eventfd pair per worker, accepted streams
   assigned to shards, post-accept distribution, monitor retained as helper.
4. **Local parking** — per-worker epoll parking and targeted wakes replace the
   shared idle condition variable and single-poller state; the 1- and
   8-connection latency and idle-CPU regimes verified first.
5. **Published imbalance** — idle/stealable bitmaps; stop scanning unpublished
   victims; preserve steal-half batching and skew throughput.
6. **Cold-path cleanup** — reduce the global queue to overflow and
   external/non-local completion fallback; remove old shared counters only after
   all callers migrated.

Stage 5 is where the overloaded bitmap and proactive donation were benchmarked
and removed. Every stage passed its acceptance gates.

## Runtime v2 final gate

The retained source was frozen before the final measurements. Hashes were
checked before and after every run.

| side | role | `bench_core` SHA-256 |
|---|---|---|
| A | exact pre-v2 Git HEAD source build | `fda481642f22ce7236a1ff849db1372de3550aa3819bf56b1aa85ffe86019188` |
| B | retained v2 source | `56d91a30b355f1f3cbd8fe097933dc08c2c75e0cdf383e590fbd948e63f76bb2` |

### Core microbenchmarks

Runs were warmed up, pinned to CPUs 0-7 or 0-23, paired, and alternated AN/NA.
Positive `ns/op` deltas mean B is slower.

- `spawn() + co_await join` improved by 16.86% at 8 workers and 19.76% at 24
  workers; all 20 isolated pairs favoured B. The retained implementation lets
  the original task complete `JoinState` from final suspend and removes the
  ordinary wrapper coroutine. Invalid and already-completed tasks retain a cold
  wrapper to preserve their old error/completion semantics and to avoid
  resuming a coroutine already at final suspend.
- Detached `go()` remained a measured v2 cost: B was 6.52% slower at 8 workers
  and 6.71% slower at 24 workers over 15 pairs. Comparing the direct-completion
  build with its immediately preceding v2 binary was neutral at 8 workers and
  2.99% faster at 24, so this cost is not hidden inside the join optimization.
- The formal 24-worker unbuffered-channel gate was neutral: paired geometric
  B/A +0.095%, median -0.683%, only 5/15 pairs slower. A seven-pair +13% screen
  did not reproduce.
- A 24-worker 1-producer/8-consumer channel screen at +23.5% also did not
  reproduce: its 15-pair confirmation was +3.49% geometric mean, -0.15% median,
  with opposite AN/NA directions. Select and the remaining channel and mutex
  screens were likewise direction-dependent rather than confirmed regressions.

### Network gate

Server and load generator ran in separately pinned processes. Echo retained
seven alternating pairs with the same cio-based load-generator binary
(`3752a0f3cb67ef7da0fc7fc4ac62fb730c818ceebd43b16c814ca770310b9ba7`) frozen
across both sides. HTTP used third-party `wrk`, ten pairs per cell, five AB and
five BA, 5-second warm-up and a separate 15-second measured window.

Local run: `wrk-matrix-clean-final-nosat-20260728`.

| workload | A mean req/s | B mean req/s | paired geometric B/A (95% CI) | median p50 A/B | median p99 A/B |
|---|---:|---:|---:|---:|---:|
| echo, 1024 connections, 128 B | 725,717 | 779,580 | **+7.43%** (about +5.1% to +9.9%) | 1351/1121 us | 3072/3968 us |
| HTTP, 1 connection, `wrk -t1` | 14,242 | 14,339 | +0.67% (-3.88% to +5.42%), neutral | 67/68 us | 109.5/110 us |
| HTTP, 8 connections, `wrk -t4` | 80,645 | 125,598 | **+56.00%** (+49.52% to +62.77%) | 92/52 us | 147/644 us |
| HTTP, 64 connections, `wrk -t16` | 729,546 | 789,141 | **+8.16%** (+6.84% to +9.50%) | 76/70 us | 518/501 us |
| HTTP, 256 connections, `wrk -t16` | 773,023 | 771,416 | -0.22% (-1.63% to +1.21%), neutral | 303.5/306 us | 915/2765 us |
| HTTP, 1024 connections, `wrk -t16` | 714,915 | 781,518 | **+9.34%** (+7.22% to +11.49%) | 1355/1125 us | 3900/4635 us |

All 100 measured HTTP sides had zero socket and HTTP errors, the servers stayed
live, and the A, B and `wrk` hashes were unchanged at the end. The AB/BA splits
agreed in direction for c8, c64 and c1024.

The warning-free matrix still shows workload-specific efficiency/tail
trade-offs. HTTP c64 server use was 7.89/7.97 cores and both p50 and p99
improved. At c8, server use was only 2.64/2.75 cores but B's median p99 was
4.38 times A. C256 was throughput-neutral with nearly identical server CPU,
while B's median p99 was 3.02 times A. C1024 improved throughput and p50 but
raised p99 by about 19%. Echo server CPU rose from 61.57 to 63.10 core-seconds
per measured window (+2.48%) and its median p99 rose by 896 us. Results
therefore include latency and CPU rather than treating throughput alone as the
gate.

### Provenance

Exact HTTP hashes:
`5650865ce18c6d029fbd0546b0ee9a6d7758da5087038f8f8db15664f78750e8` (A),
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf` (B), and
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588` (`wrk`).
A was built from clean commit `899ccad`; independent clean builds reproduced its
static-library hash. B rebuilds byte-for-byte from clean retained commit
`5e0208b`. The exact retained echo B binary was
`3916905609c9807dead082bb83fddb109e034c8056304be0007fec1496405d7e`.

Two matrices were rejected on provenance rather than on their numbers:

- The first complete HTTP matrix used `wrk -t2` at c8 and `-t8` at the larger
  cells. It passed correctness gates but reached at least 95% of configured
  client-thread capacity. A saturation warning makes a run a capacity screen,
  not release evidence.
- A byte-reproducible hybrid A
  (`1970c98716225a93d66a9b662ece04d0d93629ecd71c5b1c6356c9c7b97bf3e5`,
  local run `wrk-matrix-hybrid-nosat-20260728`) combined a dirty 09:45 UTC
  library with the 09:57 header state. It was rejected because it did not
  correspond to one clean frozen source revision.

None of either matrix's values is combined with the clean-source confirmation
above.

The final source passes all ten Release tests, all ten ASan/UBSan tests, and all
ten TSan tests. The TSan build retains the benchmark-only mismatched-allocation
warning from `bench_io`'s allocation counter; no sanitizer test failed.

### Skew sensitivity caveat

An earlier skew screen of the predecessor `candidate_frozen` build — not the
retained final candidate — exposed a possible boundary of the busy-worker
backstop. With 200 us of non-suspending work, its 16-connection/50%-heavy cell
was about 10.7% below the pre-v2 scheduler in a seven-repeat confirmation, while
other skew cells moved in both directions.

The result is consistent with heavy connections being unevenly placed across
shards, but the harness did not record the connection-to-shard mapping.
Placement is therefore an unverified explanation, and these cells are a
sensitivity signal rather than a universal scheduler ratio or a final-candidate
measurement. Recording that mapping is tracked in
[roadmap.md](roadmap.md#open-evidence-gaps).

## v0.0.1 retag rounds

These rounds ran on 2026-07-29 against `/usr/bin/wrk`, servers on CPUs 0-7,
clients on 8-21 and the harness on CPU 23. They form a chain in which each
round's retained candidate becomes the next round's baseline:

```text
c9c978fb  published v0.0.1 runtime
   |
   v  monitor balance (owner ticket + SCHED_BATCH)
80d71422
   |
   v  reusable worker driver
f1841caa
   |
   v  work-aware completion quota
ae6ae7ae -> cc5b9945 (screen) -> aa9834d2 (final retag)
```

### Monitor balance

Ten pairs per cell, five AB and five BA, eight server workers on CPUs 0-7, the
same third-party `wrk` on 14 threads across CPUs 8-21, 5-second warm-up and a
separate 15-second measured window. Local run
`wrk-confirm-v001-vs-monitor-batch-balanced-20260728-001`.

| connections | baseline mean req/s | candidate mean req/s | paired geometric candidate/baseline (95% CI) | median p50 | median p75 | median p90 | median p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 786,646 | 796,591 | **+1.27%** (+0.01% to +2.55%) | 60/59 us | 72/70.5 us | 88.5/86 us | 467.5/539.5 us |
| 1024 | 777,135 | 859,820 | **+10.65%** (+9.08% to +12.25%) | 1100/1100 us | 1900/1220 us | 2325/1530 us | 4565/2060 us |

At c1024 the candidate's paired geometric latency deltas were -0.71% at p50,
-36.05% at p75, -34.65% at p90 and -54.69% at p99. The throughput gain
reproduced in both order strata: +10.02% in AB and +11.29% in BA. The small
c1024 p50 effect was mildly order-sensitive (+0.55% AB, -1.96% BA), so it is
described as neutral. At c64, p90 improved 2.48% overall and in both strata, but
p99 was position-sensitive: AB +35.93% while BA -35.30%. That c64 p99 is
recorded as order-dependent, not as an improvement or a regression.

The single maximum latency reported by each c1024 `wrk` run did not follow the
p99 improvement: its paired geometric delta was +24.70%, with baseline maxima of
61.25-93.95 ms and candidate maxima of 86.41-167.46 ms. No candidate p99
exceeded 5 ms and no maximum exceeded the predeclared 200 ms rare-tail trigger,
so the gate did not require an observer rerun. The maximum-latency movement is
retained as a diagnostic caveat; this result does not claim that every
extreme-tail statistic improved.

Second-run versus first-run throughput drift was +0.15% at c64 and -0.57% at
c1024. All 40 measured sides had zero socket and HTTP errors, neither cell hit
the client-capacity warning, both servers stayed live, and all input hashes were
unchanged. Hashes:
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf` (baseline),
`80d71422932a59b90b1cb3eb6395dce52e06fbfae9c6763b9add36d5544a6239` (candidate),
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588` (`wrk`).

This is frozen-binary engineering evidence: the candidate executable does not
map to a committed clean source revision.

#### Extreme-tail diagnostic

A four-pair, 30-second c1024 diagnostic against the original 2026-07-28 v0.0.1
publication binary (local run `wrk-taildiag-v001-vs-monitor-batch-20260729-001`)
exposed a tradeoff hidden above p99. The monitor-balance candidate gained 7.87%
throughput and reduced p90 and p99 by 33.11% and 53.60%, but its p99.99,
p99.999 and maximum latency rose 22.29%, 13.95% and 17.89%. The scheduler
observer made this a diagnostic rather than a publication matrix, and the
intended gains reproduced in both order strata — but so did the extreme-tail
cost.

This result narrowed the next optimization target to rare foreign-monitor
dispatch rather than common readiness processing. The two designs aimed at that
path, and the two cooperative-budget designs that followed, are in the rejected
index below.

### Reusable worker driver

Ten pairs comparing the monitor-balance server with the worker-driver server at
64 and 1024 connections. Local run
`wrk-confirm-monitor-batch-vs-global-driver-v1-balanced-20260729-001`.

At c64, throughput was neutral at -0.31% (95% CI -1.25% to +0.63%). Its p50 and
p75 rose by 2.72% and 1.69%, one microsecond at the reported medians, while p99,
p99.9 and p99.99 fell by 46.21%, 4.09% and 14.21%. The three tail improvements
reproduced in both AB and BA strata. At c1024, throughput was likewise neutral
at -0.49% (-1.50% to +0.54%); p50 through p99 and p99.99 through Max were
neutral. The p99.9 aggregate favoured the driver but had a very wide interval
and reversed in magnitude by order, so it is not claimed as an improvement.

All 40 measured sides completed without socket or HTTP errors, client
saturation, early server exit or hash drift. Server hashes:
`80d71422932a59b90b1cb3eb6395dce52e06fbfae9c6763b9add36d5544a6239` (monitor
balance) and
`f1841caae0037b6306eefee81eb6c5fd928d0f9ae3f9ed56303e5caa2c141948` (worker
driver). The retained worker-driver static library is
`55190fdc7fab84882a7875e2e97a9db1c9d1ed9f5c9735d19f360a9a38d6b3bc`. As with the
preceding candidates, these hashes identify frozen dirty-tree artifacts rather
than a committed release revision.

### Work-aware completion quota

The first frozen work-aware build (`ae6ae7aedf8ebc84...`) still carried a
success flag and a non-relaxable TLS access on every TCP completion.

Its publication-ready ten-pair standard confirmation against the worker-driver
baseline (local run
`wrk-standard-confirm-workaware-adaptive128-local256-idlepublish-20260729-01`)
was neutral at c64, at -0.34% throughput (95% CI -2.21% to +1.57%). At c1024 it
lost 2.52% throughput (-3.79% to -1.24%), while paired p50 and p90 rose 3.47%
(+2.03% to +4.93%) and 2.33% (+0.07% to +4.63%). Requests per server CPU second
fell 2.51%, with both sides using about 7.91 server cores. That is a real
common-path efficiency regression, not client saturation or an idle server.

The same build's four-pair mixed confirmation (local run
`wrk-mixed-confirm-hardened-workaware-adaptive128-local256-idlepublish-20260729-01`)
showed why the fairness mechanism remains useful: pipelined bulk throughput was
neutral at -0.07% (-1.13% to +1.01%), while the ordinary probe gained 100.61%
throughput and reduced p50, p90 and p99 by 49.47%, 38.53% and 30.57%. The
pre-fast-TLS build therefore passed the mixed fairness objective but failed the
joint standard-load gate at c1024.

Removing the success flag, using the hidden TLS alias and widening the counter
produced frozen screen binary
`cc5b9945e734c0e17589af92bb98700b8e940a51803f0c5992597b763c109bed`. Its
four-pair mixed screen kept bulk throughput neutral at -0.22% (-1.85% to
+1.43%), while probe throughput rose 91.35% and probe p50, p90 and p99 fell
47.19%, 37.02% and 31.73%. In the standard screen, c64 throughput was +0.91%
(+0.02% to +1.79%). C1024 did not establish parity: its aggregate was -5.07%
with a wide -16.24% to +7.60% interval, driven by paired results of -0.50%,
-15.61%, -1.35% and -1.95%. The low run had normal server/client CPU, no socket
or HTTP error and cannot be discarded.

> The raw run directories for this `cc5b9945` screen are not present in the
> local results archive. See
> [roadmap.md](roadmap.md#open-evidence-gaps).

After that screen, the TLS storage was changed from one hidden symbol to the
shared-safe default-visible/hidden dual-symbol ABI. Final hashes: static library
`699fd88b69df76d8605a3b690d47adb52b6cab9d2964998eb6f01cb7942eedd6`, HTTP server
`aa9834d2167a6436fb451a274abc5b8cdcb09aca05ea8239519d581cded43af4`, shared
library `dca78bd16f0d2b294b57d1014d991f0a1b8c98613d6b457ce6401e8739350f3a`.

Two independent Release builds reproduced the static library and HTTP server
byte for byte. Static Release, shared Release, ASan/UBSan and TSan each passed
all ten tests. The final ABI change preserves the screen binary's entire `.text`
section byte for byte (raw section SHA-256
`b91f518c268c70239dc85220dad1bce8e67560e728df196e797a0955e1c1dedd`) but changes
the full artifact hash. **The screen is therefore mechanism evidence, not a
performance claim for the final binary.**

This round ends with a clear tradeoff: mixed-load fairness and probe latency
improved materially at nearly neutral bulk throughput, but ordinary c1024
throughput parity was not demonstrated and a residual scheduling instability
remains. The retagged v0.0.1 ships the correctness-gated implementation with
that limitation disclosed.

## Rejected designs

Every entry below was implemented, gated for correctness, measured against a
frozen baseline, and then **removed from the source**. Headline deltas are the
paired geometric throughput deltas for the candidate relative to its baseline;
the rejection reason is what actually decided it.

Baseline legend: `c9c978fb` published v0.0.1 runtime · `80d71422` monitor
balance · `f1841caa` worker driver.

| variant | base | headline throughput | why rejected | candidate | local run |
|---|---|---|---|---|---|
| Overloaded bitmap + proactive donation | `c9c978fb` | c8 -1.52%, c64 -1.36%, c1024 -2.89% | Added cross-shard handoffs with no measurable benefit; source archived as `rejected-soft-donation-source-20260728.tar.gz` | `7d0b2532` | `wrk-screen-soft-completion-active-donation-20260728-001` |
| Soft-completion shards | `c9c978fb` | c64 -0.08%, c1024 -1.93% | Claimed-idle MPSC delivery raised CPU without stable throughput recovery | `c771bab4` | `wrk-screen-soft-completion-shards-20260728-001` |
| Reactor owner ticket (early form) | `c9c978fb` | c64 -0.76%, c1024 +5.32% | Superseded by the retained ticket protocol; c64 cost not recovered | `18e56824` | `wrk-screen-reactor-owner-ticket-20260728-001` |
| Reactor owner deadline | `c9c978fb` | c64 -0.37%, c1024 +6.41% | First screen invalid (20 interrupted pairs); rerun did not justify the added deadline path | `c36a821c` | `wrk-screen-reactor-owner-deadline-20260728-002` |
| Reactor source rotation | `c9c978fb` | c64 -2.35%, c1024 +4.19% | Material low-load regression against the joint gate | `2521a9ef` | `wrk-screen-reactor-source-rotation-20260728-001` |
| Reactor batch tail sentinel | `c9c978fb` | c128 -11.64%, c1024 +9.49% | Mid-range collapse; gain confined to saturated cells | `669980f4` | `wrk-screen-reactor-batch-tail-sentinel-20260728-001` |
| Bounded `runnext` | `c9c978fb` | c176 -2.41%, c1024 -4.00% | Regressed the cell it was meant to help | `2653a97e` | `wrk-screen-bounded-runnext-20260728-001` |
| Owner-poll marker | `c9c978fb` | c192 -3.49%, c1024 +2.56% | Superseded by the coalesced ticket latch | `d07e4699` | `wrk-screen-owner-poll-marker-20260728-001` |
| Owner-poll phase | `c9c978fb` | c128 -1.98%, c1024 +6.41% | Superseded by the coalesced ticket latch | `6dd67347` | `wrk-screen-owner-poll-phase-20260728-001` |
| Foreign I/O batch (early) | `c9c978fb` | c64 -1.98%, c1024 -2.20% | Negative across nearly every cell | `c504cd62` | `wrk-screen-foreign-io-batch-20260728-001` |
| Monitor cadence 28 | `c9c978fb` | c64 -0.22%, c1024 +3.58% | Faster monitor polling made the short-sleeping monitor the common poller | `9ea3468a` | `wrk-tail-confirm-cadence28-20260728-002` |
| Monitor-local progress instead of ticket | `80d71422` | c64 +0.52%, c1024 +0.41% | Worsened p99 by about 4.7% and produced a 228 ms maximum-latency run | `60775f3d` | `wrk-screen-ticket-vs-progress-v1-20260728-001` |
| Drop `finish_io_batch()` non-empty check | `80d71422` | c64 -1.74%, c1024 -0.47% | Throughput cost with no compensating latency gain | `32bc6663` | `wrk-screen-ticket-vs-unconditional-publish-v1-20260728-001` |
| Foreign-completion batching | `80d71422` | c64 -1.38%, c1024 +1.48% | c64 p50 +3.01% and p99.99/p99.999/Max +13.44%/+58.34%/+58.78%; the better c1024 aggregate reversed by order (BA p99.99 +125.27%) | `afa00637` | `wrk-screen-monitor-batch-vs-foreign-batch-v1-20260729-001` |
| Boosted foreign dispatch | `80d71422` | c64 -1.38%, c1024 -0.26% | Negative in both cells | `26285928` | `wrk-screen-monitor-batch-vs-boosted-foreign-v1-20260729-001` |
| Foreign turn-32 | `80d71422` | c64 +1.20%, c1024 -0.31% | Gain did not survive the joint tail gate | `05af2954` | `wrk-screen-monitor-batch-vs-foreign-turn32-v1-20260729-001` |
| Normal-worker reactor rescue | `80d71422` | c64 -0.02%, c1024 -1.97% | Full correctness, sanitizer, race-repeat and soak gates passed, but c64 p99.999/Max +16.67%/+40.91% and c1024 p99.99/p99.999/Max +26.59%/+26.46%/+23.93%; AB stratum threw -5.64% throughput and +53.95% Max | `de4fc489` | `wrk-screen-monitor-batch-vs-worker-rescue-v1-20260729-001` |
| Fixed unconditional-yield budget, 64 ops | `f1841caa` | c64 +1.45% (+0.43% to +2.47%), c1024 -0.15% | c64 p99 +51.43% in all ten pairs, average latency +4.18%, p99.9 +2.85%; failed the joint latency gate | `12a964fc` | `wrk-confirm-global-driver-v1-vs-coop64-v1-balanced-20260729-001` |
| Fixed unconditional-yield budget, 32 ops | `f1841caa` | c64 -0.68%, c1024 -2.50% (-5.12% to +0.19%) | Halving the budget did not repair the tradeoff; average latency +5.06%, p90 +2.50%; apparent p99 benefit was position-dependent (AB -28.93%, BA +0.68%) | `bc830502` | `wrk-screen-global-driver-v1-vs-coop32-v1-balanced-20260729-001` |

One earlier v2-base variant has no separate HTTP screen: restricting each
steal-half propagation to one new searcher reduced perf task-clock and cycles by
about 4-5%, but regressed detached spawn by 1.4% and spawn-plus-join by 5.6% in
interleaved confirmation. It was removed. The one liveness-required transfer
that remains is the extra searcher credit granted to an original victim whose
FIFO is still published after a successful steal.

Rebuilding the tree after removing the rejected unconditional-yield variants and
their dedicated tests reproduced the retained worker-driver library and server
hashes byte for byte.

## Post-v0.0.1 screens

These ran on the same 24-core host with `bench_core` under an interleaved
alternating A/B harness (warm-up discarded, 14 pairs, both sides pinned to CPUs
0-23). They are **screens, not release evidence**: the publishable gate is the
frozen `wrk` matrix with disjoint pinned server/client sets, and `bench_core`
run-to-run variance on this host is large enough that geometric means and
medians disagree in sign on several rows.

### Rejected: lock-free MPMC ring for the shared fallback queue

Roadmap item "Lock-free MPMC ring for the shared fallback queue". A bounded
Vyukov MPMC ring (1024 slots) was placed in front of the global queue's mutex
deque, with the deque retained as the non-dropping overflow.

Two variants were measured against the retained mutex-only queue:

| variant | `go()` detached | `timer arm+fire` | verdict |
|---|---:|---:|---|
| ring for both `push()` and `push_batch()` | **-21.91%** | **+102.02%** | rejected |
| ring for `push()` only, batches on the deque | +5.65% | +4.30% | rejected |

The first variant's large `go()` gain came entirely from the batch path — a
full local FIFO spills half its contents through `push_batch()`. The same
change destroyed the timer path, which publishes a whole expiry batch at once:
per-item CAS costs one contended atomic per item where the deque costs one lock
plus a bulk insert. Both figures reproduced across two independent 14-pair runs.

Confining the ring to single pushes removed the timer regression and the `go()`
gain together, leaving a variant that was neutral in one run and broadly 2-8%
slower in its confirmation. Neither variant improves its intended regime outside
noise, so the ring and both variants were removed and the mutex queue retained.

The roadmap's precondition still stands and was not met: contention was never
demonstrated *at that queue*. `bench_core` does not saturate the global fallback
path, so this screen shows the ring costs something on paths that do not need it
rather than showing it fails under the contention it targets.

### Neutral: the descriptor-scoped cancellation check

Cancellation is checked at syscall admission, which put a new atomic load and
branch on every socket operation — the one hot-path cost added by the 0.1.0
work. Everything else it introduced is compile-time (the `Conn`/`PacketConn`/
`Listener` concepts, the error classifiers) or per-scope rather than
per-operation (`Timeout`).

The experiment isolates exactly that check: both sides are the same tree, with
the two `if (cancelled())` lines removed from `io_error()` and `begin_syscall()`
on the A side. Ten pairs at the saturated 64-connection cell, servers on CPUs
0-7 and `wrk` on 14 threads across 8-21. Local run
`wrk-cancel-check-cost-20260730-001`, publication-ready.

| | A, no check | B, with check | paired geometric B/A (95% CI) |
|---|---:|---:|---:|
| req/s | 799,184 | 796,067 | -0.38% (-2.88% to +2.18%) |

Server CPU was 7.97 cores on both sides and median p50 differed by one
microsecond. The interval straddles zero, so the check is not measurable at this
workload's resolution. That is a bound, not a proof of zero cost: it says the
cost is below what a saturated 64-connection cell can resolve over ten pairs.

The pointer load is deliberately `memory_order_relaxed`. Acquire would order it
against the publication in `set_cancel()`, but missing a binding published in
the same instant only defers cancellation to the next admission check, and a
parked operation is woken by the hook regardless — the same latitude
`set_deadline()` already has against an in-flight syscall.

### Resolved: c1024 parity, measured between clean commits

The retag round left c1024 throughput parity undemonstrated. That gap could not
be closed on its own terms — its baseline `f1841caa` was an intermediate
dirty-tree artifact that no longer exists — but the release-relevant question
can be asked in a form that only needs committed revisions: **does the shipped
retag cost c1024 throughput against the release before it?**

Both sides built from clean commits: A = `abf5672` ("Bound blocking admission
and prepare v0.0.1"), B = `b1dc55a` (tag `v0.0.1`). Ten pairs per cell, five AB
and five BA, servers on CPUs 0-7, `wrk` on 14 threads across CPUs 8-21, harness
on CPU 23. Local run `wrk-clean-retag-vs-pre-20260729-001`, publication-ready,
all 20 pairs valid.

| connections | A mean req/s | B mean req/s | paired geometric B/A (95% CI) | median p50 A/B | median p99 A/B |
|---:|---:|---:|---:|---:|---:|
| 64 | 774,555 | 806,357 | **+4.11%** (+3.32% to +4.90%) | 60.5/57.5 us | 483.5/565.0 us |
| 1024 | 788,465 | 848,720 | **+7.62%** (+5.13% to +10.16%) | 1075/1135 us | 4615/2030 us |

**c1024 is not at parity — it is 7.62% faster**, with p99 more than halved. Both
intervals exclude zero. The feared regression was an artifact of comparing
against the worker-driver intermediate rather than against the previous release.

The trade-off, by order stratum, since a tail claim needs both to agree:

| statistic | c64 AB / BA | c1024 AB / BA | reading |
|---|---|---|---|
| p99 | **+56.53% / +26.04%** | **-56.97% / -55.25%** | both reproduce: c64 worse, c1024 much better |
| p99.9 | +1.87% / +1.74% | -51.57% / -1.95% | c64 flat; c1024 improves, magnitude order-sensitive |
| Max | **+30.81% / +13.54%** | -22.26% / -2.11% | c64 worse in both strata; c1024 better in both |

**This is a newly disclosed cost.** The retag buys c1024 throughput and tail with
c64 tail latency: p99 rises 26-57% and Max 14-31%, and both reproduce in both
orders, so they are attributable to the change rather than to noise. The retag
notes recorded a c64 p99 that was "order-dependent" on a different baseline;
against the previous release it is not order-dependent, it is a regression.

Exact hashes: A
`206cc0047b599e1c8af01d7309a22ceb7b0ddbd00fcf0bce4a24b32b834faaea`, B
`becca46343ed53abab9406975956cd5a4bc3479ced75113e9dfe9d240a78ee65`, `wrk`
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588`. Both servers
were built from clean worktrees with
`g++ -std=c++20 -O2 -DNDEBUG -foptimize-sibling-calls`.

### Confirmed: the v0.0.1 libraries rebuild from the tag

The retag's artifacts were recorded as frozen dirty-tree builds, leaving open
whether they corresponded to any committed revision. Building a clean worktree
of tag `v0.0.1` (commit `b1dc55a`, `git status` empty) settles it for the
library:

| artifact | recorded | clean rebuild from `v0.0.1` |
|---|---|---|
| static library | `699fd88b69df76d8605a3b690d47adb52b6cab9d2964998eb6f01cb7942eedd6` | **identical** |
| shared library | `dca78bd1…350f3a` | **identical** (the record had dropped its last character) |
| HTTP server | `aa9834d2167a6436fb451a274abc5b8cdcb09aca05ea8239519d581cded43af4` | `becca46343ed53abab9406975956cd5a4bc3479ced75113e9dfe9d240a78ee65` |

Both libraries reproduce byte for byte, so the runtime measured in the retag
rounds does map to the tagged revision.

The server does not, and that is a gap in the record rather than in the source:
a benchmark server binary depends on the exact compile and link line, which the
retag notes never captured. The rebuild above used
`g++ -std=c++20 -O2 -DNDEBUG -foptimize-sibling-calls` against the static
library, and reproduces stably — the same hash came out of an independent build
of the same revision. Any future server hash should be recorded with its build
command.

### Neutral: the 0.1.0 I/O modules against v0.0.1

The post-v0.0.1 round refactored `TcpStream::connect()` (now
`TcpConn::dial()`) into
`begin_connect()`/`await_connect()` and routed the by-name overload through
`Dialer`, so the connect path changed shape. Everything else it added is new
surface. This matrix confirms the established-connection path did not move.

Ten pairs per cell, five AB and five BA, eight server workers on CPUs 0-7, the
same third-party `wrk` on 14 threads across CPUs 8-21, harness on CPU 23,
5-second warm-up and a separate 15-second measured window. Local run
`wrk-postround-io-modules-20260729-001`.

| connections | A mean req/s | B mean req/s | paired geometric B/A (95% CI) | median p50 A/B | median p99 A/B | server cores A/B |
|---:|---:|---:|---:|---:|---:|---:|
| 64 | 791,254 | 795,707 | +0.57% (-1.11% to +2.27%), neutral | 59/59 us | 547/519 us | 7.98/7.97 |
| 1024 | 840,762 | 835,546 | -0.65% (-3.68% to +2.48%), neutral | 1140/1145 us | 2070/2035 us | 7.92/7.91 |

Both intervals straddle zero. All 20 pairs had zero socket and HTTP errors, no
client-saturation warning, live servers and unchanged input hashes; the harness
reported the matrix publication-ready.

The deep percentiles were checked separately, by order stratum:

| statistic | c64 AB / BA | c1024 AB / BA | reading |
|---|---|---|---|
| p99.9 | -0.47% / +0.51% | **-15.86% / +33.09%** | c1024 reverses by order — noise, not a regression |
| p99.99 | -6.82% / -9.85% | -3.48% / -1.89% | improved in both strata |
| Max | -2.04% / -13.95% | -1.49% / -7.42% | improved in both strata |

The c1024 p99.9 aggregate looks worse in the combined median (6966 vs 10592 us)
but its two order strata disagree in sign, so under the joint gate it is not
attributed to the change. Nothing else moved outside noise.

Exact hashes: A
`becca46343ed53abab9406975956cd5a4bc3479ced75113e9dfe9d240a78ee65` (HEAD, the
v0.0.1 runtime), B
`1b550365c28f453a0b0171e77043804a5f23fff02710f2a1e2deabac6486da8d` (the 0.1.0
working tree), `wrk`
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588`. Side B is a
working-tree build, so this is engineering evidence until those changes are
committed.

### Confirmed: skew placement is uneven

The runtime-v2 skew caveat recorded that heavy connections were *possibly*
placed unevenly across reactor shards, but the harness never captured the
mapping, so the explanation stayed unverified.

`bench/echo-comparison/cio_echo.cpp` now records it. A task begins on the shard
`accept()` selected for it, so reading the worker id at task entry — before the
first suspension — is that placement. The server reports a per-shard table on
SIGINT/SIGTERM.

Five repeats of the 16-connection, 50%-heavy cell, server on CPUs 0-7 with eight
workers, load generator on CPUs 8-21. Eight heavy connections over eight shards;
an even placement would be one each:

| run | heavy connections per shard | idle shards | max/min |
|---:|---|---:|---:|
| 1 | 0, 0, 3, 0, 1, 2, 1, 1 | 1 | 3/0 |
| 2 | 2, 1, 3, 0, 0, 0, 2, 0 | 2 | 3/0 |
| 3 | 3, 0, 1, 1, 1, 0, 1, 1 | 2 | 3/0 |
| 4 | 1, 0, 1, 1, 0, 1, 2, 2 | 0 | 2/0 |
| 5 | 1, 1, 2, 1, 0, 0, 0, 3 | 1 | 3/0 |

Every run left at least one shard with no heavy connection while another carried
two or three. The mechanism is not a scheduler defect: `accept()` distributes
connections round-robin, but weight is a property of the traffic a connection
later carries, which is unknowable at accept time. Round-robin over connections
is therefore not round-robin over load.

This confirms the placement explanation for the earlier skew sensitivity. It
does not by itself justify descriptor migration — that remains gated on showing
migration recovers the lost throughput for less than it costs — but the
prerequisite measurement now exists.

### Not actionable: timer-heap lock removal

Roadmap item "Timer-heap lock removal". Before rewriting a structure whose
disarm handshake is load-bearing, the shard lock was screened for cost: its
`std::mutex` was swapped for a test-and-test-and-set spinlock, which strictly
undercuts an uncontended mutex — no futex path, no fairness bookkeeping.

If the lock were on the critical path the spinlock would have shown it. It did
not: `timer arm+fire (concurrent)` moved **+3.95%** (median +4.09%), the wrong
direction, across 12 pairs.

Timer shards are per-worker and armed by the worker already running the task, so
the lock is uncontended in the common path by construction; only cross-worker
disarm and monitor-driven expiry touch it from outside. There is no measurable
overhead for lock removal to remove, and removing it would put the
"disarm() must wait out a firing callback" invariant at risk for no return. The
screen was reverted and the mutex retained.

### Not measurable here: topology-aware victim selection

Roadmap item "Topology-aware / NUMA victim selection". Its precondition is a
measurement showing victim locality costs more than the added state. That
measurement cannot be taken on the benchmark host:

```text
physical_package_id   0        (single socket)
node0                 only NUMA node
cpu0 LLC shared_cpu_list   0-23   (all 24 CPUs share one last-level cache)
```

Every worker is in the same package, the same NUMA node and the same LLC, so a
topology-aware thief would always take the same-domain branch. Implementing it
here would add a `sched_getcpu()` call and a comparison to the steal path in
exchange for a distinction the hardware cannot express, and no A/B on this host
could confirm or refute it. The item stays open and needs a multi-socket or
multi-LLC machine.

## Work-aware quota parameter sweep

These runs selected the retained quota shape rather than rejecting a design.
All used the pipelined-bulk mixed harness against the worker-driver baseline
`f1841caa` unless stated otherwise.

| configuration | headline | candidate | local run |
|---|---|---|---|
| pipeline-64 bulk, quota 64 | screen only | `ef82a6b9` | `wrk-screen-pipeline64-workaware64-20260729-01` |
| pipeline-64 bulk, quota 128 | screen only | `05a53d28` | `wrk-screen-pipeline64-workaware128-20260729-01` |
| pipeline-64 bulk, quota 256 | screen only | `e32020fe` | `wrk-screen-pipeline64-workaware256-20260729-01` |
| quota 128 vs 256, head to head | c64 -0.85% | `e32020fe` vs `05a53d28` | `wrk-headtohead-pipeline64-workaware128-vs-256-20260729-01` |
| outlined vs inlined checkpoint | c64 +0.12% | `0fed1777` vs `05a53d28` | `wrk-screen-pipeline64-workaware128-outline-vs-inline-20260729-01` |
| pipeline-256 bulk, quota 128 | c64 -2.50% | `0fed1777` | `wrk-screen-pipeline256-workaware128-20260729-01` |
| pipeline-256 bulk, quota 256 | c64 -0.70% | `cf4c3e6e` | `wrk-screen-pipeline256-workaware256-20260729-01` |
| adaptive 128 + local 256 | c64 -0.31% | `c4d65a8f` | `wrk-screen-pipeline256-workaware-adaptive128-local256-20260729-01` |
| adaptive vs fixed 256, head to head | c64 -0.34% | `c4d65a8f` vs `cf4c3e6e` | `wrk-headtohead-pipeline256-workaware256-vs-adaptive128-local256-20260729-01` |
| adaptive, six-pair confirmation | c64 -0.90% | `c4d65a8f` | `wrk-confirm-pipeline256-workaware-adaptive128-local256-20260729-01` |
| adaptive + idle publish | c64 +0.41% | `ae6ae7ae` vs `c4d65a8f` | `wrk-screen-pipeline256-workaware-idlepublish-20260729-01` |
| allocation-free checkpoint | c64 +1.16%, c1024 +0.94% | `41ba0169` | `wrk-screen-workaware-noalloc-20260729-01` |

The retained shape is adaptive-128 with a 256-operation local grace quota, idle
publish and an allocation-free outlined checkpoint.

## Instrumentation diagnostics

Two runs measured the cost of the metrics build itself by comparing a binary
with itself, so both sides carry identical instrumentation:

| run | cells | note |
|---|---|---|
| `wrk-scheduler-metrics-diag-20260728-001` | c256 -0.26%, c768 -1.96% | self-comparison, `f81f02b5` both sides |
| `wrk-reactor-source-metrics-diag-20260728-001` | c64 +1.52%, c256 -1.43%, c768 +0.11%, c1024 -14.88% | self-comparison, `aa295517` both sides; the c1024 figure is run-to-run drift, not an effect |

The second run's c1024 spread is a useful calibration: on this host, a single
saturated cell can drift by well over 10% between otherwise identical sides.
That is why every retained claim requires paired, alternating runs.

## References

- [Runtime v2 design](scheduler-v2.md)
- [Roadmap and open items](roadmap.md)
- [HTTP comparison harness and methodology](../bench/http-comparison/README.md)
- [Echo comparison](../bench/echo-comparison/README.md)
