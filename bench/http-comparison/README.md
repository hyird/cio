# HTTP comparison: cio vs Boost.Asio vs Go, driven by wrk

A minimal HTTP/1.1 server on each of the three runtimes, under a load generator
that none of them is built on.

> Historical results and architecture discussion are the pre-runtime-v2
> baseline. Sections explicitly marked runtime v2 or v0.0.1 retag, plus the
> current runners, describe later work. See
> [the runtime-v2 design](../../docs/scheduler-v2.md).

## Runtime v2 frozen A/B matrix

The publishable confirmation compared a clean build of pre-v2 commit `899ccad`
with a clean build of retained-v2 commit `5e0208b`. It used ten warmed, paired
runs per cell, five in AB order and five in BA order. The server used eight
workers pinned to CPUs 0-7; the same third-party `wrk` binary was pinned to
CPUs 8-23. Each side had a 5-second warm-up and a separate 15-second measured
window.

| connections | wrk threads | pre-v2 A req/s | retained v2 B req/s | paired geometric B/A (95% CI) | median p50 A/B | median p99 A/B | server cores A/B |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 14,242 | 14,339 | +0.67% (-3.88% to +5.42%), neutral | 67/68 us | 109.5/110 us | 0.37/0.37 |
| 8 | 4 | 80,645 | 125,598 | **+56.00%** (+49.52% to +62.77%) | 92/52 us | 147/644 us | 2.64/2.75 |
| 64 | 16 | 729,546 | 789,141 | **+8.16%** (+6.84% to +9.50%) | 76/70 us | 518/501 us | 7.89/7.97 |
| 256 | 16 | 773,023 | 771,416 | -0.22% (-1.63% to +1.21%), neutral | 303.5/306 us | 915/2765 us | 7.91/7.92 |
| 1024 | 16 | 714,915 | 781,518 | **+9.34%** (+7.22% to +11.49%) | 1355/1125 us | 3900/4635 us | 7.64/7.90 |

Positive B/A throughput means B is faster. The intervals are per-cell paired
log-ratio Student-t intervals and are not adjusted for cross-cell claims.
Every one of the 100 measured sides had zero socket errors, zero non-2xx
responses, a successful `wrk` exit and a live server. Input hashes were
unchanged at the end.

The result is not a single throughput headline. At eight connections B gains
throughput and median latency, but its median p99 is 4.38 times A. At 64
connections throughput and both reported latency percentiles improve. At 256
connections throughput is neutral while median p99 is 3.02 times A. At 1024
connections B gains 9.34% and lowers p50, but median p99 rises by about 19%.

This is a gate for the complete retained runtime-v2 build, not an estimate of
the MPSC inbox's isolated causal effect: A and B differ in more than that one
component. An MPSC-only claim requires the same matrix over two otherwise
identical frozen builds.

An earlier complete screen used two `wrk` threads at c8 and eight at the three
larger cells. It passed correctness gates but reached at least 95% of the
configured client-thread capacity, so it is not the published result.
No number from that screen is combined with the clean-source confirmation.
A load-generator saturation warning makes a run a capacity screen, not release
evidence.

The exact executable hashes were
`5650865ce18c6d029fbd0546b0ee9a6d7758da5087038f8f8db15664f78750e8`
(A),
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf`
(B), and
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588`
(`wrk`).

A's static library hash is
`d58b29801a3691af204652208c6a1397edefa2ec81eb4c3aff0693d8923e5570`;
B's is
`6a27bb4da89479c1718b220d5df954d8dbef3fc952926e7d51eafdf33a2ae8f9`.
Independent clean builds reproduced the A library, and a clean B rebuild
reproduced both retained B hashes.

An earlier matrix used A executable
`1970c98716225a93d66a9b662ece04d0d93629ecd71c5b1c6356c9c7b97bf3e5`,
which was itself byte-reproducible but combined a dirty 09:45 UTC library with
the 09:57 header state. The provenance audit rejected it because it did not
correspond to one clean frozen source revision. It is retained only as a
historical diagnostic; none of its values appears in the table above.

## Frozen A/B matrix runner

`matrix_wrk.py` is the release-gate runner for comparing two cio HTTP server
binaries. It defaults to the 1/8/64/256/1024-connection matrix, assigns an
appropriate `wrk` thread count to each cell, alternates AB/BA pairs, rotates
cell order in AB/BA blocks, and pins the server, client and harness to disjoint
CPU sets.

```sh
taskset -c 23 python3 matrix_wrk.py \
  path/to/pre-v2-server path/to/candidate-server \
  --cells 1:1,8:4,64:14,256:14,1024:14 \
  --pairs 10 --warmup 5 --duration 15 \
  --server-cores 0-7 --client-cores 8-21 \
  --tail-script ./wrk_tail.lua \
  --expected-a-sha256 <sha256> \
  --expected-b-sha256 <sha256> \
  --expected-wrk-sha256 <sha256> \
  --expected-tail-script-sha256 <sha256>
```

Every run performs an HTTP correctness probe, rejects socket/non-2xx/server
failures, verifies input hashes again at the end, and retains the manifest, raw
CSV, summaries and per-side logs. An interrupted or incomplete pair makes the
matrix invalid rather than silently reducing the sample count. Default outputs
go under `results/`, which is intentionally ignored; publish only a reviewed
result set with its manifest and exact source revisions.

The harness itself is hash-frozen and must be launched on CPUs outside both
measured sets. An even minimum of four pairs is required. Each rotated cell
order is held for one AB/BA block, so cell position is not confounded with side
order in a two-cell confirmation. A saturated-only matrix can additionally set
`--min-server-utilization 0.95`; a cell below that mean worker-capacity gate is
complete but not publication-ready.

`wrk_tail.lua` is optional and only defines wrk's post-run `done()` callback.
It reads wrk's already-collected histogram after the measured window and adds
p99.9, p99.99, p99.999 and Max to the raw and summary outputs; it has no
per-request hook. When enabled, the runner freezes and rechecks the script hash
like the two servers and the `wrk` executable. Deep percentiles are diagnostic
until they reproduce in both AB and BA order; a single Max sample is not a
latency distribution.

The manifest's `publication_ready` field is the measurement-quality gate:
correctness, completeness, stable hashes and sufficient client capacity. It
cannot prove how an input binary was built, so release evidence additionally
requires the clean source revisions and build-artifact hashes recorded above.
It is also not a performance verdict. Scheduler candidates are retained only
after a separate joint review of throughput, p50-p99 and the diagnostic
p99.9/p99.99/p99.999/Max fields, including AB/BA order strata. An aggregate
gain that reverses by order, or a throughput gain paid for by a material
extreme-tail regression, fails that review even when the matrix is
publication-ready.

Do not publish a cell carrying the client-saturation warning. Increase its
`wrk` thread count or client CPU capacity and rerun the complete paired matrix;
the warning-free confirmation, not the saturated screen, is the result. The
runner records `publication_ready: false` and exits nonzero when this capacity
gate fails, even if all correctness pairs are otherwise valid.

## Mixed bulk/latency runner

`mixed_wrk.py` measures the scheduler tradeoff that one saturated throughput
run cannot show. One pinned `wrk` process keeps the server busy with
256-request HTTP pipelines, while a second, disjoint `wrk` process measures a
small ordinary connection set with the full latency histogram. The bulk result
answers how much capacity the fairness mechanism costs; only the ordinary
probe's latency is used as a request-latency result. `wrk`'s histogram is not a
valid per-request latency distribution for the pipelined stream itself.
Each side first runs and discards a separate bulk warm-up. The measured bulk
and probe processes then start together with the same duration, so bulk RPS,
probe latency and server CPU describe the same mixed-load window.

The harness must also be pinned outside all server and client CPU sets. On the
24-core benchmark host:

```sh
taskset -c 23 python3 mixed_wrk.py \
  path/to/baseline-server path/to/candidate-server \
  --pairs 4 --bulk-warmup 5 --duration 15 \
  --server-cores 0-7 \
  --bulk-cores 8-17 --bulk-threads 10 --bulk-connections 64 \
  --probe-cores 18-21 --probe-threads 4 --probe-connections 4 \
  --expected-a-sha256 <sha256> \
  --expected-b-sha256 <sha256> \
  --expected-wrk-sha256 <sha256> \
  --expected-bulk-script-sha256 <sha256> \
  --expected-probe-tail-script-sha256 <sha256> \
  --output results/<new-directory>
```

The bulk script is frozen to `wrk_pipeline_256.lua`, and the probe script is
frozen to the report-only `wrk_tail.lua`; custom workload scripts are rejected.
Publication runs require at least four pairs and equal AB/BA strata.
The runner rejects overlapping CPU sets, an unpinned harness, more threads than
assigned CPUs, socket/HTTP/server failures, input hash drift, insufficient
bulk/probe overlap, client saturation and an underutilized server. It freezes
and rechecks the harness and imported matrix helper as well as both binaries,
`wrk` and the Lua scripts. Any capacity or completeness failure sets
`publication_ready: false` and exits nonzero.

The output retains both client logs, the discarded warm-up, server logs, raw
samples, order-stratified paired summaries and an environment manifest.
Positive B/A means higher throughput for the RPS fields but worse latency for
the latency fields, so the two groups must always be reported together.
`publication_ready` validates the measurement procedure, not whether a
throughput/latency tradeoff is worth retaining.

At the ordinary probe's deliberately low request rate, p50-p99 are the primary
latency evidence and p99.9 is a diagnostic. The manifest records the minimum
request count and expected samples beyond each deep percentile. When fewer
than 10,000 or 100,000 requests are available, p99.99 or p99.999 respectively
is marked unresolved; those fields often collapse to one extreme sample or
Max and are not independent distribution claims.

## v0.0.1 retag: work-aware scheduler result

The 2026-07-29 experimental round used `/usr/bin/wrk`, servers on CPUs 0-7,
clients on 8-21 and the harness on 23. The publication-ready ten-pair standard
confirmation of the first work-aware quota build was neutral at c64
(-0.34%, 95% CI -2.21% to +1.57%), but lost 2.52% throughput at c1024
(-3.79% to -1.24%); paired p50 and p90 rose 3.47% and 2.33%. Its separate
four-pair mixed confirmation kept pipelined bulk throughput neutral at -0.07%,
while the ordinary probe gained 100.61% throughput and reduced p50, p90 and
p99 by 49.47%, 38.53% and 30.57%.

A GCC follow-up removed the TCP success flag and relaxed the completion counter
to one direct TLS decrement plus branch. Its four-pair screens kept mixed bulk
neutral at -0.22%, improved probe p50/p90/p99 by 47.19%/37.02%/31.73%, and
moved standard c64 throughput +0.91%. Standard c1024 did not establish parity:
the paired summary was -5.07% with a wide -16.24% to +7.60% interval, including
one valid -15.61% pair alongside three pairs from -0.50% to -1.95%.

The screen binary was
`cc5b9945e734c0e17589af92bb98700b8e940a51803f0c5992597b763c109bed`.
The final shared-safe dual-symbol TLS refinement changed the server hash to
`aa9834d2167a6436fb451a274abc5b8cdcb09aca05ea8239519d581cded43af4`
while preserving the screen binary's entire `.text` section byte for byte.
Consequently these screens are diagnostic mechanism evidence, not exact-final
performance evidence for the retag. The round demonstrates a strong mixed-load
latency/fairness gain with nearly neutral bulk capacity, but it does not
demonstrate ordinary c1024 throughput parity; that limitation is part of the
v0.0.1 retag record.

For a quick three-runtime comparison using locally built `cio`, Asio and Go
servers:

```
./run_wrk.sh [connections] [duration_s] [repeats] [threads]
```

## Why this exists

The echo comparison next door uses a load generator written in cio. Its current
frozen A/B procedure builds that generator once and uses the identical binary
for both runtime sides in an independently pinned process, so changing the
server runtime does not change the client during that comparison. The generator
is still project code rather than a third-party implementation.

The freeze matters. Before that procedure was adopted, the same scheduler
change measured **+7.9%** against the generator as it was built, and **+50%**
after the generator was rebuilt on the improved runtime — because the old
generator was closer to being the binding constraint, and part of the second
number was the client getting faster. Those two figures were not comparable
server-only measurements.

`wrk` is a third party. It is not built on any runtime under test, it does not
change when they do, and it is the same binary for all three. That is the whole
point of it being here.

## What is being compared

| | architecture |
|---|---|
| **cio (measured snapshot)** | work-stealing M:N, one shared reactor, one task per connection |
| **asio** | shared-nothing: one `io_context` + one `SO_REUSEPORT` acceptor per thread, callbacks |
| **go** | goroutine per connection, `GOMAXPROCS` set to the thread count |

All three servers are hand-rolled minimal HTTP: read, find the blank line, write
a fixed response. Explicitly **not** `net/http`, and not an asio HTTP library.
That is not laziness — `net/http` does routing, header parsing and `Date`
formatting that the C++ servers do not, and that difference would swamp the
runtime difference this benchmark exists to measure. What is left is the same
work on three schedulers.

The response bytes are identical across all three, and so is the request
framing: `http_common.hpp` and its transcription in `go_http.go` count complete
requests with the same carry-across-reads state machine. Responding once per
`read()` would be wrong in two directions — a request split across packets would
draw two responses, two pipelined requests in one read would draw one — and with
wrk on loopback neither happens often enough to show up as an error, only as a
number. That is exactly the kind of bug worth not having.

## Methodology

The same rules as the echo comparison, because they are what make any of it
worth reading:

- **Disjoint cores.** Server pinned to CPUs 0–7, wrk to 8–23.
- **Warm-up excluded.** A full warm-up run first, then a separate measured run,
  with server CPU sampled across the measured window only. For Go this matters
  more than for the others: the GC and the scheduler's Ms have to reach steady
  state.
- **Interleaved, rotating order.** Servers alternate within a repeat and the
  order rotates each repeat, so drift across the run cannot land on one server.
  Run-to-run drift on this class of machine is larger than the effects being
  measured; a claim from two separate sweeps is not a claim.
- **Both sides' load reported.** Server CPU is printed for every run. If a
  server is not near saturation the number is about latency, not capacity, and
  the two answer different questions.
- **Errors are printed, not swallowed.** Socket errors and non-2xx responses
  appear on the result line. A fast server that is answering wrongly is not
  fast.

Caveats that do not go away: this is loopback, so it measures the runtime and
the kernel's TCP path with no network in it; and a fixed 13-byte response is
almost pure I/O dispatch, which is the workload that flatters shared-nothing
designs most. The echo comparison's skew sweep is where that gets charged for.
