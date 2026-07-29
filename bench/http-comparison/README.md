# HTTP comparison: cio vs Boost.Asio vs Go, driven by wrk

A minimal HTTP/1.1 server on each of the three runtimes, under a load generator
that none of them is built on.

> This document covers the harnesses and the three-runtime comparison. Scheduler
> A/B results live in [the benchmark record](../../docs/scheduler-results.md);
> the architecture discussion below predates worker-local reactor shards.

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

## Quick three-runtime comparison

Using locally built `cio`, Asio and Go servers:

```
./run_wrk.sh [connections] [duration_s] [repeats] [threads]
```

This is a smoke comparison, not a gate. Publishable A/B evidence uses
`matrix_wrk.py` above.

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
| **cio** | work-stealing M:N, worker-local epoll shards, one task per connection |
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
