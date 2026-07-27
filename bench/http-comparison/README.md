# HTTP comparison: cio vs Boost.Asio vs Go, driven by wrk

A minimal HTTP/1.1 server on each of the three runtimes, under a load generator
that none of them is built on.

> Except for the frozen A/B section immediately below, the recorded results and
> architecture discussion are the pre-runtime-v2 baseline. Current cio uses
> worker-local reactor shards and directed inboxes; see
> [the runtime-v2 design](../../docs/scheduler-v2.md).

## Runtime v2 frozen A/B

Seven warmed, alternating A/B pairs pinned the server to CPUs 0-7 and `wrk` to
8-23. One connection used one `wrk` thread; 64 connections used eight:

| connections | pre-v2 A req/s | retained v2 B req/s | paired geometric B/A | median p50 A/B | median p99 A/B |
|---:|---:|---:|---:|---:|---:|
| 1 | 14,209 | 13,870 | -2.39%, neutral | 68/68 us | 112/105 us |
| 64 | 634,737 | 783,776 | **+23.48%** | 78/63 us | 716/750 us |

The one-connection log-ratio interval was approximately -7.6% to +3.2%, so its
negative centre is not a confirmed regression. At 64 connections the
throughput interval was approximately +22.0% to +25.0%; server CPU rose from
58.41 to 62.78 core-seconds per 8-second window (+7.47%).

Server hashes were
`1970c98716225a93d66a9b662ece04d0d93629ecd71c5b1c6356c9c7b97bf3e5`
(A) and
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf`
(B).

## Frozen A/B matrix runner

`matrix_wrk.py` is the release-gate runner for comparing two cio HTTP server
binaries. It defaults to the 1/8/64/256/1024-connection matrix, assigns an
appropriate `wrk` thread count to each cell, alternates AB/BA pairs, rotates
cell order, and pins the server and client to disjoint CPU sets.

```sh
python3 matrix_wrk.py \
  path/to/pre-v2-server path/to/candidate-server \
  --cells 1:1,8:2,64:8,256:8,1024:8 \
  --pairs 10 --warmup 5 --duration 15 \
  --expected-a-sha256 <sha256> \
  --expected-b-sha256 <sha256> \
  --expected-wrk-sha256 <sha256>
```

Every run performs an HTTP correctness probe, rejects socket/non-2xx/server
failures, verifies input hashes again at the end, and retains the manifest, raw
CSV, summaries and per-side logs. An interrupted or incomplete pair makes the
matrix invalid rather than silently reducing the sample count. Default outputs
go under `results/`, which is intentionally ignored; publish only a reviewed
result set with its manifest and exact source revisions.

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
