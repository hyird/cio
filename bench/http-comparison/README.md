# HTTP comparison: cio vs Boost.Asio vs Go, driven by wrk

A minimal HTTP/1.1 server on each of the three runtimes, under a load generator
that none of them is built on.

```
./run_wrk.sh [connections] [duration_s] [repeats] [threads]
```

## Why this exists

The echo comparison next door has a flaw it names but cannot fix from the
inside: its load generator is written in cio. That was mitigated by giving the
generator twice the cores and checking it never saturated, and by using the same
generator for every server so the bias is common-mode. Both are true and neither
is sufficient, because a change to cio changes the *generator* too.

That is not hypothetical. The same scheduler change measured **+7.9%** against
the generator as it was built, and **+50%** after the generator was rebuilt on
the improved runtime — because the old generator was closer to being the binding
constraint, and part of the second number is the client getting faster. Neither
figure is the server on its own, and no amount of care inside that harness can
separate them.

`wrk` is a third party. It is not built on any runtime under test, it does not
change when they do, and it is the same binary for all three. That is the whole
point of it being here.

## What is being compared

| | architecture |
|---|---|
| **cio** | work-stealing M:N, one shared reactor, one task per connection |
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
