# Roadmap and open items

Status: current as of the v0.0.1 retag plus the unreleased resolver,
dialer and stream-algorithm work.

This is the single place where deferred work and unresolved questions are
tracked. Items were consolidated from the design documents, which previously
carried them inline as "later", "deferred" and "not demonstrated" remarks.

Each item records what would have to be true to close it. An item is not a
commitment to build; several are recorded specifically so they are not
attempted again without new evidence.

## Open evidence gaps

These block release claims, not functionality. The implementation is
correctness-gated; the measurement record is incomplete.

### c1024 parity — resolved, and it exceeded parity

*Closed.* The gap could not be closed on its own terms: its baseline `f1841caa`
was an intermediate dirty-tree artifact that no longer exists. Asked in the form
that only needs committed revisions — does the shipped retag cost c1024
throughput against the release before it? — the answer is a publication-ready
**+7.62%** (95% CI +5.13% to +10.16%) with c1024 p99 more than halved, measured
between clean commits `abf5672` and `b1dc55a`.

**A new cost was disclosed in the process:** c64 p99 rises 26-57% and Max 14-31%
against the previous release, reproducing in both order strata, so it is
attributable to the retag rather than to noise. See
[the matrix](scheduler-results.md#resolved-c1024-parity-measured-between-clean-commits).

### The retag's runtime does map to the tag — closed for libraries

*Closed for the runtime itself.* A clean worktree of tag `v0.0.1` rebuilds the
recorded static library `699fd88b…` and shared library `dca78bd1…350f3a` byte
for byte, so the runtime measured in the retag rounds corresponds to a committed
revision after all. The record's shared-library hash was one character short and
has been corrected against the rebuild.

**Still open, narrowly:** the recorded *server* hash `aa9834d2…` does not
reproduce, because the retag notes never captured the compile and link line a
benchmark server binary depends on. Future server hashes must be recorded with
their build command. See
[the rebuild](scheduler-results.md#confirmed-the-v001-libraries-rebuild-from-the-tag).

### The `cc5b9945` screen has no retained raw results — superseded

The cited four-pair screens for `cc5b9945…` have no manifest in the local
archive, and their numbers still rest on the write-up alone. They are retained
with that caveat rather than deleted.

*No longer gating.* Those screens existed to answer whether the shipped retag
regressed c1024, and that question now has a publication-ready answer from two
clean commits (above). The unsourced figures describe an intermediate build that
is not the release and cannot be reconstructed; re-running them would measure a
tree nobody ships.

**Residual:** the recorded server hash `aa9834d2…` still does not reproduce,
because no build command was recorded with it. That is folded into the item
below.

### Skew placement — confirmed, closed

*Closed.* `bench/echo-comparison/cio_echo.cpp` now records the shard `accept()`
placed each connection on and reports a per-shard table on SIGINT/SIGTERM.
Five repeats of the 16-connection/50%-heavy cell showed every run leaving at
least one shard with no heavy connection while another carried two or three.

Round-robin accept placement distributes *connections* evenly, but weight is a
property of traffic a connection carries later and is unknowable at accept time,
so heavy connections cluster by chance. See
[the measurement](scheduler-results.md#confirmed-skew-placement-is-uneven).

### Extreme-tail cost of rare foreign-monitor dispatch

The monitor-balance round improved p90 and p99 substantially while raising
p99.99, p99.999 and Max by 22.29%, 13.95% and 17.89%. Four designs aimed at that
path were measured and all four were rejected — foreign-completion batching,
boosted foreign dispatch, foreign turn-32 and normal-worker reactor rescue.

**Do not retry another dispatch heuristic without new evidence.** The rejected
set already covers batching, prioritization, quota and worker-rescue variants.
A fifth attempt needs a diagnosis of *why* the rare path is slow, not another
policy.

## Deferred runtime work

Explicitly out of scope for runtime v2, and unblocked now that its stage 6 has
passed. None is scheduled.

| item | status | condition to start |
|---|---|---|
| Topology-aware / NUMA victim selection | **blocked on hardware** | Implemented nowhere: the benchmark host is single-socket, single-NUMA, one LLC shared by all 24 CPUs, so a topology-aware thief always takes the same-domain branch and no A/B here can confirm or refute it. Needs a multi-socket or multi-LLC machine. See [the screen](scheduler-results.md#not-measurable-here-topology-aware-victim-selection). |
| Lock-free MPMC ring for the shared fallback queue | **tried and rejected** | Built and measured in two variants; neither improved its regime outside noise, and the ring-for-batches variant regressed timers 102%. Removed. See [the screen](scheduler-results.md#rejected-lock-free-mpmc-ring-for-the-shared-fallback-queue). Reopening needs contention demonstrated *at that queue*, which `bench_core` does not produce. |
| Timer-heap lock removal | **not actionable** | Screened by swapping the shard mutex for a spinlock, which strictly undercuts an uncontended mutex: timers got 3.95% *slower*. The lock is per-worker and uncontended by construction, so there is no overhead to remove. See [the screen](scheduler-results.md#not-actionable-timer-heap-lock-removal). |
| Descriptor migration between epoll instances | deferred; prerequisite met | The skew harness now records shard mapping and placement imbalance is confirmed, so the motivating measurement exists. Still gated on the hard part: edge-triggered DEL/ADD/readiness races, and evidence that migration recovers the lost skew throughput for less than it costs. A cheaper alternative to evaluate first is weight-aware placement at accept time, which needs no descriptor to move. |
| `SO_REUSEPORT` listener replication | out of scope | Would be a new listener feature, not routing for an existing operation. |
| Dynamic worker resizing, priorities, preemption, task pinning | out of scope | Scheduling stays cooperative. |
| Public executor, affinity, shard, completion-token or migration controls | **rejected by design** | No processor, shard, executor, affinity or thread object appears in a public header. |
| io_uring, kqueue, IOCP backends | **rejected by design** | Linux and epoll remain the only readiness backend. |
| Go-style G/M/P processor/carrier handoff | **rejected by design** | The measured deficit is cache stalls on shared scheduling state, which G/M/P does not remove. |

## Planned I/O infrastructure

The additive API design is specified in
[io-infrastructure.md](io-infrastructure.md). Its fixed directions are Go-style
public APIs, epoll for readiness, and no io_uring. Stages are independently
releasable and must be taken in order.

| stage | scope | state |
|---|---|---|
| 1. Blocking admission | Global FIFO bound, `Errc::overloaded` rejection, shutdown rejection | **Done.** Per-class admission with independent wait queues; a task waiting for admission parks without occupying a pool thread, and admission waiters are completed with `Errc::shutdown` when the pool stops. |
| 2. Resolver and dialer | `Resolver`, `Dialer`, connect cancellation, combined deadlines | **Done**, including concurrent attempt racing. |
| 3. Files | `cio::fs::File`, positioned I/O, sync, stat, truncate, file-class admission | **Done.** `cio/fs.hpp`; no cancellation or deadline, by design. |
| 4. Generic stream algorithms | `AsyncReader`/`AsyncWriter` concepts, `read_exact()`, `copy()` | **Done.** `cio/io.hpp`; the `TcpConn::write_all()` member is retained alongside the free function. |
| 5. Optional TLS and signal modules | `cio::tls` on OpenSSL; `signalfd`-based signals with a startup contract | **Done.** Signals ship in the core (`cio/signal.hpp`, no new dependency); TLS is the optional `cio::tls` target behind `-DCIO_TLS=ON`. |

Every stage is now implemented. What remains within this plan is breadth rather
than structure: directory traversal, path mutation and whole-file helpers on top
of `File`, and ALPN/session-resumption knobs on `TlsStream`. None needs another
reactor or a new backend.

### A cancelled name lookup no longer leaks — closed

*Closed.* The cancellable lookup no longer runs as a detached coroutine. It is a
self-owned heap job that delivers through a non-suspending `try_send()`, so
there is no frame to strand when `Runtime::shutdown()` joins the workers before
stopping the blocking pool.

`test_cancelled_lookup_leaves_nothing_at_shutdown` provokes the window
deterministically by holding the single resolver admission slot, so the lookup
is provably still queued when it is cancelled and when the runtime stops. The
test was verified to have teeth: reinstating the detached version made ASan
report a leak in 3/3 runs, and the fix reports none in 6/6.

### This round's changes have an HTTP matrix — closed

*Closed.* A publication-ready ten-pair matrix against the v0.0.1 server was
neutral in both cells: c64 +0.57% (-1.11% to +2.27%) and c1024 -0.65% (-3.68% to
+2.48%). Deep percentiles improved or reversed by order; nothing moved outside
noise. See
[the matrix](scheduler-results.md#neutral-the-010-io-modules-against-v001).

Side B was a working-tree build, so it is engineering evidence until these
changes are committed; the run should be repeated from the release commit.

### Cancellation must close, not merely abandon

Cancelling a socket operation has to close the descriptor, and every spawned
attempt has to be joined before its parent returns.

The first implementation of cancellable connect detached the attempt and let
the caller stop waiting. It resumed the caller correctly, but the abandoned
connect stayed parked until the kernel gave up on its SYN retries, and if the
runtime shut down first the frame leaked outright — `Runtime::shutdown()` does
not unwind tasks parked on a socket. ASan caught it as an intermittent leak.

Both cancellable connect and `Dialer` now close the descriptor on cancellation
and join every attempt; the racing dialer cancels every loser before joining it,
which is what makes the join prompt. Any future cancellable operation must do
the same.

Not part of this plan at any stage: HTTP, a userspace DNS protocol
implementation, a DNS cache, a second event backend, or Asio-style execution
machinery in the public API.

## Documentation and tooling

| item | status |
|---|---|
| Release number for the unreleased changes | *Decided.* The additions are API-additive under a 0.x major, so the next release is **0.1.0**; `CMakeLists.txt` carries it and `CHANGELOG.md` heads the section `[0.1.0] — unreleased`. Tagging is left to the maintainer, since it is a published, immutable act. |
| Local results archive | *Decided: accept that it is host-local.* `bench/http-comparison/results/` holds ~36 MB across 47 runs and stays untracked per `.gitignore`. Committing 36 MB of logs to make prose auditable is the wrong trade; instead, [scheduler-results.md](scheduler-results.md) states inline every number it relies on, so no claim depends on reading the archive. Directory names are retained as provenance for whoever has the host. |

## References

- [Runtime v2 design](scheduler-v2.md)
- [Scheduler benchmark record](scheduler-results.md)
- [I/O infrastructure design](io-infrastructure.md)
- [Repository development rules](../AGENTS.md)
