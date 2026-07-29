# Runtime: affinity-sharded scheduler and reactor

The scheduler is shared-nothing while load is balanced, and shares work only
after imbalance is published. This document is the design and its load-bearing
invariants.

The measurement record behind every decision is in
[scheduler-results.md](scheduler-results.md); deferred work is in
[roadmap.md](roadmap.md). One limitation qualifies the design's own claim:
ordinary 1024-connection throughput parity was **not** demonstrated for the
final work-aware build.

## Why this shape

Two simple answers were ruled out by measurement. Pure shared-nothing has the
best balanced hot path but strands work on a busy shard. A global G/M/P
scheduler improves blocking-syscall integration but adds ownership state to
every scheduling path — and the measured deficit was cache stalls on shared
scheduling state, which G/M/P does not remove.

The evidence pointed at shared scheduler/reactor state and unsuccessful
stealing rather than coroutine machinery or the socket syscall fast path: the
runtime retired fewer instructions than a shared-nothing Asio server but took
27% more cycles, with lower IPC and more cache misses, and removing one third of
the network syscalls improved throughput only 1.6%. Details are in
[the echo comparison](../bench/echo-comparison/README.md#where-the-gap-actually-is).

Hence the hybrid: worker-owned shards, with stealing triggered by published
runnable state. The blocking executor stays outside the scheduler; blocking work
does not justify adding processor/carrier handoff state to the network and
channel hot paths.

## Semantic guarantees

No processor, shard, executor, affinity or thread object appears in a public
header. `RuntimeOptions` has no migration, affinity, reactor or queue option.
Internal tuning constants live only in `detail` code and are selected by
benchmarks, not by applications.

The following observable behaviour is a hard compatibility boundary:

- tasks are lazy, and directly awaiting a task is a symmetric transfer rather
  than an injected scheduling hop;
- `go()` is detached and `spawn()` is joinable. Concurrently registering a
  second join waiter is outside the single-waiter contract and deterministically
  throws `std::logic_error`;
- channel close, drain, send-on-closed, rendezvous and buffered FIFO behaviour;
- `select`'s ready-case selection, result indexing, timeout and retraction;
- `TaskGroup::join()` waits for every child, preserves the first failure and
  cooperatively cancels the rest;
- cancellation is cooperative and observable through `CancelToken::done()`;
- mutexes, wait groups, timers and sleep retain their wake and error behaviour;
- socket reads return zero for EOF, writes may be partial, deadlines are
  descriptor-scoped and persistent until reset, and `close()` wakes parked I/O;
- the one-reader/one-writer-per-socket rule and the socket lifetime rule;
- `Runtime::shutdown()` synchronously joins workers, so calling it from one of
  the same Runtime's worker tasks throws `std::logic_error` before stop or join
  begins;
- the error mapping and `Result<T>` representation.

Thread identity, worker identity, exact runnable order and fairness timing are
not public semantics. The scheduler may change them, but it may not turn a
suspension into a visible extra result, exception, callback, required option or
user-managed affinity operation. A task may resume on a different worker after
suspension.

## Architecture

```text
  Worker 0                         Worker 1
  +----------------------+         +----------------------+
  | fixed OS thread      |         | fixed OS thread      |
  | runnext + local FIFO |         | runnext + local FIFO |
  | remote MPSC inbox    |         | remote MPSC inbox    |
  | epoll shard + eventfd|         | epoll shard + eventfd|
  | timer shard          |         | timer shard          |
  | frame cache          |         | frame cache          |
  +----------+-----------+         +-----------+----------+
             |                                 |
             +-------- conditional steal ------+

  Shared, cold-path only:
      idle/stealable bitmaps
      overflow and external/non-local completion fallback queue
      blocking executor
      stale-reactor/timer monitor
```

Each worker remains one fixed OS thread. Local scheduling starts on the active
worker, but that initial enqueue is not affinity: published FIFO work may be
stolen before it runs. A worker-owned poller may retain readiness locally.
Only hard-directed internal remote submissions with a concrete ownership
target use the destination worker's inbox. Monitor or foreign-runtime I/O
dispatch, soft-affinity completions and timer batches fired outside their
preferred owner use the shared fallback path. Cross-worker coordination
otherwise happens only for an actual remote wake, an overflow or a published
load imbalance.

The blocking executor has no scheduler job classes of its own. Blocking work
does not justify adding processor/carrier handoff state to the network and
channel hot paths.

## Internal ownership model

`cio::Runtime` owns one `detail::Scheduler`, which acts as the runtime
coordinator:

```text
Runtime
  `- Scheduler                 lifetime, shutdown, foreign submissions
       |- Worker[0..N)
       |    |- OS thread
       |    |- runnext + LocalRunQueue
       |    |- RemoteInbox
       |    |- ReactorShard (epoll + eventfd)
       |    |- timer-shard access
       |    `- worker-local metrics/cache
       |- overflow and external/non-local completion fallback queue
       |- BlockingPool
       `- stale-shard monitor
```

`WorkerId` is a private stable integer in `[0, worker_count)`. A coroutine frame
does not acquire a public owner. Only internal wait records may carry a
preferred `WorkerId`, and it is a hint: stealing may resume the coroutine
elsewhere.

## Hot-path rules

1. Scheduling work onto the current worker touches no global queue, global
   idle counter or shared reactor-ownership flag.
2. An established connection whose task has not migrated is polled and resumed
   by the same worker.
3. An idle worker never scans every peer. It steals only from workers that have
   published stealable backlog.
4. Scheduler-driven load migration happens only to consume otherwise-idle
   capacity. A direct channel handoff may still move a waiter to the waker's
   worker to preserve producer/consumer locality.
5. A hard-directed internal remote notification has a concrete ownership
   destination and its directed wakes are coalesced. External/non-local
   soft-affinity completion fallback deliberately permits any idle worker to
   run it.
6. Frame lifetime: schedule the waiter last and never touch its frame
   afterwards. Delayed I/O, join, channel, synchronization and blocking-pool
   completions carry a process-lifetime-unique scheduler endpoint. Same-runtime wakeups compare a cached endpoint
   without an endpoint RMW. Foreign/cross-runtime wakeups acquire a short
   counted lease; shutdown closes the endpoint to new leases, waits for active
   leases to drain, and then clears its Scheduler pointer, so a destroyed
   target is never dereferenced.

These are stronger invariants than “try to preserve locality”. They make a
balanced network scheduling path shared-nothing by construction.

### Default placement

“Local” means the same scheduler worker and reactor shard, not merely the same
process:

- `go()` and `spawn()` called by a running task initially enqueue on the worker
  executing that caller at that instant; this is a placement choice, not a
  guarantee that the new task will run or remain there, because its FIFO entry
  may be published and stolen immediately;
- an I/O awaiter records the worker on which it suspended as a preference hint;
- a direct synchronization handoff uses the waker's current worker and
  `runnext` when both belong to the same scheduler; a foreign/cross-runtime
  handoff uses the shared completion fallback;
- an ordinary submission from a thread that is not one of this runtime's
  workers has no ownership affinity and enters the shared fallback; only an
  internal operation with a concrete ownership target uses a worker inbox;
- only a published queue imbalance causes an idle worker to steal ordinary
  FIFO work.

The `runnext` slot itself is never stolen. A fairness checkpoint may move its
old occupant into the published FIFO/overflow path before selecting unrelated
inbox, global or FIFO work; otherwise the private slot keeps the current fast
producer/consumer path stable. This is a local-first optimization, not a worker
residency guarantee or permanent physical-core affinity: a published task may
be stolen, and the OS may migrate an unpinned worker thread.

Local-first placement is not permanent affinity. Whether work must be exposed
depends on the producer context, not only on queue length:

- an I/O poller may keep one completion in an empty `runnext`; if `runnext` is
  occupied, even a single FIFO completion is published for thieves;
- a running task that enqueues another ordinary task is already occupying its
  worker, so the new FIFO task is immediately stealable even when it is the
  only queued task;
- a same-runtime reactor batch may keep at most one completion in `runnext` and
  publishes every completion placed in its FIFO as stealable;
- once a fairness checkpoint selects an inbox/global/FIFO item, it protects
  every local runnable that item bypasses through the published FIFO/overflow
  path. An inbox item reserves `runnext` before I/O/timer service, so new
  completions also enter a published path;
- with no selected inbox item, if I/O/timer service newly fills an empty
  `runnext` ahead of an existing private FIFO, that FIFO is published before
  the completion runs;
- a separate `overloaded` bitmap and proactive-donation path was benchmarked
  and rejected; it is not part of the retained v2 candidate.

Thus an idle worker plus runnable work that the owner cannot immediately
consume is already an imbalance. The design does not wait for a large queue
threshold before exposing that work.

## Worker-local reactor shards

The single `Reactor` becomes a collection of `ReactorShard`s, one per worker.
Each shard owns:

- an epoll fd;
- its wake eventfd;
- its blocking/non-blocking poll state;
- descriptors assigned to that shard;
- a local ready batch.

`IoDesc` records a home shard for its lifetime. `epoll_ctl`, stale-event
generation checks, direction state and descriptor deadlines retain their
current semantics.

`IoDesc` slab addresses remain stable until reactor destruction. An epoll token
carries an index plus generation; dispatch temporarily pins the descriptor and
revalidates its generation and closing state before touching waiter slots.
`close()` publishes closing under the lifecycle lock, unregisters and wakes
waiters, waits for per-direction syscall leases, and only then closes the
native fd. A descriptor is recycled only after its final reference is released.

Each direction's absolute deadline is authoritative operation state; timer
dispatch is only the wake mechanism. Syscall admission checks the timestamp
directly, so an expired deadline applies even when its callback is delayed.
Set and clear operations are serialized per direction, use a captured
generation, pin that incarnation, wait out the old timer, revalidate before
rearming, and sequence-check callbacks. A stale setter therefore cannot disarm
a timer belonging to a reused descriptor.

When an I/O awaiter parks, it records the current worker as a preferred resume
hint. If readiness is polled by a worker of the same scheduler, that active
poller retains the completion in its `runnext` or local FIFO even when the
recorded preference differs. Every FIFO completion is published so an idle
worker can steal it if `runnext` begins CPU-bound work. If readiness is
dispatched by the monitor or by a foreign-runtime poller, the completion enters
the shared fallback queue; wake search starts at the preferred worker, but any
idle worker may claim it.

Deadline, close and other non-batch I/O wakes remain local only when the waker
is already the preferred worker. Otherwise they use the same fallback queue.
The descriptor itself is not migrated, and preferred worker is never hard
affinity.

Migrating an edge-triggered fd between epoll instances has difficult
DEL/ADD/readiness races and is not done. See
[roadmap.md](roadmap.md#deferred-runtime-work).

### Existing listener path

`TcpListener::bind()` and `accept()` keep their signatures and usage.
Internally, after `accept4()` returns a new fd:

1. the runtime chooses a worker, initially round-robin;
2. the accept coroutine makes an internal scheduling hop to that worker;
3. the new `TcpStream` is registered with that worker's reactor shard;
4. the caller of `accept()` continues on that worker.

The common pattern:

```cpp
auto conn = co_await listener.accept();
cio::go(serve(std::move(*conn)));
```

therefore starts `serve()` on the selected shard without exposing an affinity
API. The listener itself stays on one shard. This is routing for an existing
operation, not a listener feature; listener replication and `SO_REUSEPORT` are
out of scope.

## Targeted remote inboxes

The shared global queue is bypassed only by hard-directed internal cross-worker
submissions with a concrete ownership target. Every worker gets a bounded MPSC
`RemoteInbox` with a cold mutex-protected overflow path. This scheduler-private
queue is unrelated to public `cio::Chan<T>`: channel buffer and waiter state
remain protected by a per-channel mutex and support MPMC senders and receivers.

- a local channel handoff still uses `runnext`;
- a local ordinary wake enters the local FIFO;
- a worker-owned poller retains same-runtime I/O completion locally, while
  monitor, foreign-runtime and non-local I/O wakes use the fallback queue;
- an ordinary foreign-thread submission uses the shared fallback, matching the
  pre-refactor semantics and avoiding arbitrary hard affinity to a busy worker;
- a valid concrete internal ownership target, such as the post-accept shard
  hop, uses the selected worker's MPSC inbox and directed wake;
- blocking-pool, cross-runtime join, channel-close and wait-group completions
  use soft-affinity fallback whenever the preferred owner is not the active
  local waker;
- a timer batch stays local only when fired by its preferred owner; monitor,
  foreign and different-worker firings publish the whole batch to the shared
  fallback and begin idle-worker search at the preference.

The owner drains its inbox in batches. Periodic inbox and global/FIFO fairness
checks prevent a hot local producer/consumer pair from starving other sources.
A selected fairness item first protects every local runnable it bypasses
through FIFO/overflow publication. An inbox item additionally reserves
`runnext` before reactor/timer service. With no inbox selection, a completion
that newly occupies `runnext` ahead of a private FIFO causes that FIFO to be
published. A non-suspending selected task therefore cannot hide the runnables
it bypassed.

The shared global queue remains for ordinary foreign submissions, local-FIFO
and inbox overflow, monitor or foreign-runtime I/O completions, non-local
deadline or close wakes, and sleep/select timer batches fired outside their
preferred owner. It also carries foreign/cross-runtime blocking, join, bulk
synchronization and direct-handoff completions whose worker is only a
preference. Workers service it with bounded fairness and batch pops; it is not
touched by every normal scheduler iteration.

### Queue concurrency model

The runtime does not use one general-purpose queue everywhere. Each queue keeps
the weakest concurrency contract that satisfies its ownership:

| queue | producers | consumers | structure |
|---|---|---|---|
| `runnext` | owning worker | owning worker | one atomic slot |
| local runnable FIFO | owning worker | owner plus thieves | owner-optimized steal-half ring |
| worker remote inbox | any producer carrying a valid concrete internal ownership target | owning worker | bounded MPSC |
| shared fallback queue | any thread | any worker | overflow plus external/non-local completion fallback, mutex-protected |
| blocking-pool jobs | any task/thread | blocking workers | shared queue |

A bounded lock-free MPMC ring is therefore a valid later implementation for
the shared fallback queue. It is not the default first step:
overflow is intentionally cold, it needs a non-dropping fallback when full,
and a mutex batch queue is simpler to prove during the ownership rewrite. It
may replace that queue only if contention is measured there.

The local FIFO must not become a general MPMC queue merely to let remote
producers push into it. Doing so would put atomic producer contention back on
the balanced local scheduling path. A hard-directed producer with a concrete
internal ownership target uses that worker's MPSC inbox; an untargeted or
soft-affinity producer uses the shared fallback; thieves use the local ring's
specialized steal operation.
Likewise, one global MPMC runnable queue would remove the need for stealing
only by reintroducing the shared cache line this design exists to remove.

## Conditional load balancing

The retained candidate publishes two kinds of worker state: `idle` and
`has_stealable_work`. The latter means FIFO work has been explicitly exposed to
thieves; it is not an exact queue-nonempty bit. `runnext` and deliberately
private self-reschedule or direct-handoff work are not published merely by
their own enqueue; a fairness displacement or later FIFO publication may
expose them before unrelated work runs.

Publication uses two per-worker epochs. The owner release-publishes the FIFO
tail, stores `publish_epoch`, then reads `clear_epoch`, all epoch operations
being sequentially consistent. The first publication and the first one that
observes a new clear generation set the shared stealable bit. Further items in
the same generation skip the contended stealable bitmap. An explicit batch may
still scan the separate idle bitmap to satisfy its requested wake count.

A thread that successfully clears the shared bit increments `clear_epoch`,
reads `publish_epoch`, then conservatively rechecks the FIFO. A publisher
either observes that increment and restores the bit itself, or its publish
store precedes the clearer's final load in the SC order and the clearer
observes the queue tail before deciding whether to restore and wake. Multiple
clearers participate only when their bitmap RMW actually changed one to zero.
Stale set bits may cost a failed steal, but a clear bit cannot hide published
work. Equality would have an ABA only after a complete 2^64 successful-clear
cycle between two owner observations, which is outside the executable lifetime
assumed by this implementation.

An idle worker chooses a randomized set bit and steals a batch from that
worker's bounded FIFO. Reactor batches publish every FIFO remainder
with an explicit wake count. Ordinary spawn wakes a searcher on the first
publication, after which steal-half propagation exposes work in batches.

A victim-publication wake carries a sticky searcher credit. The producer first
writes that credit to a concrete idle candidate, then attempts the exact idle
bit clear, and only a successful claim writes the candidate's eventfd. A
failed pre-arm is deliberately not revoked: the worker may already be leaving
park, and at worst performs one harmless extra search. Exactly once, on the
first scheduler-loop iteration after `Scheduler::park()` returns, a credited
worker searches published victims before executing any runnable from runnext,
local FIFO, inbox or shared fallback, including continuations made ready by
timer or I/O service. An unrelated runnable therefore cannot intercept the only
searcher. Ordinary task-to-task resumptions do not reread the credit atomic;
credits are only issued against an idle publication.

Every return from `Scheduler::park()` clears the worker's own idle bit and then
performs an SC stealable-bitmap check. This includes both the pre-poll final
work recheck and a return from an unrelated epoll event. If a victim
publication loses the idle claim because the worker is already leaving, the
worker grants itself the credit before returning to the scheduling loop. Thus
either the producer's claim observes idle and its pre-armed credit wins, or the
worker's clear wins and its post-clear check adopts the obligation.

The overloaded bitmap and proactive-donation experiment was benchmarked and
rejected. Batch publication and steal-half propagation are retained.
Restricting each steal-half propagation to one new searcher reduced perf
task-clock and cycles by about 4–5%, but regressed detached spawn by 1.4% and
spawn-plus-join by 5.6% in interleaved confirmation, so it too was removed.
One liveness-required transfer remains: after any successful steal, an
original victim whose FIFO is still published receives one additional
searcher credit. Items retained on the thief and the original victim tail are
two distinct published sources; the retained-batch wake cannot account for
both.

Work is still stolen in batches. A single long-running task cannot be
parallelized and remains cooperative; this design does not add preemption.

The runtime does not attempt to infer whether a runnable task belongs to a
lock-serialized workload. If its owner is busy and another worker is idle, the
task is eligible to run. Cache-affine `runnext` handoffs remain the narrow
exception.

## Parking and wakeups

An idle worker blocks in its own `epoll_wait`, using its next timer deadline.
The worker's eventfd is both the reactor interrupt and the remote-task wakeup.

Before blocking, the worker:

1. publishes itself in the idle bitmap;
2. rechecks runnext, its local FIFO and inbox, the shared fallback, due timers
   and the stealable bitmap;
3. enters its local `epoll_wait` only if the recheck is empty; an already-ready
   descriptor is observed by that same kernel wait.

A generic remote producer clears the idle bit with a claim and writes the
destination eventfd only when necessary. A victim publisher uses the
pre-armed searcher-credit protocol above. A per-worker `wake_pending` flag
collapses redundant writes.

`Runtime` owns its scheduler through a private shared lifetime. An open Socket
also retains that lifetime, so a Socket returned by `cio::run()` can safely
detach and close after the local Runtime object has stopped. New async
operations on such a stopped home reactor return `Errc::shutdown` rather than
parking forever. `IoDesc` does not retain the scheduler—doing so would create a
Scheduler/Reactor/slab ownership cycle—and all delayed target completions use
stable, process-lifetime-unique endpoints. Foreign/cross-runtime dispatch
acquires the counted lease described above; same-runtime dispatch does not.
Endpoint identities are never recycled, and their small tombstone records
remain reachable until process exit. This removes both ABA and
static-destruction hazards without per-waiter Scheduler shared ownership or
same-runtime endpoint RMWs. A live `IoAwaiter` independently retains the source
Scheduler until its descriptor reference is released, covering close followed
immediately by Socket replacement. This changes no public signature or Socket
ownership rule.

This removes the shared condition variable, shared `spinning_` CAS and shared
single-poller ownership from the balanced hot path. Workers that own no
connections remain asleep instead of repeatedly issuing empty
`epoll_wait(0)` calls, addressing the measured low-connection regression.

## Busy-worker I/O and timers

Per-worker reactors must not let a CPU-heavy task hide readiness on its shard.
A monitor thread is the backstop:

- every shard publishes `last_poll_ns` after `epoll_wait` returns, so a poll
  that has just completed is fresh even when the kernel wait itself was long;
- attaching a descriptor arms a coalesced owner-poll ticket, and the monitor's
  first stale-shard pass arms the same ticket;
- the owner acknowledges the ticket at either of two bounded cold points: the
  periodic worker-loop local-service checkpoint, or an I/O-completion quota
  checkpoint that found no pre-existing runnable debt. Neither path adds a
  shared read to the ordinary I/O-completion or task-selection fast path;
- if a later stale pass finds the ticket still unacknowledged, it queues one
  reusable control coroutine on the shared fallback. Any scheduler worker may
  run that coroutine, perform one non-blocking shard poll through the existing
  single-poller CAS, and place completions on its local fast path;
- the control coroutine is allocated once per reactor and reused through an
  epoch/phase handshake. Reactor shutdown owns its frame after all monitor and
  worker threads have joined, including when the last queued activation never
  runs;
- a one-worker runtime, or a multi-worker driver activation still uncovered
  after a separate 200 us grace period, retains direct monitor polling as the
  absolute liveness backstop. Foreign completions use the shared fallback;
- the monitor uses Linux `SCHED_BATCH` only when it inherited the default
  `SCHED_OTHER` policy. This keeps the liveness thread in the normal fair class
  while reducing wakeup preemption of busy workers; explicitly inherited
  FIFO, RR, IDLE or already-batch policies are preserved;
- a foreign poller never drains the shard owner's eventfd token or clears its
  `wake_pending` publication;
- normal local polling never shares a shard concurrently.

This is an owner-first, worker-driver, hard-backstop protocol rather than a
shorter watchdog interval. It avoids making the short-sleeping monitor the
common poller while retaining a foreign-poll escape for a worker monopolized by
non-cooperative user code. The ticket timestamp is a latch, not a queue:
multiple stale observations coalesce. Driver generations likewise coalesce,
and a late reusable frame adopts the newest uncovered generation instead of
losing it or allocating another frame.

This backstop has a measured sensitivity boundary under skew: a predecessor
build's 16-connection/50%-heavy cell ran about 10.7% below the pre-v2 scheduler
with 200 us of non-suspending work. The explanation — uneven placement of heavy
connections across shards — is consistent with the data but **unverified**,
because the harness did not record the connection-to-shard mapping. See
[scheduler-results.md](scheduler-results.md#skew-sensitivity-caveat).

Several narrow soft-affinity variants were measured and rejected: faster
monitor polling, global completion sharing, and claimed-idle MPSC delivery.
They added cross-shard handoffs, raised CPU use or worsened p99 without a stable
throughput recovery. The retained MPSC path therefore remains restricted to a
valid ownership target such as post-accept placement. A soft I/O completion
stays on its active home-shard poller; monitor/foreign completions use the
shared fallback. Recovering the old scheduler's placement-insensitive skew
behaviour would require descriptor ownership migration or a separately proven
stealable-completion design, not another inbox heuristic.

Timer heaps remain sharded. Timers armed on a worker prefer its shard; the
per-shard locks and the firing/disarm handshake support cross-worker cancel
and monitor-driven expiry. If the preferred shard owner fires a batch,
it may retain one continuation in `runnext` and publishes every FIFO remainder.
If the monitor, a foreign thread or a different worker fires it, the whole
batch enters the global fallback; the preferred worker is only the starting
point for idle search, not an owner. Lock removal is deferred; see [roadmap.md](roadmap.md#deferred-runtime-work).

## Worker loop

The conceptual order is:

```text
  run runnext
  run local FIFO
  periodically service remote inbox / local reactor / expired timers
  periodically admit global overflow and FIFO work

  when local work is empty:
      drain remote inbox
      drain global overflow
      if published work is stealable, poll the local reactor without blocking
      fire local expired timers
      steal from a randomized published victim
      publish idle and recheck queues / timers / published victims
      block in local epoll, which also observes already-ready descriptors
```

Budgets bound every batch so a permanent local runnable stream cannot starve
inbox, I/O or timers. Exact queue capacities, thresholds and budgets are
benchmark parameters, not architectural constants.

## Gates for future scheduler work

A stage is rejected if its intended regime does not improve outside noise, if
it materially regresses channel/synchronization microbenchmarks, or if it gives
back cio's skew advantage to win only the flat echo case.

For scheduler HTTP work, acceptance is a joint gate rather than a throughput
headline. Low-load throughput must remain neutral within the documented
regression budget; saturated throughput and p50-p99 must not materially
regress; p99.9, p99.99, p99.999 and Max are checked separately. A rare-tail
benefit or regression must reproduce in both AB and BA order before it is
attributed to the design. A valid matrix proves that the measurement completed
correctly, not that the candidate passed this performance gate.

“No improvement” is a valid result: the stage is removed and the previous
internal implementation remains. Completing the refactor is not itself a
reason to ship a slower runtime.

## What this design costs

Measured against the shared-reactor scheduler it replaced, this architecture
wins 8-56% on held-open HTTP connections and improves `spawn()`+join by
17-20%, at these standing costs:

- detached `go()` is 6.5-6.7% slower;
- tail latency is worse where throughput improved most: median p99 rose 4.38x
  at 8 connections and 3.02x at 256;
- ordinary 1024-connection throughput parity is not demonstrated for the final
  work-aware build.

These are disclosed rather than amortized into a headline. Full numbers,
baselines and hashes: [scheduler-results.md](scheduler-results.md).

Over twenty implemented variants were measured and removed. That rejected set is
what keeps the hot-path rules above narrow — each one is a place where a more
general mechanism was tried and cost more than it returned. Read
[the rejected designs](scheduler-results.md#rejected-designs) before proposing
another.

## References

- [cio echo comparison](../bench/echo-comparison/README.md)
- [cio HTTP comparison](../bench/http-comparison/README.md)
- [Go scheduler source](https://go.dev/src/runtime/proc.go)
- [Tokio scheduler redesign](https://tokio.rs/blog/2019-10-scheduler)
- [Seastar shared-nothing design](https://seastar.io/shared-nothing/)
- [Linux NAPI and per-worker epoll affinity](https://docs.kernel.org/networking/napi.html)
