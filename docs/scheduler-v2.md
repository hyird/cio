# Runtime v2: affinity-sharded scheduler and reactor

Status: the runtime-v2 base and the work-aware scheduler follow-up are
implemented in the retagged v0.0.1 source. Frozen-screen results whose complete
artifact hash predates the final ABI refinement remain diagnostic rather than
exact-final performance evidence. Benchmark-rejected variants are labelled
historical.

This milestone only restructures the existing runtime. It adds no public API,
operation, protocol, backend or configuration option. Its goal is to reduce
cio's measured cache-coherence cost on balanced network workloads without
giving up the load balancing that wins on skewed workloads.

## Delivery boundary

The refactor may change private ownership, queues, worker selection and where a
coroutine resumes. It may not make an operation possible that is not possible
today, or require an application change.

In scope:

- split the shared scheduler/reactor hot path into worker-owned shards;
- route only hard-directed internal cross-worker submissions with a concrete
  ownership target through per-worker private inboxes;
- retain work stealing, but trigger it from published runnable state;
- preserve the existing timer service, blocking pool, network operations and
  monitor behaviour while adapting their internal routing;
- remove superseded shared scheduler state after the new path passes its gates.

Explicitly deferred:

- resolver, dialer, file and other new I/O APIs;
- new protocols, HTTP, TLS, signals or filesystem watching;
- io_uring, kqueue, IOCP or any backend other than the existing epoll backend;
- `SO_REUSEPORT` listener replication;
- dynamic worker resizing, priorities, preemption or task pinning;
- public executor, affinity, shard, completion-token or migration controls;
- topology-aware/NUMA victim selection. Randomized published-victim selection
  is sufficient for this refactor; topology is a separate measured follow-up.

## Compatibility contract

This is a private implementation replacement. It does not change:

- `Task`, `JoinHandle`, `go()`, `spawn()` or `yield()`;
- `Chan`, `select`, `TaskGroup`, cancellation or synchronization semantics;
- `RuntimeOptions`, `Runtime::worker_count()` or `CIO_MAIN`;
- `TcpStream`, `TcpListener`, `UdpSocket` or their ownership/deadline rules;
- the rule that a task may resume on a different worker after suspension.

No processor, shard, executor, affinity or thread object appears in a public
header. Existing source code is recompiled without changes. The scheduling
order remains unspecified, as it is today.

For this milestone, public names and explicit function signatures in
`include/cio/runtime.hpp` and every other non-`detail` public header are
frozen. This is source-recompile compatibility, not a C++ ABI or awaiter-layout
guarantee. In particular, `RuntimeOptions` does not gain a migration, affinity,
reactor or queue option. Internal tuning constants may exist only in `detail`
implementation code and are selected by benchmarks, not applications.

### Semantic freeze

The following observable behaviour is a hard compatibility boundary:

- tasks remain lazy, and directly awaiting a task remains a symmetric transfer
  rather than an injected scheduling hop;
- `go()` remains detached, `spawn()` remains joinable, and their exception
  behaviour for valid single-waiter use does not change. Concurrently
  registering a second join waiter, which was already outside the supported
  single-waiter contract and could previously observe an invalid result, now
  deterministically throws `std::logic_error`;
- channel close, drain, send-on-closed, rendezvous and buffered FIFO behaviour
  do not change;
- `select` keeps its ready-case selection, result indexing, timeout and
  retraction behaviour;
- `TaskGroup::join()` still waits for every child, preserves the first failure
  and cooperatively cancels the rest;
- cancellation remains cooperative and observable through `CancelToken::done()`;
- mutexes, wait groups, timers and sleep retain their wake and error behaviour;
- socket reads still return zero for EOF, writes may be partial, deadlines
  remain descriptor-scoped and persistent until reset, and `close()` wakes
  parked I/O;
- the one-reader/one-writer-per-socket rule and socket lifetime rule remain;
- `Runtime::shutdown()` retains its documented behaviour, including the current
  treatment of parked detached tasks; because it synchronously joins workers,
  calling it from one of the same Runtime's worker tasks throws
  `std::logic_error` before stop or join begins;
- the existing error mapping and `Result<T>` representation remain unchanged.

Thread identity, worker identity, exact runnable order and fairness timing are
not public semantics today and remain unspecified. The scheduler may change
them, but it may not turn a suspension into a visible extra result, exception,
callback, required option or user-managed affinity operation.

The additions described in [io-infrastructure.md](io-infrastructure.md) are
not implemented as part of this milestone.

## What the measurements say

The current scheduler and a pure shared-nothing server each win a different
regime:

- with small, balanced, held-open connections, the shared-nothing Asio server
  leads by 14–23%;
- with a few uneven CPU-heavy connections, cio's work stealing leads by
  19–108%;
- at 8 saturated cores, cio's CPU cost grows while shared-nothing stays flat;
- cio retires fewer instructions than Asio but takes 27% more cycles, with
  lower IPC and more cache misses;
- removing one third of the network syscalls improved throughput only 1.6%;
  the remaining gap is not explained by syscall count;
- process CPU migration is negligible in absolute terms.

The detailed measurements and rejected hypotheses are in
[the echo comparison](../bench/echo-comparison/README.md#where-the-gap-actually-is).
They point to shared scheduler/reactor state and unsuccessful stealing as the
cost, not coroutine machinery or the socket syscall fast path.

That rules out two simple answers:

- **Pure shared-nothing** has the best balanced hot path but strands work on a
  busy shard.
- **A more elaborate global G/M/P scheduler** improves blocking-syscall
  integration but adds ownership state to every scheduling path and does not
  address the measured network coherence problem.

The target is therefore a hybrid: shared-nothing while load is balanced,
work-sharing only after imbalance is published.

## Target architecture

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

The existing blocking executor is retained without new job classes or public
operations. Blocking work does not justify adding processor/carrier handoff
state to the network and channel hot paths.

## Internal ownership model

`cio::Runtime` continues to own one `detail::Scheduler`; its public declaration
does not change. Internally that scheduler becomes the runtime coordinator:

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
       |- existing BlockingPool
       `- existing monitor
```

`WorkerId` is a private stable integer in `[0, worker_count)`. A coroutine frame
does not acquire a public owner. Only internal wait records may carry a
preferred `WorkerId`, and it is a hint: stealing may resume the coroutine
elsewhere.

The existing `Scheduler`, `Reactor` and `TimerService` entry points used by
current headers remain as compatibility facades during the migration. Callers
are moved to shard-aware private helpers before old shared fields are removed.
This avoids coupling a public-header rewrite to the concurrency rewrite.

## Hot-path rules

1. Scheduling work onto the current worker touches no global queue, global
   idle counter or shared reactor-ownership flag.
2. An established connection whose task has not migrated is polled and resumed
   by the same worker.
3. An idle worker never scans every peer. It steals only from workers that have
   published stealable backlog.
4. Scheduler-driven load migration happens only to consume otherwise-idle
   capacity. The existing direct channel handoff may still move a waiter to the
   waker's worker to preserve producer/consumer locality.
5. A hard-directed internal remote notification has a concrete ownership
   destination and its directed wakes are coalesced. External/non-local
   soft-affinity completion fallback deliberately permits any idle worker to
   run it.
6. The existing frame lifetime rule remains unchanged: schedule the waiter
   last and never touch its frame afterwards. Delayed I/O, join, channel,
   synchronization and blocking-pool completions carry a process-lifetime-
   unique scheduler endpoint. Same-runtime wakeups compare a cached endpoint
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
DEL/ADD/readiness races and is not required for the first performance target.
It may be evaluated later only if repeated remote completions become measurable.

### Existing listener path

`TcpListener::bind()` and `accept()` keep their signatures and usage.
Internally, after `accept4()` returns a new fd:

1. the runtime chooses a worker, initially round-robin;
2. the accept coroutine makes an internal scheduling hop to that worker;
3. the new `TcpStream` is registered with that worker's reactor shard;
4. the caller of `accept()` continues on that worker.

The common existing pattern:

```cpp
auto conn = co_await listener.accept();
cio::go(serve(std::move(*conn)));
```

therefore starts `serve()` on the selected shard without exposing an affinity
API. The listener itself remains on one shard. This is routing for an existing
operation, not a new listener feature; listener replication and `SO_REUSEPORT`
are outside this refactor.

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
| blocking-pool jobs | any task/thread | blocking workers | existing shared queue, unchanged |

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
only by reintroducing the shared cache line that this refactor exists to
remove.

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
worker's existing bounded FIFO. Reactor batches publish every FIFO remainder
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
The existing monitor remains as a backstop:

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

An earlier skew screen of the predecessor `candidate_frozen` build, not the
retained final candidate, exposed a possible boundary of this backstop. With
200 us of non-suspending work, its 16-connection/50%-heavy cell was about 10.7%
below the pre-v2 scheduler in a seven-repeat confirmation, while other skew
cells moved in both directions. The result is consistent with heavy connections
being unevenly placed across shards, but the harness did not record the
connection-to-shard mapping. Placement is therefore an unverified explanation,
and these cells are a sensitivity signal rather than a universal scheduler
ratio or a final-candidate measurement.

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
existing locks and firing/disarm handshake continue to support cross-worker
cancel and monitor-driven expiry. If the preferred shard owner fires a batch,
it may retain one continuation in `runnext` and publishes every FIFO remainder.
If the monitor, a foreign thread or a different worker fires it, the whole
batch enters the global fallback; the preferred worker is only the starting
point for idle search, not an owner. Lock removal is a separate optimization
and must not be coupled to reactor sharding.

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

## Why not full processor/carrier handoff

A Go-like G/M/P split is possible in C++, but it is not the performance target
for cio's measured workloads:

- established network operations already avoid blocking workers through
  epoll;
- existing explicitly blocking operations already use the blocking pool;
- processor handoff adds ownership transitions, replacement-thread state and
  shutdown races to the scheduler;
- the measured deficit is cache stalls on shared scheduling state, which a
  G/M/P split does not remove.

The blocking executor is unchanged in this refactor. Replacing it or adding
admission classes requires a separate workload and milestone.

## Migration plan

Every stage is compared with frozen baseline artifacts in interleaved A/B
runs. Benchmark-rejected stages are removed from the candidate source. No
stage is retained because it is architecturally attractive.

0. **Freeze and baseline**
   - record the current public declarations and example compile surface;
   - capture correctness, throughput, tail latency, idle CPU and scheduler
     metrics before changing ownership;
   - add no public or private tuning option that applications can select.
1. **Internal ownership seam**
   - introduce private `WorkerId` and worker/shard lookup helpers;
   - turn the current `Reactor` entry point into a temporary routing facade;
   - keep exactly one reactor shard so behaviour and performance stay unchanged.
2. **Targeted inbox**
   - add a preferred worker to internal wait nodes;
   - add per-worker remote inboxes;
   - keep the shared reactor and current stealing;
   - measure whether targeted delivery reduces steal attempts and cache stalls.
3. **Reactor sharding**
   - add one epoll/eventfd pair per worker;
   - assign accepted/connected streams to shards;
   - add internal post-accept distribution;
   - retain the current monitor as stale-shard helper.
4. **Local parking**
   - replace the shared idle condition variable and single-poller state with
     per-worker epoll parking and targeted wakes;
   - verify the 1/8-connection latency and idle-CPU regimes first.
5. **Published imbalance**
   - add idle/stealable bitmaps;
   - stop scanning unpublished victims;
   - preserve steal-half batching and skew throughput;
   - benchmark and remove overload/proactive donation after it shows no
     measurable benefit.
6. **Cold-path cleanup**
   - reduce the global queue to overflow and external/non-local completion
     fallback duties;
   - remove old shared counters only after all callers have migrated.

DNS, files, dialers, additional backends and topology-aware stealing do not
begin until stage 6 has passed all acceptance gates.

## Acceptance gates

Correctness gates:

- all existing tests and long soak pass;
- ASan/UBSan and short TSan suites pass;
- public headers and API compile fixtures have no additions or signature
  changes;
- API compile tests build the current examples and representative downstream
  call patterns without source changes;
- semantic regression tests cover every item in the compatibility contract,
  including exception, close, deadline, cancellation and shutdown behaviour;
- descriptor close, timeout, cancellation and stale-event races retain their
  current results;
- idle workers remain parked and shutdown joins every scheduler thread.

Performance gates use interleaved runs with the server and load generator in
independently pinned processes. Echo freezes the same cio-based load-generator
binary across both A/B sides; HTTP uses third-party `wrk`:

- HTTP and echo at 1, 8, 64, 256 and 1024 connections;
- balanced 16/128-byte held-open traffic;
- the 16/32-connection skew matrix;
- channel, select, spawn, timer and socket microbenchmarks;
- cycles/request, IPC, cache misses, steal attempts and remote wakes;
- idle CPU and tail latency, not only requests/second.

### Final gate snapshot

The retained source was frozen before the final measurements. The core
comparison used these exact binaries; their hashes were checked before and
after every run:

| side | role | `bench_core` SHA-256 |
|---|---|---|
| A | exact pre-v2 Git HEAD source build | `fda481642f22ce7236a1ff849db1372de3550aa3819bf56b1aa85ffe86019188` |
| B | retained v2 source | `56d91a30b355f1f3cbd8fe097933dc08c2c75e0cdf383e590fbd948e63f76bb2` |

Runs were warmed up, pinned to CPUs 0-7 or 0-23, paired, and alternated AN/NA.
Positive ns/op deltas below mean B is slower:

- `spawn() + co_await join` improved by 16.86% at 8 workers and 19.76% at
  24 workers; all 20 isolated pairs favoured B. The retained implementation
  lets the original task complete `JoinState` from final suspend and removes
  the ordinary wrapper coroutine. Invalid and already-completed tasks retain a
  cold wrapper to preserve their old error/completion semantics and avoid
  resuming a coroutine already at final suspend.
- Detached `go()` remained a measured v2 cost: B was 6.52% slower at 8 workers
  and 6.71% slower at 24 workers over 15 pairs. Comparing the direct-completion
  build with its immediately preceding v2 binary was neutral at 8 workers and
  2.99% faster at 24, so this cost is not hidden inside the join optimization.
- The formal 24-worker unbuffered-channel gate was neutral: paired geometric
  B/A +0.095%, median -0.683%, and only 5/15 pairs slower. A seven-pair
  +13% screen did not reproduce.
- A 24-worker 1-producer/8-consumer channel screen at +23.5% also did not
  reproduce: its 15-pair confirmation was +3.49% geometric mean, -0.15%
  median, with opposite AN/NA directions. Select and the remaining channel and
  mutex screens were likewise direction-dependent rather than confirmed
  regressions.

The final network evidence used separately pinned server and load-generator
processes. Echo retained its seven alternating pairs and kept the same
cio-based load-generator binary
(`3752a0f3cb67ef7da0fc7fc4ac62fb730c818ceebd43b16c814ca770310b9ba7`)
frozen across both sides. HTTP used third-party `wrk`, ten pairs per cell,
five AB and five BA, with a 5-second warm-up and 15-second measured window:

| workload | A mean req/s | B mean req/s | paired geometric B/A (95% CI) | median p50 A/B | median p99 A/B |
|---|---:|---:|---:|---:|---:|
| echo, 1024 connections, 128 B | 725,717 | 779,580 | **+7.43%** (about +5.1% to +9.9%) | 1351/1121 us | 3072/3968 us |
| HTTP, 1 connection, `wrk -t1` | 14,242 | 14,339 | +0.67% (-3.88% to +5.42%), neutral | 67/68 us | 109.5/110 us |
| HTTP, 8 connections, `wrk -t4` | 80,645 | 125,598 | **+56.00%** (+49.52% to +62.77%) | 92/52 us | 147/644 us |
| HTTP, 64 connections, `wrk -t16` | 729,546 | 789,141 | **+8.16%** (+6.84% to +9.50%) | 76/70 us | 518/501 us |
| HTTP, 256 connections, `wrk -t16` | 773,023 | 771,416 | -0.22% (-1.63% to +1.21%), neutral | 303.5/306 us | 915/2765 us |
| HTTP, 1024 connections, `wrk -t16` | 714,915 | 781,518 | **+9.34%** (+7.22% to +11.49%) | 1355/1125 us | 3900/4635 us |

The HTTP intervals are per-cell unadjusted paired log-ratio Student-t
intervals. All 100 measured HTTP sides had zero socket and HTTP errors, the
servers stayed live, and the A, B and `wrk` hashes were unchanged at the end.
The AB/BA splits agreed in direction for c8, c64 and c1024.

The first complete HTTP matrix used `wrk -t2` at c8 and `-t8` at c64/c256/c1024.
It passed correctness gates but reached at least 95% of configured client-thread
capacity. That screen is not release evidence, and none of its numbers is
combined with the clean-source confirmation above.

The warning-free matrix still shows workload-specific efficiency/tail
trade-offs. HTTP c64 server use was 7.89/7.97 cores and both p50 and p99
improved. At c8, server use was only 2.64/2.75 cores but B's median p99 was
4.38 times A. C256 was throughput-neutral with nearly identical server CPU,
while B's median p99 was 3.02 times A. C1024 improved throughput and p50 but
raised p99 by about 19%. Echo server CPU rose from 61.57 to 63.10
core-seconds per measured window (+2.48%) and its median p99 rose by 896 us.
Results therefore include latency and CPU rather than treating throughput
alone as the gate.

The exact HTTP hashes were
`5650865ce18c6d029fbd0546b0ee9a6d7758da5087038f8f8db15664f78750e8`
(A),
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf`
(B), and
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588`
(`wrk`). A was built from clean commit `899ccad`; independent clean builds
reproduced its static-library hash. B rebuilds byte-for-byte from clean retained
commit `5e0208b`. A previous byte-reproducible hybrid A combined a dirty 09:45
library with 09:57 headers and was rejected because it did not map to one clean
source revision. None of that historical matrix's values is used here. The
exact retained echo B binary was
`3916905609c9807dead082bb83fddb109e034c8056304be0007fec1496405d7e`.

The final source passes all ten Release tests, all ten ASan/UBSan tests, and all
ten TSan tests. The TSan build retains the benchmark-only mismatched-allocation
warning from `bench_io`'s allocation counter; no sanitizer test failed.

The predecessor `candidate_frozen` skew sensitivity described under Busy-worker
I/O and timers remains relevant context, but it is not a measurement of the
final retained candidate. Because the harness did not record accepted
connection-to-shard mappings, the result is only consistent with placement
imbalance and does not establish it as the cause. No rejected soft-affinity MPSC
variant is present in the final source.

### v0.0.1 retag monitor-balance history

The owner-ticket and `SCHED_BATCH` monitor protocol above is a later engineering
candidate. Its focused HTTP confirmation compared the frozen published-runtime
server with the exact candidate server for ten pairs per cell, five AB and five
BA. Both sides used eight server workers on CPUs 0-7; the same third-party
`wrk` used 14 threads on CPUs 8-21, while the harness stayed on CPU 23. Each
side had a 5-second warm-up and a separate 15-second measured window.

| connections | baseline mean req/s | candidate mean req/s | paired geometric candidate/baseline (95% CI) | median p50 | median p75 | median p90 | median p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 786,646 | 796,591 | **+1.27%** (+0.01% to +2.55%) | 60/59 us | 72/70.5 us | 88.5/86 us | 467.5/539.5 us |
| 1024 | 777,135 | 859,820 | **+10.65%** (+9.08% to +12.25%) | 1100/1100 us | 1900/1220 us | 2325/1530 us | 4565/2060 us |

At 1024 connections the candidate's paired geometric latency deltas were
-0.71% at p50, -36.05% at p75, -34.65% at p90 and -54.69% at p99. The
throughput gain reproduced in both order strata: +10.02% in AB and +11.29% in
BA. The small c1024 p50 effect was mildly order-sensitive (+0.55% in AB and
-1.96% in BA), so it is described as neutral rather than an improvement. At 64
connections p90 improved by 2.48% overall and in both order strata, but p99 was
position-sensitive: AB was +35.93% while BA was -35.30%. That c64 p99 is
therefore recorded as order-dependent, not as a candidate improvement or
regression.

The single maximum latency reported by each c1024 `wrk` run did not follow the
p99 improvement: its paired geometric delta was +24.70%, with baseline maxima
of 61.25-93.95 ms and candidate maxima of 86.41-167.46 ms. No candidate p99
exceeded 5 ms and no maximum exceeded the predeclared 200 ms rare-tail trigger,
so the gate did not require an observer rerun. The maximum-latency movement is
still retained as a diagnostic caveat; this result does not claim that every
extreme-tail statistic improved.

The second-run versus first-run throughput drift was +0.15% at c64 and -0.57%
at c1024. All 40 measured sides had zero socket and HTTP errors, neither cell
hit the client-capacity warning, both servers stayed live, and all input hashes
were unchanged. The exact executable hashes were
`c9c978fb4b4c2aae98eecd886825fcf4206b7343f0cdae0a5f09c925189c1adf`
(baseline),
`80d71422932a59b90b1cb3eb6395dce52e06fbfae9c6763b9add36d5544a6239`
(candidate), and
`3722bf8b31651d8b029b4856af9239dfb491ca93e92447368a4e183e8863b588`
(`wrk`).

This is frozen-binary engineering evidence, not yet a release claim: the
candidate executable does not map to a committed clean source revision.
Before publication it must be rebuilt from the eventual clean revision and the
confirmation repeated or byte-reproduced from that revision.

Two narrower follow-ups were rejected. Replacing the ticket with monitor-local
progress tracking moved c1024 throughput only +0.41% relative to this candidate
while worsening p99 by about 4.7% and producing a 228 ms maximum-latency run.
Removing `finish_io_batch()`'s final non-empty check moved c64 throughput
-1.74% and c1024 -0.47%, with no compensating latency gain. Neither rejected
variant is present in the retained source.

The extended `wrk` histogram exposed a real tradeoff hidden above p99. In a
four-pair, 30-second c1024 diagnostic against the original 2026-07-28 v0.0.1
publication binary, the monitor-balance candidate gained 7.87% throughput and
reduced p90 and p99 by 33.11% and 53.60%. Its p99.99, p99.999 and maximum
latency nevertheless rose 22.29%, 13.95% and 17.89%. The scheduler observer
made this a diagnostic rather than publication matrix, and the intended gains
reproduced in both order strata, but so did the extreme-tail cost. This result
does not replace the ten-pair confirmation; it narrows the next optimization
target to rare foreign-monitor dispatch rather than common readiness
processing.

Two designs aimed at that rare path were subsequently measured and rejected:

- Foreign-completion batching kept all monitor-polled completions together
  before publishing them. Against the exact monitor-balance candidate, its
  four-pair c64 screen lost 1.38% throughput, raised p50 by 3.01%, and raised
  p99.99, p99.999 and Max by 13.44%, 58.34% and 58.78%. The c1024 aggregate
  looked better, but the BA stratum worsened p99.99 by 125.27%, p99.999 by
  53.21% and Max by 45.95%; the aggregate was therefore an order-dependent
  outlier, not a stable win.
- Normal-worker reactor rescue published stale shards through a generation
  latch and bitmap so an ordinary scheduler worker could perform `poll(0)`,
  leaving direct monitor polling as a final backstop. The complete
  correctness, sanitizer, race-repeat and soak gates passed, but the frozen
  four-pair HTTP screen failed the performance gate. At c64, throughput was
  neutral (-0.02%) while p99.999 and Max rose 16.67% and 40.91%; Max worsened
  in both AB and BA strata. At c1024, throughput fell 1.97%, p50/p90 worsened
  1.97%/1.06%, and p99.99, p99.999 and Max rose 26.59%, 26.46% and 23.93%.
  The AB stratum was especially unstable: throughput fell 5.64% and Max rose
  53.95%, whereas BA was mostly neutral. The candidate executable was
  `de4fc489aa089ba347f9fe9c45ea276b42bb927fe7f0bcc4893cb388735044a1`;
  the retained comparison executable remained
  `80d71422932a59b90b1cb3eb6395dce52e06fbfae9c6763b9add36d5544a6239`.
  The rescue state machine and its tests were removed, restoring the retained
  runtime and static-library hash
  `2aadef6c6990a47a39411324cf77182a80eeca684cea603f782aab605838eea9`.

The reusable worker-driver protocol described above was the next retained
candidate. A ten-pair confirmation compared the exact monitor-balance server
with the worker-driver server at 64 and 1024 connections. At c64, worker-driver
throughput was neutral at -0.31% (95% CI -1.25% to +0.63%). Its p50 and p75
rose by 2.72% and 1.69%, one microsecond at the reported medians, while p99,
p99.9 and p99.99 fell by 46.21%, 4.09% and 14.21%. The three tail improvements
reproduced in both AB and BA strata. At c1024, throughput was likewise neutral
at -0.49% (-1.50% to +0.54%); p50 through p99 and p99.99 through Max were
neutral. The p99.9 aggregate favored the driver but had a very wide interval
and reversed in magnitude by order, so it is not claimed as an improvement.

All 40 measured sides completed without socket or HTTP errors, client
saturation, early server exit or hash drift. The exact server hashes were
`80d71422932a59b90b1cb3eb6395dce52e06fbfae9c6763b9add36d5544a6239`
(monitor balance) and
`f1841caae0037b6306eefee81eb6c5fd928d0f9ae3f9ed56303e5caa2c141948`
(worker driver). The retained worker-driver static library is
`55190fdc7fab84882a7875e2e97a9db1c9d1ed9f5c9735d19f360a9a38d6b3bc`.
As with the preceding engineering candidates, these hashes identify frozen
dirty-tree artifacts rather than a committed release revision.

Fixed, unconditional-yield cooperative I/O budgets were then tested and
rejected.
Symmetric child tasks shared a thread-local top-level-resume budget; successful
TCP, UDP and accept leaf operations consumed it only after releasing their
syscall lease. Exhaustion returned the coroutine through the scheduler's local
fairness checkpoint. The implementation passed Release, ASan/UBSan, TSan,
repeated race tests and a non-sanitized soak before measurement, but its
periodic forced yield created an unacceptable latency distribution:

- With a budget of 64 successful operations, the ten-pair confirmation moved
  c64 throughput +1.45% (+0.43% to +2.47%) and improved p50/p75 by 3.30%/2.07%.
  Average latency nevertheless rose 4.18%, p99 rose 51.43% (+29.70% to
  +76.80%) in all ten pairs, and p99.9 rose 2.85% (+1.19% to +4.54%). At
  c1024, throughput was neutral at -0.15% (-1.48% to +1.20%) while p99 rose
  2.40% (+0.15% to +4.69%). The c64 throughput gain therefore failed the
  joint latency gate.
- Halving the budget to 32 did not repair the tradeoff. In the four-pair
  screen, c1024 throughput moved -2.50% (-5.12% to +0.19%), average latency
  rose 5.06%, and p90 rose 2.50%. At c64, throughput moved -0.68% and p50
  rose 1.28%. Its aggregate p99 appeared 15.41% lower, but the AB/BA strata
  were -28.93% and +0.68%, so the apparent benefit was position-dependent.

The rejected fixed budget-64 and budget-32 server hashes were
`12a964fc14f360824903cbfe59f60206ee94c2aae2e174b0128573a231428119`
and
`bc8305025e6500473fa0439a4ae053c0bdd4f21aef3af28ce52c38fa760e787d`.
Those unconditional-yield variants and their dedicated tests were removed.
Rebuilding that restored tree reproduced the retained worker-driver library
and server hashes above byte for byte.

The next work-aware candidate keeps an operation counter but makes exhaustion
work-aware. A top-level scheduler resume grants 128 terminal leaf-completion
slots. TCP read/write leaves consume one slot for every terminal result,
including an error; an error ends that chain anyway, while counting it avoids
a success-flag store, load and branch on every ordinary completion. Accept and
UDP retain explicit post-success checkpoints. All paths touch the counter only
after the syscall lease and operation-local guards have been released. The
next boundary enters an outlined cold checkpoint, which applies these rules:

- stop, inbox debt or global debt returns to the scheduler immediately;
- purely local debt receives one extra 128-operation grace quota, then forces
  a scheduler return at the next exhausted boundary;
- with no runnable debt, an armed owner ticket performs one non-blocking poll;
  a productive poll returns to the scheduler, while an empty poll continues
  to the timer check;
- a productive timer check or a newly observed stop returns to the scheduler;
  otherwise the quota renews inline without a context switch.

Before returning, global and inbox debt are staged ahead of the parent
continuation. A non-local return publishes the parent for stealing. A
local-only return keeps the normal private FIFO path unless an idle-worker hint
shows that publication can let a peer run the parent while the owner consumes
the displaced local task. The hint affects only placement: the queue
publication and idle/search epoch handshake remain the source of visibility
and lost-wakeup safety.

Diagnostic counters distinguish quota exhaustion, no-demand renewal,
first-local-boundary deferral and actual forced yield. Once workers are
quiescent they satisfy:

```text
exhaustions =
    renew_no_demand + deferred_local_only + forced_yields
ticket_polls =
    ticket_polls_empty + ticket_polls_productive
```

The counters are independent relaxed atomics, so a live snapshot can
temporarily violate either identity while a checkpoint is in flight. The
local/inbox/global yield-reason counters are also not a partition: inbox and
global can overlap, and stop-only yields have no reason counter.

The common counter is a `uint64_t` TLS value. Zero is dormant outside a worker;
an unsigned decrement wraps it to `UINT64_MAX`, avoiding a separate disabled
check without the short wrap interval of a 32-bit counter. The shared-library
ABI exports a default-visible detail symbol so consumers retain the compiler's
normal TLS model. Library translation units use a hidden alias of that exact
storage, allowing a GCC-built statically linked PIE to relax each TCP
completion to one segment-relative `subq` and one predicted-not-taken branch.
The final GCC read and write coroutine actors are each 1,382 bytes, equal to
the pre-quota worker-driver build and without a quota TLS initialization guard.
Clang 19 preserves the semantics but does not perform the same final
relaxation, so the single-instruction code-generation claim is GCC-specific.

### Work-aware quota balance result

The first frozen work-aware build still carried a success flag and a
non-relaxable TLS access on every TCP completion. Against the worker-driver
baseline, its publication-ready ten-pair standard `wrk` confirmation was
neutral at c64, at -0.34% throughput (95% CI -2.21% to +1.57%). At c1024 it
lost 2.52% throughput (-3.79% to -1.24%), while paired p50 and p90 rose 3.47%
(+2.03% to +4.93%) and 2.33% (+0.07% to +4.63%). Requests per server CPU
second fell 2.51%, with both sides using about 7.91 server cores. That is a
real common-path efficiency regression, not client saturation or an idle
server.

The same build's four-pair mixed confirmation showed why the fairness
mechanism remains useful: pipelined bulk throughput was neutral at -0.07%
(-1.13% to +1.01%), while the ordinary probe gained 100.61% throughput and
reduced p50, p90 and p99 by 49.47%, 38.53% and 30.57%. Thus the pre-fast-TLS
build passed the mixed fairness objective but failed the joint standard-load
gate at c1024.

Removing the success flag, using the hidden TLS alias and widening the counter
produced frozen screen binary
`cc5b9945e734c0e17589af92bb98700b8e940a51803f0c5992597b763c109bed`.
Its four-pair mixed screen kept bulk throughput neutral at -0.22%
(-1.85% to +1.43%), while probe throughput rose 91.35% and probe p50, p90 and
p99 fell 47.19%, 37.02% and 31.73%. In the standard screen, c64 throughput was
+0.91% (+0.02% to +1.79%). C1024 did not establish parity: its aggregate was
-5.07% with a wide -16.24% to +7.60% interval, driven by paired results of
-0.50%, -15.61%, -1.35% and -1.95%. The low run had normal server/client CPU,
no socket or HTTP error and cannot be discarded.

After that screen, the TLS storage was changed from one hidden symbol to the
shared-safe default-visible/hidden dual-symbol ABI described above. The final
static-library and HTTP-server hashes are
`699fd88b69df76d8605a3b690d47adb52b6cab9d2964998eb6f01cb7942eedd6`
and
`aa9834d2167a6436fb451a274abc5b8cdcb09aca05ea8239519d581cded43af4`;
the shared-library hash is
`dca78bd16f0d2b294b57d1014d991f0a1b8c98613d6b457ce6401e8739350f3`.
Two independent Release builds reproduced the static library and HTTP server
byte for byte. Static Release, shared Release, ASan/UBSan and TSan each passed
all ten tests. The final ABI
change preserves the screen binary's entire `.text` section byte for byte
(raw section SHA-256
`b91f518c268c70239dc85220dad1bce8e67560e728df196e797a0955e1c1dedd`),
but changes the full artifact hash. The screen above is therefore mechanism
evidence rather than a performance claim for the final binary.

This round therefore ends with a clear tradeoff: mixed-load fairness and probe
latency improved materially at nearly neutral bulk throughput, but ordinary
c1024 throughput parity was not demonstrated and a residual scheduling
instability remains. The retagged v0.0.1 includes the correctness-gated
implementation with that limitation disclosed; the frozen screen remains
mechanism evidence rather than an exact-final performance result.

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

## References

- [cio echo comparison](../bench/echo-comparison/README.md)
- [cio HTTP comparison](../bench/http-comparison/README.md)
- [Go scheduler source](https://go.dev/src/runtime/proc.go)
- [Tokio scheduler redesign](https://tokio.rs/blog/2019-10-scheduler)
- [Seastar shared-nothing design](https://seastar.io/shared-nothing/)
- [Linux NAPI and per-worker epoll affinity](https://docs.kernel.org/networking/napi.html)
