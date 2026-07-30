# Repository instructions

These instructions apply to the entire repository.

## Project

cio is a Linux-only C++20 coroutine runtime. Its public model is intentionally
small: tasks, spawning, channels, `select`, structured groups, cancellation,
task-suspending synchronization, timers, non-blocking sockets and a blocking
pool. The implementation uses worker-local scheduling and reactor ownership
without exposing executors or completion tokens in the public API.

Preserve public names, signatures and observable semantics unless the task
explicitly requests an API change. Internal types in `cio::detail` may change,
but public headers are compiled directly by downstream users and therefore
remain part of the compatibility surface.

## Repository map

- `include/cio/`: public, mostly template-facing API
- `include/cio/detail/`: private scheduler/reactor/timer/queue contracts
- `src/`: non-template runtime implementation; `src/tls.cpp` builds only with
  `-DCIO_TLS=ON` and is the sole file with an external dependency
- `tests/`: test executables discovered automatically by CMake
- `examples/`: public API compile and usage examples
- `bench/`: C++ microbenchmarks and isolated Go/HTTP/echo comparisons
- `cmake/`: package config template consumed by `find_package(cio)`
- `.github/workflows/ci.yml`: the gates below, run on every push

This file is the only design document. `README.md` introduces the library for
users; everything a change to the runtime has to respect is here.

Do not commit generated build trees, benchmark executables, sanitizer output,
Python caches or ad-hoc raw results. The relevant patterns are in `.gitignore`.

## Build and verification

Use an out-of-tree build:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizer configurations:

```sh
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=asan
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=tsan
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

CI runs Release plus both sanitizers across GCC and Clang, a non-sanitized soak,
an install-and-`find_package` round trip, an `add_subdirectory` consumer and a
whitespace check. Locally, apply verification in proportion to the change:

- Documentation or scripts: syntax/help/smoke checks plus `git diff --check`.
- Public headers: Release build, full CTest and `test_api_surface`.
- Scheduler, queue, sync, timer or lifecycle code: full Release, ASan/UBSan and
  TSan suites.
- Network/reactor code: all three suites plus relevant repeated race tests and
  a non-sanitized soak.
- Performance changes: correctness suites first, then frozen interleaved A/B
  benchmarks.

`test_soak` defaults to a short CTest run. Longer soaks belong in an optimized,
non-sanitized build:

```sh
./build/test_soak 90
```

ASan and TSan can prevent symmetric coroutine transfer from remaining a native
tail call. Do not use a long sanitizer soak as a release-runtime stack test.

The Go comparison is a separate module:

```sh
cd bench/go-core
go test ./...
```

## Design constraints

These are settled. Reopening one needs new evidence, not a new argument.

- **Linux and epoll only.** No io_uring, kqueue or IOCP backend. Regular-file
  and system-resolver calls go to the bounded blocking executor instead.
- **No executor, affinity, shard, completion-token or migration control in a
  public header.** No processor, shard or thread object appears in the public
  API; `RuntimeOptions` gains no affinity or queue knob. Internal tuning
  constants live in `detail` and are selected by benchmarks, not applications.
- **No Go-style G/M/P processor handoff.** The measured deficit was cache stalls
  on shared scheduling state, which G/M/P does not remove.
- **Stackless coroutines.** Not a stackful-fiber runtime; symmetric transfer
  relies on tail calls.
- **Public API follows Go.** Go's names and shapes win where Go has an
  equivalent: `net.Conn`/`PacketConn`/`Listener` as concepts, `io.Reader`/
  `Writer`/`ReadFull`/`Copy` (destination first), `net.Error` classifiers,
  `sync.WaitGroup`/`Mutex`, `os.File`, `tls.Config`/`tls.Conn`,
  `Resolver.PreferGo`. Where Go has no counterpart — `Task`, `Runtime`,
  `Result` — do not invent a false parallel.
- **Concepts, not virtual interfaces.** Go can afford runtime interfaces; the
  socket fast path cannot carry a vtable. Genericity is compile-time.
- **Deadlines and cancellation live on the connection, not on the call.** That
  is why `read`, `write` and `accept` take no cancel parameter: cancellation
  binds to the descriptor and is checked at syscall admission, next to the
  deadline.
- **Cancellation closes; it does not merely abandon.** A cancelled operation
  must close its descriptor and every spawned attempt must be joined before its
  parent returns. Detaching leaves a task parked on a socket until the kernel
  gives up, and leaks its frame outright if the runtime shuts down first —
  shutdown does not unwind tasks parked on channels or sockets.
- **No context values.** Cancellation scopes carry a done signal, an error and a
  deadline. A map keyed by opaque types is a dependency-injection mechanism, not
  a cancellation one.
- **No DNS cache, DNSSEC or search list** in the built-in resolver, and no
  userspace protocol beyond stub resolution. It reads `/etc/hosts` and
  `/etc/resolv.conf` and nothing else; NSS modules are the system backend's job.

## Rejected mechanisms

A long list of scheduler mechanisms has already been implemented, measured
against frozen baselines and removed. Do not treat an untried-looking idea as
untried: search the git history for the mechanism before building it, because
re-deriving one of these costs a benchmark host-day.

The rejected set already covers, at minimum: overload bitmaps and proactive
donation, soft-affinity completion sharing, several reactor owner-poll and
deadline protocols, bounded `runnext`, foreign-completion batching and
prioritization, normal-worker reactor rescue, fixed unconditional-yield
cooperative budgets, restricting steal-half propagation, a lock-free MPMC ring
in front of the shared fallback queue, and timer-heap lock removal.

Two standing conclusions follow from it:

- **The rare foreign-monitor dispatch path has had four separate designs
  rejected.** A fifth needs a diagnosis of why that path is slow, not another
  policy.
- **Topology-aware victim selection cannot be validated on a single-socket,
  single-LLC host**, because a topology-aware thief always takes the same-domain
  branch there. It needs different hardware before it is worth writing.

## Standing costs

These are properties of the design, not bugs to be fixed opportunistically.
Disclose them; do not paper over them.

- Detached `go()` is slower than the shared-reactor design this replaced, and
  tail latency at low and mid connection counts is worse. Throughput at
  saturation and mixed-load fairness are what was bought with it.
- The extreme tail — p99.99 and beyond — is worse on rare foreign-monitor
  dispatch than the common readiness path.
- **Accept placement is round-robin over connections, not over load.** Weight is
  a property of the traffic a connection carries later, which is unknowable at
  accept time, so heavy connections cluster across reactor shards. Descriptor
  migration between epoll instances is the expensive fix; weight-aware placement
  at accept time is the cheap one to try first and moves no descriptor.
- `cio::blocking()` itself has no admission limit; only the built-in file and
  resolver classes are bounded.
- **Tail latency moved up when the 0.1.0 feature set landed, without a known
  mechanism.** A publication-ready ten-pair matrix over the feature work found
  throughput neutral in both cells — c64 -0.00% (95% CI -1.73% to +1.76%) and
  c1024 -1.11% (-3.19% to +1.03%) — while c64 p99 and Max rose in *both* AB and
  BA strata, which under the joint gate is attributable rather than noise.

  Nothing added touches the socket hot path: the additions are new translation
  units, header-only templates, or cold-path handles. The plausible mechanism is
  code layout — the library roughly doubled in size — but that is a hypothesis,
  not a measurement. Against it: a self-comparison on this host, the same binary
  on both sides, once drifted 14.88% at c1024, so deep percentiles here are
  volatile and Max is a single sample.

  Treat this as disclosed and unexplained. Do not attribute it to a specific
  change without isolating one, and do not quietly drop it either.

## Runtime invariants

Treat the following as load-bearing contracts, not implementation suggestions.

### Runnable ownership

- `runnext` is owner-only and provides the direct handoff fast path.
- The local runnable FIFO has one producer (its owner) and may be consumed by
  the owner or thieves. Remote producers must never push into it.
- `RemoteInbox` is bounded MPSC and owner-consumed. Use it only for
  hard-directed internal work with a concrete ownership target.
- Ordinary foreign submissions, soft-affinity completions and remote-inbox
  overflow use the shared fallback. Do not turn a preference into hard affinity
  to an arbitrary busy worker.
- Public `cio::Chan<T>` is mutex-protected MPMC and is unrelated to the
  scheduler MPSC inbox.

### Publication and parking

- Stealable publication and clearing use the epoch handshake. A bitmap bit is
  an advertisement, not an exact queue-nonempty state.
- A worker must publish idle before its final runnable, timer, reactor,
  fallback and victim rechecks.
- Searcher credit is consumed exactly once on the first scheduler iteration
  after `Scheduler::park()` returns. Ordinary coroutine-to-coroutine resumes
  must not add an atomic read to this path.
- After a successful steal, preserve search responsibility for any still
  published original-victim tail.

### Waiter and frame lifetime

Waking a coroutine may let another thread resume and destroy its frame
immediately. A waker must:

1. decide ownership while holding the lock or handshake that protects the
   waiter;
2. stop touching the waiter after releasing that protection unless it won;
3. schedule the frame as its final action.

Do not add waiter/frame refcounts to the channel wake path as a shortcut around
this ownership rule.

Timer disarm is unconditional. Even when a node no longer appears armed,
`disarm()` must wait out a firing callback before the node can be reused.

### Reactor and shutdown lifetime

- Each descriptor has a stable home reactor shard.
- Descriptor generations, lifecycle pins and syscall leases protect close,
  deadline, cancellation and stale-epoll-event races.
- Same-runtime completions use the cached endpoint fast path.
- Foreign or cross-runtime completions acquire a short counted endpoint lease.
  Shutdown closes new leases, waits for existing leases, then clears the
  scheduler pointer.
- Completion endpoint identities are never recycled. Their small tombstones
  intentionally remain process-lifetime to avoid ABA and static-destruction
  use-after-free.
- `Runtime::shutdown()` is an external blocking operation. Calling it from one
  of the runtime's own workers must throw before stop or join begins.

### Task completion

- A normal `spawn()` task completes its `JoinState` directly from final suspend;
  do not reintroduce an allocation-only wrapper on the hot path.
- Invalid or already-completed tasks retain the cold wrapper needed for their
  established semantics and to avoid resuming a coroutine at final suspend.
- `go()` and `go_on()` must clear any shared continuation/completion slot before
  detaching, then use the detached abort completion only for uncaught failure.
- Preserve exception capture, result-move failure capture, detach and
  cross-runtime join behaviour.

## Benchmark rules

Performance claims require reproducible evidence:

- Build Release binaries from frozen source states.
- Map each binary to one explicit clean source revision; a byte-reproducible
  hybrid of intermediate source states is still only diagnostic evidence.
- Record and verify SHA-256 for both server/runtime binaries and the load
  generator before and after the run, and record the compile and link line with
  any binary hash. A hash with no build command behind it does not reproduce.
- Pin server and client to disjoint CPU sets.
- Warm up before the measured window.
- Pair runs and alternate AB/BA order; do not compare two independent sweeps.
- Report raw samples, paired geometric deltas, latency, server CPU and errors.
- Reject a pair if either side has socket errors, non-2xx responses, an early
  server exit or a changed input hash.
- Treat a client-saturation warning as a capacity screen, not publishable
  evidence; add `wrk` threads or client CPUs and rerun the complete matrix.
- Treat order-dependent screens as noise until a longer confirmation reproduces
  them.

HTTP comparisons use third-party `wrk`:

```sh
python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline path/to/candidate \
  --cells 1:1,8:4,64:16,256:16,1024:16 \
  --pairs 10 --warmup 5 --duration 15
```

On the 24-core benchmark host, pin the server to CPUs 0-7, `wrk` to 8-21 and the
harness itself to 23. `matrix_wrk.py` rejects overlapping CPU sets, an unpinned
harness, socket/HTTP/server failures, input hash drift, client saturation and an
incomplete pair; it writes `publication_ready` into its manifest, which validates
the *measurement*, not the candidate. `mixed_wrk.py` runs a pipelined bulk load
against a small latency probe in a second, disjointly pinned `wrk`, for the
fairness tradeoff a single saturated run cannot show. `wrk_tail.lua` adds
p99.9/p99.99/p99.999/Max; those are diagnostic until they reproduce in both
orders.

Acceptance for scheduler work is a joint gate, not a throughput headline:
low-load throughput stays neutral, saturated throughput and p50-p99 do not
materially regress, and the deep percentiles are checked separately. A rare-tail
movement that reverses between AB and BA order is noise. "No improvement" is a
valid result — the change is removed and the previous implementation stands.

Echo A/B may use the cio load generator only when the exact same frozen client
binary runs on both sides in a separately pinned process. Never rebuild the
client between sides. The echo server records which reactor shard `accept()`
placed each connection on and prints the table on SIGINT, which is what makes a
skew result placement evidence rather than a hypothesis.

Raw run directories under `bench/*/results/` are local and untracked. Nothing in
this repository may depend on reading them: state every number a claim relies on
in the text.

For time-based microbenchmarks, a positive B/A ns/op delta means B is slower.
For throughput benchmarks, a positive B/A requests/second delta means B is
faster. State the direction explicitly.

## Code and change hygiene

- Follow the existing C++ style: four-space indentation, braces on the same
  line, RAII ownership, `noexcept` on non-throwing runtime paths and explicit
  memory orders on shared atomics.
- Prefer the weakest queue and synchronization contract that satisfies actual
  ownership. Do not generalize a hot-path SPSC/SPMC/MPSC structure to MPMC
  without measured need.
- Keep public headers self-contained. Add representative downstream usage to
  `tests/test_api_surface.cpp` when changing header-visible code.
- `include/cio/version.hpp` and `project(VERSION)` must agree; CMake fails the
  configure step when they drift. Bump both.
- `.clang-format` is the style, not a suggestion. New or moved code should match
  what it produces.
- Add regression tests for the exact race or lifetime failure being fixed, not
  only a broad stress test.
- Update this file when an invariant or constraint changes, and `README.md` when
  user-visible behaviour does. Keep measurement narrative out of both: record the
  conclusion and the reason, not the run log.
- When a mechanism is measured and removed, say so in the commit message with
  the reason it failed. The commit history is the record; a future proposal is
  expected to search it before rebuilding something already rejected.
- Preserve unrelated work in a dirty tree. Do not reset, overwrite or delete
  user changes.
- Do not commit or push unless the user explicitly requests it.
- Before handoff, run `git diff --check` and report tests that were actually
  executed, including any flakes or skipped environments.
