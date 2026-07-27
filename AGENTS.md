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
- `src/`: non-template runtime implementation
- `tests/`: test executables discovered automatically by CMake
- `examples/`: public API compile and usage examples
- `bench/`: C++ microbenchmarks and isolated Go/HTTP/echo comparisons
- `docs/scheduler-v2.md`: implemented scheduler design and release evidence
- `docs/io-infrastructure.md`: future additive design, not implemented API

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

Apply verification in proportion to the change:

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
- Record and verify SHA-256 for both server/runtime binaries and the load
  generator before and after the run.
- Pin server and client to disjoint CPU sets.
- Warm up before the measured window.
- Pair runs and alternate AB/BA order; do not compare two independent sweeps.
- Report raw samples, paired geometric deltas, latency, server CPU and errors.
- Reject a pair if either side has socket errors, non-2xx responses, an early
  server exit or a changed input hash.
- Treat order-dependent screens as noise until a longer confirmation reproduces
  them.

HTTP comparisons use third-party `wrk`:

```sh
python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline path/to/candidate \
  --cells 1:1,8:2,64:8,256:8,1024:8 \
  --pairs 10 --warmup 5 --duration 15
```

Echo A/B may use the cio load generator only when the exact same frozen client
binary runs on both sides in a separately pinned process. Never rebuild the
client between sides.

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
- Add regression tests for the exact race or lifetime failure being fixed, not
  only a broad stress test.
- Update the relevant design or benchmark document when an invariant,
  methodology or retained performance result changes.
- Preserve unrelated work in a dirty tree. Do not reset, overwrite or delete
  user changes.
- Do not commit or push unless the user explicitly requests it.
- Before handoff, run `git diff --check` and report tests that were actually
  executed, including any flakes or skipped environments.
