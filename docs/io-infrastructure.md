# I/O infrastructure

Status: deferred until the runtime v2 refactor is stable. Nothing in this
document is part of the current implementation milestone.

This is a later additive design. The current milestone changes no feature or
public API; see [scheduler-v2.md](scheduler-v2.md). When this work resumes, its
fixed directions remain Go-style public APIs, epoll for readiness, and no
io_uring.

This document defines the public shape and backend rules for cio's network,
name-resolution and file APIs. The public API follows Go's small,
blocking-looking interfaces. The implementation remains native to cio: tasks,
the scheduler, epoll reactors, timers and the blocking executor. Boost.Asio is
a useful source for syscall edge cases, but its executor, completion-token and
`io_context` model is not part of cio.

## Decisions

- Linux and epoll remain the only readiness backend.
- **cio will not add io_uring.** Regular-file and system-resolver calls run on
  the bounded blocking executor.
- Public asynchronous operations return `Task<Result<T>>`; public callbacks,
  executors and completion tokens are out of scope.
- Scheduler workers, reactor shards and blocking jobs are private
  implementation details. No public header or API signature depends on them.
- APIs follow Go's behaviour, not its spelling where C++ has a safer type:
  addresses and protocols are typed, resources are move-only, buffers are
  `std::span`, and options are structs rather than strings or raw flag bags.
- Cancellation is exposed only where the backend can give honest semantics.
  It is not added mechanically to every operation.
- The current socket API remains source-compatible. New APIs are additive, and
  existing convenience functions delegate to the new objects.

HTTP, a userspace DNS protocol implementation and a second event backend are
not part of this design. TLS and process signals are follow-up modules built on
the I/O contracts defined here.

## Architecture

```text
  application
      |
      | Task<Result<T>>, CancelToken, spans, move-only handles
      v
  +----------------+  +----------------+  +----------------+
  | net            |  | net::Resolver  |  | fs::File       |
  | TCP / UDP      |  | getaddrinfo    |  | POSIX file I/O |
  +-------+--------+  +-------+--------+  +-------+--------+
          |                   |                   |
          v                   +---------+---------+
  epoll Reactor                         |
  socket deadlines              bounded admission
                                      |
                              BlockingExecutor
                                      |
                           blocking Linux syscalls
```

The reactor is for pollable descriptors. A regular file is never registered
with epoll and is never described as readiness-based asynchronous I/O. It is
asynchronous from the task's point of view because the task suspends and a
blocking-executor thread performs the syscall.

The blocking executor is deliberately separate from the fixed scheduler
workers. A full Go-style processor/carrier handoff adds scheduler states and
ownership traffic to solve a path that is absent from non-blocking network
workloads, while every blocked file call still consumes an OS thread. Keeping
that mechanism out of the scheduler preserves the smallest hot path.

The performance-oriented scheduler design is specified separately in
[scheduler-v2.md](scheduler-v2.md). It keeps one fixed OS thread per scheduler
worker, shards epoll and remote ingress per worker, and uses work stealing only
when load is measurably imbalanced. This is an internal replacement: existing
task, synchronization, runtime and network APIs do not change.

### Relationship to Go

Go DNS has two relevant paths. Its native resolver sends DNS traffic over
pollable network connections; its system/cgo resolver performs blocking libc
lookups with concurrency admission and may let the lookup finish after the
caller is cancelled. cio deliberately uses the system resolver so libc/NSS
policy is preserved, and maps that second path onto its blocking executor.

This is an implementation difference, not a public API difference:

| operation | Go backend | cio backend |
|---|---|---|
| TCP/UDP | runtime network poller | worker-local epoll shard |
| regular file | blocking syscall on an M; P may move | bounded blocking executor |
| native DNS | DNS over pollable sockets | not implemented |
| system DNS | blocking libc/cgo call with admission | blocking executor with admission |

## Common API rules

### Results and partial I/O

An operation that suspends and can fail returns `Task<Result<T>>`. A fallible
operation that cannot suspend returns `Result<T>`. Expected OS failures remain
`Error` values; programming errors remain exceptions.

`read()` and `write()` each perform a "some" operation:

- a successful short read or write returns its byte count;
- a stream read returning zero means EOF;
- an error is returned only when that attempt produced no bytes;
- `write_all()` is a composed helper that loops over short writes.

This keeps the existing `Result<std::size_t>` representation and does not
introduce a second `(count, error)` result type.

The caller owns every span passed to an I/O operation and must keep its storage
alive and unchanged, as appropriate, until the returned task completes.

### Ownership

Sockets, listeners and files are move-only RAII handles. `close()` is
idempotent. A handle must outlive every task operating on it; moving or
destroying it while an operation is outstanding is invalid. Calling `close()`
on a live socket is the supported way to wake its pending operations.

There is at most one pending operation per socket direction. Regular-file
`read_at()` and `write_at()` calls may run concurrently when their buffers are
distinct. Operations using the shared file offset (`read()` and `write()`) must
not run concurrently on the same `File`.

### Cancellation and deadlines

Go does not put a context argument on every `net.Conn` or `os.File` method, and
cio follows the same split:

| operation | interruption mechanism |
|---|---|
| TCP/UDP read or write | descriptor read/write deadline, or `close()` |
| listener accept | listener deadline, or `close()` |
| connect/dial | `CancelToken` plus `Dialer::timeout` |
| resolver lookup | `CancelToken`; late `getaddrinfo` results are discarded |
| regular-file operation | no cancellation or deadline in the first API |
| TLS handshake | `CancelToken` plus the underlying stream deadlines |

Regular-file syscalls cannot be interrupted safely by the existing runtime. In
particular, returning early from a cancelled `read()` would allow its blocking
job to keep writing into a caller-owned span after that storage was
destroyed. The API therefore waits for an in-flight file syscall and reports
its real result. It does not offer a cancellation parameter that cannot keep
its promise.

Resolver jobs are different: their strings and result storage are owned by the
job, so a cancelled lookup may resume its caller with `Errc::cancelled` while
`getaddrinfo()` finishes in the background. The late result is destroyed and
never schedules the caller a second time.

Deadlines use the runtime's monotonic `TimePoint`. A cleared deadline means no
deadline. Once a socket deadline has elapsed, operations in that direction
continue to return `Errc::timed_out` until the deadline is changed or cleared,
matching the existing connection-level semantics.

## Network

The existing typed socket classes remain the low-level network API:

```cpp
namespace cio::net {

class TcpStream {
public:
    static Task<Result<TcpStream>> connect(
        SocketAddr address, CancelToken cancel = {});

    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);
    Task<Result<void>> write_all(std::span<const std::byte> buffer);

    void set_deadline(TimePoint deadline);       // both directions
    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void clear_deadline();
    void clear_read_deadline();
    void clear_write_deadline();
};

class TcpListener {
public:
    static Result<TcpListener> bind(SocketAddr address, int backlog = 1024);
    Task<Result<TcpStream>> accept();

    void set_deadline(TimePoint deadline);
    void clear_deadline();
};

}  // namespace cio::net
```

`TcpStream::connect(SocketAddr)` remains the direct, already-resolved path.
`TcpStream::connect(host, port)` remains as a convenience and delegates to a
default `Dialer`.

### Dialer

Name resolution and address selection belong to `Dialer`, not `TcpStream`:

```cpp
namespace cio::net {

struct DialOptions {
    Duration timeout{};             // zero means no overall timeout
    Duration fallback_delay{};      // zero selects the default
    bool nodelay = true;
};

class Dialer {
public:
    explicit Dialer(DialOptions options = {});

    Task<Result<TcpStream>> dial_tcp(
        std::string host,
        std::uint16_t port,
        CancelToken cancel = {}) const;
};

Task<Result<TcpStream>> dial_tcp(
    std::string host,
    std::uint16_t port,
    CancelToken cancel = {});

}  // namespace cio::net
```

The overall timeout covers resolution and connection attempts. Address attempts
are staggered across IPv6 and IPv4 rather than waiting for every address in one
family to fail before trying the other. The first successful stream wins; all
losing attempts are closed and joined before the dial task returns.

UDP keeps its concrete `UdpSocket` API. It gains the same combined/clear
deadline operations as `TcpStream`, but it does not gain a generic protocol
string or a runtime-polymorphic `Conn` base class.

## Name resolution

The system resolver remains the source of truth, so `/etc/hosts`,
`nsswitch.conf`, local resolver configuration and libc policy continue to
apply. cio does not implement DNS packets, caching, TTL policy or a recursive
resolver.

```cpp
namespace cio::net {

enum class AddressFamily {
    any,
    ipv4,
    ipv6,
};

struct LookupOptions {
    AddressFamily family = AddressFamily::any;
};

class Resolver {
public:
    explicit Resolver(LookupOptions options = {});

    Task<Result<std::vector<SocketAddr>>> lookup_host(
        std::string host,
        std::uint16_t port,
        CancelToken cancel = {}) const;

    Task<Result<std::vector<std::string>>> lookup_addr(
        SocketAddr address,
        CancelToken cancel = {}) const;
};

Task<Result<std::vector<SocketAddr>>> resolve(
    std::string host,
    std::uint16_t port);

}  // namespace cio::net
```

The existing `resolve()` function stays and delegates to the default
`Resolver`. Lookup inputs are copied before suspension. A lookup cancelled
before admission never enters the pool; a lookup cancelled after
`getaddrinfo()` starts returns `Errc::cancelled` and discards the late result.

There is no cache in the first implementation. A cache changes DNS semantics
and needs an explicit TTL, negative-cache and invalidation policy; it should be
a separate resolver wrapper if added later.

## Files

Files live in `cio::fs`, not `cio::net`, and use managed syscall requests
exclusively:

```cpp
namespace cio::fs {

enum class Access {
    read_only,
    write_only,
    read_write,
};

struct OpenOptions {
    Access access = Access::read_only;
    bool append = false;
    bool create = false;
    bool exclusive = false;
    bool truncate = false;
    bool sync = false;
    std::uint32_t permissions = 0644;
};

using FileTime = std::chrono::system_clock::time_point;

struct FileInfo {
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    FileTime modified{};

    bool is_regular() const noexcept;
    bool is_directory() const noexcept;
};

class File {
public:
    File() = default;
    ~File();

    File(File&&) noexcept;
    File& operator=(File&&) noexcept;
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    bool valid() const noexcept;
    int native_handle() const noexcept;
    void close();

    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    Task<Result<std::size_t>> read_at(
        std::span<std::byte> buffer, std::uint64_t offset);
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);
    Task<Result<std::size_t>> write_at(
        std::span<const std::byte> buffer, std::uint64_t offset);

    Task<Result<void>> sync();
    Task<Result<void>> sync_data();
    Task<Result<void>> truncate(std::uint64_t size);
    Task<Result<FileInfo>> stat();
};

Task<Result<File>> open(std::string path, OpenOptions options = {});
Task<Result<File>> create(std::string path, std::uint32_t permissions = 0644);
Task<Result<FileInfo>> stat(std::string path);

}  // namespace cio::fs
```

`open()`, `stat()`, `read()`, `write()`, `pread()`, `pwrite()`, `fsync()`,
`fdatasync()` and `ftruncate()` execute on blocking-executor threads.
`read_at()` and `write_at()` use `pread()` and `pwrite()` and do not change the
file's shared offset.

`close()` is synchronous and idempotent so RAII destruction has deterministic
descriptor ownership. Durable completion is requested explicitly with
`sync()`; destruction does not imply a sync. Applications that use filesystems
where even `close()` can take unbounded time must call it from `cio::blocking()`
or keep file ownership outside scheduler workers.

Directory traversal, path mutation and whole-file convenience helpers can be
added after `File`; they use the same blocking executor and do not require
another reactor.

## Blocking-executor admission

File, resolver and user `cio::blocking()` calls share the lazily grown blocking
executor. Built-in classes have independent asynchronous admission limits:

```cpp
struct RuntimeOptions {
    std::size_t worker_threads = 0;
    std::size_t max_blocking_threads = 512;
    std::size_t max_file_operations = 32;
    std::size_t max_resolver_operations = 8;
};
```

The last two values limit admitted operations, not threads. A task waiting for
admission is parked without consuming an executor thread. Class-aware wait
queues prevent file admissions from sitting in front of every resolver
admission. An already-running syscall cannot be preempted.

Blocking jobs remain intrusive for ordinary file and `cio::blocking()` calls:
the node and result slot live in the suspended coroutine frame. Resolver
cancellation uses heap-owned shared state because the caller may resume before
the libc lookup returns.

Shutdown stops admission, resumes admission waiters with `Errc::shutdown`, and
waits for already-running syscalls because POSIX does not provide a safe
general-purpose way to terminate their threads. A rejected syscall is never
run inline on a scheduler worker.

The executor records current/peak threads, queue depth, admission waits by
operation class, and syscall latency. Its queues may be sharded or
class-aware internally, but those choices do not affect public APIs.

## Generic stream algorithms

TLS and protocol libraries need to operate on both `TcpStream` and wrapped
streams. cio should define structural `AsyncReader` and `AsyncWriter` concepts
rather than a virtual `Conn` base class. Generic helpers such as
`read_exact()`, `write_all()` and `copy()` are free functions constrained by
those concepts.

The concepts describe the existing operations; they do not introduce an
executor association, allocator association or callback customization layer.

## Follow-up modules

TLS is an optional library target because it adds an OpenSSL dependency. Its
public shape follows Go's layered connection model:

```cpp
auto stream = cio::tls::client(std::move(tcp), config);
co_await stream.handshake(cancel);
co_await stream.read(buffer);
```

`TlsStream` implements the stream concepts and translates OpenSSL
`WANT_READ`/`WANT_WRITE` into waits on the underlying `TcpStream`.

Process signals use `signalfd` and the epoll reactor. A signal subscription is
RAII-owned and exposes received signal numbers through a channel or an awaited
`recv()`. Signal-mask installation must occur before the runtime starts its
scheduler and blocking-executor threads, so this module needs an explicit
startup contract rather than silently changing masks from an arbitrary task.

Neither follow-up changes the file backend or introduces io_uring.

## Compatibility and implementation stages

Runtime v2 has moved the scheduler from the former shared reactor to an
internal affinity-sharded design independently of these stages. Neither backend
changes the following public API plan.

1. **Blocking admission**
   - add per-class admission, fair queue selection and shutdown rejection to
     the existing blocking executor;
   - preserve `cio::blocking()` and the meaning of `max_blocking_threads`;
   - with one scheduler worker, verify that blocking work does not delay
     channels, timers or network readiness.
2. **Resolver and dialer**
   - retain `resolve()` and both existing `TcpStream::connect()` overloads;
   - add `Resolver`, `Dialer`, connect cancellation and combined deadlines;
   - test cancel-before-submit, cancel-during-lookup, timeout races, IPv4/IPv6
      fallback and runtime shutdown.
3. **Files**
   - add file-class admission and shutdown rejection;
   - add `cio::fs::File`, offset and positioned I/O, sync, stat and truncate;
   - test every open flag, EOF, short I/O, concurrent positioned I/O, descriptor
     cleanup, admission saturation and shutdown.
4. **Generic stream algorithms**
   - extract `read_exact()`, `write_all()` and `copy()` without changing the
     concrete socket fast path.
5. **Optional TLS and signal modules**
   - keep their dependencies and startup constraints out of the core runtime.

Each stage is independently releasable. No stage is allowed to replace epoll,
add io_uring, or expose Asio-style execution machinery in the public API.
