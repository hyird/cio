# Changelog

All notable changes to cio are recorded here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and semantic
versioning. While the major version is 0, the public API may change between
releases; each change is listed.

## [0.1.0] — unreleased

### Changed — breaking

The public API is now named the way Go names the same things, so a reader who
knows `net`, `os` and `crypto/tls` can predict cio's spelling.

| 0.0.1 | 0.1.0 | Go |
|---|---|---|
| `net::TcpStream` | `net::TcpConn` | `net.TCPConn` |
| `net::UdpSocket` | `net::UdpConn` | `net.UDPConn` |
| `TcpListener::bind()` | `TcpListener::listen()` | `net.ListenTCP` |
| `UdpSocket::bind()` | `UdpConn::listen()` | `net.ListenUDP` |
| `TcpStream::connect()` | `TcpConn::dial()` | `net.DialTCP` |
| `Socket::peer_addr()` | `Socket::remote_addr()` | `Conn.RemoteAddr` |
| `UdpSocket::recv_from()` | `UdpConn::read_from()` | `PacketConn.ReadFrom` |
| `UdpSocket::send_to()` | `UdpConn::write_to()` | `PacketConn.WriteTo` |
| `TcpStream::shutdown_write()` | `TcpConn::close_write()` | `TCPConn.CloseWrite` |
| `fs::FileInfo::is_directory()` | `is_dir()` | `FileInfo.IsDir` |
| `tls::ClientConfig` + `ServerConfig` | one `tls::Config` | `tls.Config` |

Name resolution now defaults to the built-in DNS resolver rather than
`getaddrinfo()`, matching Go's default on Unix and for the reason Go documents:
a blocked DNS query costs one task, a blocked C call costs an OS thread. Set
`LookupOptions::prefer_builtin` or `DialOptions::prefer_builtin_resolver` to
false to get the previous behaviour, which is required on a machine that
resolves names through NSS modules the built-in resolver cannot see — LDAP, NIS
or mDNS — or wherever answers must agree with `getent hosts`.

### Added

- `net::Conn`, `net::PacketConn` and `net::Listener`: Go's three net interfaces
  as concepts rather than virtual bases, so generic protocol code is possible
  without putting a vtable on the socket fast path. `tls::TlsStream` satisfies
  `Conn`, as Go's `tls.Conn` implements `net.Conn`.
- `Error::is_timeout()`, `is_cancelled()`, `is_temporary()`, `is_closed()` and
  `is_not_found()`, mirroring Go's `net.Error`, so callers classify a failure
  instead of comparing codes. A deadline and `ETIMEDOUT` both answer
  `is_timeout()`.
- `net::split_host_port()` and `net::join_host_port()`, mirroring
  `net.SplitHostPort` and `net.JoinHostPort`, including bracketing for IPv6
  literals and rejection of the ambiguous unbracketed form.
- `SocketAddr::ip()` and `TcpListener::addr()`, mirroring `TCPAddr.IP` and
  `Listener.Addr`.
- `cio::net::Resolver` with `LookupOptions`, `AddressFamily`, `lookup_host()`
  and `lookup_addr()`. `resolve()` is retained and delegates to a default
  `Resolver`.
- `cio::net::Dialer` with `DialOptions` (overall timeout, per-address
  `fallback_delay`, `nodelay`, family), plus the free `dial_tcp()`.
  `TcpConn::dial(host, port)` now delegates to a default `Dialer`, which
  interleaves IPv6 and IPv4 addresses instead of exhausting one family first.
- `CancelToken` overloads for `TcpConn::dial()` and every resolver and
  dialer entry point.
- Combined socket deadlines: `set_deadline()`, `set_timeout()` and
  `clear_deadline()` on `TcpConn` and `UdpConn`. `UdpConn` also gained
  `set_read_timeout()`, `set_write_timeout()`, `clear_read_deadline()` and
  `clear_write_deadline()`, which it previously lacked entirely.
- `cio/io.hpp`: `AsyncReader` and `AsyncWriter` concepts with generic
  `read_exact()`, `write_all()` and `copy()` free functions. The existing
  `TcpConn::write_all()` member is unchanged.
- Per-class blocking admission: `RuntimeOptions::max_file_operations` (32) and
  `max_resolver_operations` (8). Limits bound admitted operations rather than
  threads; a task awaiting admission parks without occupying a pool thread, and
  each class has its own wait queue. Admission waiters are resumed with
  `Errc::shutdown` when the pool stops.
- `cio/fs.hpp`: `cio::fs::File` with `read`/`write`, positioned `read_at`/
  `write_at`, `write_all`, `seek`, `sync`, `sync_data`, `truncate` and `stat`,
  plus free `open()`, `create()`, `stat()` and `remove()`. File operations take
  no cancellation token by design.
- `cio/signal.hpp`: `cio::signal::block()` and `SignalSet`, backed by signalfd
  and the existing reactor. No new dependency.
- Optional TLS module (`-DCIO_TLS=ON`, target `cio::tls`): `cio::tls::client()`,
  `server()` and `TlsStream`, which satisfies the stream concepts so
  `read_exact()`, `write_all()` and `copy()` work over it. OpenSSL is linked
  only by this target; the core library remains dependency-free.

- Descriptor-scoped cancellation: `Socket::set_cancel(token)` / `clear_cancel()`
  on every socket type. Once the token fires, operations in both directions
  fail with `Errc::cancelled`, including one already parked, which is woken.
  Checked at syscall admission alongside the deadline, so `read()`, `write()`
  and `accept()` gained cancellation without a signature change.
- `cio/scoped.hpp`: `cio::Timeout`, a scoped deadline that restores the
  enclosing one and can only tighten it, never extend it; and
  `cio::PollableFd`, which adopts a foreign non-blocking descriptor (eventfd,
  timerfd, inotify, a C library's fd) and exposes readiness, deadlines and
  cancellation over the worker-local reactor.
- `cio/dns.hpp`: a built-in DNS/UDP resolver on the runtime's own sockets.
  Cancellable mid-flight, consumes no blocking-pool thread, queries A and AAAA
  concurrently, reads `/etc/resolv.conf` and `/etc/hosts`. Selected through
  `net::LookupOptions::prefer_builtin` and `DialOptions::prefer_builtin_resolver`,
  mirroring Go's `Resolver.PreferGo` and `Dialer.Resolver`; the system backend
  remains the default.

### Notes

- Cancelling a connect closes its socket rather than abandoning the attempt, so
  no descriptor is left to the kernel's SYN timeout. A cancelled name lookup
  still resumes the caller immediately while `getaddrinfo()` finishes in the
  background; a system lookup in progress cannot be interrupted.
- `Dialer` races attempts: a new address is started every `fallback_delay`
  until one connects, then the losers are cancelled and joined.
- Three deferred scheduler optimizations were investigated and are **not**
  included: a lock-free MPMC ring for the shared fallback queue was built,
  measured and rejected; timer-heap lock removal was screened and found to have
  no measurable target; topology-aware victim selection cannot be validated on
  single-socket hardware. See
  [docs/scheduler-results.md](docs/scheduler-results.md#post-v001-screens).

## [0.0.1] — 2026-07-29

First tagged release. Tag `v0.0.1` points at `b1dc55a`, a retag that includes
the work-aware scheduler follow-up.

### Public API

The initial surface: `Task<T>`, `go()`, `spawn()`/`JoinHandle<T>`, `yield()`,
`Chan<T>`/`make_chan<T>()`, `select()`, `TaskGroup`, `CancelSource`/
`CancelToken`, `WaitGroup`, `Mutex`, `sleep()`, `blocking()`,
`net::TcpListener`/`TcpStream`/`UdpSocket`, and `Runtime`/`run()`/`CIO_MAIN`.

Linux and C++20 only. The core library has no external dependencies.

### Runtime

- Scheduling is sharded per worker: a single-slot `runnext` handoff, an
  owner-produced local FIFO whose published tail can be stolen, a bounded
  256-entry MPSC inbox for hard-directed internal work, one edge-triggered epoll
  instance and eventfd, and one 4-ary timer heap per worker.
- Work stealing is conditional on published imbalance rather than on scanning
  every peer, using idle/stealable bitmaps closed by an epoch handshake.
- Accepted sockets receive a stable home reactor shard. Descriptor generations,
  lifecycle pins and syscall leases make stale epoll events safe while close,
  deadline and cancellation race.
- Delayed cross-runtime completions use stable process-lifetime endpoint
  identities with counted foreign leases.
- A work-aware completion quota bounds how long one connection's completions can
  hold a worker, improving mixed-load fairness.

Design: [docs/scheduler-v2.md](docs/scheduler-v2.md). Measurements:
[docs/scheduler-results.md](docs/scheduler-results.md).

### Bounded blocking admission

`RuntimeOptions` bounds both the blocking pool's worker count
(`max_blocking_threads`, default 512) and its FIFO wait queue
(`max_blocking_queue`, default 1024). A submission to a full queue throws
`cio::SystemError` carrying `Errc::overloaded`; the same error is returned when
the pool has no service thread and the OS refuses to create its first one. A
rejected callable is never run.

### Performance

Against the pre-v2 runtime, measured with paired alternating runs and frozen
binaries:

- HTTP/`wrk`: +56.00% at 8 connections, +8.16% at 64, +9.34% at 1024;
  neutral at 1 and 256.
- Echo at 1024 connections, 128 B: +7.43%.
- `spawn()` + join: 16.86% faster at 8 workers, 19.76% at 24.

Costs are part of the record, not omitted: detached `go()` is 6.52%/6.71%
slower at 8/24 workers, HTTP c8 median p99 is 4.38 times the pre-v2 runtime,
c256 is 3.02 times, and c1024 p99 rose about 19%.

### Known limitations

- **Tail latency at 64 connections regressed.** Against the previous release,
  p99 rises 26-57% and Max 14-31%, reproducing in both AB and BA order. This is
  the price of the c1024 gains below and was not identified at release time; it
  was measured afterwards between clean commits `abf5672` and `b1dc55a`.
- Linux/epoll only. Scheduling and cancellation are cooperative. A started
  blocking callable cannot be preempted. Shutdown does not unwind tasks parked
  on channels or sockets.

Two limitations recorded at release time were later disproven:

- 1024-connection throughput was reported as unproven against the work-aware
  baseline. Measured against the previous release from clean commits it is
  **+7.62%** (95% CI +5.13% to +10.16%) with p99 more than halved.
- The retag's evidence was reported as not mapping to a committed revision. The
  static and shared libraries rebuild byte for byte from tag `v0.0.1`; only the
  benchmark *server* hash does not, because no build command was recorded with
  it.

See [docs/scheduler-results.md](docs/scheduler-results.md).

Both scheduler limitations and the remaining deferred work are tracked in
[docs/roadmap.md](docs/roadmap.md).

[0.0.1]: https://github.com/hyird/cio/releases/tag/v0.0.1
