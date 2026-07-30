// Generic stream algorithms, shaped like Go's io package.
//
//     co_await cio::io::read_full(stream, header);
//     co_await cio::io::copy(upstream, client);   // (dst, src), as io.Copy is
//
// The concepts are Go's io.Reader and io.Writer. They are constraints rather
// than virtual interfaces: a protocol library can accept any type that reads
// and writes bytes — a TcpConn, a TLS connection, a test double — without
// either side inheriting from a runtime-polymorphic base, and without the
// concrete socket fast path paying for a vtable.
//
// The concepts describe operations that already exist. They deliberately do not
// introduce an executor association, allocator association or callback
// customization layer.
#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "cio/pool.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::io {

// io.Reader: reads some bytes into a caller-owned span. A short read is normal;
// zero means end of stream.
template <typename T>
concept Reader = requires(T& reader, std::span<std::byte> buffer) {
    { reader.read(buffer) } -> std::same_as<Task<Result<std::size_t>>>;
};

// io.Writer: writes some bytes from a caller-owned span. A short write is
// normal.
template <typename T>
concept Writer = requires(T& writer, std::span<const std::byte> buffer) {
    { writer.write(buffer) } -> std::same_as<Task<Result<std::size_t>>>;
};

// io.ReadFull: fills `buffer` completely.
//
// End of stream before the buffer is full is Errc::closed, not a short result:
// a caller asking for exactly n bytes cannot do anything useful with n-1, and
// silently returning fewer is how framing bugs start. A partially filled buffer
// on error holds whatever arrived; its contents are unspecified.
template <Reader R>
Task<Result<void>> read_full(R& reader, std::span<std::byte> buffer) {
    while (!buffer.empty()) {
        auto n = co_await reader.read(buffer);
        if (!n) co_return n.error();
        if (*n == 0) co_return Error{Errc::closed};
        buffer = buffer.subspan(*n);
    }
    co_return ok();
}

// Writes every byte, looping over short writes.
//
// Go has no io.WriteAll — its callers loop, or use io.Copy — but a socket write
// that stops short is common enough to be worth naming.
template <Writer W>
Task<Result<void>> write_all(W& writer, std::span<const std::byte> buffer) {
    while (!buffer.empty()) {
        auto n = co_await writer.write(buffer);
        if (!n) co_return n.error();
        // A writer that reports success without progress would spin forever.
        if (*n == 0) co_return Error{Errc::closed};
        buffer = buffer.subspan(*n);
    }
    co_return ok();
}

// io.Copy: copies from `src` into `dst` until end of stream, returning the byte
// count.
//
// Destination first, as io.Copy(dst, src) is. The order is worth stating
// explicitly because it is the opposite of how it reads in English, and getting
// it backwards still compiles whenever both sides are the same type.
//
// `scratch` is caller-owned so a proxy can size it for its workload and reuse
// it across connections. On error the count copied so far is lost with the
// error; callers that need it should loop themselves.
template <Writer W, Reader R>
Task<Result<std::uint64_t>> copy(W& dst, R& src, std::span<std::byte> scratch) {
    if (scratch.empty()) co_return Error{EINVAL};

    std::uint64_t total = 0;
    for (;;) {
        auto n = co_await src.read(scratch);
        if (!n) co_return n.error();
        if (*n == 0) co_return total;

        if (auto written = co_await write_all(dst, scratch.first(*n)); !written) {
            co_return written.error();
        }
        total += *n;
    }
}

// Same, borrowing a 64 KiB buffer from the runtime pool.
//
// Pooled rather than a local vector: this overload exists to be convenient, and
// a convenient function that heap-allocates 64 KiB per call is a trap. The
// buffer is out of the coroutine frame either way, which keeps this usable from
// deep task chains where frame size matters.
template <Writer W, Reader R>
Task<Result<std::uint64_t>> copy(W& dst, R& src) {
    auto scratch = buffer_pool().take(64 * 1024);
    // Qualified: an unqualified call would also find std::copy by ADL, because
    // the argument types come from namespace std.
    co_return co_await cio::io::copy(dst, src, scratch.bytes());
}

}  // namespace cio::io
