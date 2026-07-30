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

// io.Writer, with Go's contract: write() returns the full length unless it
// returns an error. A short count without an error is a broken writer — Go
// forbids it for the same reason, because every caller would otherwise need a
// retry loop, and the ones that forget become data-loss bugs. This is why there
// is no write_all(): write() already is one.
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

        auto written = co_await dst.write(scratch.first(*n));
        if (!written) co_return written.error();
        // The Writer contract makes a short count without an error a defect in
        // the destination, as io.ErrShortWrite does.
        if (*written != *n) co_return Error{EIO};
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
    auto scratch = buffer_pool().get(64 * 1024);
    // Qualified: an unqualified call would also find std::copy by ADL, because
    // the argument types come from namespace std.
    co_return co_await cio::io::copy(dst, src, scratch.bytes());
}

// io.ReadAll: reads until end of stream.
//
// `limit` is not optional the way io.ReadAll's absence of one is: reading an
// untrusted stream into memory without a bound is how a service is made to
// exhaust its own heap. Exceeding it is EMSGSIZE rather than a truncated result,
// so a caller cannot mistake a cut-off body for a complete one.
template <Reader R>
Task<Result<std::vector<std::byte>>> read_all(
    R& reader, std::size_t limit = 64u * 1024 * 1024) {
    std::vector<std::byte> out;
    std::byte chunk[16 * 1024];
    for (;;) {
        auto n = co_await reader.read(chunk);
        if (!n) co_return n.error();
        if (*n == 0) co_return out;
        if (out.size() + *n > limit) co_return Error{EMSGSIZE};
        out.insert(out.end(), chunk, chunk + *n);
    }
}

// io.LimitReader: a reader that stops after `limit` bytes.
//
// Wrapping rather than a parameter on read: a length-delimited body is handed to
// a parser that should not need to know it is bounded, which is exactly what
// io.LimitReader is for.
template <Reader R>
class LimitReader {
public:
    LimitReader(R& source, std::uint64_t limit) noexcept
        : source_(&source), remaining_(limit) {}

    std::uint64_t remaining() const noexcept { return remaining_; }

    Task<Result<std::size_t>> read(std::span<std::byte> out) {
        if (remaining_ == 0) co_return std::size_t{0};  // end of the slice
        if (out.size() > remaining_) {
            out = out.first(static_cast<std::size_t>(remaining_));
        }
        auto n = co_await source_->read(out);
        if (!n) co_return n.error();
        remaining_ -= *n;
        co_return *n;
    }

private:
    R* source_ = nullptr;
    std::uint64_t remaining_ = 0;
};

// io.TeeReader: reads from `source` and writes what it read to `mirror`.
//
// A mirror write that fails fails the read, rather than being dropped: a tee
// whose copy silently goes missing is worse than one that stops.
template <Reader R, Writer W>
class TeeReader {
public:
    TeeReader(R& source, W& mirror) noexcept
        : source_(&source), mirror_(&mirror) {}

    Task<Result<std::size_t>> read(std::span<std::byte> out) {
        auto n = co_await source_->read(out);
        if (!n) co_return n.error();
        if (*n == 0) co_return std::size_t{0};
        auto mirrored = co_await mirror_->write(out.first(*n));
        if (!mirrored) co_return mirrored.error();
        if (*mirrored != *n) co_return Error{EIO};
        co_return *n;
    }

private:
    R* source_ = nullptr;
    W* mirror_ = nullptr;
};

}  // namespace cio::io
