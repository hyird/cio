// Generic stream algorithms.
//
//     co_await cio::read_exact(stream, header);
//     co_await cio::copy(client, upstream);
//
// These are constrained free functions, not members of a virtual Conn base
// class: a protocol library can accept any type that reads and writes bytes —
// a TcpStream, a TLS stream, a test double — without either side inheriting
// from a common runtime-polymorphic type, and without the concrete socket fast
// path paying for a vtable.
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

#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio {

// A type that can read some bytes into a caller-owned span. A short read is
// normal; zero means end of stream.
template <typename T>
concept AsyncReader = requires(T& reader, std::span<std::byte> buffer) {
    { reader.read(buffer) } -> std::same_as<Task<Result<std::size_t>>>;
};

// A type that can write some bytes from a caller-owned span. A short write is
// normal.
template <typename T>
concept AsyncWriter = requires(T& writer, std::span<const std::byte> buffer) {
    { writer.write(buffer) } -> std::same_as<Task<Result<std::size_t>>>;
};

// Fills `buffer` completely.
//
// End of stream before the buffer is full is Errc::closed, not a short result:
// a caller asking for exactly n bytes cannot do anything useful with n-1, and
// silently returning fewer is how framing bugs start. A partially filled buffer
// on error holds whatever arrived; its contents are unspecified.
template <AsyncReader Reader>
Task<Result<void>> read_exact(Reader& reader, std::span<std::byte> buffer) {
    while (!buffer.empty()) {
        auto n = co_await reader.read(buffer);
        if (!n) co_return n.error();
        if (*n == 0) co_return Error{Errc::closed};
        buffer = buffer.subspan(*n);
    }
    co_return ok();
}

// Writes every byte, looping over short writes.
template <AsyncWriter Writer>
Task<Result<void>> write_all(Writer& writer, std::span<const std::byte> buffer) {
    while (!buffer.empty()) {
        auto n = co_await writer.write(buffer);
        if (!n) co_return n.error();
        // A writer that reports success without progress would spin forever.
        if (*n == 0) co_return Error{Errc::closed};
        buffer = buffer.subspan(*n);
    }
    co_return ok();
}

// Copies until `from` reports end of stream, returning the byte count.
//
// `scratch` is caller-owned so a proxy can size it for its workload and reuse
// it across connections. On error the count copied so far is lost with the
// error; callers that need it should loop themselves.
template <AsyncReader Reader, AsyncWriter Writer>
Task<Result<std::uint64_t>> copy(Reader& from, Writer& to,
                                 std::span<std::byte> scratch) {
    if (scratch.empty()) co_return Error{EINVAL};

    std::uint64_t total = 0;
    for (;;) {
        auto n = co_await from.read(scratch);
        if (!n) co_return n.error();
        if (*n == 0) co_return total;

        if (auto written = co_await write_all(to, scratch.first(*n)); !written) {
            co_return written.error();
        }
        total += *n;
    }
}

// Same, with an internally owned 64 KiB buffer. The buffer is heap-allocated
// rather than held in the coroutine frame, which keeps this usable from deep
// task chains where frame size matters.
template <AsyncReader Reader, AsyncWriter Writer>
Task<Result<std::uint64_t>> copy(Reader& from, Writer& to) {
    std::vector<std::byte> scratch(64 * 1024);
    // Qualified: an unqualified call would also find std::copy by ADL, because
    // the argument types come from namespace std.
    co_return co_await cio::copy(from, to, std::span<std::byte>{scratch});
}

}  // namespace cio
