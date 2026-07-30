// Buffered reading and writing, shaped like Go's bufio package.
//
//     cio::bufio::Reader in(stream);
//     while (auto line = co_await in.read_line()) { ... }
//
//     cio::bufio::Writer out(stream);
//     co_await out.write_all(header);
//     co_await out.flush();
//
// Without this, a line-oriented protocol costs one syscall per read, and framing
// has to be reassembled by hand at every call site. A buffered reader turns that
// into one syscall per buffer and hands out whole lines or exact-length frames
// from memory.
//
// OWNERSHIP: a Reader or Writer borrows its stream and must not outlive it. A
// Writer holds unflushed bytes, so `flush()` before dropping it — the destructor
// cannot flush, because flushing suspends and a destructor cannot await. Bytes
// left in the buffer at destruction are discarded, which is the same contract
// Go's bufio.Writer has.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "cio/io.hpp"
#include "cio/pool.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::bufio {

inline constexpr std::size_t kDefaultBufferBytes = 8 * 1024;
// A line longer than this is treated as a protocol error rather than grown
// without bound: an unterminated stream must not become unbounded memory.
inline constexpr std::size_t kDefaultMaxLineBytes = 1024 * 1024;

// Go's bufio.Reader.
template <io::Reader R>
class Reader {
public:
    explicit Reader(R& source, std::size_t capacity = kDefaultBufferBytes)
        : source_(&source),
          storage_(buffer_pool().take(capacity == 0 ? 1 : capacity)),
          // The pool rounds up to a size class, so the block can be larger than
          // the request. Use the requested capacity as the logical size, or a
          // caller asking for a small buffer would silently get a larger one and
          // see different fill behaviour than it asked for.
          capacity_(std::min(capacity == 0 ? 1 : capacity, storage_.size())) {}

    Reader(Reader&&) noexcept = default;
    Reader& operator=(Reader&&) noexcept = default;
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    // Bytes already buffered, available without a syscall.
    std::size_t buffered() const noexcept { return end_ - begin_; }

    // Reads into `out`, from the buffer when possible.
    //
    // A request at least as large as the buffer bypasses it entirely and reads
    // straight into the caller's span: copying through the buffer would be pure
    // overhead, and Go's bufio.Reader does the same.
    Task<Result<std::size_t>> read(std::span<std::byte> out) {
        if (out.empty()) co_return std::size_t{0};
        if (buffered() == 0) {
            if (out.size() >= capacity_) {
                co_return co_await source_->read(out);
            }
            if (auto filled = co_await fill(); !filled) co_return filled.error();
            if (buffered() == 0) co_return std::size_t{0};  // end of stream
        }
        const std::size_t n = std::min(out.size(), buffered());
        std::copy_n(storage_.bytes().data() + begin_, n, out.data());
        begin_ += n;
        co_return n;
    }

    // Reads exactly `out.size()` bytes. End of stream first is Errc::closed.
    Task<Result<void>> read_full(std::span<std::byte> out) {
        while (!out.empty()) {
            auto n = co_await read(out);
            if (!n) co_return n.error();
            if (*n == 0) co_return Error{Errc::closed};
            out = out.subspan(*n);
        }
        co_return ok();
    }

    // Reads through the next `delimiter`, which is included in the result.
    //
    // Returns nullopt at a clean end of stream with nothing buffered, so a loop
    // reads `while (auto chunk = co_await in.read_until('\\n'))`. Trailing bytes
    // with no delimiter are returned as a final chunk rather than dropped.
    Task<Result<std::optional<std::string>>> read_until(
        char delimiter, std::size_t limit = kDefaultMaxLineBytes) {
        std::string out;
        for (;;) {
            const std::byte* data = storage_.bytes().data();
            for (std::size_t i = begin_; i < end_; ++i) {
                if (static_cast<char>(data[i]) == delimiter) {
                    out.append(reinterpret_cast<const char*>(data + begin_),
                               i + 1 - begin_);
                    begin_ = i + 1;
                    co_return std::optional<std::string>{std::move(out)};
                }
            }
            out.append(reinterpret_cast<const char*>(data + begin_), buffered());
            begin_ = end_;
            // Bound the accumulation: an endless stream with no delimiter is a
            // protocol error, not a reason to keep allocating.
            if (out.size() > limit) co_return Error{EMSGSIZE};

            auto filled = co_await fill();
            if (!filled) co_return filled.error();
            if (buffered() == 0) {
                if (out.empty()) co_return std::optional<std::string>{};
                co_return std::optional<std::string>{std::move(out)};
            }
        }
    }

    // read_until('\n') with the trailing "\r\n" or "\n" removed.
    Task<Result<std::optional<std::string>>> read_line(
        std::size_t limit = kDefaultMaxLineBytes) {
        auto chunk = co_await read_until('\n', limit);
        if (!chunk) co_return chunk.error();
        if (!*chunk) co_return std::optional<std::string>{};

        std::string line = std::move(**chunk);
        if (!line.empty() && line.back() == '\n') line.pop_back();
        if (!line.empty() && line.back() == '\r') line.pop_back();
        co_return std::optional<std::string>{std::move(line)};
    }

    // Buffered bytes without consuming them, for a caller that must decide how
    // much of a frame it has. Only what is already in the buffer.
    std::span<const std::byte> peek() const noexcept {
        return {storage_.bytes().data() + begin_, buffered()};
    }

    // Drops `count` buffered bytes, capped at what is buffered.
    void consume(std::size_t count) noexcept {
        begin_ += std::min(count, buffered());
    }

private:
    Task<Result<void>> fill() {
        if (begin_ == end_) {
            begin_ = 0;
            end_ = 0;
        } else if (begin_ > 0) {
            // Compact rather than grow: the buffer is a window, not a queue.
            const std::size_t held = buffered();
            std::byte* data = storage_.bytes().data();
            std::copy_n(data + begin_, held, data);
            begin_ = 0;
            end_ = held;
        }
        if (end_ == capacity_) co_return ok();  // full; nothing to add

        auto n = co_await source_->read(
            storage_.bytes().subspan(end_, capacity_ - end_));
        if (!n) co_return n.error();
        end_ += *n;
        co_return ok();
    }

    R* source_ = nullptr;
    PooledBuffer storage_;
    std::size_t capacity_ = 0;
    std::size_t begin_ = 0;
    std::size_t end_ = 0;
};

// Go's bufio.Writer.
template <io::Writer W>
class Writer {
public:
    explicit Writer(W& sink, std::size_t capacity = kDefaultBufferBytes)
        : sink_(&sink),
          storage_(buffer_pool().take(capacity == 0 ? 1 : capacity)),
          // Same reason as Reader: the pool's block may exceed the request, and
          // flush timing must follow the capacity the caller chose.
          capacity_(std::min(capacity == 0 ? 1 : capacity, storage_.size())) {}

    Writer(Writer&&) noexcept = default;
    Writer& operator=(Writer&&) noexcept = default;
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    std::size_t buffered() const noexcept { return held_; }
    std::size_t available() const noexcept { return capacity_ - held_; }

    // Buffers `in`, flushing as needed. A write at least as large as the buffer
    // goes straight through after flushing what is held, so a big write is not
    // copied twice.
    Task<Result<void>> write_all(std::span<const std::byte> in) {
        if (in.size() >= capacity_) {
            if (auto flushed = co_await flush(); !flushed) {
                co_return flushed.error();
            }
            co_return co_await io::write_all(*sink_, in);
        }
        if (in.size() > available()) {
            if (auto flushed = co_await flush(); !flushed) {
                co_return flushed.error();
            }
        }
        std::copy_n(in.data(), in.size(), storage_.bytes().data() + held_);
        held_ += in.size();
        co_return ok();
    }

    Task<Result<void>> write_string(std::string_view text) {
        co_return co_await write_all(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(text.data()), text.size()});
    }

    // Sends everything held. Call this before the Writer goes away: a
    // destructor cannot flush, because flushing suspends.
    Task<Result<void>> flush() {
        if (held_ == 0) co_return ok();
        const std::size_t pending = std::exchange(held_, 0);
        co_return co_await io::write_all(
            *sink_, std::span<const std::byte>{storage_.bytes().data(),
                                              pending});
    }

private:
    W* sink_ = nullptr;
    PooledBuffer storage_;
    std::size_t capacity_ = 0;
    std::size_t held_ = 0;
};

}  // namespace cio::bufio
