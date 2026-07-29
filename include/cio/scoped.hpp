// Scoped deadlines and adoption of foreign descriptors.
//
//     {
//         cio::Timeout overall(stream, 5s);   // whole exchange
//         co_await cio::write_all(stream, request);
//         {
//             cio::Timeout first_byte(stream, 200ms);   // tighter, nested
//             co_await stream.read(header);
//         }                                   // restores the 5s deadline
//         co_await cio::read_exact(stream, body);
//     }
//
//     auto fd = cio::PollableFd::adopt(::inotify_init1(IN_NONBLOCK));
//     co_await fd->readable();
//
// Socket deadlines are per-direction and persistent, which makes layering one
// over another awkward by hand: setting a tighter deadline destroys the outer
// one. Timeout saves what it finds and puts it back, so scopes nest.
#pragma once

#include <algorithm>
#include <utility>

#include "cio/clock.hpp"
#include "cio/net.hpp"
#include "cio/result.hpp"

namespace cio {

// Applies a deadline for the lifetime of the scope, then restores whatever was
// there before.
//
// The applied deadline never loosens an outer one: an inner scope asking for
// longer than the enclosing budget keeps the enclosing budget, so a nested
// timeout cannot silently extend its parent.
// Templated rather than taking net::Socket&, because the deadline setters live
// on the concrete types.
//
// Both shapes are supported, matching Go: TCPConn and UDPConn carry
// SetDeadline plus SetReadDeadline and SetWriteDeadline, while TCPListener
// carries only SetDeadline, because accept has one direction. A type with only
// the combined setter is scoped as a single direction.
template <typename S>
concept HasDirectionalDeadlines = requires(S& s, TimePoint t) {
    s.set_read_deadline(t);
    s.set_write_deadline(t);
    s.clear_read_deadline();
    s.clear_write_deadline();
};

template <typename S>
class [[nodiscard]] Timeout {
public:
    Timeout(S& socket, Duration duration, bool read = true, bool write = true)
        : Timeout(socket, Clock::now() + duration, read, write) {}

    Timeout(S& socket, TimePoint deadline, bool read = true,
            bool write = true)
        : socket_(&socket), read_(read), write_(write) {
        if constexpr (HasDirectionalDeadlines<S>) {
            if (read_) {
                saved_read_ = socket.deadline(/*write_direction=*/false);
                socket.set_read_deadline(tightest(saved_read_, deadline));
            }
            if (write_) {
                saved_write_ = socket.deadline(/*write_direction=*/true);
                socket.set_write_deadline(tightest(saved_write_, deadline));
            }
        } else {
            // One direction only, so the read/write selectors do not apply.
            read_ = true;
            write_ = false;
            saved_read_ = socket.deadline(/*write_direction=*/false);
            socket.set_deadline(tightest(saved_read_, deadline));
        }
    }

    ~Timeout() { restore(); }

    Timeout(Timeout&& other) noexcept
        : socket_(std::exchange(other.socket_, nullptr)),
          saved_read_(other.saved_read_),
          saved_write_(other.saved_write_),
          read_(other.read_),
          write_(other.write_) {}
    Timeout& operator=(Timeout&& other) noexcept {
        if (this != &other) {
            restore();
            socket_ = std::exchange(other.socket_, nullptr);
            saved_read_ = other.saved_read_;
            saved_write_ = other.saved_write_;
            read_ = other.read_;
            write_ = other.write_;
        }
        return *this;
    }
    Timeout(const Timeout&) = delete;
    Timeout& operator=(const Timeout&) = delete;

    // Ends the scope early, restoring the enclosing deadline now.
    void release() { restore(); }

private:
    static TimePoint tightest(TimePoint saved, TimePoint requested) noexcept {
        // A default-constructed saved value means "no deadline was set".
        if (saved == TimePoint{}) return requested;
        return std::min(saved, requested);
    }

    void restore() {
        S* const socket = std::exchange(socket_, nullptr);
        if (socket == nullptr) return;
        if constexpr (HasDirectionalDeadlines<S>) {
            if (read_) {
                if (saved_read_ == TimePoint{}) {
                    socket->clear_read_deadline();
                } else {
                    socket->set_read_deadline(saved_read_);
                }
            }
            if (write_) {
                if (saved_write_ == TimePoint{}) {
                    socket->clear_write_deadline();
                } else {
                    socket->set_write_deadline(saved_write_);
                }
            }
        } else {
            if (saved_read_ == TimePoint{}) {
                socket->clear_deadline();
            } else {
                socket->set_deadline(saved_read_);
            }
        }
    }

    S* socket_ = nullptr;
    TimePoint saved_read_{};
    TimePoint saved_write_{};
    bool read_ = true;
    bool write_ = true;
};

// A pollable descriptor the runtime did not create.
//
// This is the escape hatch for integrating a C library that hands out an fd —
// eventfd, timerfd, inotify, signalfd, a third-party protocol library. It
// registers the descriptor with the worker-local reactor and exposes readiness,
// deadlines and cancellation, and nothing else: the runtime does not perform,
// interpret or buffer the I/O, the caller makes its own syscalls.
//
// The descriptor must be non-blocking. Ownership transfers: close() unregisters
// and closes it, and the destructor does the same.
class PollableFd {
public:
    PollableFd() = default;

    PollableFd(PollableFd&&) noexcept = default;
    PollableFd& operator=(PollableFd&&) noexcept = default;
    PollableFd(const PollableFd&) = delete;
    PollableFd& operator=(const PollableFd&) = delete;

    // `already_nonblocking` avoids two fcntl calls when the caller created the
    // descriptor with O_NONBLOCK, which most of these interfaces support
    // directly. Passing true for a blocking descriptor parks a worker forever.
    static Result<PollableFd> adopt(int fd, bool already_nonblocking = false) {
        PollableFd owned;
        if (auto adopted = owned.impl_.adopt_foreign(fd, already_nonblocking);
            !adopted) {
            return adopted.error();
        }
        return owned;
    }

    bool valid() const noexcept { return impl_.valid(); }
    int native_handle() const noexcept { return impl_.native_handle(); }
    void close() { impl_.close(); }

    // Suspend until the kernel reports the descriptor ready. Readiness is
    // edge-triggered and only a hint: the caller must still handle EAGAIN and
    // retry, exactly as it would with raw epoll.
    detail::IoAwaiter readable() noexcept { return impl_.readable(); }
    detail::IoAwaiter writable() noexcept { return impl_.writable(); }

    void set_deadline(TimePoint deadline) { impl_.set_deadline(deadline); }
    void set_timeout(Duration timeout) { impl_.set_timeout(timeout); }
    void clear_deadline() { impl_.clear_deadline(); }
    void set_read_deadline(TimePoint d) { impl_.set_read_deadline(d); }
    void set_write_deadline(TimePoint d) { impl_.set_write_deadline(d); }
    void clear_read_deadline() { impl_.clear_read_deadline(); }
    void clear_write_deadline() { impl_.clear_write_deadline(); }
    TimePoint deadline(bool write_direction) const {
        return impl_.deadline(write_direction);
    }
    void set_cancel(CancelToken token) { impl_.set_cancel(std::move(token)); }
    void clear_cancel() { impl_.clear_cancel(); }

private:
    // Reuses the descriptor lifecycle, readiness, deadline and cancellation
    // machinery that already exists. The socket surface is private because a
    // timerfd has no peer address.
    class Descriptor : public net::TcpStream {
    public:
        Result<void> adopt_foreign(int fd, bool already_nonblocking) {
            return adopt(fd, already_nonblocking);
        }
    };

    Descriptor impl_;
};

}  // namespace cio
