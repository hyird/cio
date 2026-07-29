// Process signals as awaitable events.
//
//     int main() {
//         // Before the runtime starts any thread.
//         cio::signal::block({SIGINT, SIGTERM}).value();
//         return cio::run(serve());
//     }
//
//     cio::Task<int> serve() {
//         auto signals = cio::signal::SignalSet::subscribe({SIGINT, SIGTERM});
//         const auto received = co_await signals->recv();
//         co_return *received;   // SIGINT or SIGTERM
//     }
//
// STARTUP CONTRACT: block() must be called before the Runtime is constructed.
// A signal is delivered to any thread that has not blocked it, and a thread
// inherits its mask from its creator, so blocking after the scheduler and
// blocking-pool threads exist would leave those threads able to take the signal
// and run the default disposition. This module will not silently change masks
// from an arbitrary task; subscribe() reports Errc::broken if a requested
// signal is not blocked in the calling thread.
//
// Delivery is edge-like in the same way signals themselves are: identical
// signals arriving faster than they are consumed may be coalesced by the
// kernel. Use it for lifecycle events, not as a counter.
#pragma once

#include <initializer_list>
#include <vector>

#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::signal {

// Blocks `signals` in the calling thread so they can be received through a
// SignalSet instead of running their default disposition. Call from main before
// constructing a Runtime.
Result<void> block(std::initializer_list<int> signals);
Result<void> block(const std::vector<int>& signals);

// An RAII subscription to a set of signals, backed by signalfd and the epoll
// reactor. Move-only; close() is idempotent.
class SignalSet {
public:
    SignalSet() = default;

    SignalSet(SignalSet&&) noexcept = default;
    SignalSet& operator=(SignalSet&&) noexcept = default;
    SignalSet(const SignalSet&) = delete;
    SignalSet& operator=(const SignalSet&) = delete;

    // Fails with Errc::broken if any requested signal is not currently blocked
    // in this thread, because such a signal would never reach the descriptor.
    static Result<SignalSet> subscribe(std::initializer_list<int> signals);
    static Result<SignalSet> subscribe(const std::vector<int>& signals);

    bool valid() const noexcept { return socket_.valid(); }
    int native_handle() const noexcept { return socket_.native_handle(); }

    // Wakes a parked recv() with Errc::closed. Idempotent.
    void close() { socket_.close(); }

    // Suspends until one of the subscribed signals arrives, returning its
    // number. At most one task may wait at a time, matching the socket rule.
    Task<Result<int>> recv();

    void set_deadline(TimePoint deadline) { socket_.set_read_deadline(deadline); }
    void clear_deadline() { socket_.clear_read_deadline(); }

private:
    // Reuses the reactor registration, descriptor lifecycle, readiness and
    // deadline machinery that already exist for pollable descriptors. A
    // signalfd is not a socket, so this base is private and none of the socket
    // surface is re-exported.
    class Descriptor : public net::TcpStream {
    public:
        Result<void> adopt_signalfd(int fd) {
            // signalfd() already returned a non-blocking descriptor.
            return adopt(fd, /*already_nonblocking=*/true);
        }
    };

    Descriptor socket_;
};

}  // namespace cio::signal
