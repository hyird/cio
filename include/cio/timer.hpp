// One-shot and periodic timers you can hold, stop and reset.
//
//     cio::Timer timer(5s);
//     auto selected = cio::select(cio::recv(work), cio::recv(timer.chan()));
//     if (co_await selected == 1) { /* deadline */ }
//     timer.reset(5s);                      // extend it
//
//     cio::Ticker ticker(1s);
//     while (co_await ticker.chan().recv()) { /* every second, no drift */ }
//
// `sleep()` and `select`'s `after()` cover the common case and allocate nothing.
// These exist for the two things those cannot express: a deadline that outlives
// the expression that created it and can be stopped or moved, and a periodic
// tick. Go draws the line in the same place, between time.Sleep/time.After and
// time.Timer/time.Ticker.
//
// A handle owns a task, so it is a cold-path object: use `sleep()` inside a loop
// rather than constructing a Timer per iteration.
//
// OWNERSHIP: move-only. The destructor stops the timer, so a handle going out of
// scope never leaves a task waiting on a deadline nobody reads.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "cio/chan.hpp"
#include "cio/clock.hpp"
#include "cio/group.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

// Shared between the handle and the task that drives it, so a handle can be
// moved or destroyed without the task dereferencing freed storage.
struct TimerState {
    Chan<Unit> channel = make_chan<Unit>(1);
    // Wakes the driving task when the deadline moves; without it a reset() would
    // not be noticed until the old deadline elapsed.
    Chan<Unit> rearm = make_chan<Unit>(1);
    CancelSource stop;

    std::atomic<std::int64_t> deadline_ns{0};
    // Zero for a one-shot timer; the interval for a ticker.
    std::atomic<std::int64_t> period_ns{0};
    std::atomic<bool> fired{false};
    std::atomic<bool> stopped{false};

    // Set by after_func(); called instead of delivering on the channel.
    std::function<void()> on_fire;
};

Task<void> drive_timer(std::shared_ptr<TimerState> state);

}  // namespace detail

// Go's time.Timer: fires once.
class Timer {
public:
    Timer() = default;
    explicit Timer(Duration delay) : Timer(Clock::now() + delay) {}
    explicit Timer(TimePoint deadline);

    ~Timer() { stop(); }

    Timer(Timer&&) noexcept = default;
    Timer& operator=(Timer&& other) noexcept {
        if (this != &other) {
            stop();
            state_ = std::move(other.state_);
        }
        return *this;
    }
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;

    bool valid() const noexcept { return state_ != nullptr; }

    // Go's t.C. Receives once when the deadline is reached; closed when the
    // timer is stopped, so a receiver is never left waiting.
    Chan<Unit> chan() const {
        return state_ != nullptr ? state_->channel : Chan<Unit>{};
    }

    // True when this call stopped the timer before it fired, matching
    // time.Timer.Stop. False if it had already fired, or was already stopped.
    bool stop();

    // Moves the deadline. True when the timer was still pending, so a false
    // return means the old deadline had already been delivered — the caller
    // decides whether that stale tick matters.
    bool reset(Duration delay) { return reset(Clock::now() + delay); }
    bool reset(TimePoint deadline);

    // The deadline currently armed.
    TimePoint deadline() const;

private:
    friend Timer after_func_impl(TimePoint deadline, std::function<void()> fn);
    std::shared_ptr<detail::TimerState> state_;
};

// Go's time.Ticker: delivers on every interval.
//
// The next deadline advances from the previous deadline rather than from the
// moment the tick was handled, so a slow handler does not make the period drift.
// The channel holds one tick: if the receiver falls behind, ticks are dropped
// rather than queued, which is what time.Ticker does and what keeps a slow
// consumer from turning into unbounded memory.
class Ticker {
public:
    Ticker() = default;
    explicit Ticker(Duration period);

    ~Ticker() { stop(); }

    Ticker(Ticker&&) noexcept = default;
    Ticker& operator=(Ticker&& other) noexcept {
        if (this != &other) {
            stop();
            state_ = std::move(other.state_);
        }
        return *this;
    }
    Ticker(const Ticker&) = delete;
    Ticker& operator=(const Ticker&) = delete;

    bool valid() const noexcept { return state_ != nullptr; }

    Chan<Unit> chan() const {
        return state_ != nullptr ? state_->channel : Chan<Unit>{};
    }

    void stop();
    // Changes the interval; the next tick is one full period from now.
    void reset(Duration period);

private:
    std::shared_ptr<detail::TimerState> state_;
};

// Go's time.AfterFunc: runs `fn` on a runtime task after the delay. Stop the
// returned Timer to cancel it, which is reliable only before `fn` starts.
Timer after_func_impl(TimePoint deadline, std::function<void()> fn);

template <typename F>
Timer after_func(Duration delay, F fn) {
    return after_func_impl(Clock::now() + delay, std::function<void()>(std::move(fn)));
}

}  // namespace cio
