#include "cio/timer.hpp"

#include "cio/select.hpp"
#include "cio/spawn.hpp"
#include "cio/time.hpp"

namespace cio {
namespace detail {

// One task drives a handle for its whole life.
//
// It waits on three things: the stop signal, a rearm nudge, and the deadline
// itself. Without the rearm channel a reset() would only take effect after the
// old deadline elapsed, which would make Timer::reset() useless for the case it
// exists for — extending a deadline that has not yet passed.
Task<void> drive_timer(std::shared_ptr<TimerState> state) {
    auto stopped = state->stop.token().done();
    for (;;) {
        const std::int64_t deadline =
            state->deadline_ns.load(std::memory_order_acquire);
        const TimePoint at{std::chrono::nanoseconds{deadline}};

        auto selected = cio::select(cio::recv(stopped), cio::recv(state->rearm),
                                    cio::after_deadline(at));
        const std::size_t which = co_await selected;
        if (which == 0) break;      // stopped
        if (which == 1) continue;   // the deadline moved; re-read it

        // The deadline may have moved between the timer firing and this check.
        if (state->deadline_ns.load(std::memory_order_acquire) > now_ns()) {
            continue;
        }

        const std::int64_t period =
            state->period_ns.load(std::memory_order_acquire);
        if (period == 0) {
            // One-shot. Publish fired before delivering, so a concurrent stop()
            // reports "already fired" rather than claiming it stopped it.
            state->fired.store(true, std::memory_order_release);
            if (state->on_fire) {
                state->on_fire();
            } else {
                (void)state->channel.try_send(Unit{});
            }
            break;
        }

        // Periodic. try_send drops the tick when the previous one has not been
        // taken, which is what keeps a slow consumer from queueing without
        // bound.
        (void)state->channel.try_send(Unit{});

        // Advance from the previous deadline, not from now, so handling time
        // does not accumulate into drift. If the consumer fell far behind, skip
        // the missed deadlines rather than firing a burst to catch up.
        std::int64_t next = deadline + period;
        const std::int64_t current = now_ns();
        if (next <= current) {
            const std::int64_t behind = current - next;
            next += ((behind / period) + 1) * period;
        }
        state->deadline_ns.store(next, std::memory_order_release);
    }

    // Closing wakes any receiver, so nobody waits on a timer that will never
    // deliver again.
    state->channel.close();
    co_return;
}

namespace {

std::shared_ptr<TimerState> start(std::int64_t deadline_ns,
                                  std::int64_t period_ns,
                                  std::function<void()> on_fire) {
    auto state = std::make_shared<TimerState>();
    state->deadline_ns.store(deadline_ns, std::memory_order_relaxed);
    state->period_ns.store(period_ns, std::memory_order_relaxed);
    state->on_fire = std::move(on_fire);
    go(drive_timer(state));
    return state;
}

}  // namespace
}  // namespace detail

// ------------------------------------------------------------------ Timer ---

Timer::Timer(TimePoint deadline)
    : state_(detail::start(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              deadline.time_since_epoch())
              .count(),
          0, {})) {}

bool Timer::stop() {
    if (state_ == nullptr) return false;
    if (state_->stopped.exchange(true, std::memory_order_acq_rel)) return false;
    const bool was_pending = !state_->fired.load(std::memory_order_acquire);
    state_->stop.cancel();
    return was_pending;
}

bool Timer::reset(TimePoint deadline) {
    if (state_ == nullptr) return false;
    if (state_->stopped.load(std::memory_order_acquire)) return false;

    const bool was_pending = !state_->fired.load(std::memory_order_acquire);
    state_->deadline_ns.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadline.time_since_epoch())
            .count(),
        std::memory_order_release);
    // Nudge the driver so it re-reads the deadline instead of sleeping out the
    // old one. Buffered to one, so this never blocks.
    (void)state_->rearm.try_send(Unit{});
    return was_pending;
}

TimePoint Timer::deadline() const {
    if (state_ == nullptr) return TimePoint{};
    return TimePoint{std::chrono::nanoseconds{
        state_->deadline_ns.load(std::memory_order_acquire)}};
}

Timer after_func_impl(TimePoint deadline, std::function<void()> fn) {
    Timer timer;
    timer.state_ = detail::start(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadline.time_since_epoch())
            .count(),
        0, std::move(fn));
    return timer;
}

// ----------------------------------------------------------------- Ticker ---

Ticker::Ticker(Duration period) {
    const std::int64_t period_ns = to_ns(period);
    // A non-positive period would spin; Go panics, and reporting an invalid
    // handle is this API's equivalent of refusing.
    if (period_ns <= 0) return;
    state_ = detail::start(now_ns() + period_ns, period_ns, {});
}

void Ticker::stop() {
    if (state_ == nullptr) return;
    if (state_->stopped.exchange(true, std::memory_order_acq_rel)) return;
    state_->stop.cancel();
}

void Ticker::reset(Duration period) {
    if (state_ == nullptr) return;
    const std::int64_t period_ns = to_ns(period);
    if (period_ns <= 0) return;
    state_->period_ns.store(period_ns, std::memory_order_release);
    state_->deadline_ns.store(now_ns() + period_ns, std::memory_order_release);
    (void)state_->rearm.try_send(Unit{});
}

}  // namespace cio
