// Timers as seen by user code.
//
//     co_await cio::sleep(50ms);
//
// The Timer node lives in this awaiter, which lives in the coroutine frame, so
// sleeping allocates nothing. Exactly one party (the timer service) ever
// resumes a sleeper, which is why no claim protocol is needed here.
#pragma once

#include <coroutine>
#include <limits>
#include <memory>
#include <type_traits>

#include "cio/clock.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/detail/timer.hpp"

namespace cio {

namespace detail {

// An already-expired sleep never arms a timer. Keep the timer's storage in the
// coroutine frame but start its lifetime only on the suspending path, avoiding
// a cache-line worth of zeroing for sleep(0) and past sleep_until() calls.
class LazyTimerStorage {
public:
    LazyTimerStorage() noexcept {}
    ~LazyTimerStorage() = default;

    LazyTimerStorage(const LazyTimerStorage&) = delete;
    LazyTimerStorage& operator=(const LazyTimerStorage&) = delete;

    Timer& construct(std::int64_t deadline,
                     std::coroutine_handle<> waiter) noexcept {
        return *std::construct_at(std::addressof(storage_.timer),
                                  Timer::ArmTag{}, deadline, waiter, nullptr);
    }

private:
    static_assert(std::is_trivially_destructible_v<Timer>);

    union Storage {
        Storage() noexcept {}
        ~Storage() = default;

        Timer timer;
    } storage_;
};

}  // namespace detail

class [[nodiscard]] SleepAwaiter {
public:
    explicit SleepAwaiter(std::int64_t deadline_ns) noexcept
        : deadline_ns_(deadline_ns) {}

    SleepAwaiter(const SleepAwaiter&) = delete;
    SleepAwaiter& operator=(const SleepAwaiter&) = delete;

    bool await_ready() const noexcept {
        return deadline_ns_ == std::numeric_limits<std::int64_t>::min() ||
               deadline_ns_ <= now_ns();
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        detail::Timer& timer = timer_.construct(deadline_ns_, h);
        // arm() may fire and resume us on another thread before it returns, so
        // nothing below may touch *this.
        detail::current_scheduler()->timers().arm(&timer);
    }

    void await_resume() const noexcept {}

private:
    std::int64_t deadline_ns_;
    detail::LazyTimerStorage timer_;
};

inline SleepAwaiter sleep(Duration duration) noexcept {
    // A non-positive duration is unconditionally ready.  Spell that out
    // instead of taking two clock readings so this common polling fast path
    // stays both deterministic and cheap even when the awaiter uses lazy
    // storage that inhibits the compiler's previous constant folding.
    return SleepAwaiter{duration <= Duration::zero()
                            ? std::numeric_limits<std::int64_t>::min()
                            : deadline_from_now(duration)};
}

inline SleepAwaiter sleep_until(TimePoint deadline) noexcept {
    return SleepAwaiter{std::chrono::duration_cast<std::chrono::nanoseconds>(
                            deadline.time_since_epoch())
                            .count()};
}

}  // namespace cio
