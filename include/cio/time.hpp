// Timers as seen by user code.
//
//     co_await cio::sleep(50ms);
//
// The Timer node lives in this awaiter, which lives in the coroutine frame, so
// sleeping allocates nothing. Exactly one party (the timer service) ever
// resumes a sleeper, which is why no claim protocol is needed here.
#pragma once

#include <coroutine>

#include "cio/clock.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/detail/timer.hpp"

namespace cio {

class [[nodiscard]] SleepAwaiter {
public:
    explicit SleepAwaiter(std::int64_t deadline_ns) noexcept : deadline_ns_(deadline_ns) {}

    SleepAwaiter(const SleepAwaiter&) = delete;
    SleepAwaiter& operator=(const SleepAwaiter&) = delete;

    bool await_ready() const noexcept { return deadline_ns_ <= now_ns(); }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        timer_.deadline_ns = deadline_ns_;
        timer_.waiter = h;
        timer_.on_fire = nullptr;  // default action: schedule the waiter
        // arm() may fire and resume us on another thread before it returns, so
        // nothing below may touch *this.
        detail::current_scheduler()->timers().arm(&timer_);
    }

    void await_resume() const noexcept {}

private:
    std::int64_t deadline_ns_;
    detail::Timer timer_{};
};

inline SleepAwaiter sleep(Duration duration) noexcept {
    return SleepAwaiter{deadline_from_now(duration)};
}

inline SleepAwaiter sleep_until(TimePoint deadline) noexcept {
    return SleepAwaiter{
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch()).count()};
}

}  // namespace cio
