#pragma once

#include <chrono>
#include <cstdint>

namespace cio {

using Clock = std::chrono::steady_clock;
using Duration = Clock::duration;
using TimePoint = Clock::time_point;

// Nanoseconds on the runtime's monotonic clock. Deadlines are passed around as
// plain int64 nanoseconds so they fit in an atomic and compare cheaply in the
// timer heap.
inline std::int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
        .count();
}

template <typename Rep, typename Period>
inline std::int64_t to_ns(std::chrono::duration<Rep, Period> d) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}

inline std::int64_t deadline_from_now(Duration d) noexcept { return now_ns() + to_ns(d); }

}  // namespace cio
