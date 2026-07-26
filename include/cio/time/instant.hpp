#pragma once

#include <chrono>
#include <compare>
#include <optional>
#include <stdexcept>

#include "cio/detail/time_context.hpp"

namespace cio::time {

using Duration = std::chrono::nanoseconds;

/**
 * 可感知 CIO 暂停时钟的单调时间点。
 *
 * 在 runtime 外调用 now 使用 std::chrono::steady_clock；在 CIO task 内则读取
 * 关联 runtime 的时钟。该类型不表示墙上时间。
 */
class Instant final {
 public:
  using StdInstant = std::chrono::steady_clock::time_point;

  Instant() noexcept = default;

  static Instant now() {
    return from_std(detail::current_time());
  }

  static constexpr Instant from_std(StdInstant instant) noexcept {
    return Instant{instant};
  }

  [[nodiscard]] constexpr StdInstant into_std() const noexcept {
    return value_;
  }

  [[nodiscard]] Duration duration_since(Instant earlier) const noexcept {
    if (value_ <= earlier.value_) {
      return Duration::zero();
    }
    return std::chrono::duration_cast<Duration>(value_ - earlier.value_);
  }

  [[nodiscard]] std::optional<Duration> checked_duration_since(
      Instant earlier) const noexcept {
    if (value_ < earlier.value_) {
      return std::nullopt;
    }
    return std::chrono::duration_cast<Duration>(value_ - earlier.value_);
  }

  [[nodiscard]] Duration saturating_duration_since(
      Instant earlier) const noexcept {
    return duration_since(earlier);
  }

  [[nodiscard]] Duration elapsed() const {
    return now().saturating_duration_since(*this);
  }

  [[nodiscard]] std::optional<Instant> checked_add(
      Duration duration) const noexcept {
    const auto maximum = StdInstant::max();
    if (duration.count() > 0 &&
        maximum - value_ <
            std::chrono::duration_cast<StdInstant::duration>(duration)) {
      return std::nullopt;
    }
    return from_std(
        value_ + std::chrono::duration_cast<StdInstant::duration>(duration));
  }

  [[nodiscard]] std::optional<Instant> checked_sub(
      Duration duration) const noexcept {
    const auto minimum = StdInstant::min();
    if (duration.count() > 0 &&
        value_ - minimum <
            std::chrono::duration_cast<StdInstant::duration>(duration)) {
      return std::nullopt;
    }
    return from_std(
        value_ - std::chrono::duration_cast<StdInstant::duration>(duration));
  }

  friend constexpr bool operator==(
      const Instant& left,
      const Instant& right) noexcept = default;

  friend constexpr auto operator<=>(
      const Instant& left,
      const Instant& right) noexcept = default;

  friend Instant operator+(Instant instant, Duration duration) {
    const auto value = instant.checked_add(duration);
    if (!value) {
      throw std::overflow_error{"Instant 加法溢出"};
    }
    return *value;
  }

  friend Instant operator+(Duration duration, Instant instant) {
    return instant + duration;
  }

  friend Instant operator-(Instant instant, Duration duration) {
    const auto value = instant.checked_sub(duration);
    if (!value) {
      throw std::overflow_error{"Instant 减法溢出"};
    }
    return *value;
  }

  friend Duration operator-(Instant left, Instant right) noexcept {
    return left.saturating_duration_since(right);
  }

 private:
  explicit constexpr Instant(StdInstant instant) noexcept : value_{instant} {}

  StdInstant value_{};
};

}  // namespace cio::time
