#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cio/detail/timer_key.hpp"

namespace cio::detail {

class TimerWaitState;

struct TimerFire final {
  std::shared_ptr<TimerWaitState> state;
  std::uint64_t epoch{0};
};

/**
 * current-thread 时间驱动使用的代际 slot-map 与六层、每层 64 槽时间轮。
 *
 * 超出六层覆盖范围的 deadline 放入 overflow。当前正确性切片在一次 driver
 * 周期内整理所有非空槽，后续性能切片会在不改变 TimerKey/取消语义的前提下
 * 加入位图跳跃和分片时间轮。
 */
class TimerDriver final {
 public:
  explicit TimerDriver(std::uint64_t runtime_nonce) noexcept
      : runtime_nonce_{runtime_nonce} {}

  TimerDriver(const TimerDriver&) = delete;
  TimerDriver& operator=(const TimerDriver&) = delete;

  TimerKey insert(
      std::uint64_t deadline_tick,
      std::uint64_t epoch,
      std::weak_ptr<TimerWaitState> state);

  TimerKey replace(
      TimerKey old_key,
      std::uint64_t deadline_tick,
      std::uint64_t epoch,
      std::weak_ptr<TimerWaitState> state);

  void cancel(TimerKey key) noexcept;
  std::vector<TimerFire> process(std::uint64_t now_tick);
  [[nodiscard]] std::optional<std::uint64_t> next_deadline() const noexcept;
  [[nodiscard]] std::size_t active_timer_count() const noexcept;
  [[nodiscard]] std::uint64_t elapsed_tick() const noexcept {
    return elapsed_tick_;
  }
  void shutdown() noexcept;

 private:
  static constexpr std::size_t level_count = 6;
  static constexpr std::size_t slots_per_level = 64;
  static constexpr std::uint64_t wheel_horizon =
      std::uint64_t{1} << (level_count * 6);

  using Bucket = std::deque<TimerKey>;
  using Level = std::array<Bucket, slots_per_level>;

  struct TimerSlot final {
    std::uint64_t generation{1};
    std::uint64_t deadline_tick{0};
    std::uint64_t epoch{0};
    std::weak_ptr<TimerWaitState> state;
    bool active{false};
  };

  [[nodiscard]] bool matches_runtime(TimerKey key) const noexcept;
  [[nodiscard]] bool slot_matches(TimerKey key) const noexcept;
  TimerKey allocate_slot(
      std::uint64_t deadline_tick,
      std::uint64_t epoch,
      std::weak_ptr<TimerWaitState> state);
  void release_slot(TimerKey key) noexcept;
  void place(TimerKey key, std::uint64_t deadline_tick);
  [[nodiscard]] std::size_t choose_level(
      std::uint64_t deadline_tick) const noexcept;

  std::array<Level, level_count> levels_;
  std::multimap<std::uint64_t, TimerKey> overflow_;
  std::vector<TimerSlot> slots_;
  std::vector<std::uint32_t> free_slots_;
  std::uint64_t elapsed_tick_{0};
  const std::uint64_t runtime_nonce_;
};

}  // namespace cio::detail
