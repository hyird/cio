#pragma once

#include <cstdint>

namespace cio::detail {

/**
 * runtime 内 timer slot 的代际键。
 *
 * slot 用于定位，generation 防止取消或 reset 后的旧时间轮条目命中新 timer，
 * runtime_nonce 防止不同 runtime 之间误用键。该值不表达任何对象所有权。
 */
class TimerKey final {
 public:
  constexpr TimerKey() noexcept = default;

  [[nodiscard]] constexpr bool valid() const noexcept {
    return generation_ != 0 && runtime_nonce_ != 0;
  }

  friend constexpr bool operator==(
      const TimerKey& left,
      const TimerKey& right) noexcept = default;

 private:
  constexpr TimerKey(
      std::uint32_t slot,
      std::uint64_t generation,
      std::uint64_t runtime_nonce) noexcept
      : slot_{slot},
        generation_{generation},
        runtime_nonce_{runtime_nonce} {}

  std::uint32_t slot_{0};
  std::uint64_t generation_{0};
  std::uint64_t runtime_nonce_{0};

  friend struct TimerKeyFactory;
};

struct TimerKeyFactory final {
  static constexpr TimerKey make(
      std::uint32_t slot,
      std::uint64_t generation,
      std::uint64_t runtime_nonce) noexcept {
    return TimerKey{slot, generation, runtime_nonce};
  }

  static constexpr std::uint32_t slot(TimerKey key) noexcept {
    return key.slot_;
  }

  static constexpr std::uint64_t generation(TimerKey key) noexcept {
    return key.generation_;
  }

  static constexpr std::uint64_t runtime_nonce(TimerKey key) noexcept {
    return key.runtime_nonce_;
  }
};

}  // namespace cio::detail
