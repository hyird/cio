#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace cio::detail {

/**
 * runtime 内部 task slot 的 generation key。
 *
 * TaskKey 不拥有 task，也不暴露协程地址。slot 复用后 generation 变化，来自旧
 * task 的 ready/wake token 会解析失败；runtime_nonce 防止 key 跨 runtime 误用。
 */
class TaskKey final {
 public:
  constexpr TaskKey() noexcept = default;

  [[nodiscard]] constexpr bool valid() const noexcept {
    return generation_ != 0 && runtime_nonce_ != 0;
  }

  auto operator<=>(const TaskKey&) const noexcept = default;

 private:
  constexpr TaskKey(
      std::uint32_t slot,
      std::uint64_t generation,
      std::uint64_t runtime_nonce) noexcept
      : slot_{slot},
        generation_{generation},
        runtime_nonce_{runtime_nonce} {}

  std::uint32_t slot_{0};
  std::uint64_t generation_{0};
  std::uint64_t runtime_nonce_{0};

  friend class TaskKeyFactory;
  friend struct TaskKeyHash;
};

class TaskKeyFactory final {
 public:
  static constexpr TaskKey make(
      std::uint32_t slot,
      std::uint64_t generation,
      std::uint64_t runtime_nonce) noexcept {
    return TaskKey{slot, generation, runtime_nonce};
  }

  static constexpr std::uint32_t slot(TaskKey key) noexcept {
    return key.slot_;
  }

  static constexpr std::uint64_t generation(TaskKey key) noexcept {
    return key.generation_;
  }

  static constexpr std::uint64_t runtime_nonce(TaskKey key) noexcept {
    return key.runtime_nonce_;
  }
};

struct TaskKeyHash final {
  std::size_t operator()(TaskKey key) const noexcept {
    const auto first = std::hash<std::uint32_t>{}(key.slot_);
    const auto second = std::hash<std::uint64_t>{}(key.generation_);
    const auto third = std::hash<std::uint64_t>{}(key.runtime_nonce_);
    const auto combined =
        first ^ (second + static_cast<std::size_t>(0x9e3779b9U) +
                 (first << 6U) + (first >> 2U));
    return combined ^ (third + static_cast<std::size_t>(0x9e3779b9U) +
                       (combined << 6U) + (combined >> 2U));
  }
};

}  // namespace cio::detail
