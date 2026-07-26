#pragma once

#include <compare>
#include <cstdint>
#include <functional>

namespace cio::task {

/**
 * runtime 内 task 的不透明标识。
 *
 * 数值不表达调度顺序、worker 或 runtime 归属。当前实现单调分配且不复用，后续
 * 可以在保持等价与哈希语义的前提下改变分配策略。
 */
class Id final {
 public:
  constexpr Id() noexcept = default;

  [[nodiscard]] constexpr std::uint64_t value_for_diagnostics() const noexcept {
    return value_;
  }

  auto operator<=>(const Id&) const noexcept = default;

 private:
  explicit constexpr Id(std::uint64_t value) noexcept : value_{value} {}

  std::uint64_t value_{0};

  friend class IdFactory;
};

class IdFactory final {
 public:
  static constexpr Id from_runtime(std::uint64_t value) noexcept {
    return Id{value};
  }
};

}  // namespace cio::task

template <>
struct std::hash<cio::task::Id> {
  std::size_t operator()(cio::task::Id id) const noexcept {
    return std::hash<std::uint64_t>{}(id.value_for_diagnostics());
  }
};
