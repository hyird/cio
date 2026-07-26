#pragma once

#include <string_view>

namespace cio::time {

/**
 * Timeout 到达 deadline 时返回的强类型错误。
 */
class Elapsed final {
 public:
  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "deadline 已到";
  }

  friend constexpr bool operator==(
      const Elapsed& left,
      const Elapsed& right) noexcept = default;
};

}  // namespace cio::time
