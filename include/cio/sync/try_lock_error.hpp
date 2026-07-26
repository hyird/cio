#pragma once

#include <string_view>

namespace cio::sync {

/**
 * 非阻塞锁获取因当前会阻塞而失败。
 *
 * Tokio 的 Mutex 与 RwLock 共用这一错误类型；该错误不表示 poison。
 */
class TryLockError final {
public:
  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "操作将阻塞";
  }

  friend constexpr bool operator==(TryLockError,
                                   TryLockError) noexcept = default;
};

} // namespace cio::sync
