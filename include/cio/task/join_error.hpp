#pragma once

#include <exception>
#include <string_view>
#include <utility>

#include "cio/task/id.hpp"

namespace cio::task {

enum class JoinErrorKind {
  cancelled,
  panic,
};

/**
 * spawn task 未正常完成时的强类型错误。
 *
 * cancelled 表示协程帧已完成取消清理；panic 表示 C++ 未处理异常已被 runtime
 * 捕获。exception() 只在 panic 分支返回有效 exception_ptr。
 */
class JoinError final {
 public:
  static JoinError cancelled(Id id) noexcept {
    return JoinError{JoinErrorKind::cancelled, id, {}};
  }

  static JoinError panic(Id id, std::exception_ptr exception) noexcept {
    return JoinError{JoinErrorKind::panic, id, std::move(exception)};
  }

  [[nodiscard]] bool is_cancelled() const noexcept {
    return kind_ == JoinErrorKind::cancelled;
  }

  [[nodiscard]] bool is_panic() const noexcept {
    return kind_ == JoinErrorKind::panic;
  }

  [[nodiscard]] JoinErrorKind kind() const noexcept {
    return kind_;
  }

  [[nodiscard]] Id id() const noexcept {
    return id_;
  }

  [[nodiscard]] const std::exception_ptr& exception() const noexcept {
    return exception_;
  }

  [[nodiscard]] std::string_view message() const noexcept {
    return is_cancelled() ? std::string_view{"task 已取消"}
                          : std::string_view{"task 发生未处理异常"};
  }

 private:
  JoinError(JoinErrorKind kind, Id id, std::exception_ptr exception) noexcept
      : kind_{kind}, id_{id}, exception_{std::move(exception)} {}

  JoinErrorKind kind_;
  Id id_;
  std::exception_ptr exception_;
};

}  // namespace cio::task
