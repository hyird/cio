#pragma once

#include <string>
#include <system_error>
#include <utility>

#include "cio/result.hpp"

namespace cio::io {

/**
 * CIO 统一 I/O 错误分类。
 *
 * 分类用于跨平台分支，native_code 继续保留 Win32、POSIX 或其他平台的原始
 * category/value，不能只保留一段不可判定的文本。
 */
enum class ErrorKind {
  unexpected_eof,
  write_zero,
  broken_pipe,
  invalid_data,
  other,
};

class Error final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  explicit Error(
      ErrorKind kind,
      std::string message = {},
      std::error_code native_code = {})
      : kind_{kind},
        native_code_{std::move(native_code)},
        message_{std::move(message)} {}

  [[nodiscard]] static Error unexpected_eof(
      std::string message = "提前到达 EOF") {
    return Error{ErrorKind::unexpected_eof, std::move(message)};
  }

  [[nodiscard]] static Error write_zero(
      std::string message = "非空输入没有写入任何字节") {
    return Error{ErrorKind::write_zero, std::move(message)};
  }

  [[nodiscard]] static Error broken_pipe(
      std::string message = "I/O 写端已经关闭",
      std::error_code native_code = {}) {
    return Error{
        ErrorKind::broken_pipe,
        std::move(message),
        std::move(native_code)};
  }

  [[nodiscard]] static Error other(
      std::string message,
      std::error_code native_code = {}) {
    return Error{
        ErrorKind::other,
        std::move(message),
        std::move(native_code)};
  }

  [[nodiscard]] ErrorKind kind() const noexcept {
    return kind_;
  }

  [[nodiscard]] std::error_code native_code() const noexcept {
    return native_code_;
  }

  [[nodiscard]] std::string message() const {
    return message_;
  }

 private:
  ErrorKind kind_;
  std::error_code native_code_;
  std::string message_;
};

template <typename T>
using IoResult = cio::Result<T, Error>;

}  // namespace cio::io
