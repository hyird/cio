#pragma once

#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include "cio/send.hpp"

namespace cio {

/**
 * C++20 环境下用于表达成功值或强类型错误的结果类型。
 *
 * Result 不持有外部引用，移动后只保证可析构和可重新赋值。调用 value/error 前
 * 必须先检查 has_value；错误分支访问 value 或成功分支访问 error 会抛出
 * std::logic_error。
 */
template <typename T, typename E>
class Result final {
 public:
  static Result success(T value) {
    return Result{std::in_place_index<0>, std::move(value)};
  }

  static Result failure(E error) {
    return Result{std::in_place_index<1>, std::move(error)};
  }

  [[nodiscard]] bool has_value() const noexcept {
    return value_.index() == 0;
  }

  [[nodiscard]] explicit operator bool() const noexcept {
    return has_value();
  }

  T& value() & {
    ensure_success();
    return std::get<0>(value_);
  }

  const T& value() const& {
    ensure_success();
    return std::get<0>(value_);
  }

  T&& value() && {
    ensure_success();
    return std::move(std::get<0>(value_));
  }

  E& error() & {
    ensure_failure();
    return std::get<1>(value_);
  }

  const E& error() const& {
    ensure_failure();
    return std::get<1>(value_);
  }

  E&& error() && {
    ensure_failure();
    return std::move(std::get<1>(value_));
  }

 private:
  template <std::size_t Index, typename Value>
  explicit Result(std::in_place_index_t<Index>, Value&& value)
      : value_{std::in_place_index<Index>, std::forward<Value>(value)} {}

  void ensure_success() const {
    if (!has_value()) {
      throw std::logic_error{"Result 当前保存错误，不能读取成功值"};
    }
  }

  void ensure_failure() const {
    if (has_value()) {
      throw std::logic_error{"Result 当前保存成功值，不能读取错误"};
    }
  }

  std::variant<T, E> value_;
};

template <typename E>
class Result<void, E> final {
 public:
  static Result success() {
    return Result{std::in_place_index<0>};
  }

  static Result failure(E error) {
    return Result{std::in_place_index<1>, std::move(error)};
  }

  [[nodiscard]] bool has_value() const noexcept {
    return value_.index() == 0;
  }

  [[nodiscard]] explicit operator bool() const noexcept {
    return has_value();
  }

  void value() const {
    if (!has_value()) {
      throw std::logic_error{"Result 当前保存错误，不能读取成功值"};
    }
  }

  E& error() & {
    ensure_failure();
    return std::get<1>(value_);
  }

  const E& error() const& {
    ensure_failure();
    return std::get<1>(value_);
  }

  E&& error() && {
    ensure_failure();
    return std::move(std::get<1>(value_));
  }

 private:
  explicit Result(std::in_place_index_t<0>)
      : value_{std::in_place_index<0>} {}

  template <typename Value>
  explicit Result(std::in_place_index_t<1>, Value&& value)
      : value_{std::in_place_index<1>, std::forward<Value>(value)} {}

  void ensure_failure() const {
    if (has_value()) {
      throw std::logic_error{"Result 当前保存成功值，不能读取错误"};
    }
  }

  std::variant<std::monostate, E> value_;
};

template <typename T, typename E>
struct send_traits<Result<T, E>>
    : std::bool_constant<
          (std::is_void_v<T> || send_traits<std::remove_cv_t<T>>::value) &&
          send_traits<std::remove_cv_t<E>>::value> {};

template <typename T, typename E>
struct sync_traits<Result<T, E>>
    : std::bool_constant<
          (std::is_void_v<T> || sync_traits<std::remove_cv_t<T>>::value) &&
          sync_traits<std::remove_cv_t<E>>::value> {};

}  // namespace cio
