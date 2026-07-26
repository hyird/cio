#pragma once

#include <atomic>
#include <concepts>
#include <memory>
#include <string>
#include <type_traits>

namespace cio {

namespace task {
template <typename T> class JoinHandle;
}

namespace detail {

template <typename T> consteval bool default_send_trait() {
  using Value = std::remove_cv_t<T>;
  if constexpr (requires {
                  typename std::bool_constant<static_cast<bool>(
                      Value::cio_send)>;
                }) {
    return static_cast<bool>(Value::cio_send);
  } else {
    return std::is_arithmetic_v<Value> || std::is_enum_v<Value>;
  }
}

template <typename T> consteval bool default_sync_trait() {
  using Value = std::remove_cv_t<T>;
  if constexpr (requires {
                  typename std::bool_constant<static_cast<bool>(
                      Value::cio_sync)>;
                }) {
    return static_cast<bool>(Value::cio_sync);
  } else {
    return std::is_arithmetic_v<Value> || std::is_enum_v<Value>;
  }
}

} // namespace detail

/**
 * C++20 的显式跨 worker 移动安全 trait。未知用户类型默认 false。
 *
 * CIO 自有嵌套类型可公开常量 `cio_send`/`cio_sync` 声明 mobility；用户类型
 * 只能在确认其全部可达状态满足线程安全契约后声明常量或特化 trait。
 */
template <typename T>
struct send_traits : std::bool_constant<detail::default_send_trait<T>()> {};

template <typename T>
struct sync_traits : std::bool_constant<detail::default_sync_trait<T>()> {};

template <> struct send_traits<std::string> : std::true_type {};

template <> struct sync_traits<std::string> : std::true_type {};

template <typename T> struct sync_traits<std::atomic<T>> : std::true_type {};

template <typename T>
struct send_traits<std::shared_ptr<T>> : sync_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<std::shared_ptr<T>> : sync_traits<std::remove_cv_t<T>> {};

template <typename T>
struct send_traits<task::JoinHandle<T>>
    : std::bool_constant<std::is_void_v<T> || send_traits<T>::value> {};

template <typename T>
concept Send = send_traits<std::remove_cvref_t<T>>::value;

template <typename T>
concept Sync = sync_traits<std::remove_cvref_t<T>>::value;

} // namespace cio
