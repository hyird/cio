#pragma once

#include <concepts>
#include <functional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include "cio/send.hpp"
#include "cio/task/task.hpp"

namespace cio::detail {
struct PortableTaskAccess;
}

namespace cio::task {

/**
 * 已由调用方审计为可跨 worker 迁移的 Task 所有权。
 *
 * C++20 无法像 Rust 一样检查整个协程帧的 Send + 'static。本类型是显式安全
 * 承诺：协程帧及所有跨暂停点状态都不得含裸引用、thread-affine 对象或未同步
 * 共享可变状态。
 */
template <typename T>
class [[nodiscard]] PortableTask final {
 public:
  PortableTask(const PortableTask&) = delete;
  PortableTask& operator=(const PortableTask&) = delete;
  PortableTask(PortableTask&&) noexcept = default;
  PortableTask& operator=(PortableTask&&) noexcept = default;
  ~PortableTask() = default;

 private:
  explicit PortableTask(Task<T> task) noexcept : task_{std::move(task)} {}

  Task<T> task_;

  friend struct detail::PortableTaskAccess;
  template <typename Value>
  friend PortableTask<Value> assume_portable(Task<Value> task);
};

/**
 * 把经过人工审计的 Task 标记为 portable。
 *
 * 这是安全边界而不是自动推断。无法确认所有捕获和协程局部值都可跨线程时，
 * 不得调用；未知类型默认不会被 multi-thread runtime 接受。
 */
template <typename T>
PortableTask<T> assume_portable(Task<T> task) {
  return PortableTask<T>{std::move(task)};
}

namespace portable_detail {

template <typename>
struct TaskValue;

template <typename T>
struct TaskValue<Task<T>> final {
  using type = T;
};

template <typename T, typename Factory, typename... Args>
Task<T> invoke_owned(
    Factory factory,
    std::tuple<Args...> arguments) {
  auto task = std::apply(
      std::move(factory),
      std::move(arguments));
  if constexpr (std::is_void_v<T>) {
    co_await std::move(task);
  } else {
    co_return co_await std::move(task);
  }
}

}  // namespace portable_detail

/**
 * 用无捕获 factory 和显式拥有参数创建 portable task。
 *
 * factory 只会在目标 worker 首次 poll 时调用，因此用户协程帧也在目标 runtime
 * 内创建。所有参数必须满足 CIO Send；未知用户类型默认拒绝。
 */
template <typename Factory, typename... Args>
  requires(
      std::is_empty_v<std::remove_cvref_t<Factory>> &&
      (Send<std::remove_cvref_t<Args>> && ...) &&
      std::invocable<
          std::remove_cvref_t<Factory>,
          std::remove_cvref_t<Args>...>)
auto owned(Factory factory, Args&&... arguments) {
  using StoredFactory = std::remove_cvref_t<Factory>;
  using Produced = std::invoke_result_t<
      StoredFactory,
      std::remove_cvref_t<Args>...>;
  using Value =
      typename portable_detail::TaskValue<Produced>::type;
  static_assert(
      std::is_void_v<Value> || Send<Value>,
      "portable task 的输出必须满足 CIO Send");

  auto task = portable_detail::invoke_owned<Value>(
      StoredFactory{std::move(factory)},
      std::tuple<std::remove_cvref_t<Args>...>{
          std::forward<Args>(arguments)...});
  return assume_portable(std::move(task));
}

}  // namespace cio::task

namespace cio::detail {

struct PortableTaskAccess final {
  template <typename T>
  static Task<T> take(task::PortableTask<T> task) noexcept {
    return std::move(task.task_);
  }
};

}  // namespace cio::detail
