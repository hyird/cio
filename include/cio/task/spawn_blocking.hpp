#pragma once

#include <concepts>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include "cio/detail/execution_context.hpp"
#include "cio/detail/runtime_context.hpp"
#include "cio/detail/runtime_core.hpp"
#include "cio/send.hpp"
#include "cio/task/join_handle.hpp"

namespace cio::task {

/**
 * 在当前 CIO runtime 的专用 blocking pool 上执行同步函数。
 *
 * factory 必须无捕获，全部参数由 job 按值拥有并满足 CIO Send；这避免 C++ 闭包
 * 隐式捕获引用跨越同步作用域。排队阶段 abort 可以阻止执行，函数一旦开始就
 * 无法被强制中断。未处理异常转成 JoinError::panic。
 */
template <typename Factory, typename... Args>
  requires(
      std::is_empty_v<std::remove_cvref_t<Factory>> &&
      (Send<std::remove_cvref_t<Args>> && ...) &&
      std::invocable<
          std::remove_cvref_t<Factory>,
          std::remove_cvref_t<Args>...> &&
      !std::is_reference_v<std::invoke_result_t<
          std::remove_cvref_t<Factory>,
          std::remove_cvref_t<Args>...>> &&
      (std::is_void_v<std::invoke_result_t<
           std::remove_cvref_t<Factory>,
           std::remove_cvref_t<Args>...>> ||
       Send<std::invoke_result_t<
           std::remove_cvref_t<Factory>,
           std::remove_cvref_t<Args>...>>))
auto spawn_blocking(Factory factory, Args&&... arguments) {
  std::shared_ptr<detail::RuntimeState> runtime;
  if (detail::active_execution_context) {
    runtime = detail::active_execution_context->runtime();
  }
  if (!runtime) {
    runtime = detail::active_runtime_context.lock();
  }
  if (!runtime) {
    throw std::logic_error{
        "spawn_blocking 必须在 CIO runtime 上下文中调用"};
  }

  using StoredFactory = std::remove_cvref_t<Factory>;
  return runtime->spawn_blocking(
      StoredFactory{std::move(factory)},
      std::tuple<std::remove_cvref_t<Args>...>{
          std::forward<Args>(arguments)...});
}

}  // namespace cio::task
