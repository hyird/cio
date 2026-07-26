#pragma once

#include <stdexcept>
#include <utility>

#include "cio/detail/execution_context.hpp"
#include "cio/detail/runtime_core.hpp"
#include "cio/task/join_handle.hpp"
#include "cio/task/portable.hpp"
#include "cio/task/task.hpp"

namespace cio::task {

/**
 * 在当前 CIO runtime 上 spawn task。
 *
 * 调用只入队，不同步 poll。Task 所有权和返回值必须能安全存活到异步完成；
 * multi-thread runtime 只接受经过 `PortableTask` 安全边界审计的 task。
 */
template <typename T>
JoinHandle<T> spawn(Task<T> task) {
  auto context = detail::require_execution_context();
  auto runtime = context->runtime();
  if (!runtime) {
    throw std::runtime_error{"当前 CIO runtime 已关闭"};
  }
  return runtime->spawn(std::move(task));
}

template <typename T>
JoinHandle<T> spawn(PortableTask<T> task) {
  auto context = detail::require_execution_context();
  auto runtime = context->runtime();
  if (!runtime) {
    throw std::runtime_error{"当前 CIO runtime 已关闭"};
  }
  return runtime->spawn(
      detail::PortableTaskAccess::take(std::move(task)),
      true);
}

}  // namespace cio::task
