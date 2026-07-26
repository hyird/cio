#pragma once

#include <coroutine>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"

namespace cio::task {

class YieldNow final {
 public:
  [[nodiscard]] bool await_ready() const noexcept {
    return false;
  }

  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) const {
    // 编译器 ABI 句柄立即包装，异步队列不保存原生句柄类型。
    auto context = detail::require_execution_context();
    context->schedule(detail::CoroutineRef::from_abi(coroutine));
  }

  void await_resume() const noexcept {}
};

/**
 * 主动把当前 task 重新放到 ready queue 尾部。
 *
 * 取消安全：操作不拥有外部资源；task 在挂起后取消只会使排队 wake 变为无操作。
 * 线程迁移：由关联执行上下文决定，current-thread runtime 不迁移线程。
 * 阻塞行为：不阻塞调用线程。
 */
inline YieldNow yield_now() noexcept {
  return {};
}

}  // namespace cio::task
