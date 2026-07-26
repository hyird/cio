#pragma once

#include <coroutine>
#include <exception>
#include <memory>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"

namespace cio::task {

/**
 * 消耗一个合作式预算单位；预算耗尽时把 task 重新排到调度器。
 *
 * 每次独立 poll 获得 128 单位预算，与 Tokio 1.53.1 的初始值一致。
 */
class ConsumeBudget final {
 public:
  [[nodiscard]] bool await_ready() {
    context_ = detail::require_execution_context();
    if (context_->consume_cooperative_budget()) {
      return true;
    }
    debit_after_yield_ = true;
    return false;
  }

  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) const {
    context_->schedule(detail::CoroutineRef::from_abi(coroutine));
  }

  void await_resume() noexcept {
    if (debit_after_yield_ &&
        !context_->consume_cooperative_budget()) {
      // await_suspend 只会把当前 task 重新排队；恢复 poll 必须已重置预算。
      std::terminate();
    }
    debit_after_yield_ = false;
  }

 private:
  std::shared_ptr<detail::ExecutionContext> context_;
  bool debit_after_yield_{false};
};

inline ConsumeBudget consume_budget() noexcept {
  return {};
}

}  // namespace cio::task
