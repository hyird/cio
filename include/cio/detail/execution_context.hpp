#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/task_key.hpp"

namespace cio::detail {

class RuntimeState;

/**
 * cooperative budget 的一次扣费前快照。
 *
 * 快照只允许在同一个 task 的同一次 fresh poll 内恢复；完整恢复而不是简单加一，
 * 才能覆盖组合操作内部继续 poll 子操作所产生的预算调整。
 */
struct CooperativeBudgetSnapshot final {
  std::uint64_t epoch{0};
  std::size_t remaining_before{0};
};

class CooperativeBudgetState final {
 private:
  static constexpr std::size_t initial_budget{128};

  std::size_t remaining{initial_budget};
  std::uint64_t epoch{0};

  friend class ExecutionContext;
};

/**
 * Asio 关联执行器思想在 CIO task 上的最小表达。
 *
 * context 不拥有 runtime 或 task control；排队闭包必须自行验证目标仍存活。
 */
class ExecutionContext final {
 public:
  using Park = std::function<void(CoroutineRef)>;
  using Wake = std::function<void()>;

  ExecutionContext(
      std::weak_ptr<RuntimeState> runtime,
      TaskKey task_key,
      Park park,
      Wake wake,
      std::shared_ptr<ExecutionContext> cooperative_budget_owner = {})
      : runtime_{std::move(runtime)},
        task_key_{task_key},
        park_{std::move(park)},
        wake_{std::move(wake)},
        cooperative_budget_owner_{
            std::move(cooperative_budget_owner)} {
    if (cooperative_budget_owner_) {
      if (auto root =
              cooperative_budget_owner_->cooperative_budget_owner()) {
        cooperative_budget_owner_ = std::move(root);
      }
    }
  }

  void park(CoroutineRef coroutine) const noexcept {
    park_(coroutine);
  }

  void wake() const noexcept {
    // 调度与 wake 路径不得把异常泄漏到 runtime；内存耗尽属于不可恢复失败。
    wake_();
  }

  void schedule(CoroutineRef coroutine) const noexcept {
    park(coroutine);
    wake();
  }

  [[nodiscard]] std::shared_ptr<RuntimeState> runtime() const {
    return runtime_.lock();
  }

  [[nodiscard]] TaskKey task_key() const noexcept {
    return task_key_;
  }

  void reset_cooperative_budget() noexcept {
    if (cooperative_budget_owner_) {
      cooperative_budget_owner_->reset_cooperative_budget();
      return;
    }
    if (cooperative_budget_.epoch ==
        std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    ++cooperative_budget_.epoch;
    cooperative_budget_.remaining =
        CooperativeBudgetState::initial_budget;
  }

  [[nodiscard]] bool consume_cooperative_budget() noexcept {
    if (cooperative_budget_owner_) {
      return cooperative_budget_owner_->consume_cooperative_budget();
    }
    if (cooperative_budget_.remaining == 0) {
      return false;
    }
    --cooperative_budget_.remaining;
    return true;
  }

  [[nodiscard]] std::uint64_t cooperative_epoch() const noexcept {
    if (cooperative_budget_owner_) {
      return cooperative_budget_owner_->cooperative_epoch();
    }
    return cooperative_budget_.epoch;
  }

  [[nodiscard]] std::optional<CooperativeBudgetSnapshot>
  debit_cooperative_budget() noexcept {
    if (cooperative_budget_owner_) {
      return cooperative_budget_owner_->debit_cooperative_budget();
    }
    if (cooperative_budget_.remaining == 0) {
      return std::nullopt;
    }
    const CooperativeBudgetSnapshot snapshot{
        cooperative_budget_.epoch,
        cooperative_budget_.remaining};
    --cooperative_budget_.remaining;
    return snapshot;
  }

  void restore_cooperative_budget(
      CooperativeBudgetSnapshot snapshot) noexcept {
    if (cooperative_budget_owner_) {
      cooperative_budget_owner_->restore_cooperative_budget(snapshot);
      return;
    }
    if (snapshot.epoch == 0 ||
        snapshot.epoch != cooperative_budget_.epoch ||
        snapshot.remaining_before == 0 ||
        snapshot.remaining_before >
            CooperativeBudgetState::initial_budget ||
        cooperative_budget_.remaining >=
            snapshot.remaining_before) {
      // 只有持有未提交 progress ticket 的同一 task fresh poll 可以恢复。
      // 跨暂停、逆序恢复或扩张预算都表示内部所有权协议损坏。
      std::terminate();
    }
    cooperative_budget_.remaining = snapshot.remaining_before;
  }

  /**
   * 返回预算根 owner；空值表示本 context 自己内联拥有预算。
   *
   * runtime task 的常规 ExecutionContext 不为 cooperative budget 额外分配；
   * 只有组合 lane 保存父 context 的共享所有权。
   */
  [[nodiscard]] std::shared_ptr<ExecutionContext>
  cooperative_budget_owner() const noexcept {
    return cooperative_budget_owner_;
  }

 private:
  std::weak_ptr<RuntimeState> runtime_;
  TaskKey task_key_;
  Park park_;
  Wake wake_;
  CooperativeBudgetState cooperative_budget_;
  std::shared_ptr<ExecutionContext> cooperative_budget_owner_;
};

inline thread_local std::shared_ptr<ExecutionContext> active_execution_context;

class ScopedExecutionContext final {
 public:
  explicit ScopedExecutionContext(std::shared_ptr<ExecutionContext> context)
      : previous_{std::exchange(active_execution_context, std::move(context))} {}

  ScopedExecutionContext(const ScopedExecutionContext&) = delete;
  ScopedExecutionContext& operator=(const ScopedExecutionContext&) = delete;

  ~ScopedExecutionContext() {
    active_execution_context = std::move(previous_);
  }

 private:
  std::shared_ptr<ExecutionContext> previous_;
};

inline std::shared_ptr<ExecutionContext> require_execution_context() {
  if (!active_execution_context) {
    throw std::logic_error{"异步操作必须在 CIO runtime task 上下文中执行"};
  }
  return active_execution_context;
}

}  // namespace cio::detail
