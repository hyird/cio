#pragma once

#include <coroutine>
#include <exception>
#include <memory>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"

namespace cio::detail {

/**
 * 一次 fresh poll 的可退款 cooperative progress 票据。
 *
 * 获取时先预扣一个预算单位；调用 made_progress 后提交扣费。若 operation 本轮
 * 返回 Pending 且没有可观察进度，票据析构会退款，对齐 Tokio 1.53.1
 * `coop::poll_proceed`/`RestoreOnPending`。票据不得跨越可能产生 fresh poll 的
 * co_await；同一 poll 内若嵌套取得多张未提交票据，必须按 LIFO 顺序析构。
 */
class CooperativeProgressTicket final {
 public:
  CooperativeProgressTicket() noexcept = default;
  CooperativeProgressTicket(const CooperativeProgressTicket&) = delete;
  CooperativeProgressTicket& operator=(
      const CooperativeProgressTicket&) = delete;

  CooperativeProgressTicket(
      CooperativeProgressTicket&& other) noexcept
      : context_{std::move(other.context_)},
        snapshot_{std::exchange(
            other.snapshot_,
            CooperativeBudgetSnapshot{})},
        refundable_{std::exchange(other.refundable_, false)} {}

  CooperativeProgressTicket& operator=(
      CooperativeProgressTicket&&) = delete;

  ~CooperativeProgressTicket() {
    refund();
  }

  void made_progress() noexcept {
    refundable_ = false;
    context_.reset();
  }

 private:
  explicit CooperativeProgressTicket(
      std::shared_ptr<ExecutionContext> context,
      CooperativeBudgetSnapshot snapshot) noexcept
      : context_{std::move(context)},
        snapshot_{snapshot},
        refundable_{true} {}

  void refund() noexcept {
    if (context_ && refundable_) {
      context_->restore_cooperative_budget(snapshot_);
    }
    context_.reset();
    snapshot_ = {};
    refundable_ = false;
  }

  std::shared_ptr<ExecutionContext> context_;
  CooperativeBudgetSnapshot snapshot_;
  bool refundable_{false};

  friend class AcquireCooperativeProgress;
};

/**
 * 取得一张 progress ticket；预算耗尽时只把当前 task 重新排队一次。
 */
class AcquireCooperativeProgress final {
 public:
  [[nodiscard]] bool await_ready() {
    context_ = require_execution_context();
    snapshot_ = context_->debit_cooperative_budget();
    if (snapshot_.has_value()) {
      return true;
    }
    return false;
  }

  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) const {
    context_->schedule(CoroutineRef::from_abi(coroutine));
  }

  [[nodiscard]] CooperativeProgressTicket await_resume() noexcept {
    if (!snapshot_.has_value()) {
      snapshot_ = context_->debit_cooperative_budget();
      if (!snapshot_.has_value()) {
        // await_suspend 后的 fresh poll 必须已经由 runtime 重置预算。
        std::terminate();
      }
    }
    return CooperativeProgressTicket{
        std::move(context_),
        *std::exchange(snapshot_, std::nullopt)};
  }

 private:
  std::shared_ptr<ExecutionContext> context_;
  std::optional<CooperativeBudgetSnapshot> snapshot_;
};

inline AcquireCooperativeProgress
acquire_cooperative_progress() noexcept {
  return {};
}

}  // namespace cio::detail
