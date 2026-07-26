#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

#include "cio/detail/execution_context.hpp"
#include "cio/result.hpp"
#include "cio/task/join_error.hpp"

namespace cio::detail {

template <typename T>
class JoinState final {
 public:
  using JoinResult = Result<T, task::JoinError>;

  JoinState() = default;
  JoinState(const JoinState&) = delete;
  JoinState& operator=(const JoinState&) = delete;

  [[nodiscard]] bool is_ready() const {
    std::lock_guard lock{mutex_};
    return result_.has_value();
  }

  [[nodiscard]] bool register_waiter(
      std::shared_ptr<ExecutionContext> context,
      CoroutineRef coroutine) {
    std::lock_guard lock{mutex_};
    if (result_) {
      return false;
    }
    if (waiter_) {
      throw std::logic_error{"JoinHandle 不能被多个 task 同时等待"};
    }
    context->park(coroutine);
    waiter_.emplace(Waiter{std::move(context)});
    return true;
  }

  void complete(JoinResult result) noexcept {
    std::optional<Waiter> waiter;
    {
      std::lock_guard lock{mutex_};
      if (result_) {
        return;
      }
      result_.emplace(std::move(result));
      waiter = std::move(waiter_);
      waiter_.reset();
    }

    if (waiter) {
      try {
        waiter->context->wake();
      } catch (...) {
        // Join 完成通知属于 runtime wake 路径，不能把异常泄漏出事件循环。
        std::terminate();
      }
    }
  }

  JoinResult take_result() {
    std::lock_guard lock{mutex_};
    if (!result_) {
      throw std::logic_error{"JoinHandle 尚未完成"};
    }
    if (consumed_) {
      throw std::logic_error{"JoinHandle 结果已被消费"};
    }
    consumed_ = true;
    return std::move(*result_);
  }

 private:
  struct Waiter final {
    std::shared_ptr<ExecutionContext> context;
  };

  mutable std::mutex mutex_;
  std::optional<JoinResult> result_;
  std::optional<Waiter> waiter_;
  bool consumed_{false};
};

}  // namespace cio::detail
