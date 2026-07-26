#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "cio/detail/execution_context.hpp"

namespace cio::detail {

/**
 * 单 waiter、通知可合并的竞态安全唤醒槽。
 *
 * register_waiter 返回 true 表示调用协程应保持挂起；返回 false 表示已有提前
 * 通知，调用协程不应挂起。wake 和 close 均可由其他线程调用。
 */
class WakerSlot final {
 public:
  WakerSlot() = default;

  WakerSlot(const WakerSlot&) = delete;
  WakerSlot& operator=(const WakerSlot&) = delete;

  [[nodiscard]] bool register_waiter(
      std::shared_ptr<ExecutionContext> context,
      CoroutineRef coroutine) {
    std::lock_guard lock{mutex_};
    if (closed_ || notified_) {
      notified_ = false;
      return false;
    }
    if (waiter_) {
      throw std::logic_error{"WakerSlot 只允许一个等待者"};
    }

    context->park(coroutine);
    waiter_.emplace(Waiter{std::move(context)});
    return true;
  }

  void wake() noexcept {
    std::optional<Waiter> waiter;
    {
      std::lock_guard lock{mutex_};
      if (closed_) {
        return;
      }
      if (waiter_) {
        waiter = std::move(waiter_);
        waiter_.reset();
      } else {
        notified_ = true;
      }
    }

    if (waiter) {
      waiter->context->wake();
    }
  }

  void close() noexcept {
    std::lock_guard lock{mutex_};
    closed_ = true;
    notified_ = false;
    waiter_.reset();
  }

  [[nodiscard]] bool is_closed() const {
    std::lock_guard lock{mutex_};
    return closed_;
  }

 private:
  struct Waiter final {
    std::shared_ptr<ExecutionContext> context;
  };

  mutable std::mutex mutex_;
  std::optional<Waiter> waiter_;
  bool notified_{false};
  bool closed_{false};
};

}  // namespace cio::detail
