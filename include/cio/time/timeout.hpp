#pragma once

#include <coroutine>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/runtime_core.hpp"
#include "cio/result.hpp"
#include "cio/task/join_handle.hpp"
#include "cio/task/task.hpp"
#include "cio/time/error.hpp"
#include "cio/time/instant.hpp"
#include "cio/time/sleep.hpp"

namespace cio::detail {

template <typename T>
class TimeoutState final {
 public:
  using TimeoutResult = Result<T, time::Elapsed>;

  TimeoutState() = default;
  TimeoutState(const TimeoutState&) = delete;
  TimeoutState& operator=(const TimeoutState&) = delete;

  ~TimeoutState() {
    cancel_parent();
  }

  void register_parent(
      std::shared_ptr<ExecutionContext> context,
      CoroutineRef coroutine) {
    std::lock_guard lock{mutex_};
    if (waiter_) {
      throw std::logic_error{"Timeout 只能由一个 task 等待"};
    }
    context->park(coroutine);
    waiter_ = std::move(context);
  }

  void set_child_abort(task::AbortHandle handle) noexcept {
    std::lock_guard lock{mutex_};
    child_abort_ = std::move(handle);
  }

  void set_timer_abort(task::AbortHandle handle) noexcept {
    std::lock_guard lock{mutex_};
    timer_abort_ = std::move(handle);
  }

  template <typename Value = T>
  void complete_success(Value value)
    requires(!std::is_void_v<Value>)
  {
    finish(
        TimeoutResult::success(std::move(value)),
        false);
  }

  void complete_success()
    requires std::is_void_v<T>
  {
    finish(TimeoutResult::success(), false);
  }

  void complete_exception(std::exception_ptr exception) noexcept {
    std::optional<std::shared_ptr<ExecutionContext>> waiter;
    task::AbortHandle timer;
    {
      std::lock_guard lock{mutex_};
      if (completed_ || cancelled_) {
        return;
      }
      completed_ = true;
      exception_ = std::move(exception);
      timer = timer_abort_;
      waiter = std::move(waiter_);
      waiter_.reset();
    }
    AbortHandleAccess::abort_now(timer);
    wake(waiter);
  }

  void expire() noexcept {
    finish(
        TimeoutResult::failure(time::Elapsed{}),
        true);
  }

  TimeoutResult take_result() {
    std::lock_guard lock{mutex_};
    if (!completed_) {
      throw std::logic_error{"Timeout 尚未完成"};
    }
    if (consumed_) {
      throw std::logic_error{"Timeout 结果已经消费"};
    }
    consumed_ = true;
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    if (!result_) {
      throw std::logic_error{"Timeout 完成但没有结果"};
    }
    return std::move(*result_);
  }

  void cancel_parent() noexcept {
    task::AbortHandle child;
    task::AbortHandle timer;
    {
      std::lock_guard lock{mutex_};
      if (cancelled_) {
        return;
      }
      cancelled_ = true;
      waiter_.reset();
      child = child_abort_;
      timer = timer_abort_;
    }
    AbortHandleAccess::abort_now(child);
    AbortHandleAccess::abort_now(timer);
  }

 private:
  void finish(
      TimeoutResult result,
      bool cancel_child) {
    std::optional<std::shared_ptr<ExecutionContext>> waiter;
    task::AbortHandle loser;
    {
      std::lock_guard lock{mutex_};
      if (completed_ || cancelled_) {
        return;
      }
      result_.emplace(std::move(result));
      completed_ = true;
      loser = cancel_child ? child_abort_ : timer_abort_;
      waiter = std::move(waiter_);
      waiter_.reset();
    }
    AbortHandleAccess::abort_now(loser);
    wake(waiter);
  }

  static void wake(
      const std::optional<std::shared_ptr<ExecutionContext>>& waiter) noexcept {
    if (waiter) {
      (*waiter)->wake();
    }
  }

  mutable std::mutex mutex_;
  std::optional<std::shared_ptr<ExecutionContext>> waiter_;
  std::optional<TimeoutResult> result_;
  std::exception_ptr exception_;
  task::AbortHandle child_abort_;
  task::AbortHandle timer_abort_;
  bool completed_{false};
  bool cancelled_{false};
  bool consumed_{false};
};

template <typename T>
Task<void> run_timeout_value(
    Task<T> value,
    std::shared_ptr<TimeoutState<T>> state) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(value);
      state->complete_success();
    } else {
      auto result = co_await std::move(value);
      state->complete_success(std::move(result));
    }
  } catch (...) {
    state->complete_exception(std::current_exception());
  }
}

template <typename T>
Task<void> run_timeout_timer(
    time::Sleep delay,
    std::shared_ptr<TimeoutState<T>> state) {
  co_await delay.as_awaiter();
  state->expire();
}

}  // namespace cio::detail

namespace cio::time {

/**
 * 对 CIO Task 施加 deadline 的移动专属组合异步操作。
 *
 * 被包装 Task 在每轮竞争中先于 deadline 获得执行机会；如果它在一次 poll
 * 中不让出，即使墙上时间已经越过 deadline，成功结果仍然优先。deadline 获胜
 * 或 Timeout 被销毁时，内部 task 帧会在发布结果前同步销毁。
 */
template <typename T>
class [[nodiscard]] Timeout final {
 public:
  using TimeoutResult = Result<T, Elapsed>;

  Timeout(const Timeout&) = delete;
  Timeout& operator=(const Timeout&) = delete;

  Timeout(Timeout&&) noexcept = default;

  Timeout& operator=(Timeout&& other) noexcept {
    if (this != &other) {
      cancel();
      value_ = std::move(other.value_);
      delay_ = std::move(other.delay_);
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~Timeout() {
    cancel();
  }

  [[nodiscard]] bool await_ready() const noexcept {
    return false;
  }

  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    return suspend(detail::CoroutineRef::from_abi(coroutine));
  }

  TimeoutResult await_resume() {
    if (!state_) {
      throw std::logic_error{"Timeout 尚未启动"};
    }
    return state_->take_result();
  }

  [[nodiscard]] const Task<T>& get_ref() const noexcept {
    return value_;
  }

  [[nodiscard]] Task<T>& get_mut() noexcept {
    return value_;
  }

  Task<T> into_inner() && {
    if (state_) {
      throw std::logic_error{"已启动的 Timeout 不能取回内部 Task"};
    }
    return std::move(value_);
  }

 private:
  Timeout(Task<T> value, Sleep delay) noexcept
      : value_{std::move(value)}, delay_{std::move(delay)} {}

  bool suspend(detail::CoroutineRef coroutine) {
    if (state_) {
      throw std::logic_error{"Timeout 不能重复等待"};
    }
    auto context = detail::require_execution_context();
    auto runtime = context->runtime();
    if (!runtime) {
      throw std::logic_error{"CIO runtime 已关闭"};
    }

    auto state = std::make_shared<detail::TimeoutState<T>>();
    state->register_parent(std::move(context), coroutine);
    state_ = state;
    try {
      auto child = runtime->spawn(detail::run_timeout_value(
          std::move(value_),
          state));
      state->set_child_abort(child.abort_handle());

      auto timer = runtime->spawn(detail::run_timeout_timer<T>(
          std::move(delay_),
          state));
      state->set_timer_abort(timer.abort_handle());
    } catch (...) {
      state->cancel_parent();
      state_.reset();
      throw;
    }
    return true;
  }

  void cancel() noexcept {
    if (state_) {
      state_->cancel_parent();
    }
  }

  Task<T> value_;
  Sleep delay_;
  std::shared_ptr<detail::TimeoutState<T>> state_;

  template <typename Value>
  friend Timeout<Value> timeout(Duration duration, Task<Value> value);
  template <typename Value>
  friend Timeout<Value> timeout_at(Instant deadline, Task<Value> value);
};

template <typename T>
Timeout<T> timeout(Duration duration, Task<T> value) {
  return Timeout<T>{
      std::move(value),
      sleep(duration)};
}

template <typename T>
Timeout<T> timeout_at(Instant deadline, Task<T> value) {
  return Timeout<T>{
      std::move(value),
      sleep_until(deadline)};
}

}  // namespace cio::time
