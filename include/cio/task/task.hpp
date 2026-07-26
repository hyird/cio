#pragma once

#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"

namespace cio {

template <typename T>
class Task;

namespace detail {

template <typename>
class TaskControl;

class TaskPromiseBase {
 public:
  [[nodiscard]] std::suspend_always initial_suspend() const noexcept {
    return {};
  }

  class FinalAwaiter final {
   public:
    [[nodiscard]] bool await_ready() const noexcept {
      return false;
    }

    template <typename Promise>
    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Promise> handle) const noexcept {
      // std::coroutine_handle 是编译器 ABI 参数；不在 CIO 内部保存裸句柄。
      auto& promise = handle.promise();
      const auto continuation = promise.continuation();
      if (continuation.valid()) {
        // symmetric transfer 直接回到父 Task，不增加普通函数递归深度。
        return continuation.native_for_abi();
      }
      return std::noop_coroutine();
    }

    void await_resume() const noexcept {}
  };

  [[nodiscard]] FinalAwaiter final_suspend() const noexcept {
    return {};
  }

  void bind_execution(
      std::shared_ptr<ExecutionContext> context,
      CoroutineRef continuation = {}) noexcept {
    execution_context_ = std::move(context);
    continuation_ = continuation;
  }

  [[nodiscard]] const std::shared_ptr<ExecutionContext>&
  execution_context() const noexcept {
    return execution_context_;
  }

  [[nodiscard]] CoroutineRef continuation() const noexcept {
    return continuation_;
  }

  void unhandled_exception() noexcept {
    exception_ = std::current_exception();
  }

 protected:
  void rethrow_if_failed() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

 private:
  std::shared_ptr<ExecutionContext> execution_context_;
  CoroutineRef continuation_;
  std::exception_ptr exception_;
};

template <typename T>
class TaskPromise final : public TaskPromiseBase {
 public:
  Task<T> get_return_object() noexcept;

  template <typename Value>
    requires std::constructible_from<T, Value&&>
  void return_value(Value&& value) {
    value_.emplace(std::forward<Value>(value));
  }

  T take_result() {
    rethrow_if_failed();
    if (!value_) {
      throw std::logic_error{"Task 已完成但没有返回值"};
    }
    return std::move(*value_);
  }

 private:
  std::optional<T> value_;
};

template <>
class TaskPromise<void> final : public TaskPromiseBase {
 public:
  Task<void> get_return_object() noexcept;

  void return_void() const noexcept {}

  void take_result() {
    rethrow_if_failed();
  }
};

}  // namespace detail

/**
 * lazy、移动专属的 C++20 协程 task。
 *
 * Task 独占协程帧。只能通过右值 co_await 转移子 task 所有权，或移动到 runtime
 * spawn。Task 本身不会创建线程，也不会在构造时执行用户协程。
 */
template <typename T>
class [[nodiscard]] Task final {
 public:
  using promise_type = detail::TaskPromise<T>;

  Task() noexcept = default;
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&) noexcept = default;
  Task& operator=(Task&&) noexcept = default;
  ~Task() = default;

  class Awaiter final {
   public:
    explicit Awaiter(detail::CoroutineOwner<promise_type> coroutine) noexcept
        : coroutine_{std::move(coroutine)} {}

    [[nodiscard]] bool await_ready() const noexcept {
      return coroutine_.done();
    }

    template <typename ParentPromise>
    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<ParentPromise> continuation) {
      // 编译器 ABI 句柄立即包装；后续异步排队只携带 CoroutineRef。
      auto context = detail::require_execution_context();
      coroutine_.promise().bind_execution(
          context,
          detail::CoroutineRef::from_abi(continuation));
      // 普通 Task 组合与 Rust Future 一样在当前 poll 内继续；这里只在编译器
      // 协程 ABI 边界取回原生值，不转移帧所有权。
      return coroutine_.ref().native_for_abi();
    }

    decltype(auto) await_resume() {
      if (!coroutine_.done()) {
        throw std::logic_error{"Task 在完成前被恢复"};
      }
      return coroutine_.promise().take_result();
    }

   private:
    detail::CoroutineOwner<promise_type> coroutine_;
  };

  Awaiter operator co_await() && noexcept {
    return Awaiter{std::move(coroutine_)};
  }

  Awaiter operator co_await() & = delete;

 private:
  explicit Task(detail::CoroutineOwner<promise_type> coroutine) noexcept
      : coroutine_{std::move(coroutine)} {}

  void bind_root_context(
      const std::shared_ptr<detail::ExecutionContext>& context) noexcept {
    coroutine_.promise().bind_execution(context);
  }

  [[nodiscard]] bool valid() const noexcept {
    return coroutine_.valid();
  }

  [[nodiscard]] bool done() const noexcept {
    return coroutine_.done();
  }

  [[nodiscard]] detail::CoroutineRef coroutine_ref() const noexcept {
    return coroutine_.ref();
  }

  void set_resumable(detail::CoroutineRef coroutine) noexcept {
    coroutine_.set_resumable(coroutine);
  }

  void resume_current() {
    coroutine_.resume_current();
  }

  decltype(auto) take_result() {
    return coroutine_.promise().take_result();
  }

  void reset() noexcept {
    coroutine_.reset();
  }

  detail::CoroutineOwner<promise_type> coroutine_;

  template <typename>
  friend class detail::TaskControl;
  friend class detail::RuntimeState;
  friend class detail::TaskPromise<T>;
};

namespace detail {

template <typename T>
Task<T> TaskPromise<T>::get_return_object() noexcept {
  return Task<T>{CoroutineOwner<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
  return Task<void>{CoroutineOwner<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail

}  // namespace cio
