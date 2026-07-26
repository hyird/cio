#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/join_state.hpp"
#include "cio/result.hpp"
#include "cio/task/id.hpp"
#include "cio/task/join_error.hpp"

namespace cio::detail {

struct AbortRegistration final {
  task::Id id;
  std::function<void()> abort;
  std::function<void()> abort_now;
  std::function<bool()> is_finished;
};

struct JoinHandleAccess;
struct AbortHandleAccess;

}  // namespace cio::detail

namespace cio::task {

/**
 * 可复制的远程取消权限，不包含 join 权限。
 */
class AbortHandle final {
 public:
  AbortHandle() noexcept = default;

  void abort() const noexcept {
    if (registration_ && registration_->abort) {
      registration_->abort();
    }
  }

  [[nodiscard]] bool is_finished() const {
    return !registration_ || !registration_->is_finished ||
           registration_->is_finished();
  }

  [[nodiscard]] Id id() const {
    if (!registration_) {
      throw std::logic_error{"AbortHandle 已移出或未初始化"};
    }
    return registration_->id;
  }

 private:
  explicit AbortHandle(
      std::shared_ptr<detail::AbortRegistration> registration) noexcept
      : registration_{std::move(registration)} {}

  std::shared_ptr<detail::AbortRegistration> registration_;

  friend struct detail::AbortHandleAccess;
  friend struct detail::JoinHandleAccess;
  template <typename>
  friend class JoinHandle;
};

/**
 * spawn task 的唯一 join 权限。
 *
 * 析构或移动覆盖不会取消 task，只会丢弃结果。abort 仅请求取消；必须 await join
 * 结果才能确认协程帧及局部对象已完成析构。spawn_blocking job 一旦开始执行，
 * abort 是无操作，join 会等待同步函数正常返回或抛出异常。
 */
template <typename T>
class [[nodiscard]] JoinHandle final {
 public:
  using JoinResult = Result<T, JoinError>;

  JoinHandle() noexcept = default;
  JoinHandle(const JoinHandle&) = delete;
  JoinHandle& operator=(const JoinHandle&) = delete;
  JoinHandle(JoinHandle&&) noexcept = default;
  JoinHandle& operator=(JoinHandle&&) noexcept = default;
  ~JoinHandle() = default;

  void abort() const noexcept {
    abort_handle().abort();
  }

  [[nodiscard]] bool is_finished() const {
    return !state_ || state_->is_ready();
  }

  [[nodiscard]] Id id() const {
    return abort_handle().id();
  }

  [[nodiscard]] AbortHandle abort_handle() const {
    if (!registration_) {
      throw std::logic_error{"JoinHandle 已移出或未初始化"};
    }
    return AbortHandle{registration_};
  }

  class Awaiter final {
   public:
    explicit Awaiter(std::shared_ptr<detail::JoinState<T>> state) noexcept
        : state_{std::move(state)} {}

    [[nodiscard]] bool await_ready() const {
      ensure_valid();
      return state_->is_ready();
    }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> continuation) {
      ensure_valid();
      auto context = detail::require_execution_context();
      return state_->register_waiter(
          std::move(context),
          detail::CoroutineRef::from_abi(continuation));
    }

    JoinResult await_resume() {
      ensure_valid();
      return state_->take_result();
    }

   private:
    void ensure_valid() const {
      if (!state_) {
        throw std::logic_error{"JoinHandle 已移出或未初始化"};
      }
    }

    std::shared_ptr<detail::JoinState<T>> state_;
  };

  Awaiter operator co_await() & noexcept {
    return Awaiter{state_};
  }

  Awaiter operator co_await() && noexcept {
    return Awaiter{state_};
  }

 private:
  JoinHandle(
      std::shared_ptr<detail::JoinState<T>> state,
      std::shared_ptr<detail::AbortRegistration> registration) noexcept
      : state_{std::move(state)}, registration_{std::move(registration)} {}

  std::shared_ptr<detail::JoinState<T>> state_;
  std::shared_ptr<detail::AbortRegistration> registration_;

  friend struct detail::JoinHandleAccess;
};

}  // namespace cio::task

namespace cio::detail {

/**
 * 仅供 runtime 组合异步操作使用的立即取消入口。
 *
 * 调用方必须保证目标 task 不是当前正在执行的 task。公开 AbortHandle::abort
 * 仍只提出幂等取消请求，不暴露这个内部完成屏障。
 */
struct AbortHandleAccess final {
  static void abort_now(const task::AbortHandle& handle) noexcept {
    if (handle.registration_ && handle.registration_->abort_now) {
      handle.registration_->abort_now();
    }
  }
};

struct JoinHandleAccess final {
  template <typename T>
  static task::JoinHandle<T> make(
      std::shared_ptr<JoinState<T>> state,
      std::shared_ptr<AbortRegistration> registration) {
    return task::JoinHandle<T>{
        std::move(state),
        std::move(registration)};
  }

  template <typename T>
  static Result<T, task::JoinError> take(task::JoinHandle<T>& handle) {
    if (!handle.state_) {
      throw std::logic_error{"JoinHandle 已移出或未初始化"};
    }
    return handle.state_->take_result();
  }
};

}  // namespace cio::detail
