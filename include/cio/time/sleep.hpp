#pragma once

#include <chrono>
#include <coroutine>
#include <memory>
#include <stdexcept>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/timer_state.hpp"
#include "cio/time/instant.hpp"

namespace cio::time {

namespace detail_access {
struct SleepAccess;
}

/**
 * 等待到 deadline 的移动专属异步计时器。
 *
 * 所有权：Sleep 拥有等待状态，driver 只持有 weak_ptr。
 * 取消安全：析构会使 TimerKey 失效；旧 wheel 条目只能成为无操作。
 * 线程迁移：完成后通过关联 ExecutionContext 唤醒，current-thread 不迁移。
 * 阻塞行为：不阻塞 worker；实际等待由 runtime driver 执行。
 */
class [[nodiscard]] Sleep final {
 public:
  Sleep(const Sleep&) = delete;
  Sleep& operator=(const Sleep&) = delete;
  Sleep(Sleep&&) noexcept = default;
  Sleep& operator=(Sleep&& other) noexcept;
  ~Sleep();

  /**
   * 按值持有等待状态的 awaiter。
   *
   * 供组合异步操作在暂停期避免保存 Sleep 引用；销毁 awaiter 本身不取消由
   * 外层 Sleep 拥有的 timer。
   */
  class Awaiter final {
   public:
    explicit Awaiter(
        std::shared_ptr<detail::TimerWaitState> state) noexcept
        : state_{std::move(state)} {}

    [[nodiscard]] bool await_ready() const noexcept {
      return state_ && state_->is_elapsed();
    }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> coroutine) {
      if (!state_) {
        throw std::logic_error{"不能等待已移出的 Sleep"};
      }
      return state_->suspend(
          detail::require_execution_context(),
          detail::CoroutineRef::from_abi(coroutine));
    }

    void await_resume() const noexcept {}

   private:
    std::shared_ptr<detail::TimerWaitState> state_;
  };

  [[nodiscard]] bool await_ready() const noexcept;

  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    return suspend(detail::CoroutineRef::from_abi(coroutine));
  }

  void await_resume() const noexcept {}

  [[nodiscard]] Instant deadline() const noexcept;
  [[nodiscard]] bool is_elapsed() const noexcept;
  void reset(Instant deadline);

  [[nodiscard]] Awaiter as_awaiter() const noexcept {
    return Awaiter{state_};
  }

 private:
  explicit Sleep(std::shared_ptr<detail::TimerWaitState> state) noexcept
      : state_{std::move(state)} {}

  bool suspend(detail::CoroutineRef coroutine);
  void reset_without_timer(Instant deadline) noexcept;

  std::shared_ptr<detail::TimerWaitState> state_;

  friend Sleep sleep(Duration duration);
  friend Sleep sleep_until(Instant deadline);
  friend struct detail_access::SleepAccess;
};

Sleep sleep(Duration duration);
Sleep sleep_until(Instant deadline);

}  // namespace cio::time
