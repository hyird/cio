#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/timer_key.hpp"

namespace cio::detail {

class RuntimeState;

/**
 * Sleep 与 timer driver 之间的共享状态。
 *
 * driver 只持有 weak_ptr；Sleep 独占可观察生命周期。reset 会递增 epoch 并换用
 * 新 TimerKey，因此旧 wheel 条目和已经取出的旧 fire 均不能提前完成新 deadline。
 */
class TimerWaitState final
    : public std::enable_shared_from_this<TimerWaitState> {
 public:
  TimerWaitState(
      std::weak_ptr<RuntimeState> runtime,
      std::chrono::steady_clock::time_point deadline) noexcept;
  ~TimerWaitState();

  TimerWaitState(const TimerWaitState&) = delete;
  TimerWaitState& operator=(const TimerWaitState&) = delete;

  [[nodiscard]] bool is_elapsed() const noexcept;
  [[nodiscard]] std::chrono::steady_clock::time_point deadline() const noexcept;

  bool suspend(
      std::shared_ptr<ExecutionContext> context,
      CoroutineRef coroutine);
  void reset(std::chrono::steady_clock::time_point deadline);
  void reset_without_timer(
      std::chrono::steady_clock::time_point deadline) noexcept;
  void cancel() noexcept;
  void fire(std::uint64_t epoch) noexcept;

 private:
  mutable std::mutex mutex_;
  std::weak_ptr<RuntimeState> runtime_;
  std::chrono::steady_clock::time_point deadline_;
  std::optional<std::shared_ptr<ExecutionContext>> waiter_;
  TimerKey timer_key_;
  std::uint64_t epoch_{1};
  bool polled_{false};
  bool elapsed_{false};
  bool closed_{false};
};

}  // namespace cio::detail
