#include "cio/time.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

#include "cio/detail/execution_context.hpp"
#include "cio/detail/runtime_core.hpp"
#include "cio/detail/time_context.hpp"
#include "cio/task/yield_now.hpp"

namespace cio::detail {

std::chrono::steady_clock::time_point current_time() {
  if (active_execution_context) {
    if (const auto runtime = active_execution_context->runtime()) {
      return runtime->clock_now();
    }
  }
  return std::chrono::steady_clock::now();
}

std::shared_ptr<RuntimeState> require_time_runtime() {
  auto runtime = require_execution_context()->runtime();
  if (!runtime) {
    throw std::logic_error{"CIO runtime 已关闭"};
  }
  if (!runtime->time_enabled()) {
    throw std::logic_error{
        "CIO runtime 未启用时间驱动；请调用 Builder::enable_time"};
  }
  return runtime;
}

TimerWaitState::TimerWaitState(
    std::weak_ptr<RuntimeState> runtime,
    std::chrono::steady_clock::time_point deadline) noexcept
    : runtime_{std::move(runtime)}, deadline_{deadline} {}

TimerWaitState::~TimerWaitState() {
  cancel();
}

bool TimerWaitState::is_elapsed() const noexcept {
  std::lock_guard lock{mutex_};
  return polled_ && elapsed_;
}

std::chrono::steady_clock::time_point TimerWaitState::deadline() const noexcept {
  std::lock_guard lock{mutex_};
  return deadline_;
}

bool TimerWaitState::suspend(
    std::shared_ptr<ExecutionContext> context,
    CoroutineRef coroutine) {
  const auto runtime = runtime_.lock();
  if (!runtime) {
    throw std::logic_error{"Sleep 所属 CIO runtime 已关闭"};
  }

  std::uint64_t epoch = 0;
  std::chrono::steady_clock::time_point deadline;
  TimerKey previous_key;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      throw std::logic_error{"不能等待已经取消的 Sleep"};
    }
    polled_ = true;
    if (elapsed_) {
      return false;
    }
    if (runtime->timer_deadline_elapsed(deadline_)) {
      elapsed_ = true;
      return false;
    }

    context->park(coroutine);
    waiter_ = std::move(context);
    epoch = epoch_;
    deadline = deadline_;
    previous_key = timer_key_;
  }

  auto key = previous_key.valid()
                 ? runtime->replace_timer(
                       previous_key,
                       deadline,
                       epoch,
                       weak_from_this())
                 : runtime->register_timer(
                       deadline,
                       epoch,
                       weak_from_this());

  bool keep_key = false;
  {
    std::lock_guard lock{mutex_};
    if (!closed_ && epoch_ == epoch && !elapsed_) {
      timer_key_ = key;
      keep_key = true;
    }
  }
  if (!keep_key) {
    runtime->cancel_timer(key);
  }
  return keep_key;
}

void TimerWaitState::reset(
    std::chrono::steady_clock::time_point deadline) {
  const auto runtime = runtime_.lock();
  if (!runtime) {
    throw std::logic_error{"Sleep 所属 CIO runtime 已关闭"};
  }

  std::uint64_t epoch = 0;
  TimerKey previous_key;
  std::optional<std::shared_ptr<ExecutionContext>> waiter;
  bool elapsed_immediately = false;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      throw std::logic_error{"不能 reset 已经取消的 Sleep"};
    }
    ++epoch_;
    if (epoch_ == 0) {
      std::terminate();
    }
    deadline_ = deadline;
    elapsed_ = false;
    polled_ = true;
    epoch = epoch_;
    previous_key = timer_key_;
    timer_key_ = {};
    if (runtime->timer_deadline_elapsed(deadline_)) {
      elapsed_ = true;
      elapsed_immediately = true;
      waiter = std::move(waiter_);
      waiter_.reset();
    }
  }

  if (elapsed_immediately) {
    runtime->cancel_timer(previous_key);
    if (waiter) {
      (*waiter)->wake();
    }
    return;
  }

  auto key = previous_key.valid()
                 ? runtime->replace_timer(
                       previous_key,
                       deadline,
                       epoch,
                       weak_from_this())
                 : runtime->register_timer(
                       deadline,
                       epoch,
                       weak_from_this());

  bool keep_key = false;
  {
    std::lock_guard lock{mutex_};
    if (!closed_ && epoch_ == epoch && !elapsed_) {
      timer_key_ = key;
      keep_key = true;
    }
  }
  if (!keep_key) {
    runtime->cancel_timer(key);
  }
}

void TimerWaitState::reset_without_timer(
    std::chrono::steady_clock::time_point deadline) noexcept {
  TimerKey previous_key;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return;
    }
    ++epoch_;
    if (epoch_ == 0) {
      std::terminate();
    }
    deadline_ = deadline;
    polled_ = false;
    elapsed_ = false;
    previous_key = timer_key_;
    timer_key_ = {};
    waiter_.reset();
  }
  if (previous_key.valid()) {
    if (const auto runtime = runtime_.lock()) {
      runtime->cancel_timer(previous_key);
    }
  }
}

void TimerWaitState::cancel() noexcept {
  TimerKey key;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return;
    }
    closed_ = true;
    ++epoch_;
    if (epoch_ == 0) {
      std::terminate();
    }
    key = timer_key_;
    timer_key_ = {};
    waiter_.reset();
  }

  if (key.valid()) {
    if (const auto runtime = runtime_.lock()) {
      runtime->cancel_timer(key);
    }
  }
}

void TimerWaitState::fire(std::uint64_t epoch) noexcept {
  std::optional<std::shared_ptr<ExecutionContext>> waiter;
  {
    std::lock_guard lock{mutex_};
    if (closed_ || elapsed_ || epoch_ != epoch) {
      return;
    }
    elapsed_ = true;
    timer_key_ = {};
    waiter = std::move(waiter_);
    waiter_.reset();
  }
  if (waiter) {
    (*waiter)->wake();
  }
}

}  // namespace cio::detail

namespace cio::time {

Sleep::~Sleep() {
  if (state_) {
    state_->cancel();
  }
}

Sleep& Sleep::operator=(Sleep&& other) noexcept {
  if (this != &other) {
    if (state_) {
      state_->cancel();
    }
    state_ = std::move(other.state_);
  }
  return *this;
}

bool Sleep::await_ready() const noexcept {
  return state_ && state_->is_elapsed();
}

bool Sleep::suspend(detail::CoroutineRef coroutine) {
  if (!state_) {
    throw std::logic_error{"不能等待已移出的 Sleep"};
  }
  return state_->suspend(
      detail::require_execution_context(),
      coroutine);
}

Instant Sleep::deadline() const noexcept {
  if (!state_) {
    return Instant{};
  }
  return Instant::from_std(state_->deadline());
}

bool Sleep::is_elapsed() const noexcept {
  return state_ && state_->is_elapsed();
}

void Sleep::reset(Instant deadline) {
  if (!state_) {
    throw std::logic_error{"不能 reset 已移出的 Sleep"};
  }
  state_->reset(deadline.into_std());
}

void Sleep::reset_without_timer(Instant deadline) noexcept {
  if (state_) {
    state_->reset_without_timer(deadline.into_std());
  }
}

Sleep sleep(Duration duration) {
  const auto now = Instant::now();
  auto deadline = now.checked_add(duration);
  if (!deadline) {
    deadline = now.checked_add(std::chrono::hours{24 * 365 * 30});
  }
  return sleep_until(*deadline);
}

Sleep sleep_until(Instant deadline) {
  auto runtime = detail::require_time_runtime();
  return Sleep{std::make_shared<detail::TimerWaitState>(
      runtime,
      deadline.into_std())};
}

void pause() {
  auto runtime = detail::require_execution_context()->runtime();
  if (!runtime) {
    throw std::logic_error{"CIO runtime 已关闭"};
  }
  runtime->pause_clock();
}

void resume() {
  auto runtime = detail::require_execution_context()->runtime();
  if (!runtime) {
    throw std::logic_error{"CIO runtime 已关闭"};
  }
  runtime->resume_clock();
}

Task<void> advance(Duration duration) {
  auto runtime = detail::require_execution_context()->runtime();
  if (!runtime) {
    throw std::logic_error{"CIO runtime 已关闭"};
  }
  runtime->advance_clock(
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          duration));
  co_await task::yield_now();
}

}  // namespace cio::time
