#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cio/task/task.hpp"
#include "cio/time/instant.hpp"
#include "cio/time/sleep.hpp"

namespace cio::time {

enum class MissedTickBehavior {
  burst,
  delay,
  skip,
};

namespace detail_access {

struct SleepAccess final {
  static void reset_without_timer(
      Sleep& sleep,
      Instant deadline) noexcept {
    sleep.reset_without_timer(deadline);
  }
};

struct IntervalState final {
  IntervalState(Sleep delay_value, Duration period_value)
      : delay{std::make_shared<Sleep>(std::move(delay_value))},
        period{period_value} {}

  std::shared_ptr<Sleep> delay;
  Duration period;
  MissedTickBehavior behavior{MissedTickBehavior::burst};
  mutable std::mutex config_mutex;
  std::shared_ptr<std::atomic<bool>> tick_active{
      std::make_shared<std::atomic<bool>>(false)};
};

class TickLease final {
 public:
  explicit TickLease(
      std::shared_ptr<std::atomic<bool>> active) noexcept
      : active_{std::move(active)} {}

  TickLease(const TickLease&) = delete;
  TickLease& operator=(const TickLease&) = delete;

  ~TickLease() {
    active_->store(false, std::memory_order_release);
  }

 private:
  std::shared_ptr<std::atomic<bool>> active_;
};

inline Instant add_or_far_future(Instant base, Duration duration) {
  const auto result = base.checked_add(duration);
  if (result) {
    return *result;
  }
  return Instant::from_std(std::chrono::steady_clock::time_point::max());
}

inline Task<Instant> interval_tick(
    std::shared_ptr<IntervalState> state,
    std::shared_ptr<TickLease> lease) {
  (void)lease;
  co_await state->delay->as_awaiter();

  const auto scheduled = state->delay->deadline();
  const auto now = Instant::now();
  Duration period;
  MissedTickBehavior behavior;
  {
    std::lock_guard lock{state->config_mutex};
    period = state->period;
    behavior = state->behavior;
  }

  Instant next;
  if (now > add_or_far_future(
                scheduled,
                std::chrono::duration_cast<Duration>(
                    std::chrono::milliseconds{5}))) {
    switch (behavior) {
      case MissedTickBehavior::burst:
        next = add_or_far_future(scheduled, period);
        break;
      case MissedTickBehavior::delay:
        next = add_or_far_future(now, period);
        break;
      case MissedTickBehavior::skip: {
        const auto late_by = now - scheduled;
        const auto remainder = Duration{
            late_by.count() % period.count()};
        next = add_or_far_future(now, period - remainder);
        break;
      }
    }
  } else {
    next = add_or_far_future(scheduled, period);
  }

  SleepAccess::reset_without_timer(*state->delay, next);
  co_return scheduled;
}

}  // namespace detail_access

/**
 * 固定周期异步计时器。
 *
 * tick 返回安全拥有 IntervalState 的 Task，不在暂停边界捕获 this 或裸引用。
 * 取消未完成 tick 不消费该 tick；同一 Interval 同时只允许一个 tick Task。
 */
class [[nodiscard]] Interval final {
 public:
  Interval(const Interval&) = delete;
  Interval& operator=(const Interval&) = delete;
  Interval(Interval&&) noexcept = default;
  Interval& operator=(Interval&&) noexcept = default;
  ~Interval() = default;

  Task<Instant> tick() {
    ensure_valid();
    bool expected = false;
    if (!state_->tick_active->compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel)) {
      throw std::logic_error{"同一 Interval 不能并发等待多个 tick"};
    }

    try {
      auto lease = std::make_shared<detail_access::TickLease>(
          state_->tick_active);
      return detail_access::interval_tick(state_, std::move(lease));
    } catch (...) {
      state_->tick_active->store(false, std::memory_order_release);
      throw;
    }
  }

  void reset() {
    reset_at(add_now(period()));
  }

  void reset_immediately() {
    reset_at(Instant::now());
  }

  void reset_after(Duration after) {
    reset_at(add_now(after));
  }

  void reset_at(Instant deadline) {
    ensure_mutation_allowed();
    state_->delay->reset(deadline);
  }

  [[nodiscard]] Duration period() const {
    ensure_valid();
    std::lock_guard lock{state_->config_mutex};
    return state_->period;
  }

  [[nodiscard]] MissedTickBehavior missed_tick_behavior() const {
    ensure_valid();
    std::lock_guard lock{state_->config_mutex};
    return state_->behavior;
  }

  void set_missed_tick_behavior(MissedTickBehavior behavior) {
    ensure_valid();
    std::lock_guard lock{state_->config_mutex};
    state_->behavior = behavior;
  }

 private:
  explicit Interval(
      std::shared_ptr<detail_access::IntervalState> state) noexcept
      : state_{std::move(state)} {}

  static Instant add_now(Duration duration) {
    return detail_access::add_or_far_future(
        Instant::now(),
        duration);
  }

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"Interval 已移出"};
    }
  }

  void ensure_mutation_allowed() const {
    ensure_valid();
    if (state_->tick_active->load(std::memory_order_acquire)) {
      throw std::logic_error{"tick 等待期间不能 reset Interval"};
    }
  }

  std::shared_ptr<detail_access::IntervalState> state_;

  friend Interval interval(Duration period);
  friend Interval interval_at(Instant start, Duration period);
};

inline Interval interval_at(Instant start, Duration period) {
  if (period <= Duration::zero()) {
    throw std::invalid_argument{"Interval period 必须大于零"};
  }
  return Interval{
      std::make_shared<detail_access::IntervalState>(
          sleep_until(start),
          period)};
}

inline Interval interval(Duration period) {
  return interval_at(Instant::now(), period);
}

}  // namespace cio::time
