#pragma once

#include <compare>
#include <coroutine>
#include <atomic>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/task/task.hpp"

namespace cio::detail {

class ComposedWakeGate;
template <typename T>
class TaskPollLane;

/**
 * 组合 operation 中一个 lane 的 generation key。
 *
 * key 只用于拒绝已经取消或被复用 lane 的迟到 wake，不表达任何对象所有权。
 */
class LaneKey final {
 public:
  constexpr LaneKey() noexcept = default;

  [[nodiscard]] constexpr bool valid() const noexcept {
    return slot_ != 0 && generation_ != 0;
  }

  auto operator<=>(const LaneKey&) const noexcept = default;

 private:
  constexpr LaneKey(
      std::uint64_t slot,
      std::uint64_t generation) noexcept
      : slot_{slot},
        generation_{generation} {}

  std::uint64_t slot_{0};
  std::uint64_t generation_{0};

  friend class ComposedWakeGate;
  friend class LaneRegistration;
  friend class LaneWakeToken;
};

namespace composed_poll_detail {

struct WakeGateState final {
  std::atomic<std::uint64_t> sequence{0};
  std::atomic<std::uint64_t> next_slot{0};
  std::weak_ptr<ExecutionContext> parent_context;
  // 0=未绑定，1=正在发布 weak context，2=已绑定且只读。
  std::atomic<std::uint8_t> parent_bind_state{0};
};

struct LaneRegistrationState final {
  explicit LaneRegistrationState(
      std::weak_ptr<WakeGateState> wake_gate,
      std::uint64_t lane_slot) noexcept
      : gate{std::move(wake_gate)},
        slot{lane_slot} {}

  std::weak_ptr<WakeGateState> gate;
  const std::uint64_t slot{0};
  std::atomic<std::uint64_t> generation{0};
  std::atomic<bool> active{false};
  std::atomic<bool> notified{false};
};

inline std::uint64_t advance_nonzero(
    std::atomic<std::uint64_t>& value,
    std::string_view exhausted_message) {
  auto current = value.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error{std::string{exhausted_message}};
    }
    const auto next = current + 1;
    if (value.compare_exchange_weak(
            current,
            next,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return next;
    }
  }
}

inline void bind_parent(
    const std::shared_ptr<WakeGateState>& state,
    const std::shared_ptr<ExecutionContext>& context) {
  std::uint8_t expected = 0;
  if (state->parent_bind_state.compare_exchange_strong(
          expected,
          static_cast<std::uint8_t>(1),
          std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    // 先写 weak_ptr，再以 release 发布状态 2；wake 只有 acquire 读到 2 后
    // 才会读取 weak_ptr。
    state->parent_context = context;
    state->parent_bind_state.store(
        static_cast<std::uint8_t>(2),
        std::memory_order_release);
    return;
  }
  if (expected == 1) {
    throw std::logic_error{"ComposedWakeGate 正在被另一个 task 绑定"};
  }
  if (state->parent_context.lock() != context) {
    throw std::logic_error{"ComposedWakeGate 不能跨 task 共享等待权"};
  }
}

inline void signal(const std::shared_ptr<WakeGateState>& state) noexcept {
  auto current = state->sequence.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    if (state->sequence.compare_exchange_weak(
            current,
            current + 1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
  if (state->parent_bind_state.load(std::memory_order_acquire) == 2) {
    if (const auto context = state->parent_context.lock()) {
      context->wake();
    }
  }
}

struct LaneResumeState final {
  // 只有拥有组合 operation 的父 task 会读写 resumable。lane wake 只触发父
  // ExecutionContext；runtime 的 release/acquire 调度边建立跨 worker 可见性。
  CoroutineRef resumable;
};

}  // namespace composed_poll_detail

/**
 * 可复制的 lane wake 权限。
 *
 * token 只弱拥有 registration 和 gate。registration 失效后才开始的旧 wake
 * 是无副作用操作；与 invalidate 并发且已通过 generation 校验的 wake 可以
 * 线性化在取消前，并造成一次无害的父 task 多余 poll，但不能恢复已销毁 lane。
 */
class LaneWakeToken final {
 public:
  LaneWakeToken() noexcept = default;

  [[nodiscard]] LaneKey key() const noexcept {
    return key_;
  }

  void wake() const noexcept {
    const auto registration = registration_.lock();
    if (!registration || !key_.valid()) {
      return;
    }

    if (!registration->active.load(std::memory_order_acquire) ||
        registration->slot != key_.slot_ ||
        registration->generation.load(std::memory_order_acquire) !=
            key_.generation_) {
      return;
    }
    // 先发布 lane 自己的可恢复权，再发布父 gate sequence。父 task 因其他
    // lane 被唤醒时，不得误恢复本 lane 尚未获通知的 child coroutine。
    registration->notified.store(true, std::memory_order_release);
    const auto gate = registration->gate.lock();
    if (!gate) {
      return;
    }
    composed_poll_detail::signal(gate);
  }

 private:
  LaneWakeToken(
      std::weak_ptr<composed_poll_detail::LaneRegistrationState>
          registration,
      LaneKey key) noexcept
      : registration_{std::move(registration)},
        key_{key} {}

  std::weak_ptr<composed_poll_detail::LaneRegistrationState> registration_;
  LaneKey key_;

  friend class LaneRegistration;
};

/**
 * gate 中一个单代 lane 槽位的唯一 registration。
 *
 * registration 只允许 activate 一次；invalidate 在销毁 Task frame 前阻止此后
 * 才开始的 wake。已经并发进入 wake 的调用允许产生一次多余父 poll，但不会
 * 获得 lane 恢复权。新 primitive 必须创建新 registration，避免旧 wake 与同一
 * state 的新 generation 交错。
 */
class LaneRegistration final {
 public:
  LaneRegistration() noexcept = default;
  LaneRegistration(const LaneRegistration&) = delete;
  LaneRegistration& operator=(const LaneRegistration&) = delete;
  LaneRegistration(LaneRegistration&&) noexcept = default;
  LaneRegistration& operator=(LaneRegistration&&) = delete;

  ~LaneRegistration() {
    invalidate();
  }

  [[nodiscard]] LaneWakeToken activate() {
    ensure_valid();
    state_->active.store(false, std::memory_order_release);
    state_->notified.store(false, std::memory_order_release);
    std::uint64_t expected = 0;
    if (!state_->generation.compare_exchange_strong(
            expected,
            1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      throw std::logic_error{
          "组合 operation lane registration 不能重复 activate"};
    }
    state_->active.store(true, std::memory_order_release);
    return LaneWakeToken{
        state_,
        LaneKey{state_->slot, 1}};
  }

  void invalidate() noexcept {
    if (!state_) {
      return;
    }
    state_->active.store(false, std::memory_order_release);
    state_->notified.store(false, std::memory_order_release);
  }

 private:
  explicit LaneRegistration(
      std::shared_ptr<composed_poll_detail::LaneRegistrationState>
          state) noexcept
      : state_{std::move(state)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"组合 operation lane registration 已移动"};
    }
  }

  std::shared_ptr<composed_poll_detail::LaneRegistrationState> state_;

  friend class ComposedWakeGate;
  template <typename>
  friend class TaskPollLane;
};

/**
 * 多 lane 组合 operation 的防丢唤醒 gate。
 *
 * 调用方在 poll 所有 lane 前读取 sequence，全部返回 Pending 后再 wait_after。
 * wait_after 先向父 ExecutionContext park，再以 acquire 语义重查 sequence，覆盖
 * wake-before-park、park-before-wake 和多个 lane 同时 wake。
 */
class ComposedWakeGate final {
 public:
  ComposedWakeGate()
      : state_{std::make_shared<composed_poll_detail::WakeGateState>()} {}

  [[nodiscard]] std::uint64_t sequence() const noexcept {
    return state_->sequence.load(std::memory_order_acquire);
  }

  [[nodiscard]] LaneRegistration register_lane() const {
    const auto slot = composed_poll_detail::advance_nonzero(
        state_->next_slot,
        "组合 operation lane slot 已耗尽");
    return LaneRegistration{
        std::make_shared<composed_poll_detail::LaneRegistrationState>(
            state_,
            slot)};
  }

  class WaitAfter final {
   public:
    WaitAfter(
        std::shared_ptr<composed_poll_detail::WakeGateState> state,
        std::uint64_t observed_sequence) noexcept
        : state_{std::move(state)},
          observed_sequence_{observed_sequence} {}

    [[nodiscard]] bool await_ready() const noexcept {
      // 即使 wake 已提前发生也必须经过一次 scheduler poll 边界；未来 copy
      // 只能在 fresh poll 获取新的 cooperative progress ticket。
      return false;
    }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> continuation) {
      const auto context = require_execution_context();
      composed_poll_detail::bind_parent(state_, context);
      context->park(CoroutineRef::from_abi(continuation));
      // park 发布后再次检查 sequence。提前 wake 没有看到 parent context 时，
      // 由这里补一次调度；并发 wake 可能造成重复通知，但 runtime task 状态机会
      // 合并通知，且本 awaiter 始终真正挂起到下一次 fresh poll。
      if (state_->sequence.load(std::memory_order_acquire) !=
          observed_sequence_) {
        context->wake();
      }
      return true;
    }

    void await_resume() const noexcept {}

   private:
    std::shared_ptr<composed_poll_detail::WakeGateState> state_;
    std::uint64_t observed_sequence_{0};
  };

  [[nodiscard]] WaitAfter wait_after(
      std::uint64_t observed_sequence) const noexcept {
    return WaitAfter{state_, observed_sequence};
  }

 private:
  std::shared_ptr<composed_poll_detail::WakeGateState> state_;
};

template <typename T>
class LanePoll final {
 public:
  [[nodiscard]] static LanePoll pending() noexcept {
    return LanePoll{};
  }

  [[nodiscard]] static LanePoll ready(T value) {
    LanePoll result;
    result.value_.emplace(std::move(value));
    return result;
  }

  [[nodiscard]] bool is_ready() const noexcept {
    return value_.has_value();
  }

  T&& take_value() && {
    if (!value_) {
      throw std::logic_error{"Pending lane 没有完成值"};
    }
    return std::move(*value_);
  }

 private:
  std::optional<T> value_;
};

template <>
class LanePoll<void> final {
 public:
  [[nodiscard]] static LanePoll pending() noexcept {
    return LanePoll{false};
  }

  [[nodiscard]] static LanePoll ready() noexcept {
    return LanePoll{true};
  }

  [[nodiscard]] bool is_ready() const noexcept {
    return ready_;
  }

  void take_value() const {
    if (!ready_) {
      throw std::logic_error{"Pending lane 没有完成值"};
    }
  }

 private:
  explicit LanePoll(bool ready) noexcept
      : ready_{ready} {}

  bool ready_{false};
};

/**
 * 一个由当前组合 task 独占 poll 的 Task frame lane。
 *
 * lane 不 spawn、不 detach。每个 lane 创建独立调度上下文：park 只保存本 lane
 * 的 resumable coroutine，wake 只递增父 gate sequence；但 cooperative budget
 * 与父 task 共享，确保组合操作及其子 primitive 属于同一次 poll 预算。
 * cancel_now 先使 token generation 失效，再同步销毁 Task frame，使 waiter 和
 * I/O lease 在返回前完成逻辑注销。
 */
template <typename T>
class TaskPollLane final {
 public:
  explicit TaskPollLane(Task<T> task, const ComposedWakeGate& gate)
      : awaiter_{std::move(task).operator co_await()},
        registration_{gate.register_lane()},
        wake_token_{registration_.activate()},
        resume_state_{
            std::make_shared<composed_poll_detail::LaneResumeState>()} {}

  TaskPollLane(const TaskPollLane&) = delete;
  TaskPollLane& operator=(const TaskPollLane&) = delete;
  TaskPollLane(TaskPollLane&& other) noexcept
      : awaiter_{std::move(other.awaiter_)},
        registration_{std::move(other.registration_)},
        wake_token_{std::move(other.wake_token_)},
        resume_state_{std::move(other.resume_state_)},
        lane_context_{std::move(other.lane_context_)},
        started_{other.started_},
        completed_{other.completed_} {
    // optional 的 move 不会自动 disengage 源对象；显式清空可防止源析构再次
    // 取消已经转移的 owning frame。
    other.awaiter_.reset();
    other.started_ = false;
    other.completed_ = true;
  }
  TaskPollLane& operator=(TaskPollLane&&) = delete;

  ~TaskPollLane() {
    cancel_now();
  }

  [[nodiscard]] bool active() const noexcept {
    return awaiter_.has_value() && !completed_;
  }

  [[nodiscard]] LaneKey key() const noexcept {
    return wake_token_.key();
  }

  [[nodiscard]] LanePoll<T> poll_once() {
    ensure_active();
    ensure_context();

    CoroutineRef resumable;
    if (!started_) {
      ScopedExecutionContext scope{lane_context_};
      // noop continuation 只作为标准协程 ABI 边界；Task frame 仍由 Awaiter 独占。
      resumable = CoroutineRef::from_abi(
          awaiter_->await_suspend(std::noop_coroutine()));
      started_ = true;
    } else {
      if (!take_notification()) {
        return LanePoll<T>::pending();
      }
      resumable = std::exchange(
          resume_state_->resumable,
          CoroutineRef{});
    }

    if (resumable.valid()) {
      ScopedExecutionContext scope{lane_context_};
      resumable.resume();
    }

    if (!awaiter_->await_ready()) {
      return LanePoll<T>::pending();
    }

    try {
      if constexpr (std::is_void_v<T>) {
        awaiter_->await_resume();
        finish();
        return LanePoll<void>::ready();
      } else {
        auto value = awaiter_->await_resume();
        finish();
        return LanePoll<T>::ready(std::move(value));
      }
    } catch (...) {
      cancel_now();
      throw;
    }
  }

  void cancel_now() noexcept {
    if (!awaiter_) {
      return;
    }
    registration_.invalidate();
    lane_context_.reset();
    awaiter_.reset();
    if (resume_state_) {
      resume_state_->resumable = {};
    }
    completed_ = true;
  }

 private:
  using Awaiter = typename Task<T>::Awaiter;

  void ensure_active() const {
    if (!active()) {
      throw std::logic_error{"TaskPollLane 已完成或取消"};
    }
  }

  void ensure_context() {
    if (lane_context_) {
      return;
    }
    const auto parent_context = require_execution_context();
    const auto registration_state = registration_.state_;
    if (!registration_state) {
      throw std::logic_error{"TaskPollLane registration 已移动"};
    }
    const auto gate = registration_state->gate.lock();
    if (!gate) {
      throw std::logic_error{"TaskPollLane wake gate 已销毁"};
    }
    composed_poll_detail::bind_parent(gate, parent_context);
    const auto weak_resume =
        std::weak_ptr<composed_poll_detail::LaneResumeState>{
            resume_state_};
    const auto token = wake_token_;
    auto cooperative_budget_owner =
        parent_context->cooperative_budget_owner();
    if (!cooperative_budget_owner) {
      cooperative_budget_owner = parent_context;
    }
    lane_context_ = std::make_shared<ExecutionContext>(
        parent_context->runtime(),
        parent_context->task_key(),
        [weak_resume](CoroutineRef coroutine) {
          if (const auto state = weak_resume.lock()) {
            state->resumable = coroutine;
          }
        },
        [token] {
          token.wake();
        },
        std::move(cooperative_budget_owner));
  }

  [[nodiscard]] bool take_notification() noexcept {
    const auto registration_state = registration_.state_;
    return registration_state &&
        registration_state->notified.exchange(
            false,
            std::memory_order_acq_rel);
  }

  void finish() noexcept {
    registration_.invalidate();
    lane_context_.reset();
    awaiter_.reset();
    if (resume_state_) {
      resume_state_->resumable = {};
    }
    completed_ = true;
  }

  std::optional<Awaiter> awaiter_;
  LaneRegistration registration_;
  LaneWakeToken wake_token_;
  std::shared_ptr<composed_poll_detail::LaneResumeState> resume_state_;
  std::shared_ptr<ExecutionContext> lane_context_;
  bool started_{false};
  bool completed_{false};
};

}  // namespace cio::detail
