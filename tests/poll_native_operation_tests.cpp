#include <array>
#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "cio/detail/poll_native_operation.hpp"

namespace {

using cio::detail::ComposedWakeGate;
using cio::detail::LanePoll;
using cio::detail::LaneWakeToken;
using cio::detail::PollNativeOperation;
using cio::detail::PollNativeOperationLane;
using cio::detail::PollNativeOwnedValue;

struct WriteArm final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_poll_native_owned_value = true;

  std::size_t revision{0};
  std::size_t size{0};
};

struct ThrowingMoveValue final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_poll_native_owned_value = true;

  ThrowingMoveValue() noexcept = default;
  ThrowingMoveValue(const ThrowingMoveValue&) = delete;
  ThrowingMoveValue& operator=(const ThrowingMoveValue&) = delete;
  ThrowingMoveValue(ThrowingMoveValue&&) noexcept(false) {}
  ThrowingMoveValue& operator=(ThrowingMoveValue&&) = delete;
};

struct OperationState final {
  std::size_t polls{0};
  std::size_t cancellations{0};
  std::size_t destructions{0};
  std::size_t last_revision{0};
  std::size_t last_size{0};
  LaneWakeToken wake_token;
};

class RearmableWriteOperation final {
 public:
  static constexpr bool cio_poll_native_operation = true;

  explicit RearmableWriteOperation(
      std::shared_ptr<OperationState> state) noexcept
      : state_{std::move(state)} {}

  RearmableWriteOperation(const RearmableWriteOperation&) = delete;
  RearmableWriteOperation& operator=(
      const RearmableWriteOperation&) = delete;
  RearmableWriteOperation(
      RearmableWriteOperation&& other) noexcept
      : state_{std::move(other.state_)} {}
  RearmableWriteOperation& operator=(
      RearmableWriteOperation&&) = delete;

  ~RearmableWriteOperation() {
    if (state_) {
      ++state_->destructions;
      // Ready finish 必须在析构 owning operation 前使 token 失效。
      state_->wake_token.wake();
    }
  }

  [[nodiscard]] LanePoll<std::size_t> poll(
      WriteArm arm,
      const LaneWakeToken& wake_token) {
    ++state_->polls;
    state_->last_revision = arm.revision;
    state_->last_size = arm.size;
    state_->wake_token = wake_token;
    if (arm.revision < 2) {
      return LanePoll<std::size_t>::pending();
    }
    return LanePoll<std::size_t>::ready(arm.size);
  }

  void cancel_now() noexcept {
    ++state_->cancellations;
    // lane 必须在进入 operation 取消逻辑前使 token 失效。
    state_->wake_token.wake();
  }

 private:
  std::shared_ptr<OperationState> state_;
};

class ThrowingOperation final {
 public:
  static constexpr bool cio_poll_native_operation = true;

  explicit ThrowingOperation(
      std::shared_ptr<OperationState> state) noexcept
      : state_{std::move(state)} {}

  ThrowingOperation(const ThrowingOperation&) = delete;
  ThrowingOperation& operator=(const ThrowingOperation&) = delete;
  ThrowingOperation(ThrowingOperation&& other) noexcept
      : state_{std::move(other.state_)} {}
  ThrowingOperation& operator=(ThrowingOperation&&) = delete;

  [[nodiscard]] LanePoll<std::size_t> poll(
      WriteArm,
      const LaneWakeToken& wake_token) {
    state_->wake_token = wake_token;
    throw std::runtime_error{"poll failure"};
  }

  void cancel_now() noexcept {
    ++state_->cancellations;
    // 异常路径必须先 invalidate，再进入同步 cancel callback。
    state_->wake_token.wake();
  }

 private:
  std::shared_ptr<OperationState> state_;
};

class ThrowingMoveOperation final {
 public:
  static constexpr bool cio_poll_native_operation = true;

  ThrowingMoveOperation() noexcept = default;
  ThrowingMoveOperation(const ThrowingMoveOperation&) = delete;
  ThrowingMoveOperation& operator=(
      const ThrowingMoveOperation&) = delete;
  ThrowingMoveOperation(ThrowingMoveOperation&&) noexcept(false) {}
  ThrowingMoveOperation& operator=(
      ThrowingMoveOperation&&) = delete;

  [[nodiscard]] LanePoll<std::size_t> poll(
      WriteArm,
      const LaneWakeToken&) {
    return LanePoll<std::size_t>::pending();
  }

  void cancel_now() noexcept {}
};

class RvalueOnlyTokenOperation final {
 public:
  static constexpr bool cio_poll_native_operation = true;

  RvalueOnlyTokenOperation() noexcept = default;
  RvalueOnlyTokenOperation(const RvalueOnlyTokenOperation&) = delete;
  RvalueOnlyTokenOperation& operator=(
      const RvalueOnlyTokenOperation&) = delete;
  RvalueOnlyTokenOperation(
      RvalueOnlyTokenOperation&&) noexcept = default;
  RvalueOnlyTokenOperation& operator=(
      RvalueOnlyTokenOperation&&) = delete;

  [[nodiscard]] LanePoll<std::size_t> poll(
      WriteArm,
      LaneWakeToken&&) {
    return LanePoll<std::size_t>::pending();
  }

  void cancel_now() noexcept {}
};

template <typename Input, typename Output>
class ShapeOperation final {
 public:
  static constexpr bool cio_poll_native_operation = true;

  ShapeOperation() noexcept = default;
  ShapeOperation(const ShapeOperation&) = delete;
  ShapeOperation& operator=(const ShapeOperation&) = delete;
  ShapeOperation(ShapeOperation&&) noexcept = default;
  ShapeOperation& operator=(ShapeOperation&&) = delete;

  [[nodiscard]] LanePoll<Output> poll(
      Input,
      const LaneWakeToken&) {
    return LanePoll<Output>::pending();
  }

  void cancel_now() noexcept {}
};

static_assert(PollNativeOperation<
              RearmableWriteOperation,
              WriteArm,
              std::size_t>);
static_assert(PollNativeOwnedValue<WriteArm>);
static_assert(!PollNativeOwnedValue<WriteArm&>);
static_assert(!PollNativeOwnedValue<const WriteArm&>);
static_assert(!PollNativeOwnedValue<ThrowingMoveValue>);
static_assert(!PollNativeOperation<
              ThrowingMoveOperation,
              WriteArm,
              std::size_t>);
static_assert(!PollNativeOperation<
              RvalueOnlyTokenOperation,
              WriteArm,
              std::size_t>);
static_assert(!PollNativeOwnedValue<std::span<const std::byte>>);
static_assert(!PollNativeOwnedValue<std::string_view>);
static_assert(!PollNativeOwnedValue<std::reference_wrapper<int>>);
static_assert(!PollNativeOperation<
              ShapeOperation<
                  std::span<const std::byte>,
                  std::size_t>,
              std::span<const std::byte>,
              std::size_t>);
static_assert(!PollNativeOperation<
              ShapeOperation<WriteArm, std::string_view>,
              WriteArm,
              std::string_view>);
static_assert(!PollNativeOperation<
              ShapeOperation<
                  std::reference_wrapper<int>,
                  std::size_t>,
              std::reference_wrapper<int>,
              std::size_t>);
static_assert(!std::copy_constructible<
              PollNativeOperationLane<
                  RearmableWriteOperation,
                  WriteArm,
                  std::size_t>>);

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

void parent_can_rearm_without_notification() {
  ComposedWakeGate gate;
  auto state = std::make_shared<OperationState>();
  PollNativeOperationLane<
      RearmableWriteOperation,
      WriteArm,
      std::size_t>
      lane{RearmableWriteOperation{state}, gate};

  const auto observed = gate.sequence();
  auto first = lane.poll_once(WriteArm{1, 3});
  check(!first.is_ready(), "首次 write arm 应返回 Pending");
  check(gate.sequence() == observed,
        "未 wake 时 gate sequence 不应改变");

  // 没有调用保存的 token；parent 因其他状态进展，主动用扩展输入 rearm。
  auto second = lane.poll_once(WriteArm{2, 8});
  check(second.is_ready(), "主动 rearm 后应允许完成");
  check(std::move(second).take_value() == 8,
        "完成值必须来自最新 arm");
  check(state->polls == 2 &&
            state->last_revision == 2 &&
            state->last_size == 8,
        "普通状态机必须观察 parent 主动提交的最新输入");
  check(!lane.active(), "Ready 后 lane 必须进入终态");
  check(state->destructions == 1,
        "Ready 后 owning operation 必须恰好析构一次");
  check(gate.sequence() == observed,
        "Ready 析构 callback 内 wake 必须已被 generation 屏蔽");

  state->wake_token.wake();
  check(gate.sequence() == observed,
        "Ready 后的迟到 wake 必须失效");
}

void peer_wake_does_not_gate_active_poll() {
  ComposedWakeGate gate;
  auto first_state = std::make_shared<OperationState>();
  auto second_state = std::make_shared<OperationState>();
  PollNativeOperationLane<
      RearmableWriteOperation,
      WriteArm,
      std::size_t>
      first{RearmableWriteOperation{first_state}, gate};
  PollNativeOperationLane<
      RearmableWriteOperation,
      WriteArm,
      std::size_t>
      second{RearmableWriteOperation{second_state}, gate};

  check(!first.poll_once(WriteArm{1, 2}).is_ready(),
        "first 应先 Pending");
  check(!second.poll_once(WriteArm{1, 4}).is_ready(),
        "second 应先 Pending");

  const auto observed = gate.sequence();
  first_state->wake_token.wake();
  check(gate.sequence() == observed + 1,
        "first wake 必须发布父 gate sequence");

  // second 没有收到自己的通知，但 poll-native 状态机仍可由 parent 主动推进。
  auto result = second.poll_once(WriteArm{2, 9});
  check(result.is_ready() &&
            std::move(result).take_value() == 9,
        "peer wake 后 parent 必须能主动 poll 未通知的状态机");
  check(second_state->polls == 2,
        "未通知 lane 不得被 poll gate 拦截");
}

void cancel_invalidates_before_callback() {
  ComposedWakeGate gate;
  auto state = std::make_shared<OperationState>();
  PollNativeOperationLane<
      RearmableWriteOperation,
      WriteArm,
      std::size_t>
      lane{RearmableWriteOperation{state}, gate};
  check(!lane.poll_once(WriteArm{1, 5}).is_ready(),
        "取消测试必须先进入 Pending");

  const auto observed = gate.sequence();
  lane.cancel_now();
  check(!lane.active(), "cancel_now 后 lane 必须失效");
  check(state->cancellations == 1,
        "operation 必须恰好取消一次");
  check(state->destructions == 1,
        "取消后 owning operation 必须恰好析构一次");
  check(gate.sequence() == observed,
        "取消 callback 内的 wake 必须已被 generation 屏蔽");

  lane.cancel_now();
  check(state->cancellations == 1,
        "重复 cancel_now 必须幂等");
}

void move_transfers_pending_state() {
  ComposedWakeGate gate;
  auto state = std::make_shared<OperationState>();
  using Lane = PollNativeOperationLane<
      RearmableWriteOperation,
      WriteArm,
      std::size_t>;
  Lane source{RearmableWriteOperation{state}, gate};
  check(!source.poll_once(WriteArm{1, 6}).is_ready(),
        "移动测试必须先进入 Pending");

  Lane destination{std::move(source)};
  check(!source.active() && destination.active(),
        "move 必须唯一转移 operation 所有权");
  auto result = destination.poll_once(WriteArm{2, 11});
  check(result.is_ready() &&
            std::move(result).take_value() == 11,
        "移动后的 lane 必须支持未通知主动 poll");
  check(state->cancellations == 0,
        "移动和 Ready 路径都不应触发取消");
}

void exception_cancels_and_invalidates() {
  ComposedWakeGate gate;
  auto state = std::make_shared<OperationState>();
  PollNativeOperationLane<
      ThrowingOperation,
      WriteArm,
      std::size_t>
      lane{ThrowingOperation{state}, gate};

  bool observed_exception = false;
  const auto observed_sequence = gate.sequence();
  try {
    (void)lane.poll_once(WriteArm{1, 1});
  } catch (const std::runtime_error& error) {
    observed_exception =
        std::string_view{error.what()} == "poll failure";
  }
  check(observed_exception, "poll 异常必须原样传播");
  check(!lane.active() && state->cancellations == 1,
        "poll 异常必须同步取消并终结 lane");
  check(gate.sequence() == observed_sequence,
        "异常 cancel callback 内 wake 必须已被 generation 屏蔽");

  const auto observed = gate.sequence();
  state->wake_token.wake();
  check(gate.sequence() == observed,
        "异常清理后的迟到 wake 必须失效");
}

}  // namespace

int main() {
  try {
    parent_can_rearm_without_notification();
    peer_wake_does_not_gate_active_poll();
    cancel_invalidates_before_callback();
    move_transfers_pending_state();
    exception_cancels_and_invalidates();
  } catch (const std::exception& error) {
    std::cerr << "[FAIL] poll-native operation: "
              << error.what() << '\n';
    return 1;
  }

  std::cout << "poll-native operation tests passed: 5/5\n";
  return 0;
}
