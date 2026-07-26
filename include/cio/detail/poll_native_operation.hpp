#pragma once

#include <concepts>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/detail/composed_poll.hpp"
#include "cio/send.hpp"

namespace cio::detail {

template <typename T>
concept PollNativeReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::same_as<
      std::remove_reference_t<decltype(value.get())>,
      typename T::type>;
};

template <typename T>
struct PollNativeBorrowedView : std::false_type {};

template <typename Element, std::size_t Extent>
struct PollNativeBorrowedView<std::span<Element, Extent>>
    : std::true_type {};

template <typename Character, typename Traits>
struct PollNativeBorrowedView<
    std::basic_string_view<Character, Traits>>
    : std::true_type {};

template <typename T>
consteval bool audited_poll_native_owned_value() {
  using Value = std::remove_cv_t<T>;
  if constexpr (std::is_arithmetic_v<Value> ||
                std::is_enum_v<Value>) {
    return true;
  } else if constexpr (requires {
                         typename std::bool_constant<
                             static_cast<bool>(
                                 Value::
                                     cio_poll_native_owned_value)>;
                       }) {
    return static_cast<bool>(
        Value::cio_poll_native_owned_value);
  } else {
    return false;
  }
}

/**
 * 可以按值跨越 Pending 边界的已审计 owning 类型。
 *
 * 标量天然按值拥有；其他类型必须显式声明
 * `cio_poll_native_owned_value=true`。即使错误 opt-in，span、string_view 和
 * reference_wrapper 仍被拒绝，防止最常见的借用视图进入异步状态机。
 */
template <typename T>
concept PollNativeOwnedValue =
    std::is_object_v<T> &&
    (!std::is_reference_v<T>) &&
    (!std::is_pointer_v<std::remove_cv_t<T>>) &&
    (!PollNativeReferenceWrapperLike<std::remove_cv_t<T>>) &&
    (!PollNativeBorrowedView<std::remove_cv_t<T>>::value) &&
    cio::Send<T> &&
    std::is_nothrow_move_constructible_v<T> &&
    audited_poll_native_owned_value<T>();

template <typename T>
concept PollNativeOutput =
    std::is_void_v<T> || PollNativeOwnedValue<T>;

/**
 * 可由组合 operation 直接 poll 的普通状态机契约。
 *
 * operation 不是 Task，也不得保存或恢复已暂停的 coroutine。每次 poll 都按值
 * 接收最新输入和当前 lane 的 wake token；返回 Pending 后，parent 既可以等待
 * token 唤醒，也可以因其他 lane 的进展主动带着新输入再次 poll。
 *
 * cancel_now 必须同步完成逻辑注销且不抛异常。平台后端若仍有不可同步撤销的内核
 * 操作，应由自身的 owning operation record 持有资源并过滤迟到 completion。
 */
template <typename Operation, typename Input, typename Output>
concept PollNativeOperation =
    std::move_constructible<Operation> &&
    std::is_nothrow_move_constructible_v<Operation> &&
    (!std::copy_constructible<Operation>) &&
    PollNativeOwnedValue<Input> &&
    PollNativeOutput<Output> &&
    requires {
      {
        std::remove_cvref_t<Operation>::cio_poll_native_operation
      } -> std::convertible_to<bool>;
    } &&
    static_cast<bool>(
        std::remove_cvref_t<Operation>::cio_poll_native_operation) &&
    requires(
        Operation operation,
        Input input,
        const LaneWakeToken& wake_token) {
      {
        operation.poll(
            std::move(input),
            wake_token)
      } -> std::same_as<LanePoll<Output>>;
      {
        operation.cancel_now()
      } noexcept -> std::same_as<void>;
    };

/**
 * 一个 generation-safe 的 poll-native operation lane。
 *
 * 与 TaskPollLane 不同，本类型没有 resumable coroutine，也不把 lane 通知当作
 * poll 权限。调用方每次调用 poll_once 都会直接 poll 普通状态机，因此 writer
 * Pending 后可以由 reader 进展触发 parent，并用扩展后的 owning input 主动
 * rearm writer。
 *
 * 所有权：lane 独占 operation；input 按值移交给本次 poll，operation 若要跨
 * Pending 保存输入，必须取得其 owning 状态。生命周期：Ready、异常或取消先使
 * generation 失效，再销毁 operation。invalidate 后才开始的旧 token wake 无
 * 副作用；已经并发进入 wake 的调用最多产生一次 spurious parent poll，但不能
 * 复活或再次 poll 已终结 operation。
 */
template <typename Operation, typename Input, typename Output>
  requires PollNativeOperation<Operation, Input, Output>
class PollNativeOperationLane final {
 public:
  PollNativeOperationLane(
      Operation operation,
      const ComposedWakeGate& gate)
      : operation_{std::move(operation)},
        registration_{gate.register_lane()},
        wake_token_{registration_.activate()} {}

  PollNativeOperationLane(const PollNativeOperationLane&) = delete;
  PollNativeOperationLane& operator=(
      const PollNativeOperationLane&) = delete;

  PollNativeOperationLane(
      PollNativeOperationLane&& other) noexcept(
          std::is_nothrow_move_constructible_v<Operation>)
      : operation_{std::move(other.operation_)},
        registration_{std::move(other.registration_)},
        wake_token_{std::move(other.wake_token_)} {
    // optional move 后源仍 engaged；源对象必须立即失去 operation 所有权，避免
    // 析构时对已经转移的状态机执行第二次取消。
    other.operation_.reset();
  }

  PollNativeOperationLane& operator=(
      PollNativeOperationLane&&) = delete;

  ~PollNativeOperationLane() {
    cancel_now();
  }

  [[nodiscard]] bool active() const noexcept {
    return operation_.has_value();
  }

  [[nodiscard]] LaneKey key() const noexcept {
    return wake_token_.key();
  }

  /**
   * 使用最新 owning input poll 一次普通状态机。
   *
   * 本函数故意不读取 lane 的 notified 位。Pending 后的再次 poll 可以由本 lane
   * wake、其他 lane wake 或 parent 已有同步进展触发；这正是可 rearm 输入与
   * Tokio poll-native AsyncWrite 语义所需的区别。
   */
  [[nodiscard]] LanePoll<Output> poll_once(Input input) {
    ensure_active();
    try {
      auto result = operation_->poll(
          std::move(input),
          std::as_const(wake_token_));
      if (result.is_ready()) {
        finish();
      }
      return result;
    } catch (...) {
      cancel_now();
      throw;
    }
  }

  /**
   * 同步失效 lane 并取消普通状态机。
   *
   * 先 invalidate 再调用 operation.cancel_now，保证同步取消 callback 内新发起
   * 的 wake 不能发布父 gate sequence。已经并发通过 active 检查的 wake 最多
   * 造成一次 spurious parent poll，但 generation 不允许它复活 operation。
   */
  void cancel_now() noexcept {
    if (!operation_) {
      return;
    }
    registration_.invalidate();
    operation_->cancel_now();
    operation_.reset();
  }

 private:
  void ensure_active() const {
    if (!operation_) {
      throw std::logic_error{
          "poll-native operation lane 已完成、取消或移动"};
    }
  }

  void finish() noexcept {
    registration_.invalidate();
    operation_.reset();
  }

  std::optional<Operation> operation_;
  LaneRegistration registration_;
  LaneWakeToken wake_token_;
};

}  // namespace cio::detail
