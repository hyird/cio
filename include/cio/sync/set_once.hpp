#pragma once

#include <concepts>
#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/result.hpp"
#include "cio/send.hpp"
#include "cio/sync/notify.hpp"
#include "cio/task/task.hpp"

namespace cio::sync {

template <typename T> class SetOnce;

/**
 * SetOnce::set 重复写入错误。
 *
 * 错误对象保留未写入值的所有权；`message` 对齐 Tokio Display，
 * `debug_string` 提供 C++ Debug 能力。
 */
template <typename T> class SetOnceError final {
public:
  explicit SetOnceError(T value) : value_{std::move(value)} {}

  SetOnceError(const SetOnceError &) = default;
  SetOnceError &operator=(const SetOnceError &) = default;
  SetOnceError(SetOnceError &&) noexcept(
      std::is_nothrow_move_constructible_v<T>) = default;
  SetOnceError &operator=(SetOnceError &&) noexcept(
      std::is_nothrow_move_assignable_v<T>) = default;
  ~SetOnceError() = default;

  [[nodiscard]] const T &value() const & noexcept { return value_; }
  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] T into_value() && { return T{std::move(value_)}; }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "SetOnceError";
  }

  [[nodiscard]] std::string debug_string() const
    requires requires(std::ostream &stream, const T &value) {
      { stream << value } -> std::same_as<std::ostream &>;
    }
  {
    std::ostringstream stream;
    stream << "SetOnceError(" << value_ << ')';
    return stream.str();
  }

  friend bool operator==(const SetOnceError &, const SetOnceError &)
    requires std::equality_comparable<T>
  = default;

private:
  T value_;
};

template <typename T>
std::ostream &operator<<(std::ostream &stream, const SetOnceError<T> &error) {
  return stream << error.message();
}

} // namespace cio::sync

namespace cio::detail {

template <typename T> class SetOnceState final {
public:
  SetOnceState() = default;

  explicit SetOnceState(T value)
      : value_{std::make_shared<T>(std::move(value))} {}

  SetOnceState(const SetOnceState &) = delete;
  SetOnceState &operator=(const SetOnceState &) = delete;

  [[nodiscard]] bool initialized() const {
    std::lock_guard lock{mutex_};
    return static_cast<bool>(value_);
  }

  [[nodiscard]] std::shared_ptr<const T> snapshot() const {
    std::lock_guard lock{mutex_};
    return value_;
  }

  [[nodiscard]] Result<void, sync::SetOnceError<T>> set(T value) {
    // C++ 的 T 移动构造可能执行用户代码；先在临界区外建立候选值。
    auto candidate = std::make_shared<T>(std::move(value));
    bool published = false;
    {
      std::lock_guard lock{mutex_};
      if (!value_) {
        value_ = candidate;
        published = true;
      }
    }
    if (!published) {
      return Result<void, sync::SetOnceError<T>>::failure(
          sync::SetOnceError<T>{T{std::move(*candidate)}});
    }
    // 发布值的互斥锁释放发生在通知之前，所有被唤醒 waiter 都能观察完整值。
    notify_.notify_waiters();
    return Result<void, sync::SetOnceError<T>>::success();
  }

  [[nodiscard]] std::optional<T> take() {
    std::shared_ptr<T> taken;
    {
      std::lock_guard lock{mutex_};
      if (!value_) {
        return std::nullopt;
      }
      if constexpr (!std::copy_constructible<T>) {
        if (value_.use_count() != 1) {
          throw std::logic_error{
              "SetOnce::into_inner 的非复制值仍有 owning snapshot"};
        }
      }
      taken = std::exchange(value_, {});
    }

    if constexpr (std::copy_constructible<T>) {
      if (taken.use_count() == 1) {
        return std::optional<T>{std::in_place, std::move(*taken)};
      }
      // C++ owning snapshot 可比 cell
      // 活得更久；在锁外复制避免移动它观察的对象。
      return std::optional<T>{std::in_place, *taken};
    } else {
      // 锁内已证明不存在 snapshot，exchange 后也不会再产生新的旧值 snapshot。
      return std::optional<T>{std::in_place, std::move(*taken)};
    }
  }

  sync::Notify notify_;

private:
  mutable std::mutex mutex_;
  std::shared_ptr<T> value_;
};

template <typename T>
concept SetOnceDebugWritable = requires(std::ostream &stream, const T &value) {
  { stream << value } -> std::same_as<std::ostream &>;
};

} // namespace cio::detail

namespace cio::sync {

/**
 * Tokio 1.53.1 风格的线程安全单次设置事件。
 *
 * CIO 用 `shared_ptr<const T>` owning snapshot 替代 Rust 借用引用，保证返回值
 * 不因 cell 移动或析构悬空。复制执行 Tokio `Clone` 的深复制语义；多个 task
 * 共享同一事件时应像 Rust `Arc<SetOnce<T>>` 一样共享 owning handle。
 */
template <typename T> class SetOnce final {
public:
  SetOnce() : state_{std::make_shared<detail::SetOnceState<T>>()} {}

  SetOnce(const SetOnce &other)
    requires std::copy_constructible<T>
      : SetOnce{other.snapshot_copy()} {}

  SetOnce(const SetOnce &)
    requires(!std::copy_constructible<T>)
  = delete;

  SetOnce &operator=(const SetOnce &other)
    requires std::copy_constructible<T>
  {
    if (this != &other) {
      auto replacement = SetOnce{other.snapshot_copy()};
      state_ = std::move(replacement.state_);
    }
    return *this;
  }

  SetOnce &operator=(const SetOnce &)
    requires(!std::copy_constructible<T>)
  = delete;

  SetOnce(SetOnce &&) noexcept = default;
  SetOnce &operator=(SetOnce &&) noexcept = default;
  ~SetOnce() = default;

  /**
   * C++20 运行期 `const_new` 兼容工厂。
   *
   * mutex 与共享控制块不能常量求值，因此不具备 Rust 静态初始化能力。
   */
  [[nodiscard]] static SetOnce const_new() { return SetOnce{}; }

  [[nodiscard]] static SetOnce new_with(std::optional<T> value) {
    return SetOnce{std::move(value)};
  }

  /**
   * C++20 运行期 `const_new_with` 兼容工厂。
   */
  [[nodiscard]] static SetOnce const_new_with(T value) {
    return SetOnce{std::move(value)};
  }

  [[nodiscard]] static SetOnce from(T value) {
    return SetOnce{std::move(value)};
  }

  [[nodiscard]] bool initialized() const {
    ensure_valid();
    return state_->initialized();
  }

  /**
   * 返回不可变 owning snapshot；空指针表示尚未设置。
   */
  [[nodiscard]] std::shared_ptr<const T> get() const {
    ensure_valid();
    return state_->snapshot();
  }

  [[nodiscard]] Result<void, SetOnceError<T>> set(T value) const {
    ensure_valid();
    // 与 Tokio 一样先走已发布快路径，避免确定失败时仍分配候选控制块或执行
    // 额外的 T 移动构造。SetOnceState::set 在锁内再次检查并处理并发 winner。
    if (state_->initialized()) {
      return Result<void, SetOnceError<T>>::failure(
          SetOnceError<T>{std::move(value)});
    }
    return state_->set(std::move(value));
  }

  class Wait final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value &&
                                     sync_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Wait(const Wait &) = delete;
    Wait &operator=(const Wait &) = delete;
    Wait(Wait &&) noexcept = default;
    Wait &operator=(Wait &&) noexcept = default;
    ~Wait() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Wait::cio_send;
      static constexpr bool cio_sync = false;

      Awaiter(std::shared_ptr<detail::SetOnceState<T>> state,
              Notify::Notified::Awaiter notified) noexcept
          : state_{std::move(state)}, notified_{std::move(notified)} {}

      Awaiter(const Awaiter &) = delete;
      Awaiter &operator=(const Awaiter &) = delete;
      Awaiter(Awaiter &&) noexcept = default;
      Awaiter &operator=(Awaiter &&) noexcept = default;
      ~Awaiter() = default;

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        if (phase_ != Phase::created) {
          throw std::logic_error{"SetOnce Wait awaiter 被重复 poll"};
        }
        if (state_->snapshot()) {
          value_ready_ = true;
          ready_result_ = true;
          phase_ = Phase::ready_checked;
          return true;
        }
        ready_result_ = notified_.await_ready();
        phase_ = Phase::ready_checked;
        return ready_result_;
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        if (phase_ != Phase::ready_checked || ready_result_) {
          throw std::logic_error{"SetOnce Wait awaiter suspend 状态无效"};
        }
        phase_ = Phase::suspended;
        if (state_->snapshot()) {
          value_ready_ = true;
          return false;
        }
        return notified_.await_suspend(coroutine);
      }

      [[nodiscard]] std::shared_ptr<const T> await_resume() {
        ensure_valid();
        if (phase_ != Phase::ready_checked && phase_ != Phase::suspended) {
          throw std::logic_error{"SetOnce Wait awaiter resume 状态无效"};
        }
        phase_ = Phase::resumed;
        if (!value_ready_) {
          notified_.await_resume();
        }
        auto value = state_->snapshot();
        if (!value) {
          throw std::logic_error{"SetOnce waiter 收到通知但值尚未发布"};
        }
        return value;
      }

    private:
      enum class Phase {
        created,
        ready_checked,
        suspended,
        resumed,
      };

      void ensure_valid() const {
        if (!state_) {
          throw std::logic_error{"SetOnce Wait awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::SetOnceState<T>> state_;
      Notify::Notified::Awaiter notified_;
      bool value_ready_{false};
      bool ready_result_{false};
      Phase phase_{Phase::created};
    };

    [[nodiscard]] Awaiter operator co_await() & { return take_awaiter(); }

    [[nodiscard]] Awaiter operator co_await() && { return take_awaiter(); }

  private:
    [[nodiscard]] Awaiter take_awaiter() {
      ensure_valid();
      auto state = std::move(state_);
      auto notified = std::move(notified_);
      return Awaiter{std::move(state), notified.operator co_await()};
    }

    Wait(std::shared_ptr<detail::SetOnceState<T>> state,
         Notify::Notified notified) noexcept
        : state_{std::move(state)}, notified_{std::move(notified)} {}

    void ensure_valid() const {
      if (!state_) {
        throw std::logic_error{"SetOnce Wait 已移出"};
      }
    }

    std::shared_ptr<detail::SetOnceState<T>> state_;
    Notify::Notified notified_;

    friend class SetOnce;
  };

  /**
   * 等待首次成功设置并返回 owning snapshot。
   *
   * 操作拥有共享 state，不借用 SetOnce 对象；返回 snapshot 独立保持值的生命
   * 周期。取消只注销当前 Notify waiter，不消费事件、不影响其他 waiter，随后
   * 可安全重试。等待不阻塞 runtime worker，portable task 可在 worker 间迁移。
   * set 发布前后的双重检查保证 wake-before-wait、wait-before-wake 均不丢失。
   */
  [[nodiscard]] Wait wait() const {
    ensure_valid();
    return Wait{state_, state_->notify_.notified_owned()};
  }

  /**
   * 消耗唯一 cell 状态并移出值。
   *
   * 若仍有运行中的 wait 操作则拒绝；copyable 值允许既有 owning snapshot
   * 继续观察旧值，non-copyable 值有 snapshot 时拒绝移动。
   */
  [[nodiscard]] std::optional<T> into_inner() && {
    ensure_valid();
    if (state_.use_count() != 1) {
      throw std::logic_error{"SetOnce::into_inner 要求没有运行中的 wait 操作"};
    }
    auto state = std::move(state_);
    return state->take();
  }

  [[nodiscard]] SetOnce clone() const
    requires std::copy_constructible<T>
  {
    return SetOnce{*this};
  }

  [[nodiscard]] std::string debug_string() const
    requires detail::SetOnceDebugWritable<T>
  {
    std::ostringstream stream;
    stream << "SetOnce { value: ";
    if (auto value = get()) {
      stream << "Some(" << *value << ')';
    } else {
      stream << "None";
    }
    stream << " }";
    return stream.str();
  }

  friend bool operator==(const SetOnce &left, const SetOnce &right)
    requires std::equality_comparable<T>
  {
    const auto left_value = left.get();
    const auto right_value = right.get();
    if (static_cast<bool>(left_value) != static_cast<bool>(right_value)) {
      return false;
    }
    return !left_value || *left_value == *right_value;
  }

private:
  explicit SetOnce(std::optional<T> value)
      : state_{
            value ? std::make_shared<detail::SetOnceState<T>>(std::move(*value))
                  : std::make_shared<detail::SetOnceState<T>>()} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"SetOnce 已移出"};
    }
  }

  [[nodiscard]] std::optional<T> snapshot_copy() const
    requires std::copy_constructible<T>
  {
    if (auto value = get()) {
      return std::optional<T>{*value};
    }
    return std::nullopt;
  }

  std::shared_ptr<detail::SetOnceState<T>> state_;
};

template <typename T>
std::ostream &operator<<(std::ostream &stream, const SetOnce<T> &set_once)
  requires detail::SetOnceDebugWritable<T>
{
  return stream << set_once.debug_string();
}

} // namespace cio::sync

namespace cio {

template <typename T>
struct send_traits<sync::SetOnce<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::SetOnce<T>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value> {};

template <typename T>
struct send_traits<sync::SetOnceError<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::SetOnceError<T>> : sync_traits<std::remove_cv_t<T>> {};

} // namespace cio
