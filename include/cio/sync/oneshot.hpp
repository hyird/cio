#pragma once

#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <exception>
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

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/result.hpp"
#include "cio/send.hpp"
#include "cio/sync/notify.hpp"

namespace cio::sync::oneshot {

namespace error {

/**
 * Sender 未发送即析构，或 Receiver 在收到值前关闭。
 */
class RecvError final {
public:
  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "channel closed";
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return "RecvError(())";
  }

  friend constexpr bool operator==(RecvError, RecvError) noexcept = default;
};

/**
 * Receiver::try_recv 非阻塞失败原因。
 */
enum class TryRecvError {
  empty,
  closed,
};

[[nodiscard]] constexpr std::string_view message(TryRecvError error) noexcept {
  return error == TryRecvError::empty ? std::string_view{"channel empty"}
                                      : std::string_view{"channel closed"};
}

[[nodiscard]] constexpr std::string_view
debug_string(TryRecvError error) noexcept {
  return error == TryRecvError::empty ? std::string_view{"Empty"}
                                      : std::string_view{"Closed"};
}

inline std::ostream &operator<<(std::ostream &stream, RecvError error) {
  return stream << error.message();
}

inline std::ostream &operator<<(std::ostream &stream, TryRecvError error) {
  return stream << message(error);
}

} // namespace error

template <typename T> class Sender;
template <typename T> class Receiver;

template <typename T> [[nodiscard]] std::pair<Sender<T>, Receiver<T>> channel();

} // namespace cio::sync::oneshot

namespace cio::detail {

template <typename T>
concept OneshotReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::same_as<std::remove_reference_t<decltype(value.get())>,
                        typename T::type>;
};

template <typename T>
concept OneshotOwnedValue =
    std::is_object_v<T> && !std::is_pointer_v<T> &&
    !OneshotReferenceWrapperLike<std::remove_cv_t<T>>;

/**
 * Tokio oneshot 的 ready 结果属于 cooperative progress。
 *
 * 真正 pending 的 poll 不消耗预算；ready 路径每次消耗一个单位，耗尽时先把
 * 当前 CIO task 重新排队。标准 coroutine 协议强制 await_suspend 接收 ABI
 * handle，本类型只在调用点立即包装为 CoroutineRef，从不保存或暴露原始句柄。
 */
class OneshotCooperativeProgress final {
public:
  [[nodiscard]] bool allow_inline_completion() {
    context_ = active_execution_context;
    if (!context_ || context_->consume_cooperative_budget()) {
      return true;
    }
    ready_yield_required_ = true;
    debit_after_resume_ = true;
    return false;
  }

  [[nodiscard]] bool ready_yield_required() const noexcept {
    return ready_yield_required_;
  }

  template <typename Promise>
  void schedule_ready_yield(std::coroutine_handle<Promise> coroutine) const {
    context_->schedule(CoroutineRef::from_abi(coroutine));
  }

  template <typename Promise>
  [[nodiscard]] bool
  suspend_for_new_completion(std::coroutine_handle<Promise> coroutine) {
    context_ = active_execution_context;
    if (!context_ || context_->consume_cooperative_budget()) {
      return false;
    }
    debit_after_resume_ = true;
    context_->schedule(CoroutineRef::from_abi(coroutine));
    return true;
  }

  void mark_notification_suspended() {
    context_ = require_execution_context();
    debit_after_resume_ = true;
  }

  void complete_suspended_progress() noexcept {
    if (debit_after_resume_ &&
        (!context_ || !context_->consume_cooperative_budget())) {
      // 真正的通知唤醒和预算让出都会开始一次已重置预算的新 poll。
      std::terminate();
    }
    debit_after_resume_ = false;
  }

 private:
  std::shared_ptr<ExecutionContext> context_;
  bool ready_yield_required_{false};
  bool debit_after_resume_{false};
};

template <typename T> class OneshotState final {
public:
  OneshotState() = default;
  OneshotState(const OneshotState &) = delete;
  OneshotState &operator=(const OneshotState &) = delete;

  [[nodiscard]] Result<void, T> send(T value) {
    // C++ 的移动构造可能运行用户代码；先在状态锁外建立拥有候选值。
    auto candidate = std::make_shared<T>(std::move(value));
    bool accepted = false;
    {
      std::lock_guard lock{mutex_};
      if (sender_completed_) {
        throw std::logic_error{"oneshot Sender 已完成"};
      }
      if (sender_closed_wait_active_) {
        throw std::logic_error{"Sender::closed operation 存续期间不能 send"};
      }
      sender_completed_ = true;
      if (!receiver_closed_) {
        value_ = candidate;
        accepted = true;
      }
    }
    receiver_notify_.notify_waiters();
    condition_.notify_all();
    if (accepted) {
      return Result<void, T>::success();
    }
    return Result<void, T>::failure(T{std::move(*candidate)});
  }

  void sender_drop() noexcept {
    bool notify = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (!sender_completed_) {
          sender_completed_ = true;
          notify = true;
        }
      }
      if (notify) {
        receiver_notify_.notify_waiters();
        condition_.notify_all();
      }
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool sender_is_closed() const {
    std::lock_guard lock{mutex_};
    return receiver_closed_ || receiver_terminated_;
  }

  void receiver_close() {
    bool notify = false;
    {
      std::lock_guard lock{mutex_};
      if (receiver_wait_active_) {
        throw std::logic_error{
            "Receiver::receive operation 存续期间不能 close"};
      }
      if (!receiver_closed_ && !receiver_terminated_) {
        receiver_closed_ = true;
        notify = true;
      }
    }
    if (notify) {
      sender_notify_.notify_waiters();
      receiver_notify_.notify_waiters();
      condition_.notify_all();
    }
  }

  void receiver_drop() noexcept {
    std::shared_ptr<T> dropped;
    try {
      {
        std::lock_guard lock{mutex_};
        if (!receiver_terminated_) {
          receiver_closed_ = true;
          receiver_terminated_ = true;
          dropped = std::move(value_);
        }
      }
      sender_notify_.notify_waiters();
      receiver_notify_.notify_waiters();
      condition_.notify_all();
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool receiver_is_terminated() const {
    std::lock_guard lock{mutex_};
    return receiver_terminated_;
  }

  [[nodiscard]] bool receiver_is_empty() const {
    std::lock_guard lock{mutex_};
    return !value_;
  }

  [[nodiscard]] bool receiver_ready() const {
    std::lock_guard lock{mutex_};
    return receiver_ready_locked();
  }

  void begin_receive() {
    std::lock_guard lock{mutex_};
    if (receiver_terminated_) {
      throw std::logic_error{"oneshot Receiver 完成后不能再次等待"};
    }
    if (receiver_wait_active_) {
      throw std::logic_error{"oneshot Receiver 不能并发等待"};
    }
    receiver_wait_active_ = true;
  }

  void cancel_receive() noexcept {
    try {
      std::lock_guard lock{mutex_};
      receiver_wait_active_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] Result<T, sync::oneshot::error::RecvError> finish_receive() {
    std::shared_ptr<T> value;
    bool success = false;
    {
      std::lock_guard lock{mutex_};
      if (!receiver_wait_active_) {
        throw std::logic_error{"oneshot receive operation 已取消"};
      }
      if (!receiver_ready_locked()) {
        throw std::logic_error{"oneshot Receiver 在未完成时恢复"};
      }
      if (value_) {
        value = std::move(value_);
        success = true;
      }
      receiver_wait_active_ = false;
      receiver_terminated_ = true;
      receiver_closed_ = true;
    }
    sender_notify_.notify_waiters();
    condition_.notify_all();
    if (success) {
      return Result<T, sync::oneshot::error::RecvError>::success(
          T{std::move(*value)});
    }
    return Result<T, sync::oneshot::error::RecvError>::failure(
        sync::oneshot::error::RecvError{});
  }

  [[nodiscard]] Result<T, sync::oneshot::error::TryRecvError> try_receive() {
    std::shared_ptr<T> value;
    {
      std::lock_guard lock{mutex_};
      if (receiver_wait_active_) {
        throw std::logic_error{
            "Receiver::receive operation 存续期间不能 try_recv"};
      }
      if (receiver_terminated_) {
        return Result<T, sync::oneshot::error::TryRecvError>::failure(
            sync::oneshot::error::TryRecvError::closed);
      }
      if (value_) {
        value = std::move(value_);
        receiver_terminated_ = true;
        receiver_closed_ = true;
      } else if (sender_completed_ || receiver_closed_) {
        receiver_terminated_ = true;
        receiver_closed_ = true;
        return Result<T, sync::oneshot::error::TryRecvError>::failure(
            sync::oneshot::error::TryRecvError::closed);
      } else {
        return Result<T, sync::oneshot::error::TryRecvError>::failure(
            sync::oneshot::error::TryRecvError::empty);
      }
    }
    sender_notify_.notify_waiters();
    condition_.notify_all();
    return Result<T, sync::oneshot::error::TryRecvError>::success(
        T{std::move(*value)});
  }

  [[nodiscard]] Result<T, sync::oneshot::error::RecvError> blocking_receive() {
    begin_receive();
    {
      std::unique_lock lock{mutex_};
      while (!receiver_ready_locked()) {
        condition_.wait(lock);
      }
    }
    return finish_receive();
  }

  void begin_sender_closed_wait() {
    std::lock_guard lock{mutex_};
    if (sender_completed_) {
      throw std::logic_error{"oneshot Sender 完成后不能等待 Receiver close"};
    }
    if (sender_closed_wait_active_) {
      throw std::logic_error{"Sender::closed 不能并发等待"};
    }
    sender_closed_wait_active_ = true;
  }

  void cancel_sender_closed_wait() noexcept {
    try {
      std::lock_guard lock{mutex_};
      sender_closed_wait_active_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool sender_closed_ready() const {
    std::lock_guard lock{mutex_};
    return receiver_closed_ || receiver_terminated_;
  }

  void finish_sender_closed_wait() {
    std::lock_guard lock{mutex_};
    if (!sender_closed_wait_active_) {
      throw std::logic_error{"Sender::closed operation 已取消"};
    }
    if (!receiver_closed_ && !receiver_terminated_) {
      throw std::logic_error{"Sender::closed 在 Receiver 仍打开时恢复"};
    }
    sender_closed_wait_active_ = false;
  }

  [[nodiscard]] std::string sender_debug_string() const {
    std::lock_guard lock{mutex_};
    std::ostringstream stream;
    stream << "Sender { closed: "
           << static_cast<int>(receiver_closed_ || receiver_terminated_)
           << " }";
    return stream.str();
  }

  [[nodiscard]] std::string receiver_debug_string() const {
    std::lock_guard lock{mutex_};
    std::ostringstream stream;
    stream << "Receiver { terminated: "
           << static_cast<int>(receiver_terminated_)
           << ", empty: " << static_cast<int>(!value_) << " }";
    return stream.str();
  }

  sync::Notify receiver_notify_;
  sync::Notify sender_notify_;

private:
  [[nodiscard]] bool receiver_ready_locked() const noexcept {
    return static_cast<bool>(value_) || sender_completed_ || receiver_closed_;
  }

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::shared_ptr<T> value_;
  bool sender_completed_{false};
  bool receiver_closed_{false};
  bool receiver_terminated_{false};
  bool receiver_wait_active_{false};
  bool sender_closed_wait_active_{false};
};

template <typename T> class OneshotReceiveLease final {
public:
  OneshotReceiveLease(std::shared_ptr<OneshotState<T>> state,
                      sync::Notify::Notified notified) noexcept
      : state_{std::move(state)}, notified_{std::move(notified)} {}

  OneshotReceiveLease(const OneshotReceiveLease &) = delete;
  OneshotReceiveLease &operator=(const OneshotReceiveLease &) = delete;

  ~OneshotReceiveLease() {
    if (armed_) {
      state_->cancel_receive();
    }
  }

  void arm() noexcept { armed_ = true; }

  [[nodiscard]] Result<T, sync::oneshot::error::RecvError> complete() {
    auto result = state_->finish_receive();
    armed_ = false;
    return result;
  }

  std::shared_ptr<OneshotState<T>> state_;
  sync::Notify::Notified notified_;
  bool armed_{false};
};

template <typename T> class OneshotClosedLease final {
public:
  OneshotClosedLease(std::shared_ptr<OneshotState<T>> state,
                     sync::Notify::Notified notified) noexcept
      : state_{std::move(state)}, notified_{std::move(notified)} {}

  OneshotClosedLease(const OneshotClosedLease &) = delete;
  OneshotClosedLease &operator=(const OneshotClosedLease &) = delete;

  ~OneshotClosedLease() {
    if (armed_) {
      state_->cancel_sender_closed_wait();
    }
  }

  void arm() noexcept { armed_ = true; }

  void complete() {
    state_->finish_sender_closed_wait();
    armed_ = false;
  }

  std::shared_ptr<OneshotState<T>> state_;
  sync::Notify::Notified notified_;
  bool armed_{false};
};

} // namespace cio::detail

namespace cio::sync::oneshot {

/**
 * 单值 channel 的唯一发送端。
 *
 * Sender 只能移动；`send` 消费发送能力，析构未发送的 Sender 会让 Receiver
 * 观察 `RecvError`。当 `T` 满足 CIO Send 时，Sender 同时满足 CIO Send/Sync：
 * 共享状态始终在 mutex 下访问，不会向其他线程借出 `T`。
 */
template <typename T> class Sender final {
public:
  static_assert(detail::OneshotOwnedValue<T>,
                "oneshot<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  Sender(const Sender &) = delete;
  Sender &operator=(const Sender &) = delete;

  Sender(Sender &&other) noexcept : state_{std::move(other.state_)} {}

  Sender &operator=(Sender &&other) noexcept {
    if (this != &other) {
      reset();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~Sender() { reset(); }

  /**
   * 同步发送唯一值并消费 Sender。
   *
   * 发送不等待、不要求 runtime，可跨线程和跨 runtime 调用。Receiver 已关闭
   * 时错误分支返还值；成功只表示发送线性化时 Receiver 尚未关闭，不保证最终
   * 消费。方法只接受右值，所有权不会以引用跨越异步边界。
   */
  [[nodiscard]] Result<void, T> send(T value) && {
    ensure_valid();
    auto state = state_;
    auto result = state->send(std::move(value));
    state_.reset();
    return result;
  }

  /**
   * 判断 Receiver 是否已经显式关闭、完成接收或析构。
   */
  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return state_->sender_is_closed();
  }

  class Closed final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Closed(const Closed &) = delete;
    Closed &operator=(const Closed &) = delete;
    Closed(Closed &&) noexcept = default;
    Closed &operator=(Closed &&) noexcept = default;
    ~Closed() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Closed::cio_send;
      static constexpr bool cio_sync = false;

      explicit Awaiter(
          std::shared_ptr<detail::OneshotClosedLease<T>> lease) noexcept
          : lease_{std::move(lease)},
            notified_{lease_->notified_.operator co_await()} {}

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        if (phase_ != Phase::created) {
          throw std::logic_error{"Sender::Closed awaiter 被重复 poll"};
        }
        if (lease_->state_->sender_closed_ready()) {
          skip_notification_ = true;
          phase_ = Phase::ready_checked;
          ready_result_ = cooperative_.allow_inline_completion();
          return ready_result_;
        }
        const bool notified = notified_.await_ready();
        if (lease_->state_->sender_closed_ready()) {
          skip_notification_ = !notified;
          phase_ = Phase::ready_checked;
          ready_result_ = cooperative_.allow_inline_completion();
          return ready_result_;
        }
        ready_result_ =
            notified && cooperative_.allow_inline_completion();
        phase_ = Phase::ready_checked;
        return ready_result_;
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        if (phase_ != Phase::ready_checked || ready_result_) {
          throw std::logic_error{"Sender::Closed awaiter suspend 状态无效"};
        }
        phase_ = Phase::suspended;
        if (cooperative_.ready_yield_required()) {
          cooperative_.schedule_ready_yield(coroutine);
          return true;
        }
        if (lease_->state_->sender_closed_ready()) {
          skip_notification_ = true;
          return cooperative_.suspend_for_new_completion(coroutine);
        }
        const bool suspended = notified_.await_suspend(coroutine);
        if (!suspended) {
          return cooperative_.suspend_for_new_completion(coroutine);
        }
        cooperative_.mark_notification_suspended();
        return true;
      }

      void await_resume() {
        ensure_valid();
        if (phase_ != Phase::ready_checked && phase_ != Phase::suspended) {
          throw std::logic_error{"Sender::Closed awaiter resume 状态无效"};
        }
        phase_ = Phase::resumed;
        cooperative_.complete_suspended_progress();
        if (!skip_notification_) {
          notified_.await_resume();
        }
        lease_->complete();
      }

    private:
      enum class Phase {
        created,
        ready_checked,
        suspended,
        resumed,
      };

      void ensure_valid() const {
        if (!lease_) {
          throw std::logic_error{"Sender::Closed awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::OneshotClosedLease<T>> lease_;
      Notify::Notified::Awaiter notified_;
      detail::OneshotCooperativeProgress cooperative_;
      bool skip_notification_{false};
      bool ready_result_{false};
      Phase phase_{Phase::created};
    };

    [[nodiscard]] Awaiter operator co_await() & { return take_awaiter(); }

    [[nodiscard]] Awaiter operator co_await() && { return take_awaiter(); }

  private:
    [[nodiscard]] Awaiter take_awaiter() {
      ensure_valid();
      return Awaiter{std::move(lease_)};
    }

    explicit Closed(
        std::shared_ptr<detail::OneshotClosedLease<T>> lease) noexcept
        : lease_{std::move(lease)} {}

    void ensure_valid() const {
      if (!lease_) {
        throw std::logic_error{"Sender::Closed 已移出"};
      }
    }

    std::shared_ptr<detail::OneshotClosedLease<T>> lease_;

    friend class Sender;
  };

  /**
   * 等待 Receiver 显式关闭或析构。
   *
   * operation 拥有共享状态，不借用 Sender；取消只注销 waiter，Sender 随后
   * 可以重新等待或发送。等待不阻塞 worker；`T` 满足 Send 时可在线程间迁移。
   * wake-before-wait 与 close-during-register 由创建通知后双重检查覆盖。
   */
  [[nodiscard]] Closed closed() {
    ensure_valid();
    auto notified = state_->sender_notify_.notified_owned();
    auto lease = std::make_shared<detail::OneshotClosedLease<T>>(
        state_, std::move(notified));
    state_->begin_sender_closed_wait();
    lease->arm();
    return Closed{std::move(lease)};
  }

  /**
   * `poll_closed(Context)` 的安全 C++20 能力映射。
   *
   * C++ coroutine 不公开 Rust Context/Waker；本方法返回同一 owned awaitable，
   * 其 awaiter 完成注册、重新检查和最新 waiter 替换。
   */
  [[nodiscard]] Closed poll_closed() { return closed(); }

  /**
   * 返回不暴露内部地址的诊断快照。
   */
  [[nodiscard]] std::string debug_string() const {
    ensure_valid();
    return state_->sender_debug_string();
  }

private:
  explicit Sender(std::shared_ptr<detail::OneshotState<T>> state) noexcept
      : state_{std::move(state)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"oneshot Sender 已移出或已发送"};
    }
  }

  void reset() noexcept {
    if (state_) {
      state_->sender_drop();
      state_.reset();
    }
  }

  std::shared_ptr<detail::OneshotState<T>> state_;

  template <typename U> friend std::pair<Sender<U>, Receiver<U>> channel();
};

/**
 * 单值 channel 的唯一接收端。
 *
 * Receiver 只能移动；析构会关闭接收端并丢弃尚未接收的值。`receive` operation
 * 用 owning state 表达 Rust `&mut Receiver` 的异步借用，并以运行时独占检查
 * 拒绝 C++ 无法静态排除的并发操作。
 */
template <typename T> class Receiver final {
public:
  static_assert(detail::OneshotOwnedValue<T>,
                "oneshot<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  Receiver(const Receiver &) = delete;
  Receiver &operator=(const Receiver &) = delete;

  Receiver(Receiver &&other) noexcept : state_{std::move(other.state_)} {}

  Receiver &operator=(Receiver &&other) noexcept {
    if (this != &other) {
      reset();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~Receiver() { reset(); }

  /**
   * 禁止后续 send，但保留调用前已经发布的值。
   *
   * close 幂等且不阻塞；仅 close 不会把 Receiver 标记为 terminated，直到
   * receive/try_recv 实际观察值或关闭错误。active receive 存续时调用会因
   * 对应 Rust 独占借用冲突而抛出 logic_error。
   */
  void close() {
    ensure_valid();
    state_->receiver_close();
  }

  /**
   * 判断 Receiver 是否已经交付值或关闭错误。
   */
  [[nodiscard]] bool is_terminated() const {
    ensure_valid();
    return state_->receiver_is_terminated();
  }

  /**
   * 判断 channel 当前是否没有待接收值。
   *
   * Sender 已析构但关闭错误尚未被观察时仍返回 true。
   */
  [[nodiscard]] bool is_empty() const {
    ensure_valid();
    return state_->receiver_is_empty();
  }

  /**
   * 非阻塞接收，不注册异步 waiter。
   *
   * `Empty` 不终止 Receiver；成功或 `Closed` 会终止。与 active receive 并发
   * 调用会抛出 logic_error，替代 Rust `&mut Receiver` 的静态独占检查。若
   * `T` 的移动构造抛异常，异常直接传播且 channel 保持终态，避免重复交付。
   */
  [[nodiscard]] Result<T, error::TryRecvError> try_recv() {
    ensure_valid();
    return state_->try_receive();
  }

  class Receive final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Receive(const Receive &) = delete;
    Receive &operator=(const Receive &) = delete;
    Receive(Receive &&) noexcept = default;
    Receive &operator=(Receive &&) noexcept = default;
    ~Receive() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Receive::cio_send;
      static constexpr bool cio_sync = false;

      explicit Awaiter(
          std::shared_ptr<detail::OneshotReceiveLease<T>> lease) noexcept
          : lease_{std::move(lease)},
            notified_{lease_->notified_.operator co_await()} {}

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        if (phase_ != Phase::created) {
          throw std::logic_error{"Receiver::Receive awaiter 被重复 poll"};
        }
        if (lease_->state_->receiver_ready()) {
          skip_notification_ = true;
          phase_ = Phase::ready_checked;
          ready_result_ = cooperative_.allow_inline_completion();
          return ready_result_;
        }
        const bool notified = notified_.await_ready();
        if (lease_->state_->receiver_ready()) {
          skip_notification_ = !notified;
          phase_ = Phase::ready_checked;
          ready_result_ = cooperative_.allow_inline_completion();
          return ready_result_;
        }
        ready_result_ =
            notified && cooperative_.allow_inline_completion();
        phase_ = Phase::ready_checked;
        return ready_result_;
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        if (phase_ != Phase::ready_checked || ready_result_) {
          throw std::logic_error{"Receiver::Receive awaiter suspend 状态无效"};
        }
        phase_ = Phase::suspended;
        if (cooperative_.ready_yield_required()) {
          cooperative_.schedule_ready_yield(coroutine);
          return true;
        }
        if (lease_->state_->receiver_ready()) {
          skip_notification_ = true;
          return cooperative_.suspend_for_new_completion(coroutine);
        }
        const bool suspended = notified_.await_suspend(coroutine);
        if (!suspended) {
          return cooperative_.suspend_for_new_completion(coroutine);
        }
        cooperative_.mark_notification_suspended();
        return true;
      }

      [[nodiscard]] Result<T, error::RecvError> await_resume() {
        ensure_valid();
        if (phase_ != Phase::ready_checked && phase_ != Phase::suspended) {
          throw std::logic_error{"Receiver::Receive awaiter resume 状态无效"};
        }
        phase_ = Phase::resumed;
        cooperative_.complete_suspended_progress();
        if (!skip_notification_) {
          notified_.await_resume();
        }
        return lease_->complete();
      }

    private:
      enum class Phase {
        created,
        ready_checked,
        suspended,
        resumed,
      };

      void ensure_valid() const {
        if (!lease_) {
          throw std::logic_error{"Receiver::Receive awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::OneshotReceiveLease<T>> lease_;
      Notify::Notified::Awaiter notified_;
      detail::OneshotCooperativeProgress cooperative_;
      bool skip_notification_{false};
      bool ready_result_{false};
      Phase phase_{Phase::created};
    };

    [[nodiscard]] Awaiter operator co_await() & { return take_awaiter(); }

    [[nodiscard]] Awaiter operator co_await() && { return take_awaiter(); }

  private:
    [[nodiscard]] Awaiter take_awaiter() {
      ensure_valid();
      return Awaiter{std::move(lease_)};
    }

    explicit Receive(
        std::shared_ptr<detail::OneshotReceiveLease<T>> lease) noexcept
        : lease_{std::move(lease)} {}

    void ensure_valid() const {
      if (!lease_) {
        throw std::logic_error{"Receiver::Receive 已移出"};
      }
    }

    std::shared_ptr<detail::OneshotReceiveLease<T>> lease_;

    friend class Receiver;
  };

  /**
   * 等待单值或 Sender 无值关闭。
   *
   * await operation 拥有共享状态，不保存 Receiver 引用；值只在
   * `await_resume` 中移出。任一暂停边界取消只注销 waiter，不消费消息也不终止
   * Receiver，之后可重新 receive/try_recv。等待不阻塞 worker；`T: Send` 时
   * operation 可跨 worker 迁移。并发 receive 由运行时独占检查拒绝。若最终
   * 移出 `T` 时抛异常，异常由 task 边界捕获且 channel 保持终态。
   */
  [[nodiscard]] Receive receive() {
    ensure_valid();
    auto notified = state_->receiver_notify_.notified_owned();
    auto lease = std::make_shared<detail::OneshotReceiveLease<T>>(
        state_, std::move(notified));
    state_->begin_receive();
    lease->arm();
    return Receive{std::move(lease)};
  }

  [[nodiscard]] typename Receive::Awaiter operator co_await() & {
    return receive().operator co_await();
  }

  /**
   * 在普通同步线程阻塞接收。
   *
   * 消耗 Receiver 的接收能力并阻塞调用线程；CIO 异步执行上下文中调用会抛出
   * logic_error。无裸引用跨越等待，跨线程发布由状态 mutex/condition 保证。
   * `T` 移动构造抛出的异常原样传播，channel 保持终态。
   */
  [[nodiscard]] Result<T, error::RecvError> blocking_recv() && {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{
          "Receiver::blocking_recv 不能在 CIO 异步执行上下文中调用"};
    }
    auto result = state_->blocking_receive();
    state_.reset();
    return result;
  }

  /**
   * 返回不暴露内部地址的诊断快照。
   */
  [[nodiscard]] std::string debug_string() const {
    ensure_valid();
    return state_->receiver_debug_string();
  }

private:
  explicit Receiver(std::shared_ptr<detail::OneshotState<T>> state) noexcept
      : state_{std::move(state)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"oneshot Receiver 已移出"};
    }
  }

  void reset() noexcept {
    if (state_) {
      state_->receiver_drop();
      state_.reset();
    }
  }

  std::shared_ptr<detail::OneshotState<T>> state_;

  template <typename U> friend std::pair<Sender<U>, Receiver<U>> channel();
};

/**
 * 创建容量固定为一个值的单生产者、单消费者 channel。
 *
 * 两端拥有同一内部状态；工厂不要求 runtime，也不暴露 allocator 或原生地址。
 */
template <typename T> std::pair<Sender<T>, Receiver<T>> channel() {
  auto state = std::make_shared<detail::OneshotState<T>>();
  return {Sender<T>{state}, Receiver<T>{state}};
}

template <typename T>
std::ostream &operator<<(std::ostream &stream, const Sender<T> &sender) {
  return stream << sender.debug_string();
}

template <typename T>
std::ostream &operator<<(std::ostream &stream, const Receiver<T> &receiver) {
  return stream << receiver.debug_string();
}

} // namespace cio::sync::oneshot

namespace cio {

template <>
struct send_traits<sync::oneshot::error::RecvError> : std::true_type {};

template <>
struct sync_traits<sync::oneshot::error::RecvError> : std::true_type {};

template <>
struct send_traits<sync::oneshot::error::TryRecvError> : std::true_type {};

template <>
struct sync_traits<sync::oneshot::error::TryRecvError> : std::true_type {};

template <typename T>
struct send_traits<sync::oneshot::Sender<T>>
    : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::oneshot::Sender<T>>
    : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct send_traits<sync::oneshot::Receiver<T>>
    : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::oneshot::Receiver<T>>
    : send_traits<std::remove_cv_t<T>> {};

} // namespace cio
