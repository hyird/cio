#pragma once

#include <concepts>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/result.hpp"
#include "cio/send.hpp"
#include "cio/sync/notify.hpp"
#include "cio/task/consume_budget.hpp"
#include "cio/task/task.hpp"

namespace cio::sync::watch {

namespace error {

/**
 * watch 发送失败；错误拥有未发布的值。
 */
template <typename T> class SendError final {
public:
  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = sync_traits<std::remove_cv_t<T>>::value;

  explicit SendError(T value) : value_{std::move(value)} {}

  /** 返回错误中仍由本对象拥有的值；引用不得超过 SendError 生命周期。 */
  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] const T &value() const & noexcept { return value_; }
  [[nodiscard]] T &&value() && = delete;
  [[nodiscard]] const T &value() const && = delete;

  /** 移出失败发送的值；按值返回避免从临时错误对象泄漏悬空引用。 */
  [[nodiscard]] T
  into_inner() && noexcept(std::is_nothrow_move_constructible_v<T>) {
    return T{std::move(value_)};
  }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "channel closed";
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return "SendError { .. }";
  }

private:
  T value_;
};

/**
 * 所有 Sender 已析构，且 Receiver 没有尚未观察的新版本。
 */
class RecvError final {
public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "channel closed";
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return "RecvError(())";
  }

  friend constexpr bool operator==(RecvError, RecvError) noexcept = default;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &stream,
                               const SendError<T> &error) {
  return stream << error.message();
}

inline std::ostream &operator<<(std::ostream &stream, RecvError error) {
  return stream << error.message();
}

} // namespace error

/**
 * watch 当前值的拥有快照。
 *
 * Tokio 的 `Ref<'_, T>` 借用会持有读锁，不能安全跨越 await。CIO 快照改为共享
 * 拥有一个不可变版本，不持锁且可跨线程、跨暂停点保存。`value()` 返回的引用只
 * 在 Snapshot owner 存续期间有效；禁止从临时 Snapshot 取引用。
 */
template <typename T> class Snapshot final {
public:
  static constexpr bool cio_send =
      send_traits<std::remove_cv_t<T>>::value &&
      sync_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

  Snapshot(std::shared_ptr<const T> value, bool changed) noexcept
      : value_{std::move(value)}, changed_{changed} {}

  /**
   * 借用拥有快照中的值。
   *
   * 只允许 lvalue Snapshot；引用不得超过 Snapshot 生命周期，也不得单独捕获进
   * 可能超过当前同步作用域的异步工作。
   */
  [[nodiscard]] const T &value() const & {
    ensure_valid();
    return *value_;
  }

  /** 从临时 Snapshot 复制出值；不可复制 T 没有该重载。 */
  [[nodiscard]] T value() && requires std::copy_constructible<T> {
    ensure_valid();
    return T{*value_};
  }

  [[nodiscard]] const T &value() const && = delete;

  /** 返回该快照相对 Receiver 原游标是否来自未读版本。 */
  [[nodiscard]] bool has_changed() const noexcept { return changed_; }

private:
  void ensure_valid() const {
    if (!value_) {
      throw std::logic_error{"watch Snapshot 已移出"};
    }
  }

  std::shared_ptr<const T> value_;
  bool changed_{false};
};

template <typename T> class Sender;
template <typename T> class Receiver;

template <typename T>
[[nodiscard]] std::pair<Sender<T>, Receiver<T>> channel(T initial);

} // namespace cio::sync::watch

namespace cio::detail {

template <typename T>
concept WatchReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::same_as<std::remove_reference_t<decltype(value.get())>,
                        typename T::type>;
};

template <typename T>
concept WatchOwnedValue =
    std::is_object_v<T> && !std::is_pointer_v<T> &&
    !WatchReferenceWrapperLike<std::remove_cv_t<T>>;

enum class WatchChangeStatus {
  pending,
  changed,
  closed,
};

/**
 * 逻辑句柄计数不得回绕成零，否则最后端点判断会被伪造。
 *
 * Tokio 的逻辑计数受 Arc 引用计数上限保护；CIO 在独立逻辑计数处显式拒绝
 * 溢出。调用失败不会改变原计数。
 */
inline void watch_checked_increment(std::size_t &count,
                                    std::string_view kind) {
  if (count == std::numeric_limits<std::size_t>::max()) {
    throw std::length_error{std::string{kind}};
  }
  ++count;
}

/**
 * 把 Notify 的真实暂停状态返回给组合 operation。
 *
 * 标准 coroutine ABI 强制 await_suspend 接收原生 handle；这里只立即转发给
 * Notify 的审核包装层，不保存、返回或把地址放入 channel 状态。
 */
class WatchNotification final {
public:
  explicit WatchNotification(sync::Notify::Notified notified) noexcept
      : notified_{std::move(notified)} {}

  WatchNotification(const WatchNotification &) = delete;
  WatchNotification &operator=(const WatchNotification &) = delete;
  WatchNotification(WatchNotification &&) noexcept = default;
  WatchNotification &operator=(WatchNotification &&) noexcept = default;

  class Awaiter final {
  public:
    explicit Awaiter(sync::Notify::Notified::Awaiter awaiter) noexcept
        : awaiter_{std::move(awaiter)} {}

    [[nodiscard]] bool await_ready() { return awaiter_.await_ready(); }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> coroutine) {
      suspended_ = awaiter_.await_suspend(coroutine);
      return suspended_;
    }

    [[nodiscard]] bool await_resume() {
      awaiter_.await_resume();
      return suspended_;
    }

  private:
    sync::Notify::Notified::Awaiter awaiter_;
    bool suspended_{false};
  };

  [[nodiscard]] Awaiter operator co_await() && {
    return Awaiter{notified_.operator co_await()};
  }

private:
  sync::Notify::Notified notified_;
};

template <typename T> class WatchState final {
public:
  struct Read final {
    std::shared_ptr<const T> value;
    std::uint64_t version{0};
    bool senders_closed{false};
  };

  explicit WatchState(T initial, std::size_t receiver_count)
      : value_{std::make_shared<T>(std::move(initial))},
        receiver_count_{receiver_count} {}

  WatchState(const WatchState &) = delete;
  WatchState &operator=(const WatchState &) = delete;

  [[nodiscard]] Read read() const {
    std::lock_guard lock{mutex_};
    return Read{value_, version_, senders_closed_};
  }

  [[nodiscard]] bool has_receivers() const {
    std::lock_guard lock{mutex_};
    return receiver_count_ != 0;
  }

  [[nodiscard]] std::shared_ptr<T>
  publish(std::shared_ptr<T> candidate) {
    std::shared_ptr<T> old;
    {
      std::lock_guard lock{mutex_};
      old = std::move(value_);
      value_ = std::move(candidate);
      ++version_;
    }
    receiver_notify_.notify_waiters();
    return old;
  }

  [[nodiscard]] std::shared_ptr<T>
  publish_replace(std::shared_ptr<T> candidate, bool require_unique_old) {
    std::shared_ptr<T> old;
    {
      std::lock_guard lock{mutex_};
      if (require_unique_old && value_.use_count() != 1) {
        throw std::logic_error{
            "不可复制 watch 值存在 Snapshot 时不能 send_replace"};
      }
      old = std::move(value_);
      value_ = std::move(candidate);
      ++version_;
    }
    receiver_notify_.notify_waiters();
    return old;
  }

  void add_sender() {
    std::lock_guard lock{mutex_};
    if (sender_count_ == 0) {
      throw std::logic_error{"已关闭的 watch channel 不能恢复 Sender"};
    }
    watch_checked_increment(sender_count_, "watch Sender 计数溢出");
  }

  void drop_sender() noexcept {
    bool close = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (sender_count_ == 0) {
          std::terminate();
        }
        --sender_count_;
        close = sender_count_ == 0 && sender_borrow_count_ == 0;
        if (close) {
          senders_closed_ = true;
        }
      }
      if (close) {
        receiver_notify_.notify_waiters();
      }
    } catch (...) {
      std::terminate();
    }
  }

  void add_sender_borrow() {
    std::lock_guard lock{mutex_};
    if (sender_count_ == 0 || senders_closed_) {
      throw std::logic_error{"已关闭的 watch Sender 不能创建 closed operation"};
    }
    watch_checked_increment(sender_borrow_count_,
                            "watch Sender operation 计数溢出");
  }

  void drop_sender_borrow() noexcept {
    bool close = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (sender_borrow_count_ == 0) {
          std::terminate();
        }
        --sender_borrow_count_;
        close = sender_count_ == 0 && sender_borrow_count_ == 0;
        if (close) {
          senders_closed_ = true;
        }
      }
      if (close) {
        receiver_notify_.notify_waiters();
      }
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] std::size_t sender_count() const {
    std::lock_guard lock{mutex_};
    return sender_count_;
  }

  void add_receiver() {
    std::lock_guard lock{mutex_};
    watch_checked_increment(receiver_count_, "watch Receiver 计数溢出");
  }

  void drop_receiver() noexcept {
    bool close = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (receiver_count_ == 0) {
          std::terminate();
        }
        --receiver_count_;
        close = receiver_count_ == 0;
      }
      if (close) {
        sender_notify_.notify_waiters();
      }
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] std::size_t receiver_count() const {
    std::lock_guard lock{mutex_};
    return receiver_count_;
  }

  sync::Notify receiver_notify_;
  sync::Notify sender_notify_;

private:
  mutable std::mutex mutex_;
  std::shared_ptr<T> value_;
  std::uint64_t version_{0};
  std::size_t receiver_count_{0};
  std::size_t sender_count_{1};
  std::size_t sender_borrow_count_{0};
  bool senders_closed_{false};
};

template <typename T> class WatchReceiverLease final {
public:
  WatchReceiverLease(std::shared_ptr<WatchState<T>> state,
                     std::uint64_t version) noexcept
      : state_{std::move(state)}, version_{version} {}

  WatchReceiverLease(const WatchReceiverLease &) = delete;
  WatchReceiverLease &operator=(const WatchReceiverLease &) = delete;

  ~WatchReceiverLease() { state_->drop_receiver(); }

  [[nodiscard]] std::shared_ptr<WatchReceiverLease> clone() const {
    std::uint64_t version = 0;
    {
      std::lock_guard lock{cursor_mutex_};
      ensure_idle_locked();
      version = version_;
    }
    state_->add_receiver();
    try {
      return std::make_shared<WatchReceiverLease>(state_, version);
    } catch (...) {
      state_->drop_receiver();
      throw;
    }
  }

  void begin_operation() {
    std::lock_guard lock{cursor_mutex_};
    ensure_idle_locked();
    operation_active_ = true;
  }

  void end_operation() noexcept {
    try {
      std::lock_guard lock{cursor_mutex_};
      if (!operation_active_) {
        std::terminate();
      }
      operation_active_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] sync::watch::Snapshot<T> snapshot(bool update,
                                                   bool require_idle) {
    const auto current = state_->read();
    bool changed = false;
    {
      std::lock_guard lock{cursor_mutex_};
      if (require_idle) {
        ensure_idle_locked();
      }
      changed = version_ != current.version;
      if (update) {
        version_ = current.version;
      }
    }
    return sync::watch::Snapshot<T>{current.value, changed};
  }

  [[nodiscard]] Result<bool, sync::watch::error::RecvError>
  has_changed() const {
    const auto current = state_->read();
    std::lock_guard lock{cursor_mutex_};
    ensure_idle_locked();
    if (current.senders_closed) {
      return Result<bool, sync::watch::error::RecvError>::failure(
          sync::watch::error::RecvError{});
    }
    return Result<bool, sync::watch::error::RecvError>::success(
        version_ != current.version);
  }

  void mark_changed() {
    std::lock_guard lock{cursor_mutex_};
    ensure_idle_locked();
    --version_;
  }

  void mark_unchanged() {
    const auto current = state_->read();
    std::lock_guard lock{cursor_mutex_};
    ensure_idle_locked();
    version_ = current.version;
  }

  [[nodiscard]] WatchChangeStatus peek_change() const {
    const auto current = state_->read();
    std::lock_guard lock{cursor_mutex_};
    if (!operation_active_) {
      throw std::logic_error{"watch Receiver operation 已取消"};
    }
    if (version_ != current.version) {
      return WatchChangeStatus::changed;
    }
    return current.senders_closed ? WatchChangeStatus::closed
                                  : WatchChangeStatus::pending;
  }

  [[nodiscard]] WatchChangeStatus commit_change() {
    const auto current = state_->read();
    std::lock_guard lock{cursor_mutex_};
    if (!operation_active_) {
      throw std::logic_error{"watch Receiver operation 已取消"};
    }
    if (version_ != current.version) {
      version_ = current.version;
      return WatchChangeStatus::changed;
    }
    return current.senders_closed ? WatchChangeStatus::closed
                                  : WatchChangeStatus::pending;
  }

  struct WaitObservation final {
    sync::watch::Snapshot<T> snapshot;
    bool closed{false};
  };

  [[nodiscard]] WaitObservation observe_for_wait() {
    const auto current = state_->read();
    bool changed = false;
    {
      std::lock_guard lock{cursor_mutex_};
      if (!operation_active_) {
        throw std::logic_error{"watch Receiver operation 已取消"};
      }
      changed = version_ != current.version;
      version_ = current.version;
    }
    return WaitObservation{
        sync::watch::Snapshot<T>{current.value, changed},
        current.senders_closed};
  }

  [[nodiscard]] const std::shared_ptr<WatchState<T>> &state() const noexcept {
    return state_;
  }

private:
  void ensure_idle_locked() const {
    if (operation_active_) {
      throw std::logic_error{"watch Receiver 已有未完成的异步 operation"};
    }
  }

  std::shared_ptr<WatchState<T>> state_;
  mutable std::mutex cursor_mutex_;
  std::uint64_t version_{0};
  bool operation_active_{false};
};

template <typename T> class WatchReceiverOperation final {
public:
  explicit WatchReceiverOperation(
      std::shared_ptr<WatchReceiverLease<T>> receiver)
      : receiver_{std::move(receiver)} {
    receiver_->begin_operation();
  }

  WatchReceiverOperation(const WatchReceiverOperation &) = delete;
  WatchReceiverOperation &operator=(const WatchReceiverOperation &) = delete;

  ~WatchReceiverOperation() { receiver_->end_operation(); }

  [[nodiscard]] const std::shared_ptr<WatchReceiverLease<T>> &
  receiver() const noexcept {
    return receiver_;
  }

private:
  std::shared_ptr<WatchReceiverLease<T>> receiver_;
};

/**
 * wait_for 的 frame-owned 状态。
 *
 * predicate_ 必须先声明，operation_ 后声明；C++ 逆序析构成员，因此取消、
 * 正常完成和异常展开都会先释放 Receiver 的独占 operation，再运行用户
 * Predicate 析构。不得依赖 coroutine 参数的实现相关析构顺序。
 */
template <typename T, typename Predicate> class WatchWaitOperation final {
public:
  WatchWaitOperation(std::shared_ptr<WatchReceiverLease<T>> receiver,
                     Predicate predicate)
      : predicate_{std::move(predicate)}, operation_{std::move(receiver)} {}

  WatchWaitOperation(const WatchWaitOperation &) = delete;
  WatchWaitOperation &operator=(const WatchWaitOperation &) = delete;

  [[nodiscard]] WatchReceiverOperation<T> &operation() noexcept {
    return operation_;
  }

  [[nodiscard]] Predicate &predicate() noexcept { return predicate_; }

private:
  Predicate predicate_;
  WatchReceiverOperation<T> operation_;
};

template <typename T> class WatchSenderBorrow final {
public:
  explicit WatchSenderBorrow(std::shared_ptr<WatchState<T>> state)
      : state_{std::move(state)} {
    state_->add_sender_borrow();
  }

  WatchSenderBorrow(const WatchSenderBorrow &) = delete;
  WatchSenderBorrow &operator=(const WatchSenderBorrow &) = delete;

  ~WatchSenderBorrow() { state_->drop_sender_borrow(); }

  [[nodiscard]] const std::shared_ptr<WatchState<T>> &state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<WatchState<T>> state_;
};

} // namespace cio::detail

namespace cio::sync::watch {

/**
 * watch 多生产者发送句柄。
 *
 * 复制增加 Sender 计数；最后一个 Sender（含正在借用 Sender 的 `closed`
 * operation）消失时关闭接收方向并唤醒全部 Receiver。同步发送不阻塞 runtime
 * worker 等待异步锁，用户构造、移动与析构均在 channel mutex 外执行。
 */
template <typename T> class Sender final {
public:
  static constexpr bool cio_send =
      send_traits<std::remove_cv_t<T>>::value &&
      sync_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

  static_assert(detail::WatchOwnedValue<T>,
                "watch<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  /**
   * 以 T 的默认值创建没有 Receiver 的 watch Sender。
   *
   * 后续 `send` 会因没有 Receiver 而返还值；`send_replace` 仍会更新最新值，
   * `subscribe` 可随时创建第一个 Receiver。
   */
  Sender() requires std::default_initializable<T>
      : state_{std::make_shared<detail::WatchState<T>>(T{}, std::size_t{0})} {}

  /**
   * 创建没有 Receiver、初值为 initial 的 watch Sender。
   */
  [[nodiscard]] static Sender new_sender(T initial) {
    return Sender{std::make_shared<detail::WatchState<T>>(
        std::move(initial), std::size_t{0})};
  }

  Sender(const Sender &other) : state_{other.state_} {
    ensure_valid();
    state_->add_sender();
  }

  Sender &operator=(const Sender &other) {
    if (this == &other) {
      return *this;
    }
    other.ensure_valid();
    other.state_->add_sender();
    reset();
    state_ = other.state_;
    return *this;
  }

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
   * 发布新版本；线性化检查时没有 Receiver 则返还值且不修改 channel。
   *
   * 与 Tokio 一样，receiver count 只作为并发提示：检查后最后一个 Receiver
   * 仍可能退出，此时发送可以成功。发送不跨暂停点，值候选及旧值析构均在锁外。
   */
  [[nodiscard]] Result<void, error::SendError<T>> send(T value) const {
    ensure_valid();
    if (!state_->has_receivers()) {
      return Result<void, error::SendError<T>>::failure(
          error::SendError<T>{std::move(value)});
    }
    auto candidate = std::make_shared<T>(std::move(value));
    auto old = state_->publish(std::move(candidate));
    old.reset();
    return Result<void, error::SendError<T>>::success();
  }

  /**
   * 无条件发布新版本并返回旧值，即 Tokio 1.53.1 的 `send_replace`。
   *
   * 即使没有 Receiver 也会保存新值。若旧值仍被 Snapshot 共享，CIO 必须复制
   * 旧值以保持快照不可变；因此不可复制 T 在存在快照时会在发布前抛出
   * `logic_error`。候选构造失败不修改 channel；返回旧值时的复制/移动异常发生
   * 在新版本已发布并已通知之后。
   */
  [[nodiscard]] T send_replace(T value) const {
    ensure_valid();
    auto candidate = std::make_shared<T>(std::move(value));

    auto old = state_->publish_replace(
        std::move(candidate), !std::copy_constructible<T>);
    if constexpr (std::copy_constructible<T>) {
      return T{*old};
    } else {
      return T{std::move(*old)};
    }
  }

  /**
   * 返回当前值的拥有快照；Sender 视角始终标记为未变化。
   */
  [[nodiscard]] Snapshot<T> borrow() const {
    ensure_valid();
    const auto current = state_->read();
    return Snapshot<T>{current.value, false};
  }

  /**
   * 创建 Receiver；创建前的最新版本对新 Receiver 已读。
   */
  [[nodiscard]] Receiver<T> subscribe() const {
    ensure_valid();
    const auto current = state_->read();
    state_->add_receiver();
    try {
      return Receiver<T>{
          std::make_shared<detail::WatchReceiverLease<T>>(state_,
                                                          current.version)};
    } catch (...) {
      state_->drop_receiver();
      throw;
    }
  }

  /**
   * 等待全部 Receiver 消失。
   *
   * operation 拥有内部 Sender 借用但不改变公开 `sender_count()`；取消只注销
   * Notify waiter 并释放借用。创建通知后再次检查计数，覆盖 wake-before-wait
   * 与最后 Receiver 析构竞态；等待不会阻塞 worker。
   */
  [[nodiscard]] Task<void> closed() const {
    ensure_valid();
    return closed_impl(
        std::make_shared<detail::WatchSenderBorrow<T>>(state_));
  }

  /** 返回当前是否没有 Receiver；这是并发快照，不阻塞也不跨暂停点。 */
  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return state_->receiver_count() == 0;
  }

  /** 返回当前 Receiver 逻辑计数；并发变化可在返回后立即发生。 */
  [[nodiscard]] std::size_t receiver_count() const {
    ensure_valid();
    return state_->receiver_count();
  }

  /** 返回当前公开 Sender 逻辑计数，不包含 closed operation 的隐藏借用。 */
  [[nodiscard]] std::size_t sender_count() const {
    ensure_valid();
    return state_->sender_count();
  }

  /** 判断两个有效 Sender 是否属于同一个 watch channel。 */
  [[nodiscard]] bool same_channel(const Sender &other) const noexcept {
    return state_ && other.state_ && state_ == other.state_;
  }

private:
  explicit Sender(std::shared_ptr<detail::WatchState<T>> state) noexcept
      : state_{std::move(state)} {}

  static Task<void>
  closed_impl(std::shared_ptr<detail::WatchSenderBorrow<T>> borrow) {
    for (;;) {
      auto notified = borrow->state()->sender_notify_.notified_owned();
      if (borrow->state()->receiver_count() == 0) {
        // Pending poll 不扣预算；观察到 ready 后才进入 cooperative gate。
        // 即使 gate 让出期间重新 subscribe，Tokio 也允许已观察到短暂关闭的
        // closed future 返回。
        co_await task::consume_budget();
        co_return;
      }
      co_await std::move(notified);
    }
  }

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"watch Sender 已移出"};
    }
  }

  void reset() noexcept {
    if (state_) {
      state_->drop_sender();
      state_.reset();
    }
  }

  std::shared_ptr<detail::WatchState<T>> state_;

  friend std::pair<Sender<T>, Receiver<T>> channel<T>(T);
};

/**
 * watch 多消费者接收句柄。
 *
 * 每个 Receiver 具有独立版本游标；复制保留当前游标。异步 operation 共享拥有
 * 原 Receiver lease，因此调用方销毁句柄也不会留下悬空引用。一个 Receiver
 * 同时只允许一个改变游标的异步 operation，这对应 Rust 的 `&mut self` 约束。
 */
template <typename T> class Receiver final {
public:
  static constexpr bool cio_send =
      send_traits<std::remove_cv_t<T>>::value &&
      sync_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

  static_assert(detail::WatchOwnedValue<T>,
                "watch<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  Receiver(const Receiver &other) {
    other.ensure_valid();
    lease_ = other.lease_->clone();
  }

  Receiver &operator=(const Receiver &other) {
    if (this == &other) {
      return *this;
    }
    other.ensure_valid();
    auto replacement = other.lease_->clone();
    lease_ = std::move(replacement);
    return *this;
  }

  Receiver(Receiver &&) noexcept = default;
  Receiver &operator=(Receiver &&) noexcept = default;
  ~Receiver() = default;

  /**
   * 返回当前值的拥有快照，不更新当前 Receiver 的版本游标。
   */
  [[nodiscard]] Snapshot<T> borrow() const {
    ensure_valid();
    return lease_->snapshot(false, true);
  }

  /**
   * 返回当前值的拥有快照，并把该版本标记为已读。
   */
  [[nodiscard]] Snapshot<T> borrow_and_update() {
    ensure_valid();
    return lease_->snapshot(true, true);
  }

  /**
   * 判断是否有未读版本；所有 Sender 已析构时始终返回 RecvError。
   */
  [[nodiscard]] Result<bool, error::RecvError> has_changed() const {
    ensure_valid();
    return lease_->has_changed();
  }

  /** 人工把当前 Receiver 游标标记为有变化；不发布值也不通知其他 Receiver。 */
  void mark_changed() {
    ensure_valid();
    lease_->mark_changed();
  }

  /** 把当前最新版本标为已读；不阻塞且不执行用户代码。 */
  void mark_unchanged() {
    ensure_valid();
    lease_->mark_unchanged();
  }

  /**
   * 等待并标记一个尚未观察的新版本。
   *
   * 若已有未读版本，即使全部 Sender 已析构也先成功一次；只有关闭且当前版本
   * 已读才返回 RecvError。取消发生在版本提交前或等待通知时，不会错误标记版本。
   */
  [[nodiscard]] Task<Result<void, error::RecvError>> changed() {
    ensure_valid();
    auto operation =
        std::make_shared<detail::WatchReceiverOperation<T>>(lease_);
    return changed_impl(std::move(operation));
  }

  /**
   * 等待谓词接受最新值，并返回该版本的拥有快照。
   *
   * Predicate 由 coroutine frame 拥有，只在 channel/cursor 锁外调用。每个被
   * 观察版本至多调用一次；返回 false 或抛异常前版本已标记为已读。取消保证：
   * 最后提交的版本必定已由 Predicate 返回 false，不会提交返回 true 的版本。
   */
  template <typename Predicate>
    requires std::predicate<Predicate &, const T &>
  [[nodiscard]] Task<Result<Snapshot<T>, error::RecvError>>
  wait_for(Predicate predicate) {
    ensure_valid();
    auto operation =
        std::make_shared<detail::WatchWaitOperation<T, Predicate>>(
            lease_, std::move(predicate));
    return wait_for_impl(std::move(operation));
  }

  /** 判断两个有效 Receiver 是否属于同一个 watch channel。 */
  [[nodiscard]] bool same_channel(const Receiver &other) const noexcept {
    return lease_ && other.lease_ &&
           lease_->state() == other.lease_->state();
  }

private:
  explicit Receiver(
      std::shared_ptr<detail::WatchReceiverLease<T>> lease) noexcept
      : lease_{std::move(lease)} {}

  static Task<Result<void, error::RecvError>>
  changed_impl(
      std::shared_ptr<detail::WatchReceiverOperation<T>> operation) {
    for (;;) {
      auto notified =
          operation->receiver()->state()->receiver_notify_.notified_owned();
      const auto status = operation->receiver()->peek_change();
      if (status != detail::WatchChangeStatus::pending) {
        // 先通过 cooperative gate，再提交 seen version；预算让出期间取消不会
        // 误标已读。gate 之后重新读取可合并期间到达的更多 watch 更新。
        co_await task::consume_budget();
        const auto committed = operation->receiver()->commit_change();
        if (committed == detail::WatchChangeStatus::changed) {
          co_return Result<void, error::RecvError>::success();
        }
        if (committed == detail::WatchChangeStatus::closed) {
          co_return Result<void, error::RecvError>::failure(
              error::RecvError{});
        }
        continue;
      }
      co_await std::move(notified);
    }
  }

  template <typename Predicate>
  static Task<Result<Snapshot<T>, error::RecvError>>
  wait_for_impl(
      std::shared_ptr<detail::WatchWaitOperation<T, Predicate>> operation) {
    bool first = true;
    bool budget_granted = false;
    for (;;) {
      // 同一 runtime poll 内的立即通知循环只扣一次预算；真正暂停后由新 poll
      // 重新进入 gate。这样 Predicate 永远不会在预算耗尽时先运行。
      if (!budget_granted) {
        co_await task::consume_budget();
        budget_granted = true;
      }
      auto notified =
          operation->operation()
              .receiver()
              ->state()
              ->receiver_notify_.notified_owned();
      bool closed = false;
      {
        // Tokio 在真正等待通知前释放 Ref；CIO 同样必须在 await 前释放 false
        // Snapshot，避免把被替换值的析构无意延迟到下一次唤醒或取消。
        auto observed =
            operation->operation().receiver()->observe_for_wait();
        if (first || observed.snapshot.has_changed()) {
          first = false;
          if (std::invoke(operation->predicate(),
                          observed.snapshot.value())) {
            co_return Result<Snapshot<T>, error::RecvError>::success(
                std::move(observed.snapshot));
          }
        }
        closed = observed.closed;
      }
      if (closed) {
        co_return Result<Snapshot<T>, error::RecvError>::failure(
            error::RecvError{});
      }
      const bool truly_suspended =
          co_await detail::WatchNotification{std::move(notified)};
      if (truly_suspended) {
        budget_granted = false;
      }
    }
  }

  void ensure_valid() const {
    if (!lease_) {
      throw std::logic_error{"watch Receiver 已移出"};
    }
  }

  std::shared_ptr<detail::WatchReceiverLease<T>> lease_;

  friend class Sender<T>;
  friend std::pair<Sender<T>, Receiver<T>> channel<T>(T);
};

/**
 * 创建一个带初始 Receiver 的 watch channel；初值对该 Receiver 已读。
 *
 * 初值按值移入共享状态，不保留调用方引用；本同步工厂不阻塞、不跨暂停点。
 */
template <typename T>
[[nodiscard]] std::pair<Sender<T>, Receiver<T>> channel(T initial) {
  static_assert(detail::WatchOwnedValue<T>,
                "watch<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");
  auto state = std::make_shared<detail::WatchState<T>>(
      std::move(initial), std::size_t{1});
  auto lease =
      std::make_shared<detail::WatchReceiverLease<T>>(state, std::uint64_t{0});
  return {Sender<T>{state}, Receiver<T>{std::move(lease)}};
}

} // namespace cio::sync::watch
