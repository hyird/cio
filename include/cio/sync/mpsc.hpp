#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/result.hpp"
#include "cio/runtime/runtime.hpp"
#include "cio/send.hpp"
#include "cio/sync/notify.hpp"
#include "cio/sync/semaphore.hpp"
#include "cio/task/consume_budget.hpp"
#include "cio/task/task.hpp"

namespace cio::sync::mpsc {

namespace error {

/**
 * 异步发送失败。
 *
 * 错误拥有未发送的值；`send` 只有在 Receiver 已关闭时产生该错误。
 */
template <typename T> class SendError final {
public:
  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = sync_traits<std::remove_cv_t<T>>::value;

  explicit SendError(T value) : value_{std::move(value)} {}

  SendError(const SendError &) = default;
  SendError &operator=(const SendError &) = default;
  SendError(SendError &&) noexcept(std::is_nothrow_move_constructible_v<T>) =
      default;
  SendError &
  operator=(SendError &&) noexcept(std::is_nothrow_move_assignable_v<T>) =
      default;

  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] const T &value() const & noexcept { return value_; }

  [[nodiscard]] T into_inner() && { return T{std::move(value_)}; }

  [[nodiscard]] static constexpr std::string_view message() noexcept {
    return "channel closed";
  }

  [[nodiscard]] static constexpr std::string_view debug_string() noexcept {
    return "SendError { .. }";
  }

private:
  T value_;
};

/**
 * `reserve` 的关闭错误，对应 Tokio 的 `SendError<()>`。
 */
template <> class SendError<void> final {
public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  [[nodiscard]] static constexpr std::string_view message() noexcept {
    return "channel closed";
  }

  [[nodiscard]] static constexpr std::string_view debug_string() noexcept {
    return "SendError { .. }";
  }

  friend constexpr bool operator==(SendError, SendError) noexcept = default;
};

enum class TrySendErrorKind {
  full,
  closed,
};

/**
 * 非阻塞发送失败。
 *
 * `full` 表示当前没有可立即取得的容量，`closed` 表示 Receiver 已关闭。两种
 * 分支都拥有原消息。
 */
template <typename T> class TrySendError final {
public:
  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = sync_traits<std::remove_cv_t<T>>::value;

  [[nodiscard]] static TrySendError full(T value) {
    return TrySendError{TrySendErrorKind::full, std::move(value)};
  }

  [[nodiscard]] static TrySendError closed(T value) {
    return TrySendError{TrySendErrorKind::closed, std::move(value)};
  }

  [[nodiscard]] TrySendErrorKind kind() const noexcept { return kind_; }
  [[nodiscard]] bool is_full() const noexcept {
    return kind_ == TrySendErrorKind::full;
  }
  [[nodiscard]] bool is_closed() const noexcept {
    return kind_ == TrySendErrorKind::closed;
  }

  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] const T &value() const & noexcept { return value_; }
  [[nodiscard]] T into_inner() && { return T{std::move(value_)}; }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return is_full() ? std::string_view{"no available capacity"}
                     : std::string_view{"channel closed"};
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return is_full() ? std::string_view{"\"Full(..)\""}
                     : std::string_view{"\"Closed(..)\""};
  }

private:
  TrySendError(TrySendErrorKind kind, T value)
      : kind_{kind}, value_{std::move(value)} {}

  TrySendErrorKind kind_;
  T value_;
};

/**
 * `try_reserve` 的无 payload 错误，对应 Tokio 的 `TrySendError<()>`。
 */
template <> class TrySendError<void> final {
public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  [[nodiscard]] static constexpr TrySendError full() noexcept {
    return TrySendError{TrySendErrorKind::full};
  }

  [[nodiscard]] static constexpr TrySendError closed() noexcept {
    return TrySendError{TrySendErrorKind::closed};
  }

  [[nodiscard]] constexpr TrySendErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr bool is_full() const noexcept {
    return kind_ == TrySendErrorKind::full;
  }
  [[nodiscard]] constexpr bool is_closed() const noexcept {
    return kind_ == TrySendErrorKind::closed;
  }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return is_full() ? std::string_view{"no available capacity"}
                     : std::string_view{"channel closed"};
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return is_full() ? std::string_view{"\"Full(..)\""}
                     : std::string_view{"\"Closed(..)\""};
  }

  /**
   * 消费无 payload 错误。
   *
   * 这是 Tokio `TrySendError<()>::into_inner() -> ()` 的 C++20 等价；
   * 不返回对象、不挂起，也不执行用户代码。
   */
  void into_inner() && noexcept {}

  friend constexpr bool operator==(TrySendError,
                                   TrySendError) noexcept = default;

private:
  explicit constexpr TrySendError(TrySendErrorKind kind) noexcept
      : kind_{kind} {}

  TrySendErrorKind kind_;
};

enum class TryRecvError {
  empty,
  disconnected,
};

[[nodiscard]] constexpr std::string_view
message(TryRecvError error) noexcept {
  return error == TryRecvError::empty
             ? std::string_view{"receiving on an empty channel"}
             : std::string_view{"receiving on a closed channel"};
}

[[nodiscard]] constexpr std::string_view
debug_string(TryRecvError error) noexcept {
  return error == TryRecvError::empty ? std::string_view{"Empty"}
                                      : std::string_view{"Disconnected"};
}

template <typename T>
inline std::ostream &operator<<(std::ostream &stream,
                               const SendError<T> &) {
  return stream << SendError<T>::message();
}

inline std::ostream &operator<<(std::ostream &stream, SendError<void>) {
  return stream << SendError<void>::message();
}

template <typename T>
inline std::ostream &operator<<(std::ostream &stream,
                               const TrySendError<T> &error) {
  return stream << error.message();
}

inline std::ostream &operator<<(std::ostream &stream, TryRecvError error) {
  return stream << message(error);
}

} // namespace error

template <typename T> class Sender;
template <typename T> class WeakSender;
template <typename T> class Receiver;
template <typename T> class Permit;
template <typename T> class OwnedPermit;
template <typename T> class UnboundedSender;
template <typename T> class WeakUnboundedSender;
template <typename T> class UnboundedReceiver;

template <typename T>
[[nodiscard]] std::pair<Sender<T>, Receiver<T>> channel(std::size_t capacity);

template <typename T>
[[nodiscard]]
std::pair<UnboundedSender<T>, UnboundedReceiver<T>> unbounded_channel();

} // namespace cio::sync::mpsc

namespace cio::detail {

template <typename T>
concept MpscReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::same_as<std::remove_reference_t<decltype(value.get())>,
                        typename T::type>;
};

template <typename T>
concept MpscOwnedValue =
    std::is_object_v<T> && std::move_constructible<T> &&
    !std::is_pointer_v<T> &&
    !MpscReferenceWrapperLike<std::remove_cv_t<T>>;

template <typename T> class MpscState;

enum class MpscSenderLeaseMode {
  initial,
  clone_if_alive,
};

template <typename T> class MpscSenderLease final {
public:
  MpscSenderLease(std::shared_ptr<MpscState<T>> state,
                  MpscSenderLeaseMode mode) noexcept;
  MpscSenderLease(const MpscSenderLease &) = delete;
  MpscSenderLease &operator=(const MpscSenderLease &) = delete;
  ~MpscSenderLease();

  [[nodiscard]] bool active() const noexcept { return active_; }
  [[nodiscard]] const std::shared_ptr<MpscState<T>> &state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<MpscState<T>> state_;
  bool active_{false};
};

template <typename T> class MpscReceiverLease final {
public:
  explicit MpscReceiverLease(std::shared_ptr<MpscState<T>> state) noexcept
      : state_{std::move(state)} {}

  MpscReceiverLease(const MpscReceiverLease &) = delete;
  MpscReceiverLease &operator=(const MpscReceiverLease &) = delete;
  ~MpscReceiverLease();

  [[nodiscard]] const std::shared_ptr<MpscState<T>> &state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<MpscState<T>> state_;
};

enum class MpscReceiveStatus {
  message,
  pending,
  disconnected,
};

template <typename T> struct MpscReceiveAttempt final {
  MpscReceiveStatus status{MpscReceiveStatus::pending};
  std::shared_ptr<T> message;
};

/**
 * bounded mpsc 的共享状态。
 *
 * `messages_` 只保存拥有句柄，锁内不移动、构造或析构用户 `T`。容量等待由
 * Semaphore 的 FIFO 队列负责，接收侧通知由 Notify 负责；所有 wake 都在本状态
 * mutex 之外执行。
 */
template <typename T>
class MpscState final : public std::enable_shared_from_this<MpscState<T>> {
public:
  static_assert(MpscOwnedValue<T>,
                "mpsc 状态只能拥有非指针、非引用包装的可移动对象");

  explicit MpscState(std::size_t capacity)
      : permits_{capacity}, max_capacity_{capacity} {}

  MpscState(const MpscState &) = delete;
  MpscState &operator=(const MpscState &) = delete;

  [[nodiscard]] sync::Semaphore::Acquire acquire_capacity() const {
    return permits_.acquire();
  }

  [[nodiscard]] Result<sync::SemaphorePermit, sync::TryAcquireError>
  try_acquire_capacity() const {
    return permits_.try_acquire();
  }

  [[nodiscard]] std::size_t capacity() const {
    return permits_.available_permits();
  }

  [[nodiscard]] std::size_t max_capacity() const noexcept {
    return max_capacity_;
  }

  [[nodiscard]] sync::Notify::Notified receiver_notification() const {
    return receiver_notify_.notified_owned();
  }

  [[nodiscard]] sync::Notify::Notified sender_closed_notification() const {
    return sender_closed_notify_.notified_owned();
  }

  [[nodiscard]] std::size_t message_count() const {
    std::lock_guard lock{mutex_};
    return messages_.size();
  }

  [[nodiscard]] bool messages_empty() const {
    std::lock_guard lock{mutex_};
    return messages_.empty();
  }

  void enqueue(std::shared_ptr<T> message) {
    {
      std::lock_guard lock{mutex_};
      messages_.push_back(message);
    }
    wake_receiver_noexcept();
  }

  [[nodiscard]] MpscReceiveAttempt<T> try_receive() {
    std::shared_ptr<T> message;
    bool sender_disconnected = false;
    bool receiver_closed = false;
    {
      std::lock_guard lock{mutex_};
      if (!messages_.empty()) {
        message = std::move(messages_.front());
        messages_.pop_front();
      } else {
        sender_disconnected = strong_sender_count_ == 0;
        receiver_closed = receiver_closed_ || receiver_terminated_;
      }
    }

    if (message) {
      permits_.add_permits(1);
      return MpscReceiveAttempt<T>{MpscReceiveStatus::message,
                                   std::move(message)};
    }

    if ((sender_disconnected || receiver_closed) &&
        permits_.available_permits() == max_capacity_) {
      return MpscReceiveAttempt<T>{MpscReceiveStatus::disconnected, {}};
    }
    return MpscReceiveAttempt<T>{MpscReceiveStatus::pending, {}};
  }

  [[nodiscard]] bool receive_ready() {
    bool has_message = false;
    bool sender_disconnected = false;
    bool receiver_closed = false;
    {
      std::lock_guard lock{mutex_};
      has_message = !messages_.empty();
      sender_disconnected = strong_sender_count_ == 0;
      receiver_closed = receiver_closed_ || receiver_terminated_;
    }
    if (has_message) {
      return true;
    }
    return (sender_disconnected || receiver_closed) &&
           permits_.available_permits() == max_capacity_;
  }

  void begin_receive() {
    std::lock_guard lock{mutex_};
    if (receiver_terminated_) {
      throw std::logic_error{"mpsc Receiver 已析构"};
    }
    if (receiver_closing_) {
      throw std::logic_error{"mpsc Receiver 正在关闭"};
    }
    if (receiver_operation_active_) {
      throw std::logic_error{"同一 mpsc Receiver 不能并发接收"};
    }
    receiver_operation_active_ = true;
  }

  void end_receive_noexcept() noexcept {
    try {
      std::lock_guard lock{mutex_};
      receiver_operation_active_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  void receiver_close() {
    {
      std::lock_guard lock{mutex_};
      if (receiver_terminated_) {
        return;
      }
      if (receiver_operation_active_) {
        throw std::logic_error{
            "mpsc Receiver::close 不能与接收操作并发"};
      }
      if (receiver_closed_ || receiver_closing_) {
        return;
      }
      receiver_closing_ = true;
    }

    try {
      permits_.close();
    } catch (...) {
      std::lock_guard lock{mutex_};
      receiver_closing_ = false;
      throw;
    }

    {
      std::lock_guard lock{mutex_};
      receiver_closed_ = true;
      receiver_closing_ = false;
    }
    wake_senders_noexcept();
    wake_receiver_noexcept();
  }

  void receiver_drop_noexcept() noexcept {
    std::deque<std::shared_ptr<T>> dropped;
    std::size_t released = 0;
    try {
      {
        std::lock_guard lock{mutex_};
        if (receiver_terminated_) {
          return;
        }
        receiver_closing_ = true;
      }

      permits_.close();

      {
        std::lock_guard lock{mutex_};
        receiver_closed_ = true;
        receiver_terminated_ = true;
        receiver_closing_ = false;
        released = messages_.size();
        dropped.swap(messages_);
      }
      if (released != 0) {
        permits_.add_permits(released);
      }
      wake_senders_noexcept();
      wake_receiver_noexcept();
      // dropped 在本函数返回时于 mutex 外析构用户 T。
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool sender_is_closed() const {
    return permits_.is_closed();
  }

  [[nodiscard]] bool receiver_is_closed() const {
    bool sender_disconnected = false;
    bool receiver_closed = false;
    {
      std::lock_guard lock{mutex_};
      sender_disconnected = strong_sender_count_ == 0;
      receiver_closed = receiver_closed_ || receiver_terminated_;
    }
    return sender_disconnected || receiver_closed || permits_.is_closed();
  }

  [[nodiscard]] bool clone_sender_if_alive() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (strong_sender_count_ == 0) {
        return false;
      }
      if (strong_sender_count_ == std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      ++strong_sender_count_;
      return true;
    } catch (...) {
      std::terminate();
    }
  }

  void sender_drop_noexcept() noexcept {
    bool close = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (strong_sender_count_ == 0) {
          std::terminate();
        }
        --strong_sender_count_;
        close = strong_sender_count_ == 0;
      }
      if (close) {
        permits_.close();
        wake_receiver_noexcept();
      }
    } catch (...) {
      std::terminate();
    }
  }

  void add_weak_sender() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (weak_sender_count_ == std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      ++weak_sender_count_;
    } catch (...) {
      std::terminate();
    }
  }

  void weak_sender_drop_noexcept() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (weak_sender_count_ == 0) {
        std::terminate();
      }
      --weak_sender_count_;
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] std::size_t strong_sender_count() const {
    std::lock_guard lock{mutex_};
    return strong_sender_count_;
  }

  [[nodiscard]] std::size_t weak_sender_count() const {
    std::lock_guard lock{mutex_};
    return weak_sender_count_;
  }

  void capacity_operation_finished_noexcept() noexcept {
    wake_receiver_noexcept();
  }

private:
  void wake_receiver_noexcept() const noexcept {
    try {
      receiver_notify_.notify_waiters();
    } catch (...) {
      std::terminate();
    }
  }

  void wake_senders_noexcept() const noexcept {
    try {
      sender_closed_notify_.notify_waiters();
    } catch (...) {
      std::terminate();
    }
  }

  sync::Semaphore permits_;
  const std::size_t max_capacity_;
  mutable std::mutex mutex_;
  mutable sync::Notify receiver_notify_;
  mutable sync::Notify sender_closed_notify_;
  std::deque<std::shared_ptr<T>> messages_;
  std::size_t strong_sender_count_{1};
  std::size_t weak_sender_count_{0};
  bool receiver_closed_{false};
  bool receiver_terminated_{false};
  bool receiver_closing_{false};
  bool receiver_operation_active_{false};
};

template <typename T>
MpscSenderLease<T>::MpscSenderLease(std::shared_ptr<MpscState<T>> state,
                                    MpscSenderLeaseMode mode) noexcept
    : state_{std::move(state)} {
  active_ = mode == MpscSenderLeaseMode::initial ||
            state_->clone_sender_if_alive();
}

template <typename T> MpscSenderLease<T>::~MpscSenderLease() {
  if (active_) {
    state_->sender_drop_noexcept();
  }
}

template <typename T> MpscReceiverLease<T>::~MpscReceiverLease() {
  state_->receiver_drop_noexcept();
}

template <typename T> class MpscReceiveGuard final {
public:
  explicit MpscReceiveGuard(std::shared_ptr<MpscState<T>> state)
      : state_{std::move(state)} {
    state_->begin_receive();
  }

  MpscReceiveGuard(const MpscReceiveGuard &) = delete;
  MpscReceiveGuard &operator=(const MpscReceiveGuard &) = delete;
  ~MpscReceiveGuard() { state_->end_receive_noexcept(); }

private:
  std::shared_ptr<MpscState<T>> state_;
};

template <typename T> class MpscCapacityChangeGuard final {
public:
  explicit MpscCapacityChangeGuard(std::shared_ptr<MpscState<T>> state) noexcept
      : state_{std::move(state)} {}

  MpscCapacityChangeGuard(const MpscCapacityChangeGuard &) = delete;
  MpscCapacityChangeGuard &
  operator=(const MpscCapacityChangeGuard &) = delete;

  ~MpscCapacityChangeGuard() {
    if (state_) {
      state_->capacity_operation_finished_noexcept();
    }
  }

  void dismiss() noexcept { state_.reset(); }

private:
  std::shared_ptr<MpscState<T>> state_;
};

template <typename T>
Task<Result<void, sync::mpsc::error::SendError<T>>>
mpsc_send_task(std::shared_ptr<MpscSenderLease<T>> sender, T value);

template <typename T>
Task<Result<sync::mpsc::Permit<T>, sync::mpsc::error::SendError<void>>>
mpsc_reserve_task(std::shared_ptr<MpscSenderLease<T>> sender);

template <typename T>
Task<Result<sync::mpsc::OwnedPermit<T>,
            sync::mpsc::error::SendError<void>>>
mpsc_reserve_owned_task(std::shared_ptr<MpscSenderLease<T>> sender);

template <typename T>
Task<void>
mpsc_sender_closed_task(std::shared_ptr<MpscSenderLease<T>> sender);

template <typename T>
Task<std::optional<T>>
mpsc_recv_task(std::shared_ptr<MpscReceiverLease<T>> receiver);

} // namespace cio::detail

namespace cio::sync::mpsc {

/**
 * 已预留一个 bounded channel 槽位的 move-only 发送许可。
 *
 * Permit 共享原 Sender 的逻辑 lease，不增加 `strong_count()`；因此在 C++ 中即使
 * 原 Sender 对象先析构，也不会违反 Rust 借用 permit 隐含的发送端存活约束。
 * 析构未使用 permit 会归还容量并唤醒公平队列；`send` 成功后容量直到 Receiver
 * 取走消息才归还。Permit 不保存引用或裸地址。
 */
template <typename T> class Permit final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  Permit(const Permit &) = delete;
  Permit &operator=(const Permit &) = delete;

  Permit(Permit &&other) noexcept
      : state_{std::move(other.state_)}, sender_{std::move(other.sender_)},
        capacity_permit_{std::move(other.capacity_permit_)} {}

  Permit &operator=(Permit &&other) noexcept {
    if (this != &other) {
      release_noexcept();
      state_ = std::move(other.state_);
      sender_ = std::move(other.sender_);
      capacity_permit_ = std::move(other.capacity_permit_);
    }
    return *this;
  }

  ~Permit() { release_noexcept(); }

  /**
   * 消费 permit 并发送值。
   *
   * Receiver 在 permit 取得后关闭或析构也不改变成功语义。用户 `T` 的构造与
   * 移动发生在状态锁外；队列发布成功前抛异常时 permit 仍保持有效。
   */
  void send(T value) && {
    ensure_valid();
    auto candidate = std::make_shared<T>(std::move(value));
    send_owned(std::move(candidate));
  }

private:
  Permit(std::shared_ptr<detail::MpscState<T>> state,
         std::shared_ptr<detail::MpscSenderLease<T>> sender,
         sync::SemaphorePermit capacity_permit)
      : state_{std::move(state)}, sender_{std::move(sender)},
        capacity_permit_{std::move(capacity_permit)} {}

  void send_owned(std::shared_ptr<T> value) {
    ensure_valid();
    auto state = state_;
    state->enqueue(std::move(value));
    capacity_permit_->forget();
    capacity_permit_.reset();
    sender_.reset();
    state_.reset();
  }

  void release_noexcept() noexcept {
    auto state = std::move(state_);
    capacity_permit_.reset();
    if (state) {
      state->capacity_operation_finished_noexcept();
    }
    // 先让 close 后等待 idle 的 Receiver 观察到容量归还，再释放强发送端；
    // 这样最后一个 Sender 的断开通知不会早于 permit 结清。
    sender_.reset();
  }

  void ensure_valid() const {
    if (!state_ || !sender_ || !capacity_permit_) {
      throw std::logic_error{"mpsc Permit 已移出或已消费"};
    }
  }

  std::shared_ptr<detail::MpscState<T>> state_;
  std::shared_ptr<detail::MpscSenderLease<T>> sender_;
  std::optional<sync::SemaphorePermit> capacity_permit_;

  friend Task<Result<void, error::SendError<T>>>
  detail::mpsc_send_task<T>(
      std::shared_ptr<detail::MpscSenderLease<T>> sender, T value);
  friend Task<Result<Permit<T>, error::SendError<void>>>
  detail::mpsc_reserve_task<T>(
      std::shared_ptr<detail::MpscSenderLease<T>> sender);
  friend class Sender<T>;
};

/**
 * Tokio 风格的可复制 bounded mpsc 发送端。
 *
 * clone 产生独立逻辑强计数；异步操作共享当前 Sender 的 lease，不捕获 `this`
 * 或引用。销毁最后一个强发送端后，Receiver 在缓冲区和既有 permit 排空后观察
 * 到断开。
 */
template <typename T> class Sender final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  Sender(const Sender &other) {
    other.ensure_valid();
    auto replacement =
        std::make_shared<detail::MpscSenderLease<T>>(
            other.lease_->state(),
            detail::MpscSenderLeaseMode::clone_if_alive);
    if (!replacement->active()) {
      throw std::logic_error{"不能 clone 已断开的 mpsc Sender"};
    }
    lease_ = std::move(replacement);
  }

  Sender &operator=(const Sender &other) {
    if (this != &other) {
      Sender replacement{other};
      lease_.swap(replacement.lease_);
    }
    return *this;
  }

  Sender(Sender &&) noexcept = default;
  Sender &operator=(Sender &&) noexcept = default;
  ~Sender() = default;

  /**
   * 等待一个容量槽并发送拥有值。
   *
   * operation 共享当前 Sender 的逻辑 lease，既不借用 `this`，也不保存调用方
   * 引用；`T` 满足 CIO Send 时可随 task 在线程间迁移。容量等待按 FIFO 排队且
   * 挂起 task，不阻塞 runtime worker。取消发生在发送提交前时保证消息未入队，
   * payload 随 operation 析构；取消会失去队位并归还已分配容量。成功只表示
   * 提交时已取得 permit，不保证 Receiver 最终消费。
   */
  [[nodiscard]] Task<Result<void, error::SendError<T>>> send(T value) const {
    ensure_valid();
    return detail::mpsc_send_task<T>(lease_, std::move(value));
  }

  /**
   * 立即尝试发送拥有值。
   *
   * 本方法不挂起、不阻塞 worker、不保留引用；无容量返回 `Full(T)`，Receiver
   * 已关闭返回 `Closed(T)`。成功后值由 channel 拥有。
   */
  [[nodiscard]] Result<void, error::TrySendError<T>> try_send(T value) const {
    ensure_valid();
    auto acquired = lease_->state()->try_acquire_capacity();
    if (!acquired.has_value()) {
      if (acquired.error() == sync::TryAcquireError::closed) {
        return Result<void, error::TrySendError<T>>::failure(
            error::TrySendError<T>::closed(std::move(value)));
      }
      return Result<void, error::TrySendError<T>>::failure(
          error::TrySendError<T>::full(std::move(value)));
    }

    auto state = lease_->state();
    detail::MpscCapacityChangeGuard<T> completion_guard{state};
    Permit<T> permit{state, lease_, std::move(acquired).value()};
    auto candidate = std::make_shared<T>(std::move(value));
    permit.send_owned(std::move(candidate));
    completion_guard.dismiss();
    return Result<void, error::TrySendError<T>>::success();
  }

  /**
   * FIFO 等待并预留一个容量槽。
   *
   * 返回的 move-only Permit 共享 Sender lease，可安全跨暂停点和 worker
   * 迁移；等待只挂起 task，不阻塞 worker。取消会失去公平队位、归还已分配容量
   * 且不产生消息。Receiver 在 permit 取得后关闭时，permit 仍可发送。
   */
  [[nodiscard]] Task<Result<Permit<T>, error::SendError<void>>>
  reserve() const {
    ensure_valid();
    return detail::mpsc_reserve_task<T>(lease_);
  }

  /**
   * 立即预留一个容量槽，不等待、不阻塞 worker。
   *
   * 成功 Permit 与 `reserve` 相同并共享当前 Sender lease；无容量返回
   * `TrySendError<void>::full()`，Receiver 已关闭返回 `closed()`。失败不改变
   * Sender 所有权，成功 permit 析构会归还容量。
   */
  [[nodiscard]] Result<Permit<T>, error::TrySendError<void>>
  try_reserve() const {
    ensure_valid();
    auto acquired = lease_->state()->try_acquire_capacity();
    if (!acquired.has_value()) {
      if (acquired.error() == sync::TryAcquireError::closed) {
        return Result<Permit<T>, error::TrySendError<void>>::failure(
            error::TrySendError<void>::closed());
      }
      return Result<Permit<T>, error::TrySendError<void>>::failure(
          error::TrySendError<void>::full());
    }
    return Result<Permit<T>, error::TrySendError<void>>::success(
        Permit<T>{lease_->state(), lease_,
                  std::move(acquired).value()});
  }

  /**
   * 消费当前 Sender 并 FIFO 等待一个 owned permit。
   *
   * operation 独占原 Sender 的逻辑 lease，不保存 `this` 或引用，可随 Send 类型
   * 在线程间迁移；等待挂起 task，不阻塞 worker。cooperative gate 先于容量状态
   * 检查。取消或关闭错误会销毁被消费的 Sender；成功返回的 OwnedPermit 可跨越
   * 任意同步作用域，析构归还容量并释放该强发送端。
   */
  [[nodiscard]] Task<Result<OwnedPermit<T>, error::SendError<void>>>
  reserve_owned() &&;
  Task<Result<OwnedPermit<T>, error::SendError<void>>>
  reserve_owned() & = delete;

  /**
   * 消费当前 Sender 并立即尝试取得 owned permit。
   *
   * 成功转移同一个逻辑强发送端到 OwnedPermit；Full/Closed 错误拥有并返还原
   * Sender，调用方可用 `into_inner()` 继续持有它。本方法不挂起、不阻塞 worker。
   */
  [[nodiscard]]
  Result<OwnedPermit<T>, error::TrySendError<Sender<T>>>
  try_reserve_owned() &&;
  Result<OwnedPermit<T>, error::TrySendError<Sender<T>>>
  try_reserve_owned() & = delete;

  /**
   * 判断两个 Sender 是否属于同一 channel。
   */
  [[nodiscard]] bool same_channel(const Sender &other) const {
    ensure_valid();
    other.ensure_valid();
    return lease_->state() == other.lease_->state();
  }

  /**
   * 等待 Receiver 显式 close 或析构。
   *
   * operation 共享当前 Sender lease，不借用 `this`；T 满足 CIO Send 时可跨
   * worker 迁移。等待只挂起 task且 cancel-safe，取消不改变 channel；一旦关闭
   * 永久保持 ready。Tokio 1.53.1 的 `Tx::closed`/`Notified` 路径不参与
   * cooperative budget，CIO 保持这一固定基线行为。
   */
  [[nodiscard]] Task<void> closed() const {
    ensure_valid();
    return detail::mpsc_sender_closed_task<T>(lease_);
  }

  /**
   * 同步线程到异步 channel 的阻塞桥接。
   *
   * 值所有权转入与 `send` 相同的 FIFO operation，不借用调用方状态；普通线程会
   * 阻塞到容量可用或关闭。CIO 异步执行上下文内调用会抛出
   * `std::logic_error`，因此 runtime worker 永不被该方法阻塞。
   */
  [[nodiscard]] Result<void, error::SendError<T>>
  blocking_send(T value) const {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{
          "mpsc Sender::blocking_send 不能在 CIO 异步执行上下文中调用"};
    }
    runtime::Runtime runtime;
    return runtime.block_on(send(std::move(value)));
  }

  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return lease_->state()->sender_is_closed();
  }

  [[nodiscard]] std::size_t capacity() const {
    ensure_valid();
    return lease_->state()->capacity();
  }

  [[nodiscard]] std::size_t max_capacity() const {
    ensure_valid();
    return lease_->state()->max_capacity();
  }

  [[nodiscard]] WeakSender<T> downgrade() const;

  [[nodiscard]] std::size_t strong_count() const {
    ensure_valid();
    return lease_->state()->strong_sender_count();
  }

  [[nodiscard]] std::size_t weak_count() const {
    ensure_valid();
    return lease_->state()->weak_sender_count();
  }

private:
  explicit Sender(std::shared_ptr<detail::MpscSenderLease<T>> lease)
      : lease_{std::move(lease)} {
    if (!lease_ || !lease_->active()) {
      throw std::logic_error{"不能创建无效 mpsc Sender"};
    }
  }

  void ensure_valid() const {
    if (!lease_ || !lease_->active()) {
      throw std::logic_error{"mpsc Sender 已移出"};
    }
  }

  std::shared_ptr<detail::MpscSenderLease<T>> lease_;

  friend class WeakSender<T>;
  friend class OwnedPermit<T>;
  friend std::pair<Sender<T>, Receiver<T>> channel<T>(std::size_t capacity);
};

/**
 * 拥有 Sender 与一个预留槽位的 move-only permit。
 *
 * 与借用 Permit 不同，OwnedPermit 独占一个完整逻辑 Sender lease；不会增加或
 * 减少 `strong_count()`，直到 send/release 把同一 Sender 返回，或析构时释放。
 * 它不保存引用或裸地址，可在 T 满足 CIO Send 时跨线程和暂停边界迁移。
 */
template <typename T> class OwnedPermit final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  OwnedPermit(const OwnedPermit &) = delete;
  OwnedPermit &operator=(const OwnedPermit &) = delete;

  OwnedPermit(OwnedPermit &&other) noexcept
      : state_{std::move(other.state_)}, sender_{std::move(other.sender_)},
        capacity_permit_{std::move(other.capacity_permit_)} {}

  OwnedPermit &operator=(OwnedPermit &&other) noexcept {
    if (this != &other) {
      release_noexcept();
      state_ = std::move(other.state_);
      sender_ = std::move(other.sender_);
      capacity_permit_ = std::move(other.capacity_permit_);
    }
    return *this;
  }

  ~OwnedPermit() { release_noexcept(); }

  /**
   * 使用预留容量发送值，并返回原 Sender。
   *
   * Receiver 在 permit 取得后 close/drop 仍不改变成功语义。用户 T 的构造与
   * 移动、队列通知均在 channel 状态锁外；发布前抛异常时 OwnedPermit 仍有效且
   * 可 release。成功消费 permit，消息被接收前容量保持占用。
   */
  [[nodiscard]] Sender<T> send(T value) && {
    ensure_valid();
    auto candidate = std::make_shared<T>(std::move(value));
    auto state = state_;
    state->enqueue(std::move(candidate));
    capacity_permit_->forget();
    capacity_permit_.reset();
    auto sender = std::move(sender_);
    state_.reset();
    return Sender<T>{std::move(sender)};
  }

  /**
   * 不发送消息地归还容量，并返回原 Sender。
   *
   * 归还会唤醒 FIFO 容量等待者及可能等待关闭排空的 Receiver；不执行用户代码，
   * 不阻塞 runtime worker。
   */
  [[nodiscard]] Sender<T> release() && {
    ensure_valid();
    auto state = state_;
    capacity_permit_.reset();
    auto sender = std::move(sender_);
    state_.reset();
    state->capacity_operation_finished_noexcept();
    return Sender<T>{std::move(sender)};
  }

  /**
   * 判断两个仍有效的 OwnedPermit 是否属于同一 channel。
   *
   * 本方法只比较拥有状态，不挂起、不执行用户代码，也不改变 permit 或容量。
   */
  [[nodiscard]] bool same_channel(const OwnedPermit &other) const noexcept {
    return state_ && other.state_ && state_ == other.state_;
  }

  /**
   * 判断本 OwnedPermit 与一个 Sender 是否属于同一 channel。
   *
   * 这是同步身份查询，不转移 Sender 或 permit 所有权，也不改变关闭状态。
   */
  [[nodiscard]] bool
  same_channel_as_sender(const Sender<T> &sender) const noexcept {
    return state_ && sender.lease_ && state_ == sender.lease_->state();
  }

private:
  OwnedPermit(std::shared_ptr<detail::MpscState<T>> state,
              std::shared_ptr<detail::MpscSenderLease<T>> sender,
              sync::SemaphorePermit capacity_permit)
      : state_{std::move(state)}, sender_{std::move(sender)},
        capacity_permit_{std::move(capacity_permit)} {}

  void release_noexcept() noexcept {
    auto state = std::move(state_);
    capacity_permit_.reset();
    if (state) {
      state->capacity_operation_finished_noexcept();
    }
    // 与 release() 相同：Receiver 先观察到 owned permit 已结清，随后才可能
    // 因最后一个强 Sender 释放而观察到发送侧断开。
    sender_.reset();
  }

  void ensure_valid() const {
    if (!state_ || !sender_ || !capacity_permit_) {
      throw std::logic_error{"mpsc OwnedPermit 已移出或已消费"};
    }
  }

  std::shared_ptr<detail::MpscState<T>> state_;
  std::shared_ptr<detail::MpscSenderLease<T>> sender_;
  std::optional<sync::SemaphorePermit> capacity_permit_;

  friend Task<Result<OwnedPermit<T>, error::SendError<void>>>
  detail::mpsc_reserve_owned_task<T>(
      std::shared_ptr<detail::MpscSenderLease<T>> sender);
  friend class Sender<T>;
};

template <typename T>
Task<Result<OwnedPermit<T>, error::SendError<void>>>
Sender<T>::reserve_owned() && {
  ensure_valid();
  auto sender = std::move(lease_);
  return detail::mpsc_reserve_owned_task<T>(std::move(sender));
}

template <typename T>
Result<OwnedPermit<T>, error::TrySendError<Sender<T>>>
Sender<T>::try_reserve_owned() && {
  ensure_valid();
  auto sender = std::move(lease_);
  auto state = sender->state();
  auto acquired = state->try_acquire_capacity();
  if (!acquired.has_value()) {
    Sender<T> returned{std::move(sender)};
    if (acquired.error() == sync::TryAcquireError::closed) {
      return Result<OwnedPermit<T>,
                    error::TrySendError<Sender<T>>>::failure(
          error::TrySendError<Sender<T>>::closed(std::move(returned)));
    }
    return Result<OwnedPermit<T>,
                  error::TrySendError<Sender<T>>>::failure(
        error::TrySendError<Sender<T>>::full(std::move(returned)));
  }

  return Result<OwnedPermit<T>,
                error::TrySendError<Sender<T>>>::success(
      OwnedPermit<T>{std::move(state), std::move(sender),
                     std::move(acquired).value()});
}

/**
 * 不维持发送侧开放的弱发送端。
 *
 * WeakSender 自身维持控制块分配寿命；逻辑 strong count 一旦到零，`upgrade`
 * 永久失败，不能依靠共享控制块复活通道。
 */
template <typename T> class WeakSender final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  WeakSender(const WeakSender &other) : state_{other.state_} {
    ensure_valid();
    state_->add_weak_sender();
  }

  WeakSender &operator=(const WeakSender &other) {
    if (this != &other) {
      WeakSender replacement{other};
      state_.swap(replacement.state_);
    }
    return *this;
  }

  WeakSender(WeakSender &&) noexcept = default;

  WeakSender &operator=(WeakSender &&other) noexcept {
    if (this != &other) {
      reset();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~WeakSender() { reset(); }

  [[nodiscard]] std::optional<Sender<T>> upgrade() const {
    ensure_valid();
    auto lease = std::make_shared<detail::MpscSenderLease<T>>(
        state_, detail::MpscSenderLeaseMode::clone_if_alive);
    if (!lease->active()) {
      return std::nullopt;
    }
    return Sender<T>{std::move(lease)};
  }

  [[nodiscard]] std::size_t strong_count() const {
    ensure_valid();
    return state_->strong_sender_count();
  }

  [[nodiscard]] std::size_t weak_count() const {
    ensure_valid();
    return state_->weak_sender_count();
  }

private:
  explicit WeakSender(std::shared_ptr<detail::MpscState<T>> state)
      : state_{std::move(state)} {
    state_->add_weak_sender();
  }

  void reset() noexcept {
    auto state = std::move(state_);
    if (state) {
      state->weak_sender_drop_noexcept();
    }
  }

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"mpsc WeakSender 已移出"};
    }
  }

  std::shared_ptr<detail::MpscState<T>> state_;

  friend class Sender<T>;
};

template <typename T> WeakSender<T> Sender<T>::downgrade() const {
  ensure_valid();
  return WeakSender<T>{lease_->state()};
}

/**
 * bounded mpsc 的唯一接收端。
 *
 * Receiver 只能移动。Rust 的 `&mut self` 独占借用在 CIO 中由共享 receiver
 * lease 与 operation guard 共同保证：同一时间最多一个 recv/try_recv/
 * blocking_recv，销毁外层 Receiver 不会让已借出的异步操作悬空。
 */
template <typename T> class Receiver final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  Receiver(const Receiver &) = delete;
  Receiver &operator=(const Receiver &) = delete;
  Receiver(Receiver &&) noexcept = default;

  Receiver &operator=(Receiver &&other) noexcept {
    if (this != &other) {
      lease_.reset();
      lease_ = std::move(other.lease_);
    }
    return *this;
  }

  ~Receiver() = default;

  /**
   * 接收下一条消息，关闭且排空后返回空值。
   *
   * operation 共享 Receiver lease，不保存 `this` 或裸引用；`T` 满足 CIO Send
   * 时可随 task 在线程间迁移。等待使用 Notify 挂起，不阻塞 worker。操作在真正
   * 取走消息前取消不会消费消息；同一 Receiver 同时只允许一个接收 operation。
   * `close()` 后仍会 drain 已缓冲消息，并等待既有 Permit 发送或释放后才终止。
   */
  [[nodiscard]] Task<std::optional<T>> recv() {
    ensure_valid();
    return detail::mpsc_recv_task<T>(lease_);
  }

  /**
   * 立即尝试接收，不挂起也不阻塞 worker。
   *
   * 返回成功后值所有权转给调用方；尚可能有未来消息或 outstanding permit 时
   * 返回 `Empty`，关闭、队列为空且全部 permit 已结清时返回 `Disconnected`。
   * 与异步 `recv` 并发调用会被拒绝。
   */
  [[nodiscard]] Result<T, error::TryRecvError> try_recv() {
    ensure_valid();
    detail::MpscReceiveGuard<T> guard{lease_->state()};
    auto attempt = lease_->state()->try_receive();
    if (attempt.status == detail::MpscReceiveStatus::message) {
      return Result<T, error::TryRecvError>::success(
          T{std::move(*attempt.message)});
    }
    if (attempt.status == detail::MpscReceiveStatus::disconnected) {
      return Result<T, error::TryRecvError>::failure(
          error::TryRecvError::disconnected);
    }
    return Result<T, error::TryRecvError>::failure(
        error::TryRecvError::empty);
  }

  /**
   * 同步线程到异步接收 operation 的阻塞桥接。
   *
   * 生命周期、取消后的独占清理和 drain 规则与 `recv` 相同。普通线程阻塞到消息
   * 或终态；CIO 异步执行上下文内调用抛出 `std::logic_error`，不会阻塞 runtime
   * worker。
   */
  [[nodiscard]] std::optional<T> blocking_recv() {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{
          "mpsc Receiver::blocking_recv 不能在 CIO 异步执行上下文中调用"};
    }
    runtime::Runtime runtime;
    return runtime.block_on(recv());
  }

  /**
   * 关闭接收侧但保留 drain 能力。
   *
   * 调用后拒绝新的 send/reserve，既有 Permit 仍有效；本方法同步完成且不阻塞
   * worker。为替代 Rust `&mut self`，与活动接收 operation 并发时明确拒绝。
   */
  void close() {
    ensure_valid();
    lease_->state()->receiver_close();
  }

  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return lease_->state()->receiver_is_closed();
  }

  /**
   * 返回当前已缓冲消息数量的同步快照。
   *
   * 已预留但尚未发送的 Permit/OwnedPermit 不计入长度；本方法不执行用户代码，
   * 不挂起也不阻塞 runtime worker，但并发发送或接收可在返回后立即改变结果。
   */
  [[nodiscard]] std::size_t len() const {
    ensure_valid();
    return lease_->state()->message_count();
  }

  /**
   * 判断当前缓冲队列是否为空。
   *
   * 只观察已发布消息，不把 outstanding permit 视为消息；结果是并发快照，不
   * 表示 channel 已关闭或未来不会收到消息。
   */
  [[nodiscard]] bool is_empty() const {
    ensure_valid();
    return lease_->state()->messages_empty();
  }

  [[nodiscard]] std::size_t capacity() const {
    ensure_valid();
    return lease_->state()->capacity();
  }

  [[nodiscard]] std::size_t max_capacity() const {
    ensure_valid();
    return lease_->state()->max_capacity();
  }

  [[nodiscard]] std::size_t sender_strong_count() const {
    ensure_valid();
    return lease_->state()->strong_sender_count();
  }

  [[nodiscard]] std::size_t sender_weak_count() const {
    ensure_valid();
    return lease_->state()->weak_sender_count();
  }

private:
  explicit Receiver(std::shared_ptr<detail::MpscReceiverLease<T>> lease)
      : lease_{std::move(lease)} {}

  void ensure_valid() const {
    if (!lease_) {
      throw std::logic_error{"mpsc Receiver 已移出"};
    }
  }

  std::shared_ptr<detail::MpscReceiverLease<T>> lease_;

  friend std::pair<Sender<T>, Receiver<T>> channel<T>(std::size_t capacity);
};

} // namespace cio::sync::mpsc

namespace cio::detail {

template <typename T>
Task<Result<void, sync::mpsc::error::SendError<T>>>
mpsc_send_task(std::shared_ptr<MpscSenderLease<T>> sender, T value) {
  auto state = sender->state();
  // Tokio 1.53.1 的 batch semaphore 在检查 Closed 或取得 ready permit 之前先
  // 消耗 cooperative budget；预算耗尽时不得提前占用容量或公平队位。
  co_await task::consume_budget();
  MpscCapacityChangeGuard<T> completion_guard{state};
  auto acquire = state->acquire_capacity();
  auto acquired = co_await acquire;
  if (!acquired.has_value()) {
    co_return Result<void, sync::mpsc::error::SendError<T>>::failure(
        sync::mpsc::error::SendError<T>{std::move(value)});
  }

  // Tokio 的 send 在 reserve 成功前只让 future 持有 value。C++ 的额外共享值
  // 构造放到 permit 到手后，pending/cancel 不提前执行用户移动构造。
  auto candidate = std::make_shared<T>(std::move(value));
  sync::mpsc::Permit<T> permit{
      state, std::move(sender), std::move(acquired).value()};
  permit.send_owned(std::move(candidate));
  completion_guard.dismiss();
  co_return Result<void, sync::mpsc::error::SendError<T>>::success();
}

template <typename T>
Task<Result<sync::mpsc::Permit<T>, sync::mpsc::error::SendError<void>>>
mpsc_reserve_task(std::shared_ptr<MpscSenderLease<T>> sender) {
  auto state = sender->state();
  // 与 send 共用 Tokio 的 poll_proceed-before-poll_acquire 顺序。
  co_await task::consume_budget();
  MpscCapacityChangeGuard<T> completion_guard{state};
  auto acquire = state->acquire_capacity();
  auto acquired = co_await acquire;
  if (!acquired.has_value()) {
    co_return Result<sync::mpsc::Permit<T>,
                     sync::mpsc::error::SendError<void>>::failure(
        sync::mpsc::error::SendError<void>{});
  }

  sync::mpsc::Permit<T> permit{
      state, std::move(sender), std::move(acquired).value()};
  completion_guard.dismiss();
  co_return Result<sync::mpsc::Permit<T>,
                   sync::mpsc::error::SendError<void>>::success(
      std::move(permit));
}

template <typename T>
Task<Result<sync::mpsc::OwnedPermit<T>,
            sync::mpsc::error::SendError<void>>>
mpsc_reserve_owned_task(std::shared_ptr<MpscSenderLease<T>> sender) {
  auto state = sender->state();
  // 与 Tokio reserve_owned 的 reserve_inner 相同，先经过 cooperative gate，
  // 再观察关闭或容量 ready；预算耗尽时不得提前占位或插入公平队列。
  co_await task::consume_budget();
  MpscCapacityChangeGuard<T> completion_guard{state};
  auto acquire = state->acquire_capacity();
  auto acquired = co_await acquire;
  if (!acquired.has_value()) {
    co_return Result<sync::mpsc::OwnedPermit<T>,
                     sync::mpsc::error::SendError<void>>::failure(
        sync::mpsc::error::SendError<void>{});
  }

  sync::mpsc::OwnedPermit<T> permit{
      state, std::move(sender), std::move(acquired).value()};
  completion_guard.dismiss();
  co_return Result<sync::mpsc::OwnedPermit<T>,
                   sync::mpsc::error::SendError<void>>::success(
      std::move(permit));
}

template <typename T>
Task<void>
mpsc_sender_closed_task(std::shared_ptr<MpscSenderLease<T>> sender) {
  auto state = sender->state();
  for (;;) {
    // 必须先创建通知 future 再检查关闭状态，覆盖检查与 close/drop 之间的竞态。
    // Tokio 1.53.1 的 Tx::closed 不经过 cooperative budget。
    auto notified = state->sender_closed_notification();
    if (state->sender_is_closed()) {
      co_return;
    }
    co_await notified;
  }
}

template <typename T>
Task<std::optional<T>>
mpsc_recv_task(std::shared_ptr<MpscReceiverLease<T>> receiver) {
  auto state = receiver->state();
  MpscReceiveGuard<T> receive_guard{state};

  for (;;) {
    auto notified = state->receiver_notification();
    if (state->receive_ready()) {
      co_await task::consume_budget();
      auto attempt = state->try_receive();
      if (attempt.status == MpscReceiveStatus::message) {
        co_return std::optional<T>{std::in_place,
                                  std::move(*attempt.message)};
      }
      if (attempt.status == MpscReceiveStatus::disconnected) {
        co_return std::nullopt;
      }
      continue;
    }
    co_await notified;
  }
}

} // namespace cio::detail

namespace cio::sync::mpsc {

template <typename T>
std::pair<Sender<T>, Receiver<T>> channel(std::size_t capacity) {
  static_assert(detail::MpscOwnedValue<T>,
                "mpsc<T> 只接受拥有对象，禁止裸指针、引用或非拥有引用包装");

  if (capacity == 0) {
    throw std::invalid_argument{"bounded mpsc channel 容量必须大于零"};
  }
  if (capacity > sync::Semaphore::MAX_PERMITS) {
    throw std::invalid_argument{
        "bounded mpsc channel 容量超过 MAX_PERMITS"};
  }

  auto state = std::make_shared<detail::MpscState<T>>(capacity);
  auto sender_lease = std::make_shared<detail::MpscSenderLease<T>>(
      state, detail::MpscSenderLeaseMode::initial);
  auto receiver_lease =
      std::make_shared<detail::MpscReceiverLease<T>>(state);
  return std::pair<Sender<T>, Receiver<T>>{
      Sender<T>{std::move(sender_lease)},
      Receiver<T>{std::move(receiver_lease)}};
}

} // namespace cio::sync::mpsc

namespace cio::detail {

template <typename T> class UnboundedMpscState;

enum class UnboundedMpscSenderLeaseMode {
  initial,
  clone_if_alive,
};

template <typename T> class UnboundedMpscSenderLease final {
public:
  UnboundedMpscSenderLease(
      std::shared_ptr<UnboundedMpscState<T>> state,
      UnboundedMpscSenderLeaseMode mode) noexcept;
  UnboundedMpscSenderLease(const UnboundedMpscSenderLease &) = delete;
  UnboundedMpscSenderLease &
  operator=(const UnboundedMpscSenderLease &) = delete;
  ~UnboundedMpscSenderLease();

  [[nodiscard]] bool active() const noexcept { return active_; }
  [[nodiscard]] const std::shared_ptr<UnboundedMpscState<T>> &
  state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<UnboundedMpscState<T>> state_;
  bool active_{false};
};

template <typename T> class UnboundedMpscReceiverLease final {
public:
  explicit UnboundedMpscReceiverLease(
      std::shared_ptr<UnboundedMpscState<T>> state) noexcept
      : state_{std::move(state)} {}

  UnboundedMpscReceiverLease(const UnboundedMpscReceiverLease &) = delete;
  UnboundedMpscReceiverLease &
  operator=(const UnboundedMpscReceiverLease &) = delete;
  ~UnboundedMpscReceiverLease();

  [[nodiscard]] const std::shared_ptr<UnboundedMpscState<T>> &
  state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<UnboundedMpscState<T>> state_;
};

/**
 * unbounded mpsc 的共享状态。
 *
 * send 与 Receiver close/drop 在同一 mutex 下决定先后关系；队列只保存拥有
 * `T` 的 shared_ptr，因此用户值移动、析构与重入回调均不发生在状态锁内。
 * send 在入锁前完成值构造、在锁内一次性完成入队，所以不需要独立的 in-flight
 * publication 计数：先入队则 close 必须 drain 该消息，先 close 则 send 返回
 * 原值。Notify 只负责 task 唤醒，普通线程阻塞由临时 current-thread runtime
 * 桥接。
 */
template <typename T>
class UnboundedMpscState final {
public:
  static_assert(MpscOwnedValue<T>,
                "unbounded mpsc 状态只能拥有非指针、非引用包装的可移动对象");

  UnboundedMpscState() = default;
  UnboundedMpscState(const UnboundedMpscState &) = delete;
  UnboundedMpscState &operator=(const UnboundedMpscState &) = delete;

  [[nodiscard]] sync::Notify::Notified receiver_notification() const {
    return receiver_notify_.notified_owned();
  }

  [[nodiscard]] sync::Notify::Notified sender_closed_notification() const {
    return sender_closed_notify_.notified_owned();
  }

  /**
   * 尝试发布已完整构造的消息。
   *
   * 返回 true 的线性化点是持锁入队；返回 false 的线性化点是观察到接收端已经
   * close/drop。shared_ptr 的移动不执行 T，队列分配异常也不会半提交消息。
   */
  [[nodiscard]] bool try_enqueue(std::shared_ptr<T> message) {
    {
      std::lock_guard lock{mutex_};
      if (receiver_closed_ || receiver_terminated_) {
        return false;
      }
      messages_.push_back(std::move(message));
    }
    wake_receiver_noexcept();
    return true;
  }

  [[nodiscard]] MpscReceiveAttempt<T> try_receive() {
    std::shared_ptr<T> message;
    bool disconnected = false;
    {
      std::lock_guard lock{mutex_};
      if (!messages_.empty()) {
        message = std::move(messages_.front());
        messages_.pop_front();
      } else {
        disconnected = strong_sender_count_ == 0 ||
                       receiver_closed_ || receiver_terminated_;
      }
    }

    if (message) {
      return MpscReceiveAttempt<T>{MpscReceiveStatus::message,
                                   std::move(message)};
    }
    return MpscReceiveAttempt<T>{
        disconnected ? MpscReceiveStatus::disconnected
                     : MpscReceiveStatus::pending,
        {}};
  }

  [[nodiscard]] bool receive_ready() const {
    std::lock_guard lock{mutex_};
    return !messages_.empty() || strong_sender_count_ == 0 ||
           receiver_closed_ || receiver_terminated_;
  }

  [[nodiscard]] std::size_t message_count() const {
    std::lock_guard lock{mutex_};
    return messages_.size();
  }

  [[nodiscard]] bool messages_empty() const {
    std::lock_guard lock{mutex_};
    return messages_.empty();
  }

  void begin_receive() {
    std::lock_guard lock{mutex_};
    if (receiver_terminated_) {
      throw std::logic_error{"unbounded mpsc Receiver 已析构"};
    }
    if (receiver_operation_active_) {
      throw std::logic_error{
          "同一 unbounded mpsc Receiver 不能并发接收"};
    }
    receiver_operation_active_ = true;
  }

  void end_receive_noexcept() noexcept {
    try {
      std::lock_guard lock{mutex_};
      receiver_operation_active_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  void receiver_close() {
    {
      std::lock_guard lock{mutex_};
      if (receiver_terminated_ || receiver_closed_) {
        return;
      }
      if (receiver_operation_active_) {
        throw std::logic_error{
            "unbounded mpsc Receiver::close 不能与接收操作并发"};
      }
      receiver_closed_ = true;
    }
    wake_senders_noexcept();
    wake_receiver_noexcept();
  }

  void receiver_drop_noexcept() noexcept {
    std::deque<std::shared_ptr<T>> dropped;
    try {
      {
        std::lock_guard lock{mutex_};
        if (receiver_terminated_) {
          return;
        }
        receiver_closed_ = true;
        receiver_terminated_ = true;
        dropped.swap(messages_);
      }
      wake_senders_noexcept();
      wake_receiver_noexcept();
      // dropped 在 mutex 外析构，允许用户析构函数安全查询 channel。
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool sender_is_closed() const {
    std::lock_guard lock{mutex_};
    return receiver_closed_ || receiver_terminated_;
  }

  [[nodiscard]] bool receiver_is_closed() const {
    std::lock_guard lock{mutex_};
    return strong_sender_count_ == 0 || receiver_closed_ ||
           receiver_terminated_;
  }

  [[nodiscard]] bool clone_sender_if_alive() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (strong_sender_count_ == 0) {
        return false;
      }
      if (strong_sender_count_ == std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      ++strong_sender_count_;
      return true;
    } catch (...) {
      std::terminate();
    }
  }

  void sender_drop_noexcept() noexcept {
    bool disconnected = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (strong_sender_count_ == 0) {
          std::terminate();
        }
        --strong_sender_count_;
        disconnected = strong_sender_count_ == 0;
      }
      if (disconnected) {
        wake_receiver_noexcept();
      }
    } catch (...) {
      std::terminate();
    }
  }

  void add_weak_sender() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (weak_sender_count_ == std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      ++weak_sender_count_;
    } catch (...) {
      std::terminate();
    }
  }

  void weak_sender_drop_noexcept() noexcept {
    try {
      std::lock_guard lock{mutex_};
      if (weak_sender_count_ == 0) {
        std::terminate();
      }
      --weak_sender_count_;
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] std::size_t strong_sender_count() const {
    std::lock_guard lock{mutex_};
    return strong_sender_count_;
  }

  [[nodiscard]] std::size_t weak_sender_count() const {
    std::lock_guard lock{mutex_};
    return weak_sender_count_;
  }

private:
  void wake_receiver_noexcept() const noexcept {
    try {
      receiver_notify_.notify_waiters();
    } catch (...) {
      std::terminate();
    }
  }

  void wake_senders_noexcept() const noexcept {
    try {
      sender_closed_notify_.notify_waiters();
    } catch (...) {
      std::terminate();
    }
  }

  mutable std::mutex mutex_;
  mutable sync::Notify receiver_notify_;
  mutable sync::Notify sender_closed_notify_;
  std::deque<std::shared_ptr<T>> messages_;
  std::size_t strong_sender_count_{1};
  std::size_t weak_sender_count_{0};
  bool receiver_closed_{false};
  bool receiver_terminated_{false};
  bool receiver_operation_active_{false};
};

template <typename T>
UnboundedMpscSenderLease<T>::UnboundedMpscSenderLease(
    std::shared_ptr<UnboundedMpscState<T>> state,
    UnboundedMpscSenderLeaseMode mode) noexcept
    : state_{std::move(state)} {
  active_ = mode == UnboundedMpscSenderLeaseMode::initial ||
            state_->clone_sender_if_alive();
}

template <typename T>
UnboundedMpscSenderLease<T>::~UnboundedMpscSenderLease() {
  if (active_) {
    state_->sender_drop_noexcept();
  }
}

template <typename T>
UnboundedMpscReceiverLease<T>::~UnboundedMpscReceiverLease() {
  state_->receiver_drop_noexcept();
}

template <typename T> class UnboundedMpscReceiveGuard final {
public:
  explicit UnboundedMpscReceiveGuard(
      std::shared_ptr<UnboundedMpscState<T>> state)
      : state_{std::move(state)} {
    state_->begin_receive();
  }

  UnboundedMpscReceiveGuard(const UnboundedMpscReceiveGuard &) = delete;
  UnboundedMpscReceiveGuard &
  operator=(const UnboundedMpscReceiveGuard &) = delete;
  ~UnboundedMpscReceiveGuard() { state_->end_receive_noexcept(); }

private:
  std::shared_ptr<UnboundedMpscState<T>> state_;
};

template <typename T>
Task<void> unbounded_mpsc_sender_closed_task(
    std::shared_ptr<UnboundedMpscSenderLease<T>> sender) {
  auto state = sender->state();
  for (;;) {
    // 先注册通知再检查 close/drop，覆盖两者之间的并发窗口。Tokio 1.53.1
    // Tx::closed 不调用 coop::poll_proceed，因此这里不消耗协作预算。
    auto notified = state->sender_closed_notification();
    if (state->sender_is_closed()) {
      co_return;
    }
    co_await notified;
  }
}

template <typename T>
Task<std::optional<T>> unbounded_mpsc_recv_task(
    std::shared_ptr<UnboundedMpscReceiverLease<T>> receiver) {
  auto state = receiver->state();
  UnboundedMpscReceiveGuard<T> receive_guard{state};

  for (;;) {
    auto notified = state->receiver_notification();
    if (state->receive_ready()) {
      // Tokio chan::recv 的 ready、关闭与错误路径均先经过 poll_proceed。
      // consume_budget 在真实 Notify 唤醒后的 fresh poll 也精确扣除一次预算。
      co_await task::consume_budget();
      auto attempt = state->try_receive();
      if (attempt.status == MpscReceiveStatus::message) {
        co_return std::optional<T>{std::in_place,
                                  std::move(*attempt.message)};
      }
      if (attempt.status == MpscReceiveStatus::disconnected) {
        co_return std::nullopt;
      }
      continue;
    }
    co_await notified;
  }
}

} // namespace cio::detail

namespace cio::sync::mpsc {

/**
 * unbounded mpsc 的发送端。
 *
 * 发送端可复制；每个实例持有一个逻辑强 lease。`T` 必须是 CIO 审核通过的
 * owning value，公开 API 不接受或暴露裸指针。
 */
template <typename T> class UnboundedSender final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "unbounded mpsc<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  UnboundedSender(const UnboundedSender &other)
      : lease_{std::make_shared<detail::UnboundedMpscSenderLease<T>>(
            other.valid_state(),
            detail::UnboundedMpscSenderLeaseMode::clone_if_alive)} {
    if (!lease_->active()) {
      throw std::logic_error{"不能复制已断开的 UnboundedSender"};
    }
  }

  UnboundedSender &operator=(const UnboundedSender &other) {
    if (this != &other) {
      UnboundedSender replacement{other};
      lease_.swap(replacement.lease_);
    }
    return *this;
  }

  UnboundedSender(UnboundedSender &&) noexcept = default;
  UnboundedSender &operator=(UnboundedSender &&) noexcept = default;
  ~UnboundedSender() = default;

  /**
   * 同步发送一条拥有消息。
   *
   * unbounded channel 没有背压，因此本方法不挂起、不阻塞 runtime worker，也
   * 不消耗 cooperative budget。Receiver close/drop 与入队在状态锁内线性化；
   * 失败错误拥有未发送值。用户值移动、析构和异常均发生在锁外，异常不会发布
   * 半条消息。
   */
  [[nodiscard]] Result<void, error::SendError<T>> send(T value) const {
    auto state = valid_state();
    if (state->sender_is_closed()) {
      return Result<void, error::SendError<T>>::failure(
          error::SendError<T>{std::move(value)});
    }

    auto candidate = std::make_shared<T>(std::move(value));
    if (!state->try_enqueue(candidate)) {
      return Result<void, error::SendError<T>>::failure(
          error::SendError<T>{std::move(*candidate)});
    }
    return Result<void, error::SendError<T>>::success();
  }

  /**
   * 等待 Receiver close/drop。
   *
   * operation 持有 Sender lease 而非 `this` 或裸引用，可安全跨暂停点并在 T
   * 满足 CIO Send 时迁移线程。取消只撤销 waiter；关闭一旦发生永久可观察。
   * Tokio 1.53.1 的该操作不消耗 cooperative budget，也不阻塞 worker。
   */
  [[nodiscard]] Task<void> closed() const {
    ensure_valid();
    return detail::unbounded_mpsc_sender_closed_task<T>(lease_);
  }

  /**
   * 判断 Receiver 是否已经 close 或析构。
   *
   * 这是并发快照，不挂起、不消耗 cooperative budget，也不执行用户代码。
   */
  [[nodiscard]] bool is_closed() const {
    return valid_state()->sender_is_closed();
  }

  /**
   * 判断两个强发送端是否属于同一 channel。
   *
   * 比较拥有式状态身份，不暴露原生地址；moved-from 对象与任何对象都不相同。
   */
  [[nodiscard]] bool
  same_channel(const UnboundedSender &other) const noexcept {
    return lease_ && other.lease_ &&
           lease_->state() == other.lease_->state();
  }

  /**
   * 创建不维持 channel 开放的逻辑弱发送端。
   */
  [[nodiscard]] WeakUnboundedSender<T> downgrade() const;

  /**
   * 返回当前逻辑强发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t strong_count() const {
    return valid_state()->strong_sender_count();
  }

  /**
   * 返回当前逻辑弱发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t weak_count() const {
    return valid_state()->weak_sender_count();
  }

private:
  explicit UnboundedSender(
      std::shared_ptr<detail::UnboundedMpscSenderLease<T>> lease)
      : lease_{std::move(lease)} {
    if (!lease_ || !lease_->active()) {
      throw std::logic_error{"不能创建无效 UnboundedSender"};
    }
  }

  void ensure_valid() const {
    if (!lease_ || !lease_->active()) {
      throw std::logic_error{"UnboundedSender 已移出"};
    }
  }

  [[nodiscard]] const std::shared_ptr<detail::UnboundedMpscState<T>> &
  valid_state() const {
    ensure_valid();
    return lease_->state();
  }

  std::shared_ptr<detail::UnboundedMpscSenderLease<T>> lease_;

  friend class WeakUnboundedSender<T>;
  friend std::pair<UnboundedSender<T>, UnboundedReceiver<T>>
  unbounded_channel<T>();
};

/**
 * 不维持发送侧开放的 unbounded 弱发送端。
 *
 * 弱发送端只维持控制块寿命；逻辑强计数归零后 upgrade 永久失败。
 */
template <typename T> class WeakUnboundedSender final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "unbounded mpsc<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  WeakUnboundedSender(const WeakUnboundedSender &other)
      : state_{other.valid_state()} {
    state_->add_weak_sender();
  }

  WeakUnboundedSender &operator=(const WeakUnboundedSender &other) {
    if (this != &other) {
      WeakUnboundedSender replacement{other};
      state_.swap(replacement.state_);
    }
    return *this;
  }

  WeakUnboundedSender(WeakUnboundedSender &&) noexcept = default;

  WeakUnboundedSender &operator=(WeakUnboundedSender &&other) noexcept {
    if (this != &other) {
      reset();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~WeakUnboundedSender() { reset(); }

  /**
   * 尝试升级为强发送端。
   *
   * strong count 归零后永久失败；Receiver 已 close/drop 但仍存在 strong 时可以
   * 成功，所得 Sender 保持 closed。同步完成、不挂起也不执行用户代码。
   */
  [[nodiscard]] std::optional<UnboundedSender<T>> upgrade() const {
    auto lease =
        std::make_shared<detail::UnboundedMpscSenderLease<T>>(
            valid_state(),
            detail::UnboundedMpscSenderLeaseMode::clone_if_alive);
    if (!lease->active()) {
      return std::nullopt;
    }
    return UnboundedSender<T>{std::move(lease)};
  }

  /**
   * 返回当前逻辑强发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t strong_count() const {
    return valid_state()->strong_sender_count();
  }

  /**
   * 返回当前逻辑弱发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t weak_count() const {
    return valid_state()->weak_sender_count();
  }

private:
  explicit WeakUnboundedSender(
      std::shared_ptr<detail::UnboundedMpscState<T>> state)
      : state_{std::move(state)} {
    state_->add_weak_sender();
  }

  void reset() noexcept {
    auto state = std::move(state_);
    if (state) {
      state->weak_sender_drop_noexcept();
    }
  }

  [[nodiscard]] const std::shared_ptr<detail::UnboundedMpscState<T>> &
  valid_state() const {
    if (!state_) {
      throw std::logic_error{"WeakUnboundedSender 已移出"};
    }
    return state_;
  }

  std::shared_ptr<detail::UnboundedMpscState<T>> state_;

  friend class UnboundedSender<T>;
};

template <typename T>
WeakUnboundedSender<T> UnboundedSender<T>::downgrade() const {
  return WeakUnboundedSender<T>{valid_state()};
}

/**
 * unbounded mpsc 的唯一接收端。
 *
 * Receiver 只能移动。operation 持有共享 receiver lease，不捕获 `this` 或裸
 * 引用；同一时间最多一个 recv/try_recv/blocking_recv。
 */
template <typename T> class UnboundedReceiver final {
public:
  static_assert(detail::MpscOwnedValue<T>,
                "unbounded mpsc<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = send_traits<std::remove_cv_t<T>>::value;

  UnboundedReceiver(const UnboundedReceiver &) = delete;
  UnboundedReceiver &operator=(const UnboundedReceiver &) = delete;
  UnboundedReceiver(UnboundedReceiver &&) noexcept = default;

  UnboundedReceiver &operator=(UnboundedReceiver &&other) noexcept {
    if (this != &other) {
      lease_.reset();
      lease_ = std::move(other.lease_);
    }
    return *this;
  }

  ~UnboundedReceiver() = default;

  /**
   * 接收下一条消息，关闭且排空后返回空值。
   *
   * 取走消息前取消不会消费消息；等待通过 Notify 挂起，不阻塞 worker。ready、
   * 关闭和终态路径均按 Tokio 1.53.1 消耗 cooperative budget。operation 拥有
   * receiver lease，T 满足 CIO Send 时可在线程间迁移。
   */
  [[nodiscard]] Task<std::optional<T>> recv() {
    ensure_valid();
    return detail::unbounded_mpsc_recv_task<T>(lease_);
  }

  /**
   * 立即尝试接收。
   *
   * 该操作不挂起、不消耗预算；成功时转移消息所有权，仍有 Sender 时空队列返回
   * Empty，发送侧断开或 Receiver close 且排空后返回 Disconnected。
   */
  [[nodiscard]] Result<T, error::TryRecvError> try_recv() {
    ensure_valid();
    detail::UnboundedMpscReceiveGuard<T> guard{lease_->state()};
    auto attempt = lease_->state()->try_receive();
    if (attempt.status == detail::MpscReceiveStatus::message) {
      return Result<T, error::TryRecvError>::success(
          T{std::move(*attempt.message)});
    }
    if (attempt.status == detail::MpscReceiveStatus::disconnected) {
      return Result<T, error::TryRecvError>::failure(
          error::TryRecvError::disconnected);
    }
    return Result<T, error::TryRecvError>::failure(
        error::TryRecvError::empty);
  }

  /**
   * 在普通线程阻塞等待 recv。
   *
   * 内部使用临时 current-thread runtime 驱动同一个取消安全 operation；CIO
   * 异步执行上下文内调用会抛出 logic_error，绝不阻塞 runtime worker。
   */
  [[nodiscard]] std::optional<T> blocking_recv() {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{
          "UnboundedReceiver::blocking_recv 不能在 CIO 异步执行上下文中调用"};
    }
    runtime::Runtime runtime;
    return runtime.block_on(recv());
  }

  /**
   * 关闭接收侧并保留已缓冲消息的 drain 能力。
   *
   * close 后所有新 send 返回拥有原值的 SendError。为对应 Rust `&mut self`，
   * 本方法拒绝与活动接收 operation 并发。
   */
  void close() {
    ensure_valid();
    lease_->state()->receiver_close();
  }

  /**
   * 判断接收侧是否已显式关闭，或全部强发送端是否已析构。
   *
   * 即使仍有待 drain 消息也可返回 true；结果是同步并发快照。
   */
  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return lease_->state()->receiver_is_closed();
  }

  /**
   * 返回当前已发布且尚未接收的消息数量快照。
   */
  [[nodiscard]] std::size_t len() const {
    ensure_valid();
    return lease_->state()->message_count();
  }

  /**
   * 判断当前消息队列是否为空。
   *
   * 该状态与 channel 是否关闭正交，并发发送可在返回后立即改变结果。
   */
  [[nodiscard]] bool is_empty() const {
    ensure_valid();
    return lease_->state()->messages_empty();
  }

  /**
   * 返回当前逻辑强发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t sender_strong_count() const {
    ensure_valid();
    return lease_->state()->strong_sender_count();
  }

  /**
   * 返回当前逻辑弱发送端数量的并发快照。
   */
  [[nodiscard]] std::size_t sender_weak_count() const {
    ensure_valid();
    return lease_->state()->weak_sender_count();
  }

private:
  explicit UnboundedReceiver(
      std::shared_ptr<detail::UnboundedMpscReceiverLease<T>> lease)
      : lease_{std::move(lease)} {}

  void ensure_valid() const {
    if (!lease_) {
      throw std::logic_error{"UnboundedReceiver 已移出"};
    }
  }

  std::shared_ptr<detail::UnboundedMpscReceiverLease<T>> lease_;

  friend std::pair<UnboundedSender<T>, UnboundedReceiver<T>>
  unbounded_channel<T>();
};

/**
 * 创建 Tokio 风格的无界 mpsc channel。
 *
 * 系统内存是隐式上限；本函数只创建拥有控制块，不启动线程、不阻塞。消息类型
 * 必须是可移动的 owning value，禁止裸指针与引用包装。
 */
template <typename T>
std::pair<UnboundedSender<T>, UnboundedReceiver<T>>
unbounded_channel() {
  static_assert(detail::MpscOwnedValue<T>,
                "unbounded mpsc<T> 禁止裸指针、引用或非拥有引用包装");

  auto state = std::make_shared<detail::UnboundedMpscState<T>>();
  auto sender_lease =
      std::make_shared<detail::UnboundedMpscSenderLease<T>>(
          state, detail::UnboundedMpscSenderLeaseMode::initial);
  auto receiver_lease =
      std::make_shared<detail::UnboundedMpscReceiverLease<T>>(state);
  return std::pair<UnboundedSender<T>, UnboundedReceiver<T>>{
      UnboundedSender<T>{std::move(sender_lease)},
      UnboundedReceiver<T>{std::move(receiver_lease)}};
}

} // namespace cio::sync::mpsc
