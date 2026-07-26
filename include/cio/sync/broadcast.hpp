#pragma once

#include <bit>
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/result.hpp"
#include "cio/runtime/runtime.hpp"
#include "cio/send.hpp"
#include "cio/sync/notify.hpp"
#include "cio/task/consume_budget.hpp"
#include "cio/task/task.hpp"

namespace cio::sync::broadcast {

namespace error {

/**
 * broadcast 发送失败；错误拥有未发布的值。
 */
template <typename T> class SendError final {
public:
  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = sync_traits<std::remove_cv_t<T>>::value;

  explicit SendError(T value) : value_{std::move(value)} {}

  /** 借用错误值；引用不得超过当前 lvalue SendError 生命周期。 */
  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] const T &value() const & noexcept { return value_; }
  [[nodiscard]] T &&value() && = delete;
  [[nodiscard]] const T &value() const && = delete;

  /** 按值取回失败发送的值，避免临时错误对象暴露悬空引用。 */
  [[nodiscard]] T
  into_inner() && noexcept(std::is_nothrow_move_constructible_v<T>) {
    return T{std::move(value_)};
  }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "channel closed";
  }

  [[nodiscard]] constexpr std::string_view debug_string() const noexcept {
    return "SendError(..)";
  }

private:
  T value_;
};

enum class RecvErrorKind {
  closed,
  lagged,
};

/**
 * broadcast 异步接收错误。
 */
class RecvError final {
public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  [[nodiscard]] static constexpr RecvError closed() noexcept {
    return RecvError{RecvErrorKind::closed, 0};
  }

  [[nodiscard]] static constexpr RecvError
  lagged(std::uint64_t skipped) noexcept {
    return RecvError{RecvErrorKind::lagged, skipped};
  }

  [[nodiscard]] constexpr RecvErrorKind kind() const noexcept { return kind_; }
  [[nodiscard]] constexpr bool is_closed() const noexcept {
    return kind_ == RecvErrorKind::closed;
  }
  [[nodiscard]] constexpr bool is_lagged() const noexcept {
    return kind_ == RecvErrorKind::lagged;
  }
  [[nodiscard]] constexpr std::uint64_t skipped() const noexcept {
    return skipped_;
  }
  [[nodiscard]] constexpr std::uint64_t lagged() const noexcept {
    return skipped_;
  }

  [[nodiscard]] std::string message() const {
    if (is_closed()) {
      return "channel closed";
    }
    return "channel lagged by " + std::to_string(skipped_);
  }

  [[nodiscard]] std::string debug_string() const {
    if (is_closed()) {
      return "Closed";
    }
    return "Lagged(" + std::to_string(skipped_) + ")";
  }

  friend constexpr bool operator==(RecvError, RecvError) noexcept = default;

private:
  constexpr RecvError(RecvErrorKind kind, std::uint64_t skipped) noexcept
      : kind_{kind}, skipped_{skipped} {}

  RecvErrorKind kind_;
  std::uint64_t skipped_;
};

enum class TryRecvErrorKind {
  empty,
  closed,
  lagged,
};

/**
 * broadcast 同步尝试接收错误。
 */
class TryRecvError final {
public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  [[nodiscard]] static constexpr TryRecvError empty() noexcept {
    return TryRecvError{TryRecvErrorKind::empty, 0};
  }

  [[nodiscard]] static constexpr TryRecvError closed() noexcept {
    return TryRecvError{TryRecvErrorKind::closed, 0};
  }

  [[nodiscard]] static constexpr TryRecvError
  lagged(std::uint64_t skipped) noexcept {
    return TryRecvError{TryRecvErrorKind::lagged, skipped};
  }

  [[nodiscard]] constexpr TryRecvErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr bool is_empty() const noexcept {
    return kind_ == TryRecvErrorKind::empty;
  }
  [[nodiscard]] constexpr bool is_closed() const noexcept {
    return kind_ == TryRecvErrorKind::closed;
  }
  [[nodiscard]] constexpr bool is_lagged() const noexcept {
    return kind_ == TryRecvErrorKind::lagged;
  }
  [[nodiscard]] constexpr std::uint64_t skipped() const noexcept {
    return skipped_;
  }
  [[nodiscard]] constexpr std::uint64_t lagged() const noexcept {
    return skipped_;
  }

  [[nodiscard]] std::string message() const {
    if (is_empty()) {
      return "channel empty";
    }
    if (is_closed()) {
      return "channel closed";
    }
    return "channel lagged by " + std::to_string(skipped_);
  }

  [[nodiscard]] std::string debug_string() const {
    if (is_empty()) {
      return "Empty";
    }
    if (is_closed()) {
      return "Closed";
    }
    return "Lagged(" + std::to_string(skipped_) + ")";
  }

  friend constexpr bool operator==(TryRecvError,
                                   TryRecvError) noexcept = default;

private:
  constexpr TryRecvError(TryRecvErrorKind kind,
                         std::uint64_t skipped) noexcept
      : kind_{kind}, skipped_{skipped} {}

  TryRecvErrorKind kind_;
  std::uint64_t skipped_;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &stream,
                               const SendError<T> &error) {
  return stream << error.message();
}

inline std::ostream &operator<<(std::ostream &stream,
                               const RecvError &error) {
  return stream << error.message();
}

inline std::ostream &operator<<(std::ostream &stream,
                               const TryRecvError &error) {
  return stream << error.message();
}

} // namespace error

template <typename T> class Sender;
template <typename T> class WeakSender;
template <typename T> class Receiver;

template <typename T>
[[nodiscard]] std::pair<Sender<T>, Receiver<T>>
channel(std::size_t capacity);

} // namespace cio::sync::broadcast

namespace cio::detail {

template <typename T>
concept BroadcastReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::same_as<std::remove_reference_t<decltype(value.get())>,
                        typename T::type>;
};

template <typename T> struct BroadcastBorrowedView : std::false_type {};

template <typename Element, std::size_t Extent>
struct BroadcastBorrowedView<std::span<Element, Extent>> : std::true_type {};

template <typename Character, typename Traits>
struct BroadcastBorrowedView<std::basic_string_view<Character, Traits>>
    : std::true_type {};

template <typename T>
concept BroadcastOwnedValue =
    std::is_object_v<T> && !std::is_pointer_v<T> &&
    !BroadcastReferenceWrapperLike<std::remove_cv_t<T>> &&
    !BroadcastBorrowedView<std::remove_cv_t<T>>::value;

inline void broadcast_checked_increment(std::size_t &count,
                                        std::string_view message) {
  if (count == std::numeric_limits<std::size_t>::max()) {
    throw std::length_error{std::string{message}};
  }
  ++count;
}

inline std::size_t broadcast_effective_capacity(std::size_t requested) {
  if (requested == 0) {
    throw std::invalid_argument{"broadcast channel capacity cannot be zero"};
  }
  if (requested > (std::numeric_limits<std::size_t>::max() >> 1U)) {
    throw std::length_error{
        "broadcast channel capacity exceeded size_t::max / 2"};
  }
  return std::bit_ceil(requested);
}

/**
 * 单条广播消息的拥有节点。
 *
 * 多个 Receiver 可以跨线程复制同一个非 Sync 的 T。节点自身的递归互斥量只
 * 串行化用户复制/失败返还，不持有 channel 状态锁，因此复制重入 channel 查询
 * 不会死锁。
 */
template <typename T> class BroadcastMessage final {
public:
  explicit BroadcastMessage(T value) : value_{std::move(value)} {}

  [[nodiscard]] T clone_value() const requires std::copy_constructible<T> {
    std::lock_guard lock{copy_mutex_};
    return T{value_};
  }

  [[nodiscard]] T take_value() {
    std::lock_guard lock{copy_mutex_};
    return T{std::move(value_)};
  }

private:
  mutable std::recursive_mutex copy_mutex_;
  T value_;
};

enum class BroadcastReceiveStatus {
  pending,
  value,
  closed,
  lagged,
};

template <typename T> struct BroadcastReceiveAttempt final {
  BroadcastReceiveStatus status{BroadcastReceiveStatus::pending};
  std::shared_ptr<BroadcastMessage<T>> message;
  std::uint64_t skipped{0};
  // 保持到 Receiver 游标锁释放之后再析构，禁止用户 T 析构重入游标临界区。
  std::shared_ptr<BroadcastMessage<T>> retired;
};

template <typename T> class BroadcastState final {
public:
  struct Entry final {
    std::uint64_t sequence{0};
    std::size_t remaining{0};
    std::shared_ptr<BroadcastMessage<T>> message;
  };

  BroadcastState(std::size_t capacity, std::size_t receiver_count)
      : capacity_{broadcast_effective_capacity(capacity)},
        receiver_count_{receiver_count} {}

  BroadcastState(const BroadcastState &) = delete;
  BroadcastState &operator=(const BroadcastState &) = delete;

  /**
   * 在单一状态锁临界区线性化 Receiver 数、序号和发布。
   */
  [[nodiscard]] std::size_t
  publish(const std::shared_ptr<BroadcastMessage<T>> &candidate,
          bool &published) {
    std::shared_ptr<BroadcastMessage<T>> evicted;
    std::size_t receivers = 0;
    {
      std::lock_guard lock{mutex_};
      receivers = receiver_count_;
      if (receivers == 0) {
        published = false;
        return 0;
      }

      entries_.push_back(
          Entry{tail_, receivers, candidate});
      tail_ = tail_ + std::uint64_t{1};
      if (entries_.size() > capacity_) {
        evicted = std::move(entries_.front().message);
        entries_.pop_front();
      }
      published = true;
    }
    receiver_notify_.notify_waiters();
    // evicted 与 candidate 的最后 owner 均只能在 channel 锁外析构。
    return receivers;
  }

  [[nodiscard]] std::uint64_t add_receiver_at_tail() {
    std::lock_guard lock{mutex_};
    broadcast_checked_increment(receiver_count_,
                                "broadcast Receiver 计数溢出");
    return tail_;
  }

  void drop_receiver(std::uint64_t cursor) noexcept {
    std::list<Entry> released;
    bool last = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (receiver_count_ == 0) {
          std::terminate();
        }

        const auto distance = tail_ - cursor;
        for (auto &entry : entries_) {
          const auto offset = entry.sequence - cursor;
          if (offset < distance && entry.remaining != 0) {
            --entry.remaining;
          }
        }
        collect_consumed_prefix_locked(released);

        --receiver_count_;
        last = receiver_count_ == 0;
      }
      if (last) {
        last_receiver_notify_.notify_waiters();
      }
      // released 在状态锁外析构用户 T。
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] BroadcastReceiveStatus
  probe(std::uint64_t cursor) const {
    std::lock_guard lock{mutex_};
    const auto distance = tail_ - cursor;
    if (distance > capacity_) {
      return BroadcastReceiveStatus::lagged;
    }
    if (distance != 0) {
      return BroadcastReceiveStatus::value;
    }
    return senders_closed_ ? BroadcastReceiveStatus::closed
                           : BroadcastReceiveStatus::pending;
  }

  [[nodiscard]] BroadcastReceiveAttempt<T>
  take(std::uint64_t &cursor) {
    BroadcastReceiveAttempt<T> result;
    {
      std::lock_guard lock{mutex_};
      const auto distance = tail_ - cursor;
      if (distance > capacity_) {
        const auto skipped = distance - capacity_;
        cursor = cursor + skipped;
        return BroadcastReceiveAttempt<T>{
            BroadcastReceiveStatus::lagged, {}, skipped};
      }
      if (distance == 0) {
        return BroadcastReceiveAttempt<T>{
            senders_closed_ ? BroadcastReceiveStatus::closed
                            : BroadcastReceiveStatus::pending,
            {}, 0};
      }

      for (auto &entry : entries_) {
        if (entry.sequence != cursor) {
          continue;
        }
        if (entry.remaining == 0 || !entry.message) {
          throw std::logic_error{
              "broadcast 环形槽与 Receiver 游标不一致"};
        }
        result = BroadcastReceiveAttempt<T>{
            BroadcastReceiveStatus::value, entry.message, 0};
        cursor = cursor + std::uint64_t{1};
        --entry.remaining;
        if (!entries_.empty() && entries_.front().remaining == 0) {
          result.retired = std::move(entries_.front().message);
          entries_.pop_front();
        }
        break;
      }

      if (result.status != BroadcastReceiveStatus::value) {
        throw std::logic_error{"broadcast 保留消息槽缺失"};
      }
    }
    return result;
  }

  [[nodiscard]] std::size_t sender_len() const {
    std::lock_guard lock{mutex_};
    std::size_t count = 0;
    for (const auto &entry : entries_) {
      if (entry.remaining != 0) {
        ++count;
      }
    }
    return count;
  }

  [[nodiscard]] std::size_t receiver_len(std::uint64_t cursor) const {
    std::lock_guard lock{mutex_};
    return static_cast<std::size_t>(tail_ - cursor);
  }

  [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

  [[nodiscard]] std::size_t receiver_count() const {
    std::lock_guard lock{mutex_};
    return receiver_count_;
  }

  [[nodiscard]] bool senders_closed() const {
    std::lock_guard lock{mutex_};
    return senders_closed_;
  }

  [[nodiscard]] bool clone_sender_if_alive() {
    std::lock_guard lock{mutex_};
    if (senders_closed_) {
      return false;
    }
    broadcast_checked_increment(strong_sender_count_,
                                "broadcast Sender 计数溢出");
    return true;
  }

  void drop_sender() noexcept {
    bool close = false;
    try {
      {
        std::lock_guard lock{mutex_};
        if (strong_sender_count_ == 0) {
          std::terminate();
        }
        --strong_sender_count_;
        close =
            strong_sender_count_ == 0 && sender_borrow_count_ == 0;
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
    if (senders_closed_) {
      throw std::logic_error{
          "已关闭的 broadcast Sender 不能创建 closed operation"};
    }
    broadcast_checked_increment(
        sender_borrow_count_,
        "broadcast Sender operation 计数溢出");
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
        close =
            strong_sender_count_ == 0 && sender_borrow_count_ == 0;
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

  void add_weak_sender() {
    std::lock_guard lock{mutex_};
    broadcast_checked_increment(weak_sender_count_,
                                "broadcast WeakSender 计数溢出");
  }

  void drop_weak_sender() noexcept {
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

  [[nodiscard]] sync::Notify::Notified receiver_notification() const {
    return receiver_notify_.notified_owned();
  }

  [[nodiscard]] sync::Notify::Notified last_receiver_notification() const {
    return last_receiver_notify_.notified_owned();
  }

private:
  void collect_consumed_prefix_locked(
      std::list<Entry> &released) noexcept {
    while (!entries_.empty() && entries_.front().remaining == 0) {
      released.splice(released.end(), entries_, entries_.begin());
    }
  }

  const std::size_t capacity_;
  mutable std::mutex mutex_;
  mutable sync::Notify receiver_notify_;
  mutable sync::Notify last_receiver_notify_;
  std::list<Entry> entries_;
  std::uint64_t tail_{0};
  std::size_t receiver_count_{0};
  std::size_t strong_sender_count_{1};
  std::size_t sender_borrow_count_{0};
  std::size_t weak_sender_count_{0};
  bool senders_closed_{false};
};

template <typename T> class BroadcastReceiverLease final {
public:
  BroadcastReceiverLease(std::shared_ptr<BroadcastState<T>> state,
                         std::uint64_t cursor) noexcept
      : state_{std::move(state)}, cursor_{cursor} {}

  BroadcastReceiverLease(const BroadcastReceiverLease &) = delete;
  BroadcastReceiverLease &operator=(const BroadcastReceiverLease &) = delete;

  ~BroadcastReceiverLease() {
    std::uint64_t cursor = 0;
    try {
      std::lock_guard lock{cursor_mutex_};
      cursor = cursor_;
    } catch (...) {
      std::terminate();
    }
    state_->drop_receiver(cursor);
  }

  void begin_receive() {
    std::lock_guard lock{cursor_mutex_};
    if (operation_active_) {
      throw std::logic_error{
          "同一 broadcast Receiver 不能并发接收"};
    }
    operation_active_ = true;
  }

  void end_receive() noexcept {
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

  [[nodiscard]] BroadcastReceiveStatus probe() const {
    std::lock_guard lock{cursor_mutex_};
    ensure_active_locked();
    return state_->probe(cursor_);
  }

  [[nodiscard]] BroadcastReceiveAttempt<T> take() {
    std::lock_guard lock{cursor_mutex_};
    ensure_active_locked();
    return state_->take(cursor_);
  }

  [[nodiscard]] std::size_t len() const {
    std::lock_guard lock{cursor_mutex_};
    return state_->receiver_len(cursor_);
  }

  [[nodiscard]] const std::shared_ptr<BroadcastState<T>> &
  state() const noexcept {
    return state_;
  }

private:
  void ensure_active_locked() const {
    if (!operation_active_) {
      throw std::logic_error{"broadcast Receiver operation 已取消"};
    }
  }

  std::shared_ptr<BroadcastState<T>> state_;
  mutable std::mutex cursor_mutex_;
  std::uint64_t cursor_{0};
  bool operation_active_{false};
};

template <typename T> class BroadcastReceiveOperation final {
public:
  explicit BroadcastReceiveOperation(
      std::shared_ptr<BroadcastReceiverLease<T>> receiver)
      : receiver_{std::move(receiver)} {
    receiver_->begin_receive();
  }

  BroadcastReceiveOperation(const BroadcastReceiveOperation &) = delete;
  BroadcastReceiveOperation &
  operator=(const BroadcastReceiveOperation &) = delete;

  ~BroadcastReceiveOperation() { receiver_->end_receive(); }

  [[nodiscard]] const std::shared_ptr<BroadcastReceiverLease<T>> &
  receiver() const noexcept {
    return receiver_;
  }

private:
  std::shared_ptr<BroadcastReceiverLease<T>> receiver_;
};

template <typename T> class BroadcastSenderBorrow final {
public:
  explicit BroadcastSenderBorrow(
      std::shared_ptr<BroadcastState<T>> state)
      : state_{std::move(state)} {
    state_->add_sender_borrow();
  }

  BroadcastSenderBorrow(const BroadcastSenderBorrow &) = delete;
  BroadcastSenderBorrow &operator=(const BroadcastSenderBorrow &) = delete;

  ~BroadcastSenderBorrow() { state_->drop_sender_borrow(); }

  [[nodiscard]] const std::shared_ptr<BroadcastState<T>> &
  state() const noexcept {
    return state_;
  }

private:
  std::shared_ptr<BroadcastState<T>> state_;
};

template <typename T>
[[nodiscard]] std::shared_ptr<BroadcastReceiverLease<T>>
broadcast_new_receiver(const std::shared_ptr<BroadcastState<T>> &state) {
  const auto cursor = state->add_receiver_at_tail();
  try {
    return std::make_shared<BroadcastReceiverLease<T>>(state, cursor);
  } catch (...) {
    state->drop_receiver(cursor);
    throw;
  }
}

template <typename T>
Task<Result<T, sync::broadcast::error::RecvError>>
broadcast_recv_task(
    std::shared_ptr<BroadcastReceiveOperation<T>> operation)
    requires std::copy_constructible<T> {
  for (;;) {
    auto notified =
        operation->receiver()->state()->receiver_notification();
    if (operation->receiver()->probe() !=
        BroadcastReceiveStatus::pending) {
      // ready value、lag 与 closed 在提交游标前统一经过 cooperative gate。
      co_await task::consume_budget();
      auto attempt = operation->receiver()->take();
      if (attempt.status == BroadcastReceiveStatus::value) {
        co_return Result<T, sync::broadcast::error::RecvError>::success(
            attempt.message->clone_value());
      }
      if (attempt.status == BroadcastReceiveStatus::lagged) {
        co_return Result<T, sync::broadcast::error::RecvError>::failure(
            sync::broadcast::error::RecvError::lagged(attempt.skipped));
      }
      if (attempt.status == BroadcastReceiveStatus::closed) {
        co_return Result<T, sync::broadcast::error::RecvError>::failure(
            sync::broadcast::error::RecvError::closed());
      }
      continue;
    }
    co_await std::move(notified);
  }
}

} // namespace cio::detail

namespace cio::sync::broadcast {

/**
 * broadcast 多生产者发送句柄。
 *
 * 端点仅拥有共享状态；所有用户 T 构造、移动、复制、析构和 task 唤醒均在
 * channel 状态锁外发生。
 */
template <typename T> class Sender final {
public:
  static_assert(detail::BroadcastOwnedValue<T>,
                "broadcast<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

  /** 创建没有初始 Receiver 的 Sender。 */
  [[nodiscard]] static Sender new_sender(std::size_t capacity) {
    return Sender{
        std::make_shared<detail::BroadcastState<T>>(capacity, 0)};
  }

  Sender(const Sender &other) : state_{other.valid_state()} {
    if (!state_->clone_sender_if_alive()) {
      throw std::logic_error{"已关闭的 broadcast Sender 不能复制"};
    }
  }

  Sender &operator=(const Sender &other) {
    if (this == &other) {
      return *this;
    }
    auto replacement = other.valid_state();
    if (!replacement->clone_sender_if_alive()) {
      throw std::logic_error{"已关闭的 broadcast Sender 不能复制"};
    }
    reset();
    state_ = std::move(replacement);
    return *this;
  }

  Sender(Sender &&) noexcept = default;

  Sender &operator=(Sender &&other) noexcept {
    if (this != &other) {
      reset();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~Sender() { reset(); }

  /**
   * 向发送线性化点存在的全部 Receiver 发布值并返回 Receiver 数。
   */
  [[nodiscard]] Result<std::size_t, error::SendError<T>>
  send(T value) const {
    auto state = valid_state();
    auto candidate =
        std::make_shared<detail::BroadcastMessage<T>>(std::move(value));
    bool published = false;
    const auto receivers = state->publish(candidate, published);
    if (!published) {
      auto returned = candidate->take_value();
      candidate.reset();
      return Result<std::size_t, error::SendError<T>>::failure(
          error::SendError<T>{std::move(returned)});
    }
    candidate.reset();
    return Result<std::size_t, error::SendError<T>>::success(receivers);
  }

  /** 从当前 tail 创建 Receiver，不接收订阅前的历史值。 */
  [[nodiscard]] Receiver<T> subscribe() const {
    return Receiver<T>{
        detail::broadcast_new_receiver<T>(valid_state())};
  }

  /** 创建不维持发送方向存活的 WeakSender。 */
  [[nodiscard]] WeakSender<T> downgrade() const {
    auto state = valid_state();
    state->add_weak_sender();
    return WeakSender<T>{std::move(state)};
  }

  /** 等待 Receiver 数重新观察为零；取消不会改变公开 Sender 计数。 */
  [[nodiscard]] Task<void> closed() const {
    return closed_impl(
        std::make_shared<detail::BroadcastSenderBorrow<T>>(valid_state()));
  }

  /** 返回环中仍有至少一个原订阅者未领取的消息数。 */
  [[nodiscard]] std::size_t len() const {
    return valid_state()->sender_len();
  }

  [[nodiscard]] bool is_empty() const { return len() == 0; }
  [[nodiscard]] std::size_t receiver_count() const {
    return valid_state()->receiver_count();
  }
  [[nodiscard]] std::size_t strong_count() const {
    return valid_state()->strong_sender_count();
  }
  [[nodiscard]] std::size_t weak_count() const {
    return valid_state()->weak_sender_count();
  }

  [[nodiscard]] bool same_channel(const Sender &other) const noexcept {
    return state_ && other.state_ && state_ == other.state_;
  }

private:
  explicit Sender(std::shared_ptr<detail::BroadcastState<T>> state) noexcept
      : state_{std::move(state)} {}

  static Task<void>
  closed_impl(std::shared_ptr<detail::BroadcastSenderBorrow<T>> borrow) {
    for (;;) {
      auto notified =
          borrow->state()->last_receiver_notification();
      if (borrow->state()->receiver_count() == 0) {
        co_return;
      }
      co_await std::move(notified);
    }
  }

  [[nodiscard]] std::shared_ptr<detail::BroadcastState<T>>
  valid_state() const {
    if (!state_) {
      throw std::logic_error{"broadcast Sender 已移出"};
    }
    return state_;
  }

  void reset() noexcept {
    if (state_) {
      state_->drop_sender();
      state_.reset();
    }
  }

  std::shared_ptr<detail::BroadcastState<T>> state_;

  friend class WeakSender<T>;
  friend std::pair<Sender<T>, Receiver<T>>
  channel<T>(std::size_t);
};

/**
 * 不维持 broadcast 发送方向的弱句柄。
 */
template <typename T> class WeakSender final {
public:
  static_assert(detail::BroadcastOwnedValue<T>,
                "broadcast<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

  WeakSender(const WeakSender &other) : state_{other.valid_state()} {
    state_->add_weak_sender();
  }

  WeakSender &operator=(const WeakSender &other) {
    if (this == &other) {
      return *this;
    }
    auto replacement = other.valid_state();
    replacement->add_weak_sender();
    reset();
    state_ = std::move(replacement);
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

  /** 仅当仍有强 Sender 时升级；强计数归零后永久失败。 */
  [[nodiscard]] std::optional<Sender<T>> upgrade() const {
    auto state = valid_state();
    if (!state->clone_sender_if_alive()) {
      return std::nullopt;
    }
    return Sender<T>{std::move(state)};
  }

  [[nodiscard]] std::size_t strong_count() const {
    return valid_state()->strong_sender_count();
  }
  [[nodiscard]] std::size_t weak_count() const {
    return valid_state()->weak_sender_count();
  }

private:
  explicit WeakSender(
      std::shared_ptr<detail::BroadcastState<T>> state) noexcept
      : state_{std::move(state)} {}

  [[nodiscard]] std::shared_ptr<detail::BroadcastState<T>>
  valid_state() const {
    if (!state_) {
      throw std::logic_error{"broadcast WeakSender 已移出"};
    }
    return state_;
  }

  void reset() noexcept {
    if (state_) {
      state_->drop_weak_sender();
      state_.reset();
    }
  }

  std::shared_ptr<detail::BroadcastState<T>> state_;

  friend class Sender<T>;
};

/**
 * broadcast 独立游标接收端。
 *
 * Receiver 只能移动。异步 operation 拥有内部 lease，不捕获调用方 `this`；
 * 同一 Receiver 同时最多一个 recv/try_recv/blocking_recv。
 */
template <typename T> class Receiver final {
public:
  static_assert(detail::BroadcastOwnedValue<T>,
                "broadcast<T> 禁止裸指针、引用或非拥有引用包装");

  static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
  static constexpr bool cio_sync = cio_send;

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
   * 异步接收下一值。value、lag、closed ready 路径均消耗 cooperative budget；
   * 在提交游标前取消不领取消息。复制异常时游标已经推进。
   */
  [[nodiscard]] Task<Result<T, error::RecvError>>
  recv() requires std::copy_constructible<T> {
    ensure_valid();
    auto operation =
        std::make_shared<detail::BroadcastReceiveOperation<T>>(lease_);
    return detail::broadcast_recv_task<T>(std::move(operation));
  }

  /**
   * 不挂起地接收下一值。复制在 channel 锁外串行执行；复制异常时游标已推进。
   */
  [[nodiscard]] Result<T, error::TryRecvError>
  try_recv() requires std::copy_constructible<T> {
    ensure_valid();
    detail::BroadcastReceiveOperation<T> operation{lease_};
    auto attempt = operation.receiver()->take();
    if (attempt.status == detail::BroadcastReceiveStatus::value) {
      return Result<T, error::TryRecvError>::success(
          attempt.message->clone_value());
    }
    if (attempt.status == detail::BroadcastReceiveStatus::lagged) {
      return Result<T, error::TryRecvError>::failure(
          error::TryRecvError::lagged(attempt.skipped));
    }
    if (attempt.status == detail::BroadcastReceiveStatus::closed) {
      return Result<T, error::TryRecvError>::failure(
          error::TryRecvError::closed());
    }
    return Result<T, error::TryRecvError>::failure(
        error::TryRecvError::empty());
  }

  /** 普通线程阻塞桥接；runtime worker 内调用会抛出 logic_error。 */
  [[nodiscard]] Result<T, error::RecvError>
  blocking_recv() requires std::copy_constructible<T> {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{
          "broadcast Receiver::blocking_recv 不能在 CIO 异步执行上下文中调用"};
    }
    runtime::Runtime runtime;
    return runtime.block_on(recv());
  }

  /** 从当前 tail 创建新游标，不继承当前 Receiver backlog。 */
  [[nodiscard]] Receiver
  resubscribe() const requires std::copy_constructible<T> {
    ensure_valid();
    return Receiver{
        detail::broadcast_new_receiver<T>(lease_->state())};
  }

  /** 返回 tail 与本 Receiver 游标的 wrapping 距离，可大于容量。 */
  [[nodiscard]] std::size_t len() const {
    ensure_valid();
    return lease_->len();
  }

  [[nodiscard]] bool is_empty() const { return len() == 0; }
  [[nodiscard]] std::size_t sender_strong_count() const {
    ensure_valid();
    return lease_->state()->strong_sender_count();
  }
  [[nodiscard]] std::size_t sender_weak_count() const {
    ensure_valid();
    return lease_->state()->weak_sender_count();
  }
  [[nodiscard]] bool is_closed() const {
    ensure_valid();
    return lease_->state()->senders_closed();
  }

  [[nodiscard]] bool same_channel(const Receiver &other) const noexcept {
    return lease_ && other.lease_ &&
           lease_->state() == other.lease_->state();
  }

private:
  explicit Receiver(
      std::shared_ptr<detail::BroadcastReceiverLease<T>> lease) noexcept
      : lease_{std::move(lease)} {}

  void ensure_valid() const {
    if (!lease_) {
      throw std::logic_error{"broadcast Receiver 已移出"};
    }
  }

  std::shared_ptr<detail::BroadcastReceiverLease<T>> lease_;

  friend class Sender<T>;
  friend std::pair<Sender<T>, Receiver<T>>
  channel<T>(std::size_t);
};

/**
 * 创建一个 Sender 和从序号零开始的 Receiver。
 *
 * capacity 大于零且不超过 size_t 最大值的一半；非 2 的幂向上取整。
 */
template <typename T>
[[nodiscard]] std::pair<Sender<T>, Receiver<T>>
channel(std::size_t capacity) {
  static_assert(detail::BroadcastOwnedValue<T>,
                "broadcast<T> 禁止裸指针、引用或非拥有引用包装");
  auto state =
      std::make_shared<detail::BroadcastState<T>>(capacity, 1);
  auto lease =
      std::make_shared<detail::BroadcastReceiverLease<T>>(state, 0);
  return {Sender<T>{state}, Receiver<T>{std::move(lease)}};
}

} // namespace cio::sync::broadcast
