#pragma once

#include <algorithm>
#include <coroutine>
#include <cstdint>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/send.hpp"

namespace cio::detail {

enum class NotifyOneOrder {
  fifo,
  lifo,
};

enum class NotifyMark {
  none,
  one_fifo,
  one_lifo,
  all,
};

class NotifyWaitOperation;

class NotifyState final : public std::enable_shared_from_this<NotifyState> {
public:
  NotifyState() = default;
  NotifyState(const NotifyState &) = delete;
  NotifyState &operator=(const NotifyState &) = delete;

  std::shared_ptr<NotifyWaitOperation> make_operation();
  void notify_one(NotifyOneOrder order);
  void notify_waiters();

  bool enable(const std::shared_ptr<NotifyWaitOperation> &operation);
  bool suspend(const std::shared_ptr<NotifyWaitOperation> &operation,
               std::shared_ptr<ExecutionContext> context,
               CoroutineRef coroutine);
  void consume(NotifyWaitOperation &operation);
  void cancel(NotifyWaitOperation &operation) noexcept;

private:
  struct WaiterEntry final {
    std::uint64_t id{0};
    std::weak_ptr<NotifyWaitOperation> operation;
    std::shared_ptr<NotifyWaitOperation> retained_operation;
  };

  using WaiterList = std::list<WaiterEntry>;

  bool complete_if_ready_locked(NotifyWaitOperation &operation);
  std::shared_ptr<ExecutionContext> notify_one_locked(NotifyOneOrder order,
                                                      WaiterList &retired);
  void remove_waiter_locked(std::uint64_t id, WaiterList &retired) noexcept;

  std::mutex mutex_;
  bool permit_{false};
  std::uint64_t notify_waiters_generation_{0};
  std::uint64_t next_waiter_id_{1};
  WaiterList waiters_;
};

class NotifyWaitOperation final
    : public std::enable_shared_from_this<NotifyWaitOperation> {
public:
  NotifyWaitOperation(std::shared_ptr<NotifyState> state, std::uint64_t id,
                      std::uint64_t notify_waiters_generation) noexcept
      : state_{std::move(state)}, id_{id},
        notify_waiters_generation_{notify_waiters_generation} {}

  NotifyWaitOperation(const NotifyWaitOperation &) = delete;
  NotifyWaitOperation &operator=(const NotifyWaitOperation &) = delete;

  ~NotifyWaitOperation() { state_->cancel(*this); }

  bool enable() { return state_->enable(shared_from_this()); }

  bool suspend(std::shared_ptr<ExecutionContext> context,
               CoroutineRef coroutine) {
    return state_->suspend(shared_from_this(), std::move(context), coroutine);
  }

  void consume() { state_->consume(*this); }

private:
  std::shared_ptr<NotifyState> state_;
  std::uint64_t id_{0};
  std::uint64_t notify_waiters_generation_{0};
  bool registered_{false};
  bool completed_{false};
  bool cancelled_{false};
  NotifyMark notification_{NotifyMark::none};
  std::shared_ptr<ExecutionContext> context_;

  friend class NotifyState;
};

inline std::shared_ptr<NotifyWaitOperation> NotifyState::make_operation() {
  std::uint64_t id = 0;
  std::uint64_t generation = 0;
  {
    std::lock_guard lock{mutex_};
    id = next_waiter_id_++;
    if (id == 0) {
      std::terminate();
    }
    generation = notify_waiters_generation_;
  }
  return std::make_shared<NotifyWaitOperation>(shared_from_this(), id,
                                               generation);
}

inline bool
NotifyState::complete_if_ready_locked(NotifyWaitOperation &operation) {
  if (operation.completed_) {
    return true;
  }
  if (operation.notification_ != NotifyMark::none) {
    operation.notification_ = NotifyMark::none;
    operation.completed_ = true;
    operation.context_.reset();
    return true;
  }
  if (operation.notify_waiters_generation_ != notify_waiters_generation_) {
    operation.completed_ = true;
    operation.context_.reset();
    return true;
  }
  if (permit_) {
    permit_ = false;
    operation.completed_ = true;
    operation.context_.reset();
    return true;
  }
  return false;
}

inline bool
NotifyState::enable(const std::shared_ptr<NotifyWaitOperation> &operation) {
  std::lock_guard lock{mutex_};
  if (operation->cancelled_) {
    throw std::logic_error{"Notified 已取消"};
  }
  if (complete_if_ready_locked(*operation)) {
    return true;
  }
  if (!operation->registered_) {
    waiters_.push_back(WaiterEntry{operation->id_, operation});
    operation->registered_ = true;
  }
  return false;
}

inline bool
NotifyState::suspend(const std::shared_ptr<NotifyWaitOperation> &operation,
                     std::shared_ptr<ExecutionContext> context,
                     CoroutineRef coroutine) {
  std::lock_guard lock{mutex_};
  if (operation->cancelled_) {
    throw std::logic_error{"Notified 已取消"};
  }
  if (complete_if_ready_locked(*operation)) {
    return false;
  }
  if (!operation->registered_) {
    waiters_.push_back(WaiterEntry{operation->id_, operation});
    operation->registered_ = true;
  }
  if (operation->context_) {
    throw std::logic_error{"同一 Notified 不能被多个 task 同时等待"};
  }
  context->park(coroutine);
  operation->context_ = std::move(context);
  return true;
}

inline void NotifyState::consume(NotifyWaitOperation &operation) {
  std::lock_guard lock{mutex_};
  if (operation.cancelled_) {
    throw std::logic_error{"Notified 已取消"};
  }
  if (operation.completed_) {
    return;
  }
  if (!complete_if_ready_locked(operation)) {
    throw std::logic_error{"Notified 在未收到通知时恢复"};
  }
}

inline void NotifyState::remove_waiter_locked(std::uint64_t id,
                                              WaiterList &retired) noexcept {
  const auto position =
      std::find_if(waiters_.begin(), waiters_.end(),
                   [id](const WaiterEntry &entry) { return entry.id == id; });
  if (position != waiters_.end()) {
    retired.splice(retired.end(), waiters_, position);
  }
}

inline std::shared_ptr<ExecutionContext>
NotifyState::notify_one_locked(NotifyOneOrder order, WaiterList &retired) {
  while (!waiters_.empty()) {
    auto position = waiters_.begin();
    if (order == NotifyOneOrder::lifo) {
      position = std::prev(waiters_.end());
    }
    retired.splice(retired.end(), waiters_, position);
    auto &entry = retired.back();

    // weak_ptr 提升出的强引用保存在已分配的 waiter 节点中；retired 的生命周期
    // 跨越 mutex_ 临界区，因此最后一个 owner 不会在持锁时析构并重入 cancel。
    entry.retained_operation = entry.operation.lock();
    const auto &operation = entry.retained_operation;
    if (!operation || !operation->registered_ || operation->cancelled_ ||
        operation->completed_) {
      continue;
    }
    operation->registered_ = false;
    operation->notification_ = order == NotifyOneOrder::fifo
                                   ? NotifyMark::one_fifo
                                   : NotifyMark::one_lifo;
    return std::move(operation->context_);
  }

  permit_ = true;
  return {};
}

inline void NotifyState::notify_one(NotifyOneOrder order) {
  WaiterList retired;
  std::shared_ptr<ExecutionContext> context;
  {
    std::lock_guard lock{mutex_};
    context = notify_one_locked(order, retired);
  }
  if (context) {
    context->wake();
  }
}

inline void NotifyState::notify_waiters() {
  WaiterList retired;
  {
    std::lock_guard lock{mutex_};
    ++notify_waiters_generation_;
    retired.splice(retired.end(), waiters_);
    for (auto &entry : retired) {
      entry.retained_operation = entry.operation.lock();
      const auto &operation = entry.retained_operation;
      if (operation && operation->registered_ && !operation->cancelled_ &&
          !operation->completed_) {
        operation->registered_ = false;
        operation->notification_ = NotifyMark::all;
      }
    }
  }

  for (const auto &entry : retired) {
    std::shared_ptr<ExecutionContext> context;
    {
      std::lock_guard lock{mutex_};
      const auto &operation = entry.retained_operation;
      if (operation && operation->notification_ == NotifyMark::all) {
        context = std::move(operation->context_);
      }
    }
    if (context) {
      context->wake();
    }
  }
}

inline void NotifyState::cancel(NotifyWaitOperation &operation) noexcept {
  WaiterList retired;
  std::shared_ptr<ExecutionContext> transfer;
  {
    std::lock_guard lock{mutex_};
    if (operation.cancelled_) {
      return;
    }
    operation.cancelled_ = true;
    operation.context_.reset();
    if (operation.registered_) {
      remove_waiter_locked(operation.id_, retired);
      operation.registered_ = false;
    }

    if (operation.notification_ == NotifyMark::one_fifo) {
      operation.notification_ = NotifyMark::none;
      transfer = notify_one_locked(NotifyOneOrder::fifo, retired);
    } else if (operation.notification_ == NotifyMark::one_lifo) {
      operation.notification_ = NotifyMark::none;
      transfer = notify_one_locked(NotifyOneOrder::lifo, retired);
    }
  }
  if (transfer) {
    transfer->wake();
  }
}

} // namespace cio::detail

namespace cio::sync {

/**
 * Tokio 风格的单 permit 异步通知器。
 *
 * Notify 是共享值句柄；复制等价于复制 Rust `Arc<Notify>`。notify_one 最多保存
 * 一个 permit，notify_waiters 只通知在调用前已创建的 Notified。所有等待状态
 * 都拥有自身生命周期，不捕获裸引用。
 */
class Notify final {
public:
  Notify() : state_{std::make_shared<detail::NotifyState>()} {}

  class Notified final {
  public:
    Notified(const Notified &) = delete;
    Notified &operator=(const Notified &) = delete;
    Notified(Notified &&) noexcept = default;
    Notified &operator=(Notified &&) noexcept = default;
    ~Notified() = default;

    /**
     * 在首次 await 前进入 FIFO 等待队列。
     *
     * 返回 true 表示已经消费 permit 或观察到 notify_waiters；返回 false 表示已
     * 排队。取消尚未收到通知的等待会失去队列位置。
     */
    bool enable() {
      ensure_valid();
      return operation_->enable();
    }

    class Awaiter final {
    public:
      explicit Awaiter(
          std::shared_ptr<detail::NotifyWaitOperation> operation) noexcept
          : operation_{std::move(operation)} {}

      bool await_ready() {
        ensure_valid();
        return operation_->enable();
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        return operation_->suspend(detail::require_execution_context(),
                                   detail::CoroutineRef::from_abi(coroutine));
      }

      void await_resume() {
        ensure_valid();
        operation_->consume();
      }

    private:
      void ensure_valid() const {
        if (!operation_) {
          throw std::logic_error{"Notified 已移出"};
        }
      }

      std::shared_ptr<detail::NotifyWaitOperation> operation_;
    };

    Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{operation_};
    }

  private:
    explicit Notified(
        std::shared_ptr<detail::NotifyWaitOperation> operation) noexcept
        : operation_{std::move(operation)} {}

    void ensure_valid() const {
      if (!operation_) {
        throw std::logic_error{"Notified 已移出"};
      }
    }

    std::shared_ptr<detail::NotifyWaitOperation> operation_;

    friend class Notify;
  };

  [[nodiscard]] Notified notified() const {
    return Notified{state_->make_operation()};
  }

  [[nodiscard]] Notified notified_owned() const { return notified(); }

  void notify_one() const { state_->notify_one(detail::NotifyOneOrder::fifo); }

  void notify_last() const { state_->notify_one(detail::NotifyOneOrder::lifo); }

  void notify_waiters() const { state_->notify_waiters(); }

private:
  std::shared_ptr<detail::NotifyState> state_;
};

} // namespace cio::sync

namespace cio {

template <> struct send_traits<sync::Notify> : std::true_type {};

template <> struct sync_traits<sync::Notify> : std::true_type {};

template <> struct send_traits<sync::Notify::Notified> : std::true_type {};

} // namespace cio
