#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

#include "cio/send.hpp"
#include "cio/sync/notify.hpp"

namespace cio::sync {

/**
 * Barrier 一代等待的完成结果。
 *
 * 每一代恰有一个结果是 leader。该值不借用 Barrier 或等待 task，可以自由复制、
 * 跨线程传递并在原 Barrier 析构后继续使用。
 */
class BarrierWaitResult final {
public:
  explicit constexpr BarrierWaitResult(bool leader = false) noexcept
      : leader_{leader} {}

  /**
   * 当前等待是否为本代唯一 leader。
   */
  [[nodiscard]] constexpr bool is_leader() const noexcept { return leader_; }

private:
  bool leader_{false};
};

} // namespace cio::sync

namespace cio::detail {

struct BarrierArrival final {
  bool leader{false};
  std::optional<sync::Notify::Notified> notification;
};

class BarrierState final {
public:
  explicit BarrierState(std::size_t participants) noexcept
      : participants_{participants == 0 ? 1 : participants} {}

  BarrierState(const BarrierState &) = delete;
  BarrierState &operator=(const BarrierState &) = delete;

  [[nodiscard]] BarrierArrival arrive() {
    std::optional<sync::Notify> completed_generation;
    BarrierArrival arrival;
    {
      std::lock_guard lock{mutex_};
      if (arrived_ == participants_ - 1) {
        completed_generation.emplace(generation_notify_);
        generation_notify_ = sync::Notify{};
        arrived_ = 0;
        ++generation_;
        arrival.leader = true;
      } else {
        /*
         * 先创建拥有式通知操作，再提交到达计数。若控制块分配失败，本次 wait
         * 尚未产生可观察的到达，保持强异常安全。
         */
        arrival.notification.emplace(generation_notify_.notified_owned());
        ++arrived_;
      }
    }

    /*
     * 唤醒必须发生在短临界区之外，避免恢复 task 时重入 Barrier 状态锁。
     * 每代使用独立 Notify，因此迟到的旧代 wake 不会影响下一代。
     */
    if (completed_generation) {
      completed_generation->notify_waiters();
    }
    return arrival;
  }

private:
  std::mutex mutex_;
  std::size_t participants_{1};
  std::size_t arrived_{0};
  std::uint64_t generation_{1};
  sync::Notify generation_notify_;
};

class BarrierWaitOperation final {
public:
  explicit BarrierWaitOperation(std::shared_ptr<BarrierState> state) noexcept
      : state_{std::move(state)} {}

  BarrierWaitOperation(const BarrierWaitOperation &) = delete;
  BarrierWaitOperation &operator=(const BarrierWaitOperation &) = delete;

  [[nodiscard]] bool ready() {
    std::lock_guard lock{start_mutex_};
    ensure_valid();
    if (started_) {
      throw std::logic_error{"同一个 Barrier wait 不能等待两次"};
    }
    started_ = true;

    auto arrival = state_->arrive();
    leader_ = arrival.leader;
    if (leader_) {
      return true;
    }

    notification_awaiter_.emplace(arrival.notification->operator co_await());
    return notification_awaiter_->await_ready();
  }

  template <typename Promise>
  bool suspend(std::coroutine_handle<Promise> coroutine) {
    ensure_started_waiter();
    return notification_awaiter_->await_suspend(coroutine);
  }

  [[nodiscard]] sync::BarrierWaitResult resume() {
    ensure_valid();
    if (!started_) {
      throw std::logic_error{"Barrier wait 尚未开始"};
    }
    if (!leader_) {
      ensure_started_waiter();
      notification_awaiter_->await_resume();
    }
    return sync::BarrierWaitResult{leader_};
  }

private:
  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"Barrier wait 已移出"};
    }
  }

  void ensure_started_waiter() const {
    ensure_valid();
    if (!started_ || leader_ || !notification_awaiter_) {
      throw std::logic_error{"Barrier wait 状态无效"};
    }
  }

  std::shared_ptr<BarrierState> state_;
  std::mutex start_mutex_;
  bool started_{false};
  bool leader_{false};
  std::optional<sync::Notify::Notified::Awaiter> notification_awaiter_;
};

} // namespace cio::detail

namespace cio::sync {

/**
 * Tokio 1.53.1 风格的可复用异步屏障。
 *
 * Barrier 是共享值句柄；复制等价于复制 Rust `Arc<Barrier>`。`n - 1` 个 task
 * 等待，第 `n` 个到达者释放本代全部等待者，并成为唯一 leader；`n == 0`
 * 与 Tokio 一样按 `n == 1` 处理。内部同步锁只覆盖有限状态更新且绝不跨
 * 协程暂停点，竞争等待只挂起 task，不阻塞 runtime worker。
 */
class Barrier final {
public:
  explicit Barrier(std::size_t participants)
      : state_{std::make_shared<detail::BarrierState>(participants)} {}

  class Wait final {
  public:
    static constexpr bool cio_send = true;
    static constexpr bool cio_sync = false;

    Wait(const Wait &) = delete;
    Wait &operator=(const Wait &) = delete;
    Wait(Wait &&) noexcept = default;
    Wait &operator=(Wait &&) noexcept = default;
    ~Wait() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = true;
      static constexpr bool cio_sync = false;

      explicit Awaiter(
          std::shared_ptr<detail::BarrierWaitOperation> operation) noexcept
          : operation_{std::move(operation)} {}

      Awaiter(const Awaiter &) = delete;
      Awaiter &operator=(const Awaiter &) = delete;
      Awaiter(Awaiter &&) noexcept = default;
      Awaiter &operator=(Awaiter &&) noexcept = default;
      ~Awaiter() = default;

      /**
       * 首次 poll 才把当前 task 计入屏障；创建 Wait 本身没有副作用。
       */
      [[nodiscard]] bool await_ready() {
        ensure_valid();
        return operation_->ready();
      }

      /**
       * 未凑齐本代参与者时挂起当前 task；恢复允许发生在任意 runtime worker。
       */
      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        return operation_->suspend(coroutine);
      }

      /**
       * 返回本代 leader 标记；结果不借用 Barrier。
       */
      [[nodiscard]] BarrierWaitResult await_resume() {
        ensure_valid();
        return operation_->resume();
      }

    private:
      void ensure_valid() const {
        if (!operation_) {
          throw std::logic_error{"Barrier awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::BarrierWaitOperation> operation_;
    };

    /**
     * 取得拥有式 awaiter。多个 awaiter 不能消费同一个 Wait。
     */
    [[nodiscard]] Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{operation_};
    }

  private:
    explicit Wait(
        std::shared_ptr<detail::BarrierWaitOperation> operation) noexcept
        : operation_{std::move(operation)} {}

    void ensure_valid() const {
      if (!operation_) {
        throw std::logic_error{"Barrier Wait 已移出"};
      }
    }

    std::shared_ptr<detail::BarrierWaitOperation> operation_;

    friend class Barrier;
  };

  /**
   * 等待当前一代全部参与者到达。
   *
   * 返回的 Wait 拥有 Barrier 状态，不要求原句柄继续存活；等待只挂起 task，
   * 不阻塞 worker，恢复后可以迁移到其他 worker。
   *
   * 取消安全：与 Tokio 1.53.1 一致，本操作不是 cancel-safe。首次 poll 计入
   * 到达数后，若 task 被取消或析构，该到达不会回滚，也不会由下一位等待者
   * 补领队列位置；后续到达者仍可能据此完成当前一代。尚未首次 poll 的 Wait
   * 被析构不会改变屏障。
   */
  [[nodiscard]] Wait wait() const {
    ensure_valid();
    return Wait{std::make_shared<detail::BarrierWaitOperation>(state_)};
  }

private:
  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"Barrier 已移出"};
    }
  }

  std::shared_ptr<detail::BarrierState> state_;
};

} // namespace cio::sync

namespace cio {

template <> struct send_traits<sync::Barrier> : std::true_type {};

template <> struct sync_traits<sync::Barrier> : std::true_type {};

template <> struct send_traits<sync::BarrierWaitResult> : std::true_type {};

template <> struct sync_traits<sync::BarrierWaitResult> : std::true_type {};

template <> struct send_traits<sync::Barrier::Wait> : std::true_type {};

} // namespace cio
