#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include "cio/detail/coroutine.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/result.hpp"
#include "cio/send.hpp"

namespace cio::detail {

class SemaphoreState;
class SemaphoreAcquireOperation;

} // namespace cio::detail

namespace cio::sync {

class Semaphore;

/**
 * Semaphore 异步获取失败。
 *
 * Tokio 1.53.1 中异步获取只在 semaphore 已关闭时失败。
 */
class AcquireError final {
public:
  [[nodiscard]] constexpr bool is_closed() const noexcept { return true; }

  [[nodiscard]] constexpr std::string_view message() const noexcept {
    return "semaphore 已关闭";
  }

  friend constexpr bool operator==(AcquireError,
                                   AcquireError) noexcept = default;
};

/**
 * Semaphore 非阻塞获取失败原因。
 */
enum class TryAcquireError {
  closed,
  no_permits,
};

[[nodiscard]] constexpr std::string_view
message(TryAcquireError error) noexcept {
  return error == TryAcquireError::closed ? std::string_view{"semaphore 已关闭"}
                                          : std::string_view{"没有足够许可"};
}

/**
 * 拥有 semaphore 共享状态的 RAII permit。
 *
 * guard 可以跨协程暂停点和 worker 线程迁移；析构会且只会归还一次许可。CIO
 * 不提供保存裸引用的借用 guard，因此该类型同时承担 Tokio
 * `SemaphorePermit` 与 `OwnedSemaphorePermit` 的安全能力。
 */
class SemaphorePermit final {
public:
  SemaphorePermit(const SemaphorePermit &) = delete;
  SemaphorePermit &operator=(const SemaphorePermit &) = delete;

  SemaphorePermit(SemaphorePermit &&other) noexcept;
  SemaphorePermit &operator=(SemaphorePermit &&other) noexcept;
  ~SemaphorePermit();

  /**
   * 永久消耗当前 guard 的许可，析构时不再归还。
   */
  void forget() noexcept;

  /**
   * 合并同一个 semaphore 的另一 guard。
   *
   * 不同源 guard 会抛出 `std::invalid_argument`；当前 guard 保持不变，被移入
   * 参数的 guard 会按异常展开规则析构并归还自身许可。
   */
  void merge(SemaphorePermit other);

  /**
   * 从当前 guard 拆出 `n` 个许可。
   *
   * 许可不足时返回空值；拆分本身不接触共享可用计数。
   */
  [[nodiscard]] std::optional<SemaphorePermit> split(std::size_t n);

  [[nodiscard]] std::size_t num_permits() const noexcept { return permits_; }

  /**
   * 返回来源 semaphore 的共享值句柄，不暴露可能悬空的引用。
   */
  [[nodiscard]] Semaphore semaphore() const;

private:
  SemaphorePermit(std::shared_ptr<detail::SemaphoreState> state,
                  std::size_t permits) noexcept;

  void release() noexcept;

  std::shared_ptr<detail::SemaphoreState> state_;
  std::size_t permits_{0};

  friend class Semaphore;
  friend class detail::SemaphoreAcquireOperation;
};

using OwnedSemaphorePermit = SemaphorePermit;

} // namespace cio::sync

namespace cio::detail {

enum class SemaphoreCompletion {
  pending,
  acquired,
  closed,
};

enum class SemaphoreTryStatus {
  acquired,
  closed,
  no_permits,
};

class SemaphoreAcquireOperation final
    : public std::enable_shared_from_this<SemaphoreAcquireOperation> {
public:
  SemaphoreAcquireOperation(std::shared_ptr<SemaphoreState> state,
                            std::uint64_t id, std::size_t requested) noexcept;

  SemaphoreAcquireOperation(const SemaphoreAcquireOperation &) = delete;
  SemaphoreAcquireOperation &
  operator=(const SemaphoreAcquireOperation &) = delete;
  ~SemaphoreAcquireOperation();

  [[nodiscard]] bool ready();
  bool suspend(std::shared_ptr<ExecutionContext> context,
               CoroutineRef coroutine);
  Result<sync::SemaphorePermit, sync::AcquireError> resume();

private:
  std::shared_ptr<SemaphoreState> state_;
  std::uint64_t id_{0};
  std::size_t requested_{0};
  std::size_t remaining_{0};
  bool registered_{false};
  bool consumed_{false};
  bool cancelled_{false};
  SemaphoreCompletion completion_{SemaphoreCompletion::pending};
  std::shared_ptr<ExecutionContext> context_;

  friend class SemaphoreState;
};

class SemaphoreState final
    : public std::enable_shared_from_this<SemaphoreState> {
public:
  static constexpr std::size_t max_permits =
      std::numeric_limits<std::size_t>::max() >> 3U;

  explicit SemaphoreState(std::size_t permits);
  SemaphoreState(const SemaphoreState &) = delete;
  SemaphoreState &operator=(const SemaphoreState &) = delete;

  [[nodiscard]] std::shared_ptr<SemaphoreAcquireOperation>
  make_operation(std::size_t requested);

  [[nodiscard]] std::size_t available_permits() const;
  [[nodiscard]] bool is_closed() const;
  [[nodiscard]] SemaphoreTryStatus try_acquire(std::size_t requested);
  void add_permits(std::size_t permits);
  [[nodiscard]] std::size_t forget_permits(std::size_t permits);
  void close();
  void release_permit_noexcept(std::size_t permits) noexcept;

  [[nodiscard]] bool
  ready(const std::shared_ptr<SemaphoreAcquireOperation> &operation);
  bool suspend(const std::shared_ptr<SemaphoreAcquireOperation> &operation,
               std::shared_ptr<ExecutionContext> context,
               CoroutineRef coroutine);
  [[nodiscard]] bool consume(SemaphoreAcquireOperation &operation);
  void cancel(SemaphoreAcquireOperation &operation) noexcept;

private:
  struct WaiterEntry final {
    std::uint64_t id{0};
    std::weak_ptr<SemaphoreAcquireOperation> operation;
  };

  using WakeBatch = std::vector<std::shared_ptr<ExecutionContext>>;
  using OperationKeepAliveBatch =
      std::vector<std::shared_ptr<SemaphoreAcquireOperation>>;

  [[nodiscard]] bool
  ready_locked(const std::shared_ptr<SemaphoreAcquireOperation> &operation);
  void remove_waiter_locked(std::uint64_t id) noexcept;
  void distribute_locked(std::size_t permits, WakeBatch &wakes,
                         OperationKeepAliveBatch &keep_alive);
  void add_permits_impl(std::size_t permits, bool terminate_on_overflow);
  static void wake_all(WakeBatch wakes) noexcept;

  mutable std::mutex mutex_;
  std::size_t available_{0};
  bool closed_{false};
  std::uint64_t next_waiter_id_{1};
  std::deque<WaiterEntry> waiters_;
};

} // namespace cio::detail

namespace cio::sync {

/**
 * Tokio 风格的公平异步计数 semaphore。
 *
 * Semaphore 是可复制共享值句柄，复制等价于 `Arc<Semaphore>`。等待采用 FIFO，
 * 队首批量请求允许形成头阻塞。内部只短暂锁定状态，不执行用户代码、系统 I/O
 * 或无界阻塞；等待会挂起 task，恢复后允许迁移到任意 runtime worker。
 */
class Semaphore final {
public:
  static constexpr std::size_t MAX_PERMITS =
      detail::SemaphoreState::max_permits;

  explicit Semaphore(std::size_t permits);

  /**
   * C++20 运行期兼容工厂。
   *
   * `std::mutex` 与共享控制块不能常量求值，因此该函数不具备 Rust
   * `const_new` 的静态初始化能力。
   */
  [[nodiscard]] static Semaphore const_new(std::size_t permits) {
    return Semaphore{permits};
  }

  class Acquire final {
  public:
    Acquire(const Acquire &) = delete;
    Acquire &operator=(const Acquire &) = delete;
    Acquire(Acquire &&) noexcept = default;
    Acquire &operator=(Acquire &&) noexcept = default;
    ~Acquire() = default;

    class Awaiter final {
    public:
      explicit Awaiter(
          std::shared_ptr<detail::SemaphoreAcquireOperation> operation) noexcept
          : operation_{std::move(operation)} {}

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        return operation_->ready();
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        return operation_->suspend(detail::require_execution_context(),
                                   detail::CoroutineRef::from_abi(coroutine));
      }

      Result<SemaphorePermit, AcquireError> await_resume() {
        ensure_valid();
        return operation_->resume();
      }

    private:
      void ensure_valid() const {
        if (!operation_) {
          throw std::logic_error{"Semaphore 获取操作已移出"};
        }
      }

      std::shared_ptr<detail::SemaphoreAcquireOperation> operation_;
    };

    [[nodiscard]] Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{operation_};
    }

  private:
    explicit Acquire(
        std::shared_ptr<detail::SemaphoreAcquireOperation> operation) noexcept
        : operation_{std::move(operation)} {}

    void ensure_valid() const {
      if (!operation_) {
        throw std::logic_error{"Semaphore 获取操作已移出"};
      }
    }

    std::shared_ptr<detail::SemaphoreAcquireOperation> operation_;

    friend class Semaphore;
  };

  /**
   * 异步获取一个许可。
   *
   * future 拥有共享状态；取消会失去 FIFO 位置，并把已部分分配的许可转交。等待
   * 不阻塞 worker，成功 guard 可跨暂停点和 worker 线程。
   */
  [[nodiscard]] Acquire acquire() const;

  /**
   * 异步获取 `n` 个许可；队首大请求会阻塞后续小请求。
   */
  [[nodiscard]] Acquire acquire_many(std::uint32_t n) const;

  [[nodiscard]] Acquire acquire_owned() const { return acquire(); }

  [[nodiscard]] Acquire acquire_many_owned(std::uint32_t n) const {
    return acquire_many(n);
  }

  [[nodiscard]] Result<SemaphorePermit, TryAcquireError> try_acquire() const;

  [[nodiscard]] Result<SemaphorePermit, TryAcquireError>
  try_acquire_many(std::uint32_t n) const;

  [[nodiscard]] Result<OwnedSemaphorePermit, TryAcquireError>
  try_acquire_owned() const {
    return try_acquire();
  }

  [[nodiscard]] Result<OwnedSemaphorePermit, TryAcquireError>
  try_acquire_many_owned(std::uint32_t n) const {
    return try_acquire_many(n);
  }

  [[nodiscard]] std::size_t available_permits() const {
    return state_->available_permits();
  }

  void add_permits(std::size_t n) const { state_->add_permits(n); }

  [[nodiscard]] std::size_t forget_permits(std::size_t n) const {
    return state_->forget_permits(n);
  }

  void close() const { state_->close(); }

  [[nodiscard]] bool is_closed() const { return state_->is_closed(); }

private:
  explicit Semaphore(std::shared_ptr<detail::SemaphoreState> state) noexcept
      : state_{std::move(state)} {}

  std::shared_ptr<detail::SemaphoreState> state_;

  friend class SemaphorePermit;
};

} // namespace cio::sync

namespace cio {

template <> struct send_traits<sync::Semaphore> : std::true_type {};

template <> struct sync_traits<sync::Semaphore> : std::true_type {};

template <> struct send_traits<sync::SemaphorePermit> : std::true_type {};

template <> struct sync_traits<sync::SemaphorePermit> : std::true_type {};

template <> struct send_traits<sync::Semaphore::Acquire> : std::true_type {};

} // namespace cio
