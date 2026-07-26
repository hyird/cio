#pragma once

#include <concepts>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "cio/result.hpp"
#include "cio/runtime/runtime.hpp"
#include "cio/send.hpp"
#include "cio/sync/semaphore.hpp"
#include "cio/sync/try_lock_error.hpp"
#include "cio/task/task.hpp"

namespace cio::sync {

template <typename T> class RwLock;

template <typename T, typename U = T> class RwLockReadGuard;

template <typename T> class RwLockWriteGuard;

template <typename T, typename U> class RwLockMappedWriteGuard;

} // namespace cio::sync

namespace cio::detail {

inline constexpr std::uint32_t rwlock_max_readers =
    std::numeric_limits<std::uint32_t>::max() >> 3U;

template <typename Projection, typename Value>
using rwlock_read_projection_value_t = std::remove_cv_t<
    std::remove_reference_t<std::invoke_result_t<Projection &, const Value &>>>;

template <typename Projection, typename Value>
concept RwLockReadProjection =
    std::invocable<std::remove_cvref_t<Projection> &, const Value &> &&
    std::is_lvalue_reference_v<
        std::invoke_result_t<std::remove_cvref_t<Projection> &, const Value &>>;

template <typename Predicate, typename Value>
concept RwLockReadMapPredicate =
    std::predicate<std::remove_cvref_t<Predicate> &, const Value &>;

template <typename Projection, typename Value>
using rwlock_write_projection_value_t =
    std::remove_reference_t<std::invoke_result_t<Projection &, Value &>>;

template <typename Projection, typename Value>
concept RwLockWriteProjection =
    std::invocable<std::remove_cvref_t<Projection> &, Value &> &&
    std::is_lvalue_reference_v<
        std::invoke_result_t<std::remove_cvref_t<Projection> &, Value &>> &&
    !std::is_const_v<rwlock_write_projection_value_t<
        std::remove_cvref_t<Projection>, Value>>;

template <typename Predicate, typename Value>
concept RwLockWriteMapPredicate =
    std::predicate<std::remove_cvref_t<Predicate> &, Value &>;

template <typename T> class RwLockState final {
public:
  RwLockState(T value, std::uint32_t max_readers)
      : semaphore_{validated_max_readers(max_readers)},
        max_readers_{max_readers}, value_{std::move(value)} {}

  RwLockState(const RwLockState &) = delete;
  RwLockState &operator=(const RwLockState &) = delete;

  static std::uint32_t validated_max_readers(std::uint32_t max_readers) {
    if (max_readers == 0) {
      throw std::invalid_argument{"RwLock 最大并发读者数量不能为零"};
    }
    if (max_readers > rwlock_max_readers) {
      throw std::invalid_argument{
          "RwLock 最大并发读者数量超过 Tokio 1.53.1 上限"};
    }
    return max_readers;
  }

  sync::Semaphore semaphore_;
  std::uint32_t max_readers_;
  T value_;
};

template <typename T> struct RwLockGuardAccess;

template <typename T>
Task<sync::RwLockReadGuard<T>> blocking_rwlock_read(sync::RwLock<T> rwlock);

template <typename T>
Task<sync::RwLockWriteGuard<T>> blocking_rwlock_write(sync::RwLock<T> rwlock);

} // namespace cio::detail

namespace cio::sync {

/**
 * 拥有 RwLock 共享状态和一个读许可的 RAII guard。
 *
 * `U` 是映射后的只读视图类型；根 guard 的 `U` 等于 `T`。映射时只执行
 * 一次投影，并由共享所有权的别名视图维持根对象和稳定子对象的生命周期；
 * 公开 API 不接收、暴露或以裸指针表达所有权。
 */
template <typename T, typename U> class RwLockReadGuard final {
public:
  RwLockReadGuard(const RwLockReadGuard &) = delete;
  RwLockReadGuard &operator=(const RwLockReadGuard &) = delete;
  RwLockReadGuard(RwLockReadGuard &&) noexcept = default;
  RwLockReadGuard &operator=(RwLockReadGuard &&) noexcept = default;
  ~RwLockReadGuard() = default;

  [[nodiscard]] const U &get() const {
    ensure_valid();
    return *view_;
  }

  [[nodiscard]] const U &operator*() const { return get(); }

  /**
   * 返回来源 RwLock 的共享值句柄。
   */
  [[nodiscard]] RwLock<T> rwlock() const;

  /**
   * 映射到稳定只读子对象。
   *
   * projection 只同步执行一次，且必须返回由当前受保护 T 拥有的子对象左值。
   * 返回 guard 用共享所有权的别名视图稳定保存该选择，不保存用户 callable。
   */
  template <typename Projection>
    requires detail::RwLockReadProjection<Projection, U>
  [[nodiscard]] static auto map(RwLockReadGuard guard, Projection projection)
      -> RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                std::remove_cvref_t<Projection>, U>>;

  /**
   * 条件只读映射；失败时错误分支保存原 guard。
   */
  template <typename Predicate, typename Projection>
    requires(detail::RwLockReadMapPredicate<Predicate, U> &&
             detail::RwLockReadProjection<Projection, U>)
  [[nodiscard]] static auto try_map(RwLockReadGuard guard, Predicate predicate,
                                    Projection projection)
      -> Result<RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                       std::remove_cvref_t<Projection>, U>>,
                RwLockReadGuard>;

private:
  RwLockReadGuard(std::shared_ptr<detail::RwLockState<T>> state,
                  SemaphorePermit permit,
                  std::shared_ptr<const U> view = {}) noexcept
      : state_{std::move(state)}, permit_{std::move(permit)},
        view_{std::move(view)} {
    if constexpr (std::same_as<T, U>) {
      if (!view_ && state_) {
        // aliasing shared_ptr 仅在此同步表达式中取得子对象地址；公开 API
        // 不暴露裸地址，生命周期和所有权始终由 state_ 的控制块维持。
        view_ =
            std::shared_ptr<const U>{state_, std::addressof(state_->value_)};
      }
    }
  }

  void ensure_valid() const {
    if (!state_ || !view_) {
      throw std::logic_error{"RwLockReadGuard 已移出"};
    }
  }

  std::shared_ptr<detail::RwLockState<T>> state_;
  SemaphorePermit permit_;
  std::shared_ptr<const U> view_;

  friend struct detail::RwLockGuardAccess<T>;
  template <typename, typename> friend class RwLockReadGuard;
  template <typename> friend class RwLockWriteGuard;
};

template <typename T, typename U = T>
using OwnedRwLockReadGuard = RwLockReadGuard<T, U>;

/**
 * 拥有 RwLock 全部读许可的排他写 guard。
 *
 * guard 可跨协程暂停点和 worker 迁移；异常展开、取消和协程销毁都会通过
 * `SemaphorePermit` 且只归还一次全部许可。
 */
template <typename T> class RwLockWriteGuard final {
public:
  RwLockWriteGuard(const RwLockWriteGuard &) = delete;
  RwLockWriteGuard &operator=(const RwLockWriteGuard &) = delete;
  RwLockWriteGuard(RwLockWriteGuard &&) noexcept = default;
  RwLockWriteGuard &operator=(RwLockWriteGuard &&) noexcept = default;
  ~RwLockWriteGuard() = default;

  [[nodiscard]] T &get() {
    ensure_valid();
    return state_->value_;
  }

  [[nodiscard]] const T &get() const {
    ensure_valid();
    return state_->value_;
  }

  [[nodiscard]] T &operator*() { return get(); }

  [[nodiscard]] const T &operator*() const { return get(); }

  [[nodiscard]] RwLock<T> rwlock() const;

  template <typename Projection>
    requires detail::RwLockWriteProjection<Projection, T>
  [[nodiscard]] static auto map(RwLockWriteGuard guard, Projection projection)
      -> RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                       std::remove_cvref_t<Projection>, T>>;

  template <typename Predicate, typename Projection>
    requires(detail::RwLockWriteMapPredicate<Predicate, T> &&
             detail::RwLockWriteProjection<Projection, T>)
  [[nodiscard]] static auto try_map(RwLockWriteGuard guard, Predicate predicate,
                                    Projection projection)
      -> Result<
          RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                        std::remove_cvref_t<Projection>, T>>,
          RwLockWriteGuard>;

  /**
   * 原子降级为根对象读 guard。
   *
   * 转换期间始终保留一个许可，任何排队写者都不能在中间取得排他访问。
   */
  [[nodiscard]] static RwLockReadGuard<T> downgrade(RwLockWriteGuard guard);

  /**
   * 在仍持排他访问时选取只读子对象并原子降级。
   */
  template <typename Projection>
    requires detail::RwLockReadProjection<Projection, T>
  [[nodiscard]] static auto downgrade_map(RwLockWriteGuard guard,
                                          Projection projection)
      -> RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                std::remove_cvref_t<Projection>, T>>;

  template <typename Predicate, typename Projection>
    requires(detail::RwLockReadMapPredicate<Predicate, T> &&
             detail::RwLockReadProjection<Projection, T>)
  [[nodiscard]] static auto try_downgrade_map(RwLockWriteGuard guard,
                                              Predicate predicate,
                                              Projection projection)
      -> Result<RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                       std::remove_cvref_t<Projection>, T>>,
                RwLockWriteGuard>;

  [[nodiscard]] static RwLockMappedWriteGuard<T, T>
  into_mapped(RwLockWriteGuard guard);

private:
  RwLockWriteGuard(std::shared_ptr<detail::RwLockState<T>> state,
                   SemaphorePermit permit) noexcept
      : state_{std::move(state)}, permit_{std::move(permit)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"RwLockWriteGuard 已移出"};
    }
  }

  std::shared_ptr<detail::RwLockState<T>> state_;
  SemaphorePermit permit_;

  friend struct detail::RwLockGuardAccess<T>;
  template <typename, typename> friend class RwLockMappedWriteGuard;
};

template <typename T> using OwnedRwLockWriteGuard = RwLockWriteGuard<T>;

/**
 * 由共享所有权别名视图保存稳定子对象的排他 mapped write guard。
 *
 * guard 不保存用户 projection，也不接收或暴露裸指针；别名视图与根状态共享
 * 控制块，写许可覆盖其整个生命周期。
 */
template <typename T, typename U> class RwLockMappedWriteGuard final {
public:
  RwLockMappedWriteGuard(const RwLockMappedWriteGuard &) = delete;
  RwLockMappedWriteGuard &operator=(const RwLockMappedWriteGuard &) = delete;
  RwLockMappedWriteGuard(RwLockMappedWriteGuard &&) noexcept = default;
  RwLockMappedWriteGuard &
  operator=(RwLockMappedWriteGuard &&) noexcept = default;
  ~RwLockMappedWriteGuard() = default;

  [[nodiscard]] U &get() {
    ensure_valid();
    return *view_;
  }

  [[nodiscard]] const U &get() const {
    ensure_valid();
    return *view_;
  }

  [[nodiscard]] U &operator*() { return get(); }

  [[nodiscard]] const U &operator*() const { return get(); }

  [[nodiscard]] RwLock<T> rwlock() const;

  template <typename Projection>
    requires detail::RwLockWriteProjection<Projection, U>
  [[nodiscard]] static auto map(RwLockMappedWriteGuard guard,
                                Projection projection)
      -> RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                       std::remove_cvref_t<Projection>, U>>;

  template <typename Predicate, typename Projection>
    requires(detail::RwLockWriteMapPredicate<Predicate, U> &&
             detail::RwLockWriteProjection<Projection, U>)
  [[nodiscard]] static auto try_map(RwLockMappedWriteGuard guard,
                                    Predicate predicate, Projection projection)
      -> Result<
          RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                        std::remove_cvref_t<Projection>, U>>,
          RwLockMappedWriteGuard>;

private:
  RwLockMappedWriteGuard(std::shared_ptr<detail::RwLockState<T>> state,
                         SemaphorePermit permit, std::shared_ptr<U> view)
      : state_{std::move(state)}, permit_{std::move(permit)},
        view_{std::move(view)} {}

  void ensure_valid() const {
    if (!state_ || !view_) {
      throw std::logic_error{"RwLockMappedWriteGuard 已移出"};
    }
  }

  std::shared_ptr<detail::RwLockState<T>> state_;
  SemaphorePermit permit_;
  std::shared_ptr<U> view_;

  template <typename> friend class RwLockWriteGuard;
  template <typename, typename> friend class RwLockMappedWriteGuard;
};

template <typename T, typename U = T>
using OwnedRwLockMappedWriteGuard = RwLockMappedWriteGuard<T, U>;

/**
 * Tokio 风格的 FIFO、写者优先异步 RwLock。
 *
 * 每个读者获取一个公平 Semaphore 许可；写者获取 `max_readers` 个许可。
 * 因批量请求位于同一 FIFO 队列，已排队写者会阻止后续读者插队。RwLock 是
 * 可复制共享值句柄，复制等价于 `Arc<RwLock<T>>`。
 */
template <typename T> class RwLock final {
public:
  explicit RwLock(T value)
      : state_{std::make_shared<detail::RwLockState<T>>(
            std::move(value), detail::rwlock_max_readers)} {}

  RwLock()
    requires std::default_initializable<T>
      : RwLock{T{}} {}

  RwLock(const RwLock &) noexcept = default;
  RwLock &operator=(const RwLock &) noexcept = default;
  RwLock(RwLock &&) noexcept = default;
  RwLock &operator=(RwLock &&) noexcept = default;
  ~RwLock() = default;

  [[nodiscard]] static RwLock with_max_readers(T value,
                                               std::uint32_t max_readers) {
    return RwLock{std::make_shared<detail::RwLockState<T>>(std::move(value),
                                                           max_readers)};
  }

  /**
   * C++20 运行期兼容工厂；共享控制块不能常量求值。
   */
  [[nodiscard]] static RwLock const_new(T value) {
    return RwLock{std::move(value)};
  }

  [[nodiscard]] static RwLock
  const_with_max_readers(T value, std::uint32_t max_readers) {
    return with_max_readers(std::move(value), max_readers);
  }

  class Read final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value &&
                                     sync_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Read(const Read &) = delete;
    Read &operator=(const Read &) = delete;
    Read(Read &&) noexcept = default;
    Read &operator=(Read &&) noexcept = default;
    ~Read() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Read::cio_send;
      static constexpr bool cio_sync = false;

      Awaiter(std::shared_ptr<detail::RwLockState<T>> state,
              const Semaphore::Acquire &acquisition)
          : state_{std::move(state)},
            semaphore_awaiter_{acquisition.operator co_await()} {}

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        return semaphore_awaiter_.await_ready();
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        return semaphore_awaiter_.await_suspend(coroutine);
      }

      RwLockReadGuard<T> await_resume();

    private:
      void ensure_valid() const {
        if (!state_) {
          throw std::logic_error{"RwLock Read awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::RwLockState<T>> state_;
      Semaphore::Acquire::Awaiter semaphore_awaiter_;
    };

    [[nodiscard]] Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{state_, acquisition_};
    }

  private:
    Read(std::shared_ptr<detail::RwLockState<T>> state,
         Semaphore::Acquire acquisition) noexcept
        : state_{std::move(state)}, acquisition_{std::move(acquisition)} {}

    void ensure_valid() const {
      if (!state_) {
        throw std::logic_error{"RwLock Read 已移出"};
      }
    }

    std::shared_ptr<detail::RwLockState<T>> state_;
    Semaphore::Acquire acquisition_;

    friend class RwLock;
  };

  class Write final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value &&
                                     sync_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Write(const Write &) = delete;
    Write &operator=(const Write &) = delete;
    Write(Write &&) noexcept = default;
    Write &operator=(Write &&) noexcept = default;
    ~Write() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Write::cio_send;
      static constexpr bool cio_sync = false;

      Awaiter(std::shared_ptr<detail::RwLockState<T>> state,
              const Semaphore::Acquire &acquisition)
          : state_{std::move(state)},
            semaphore_awaiter_{acquisition.operator co_await()} {}

      [[nodiscard]] bool await_ready() {
        ensure_valid();
        return semaphore_awaiter_.await_ready();
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> coroutine) {
        ensure_valid();
        return semaphore_awaiter_.await_suspend(coroutine);
      }

      RwLockWriteGuard<T> await_resume();

    private:
      void ensure_valid() const {
        if (!state_) {
          throw std::logic_error{"RwLock Write awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::RwLockState<T>> state_;
      Semaphore::Acquire::Awaiter semaphore_awaiter_;
    };

    [[nodiscard]] Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{state_, acquisition_};
    }

  private:
    Write(std::shared_ptr<detail::RwLockState<T>> state,
          Semaphore::Acquire acquisition) noexcept
        : state_{std::move(state)}, acquisition_{std::move(acquisition)} {}

    void ensure_valid() const {
      if (!state_) {
        throw std::logic_error{"RwLock Write 已移出"};
      }
    }

    std::shared_ptr<detail::RwLockState<T>> state_;
    Semaphore::Acquire acquisition_;

    friend class RwLock;
  };

  /**
   * FIFO 异步取得共享读访问。
   *
   * 返回的 future 与成功 guard 都共享持有锁状态；调用者无需保证原 RwLock
   * 句柄继续存活。等待期间取消会失去队列位置并安全转交已分配许可。guard
   * 可随 Send task 跨 worker 迁移，等待只挂起 task，绝不阻塞 runtime worker。
   *
   * 已排队写者会阻止本操作插队。
   */
  [[nodiscard]] Read read() const {
    ensure_valid();
    return Read{state_, state_->semaphore_.acquire_owned()};
  }

  /**
   * 拥有式异步取得共享读访问。
   *
   * CIO 的 RwLock 本身是共享值句柄，因此本 API 与 read 的所有权语义一致：
   * future 和成功 guard 均拥有锁状态。取消会失去 FIFO 队列位置但不会泄漏
   * 许可；Send task 可跨 worker 恢复；竞争时只挂起 task，不阻塞 worker。
   */
  [[nodiscard]] Read read_owned() const { return read(); }

  /**
   * FIFO 异步取得排他写访问。
   *
   * 返回的 future 与成功 guard 都共享持有锁状态，不借用调用者。取消会失去
   * 队列位置并安全转交已经部分取得的许可；guard 可随 Send task 跨 worker
   * 迁移；竞争等待仅挂起 task，绝不阻塞 runtime worker。
   */
  [[nodiscard]] Write write() const {
    ensure_valid();
    return Write{state_,
                 state_->semaphore_.acquire_many_owned(state_->max_readers_)};
  }

  /**
   * 拥有式异步取得排他写访问。
   *
   * CIO 的 RwLock 本身是共享值句柄，因此本 API 与 write 的所有权语义一致：
   * future 和成功 guard 均拥有锁状态。取消会失去 FIFO 队列位置但保持许可
   * 守恒；Send task 可跨 worker 恢复；竞争时只挂起 task，不阻塞 worker。
   */
  [[nodiscard]] Write write_owned() const { return write(); }

  [[nodiscard]] Result<RwLockReadGuard<T>, TryLockError> try_read() const {
    ensure_valid();
    auto acquired = state_->semaphore_.try_acquire_owned();
    if (!acquired.has_value()) {
      return Result<RwLockReadGuard<T>, TryLockError>::failure(TryLockError{});
    }
    return Result<RwLockReadGuard<T>, TryLockError>::success(
        detail::RwLockGuardAccess<T>::make_read(state_,
                                                std::move(acquired).value()));
  }

  [[nodiscard]] Result<OwnedRwLockReadGuard<T>, TryLockError>
  try_read_owned() const {
    return try_read();
  }

  [[nodiscard]] Result<RwLockWriteGuard<T>, TryLockError> try_write() const {
    ensure_valid();
    auto acquired =
        state_->semaphore_.try_acquire_many_owned(state_->max_readers_);
    if (!acquired.has_value()) {
      return Result<RwLockWriteGuard<T>, TryLockError>::failure(TryLockError{});
    }
    return Result<RwLockWriteGuard<T>, TryLockError>::success(
        detail::RwLockGuardAccess<T>::make_write(state_,
                                                 std::move(acquired).value()));
  }

  [[nodiscard]] Result<OwnedRwLockWriteGuard<T>, TryLockError>
  try_write_owned() const {
    return try_write();
  }

  /**
   * 在同步线程阻塞取得读访问；异步执行上下文中调用会抛出。
   */
  [[nodiscard]] RwLockReadGuard<T> blocking_read() const;

  [[nodiscard]] OwnedRwLockReadGuard<T> blocking_read_owned() const {
    return blocking_read();
  }

  /**
   * 在同步线程阻塞取得写访问；异步执行上下文中调用会抛出。
   */
  [[nodiscard]] RwLockWriteGuard<T> blocking_write() const;

  [[nodiscard]] OwnedRwLockWriteGuard<T> blocking_write_owned() const {
    return blocking_write();
  }

  /**
   * 取得受全许可保护的同步独占可变 guard。
   *
   * Rust 可用 `&mut self` 在编译期维持返回引用的独占期，C++20 无法证明该
   * 借用关系。CIO 因此返回拥有全部许可的 guard：即使随后复制 RwLock
   * 句柄，其他访问也必须等待 guard 析构。调用时若存在其他锁句柄、等待者
   * 或 guard，会抛出 `std::logic_error`；本操作不阻塞且不发生协程暂停。
   */
  [[nodiscard]] RwLockWriteGuard<T> get_mut() {
    ensure_unique("RwLock::get_mut 要求唯一共享句柄");
    auto acquired =
        state_->semaphore_.try_acquire_many_owned(state_->max_readers_);
    if (!acquired.has_value()) {
      throw std::logic_error{"RwLock::get_mut 无法取得全部许可"};
    }
    return detail::RwLockGuardAccess<T>::make_write(
        state_, std::move(acquired).value());
  }

  [[nodiscard]] T into_inner() && {
    ensure_unique("RwLock::into_inner 要求唯一共享句柄");
    auto state = std::move(state_);
    return T{std::move(state->value_)};
  }

private:
  explicit RwLock(std::shared_ptr<detail::RwLockState<T>> state) noexcept
      : state_{std::move(state)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"RwLock 已移出"};
    }
  }

  void ensure_unique(std::string_view message) const {
    ensure_valid();
    if (state_.use_count() != 1) {
      throw std::logic_error{std::string{message}};
    }
  }

  std::shared_ptr<detail::RwLockState<T>> state_;

  friend struct detail::RwLockGuardAccess<T>;
  template <typename, typename> friend class RwLockReadGuard;
  template <typename> friend class RwLockWriteGuard;
  template <typename, typename> friend class RwLockMappedWriteGuard;
};

} // namespace cio::sync

namespace cio::detail {

template <typename T> struct RwLockGuardAccess final {
  static sync::RwLockReadGuard<T>
  make_read(std::shared_ptr<RwLockState<T>> state,
            sync::SemaphorePermit permit) {
    return sync::RwLockReadGuard<T>{std::move(state), std::move(permit)};
  }

  static sync::RwLockWriteGuard<T>
  make_write(std::shared_ptr<RwLockState<T>> state,
             sync::SemaphorePermit permit) {
    return sync::RwLockWriteGuard<T>{std::move(state), std::move(permit)};
  }
};

template <typename T>
Task<sync::RwLockReadGuard<T>> blocking_rwlock_read(sync::RwLock<T> rwlock) {
  co_return co_await rwlock.read_owned();
}

template <typename T>
Task<sync::RwLockWriteGuard<T>> blocking_rwlock_write(sync::RwLock<T> rwlock) {
  co_return co_await rwlock.write_owned();
}

} // namespace cio::detail

namespace cio::sync {

template <typename T>
RwLockReadGuard<T> RwLock<T>::Read::Awaiter::await_resume() {
  auto acquired = semaphore_awaiter_.await_resume();
  if (!acquired.has_value()) {
    std::terminate();
  }
  return detail::RwLockGuardAccess<T>::make_read(std::move(state_),
                                                 std::move(acquired).value());
}

template <typename T>
RwLockWriteGuard<T> RwLock<T>::Write::Awaiter::await_resume() {
  auto acquired = semaphore_awaiter_.await_resume();
  if (!acquired.has_value()) {
    std::terminate();
  }
  return detail::RwLockGuardAccess<T>::make_write(std::move(state_),
                                                  std::move(acquired).value());
}

template <typename T> RwLockReadGuard<T> RwLock<T>::blocking_read() const {
  ensure_valid();
  if (detail::active_execution_context) {
    throw std::logic_error{
        "RwLock::blocking_read 不能在 CIO 异步执行上下文中调用"};
  }
  runtime::Runtime runtime;
  return runtime.block_on(detail::blocking_rwlock_read(*this));
}

template <typename T> RwLockWriteGuard<T> RwLock<T>::blocking_write() const {
  ensure_valid();
  if (detail::active_execution_context) {
    throw std::logic_error{
        "RwLock::blocking_write 不能在 CIO 异步执行上下文中调用"};
  }
  runtime::Runtime runtime;
  return runtime.block_on(detail::blocking_rwlock_write(*this));
}

template <typename T, typename U>
RwLock<T> RwLockReadGuard<T, U>::rwlock() const {
  ensure_valid();
  return RwLock<T>{state_};
}

template <typename T> RwLock<T> RwLockWriteGuard<T>::rwlock() const {
  ensure_valid();
  return RwLock<T>{state_};
}

template <typename T, typename U>
RwLock<T> RwLockMappedWriteGuard<T, U>::rwlock() const {
  ensure_valid();
  return RwLock<T>{state_};
}

template <typename T, typename U>
template <typename Projection>
  requires detail::RwLockReadProjection<Projection, U>
auto RwLockReadGuard<T, U>::map(RwLockReadGuard guard, Projection projection)
    -> RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                              std::remove_cvref_t<Projection>, U>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using V = detail::rwlock_read_projection_value_t<StoredProjection, U>;

  guard.ensure_valid();
  const V &selected = std::invoke(projection, guard.get());
  // aliasing shared_ptr 将一次投影得到的子对象绑定到根状态控制块，后续访问
  // 不会再次调用 projection，也不会保存或公开裸地址。
  std::shared_ptr<const V> view{guard.state_, std::addressof(selected)};

  auto state = std::move(guard.state_);
  auto permit = std::move(guard.permit_);
  return RwLockReadGuard<T, V>{std::move(state), std::move(permit),
                               std::move(view)};
}

template <typename T, typename U>
template <typename Predicate, typename Projection>
  requires(detail::RwLockReadMapPredicate<Predicate, U> &&
           detail::RwLockReadProjection<Projection, U>)
auto RwLockReadGuard<T, U>::try_map(RwLockReadGuard guard, Predicate predicate,
                                    Projection projection)
    -> Result<RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                     std::remove_cvref_t<Projection>, U>>,
              RwLockReadGuard> {
  using Mapped = RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                        std::remove_cvref_t<Projection>, U>>;
  guard.ensure_valid();
  if (!std::invoke(predicate, guard.get())) {
    return Result<Mapped, RwLockReadGuard>::failure(std::move(guard));
  }
  return Result<Mapped, RwLockReadGuard>::success(
      map(std::move(guard), std::move(projection)));
}

template <typename T>
template <typename Projection>
  requires detail::RwLockWriteProjection<Projection, T>
auto RwLockWriteGuard<T>::map(RwLockWriteGuard guard, Projection projection)
    -> RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                     std::remove_cvref_t<Projection>, T>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using U = detail::rwlock_write_projection_value_t<StoredProjection, T>;
  guard.ensure_valid();
  U &selected = std::invoke(projection, guard.get());
  std::shared_ptr<U> view{guard.state_, std::addressof(selected)};
  auto state = std::move(guard.state_);
  auto permit = std::move(guard.permit_);
  return RwLockMappedWriteGuard<T, U>{std::move(state), std::move(permit),
                                      std::move(view)};
}

template <typename T>
template <typename Predicate, typename Projection>
  requires(detail::RwLockWriteMapPredicate<Predicate, T> &&
           detail::RwLockWriteProjection<Projection, T>)
auto RwLockWriteGuard<T>::try_map(RwLockWriteGuard guard, Predicate predicate,
                                  Projection projection)
    -> Result<
        RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                      std::remove_cvref_t<Projection>, T>>,
        RwLockWriteGuard> {
  using Mapped =
      RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                    std::remove_cvref_t<Projection>, T>>;
  guard.ensure_valid();
  if (!std::invoke(predicate, guard.get())) {
    return Result<Mapped, RwLockWriteGuard>::failure(std::move(guard));
  }
  return Result<Mapped, RwLockWriteGuard>::success(
      map(std::move(guard), std::move(projection)));
}

template <typename T>
RwLockReadGuard<T> RwLockWriteGuard<T>::downgrade(RwLockWriteGuard guard) {
  guard.ensure_valid();
  auto state = std::move(guard.state_);
  auto permits = std::move(guard.permit_);
  auto read_permit = permits.split(1);
  if (!read_permit.has_value()) {
    std::terminate();
  }
  return RwLockReadGuard<T>{std::move(state), std::move(*read_permit)};
}

template <typename T>
template <typename Projection>
  requires detail::RwLockReadProjection<Projection, T>
auto RwLockWriteGuard<T>::downgrade_map(RwLockWriteGuard guard,
                                        Projection projection)
    -> RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                              std::remove_cvref_t<Projection>, T>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using U = detail::rwlock_read_projection_value_t<StoredProjection, T>;
  guard.ensure_valid();
  const U &selected = std::invoke(projection, std::as_const(guard.get()));
  std::shared_ptr<const U> view{guard.state_, std::addressof(selected)};

  auto state = std::move(guard.state_);
  auto permits = std::move(guard.permit_);
  auto read_permit = permits.split(1);
  if (!read_permit.has_value()) {
    std::terminate();
  }
  return RwLockReadGuard<T, U>{std::move(state), std::move(*read_permit),
                               std::move(view)};
}

template <typename T>
template <typename Predicate, typename Projection>
  requires(detail::RwLockReadMapPredicate<Predicate, T> &&
           detail::RwLockReadProjection<Projection, T>)
auto RwLockWriteGuard<T>::try_downgrade_map(RwLockWriteGuard guard,
                                            Predicate predicate,
                                            Projection projection)
    -> Result<RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                     std::remove_cvref_t<Projection>, T>>,
              RwLockWriteGuard> {
  using ReadGuard = RwLockReadGuard<T, detail::rwlock_read_projection_value_t<
                                           std::remove_cvref_t<Projection>, T>>;
  guard.ensure_valid();
  if (!std::invoke(predicate, std::as_const(guard.get()))) {
    return Result<ReadGuard, RwLockWriteGuard>::failure(std::move(guard));
  }
  return Result<ReadGuard, RwLockWriteGuard>::success(
      downgrade_map(std::move(guard), std::move(projection)));
}

template <typename T>
RwLockMappedWriteGuard<T, T>
RwLockWriteGuard<T>::into_mapped(RwLockWriteGuard guard) {
  return map(std::move(guard), [](T &value) -> T & { return value; });
}

template <typename T, typename U>
template <typename Projection>
  requires detail::RwLockWriteProjection<Projection, U>
auto RwLockMappedWriteGuard<T, U>::map(RwLockMappedWriteGuard guard,
                                       Projection projection)
    -> RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                     std::remove_cvref_t<Projection>, U>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using V = detail::rwlock_write_projection_value_t<StoredProjection, U>;
  guard.ensure_valid();
  V &selected = std::invoke(projection, guard.get());
  std::shared_ptr<V> view{guard.state_, std::addressof(selected)};
  auto state = std::move(guard.state_);
  auto permit = std::move(guard.permit_);
  return RwLockMappedWriteGuard<T, V>{std::move(state), std::move(permit),
                                      std::move(view)};
}

template <typename T, typename U>
template <typename Predicate, typename Projection>
  requires(detail::RwLockWriteMapPredicate<Predicate, U> &&
           detail::RwLockWriteProjection<Projection, U>)
auto RwLockMappedWriteGuard<T, U>::try_map(RwLockMappedWriteGuard guard,
                                           Predicate predicate,
                                           Projection projection)
    -> Result<
        RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                      std::remove_cvref_t<Projection>, U>>,
        RwLockMappedWriteGuard> {
  using Mapped =
      RwLockMappedWriteGuard<T, detail::rwlock_write_projection_value_t<
                                    std::remove_cvref_t<Projection>, U>>;
  guard.ensure_valid();
  if (!std::invoke(predicate, guard.get())) {
    return Result<Mapped, RwLockMappedWriteGuard>::failure(std::move(guard));
  }
  return Result<Mapped, RwLockMappedWriteGuard>::success(
      map(std::move(guard), std::move(projection)));
}

} // namespace cio::sync

namespace cio {

template <typename T>
struct send_traits<sync::RwLock<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::RwLock<T>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value> {};

template <typename T, typename U>
struct send_traits<sync::RwLockReadGuard<T, U>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<U>>::value> {};

template <typename T, typename U>
struct sync_traits<sync::RwLockReadGuard<T, U>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value &&
                         send_traits<std::remove_cv_t<U>>::value &&
                         sync_traits<std::remove_cv_t<U>>::value> {};

template <typename T>
struct send_traits<sync::RwLockWriteGuard<T>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value> {};

template <typename T>
struct sync_traits<sync::RwLockWriteGuard<T>> : std::false_type {};

template <typename T, typename U>
struct send_traits<sync::RwLockMappedWriteGuard<T, U>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value &&
                         send_traits<std::remove_cv_t<U>>::value &&
                         sync_traits<std::remove_cv_t<U>>::value> {};

template <typename T, typename U>
struct sync_traits<sync::RwLockMappedWriteGuard<T, U>> : std::false_type {};

} // namespace cio
