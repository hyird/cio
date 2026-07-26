#pragma once

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
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

template <typename T> class Mutex;

template <typename T> class MutexGuard;

template <typename T, typename U> class MappedMutexGuard;

} // namespace cio::sync

namespace cio::detail {

template <typename Projection, typename Value>
using mutex_projection_value_t =
    std::remove_reference_t<std::invoke_result_t<Projection &, Value &>>;

template <typename Projection, typename Value>
concept MutexProjection =
    std::invocable<std::remove_cvref_t<Projection> &, Value &> &&
    std::is_lvalue_reference_v<
        std::invoke_result_t<std::remove_cvref_t<Projection> &, Value &>> &&
    !std::is_const_v<
        mutex_projection_value_t<std::remove_cvref_t<Projection>, Value>>;

template <typename Predicate, typename Value>
concept MutexMapPredicate =
    std::predicate<std::remove_cvref_t<Predicate> &, Value &>;

template <typename T> class MutexState final {
public:
  explicit MutexState(T value) : value_{std::move(value)} {}

  MutexState(const MutexState &) = delete;
  MutexState &operator=(const MutexState &) = delete;

  sync::Semaphore semaphore_{1};
  T value_;
};

template <typename T> struct MutexGuardAccess;

template <typename T>
Task<sync::MutexGuard<T>> blocking_mutex_lock(sync::Mutex<T> mutex);

} // namespace cio::detail

namespace cio::sync {

/**
 * 拥有 Mutex 共享状态和唯一锁许可的 RAII guard。
 *
 * guard 可跨协程暂停点和 worker 迁移。引用只在 guard 仍持锁时有效，不得保存到
 * guard 析构之后，也不得捕获到寿命更长的异步工作。
 */
template <typename T> class MutexGuard final {
public:
  MutexGuard(const MutexGuard &) = delete;
  MutexGuard &operator=(const MutexGuard &) = delete;

  MutexGuard(MutexGuard &&other) noexcept
      : state_{std::move(other.state_)}, permit_{std::move(other.permit_)} {}

  MutexGuard &operator=(MutexGuard &&other) noexcept {
    if (this != &other) {
      permit_ = std::move(other.permit_);
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~MutexGuard() = default;

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

  /**
   * 返回来源 Mutex 的共享值句柄。
   */
  [[nodiscard]] Mutex<T> mutex() const;

  /**
   * 把 guard 映射到根对象的稳定子对象。
   *
   * projection 只同步执行一次，且必须返回由当前受保护 T 拥有的子对象左值。
   * 返回 guard 用共享所有权别名视图稳定保存该选择，不保存用户 callable。
   */
  template <typename Projection>
    requires detail::MutexProjection<Projection, T>
  [[nodiscard]] static auto
  map(MutexGuard guard, Projection projection) -> MappedMutexGuard<
      T, detail::mutex_projection_value_t<std::remove_cvref_t<Projection>, T>>;

  /**
   * 条件映射；失败时 Result 的错误分支保存原 guard。
   */
  template <typename Predicate, typename Projection>
    requires(detail::MutexMapPredicate<Predicate, T> &&
             detail::MutexProjection<Projection, T>)
  [[nodiscard]] static auto try_map(MutexGuard guard, Predicate predicate,
                                    Projection projection)
      -> Result<MappedMutexGuard<T, detail::mutex_projection_value_t<
                                        std::remove_cvref_t<Projection>, T>>,
                MutexGuard>;

private:
  MutexGuard(std::shared_ptr<detail::MutexState<T>> state,
             SemaphorePermit permit) noexcept
      : state_{std::move(state)}, permit_{std::move(permit)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"MutexGuard 已移出"};
    }
  }

  std::shared_ptr<detail::MutexState<T>> state_;
  SemaphorePermit permit_;

  friend struct detail::MutexGuardAccess<T>;
  template <typename, typename> friend class MappedMutexGuard;
};

template <typename T> using OwnedMutexGuard = MutexGuard<T>;

/**
 * 由共享所有权别名视图保存稳定子对象的 mapped Mutex guard。
 *
 * projection 在映射调用中只同步执行一次；后续访问不会再次执行用户 callable。
 * 别名视图与根状态共享控制块，guard 仍独占整把 Mutex，且公开 API 不接收或
 * 暴露裸指针。
 */
template <typename T, typename U> class MappedMutexGuard final {
public:
  MappedMutexGuard(const MappedMutexGuard &) = delete;
  MappedMutexGuard &operator=(const MappedMutexGuard &) = delete;

  MappedMutexGuard(MappedMutexGuard &&other) noexcept
      : state_{std::move(other.state_)}, permit_{std::move(other.permit_)},
        view_{std::move(other.view_)} {}

  MappedMutexGuard &operator=(MappedMutexGuard &&other) noexcept {
    if (this != &other) {
      permit_ = std::move(other.permit_);
      state_ = std::move(other.state_);
      view_ = std::move(other.view_);
    }
    return *this;
  }

  ~MappedMutexGuard() = default;

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

  template <typename Projection>
    requires detail::MutexProjection<Projection, U>
  [[nodiscard]] static auto
  map(MappedMutexGuard guard, Projection projection) -> MappedMutexGuard<
      T, detail::mutex_projection_value_t<std::remove_cvref_t<Projection>, U>>;

  template <typename Predicate, typename Projection>
    requires(detail::MutexMapPredicate<Predicate, U> &&
             detail::MutexProjection<Projection, U>)
  [[nodiscard]] static auto try_map(MappedMutexGuard guard, Predicate predicate,
                                    Projection projection)
      -> Result<MappedMutexGuard<T, detail::mutex_projection_value_t<
                                        std::remove_cvref_t<Projection>, U>>,
                MappedMutexGuard>;

private:
  MappedMutexGuard(std::shared_ptr<detail::MutexState<T>> state,
                   SemaphorePermit permit, std::shared_ptr<U> view) noexcept
      : state_{std::move(state)}, permit_{std::move(permit)},
        view_{std::move(view)} {}

  void ensure_valid() const {
    if (!state_ || !view_) {
      throw std::logic_error{"MappedMutexGuard 已移出"};
    }
  }

  std::shared_ptr<detail::MutexState<T>> state_;
  SemaphorePermit permit_;
  std::shared_ptr<U> view_;

  template <typename> friend class MutexGuard;
  template <typename, typename> friend class MappedMutexGuard;
};

template <typename T, typename U = T>
using OwnedMappedMutexGuard = MappedMutexGuard<T, U>;

/**
 * Tokio 风格的严格 FIFO 异步 Mutex。
 *
 * Mutex 是可复制共享值句柄；复制等价于 `Arc<Mutex<T>>`。异步等待不阻塞
 * worker，guard 拥有共享状态并可跨暂停点。持锁异常不会 poison。
 */
template <typename T> class Mutex final {
public:
  explicit Mutex(T value)
      : state_{std::make_shared<detail::MutexState<T>>(std::move(value))} {}

  Mutex()
    requires std::default_initializable<T>
      : Mutex{T{}} {}

  Mutex(const Mutex &) noexcept = default;
  Mutex &operator=(const Mutex &) noexcept = default;
  Mutex(Mutex &&) noexcept = default;
  Mutex &operator=(Mutex &&) noexcept = default;
  ~Mutex() = default;

  [[nodiscard]] static Mutex const_new(T value) {
    return Mutex{std::move(value)};
  }

  class Lock final {
  public:
    static constexpr bool cio_send = send_traits<std::remove_cv_t<T>>::value;
    static constexpr bool cio_sync = false;

    Lock(const Lock &) = delete;
    Lock &operator=(const Lock &) = delete;
    Lock(Lock &&) noexcept = default;
    Lock &operator=(Lock &&) noexcept = default;
    ~Lock() = default;

    class Awaiter final {
    public:
      static constexpr bool cio_send = Lock::cio_send;
      static constexpr bool cio_sync = false;

      Awaiter(std::shared_ptr<detail::MutexState<T>> state,
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

      MutexGuard<T> await_resume();

    private:
      void ensure_valid() const {
        if (!state_) {
          throw std::logic_error{"Mutex Lock awaiter 已移出"};
        }
      }

      std::shared_ptr<detail::MutexState<T>> state_;
      Semaphore::Acquire::Awaiter semaphore_awaiter_;
    };

    [[nodiscard]] Awaiter operator co_await() const {
      ensure_valid();
      return Awaiter{state_, acquisition_};
    }

  private:
    Lock(std::shared_ptr<detail::MutexState<T>> state,
         Semaphore::Acquire acquisition) noexcept
        : state_{std::move(state)}, acquisition_{std::move(acquisition)} {}

    void ensure_valid() const {
      if (!state_) {
        throw std::logic_error{"Mutex Lock 已移出"};
      }
    }

    std::shared_ptr<detail::MutexState<T>> state_;
    Semaphore::Acquire acquisition_;

    friend class Mutex;
  };

  /**
   * 按 FIFO 异步取得锁。
   *
   * 取消会失去队列位置并转交已经分配的许可；future 与 guard 都拥有共享状态，
   * 不保存裸引用。等待不阻塞 worker，恢复后可迁移线程。
   */
  [[nodiscard]] Lock lock() const {
    ensure_valid();
    return Lock{state_, state_->semaphore_.acquire_owned()};
  }

  [[nodiscard]] Lock lock_owned() const { return lock(); }

  /**
   * 在同步线程阻塞等待同一 FIFO 锁。
   *
   * CIO 异步执行上下文中调用会抛出 `logic_error`；该方法会阻塞调用线程。
   */
  [[nodiscard]] MutexGuard<T> blocking_lock() const;

  [[nodiscard]] OwnedMutexGuard<T> blocking_lock_owned() const {
    return blocking_lock();
  }

  [[nodiscard]] Result<MutexGuard<T>, TryLockError> try_lock() const {
    ensure_valid();
    auto acquired = state_->semaphore_.try_acquire_owned();
    if (!acquired.has_value()) {
      return Result<MutexGuard<T>, TryLockError>::failure(TryLockError{});
    }
    return Result<MutexGuard<T>, TryLockError>::success(
        detail::MutexGuardAccess<T>::make(state_, std::move(acquired).value()));
  }

  [[nodiscard]] Result<OwnedMutexGuard<T>, TryLockError>
  try_lock_owned() const {
    return try_lock();
  }

  /**
   * 取得受锁许可保护的同步独占可变 guard。
   *
   * Rust 可用 `&mut self` 在编译期维持返回引用的独占期，C++20 无法证明该
   * 借用关系。CIO 因此返回拥有唯一许可的 guard：即使随后复制 Mutex 句柄，
   * 其他访问也必须等待 guard 析构。调用时若存在其他锁句柄、等待者或 guard，
   * 会抛出 `std::logic_error`；本操作不阻塞且不发生协程暂停。
   */
  [[nodiscard]] MutexGuard<T> get_mut() {
    ensure_unique("Mutex::get_mut 要求唯一共享句柄");
    auto acquired = state_->semaphore_.try_acquire_owned();
    if (!acquired.has_value()) {
      throw std::logic_error{"Mutex::get_mut 无法取得锁许可"};
    }
    return detail::MutexGuardAccess<T>::make(state_,
                                             std::move(acquired).value());
  }

  /**
   * 消耗唯一 Mutex 句柄并移出内部值。
   */
  [[nodiscard]] T into_inner() && {
    ensure_unique("Mutex::into_inner 要求唯一共享句柄");
    auto state = std::move(state_);
    return T{std::move(state->value_)};
  }

private:
  explicit Mutex(std::shared_ptr<detail::MutexState<T>> state) noexcept
      : state_{std::move(state)} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"Mutex 已移出"};
    }
  }

  void ensure_unique(std::string_view message) const {
    ensure_valid();
    if (state_.use_count() != 1) {
      throw std::logic_error{std::string{message}};
    }
  }

  std::shared_ptr<detail::MutexState<T>> state_;

  friend class MutexGuard<T>;
  template <typename, typename> friend class MappedMutexGuard;
};

} // namespace cio::sync

namespace cio::detail {

template <typename T> struct MutexGuardAccess final {
  static sync::MutexGuard<T> make(std::shared_ptr<MutexState<T>> state,
                                  sync::SemaphorePermit permit) {
    return sync::MutexGuard<T>{std::move(state), std::move(permit)};
  }
};

template <typename T>
Task<sync::MutexGuard<T>> blocking_mutex_lock(sync::Mutex<T> mutex) {
  co_return co_await mutex.lock_owned();
}

} // namespace cio::detail

namespace cio::sync {

template <typename T> MutexGuard<T> Mutex<T>::Lock::Awaiter::await_resume() {
  auto acquired = semaphore_awaiter_.await_resume();
  if (!acquired.has_value()) {
    std::terminate();
  }
  return detail::MutexGuardAccess<T>::make(std::move(state_),
                                           std::move(acquired).value());
}

template <typename T> MutexGuard<T> Mutex<T>::blocking_lock() const {
  ensure_valid();
  if (detail::active_execution_context) {
    throw std::logic_error{
        "Mutex::blocking_lock 不能在 CIO 异步执行上下文中调用"};
  }
  runtime::Runtime runtime;
  return runtime.block_on(detail::blocking_mutex_lock(*this));
}

template <typename T> Mutex<T> MutexGuard<T>::mutex() const {
  ensure_valid();
  return Mutex<T>{state_};
}

template <typename T>
template <typename Projection>
  requires detail::MutexProjection<Projection, T>
auto MutexGuard<T>::map(MutexGuard guard, Projection projection)
    -> MappedMutexGuard<T, detail::mutex_projection_value_t<
                               std::remove_cvref_t<Projection>, T>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using U = detail::mutex_projection_value_t<StoredProjection, T>;

  guard.ensure_valid();
  U &selected = std::invoke(projection, guard.get());
  // aliasing shared_ptr 把一次投影得到的子对象绑定到根状态控制块；后续访问
  // 不保存或再次执行用户 callable，跨 worker 移动也不会改变视图。
  std::shared_ptr<U> view{guard.state_, std::addressof(selected)};
  auto state = std::move(guard.state_);
  auto permit = std::move(guard.permit_);
  return MappedMutexGuard<T, U>{std::move(state), std::move(permit),
                                std::move(view)};
}

template <typename T>
template <typename Predicate, typename Projection>
  requires(detail::MutexMapPredicate<Predicate, T> &&
           detail::MutexProjection<Projection, T>)
auto MutexGuard<T>::try_map(MutexGuard guard, Predicate predicate,
                            Projection projection)
    -> Result<MappedMutexGuard<T, detail::mutex_projection_value_t<
                                      std::remove_cvref_t<Projection>, T>>,
              MutexGuard> {
  using Mapped = MappedMutexGuard<
      T, detail::mutex_projection_value_t<std::remove_cvref_t<Projection>, T>>;

  guard.ensure_valid();
  if (!std::invoke(predicate, guard.get())) {
    return Result<Mapped, MutexGuard>::failure(std::move(guard));
  }
  return Result<Mapped, MutexGuard>::success(
      map(std::move(guard), std::move(projection)));
}

template <typename T, typename U>
template <typename Projection>
  requires detail::MutexProjection<Projection, U>
auto MappedMutexGuard<T, U>::map(MappedMutexGuard guard, Projection projection)
    -> MappedMutexGuard<T, detail::mutex_projection_value_t<
                               std::remove_cvref_t<Projection>, U>> {
  using StoredProjection = std::remove_cvref_t<Projection>;
  using S = detail::mutex_projection_value_t<StoredProjection, U>;

  guard.ensure_valid();
  S &selected = std::invoke(projection, guard.get());
  std::shared_ptr<S> view{guard.state_, std::addressof(selected)};
  auto state = std::move(guard.state_);
  auto permit = std::move(guard.permit_);
  return MappedMutexGuard<T, S>{std::move(state), std::move(permit),
                                std::move(view)};
}

template <typename T, typename U>
template <typename Predicate, typename Projection>
  requires(detail::MutexMapPredicate<Predicate, U> &&
           detail::MutexProjection<Projection, U>)
auto MappedMutexGuard<T, U>::try_map(MappedMutexGuard guard,
                                     Predicate predicate, Projection projection)
    -> Result<MappedMutexGuard<T, detail::mutex_projection_value_t<
                                      std::remove_cvref_t<Projection>, U>>,
              MappedMutexGuard> {
  using Mapped = MappedMutexGuard<
      T, detail::mutex_projection_value_t<std::remove_cvref_t<Projection>, U>>;

  guard.ensure_valid();
  if (!std::invoke(predicate, guard.get())) {
    return Result<Mapped, MappedMutexGuard>::failure(std::move(guard));
  }
  return Result<Mapped, MappedMutexGuard>::success(
      map(std::move(guard), std::move(projection)));
}

} // namespace cio::sync

namespace cio {

template <typename T>
struct send_traits<sync::Mutex<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::Mutex<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct send_traits<sync::MutexGuard<T>> : send_traits<std::remove_cv_t<T>> {};

// Rust 的 Arc 只能从共享引用取得 `&MutexGuard`，不能调用需要 `&mut` 的写访问；
// C++ shared_ptr<T> 即使自身为 const，解引用仍可取得 T&。因此任何拥有可变访问的
// CIO guard 都不能标记为 Sync，否则多个 shared_ptr owner 可并发调用非 const
// get。
template <typename T>
struct sync_traits<sync::MutexGuard<T>> : std::false_type {};

template <typename T, typename U>
struct send_traits<sync::MappedMutexGuard<T, U>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         send_traits<std::remove_cv_t<U>>::value> {};

template <typename T, typename U>
struct sync_traits<sync::MappedMutexGuard<T, U>> : std::false_type {};

} // namespace cio
