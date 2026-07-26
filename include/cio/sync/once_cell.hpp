#pragma once

#include <concepts>
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
#include "cio/sync/semaphore.hpp"
#include "cio/task/task.hpp"

namespace cio::sync {

template <typename T> class OnceCell;
template <typename T> class OnceCellMutGuard;

enum class SetErrorKind {
  already_initialized,
  initializing,
};

/**
 * OnceCell::set 失败及未被写入值的所有权。
 */
template <typename T> class SetError final {
public:
  SetError(const SetError &) = default;
  SetError &operator=(const SetError &) = default;
  SetError(SetError &&) noexcept(std::is_nothrow_move_constructible_v<T>) =
      default;
  SetError &operator=(SetError &&) noexcept(
      std::is_nothrow_move_assignable_v<T>) = default;
  ~SetError() = default;

  [[nodiscard]] bool is_already_init_err() const noexcept {
    return kind_ == SetErrorKind::already_initialized;
  }

  [[nodiscard]] bool is_initializing_err() const noexcept {
    return kind_ == SetErrorKind::initializing;
  }

  [[nodiscard]] SetErrorKind kind() const noexcept { return kind_; }

  [[nodiscard]] std::string_view message() const noexcept {
    return is_already_init_err() ? std::string_view{"AlreadyInitializedError"}
                                 : std::string_view{"InitializingError"};
  }

  [[nodiscard]] std::string debug_string() const
    requires requires(std::ostream &stream, const T &value) {
      { stream << value } -> std::same_as<std::ostream &>;
    }
  {
    std::ostringstream stream;
    stream << message() << '(' << value_ << ')';
    return stream.str();
  }

  [[nodiscard]] const T &value() const & noexcept { return value_; }
  [[nodiscard]] T &value() & noexcept { return value_; }
  [[nodiscard]] T into_value() && { return T{std::move(value_)}; }

  friend bool operator==(const SetError &, const SetError &)
    requires std::equality_comparable<T>
  = default;

private:
  SetError(SetErrorKind kind, T value)
      : kind_{kind}, value_{std::move(value)} {}

  SetErrorKind kind_;
  T value_;

  template <typename> friend class OnceCell;
};

} // namespace cio::sync

namespace cio::detail {

template <typename T> class OnceCellState final {
public:
  OnceCellState() : semaphore_{1} {}

  explicit OnceCellState(T value)
      : value_{std::make_shared<T>(std::move(value))}, semaphore_{1} {}

  OnceCellState(const OnceCellState &) = delete;
  OnceCellState &operator=(const OnceCellState &) = delete;

  [[nodiscard]] bool initialized() const {
    std::lock_guard lock{mutex_};
    ensure_not_mutating_locked();
    return static_cast<bool>(value_);
  }

  [[nodiscard]] std::shared_ptr<const T> snapshot() const {
    std::lock_guard lock{mutex_};
    ensure_not_mutating_locked();
    return value_;
  }

  [[nodiscard]] std::shared_ptr<const T> publish(T value) {
    auto published = std::make_shared<T>(std::move(value));
    {
      std::lock_guard lock{mutex_};
      ensure_not_mutating_locked();
      if (value_) {
        throw std::logic_error{"OnceCell 重复发布值"};
      }
      value_ = published;
    }
    return published;
  }

  [[nodiscard]] std::optional<T> take() {
    std::shared_ptr<T> taken;
    {
      std::lock_guard lock{mutex_};
      ensure_not_mutating_locked();
      if (!value_) {
        return std::nullopt;
      }
      if constexpr (!std::copy_constructible<T>) {
        if (value_.use_count() != 1) {
          throw std::logic_error{
              "OnceCell::take 的非复制值仍有 owning snapshot"};
        }
      }
      taken = std::exchange(value_, {});
    }

    if constexpr (std::copy_constructible<T>) {
      if (taken.use_count() == 1) {
        return std::optional<T>{std::in_place, std::move(*taken)};
      }
      // Rust 的 &mut 借用会排除旧引用。C++ 安全映射允许既有 owning snapshot
      // 继续观察旧版本，因此这里在锁外复制，不移动它正在观察的对象。
      return std::optional<T>{std::in_place, *taken};
    } else {
      // 锁内已证明不存在 snapshot，exchange 后也不会再产生新的旧值 snapshot。
      return std::optional<T>{std::in_place, std::move(*taken)};
    }
  }

  [[nodiscard]] std::shared_ptr<T> begin_mutation() {
    std::lock_guard lock{mutex_};
    ensure_not_mutating_locked();
    if (!value_) {
      return {};
    }
    if (value_.use_count() != 1) {
      throw std::logic_error{"OnceCell::get_mut 要求不存在 owning snapshot"};
    }
    mutating_ = true;
    return std::exchange(value_, {});
  }

  void finish_mutation(std::shared_ptr<T> value) noexcept {
    if (!value) {
      return;
    }
    try {
      std::lock_guard lock{mutex_};
      if (!mutating_ || value_) {
        std::terminate();
      }
      value_ = std::move(value);
      mutating_ = false;
    } catch (...) {
      std::terminate();
    }
  }

  sync::Semaphore semaphore_{1};

private:
  void ensure_not_mutating_locked() const {
    if (mutating_) {
      throw std::logic_error{"OnceCell 正由 get_mut guard 独占修改"};
    }
  }

  mutable std::mutex mutex_;
  std::shared_ptr<T> value_;
  bool mutating_{false};
};

template <typename Awaitable>
using OnceCellAwaitResult =
    decltype(std::declval<Awaitable &&>().operator co_await().await_resume());

template <typename> struct OnceCellResultTraits;

template <typename Value, typename Error>
struct OnceCellResultTraits<Result<Value, Error>> final {
  using value_type = Value;
  using error_type = Error;
};

template <typename T, typename Factory>
Task<std::shared_ptr<const T>>
once_cell_get_or_init(std::shared_ptr<OnceCellState<T>> state,
                      Factory factory) {
  if (auto current = state->snapshot()) {
    co_return current;
  }

  auto acquired = co_await state->semaphore_.acquire_owned();
  if (!acquired.has_value()) {
    throw std::logic_error{"OnceCell 私有初始化 semaphore 被意外关闭"};
  }

  std::shared_ptr<const T> published;
  {
    auto permit = std::move(acquired).value();
    if (auto current = state->snapshot()) {
      co_return current;
    }

    // factory 只在唯一初始化许可持有者中调用。异常或 task 取消会析构
    // permit，把初始化权转交给 FIFO 队首且不发布半成品。
    auto produced = co_await std::invoke(std::move(factory));
    published = state->publish(T{std::move(produced)});
  }
  co_return published;
}

template <typename T, typename Factory>
using OnceCellTryAwaitable =
    std::invoke_result_t<std::remove_cvref_t<Factory> &&>;

template <typename T, typename Factory>
using OnceCellTryResult =
    std::remove_cvref_t<OnceCellAwaitResult<OnceCellTryAwaitable<T, Factory>>>;

template <typename T, typename Factory>
using OnceCellTryError =
    typename OnceCellResultTraits<OnceCellTryResult<T, Factory>>::error_type;

template <typename T, typename Factory>
Task<Result<std::shared_ptr<const T>, OnceCellTryError<T, Factory>>>
once_cell_get_or_try_init(std::shared_ptr<OnceCellState<T>> state,
                          Factory factory) {
  using Error = OnceCellTryError<T, Factory>;
  using Output = Result<std::shared_ptr<const T>, Error>;

  if (auto current = state->snapshot()) {
    co_return Output::success(std::move(current));
  }

  auto acquired = co_await state->semaphore_.acquire_owned();
  if (!acquired.has_value()) {
    throw std::logic_error{"OnceCell 私有初始化 semaphore 被意外关闭"};
  }

  std::shared_ptr<const T> published;
  {
    auto permit = std::move(acquired).value();
    if (auto current = state->snapshot()) {
      co_return Output::success(std::move(current));
    }

    auto produced = co_await std::invoke(std::move(factory));
    if (!produced.has_value()) {
      co_return Output::failure(std::move(produced).error());
    }
    published = state->publish(T{std::move(produced).value()});
  }
  co_return Output::success(std::move(published));
}

template <typename T>
concept OnceCellDebugWritable = requires(std::ostream &stream, const T &value) {
  { stream << value } -> std::same_as<std::ostream &>;
};

} // namespace cio::detail

namespace cio::sync {

/**
 * OnceCell 独占同步修改 guard。
 *
 * 这是 Tokio `Option<&mut T>` 的保守 C++20 映射：guard 不直接返回可逃逸
 * 引用。`update` 的 callable 必须同步完成；C++20 无法证明 callback 未通过
 * 副作用保存地址，因此这仍是调用方必须遵守的安全契约。guard 不可复制，
 * 析构时把值重新发布到 cell；它不得跨越协程暂停点。
 */
template <typename T> class OnceCellMutGuard final {
public:
  OnceCellMutGuard(const OnceCellMutGuard &) = delete;
  OnceCellMutGuard &operator=(const OnceCellMutGuard &) = delete;

  OnceCellMutGuard(OnceCellMutGuard &&other) noexcept
      : state_{std::move(other.state_)}, value_{std::move(other.value_)} {}

  OnceCellMutGuard &operator=(OnceCellMutGuard &&) = delete;

  ~OnceCellMutGuard() noexcept {
    if (state_ && value_) {
      state_->finish_mutation(std::move(value_));
    }
  }

  template <typename Function>
    requires std::invocable<std::remove_cvref_t<Function> &, T &>
  decltype(auto) update(Function function) {
    ensure_valid();
    using ResultType =
        std::invoke_result_t<std::remove_cvref_t<Function> &, T &>;
    static_assert(!std::is_reference_v<ResultType> &&
                      !std::is_pointer_v<ResultType>,
                  "OnceCellMutGuard::update 不允许返回可逃逸引用或指针");
    return std::invoke(function, *value_);
  }

  void replace(T value) {
    ensure_valid();
    value_ = std::make_shared<T>(std::move(value));
  }

  [[nodiscard]] T copy() const
    requires std::copy_constructible<T>
  {
    ensure_valid();
    return *value_;
  }

private:
  OnceCellMutGuard(std::shared_ptr<detail::OnceCellState<T>> state,
                   std::shared_ptr<T> value) noexcept
      : state_{std::move(state)}, value_{std::move(value)} {}

  void ensure_valid() const {
    if (!state_ || !value_) {
      throw std::logic_error{"OnceCellMutGuard 已移出"};
    }
  }

  std::shared_ptr<detail::OnceCellState<T>> state_;
  std::shared_ptr<T> value_;

  friend class OnceCell<T>;
};

/**
 * Tokio 1.53.1 风格的异步单次初始化 cell。
 *
 * CIO 返回 `shared_ptr<const T>` owning snapshot，替代 Rust 借用引用。snapshot
 * 可安全越过 cell 移动或 `take`，不会保存裸指针。OnceCell 的复制执行 Tokio
 * `Clone` 的深复制语义，不会让两个 cell 共享初始化状态。
 */
template <typename T> class OnceCell final {
public:
  OnceCell() : state_{std::make_shared<detail::OnceCellState<T>>()} {}

  OnceCell(const OnceCell &other)
    requires std::copy_constructible<T>
      : OnceCell{other.snapshot_copy()} {}

  OnceCell(const OnceCell &)
    requires(!std::copy_constructible<T>)
  = delete;

  OnceCell &operator=(const OnceCell &other)
    requires std::copy_constructible<T>
  {
    if (this != &other) {
      auto replacement = OnceCell{other.snapshot_copy()};
      state_ = std::move(replacement.state_);
    }
    return *this;
  }

  OnceCell &operator=(const OnceCell &)
    requires(!std::copy_constructible<T>)
  = delete;

  OnceCell(OnceCell &&) noexcept = default;
  OnceCell &operator=(OnceCell &&) noexcept = default;
  ~OnceCell() = default;

  /**
   * C++20 运行期兼容工厂。
   *
   * 共享控制块与 mutex 不能常量求值，因此不具备 Rust 静态
   * `const fn` 初始化能力。
   */
  [[nodiscard]] static OnceCell const_new() { return OnceCell{}; }

  [[nodiscard]] static OnceCell new_with(std::optional<T> value) {
    return OnceCell{std::move(value)};
  }

  /**
   * C++20 运行期 `const_new_with` 兼容工厂。
   */
  [[nodiscard]] static OnceCell const_new_with(T value) {
    return OnceCell{std::move(value)};
  }

  [[nodiscard]] static OnceCell from(T value) {
    return OnceCell{std::move(value)};
  }

  [[nodiscard]] bool initialized() const {
    ensure_valid();
    return state_->initialized();
  }

  /**
   * 返回当前不可变 owning snapshot；空指针表示尚未初始化。
   */
  [[nodiscard]] std::shared_ptr<const T> get() const {
    ensure_valid();
    return state_->snapshot();
  }

  /**
   * 取得独占同步修改 guard。
   *
   * C++ 无法静态证明 `&mut self`，因此要求没有正在运行的初始化操作、cell
   * 共享句柄或 owning snapshot；否则抛出 `logic_error`。空 cell 返回空值。
   * guard 不得跨越 `co_await`。
   */
  [[nodiscard]] std::optional<OnceCellMutGuard<T>> get_mut() {
    ensure_unique("OnceCell::get_mut 要求唯一 cell 状态");
    auto value = state_->begin_mutation();
    if (!value) {
      return std::nullopt;
    }
    return std::optional<OnceCellMutGuard<T>>{
        OnceCellMutGuard<T>{state_, std::move(value)}};
  }

  [[nodiscard]] Result<void, SetError<T>> set(T value) const {
    ensure_valid();
    if (state_->initialized()) {
      return Result<void, SetError<T>>::failure(
          SetError<T>{SetErrorKind::already_initialized, std::move(value)});
    }

    auto acquired = state_->semaphore_.try_acquire_owned();
    if (!acquired.has_value()) {
      if (acquired.error() == TryAcquireError::closed) {
        throw std::logic_error{"OnceCell 私有初始化 semaphore 被意外关闭"};
      }
      return Result<void, SetError<T>>::failure(
          SetError<T>{SetErrorKind::initializing, std::move(value)});
    }
    auto permit = std::move(acquired).value();
    if (state_->initialized()) {
      return Result<void, SetError<T>>::failure(
          SetError<T>{SetErrorKind::already_initialized, std::move(value)});
    }
    (void)state_->publish(std::move(value));
    return Result<void, SetError<T>>::success();
  }

  /**
   * 读取现值，或由唯一初始化者异步生成并发布值。
   *
   * factory 按值保存于操作帧，state 由共享句柄拥有；CIO 自身不引入调用者
   * 引用，但 C++20 无法审计 factory 内部捕获，portable 承诺仍由 factory 的
   * Send/Sync 审计承担。取消、factory 异常或发布前分配失败都会归还初始化
   * 许可，FIFO 队首可重试且不会发布半成品。等待不阻塞 worker；portable
   * factory 的暂停恢复允许线程迁移。递归初始化同一个 cell 与 Tokio 一样会
   * 形成逻辑死锁。
   */
  template <typename Factory>
  [[nodiscard]] Task<std::shared_ptr<const T>>
  get_or_init(Factory factory) const {
    ensure_valid();
    return detail::once_cell_get_or_init<T>(
        state_, std::remove_cvref_t<Factory>{std::move(factory)});
  }

  /**
   * 读取现值，或执行可失败的异步初始化。
   *
   * factory 及 state 均由操作拥有，不借用调用者对象。错误、异常、取消及任一
   * 暂停边界销毁都会归还许可并允许重试；失败值绝不发布。等待不阻塞 worker，
   * 满足 portable 安全边界时可在 worker 间迁移。递归初始化同一 cell 会死锁。
   */
  template <typename Factory>
  [[nodiscard]] auto get_or_try_init(Factory factory) const -> Task<
      Result<std::shared_ptr<const T>, detail::OnceCellTryError<T, Factory>>> {
    ensure_valid();
    return detail::once_cell_get_or_try_init<T>(
        state_, std::remove_cvref_t<Factory>{std::move(factory)});
  }

  /**
   * 移出当前值并恢复为空 cell。
   *
   * 要求没有运行中的 cell 操作。若 non-copyable 值仍有 owning snapshot，
   * 为避免移动正在观察的对象而抛出 `logic_error`。
   */
  [[nodiscard]] std::optional<T> take() {
    ensure_unique("OnceCell::take 要求没有运行中的 cell 操作");
    return state_->take();
  }

  [[nodiscard]] std::optional<T> into_inner() && {
    ensure_unique("OnceCell::into_inner 要求唯一 cell 状态");
    auto state = std::move(state_);
    return state->take();
  }

  [[nodiscard]] OnceCell clone() const
    requires std::copy_constructible<T>
  {
    return OnceCell{*this};
  }

  [[nodiscard]] std::string debug_string() const
    requires detail::OnceCellDebugWritable<T>
  {
    std::ostringstream stream;
    stream << "OnceCell { value: ";
    if (auto value = get()) {
      stream << "Some(" << *value << ')';
    } else {
      stream << "None";
    }
    stream << " }";
    return stream.str();
  }

  friend bool operator==(const OnceCell &left, const OnceCell &right)
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
  explicit OnceCell(std::optional<T> value)
      : state_{value ? std::make_shared<detail::OnceCellState<T>>(
                           std::move(*value))
                     : std::make_shared<detail::OnceCellState<T>>()} {}

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"OnceCell 已移出"};
    }
  }

  void ensure_unique(std::string_view message) const {
    ensure_valid();
    if (state_.use_count() != 1) {
      throw std::logic_error{std::string{message}};
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

  std::shared_ptr<detail::OnceCellState<T>> state_;
};

template <typename T>
std::ostream &operator<<(std::ostream &stream, const OnceCell<T> &cell)
  requires detail::OnceCellDebugWritable<T>
{
  return stream << cell.debug_string();
}

template <typename T>
std::ostream &operator<<(std::ostream &stream, const SetError<T> &error) {
  return stream << error.message();
}

} // namespace cio::sync

namespace cio {

template <typename T>
struct send_traits<sync::OnceCell<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::OnceCell<T>>
    : std::bool_constant<send_traits<std::remove_cv_t<T>>::value &&
                         sync_traits<std::remove_cv_t<T>>::value> {};

template <typename T>
struct send_traits<sync::OnceCellMutGuard<T>> : std::false_type {};

template <typename T>
struct sync_traits<sync::OnceCellMutGuard<T>> : std::false_type {};

template <typename T>
struct send_traits<sync::SetError<T>> : send_traits<std::remove_cv_t<T>> {};

template <typename T>
struct sync_traits<sync::SetError<T>> : sync_traits<std::remove_cv_t<T>> {};

} // namespace cio
