#pragma once

#include <coroutine>
#include <utility>

namespace cio::detail {

/**
 * 非拥有的标准协程句柄包装。
 *
 * 该值只允许在持有协程帧所有权的 TaskControl 仍存活时恢复协程，不得保存到
 * TaskControl 生命周期之外。
 */
class CoroutineRef final {
 public:
  constexpr CoroutineRef() noexcept = default;

  template <typename Promise>
  static CoroutineRef from_abi(std::coroutine_handle<Promise> handle) noexcept {
    // std::coroutine_handle 是编译器 ABI 强制边界；进入 CIO 后立即值包装。
    return CoroutineRef{std::coroutine_handle<>{handle}};
  }

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(handle_);
  }

  [[nodiscard]] bool done() const noexcept {
    return !handle_ || handle_.done();
  }

  void resume() const {
    if (handle_ && !handle_.done()) {
      handle_.resume();
    }
  }

  [[nodiscard]] std::coroutine_handle<> native_for_abi() const noexcept {
    // 只供 await_suspend 的编译器 ABI 返回值使用，不转移或表达协程帧所有权。
    return handle_;
  }

 private:
  explicit CoroutineRef(std::coroutine_handle<> handle) noexcept
      : handle_{handle} {}

  std::coroutine_handle<> handle_{};
};

/**
 * 独占协程帧的 RAII 值类型。
 */
template <typename Promise>
class CoroutineOwner final {
 public:
  CoroutineOwner() noexcept = default;

  static CoroutineOwner from_promise(Promise& promise) noexcept {
    return CoroutineOwner{std::coroutine_handle<Promise>::from_promise(promise)};
  }

  CoroutineOwner(const CoroutineOwner&) = delete;
  CoroutineOwner& operator=(const CoroutineOwner&) = delete;

  CoroutineOwner(CoroutineOwner&& other) noexcept
      : handle_{std::exchange(other.handle_, {})},
        resumable_{std::exchange(other.resumable_, {})} {}

  CoroutineOwner& operator=(CoroutineOwner&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, {});
      resumable_ = std::exchange(other.resumable_, {});
    }
    return *this;
  }

  ~CoroutineOwner() {
    reset();
  }

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(handle_);
  }

  [[nodiscard]] bool done() const noexcept {
    return !handle_ || handle_.done();
  }

  [[nodiscard]] CoroutineRef ref() const noexcept {
    return CoroutineRef::from_abi(handle_);
  }

  void set_resumable(CoroutineRef coroutine) noexcept {
    resumable_ = coroutine;
  }

  void resume_current() const {
    resumable_.resume();
  }

  Promise& promise() const {
    return handle_.promise();
  }

  void reset() noexcept {
    if (handle_) {
      resumable_ = {};
      handle_.destroy();
      handle_ = {};
    }
  }

 private:
  explicit CoroutineOwner(std::coroutine_handle<Promise> handle) noexcept
      : handle_{handle}, resumable_{CoroutineRef::from_abi(handle)} {}

  std::coroutine_handle<Promise> handle_{};
  CoroutineRef resumable_;
};

}  // namespace cio::detail
