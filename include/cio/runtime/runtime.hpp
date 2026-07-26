#pragma once

#include <concepts>
#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

#include "cio/send.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/runtime_core.hpp"
#include "cio/task/join_handle.hpp"
#include "cio/task/portable.hpp"
#include "cio/task/task.hpp"

namespace cio::detail {
struct RuntimeAccess;
}

namespace cio::runtime {

class Runtime;

/**
 * runtime 构建器。支持 current-thread、multi-thread、时间驱动和 blocking
 * pool 核心配置；尚未实现的 Builder 配置以兼容矩阵为准。
 */
class Builder final {
 public:
  static Builder new_current_thread() noexcept {
    return Builder{};
  }

  static Builder new_multi_thread() noexcept {
    Builder builder;
    builder.multi_thread_ = true;
    return builder;
  }

  Builder& enable_time() noexcept {
    enable_time_ = true;
    return *this;
  }

  Builder& enable_all() noexcept {
    return enable_time();
  }

  Builder& start_paused(bool value = true) noexcept {
    start_paused_ = value;
    return *this;
  }

  Builder& worker_threads(std::size_t count) {
    if (count == 0) {
      throw std::invalid_argument{"multi-thread worker 数量必须大于零"};
    }
    worker_threads_ = count;
    return *this;
  }

  Builder& max_blocking_threads(std::size_t count) {
    if (count == 0) {
      throw std::invalid_argument{
          "blocking worker 数量上限必须大于零"};
    }
    max_blocking_threads_ = count;
    return *this;
  }

  Runtime build() const;

 private:
  Builder() = default;

  bool enable_time_{false};
  bool start_paused_{false};
  bool multi_thread_{false};
  std::size_t worker_threads_{0};
  std::size_t max_blocking_threads_{512};
};

/**
 * CIO current-thread 或 multi-thread runtime。
 *
 * current-thread 在 block_on 期间由调用线程驱动 task；multi-thread 由 G/M/P
 * worker 持续驱动 portable task。析构取消异步 task、等待已开始的 blocking
 * job，并在发布 cancelled join 结果前完成相应清理。
 */
class Runtime final {
 public:
  Runtime()
      : state_{std::make_shared<detail::RuntimeState>(
            true,
            false,
            0,
            512)} {}

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;
  Runtime(Runtime&&) noexcept = default;
  Runtime& operator=(Runtime&& other) noexcept {
    if (this != &other) {
      if (state_) {
        state_->shutdown();
      }
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~Runtime() {
    if (state_) {
      state_->shutdown();
    }
  }

  template <typename T>
  task::JoinHandle<T> spawn(Task<T> task) {
    ensure_valid();
    return state_->spawn(std::move(task));
  }

  template <typename T>
  task::JoinHandle<T> spawn(task::PortableTask<T> task) {
    ensure_valid();
    return state_->spawn(
        detail::PortableTaskAccess::take(std::move(task)),
        true);
  }

  template <typename Factory, typename... Args>
    requires(
        std::is_empty_v<std::remove_cvref_t<Factory>> &&
        (Send<std::remove_cvref_t<Args>> && ...) &&
        std::invocable<
            std::remove_cvref_t<Factory>,
            std::remove_cvref_t<Args>...> &&
        !std::is_reference_v<std::invoke_result_t<
            std::remove_cvref_t<Factory>,
            std::remove_cvref_t<Args>...>> &&
        (std::is_void_v<std::invoke_result_t<
             std::remove_cvref_t<Factory>,
             std::remove_cvref_t<Args>...>> ||
         Send<std::invoke_result_t<
             std::remove_cvref_t<Factory>,
             std::remove_cvref_t<Args>...>>))
  auto spawn_blocking(Factory factory, Args&&... arguments) {
    ensure_valid();
    using StoredFactory = std::remove_cvref_t<Factory>;
    return state_->spawn_blocking(
        StoredFactory{std::move(factory)},
        std::tuple<std::remove_cvref_t<Args>...>{
            std::forward<Args>(arguments)...});
  }

  template <typename T>
  T block_on(Task<T> task) {
    return block_on_impl(std::move(task), false);
  }

  template <typename T>
  T block_on(task::PortableTask<T> task) {
    return block_on_impl(
        detail::PortableTaskAccess::take(std::move(task)),
        true);
  }

 private:
  template <typename T>
  T block_on_impl(Task<T> task, bool portable) {
    ensure_valid();
    if (detail::active_execution_context) {
      throw std::logic_error{"Runtime::block_on 不能在异步上下文中嵌套调用"};
    }

    auto root = state_->spawn(std::move(task), portable);
    const auto state = state_;
    state_->run_until([join_state = root.abort_handle()] {
      return join_state.is_finished();
    });

    auto result = detail::JoinHandleAccess::take(root);
    if (!result) {
      const auto error = std::move(result).error();
      if (error.is_panic() && error.exception()) {
        std::rethrow_exception(error.exception());
      }
      throw std::runtime_error{"block_on 根 task 被取消"};
    }

    if constexpr (std::is_void_v<T>) {
      result.value();
      return;
    } else {
      return T{std::move(result).value()};
    }
  }

  Runtime(
      bool enable_time,
      bool start_paused,
      std::size_t worker_count,
      std::size_t max_blocking_threads)
      : state_{std::make_shared<detail::RuntimeState>(
            enable_time,
            start_paused,
            worker_count,
            max_blocking_threads)} {
    state_->start_workers();
  }

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"Runtime 已移出"};
    }
  }

  std::shared_ptr<detail::RuntimeState> state_;

  friend class Builder;
  friend struct detail::RuntimeAccess;
};

inline Runtime Builder::build() const {
  if (multi_thread_ && start_paused_) {
    throw std::logic_error{
        "暂停时间只支持 current-thread CIO runtime"};
  }
  std::size_t worker_count = 0;
  if (multi_thread_) {
    worker_count = worker_threads_;
    if (worker_count == 0) {
      worker_count = static_cast<std::size_t>(
          std::thread::hardware_concurrency());
      if (worker_count == 0) {
        worker_count = 1;
      }
    }
  }
  return Runtime{
      enable_time_,
      start_paused_,
      worker_count,
      max_blocking_threads_};
}

}  // namespace cio::runtime

namespace cio::detail {

struct RuntimeAccess final {
  static std::shared_ptr<RuntimeState> state(
      const runtime::Runtime& runtime) {
    runtime.ensure_valid();
    return runtime.state_;
  }
};

}  // namespace cio::detail
