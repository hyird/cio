#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/detail/join_state.hpp"
#include "cio/detail/runtime_context.hpp"
#include "cio/result.hpp"
#include "cio/task/id.hpp"
#include "cio/task/join_error.hpp"

namespace cio::detail {

class BlockingJobBase {
 public:
  BlockingJobBase() = default;
  BlockingJobBase(const BlockingJobBase&) = delete;
  BlockingJobBase& operator=(const BlockingJobBase&) = delete;
  virtual ~BlockingJobBase() = default;

  virtual void run() noexcept = 0;
  virtual void cancel_if_queued() noexcept = 0;
};

template <typename Output, typename Factory, typename... Args>
class BlockingJob final : public BlockingJobBase {
 public:
  using JoinResult = Result<Output, task::JoinError>;
  using Invocation = std::tuple<Factory, Args...>;

  BlockingJob(
      task::Id id,
      Factory factory,
      std::tuple<Args...> arguments,
      std::shared_ptr<JoinState<Output>> join_state,
      std::weak_ptr<RuntimeState> runtime,
      std::function<void()> on_release)
      : id_{id},
        invocation_{std::make_shared<Invocation>(
            std::tuple_cat(
                std::tuple<Factory>{std::move(factory)},
                std::move(arguments)))},
        join_state_{std::move(join_state)},
        runtime_{std::move(runtime)},
        on_release_{std::move(on_release)} {}

  void run() noexcept override {
    std::shared_ptr<Invocation> invocation;
    {
      std::lock_guard lock{mutex_};
      if (phase_ != Phase::queued) {
        return;
      }
      phase_ = Phase::running;
      invocation = std::move(invocation_);
      invocation_.reset();
    }

    ScopedRuntimeContext runtime_scope{runtime_};
    JoinResult result = execute(*invocation);
    invocation.reset();
    {
      std::lock_guard lock{mutex_};
      phase_ = Phase::finished;
    }
    join_state_->complete(std::move(result));
    release_once();
  }

  void cancel_if_queued() noexcept override {
    bool cancelled = false;
    {
      std::lock_guard lock{mutex_};
      if (phase_ == Phase::queued) {
        phase_ = Phase::cancelled;
        invocation_.reset();
        cancelled = true;
      }
    }
    if (!cancelled) {
      return;
    }
    join_state_->complete(
        JoinResult::failure(task::JoinError::cancelled(id_)));
    release_once();
  }

 private:
  enum class Phase {
    queued,
    running,
    finished,
    cancelled,
  };

  JoinResult execute(Invocation& invocation) noexcept {
    try {
      if constexpr (std::is_void_v<Output>) {
        std::apply(
            [](Factory& factory, Args&... arguments) {
              std::invoke(
                  std::move(factory),
                  std::move(arguments)...);
            },
            invocation);
        return JoinResult::success();
      } else {
        auto output = std::apply(
            [](Factory& factory, Args&... arguments) -> Output {
              return std::invoke(
                  std::move(factory),
                  std::move(arguments)...);
            },
            invocation);
        return JoinResult::success(std::move(output));
      }
    } catch (...) {
      return JoinResult::failure(
          task::JoinError::panic(
              id_,
              std::current_exception()));
    }
  }

  void release_once() noexcept {
    if (released_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    try {
      on_release_();
    } catch (...) {
      // blocking 完成释放属于 runtime 内部路径，不能传播异常。
      std::terminate();
    }
  }

  task::Id id_;
  std::mutex mutex_;
  Phase phase_{Phase::queued};
  std::shared_ptr<Invocation> invocation_;
  std::shared_ptr<JoinState<Output>> join_state_;
  std::weak_ptr<RuntimeState> runtime_;
  std::function<void()> on_release_;
  std::atomic<bool> released_{false};
};

/**
 * Tokio 风格的按需 blocking worker pool。
 *
 * 队列和线程计数由同一互斥保护；job 自身独立同步排队取消与开始执行的竞态。
 * 当前切片不回收空闲线程，thread_keep_alive 将在后续性能切片实现。
 */
class BlockingPool final
    : public std::enable_shared_from_this<BlockingPool> {
 public:
  explicit BlockingPool(std::size_t thread_cap);
  BlockingPool(const BlockingPool&) = delete;
  BlockingPool& operator=(const BlockingPool&) = delete;
  ~BlockingPool();

  bool submit(std::shared_ptr<BlockingJobBase> job);
  void shutdown() noexcept;

  [[nodiscard]] std::size_t thread_cap() const noexcept;
  [[nodiscard]] std::size_t thread_count() const noexcept;
  [[nodiscard]] std::size_t idle_thread_count() const noexcept;
  [[nodiscard]] std::size_t queue_depth() const noexcept;

 private:
  void worker_loop() noexcept;

  const std::size_t thread_cap_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<std::shared_ptr<BlockingJobBase>> queue_;
  std::vector<std::thread> threads_;
  std::size_t live_threads_{0};
  std::size_t idle_threads_{0};
  bool shutdown_started_{false};
};

}  // namespace cio::detail
