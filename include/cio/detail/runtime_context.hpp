#pragma once

#include <memory>
#include <utility>

namespace cio::detail {

class RuntimeState;

/**
 * 当前同步线程关联的 CIO runtime。
 *
 * blocking worker 没有异步 task 的 ExecutionContext，但 Tokio 允许从
 * spawn_blocking 函数中继续访问当前 runtime。本上下文只保存弱所有权，不延长
 * runtime 生命周期，也不允许借此把协程暂停位置暴露到 blocking 线程。
 */
inline thread_local std::weak_ptr<RuntimeState> active_runtime_context;

class ScopedRuntimeContext final {
 public:
  explicit ScopedRuntimeContext(
      std::weak_ptr<RuntimeState> runtime) noexcept
      : previous_{std::exchange(
            active_runtime_context,
            std::move(runtime))} {}

  ScopedRuntimeContext(const ScopedRuntimeContext&) = delete;
  ScopedRuntimeContext& operator=(const ScopedRuntimeContext&) = delete;

  ~ScopedRuntimeContext() {
    active_runtime_context = std::move(previous_);
  }

 private:
  std::weak_ptr<RuntimeState> previous_;
};

}  // namespace cio::detail
