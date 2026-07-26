#include "cio/sync/semaphore.hpp"

#include <algorithm>
#include <exception>
#include <utility>

namespace cio::detail {

SemaphoreAcquireOperation::SemaphoreAcquireOperation(
    std::shared_ptr<SemaphoreState> state, std::uint64_t id,
    std::size_t requested) noexcept
    : state_{std::move(state)}, id_{id}, requested_{requested},
      remaining_{requested} {}

SemaphoreAcquireOperation::~SemaphoreAcquireOperation() {
  state_->cancel(*this);
}

bool SemaphoreAcquireOperation::ready() {
  return state_->ready(shared_from_this());
}

bool SemaphoreAcquireOperation::suspend(
    std::shared_ptr<ExecutionContext> context, CoroutineRef coroutine) {
  return state_->suspend(shared_from_this(), std::move(context), coroutine);
}

Result<sync::SemaphorePermit, sync::AcquireError>
SemaphoreAcquireOperation::resume() {
  if (state_->consume(*this)) {
    return Result<sync::SemaphorePermit, sync::AcquireError>::success(
        sync::SemaphorePermit{state_, requested_});
  }
  return Result<sync::SemaphorePermit, sync::AcquireError>::failure(
      sync::AcquireError{});
}

SemaphoreState::SemaphoreState(std::size_t permits) : available_{permits} {
  if (permits > max_permits) {
    throw std::invalid_argument{"Semaphore 初始许可超过 MAX_PERMITS"};
  }
}

std::shared_ptr<SemaphoreAcquireOperation>
SemaphoreState::make_operation(std::size_t requested) {
  if (requested > max_permits) {
    throw std::invalid_argument{"Semaphore 获取数量超过 MAX_PERMITS"};
  }

  std::uint64_t id = 0;
  {
    std::lock_guard lock{mutex_};
    id = next_waiter_id_++;
    if (id == 0) {
      std::terminate();
    }
  }
  return std::make_shared<SemaphoreAcquireOperation>(shared_from_this(), id,
                                                     requested);
}

std::size_t SemaphoreState::available_permits() const {
  std::lock_guard lock{mutex_};
  return available_;
}

bool SemaphoreState::is_closed() const {
  std::lock_guard lock{mutex_};
  return closed_;
}

SemaphoreTryStatus SemaphoreState::try_acquire(std::size_t requested) {
  if (requested > max_permits) {
    throw std::invalid_argument{"Semaphore 获取数量超过 MAX_PERMITS"};
  }

  std::lock_guard lock{mutex_};
  if (closed_) {
    return SemaphoreTryStatus::closed;
  }
  if (requested == 0) {
    return SemaphoreTryStatus::acquired;
  }
  if (!waiters_.empty() || available_ < requested) {
    return SemaphoreTryStatus::no_permits;
  }
  available_ -= requested;
  return SemaphoreTryStatus::acquired;
}

void SemaphoreState::wake_all(WakeBatch wakes) noexcept {
  for (auto &context : wakes) {
    if (context) {
      context->wake();
    }
  }
}

void SemaphoreState::remove_waiter_locked(std::uint64_t id) noexcept {
  const auto position =
      std::find_if(waiters_.begin(), waiters_.end(),
                   [id](const WaiterEntry &entry) { return entry.id == id; });
  if (position != waiters_.end()) {
    waiters_.erase(position);
  }
}

void SemaphoreState::distribute_locked(std::size_t permits, WakeBatch &wakes,
                                       OperationKeepAliveBatch &keep_alive) {
  // weak_ptr::lock() 得到的强引用可能恰好成为 operation 的最后一个 owner。
  // 批次在调用方的 mutex_ 临界区外声明，并在提升 weak_ptr 前一次性预留空间，
  // 确保每个提升结果立即转移到批次且只在解锁后析构，避免析构再次进入 cancel()
  // 而递归锁定非递归 mutex。
  if (waiters_.size() > keep_alive.max_size() - keep_alive.size()) {
    throw std::length_error{"Semaphore waiter keep-alive 批次过大"};
  }
  keep_alive.reserve(keep_alive.size() + waiters_.size());

  while (permits > 0 && !waiters_.empty()) {
    auto operation = waiters_.front().operation.lock();
    if (!operation) {
      waiters_.pop_front();
      continue;
    }
    keep_alive.push_back(std::move(operation));
    const auto &retained_operation = keep_alive.back();
    if (retained_operation->cancelled_ || !retained_operation->registered_ ||
        retained_operation->completion_ != SemaphoreCompletion::pending) {
      waiters_.pop_front();
      continue;
    }

    const auto assigned = std::min(permits, retained_operation->remaining_);
    retained_operation->remaining_ -= assigned;
    permits -= assigned;
    if (retained_operation->remaining_ != 0) {
      break;
    }

    waiters_.pop_front();
    retained_operation->registered_ = false;
    retained_operation->completion_ = SemaphoreCompletion::acquired;
    if (retained_operation->context_) {
      wakes.push_back(std::move(retained_operation->context_));
    }
  }

  if (permits > 0) {
    available_ += permits;
  }
}

void SemaphoreState::add_permits_impl(std::size_t permits,
                                      bool terminate_on_overflow) {
  if (permits == 0) {
    return;
  }

  WakeBatch wakes;
  OperationKeepAliveBatch keep_alive;
  {
    std::lock_guard lock{mutex_};
    wakes.reserve(waiters_.size());
    keep_alive.reserve(waiters_.size());

    auto remainder = permits;
    for (const auto &entry : waiters_) {
      auto operation = entry.operation.lock();
      if (!operation) {
        continue;
      }
      keep_alive.push_back(std::move(operation));
      const auto &retained_operation = keep_alive.back();
      if (retained_operation->cancelled_ || !retained_operation->registered_ ||
          retained_operation->completion_ != SemaphoreCompletion::pending) {
        continue;
      }
      if (remainder <= retained_operation->remaining_) {
        remainder = 0;
      } else {
        remainder -= retained_operation->remaining_;
      }
      if (remainder == 0) {
        break;
      }
    }

    if (remainder > max_permits || available_ > max_permits - remainder) {
      if (terminate_on_overflow) {
        std::terminate();
      }
      throw std::overflow_error{"Semaphore 可用许可将超过 MAX_PERMITS"};
    }

    distribute_locked(permits, wakes, keep_alive);
  }
  wake_all(std::move(wakes));
}

void SemaphoreState::add_permits(std::size_t permits) {
  add_permits_impl(permits, false);
}

void SemaphoreState::release_permit_noexcept(std::size_t permits) noexcept {
  try {
    add_permits_impl(permits, true);
  } catch (...) {
    std::terminate();
  }
}

std::size_t SemaphoreState::forget_permits(std::size_t permits) {
  std::lock_guard lock{mutex_};
  const auto forgotten = std::min(available_, permits);
  available_ -= forgotten;
  return forgotten;
}

void SemaphoreState::close() {
  WakeBatch wakes;
  OperationKeepAliveBatch keep_alive;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return;
    }
    wakes.reserve(waiters_.size());
    keep_alive.reserve(waiters_.size());
    closed_ = true;

    while (!waiters_.empty()) {
      auto entry = std::move(waiters_.front());
      waiters_.pop_front();
      auto operation = entry.operation.lock();
      if (!operation) {
        continue;
      }
      keep_alive.push_back(std::move(operation));
      const auto &retained_operation = keep_alive.back();
      if (retained_operation->cancelled_ || !retained_operation->registered_ ||
          retained_operation->completion_ != SemaphoreCompletion::pending) {
        continue;
      }

      retained_operation->registered_ = false;
      retained_operation->completion_ = SemaphoreCompletion::closed;
      if (retained_operation->context_) {
        wakes.push_back(std::move(retained_operation->context_));
      }
    }
  }
  wake_all(std::move(wakes));
}

bool SemaphoreState::ready_locked(
    const std::shared_ptr<SemaphoreAcquireOperation> &operation) {
  if (operation->cancelled_) {
    throw std::logic_error{"Semaphore 获取操作已取消"};
  }
  if (operation->consumed_) {
    throw std::logic_error{"Semaphore 获取结果已消费"};
  }
  if (operation->completion_ != SemaphoreCompletion::pending) {
    return true;
  }
  if (operation->registered_) {
    return false;
  }
  if (closed_) {
    operation->completion_ = SemaphoreCompletion::closed;
    return true;
  }
  if (operation->requested_ == 0) {
    operation->completion_ = SemaphoreCompletion::acquired;
    return true;
  }

  if (waiters_.empty() && available_ >= operation->remaining_) {
    available_ -= operation->remaining_;
    operation->remaining_ = 0;
    operation->completion_ = SemaphoreCompletion::acquired;
    return true;
  }

  waiters_.push_back(WaiterEntry{operation->id_, operation});
  operation->registered_ = true;
  if (waiters_.size() == 1 && available_ > 0) {
    const auto assigned = std::min(available_, operation->remaining_);
    available_ -= assigned;
    operation->remaining_ -= assigned;
  }
  return false;
}

bool SemaphoreState::ready(
    const std::shared_ptr<SemaphoreAcquireOperation> &operation) {
  std::lock_guard lock{mutex_};
  return ready_locked(operation);
}

bool SemaphoreState::suspend(
    const std::shared_ptr<SemaphoreAcquireOperation> &operation,
    std::shared_ptr<ExecutionContext> context, CoroutineRef coroutine) {
  std::lock_guard lock{mutex_};
  if (ready_locked(operation)) {
    return false;
  }
  if (operation->context_) {
    throw std::logic_error{"同一 Semaphore 获取操作不能被多个 task 同时等待"};
  }
  context->park(coroutine);
  operation->context_ = std::move(context);
  return true;
}

bool SemaphoreState::consume(SemaphoreAcquireOperation &operation) {
  WakeBatch wakes;
  OperationKeepAliveBatch keep_alive;
  bool acquired = false;
  {
    std::lock_guard lock{mutex_};
    if (operation.cancelled_) {
      throw std::logic_error{"Semaphore 获取操作已取消"};
    }
    if (operation.consumed_) {
      throw std::logic_error{"Semaphore 获取结果已消费"};
    }
    if (operation.completion_ == SemaphoreCompletion::pending) {
      throw std::logic_error{"Semaphore 获取操作在完成前恢复"};
    }

    acquired = operation.completion_ == SemaphoreCompletion::acquired;
    if (!acquired) {
      wakes.reserve(waiters_.size());
      const auto partially_acquired =
          operation.requested_ - operation.remaining_;
      operation.remaining_ = operation.requested_;
      distribute_locked(partially_acquired, wakes, keep_alive);
    }
    operation.consumed_ = true;
    operation.context_.reset();
  }
  wake_all(std::move(wakes));
  return acquired;
}

void SemaphoreState::cancel(SemaphoreAcquireOperation &operation) noexcept {
  WakeBatch wakes;
  OperationKeepAliveBatch keep_alive;
  try {
    {
      std::lock_guard lock{mutex_};
      if (operation.cancelled_) {
        return;
      }
      operation.cancelled_ = true;
      operation.context_.reset();
      if (operation.registered_) {
        remove_waiter_locked(operation.id_);
        operation.registered_ = false;
      }

      if (!operation.consumed_) {
        wakes.reserve(waiters_.size());
        const auto acquired = operation.requested_ - operation.remaining_;
        operation.remaining_ = operation.requested_;
        distribute_locked(acquired, wakes, keep_alive);
      }
    }
    wake_all(std::move(wakes));
  } catch (...) {
    std::terminate();
  }
}

} // namespace cio::detail

namespace cio::sync {

SemaphorePermit::SemaphorePermit(std::shared_ptr<detail::SemaphoreState> state,
                                 std::size_t permits) noexcept
    : state_{std::move(state)}, permits_{permits} {}

SemaphorePermit::SemaphorePermit(SemaphorePermit &&other) noexcept
    : state_{std::move(other.state_)},
      permits_{std::exchange(other.permits_, 0)} {}

SemaphorePermit &SemaphorePermit::operator=(SemaphorePermit &&other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    permits_ = std::exchange(other.permits_, 0);
  }
  return *this;
}

SemaphorePermit::~SemaphorePermit() { release(); }

void SemaphorePermit::release() noexcept {
  auto state = std::move(state_);
  const auto permits = std::exchange(permits_, 0);
  if (state && permits > 0) {
    state->release_permit_noexcept(permits);
  }
}

void SemaphorePermit::forget() noexcept {
  permits_ = 0;
  state_.reset();
}

void SemaphorePermit::merge(SemaphorePermit other) {
  if (!state_ || !other.state_) {
    throw std::logic_error{"不能合并已移出或已 forget 的 permit"};
  }
  if (state_ != other.state_) {
    throw std::invalid_argument{"不能合并来自不同 Semaphore 的 permit"};
  }
  if (other.permits_ > detail::SemaphoreState::max_permits - permits_) {
    throw std::overflow_error{"合并后的 permit 数量超过 MAX_PERMITS"};
  }

  permits_ += other.permits_;
  other.permits_ = 0;
  other.state_.reset();
}

std::optional<SemaphorePermit> SemaphorePermit::split(std::size_t n) {
  if (!state_) {
    throw std::logic_error{"不能拆分已移出或已 forget 的 permit"};
  }
  if (n > permits_) {
    return std::nullopt;
  }

  permits_ -= n;
  return SemaphorePermit{state_, n};
}

Semaphore SemaphorePermit::semaphore() const {
  if (!state_) {
    throw std::logic_error{"已移出或已 forget 的 permit 没有来源 Semaphore"};
  }
  return Semaphore{state_};
}

Semaphore::Semaphore(std::size_t permits)
    : state_{std::make_shared<detail::SemaphoreState>(permits)} {}

Semaphore::Acquire Semaphore::acquire() const { return acquire_many(1); }

Semaphore::Acquire Semaphore::acquire_many(std::uint32_t n) const {
  return Acquire{state_->make_operation(static_cast<std::size_t>(n))};
}

Result<SemaphorePermit, TryAcquireError> Semaphore::try_acquire() const {
  return try_acquire_many(1);
}

Result<SemaphorePermit, TryAcquireError>
Semaphore::try_acquire_many(std::uint32_t n) const {
  const auto count = static_cast<std::size_t>(n);
  switch (state_->try_acquire(count)) {
  case detail::SemaphoreTryStatus::acquired:
    return Result<SemaphorePermit, TryAcquireError>::success(
        SemaphorePermit{state_, count});
  case detail::SemaphoreTryStatus::closed:
    return Result<SemaphorePermit, TryAcquireError>::failure(
        TryAcquireError::closed);
  case detail::SemaphoreTryStatus::no_permits:
    return Result<SemaphorePermit, TryAcquireError>::failure(
        TryAcquireError::no_permits);
  }
  std::terminate();
}

} // namespace cio::sync
