#include "cio/detail/blocking_pool.hpp"

#include <stdexcept>
#include <system_error>

namespace cio::detail {

BlockingPool::BlockingPool(std::size_t thread_cap)
    : thread_cap_{thread_cap} {
  if (thread_cap_ == 0) {
    throw std::invalid_argument{
        "blocking pool 线程上限必须大于零"};
  }
}

BlockingPool::~BlockingPool() {
  shutdown();
}

bool BlockingPool::submit(std::shared_ptr<BlockingJobBase> job) {
  if (!job) {
    throw std::invalid_argument{"不能提交空 blocking job"};
  }

  std::vector<std::shared_ptr<BlockingJobBase>> abandoned;
  {
    std::unique_lock lock{mutex_};
    if (shutdown_started_) {
      lock.unlock();
      job->cancel_if_queued();
      return false;
    }

    queue_.push_back(std::move(job));
    if (queue_.size() <= idle_threads_) {
      changed_.notify_one();
      return true;
    }
    if (live_threads_ >= thread_cap_) {
      return true;
    }

    ++live_threads_;
    try {
      const auto self = shared_from_this();
      threads_.emplace_back([self] {
        self->worker_loop();
      });
    } catch (...) {
      --live_threads_;
      if (live_threads_ != 0) {
        changed_.notify_one();
        return true;
      }
      abandoned.assign(queue_.begin(), queue_.end());
      queue_.clear();
      lock.unlock();
      for (const auto& pending : abandoned) {
        pending->cancel_if_queued();
      }
      throw;
    }
  }
  return true;
}

void BlockingPool::shutdown() noexcept {
  std::vector<std::shared_ptr<BlockingJobBase>> pending;
  std::vector<std::thread> threads;
  {
    std::lock_guard lock{mutex_};
    if (shutdown_started_) {
      return;
    }
    shutdown_started_ = true;
    pending.assign(queue_.begin(), queue_.end());
    queue_.clear();
    threads = std::move(threads_);
  }

  for (const auto& job : pending) {
    job->cancel_if_queued();
  }
  changed_.notify_all();

  for (auto& thread : threads) {
    if (!thread.joinable()) {
      continue;
    }
    if (thread.get_id() == std::this_thread::get_id()) {
      thread.detach();
    } else {
      try {
        thread.join();
      } catch (...) {
        std::terminate();
      }
    }
  }
}

std::size_t BlockingPool::thread_cap() const noexcept {
  return thread_cap_;
}

std::size_t BlockingPool::thread_count() const noexcept {
  std::lock_guard lock{mutex_};
  return live_threads_;
}

std::size_t BlockingPool::idle_thread_count() const noexcept {
  std::lock_guard lock{mutex_};
  return idle_threads_;
}

std::size_t BlockingPool::queue_depth() const noexcept {
  std::lock_guard lock{mutex_};
  return queue_.size();
}

void BlockingPool::worker_loop() noexcept {
  while (true) {
    std::shared_ptr<BlockingJobBase> job;
    {
      std::unique_lock lock{mutex_};
      ++idle_threads_;
      changed_.wait(lock, [this] {
        return shutdown_started_ || !queue_.empty();
      });
      --idle_threads_;

      if (shutdown_started_) {
        --live_threads_;
        changed_.notify_all();
        return;
      }

      job = std::move(queue_.front());
      queue_.pop_front();
    }
    job->run();
  }
}

}  // namespace cio::detail
