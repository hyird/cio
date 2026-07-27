#include "cio/detail/blocking_pool.hpp"

#include <chrono>

#include "cio/detail/scheduler.hpp"

namespace cio::detail {
namespace {
constexpr auto kIdleKeepAlive = std::chrono::seconds(10);
}

BlockingPool::BlockingPool(std::size_t max_threads)
    : max_threads_(max_threads == 0 ? 512 : max_threads) {}

BlockingPool::~BlockingPool() { shutdown(); }

void BlockingPool::submit(BlockingJob* job) {
    bool need_thread = false;
    bool run_inline = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            // The runtime is going away. Running inline costs the caller
            // latency; leaving the job unrun would hang whatever awaits it.
            run_inline = true;
        } else {
            job->next = nullptr;
            if (tail_ != nullptr) {
                tail_->next = job;
                tail_ = job;
            } else {
                head_ = tail_ = job;
            }
            // Grow only when there is genuinely nobody to pick this up.
            need_thread = idle_ == 0 && threads_.load(std::memory_order_relaxed) < max_threads_;
        }
    }
    if (run_inline) {
        job->run(job);
        return;
    }
    if (need_thread) spawn_thread();
    cv_.notify_one();
}

void BlockingPool::spawn_thread() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || threads_.load(std::memory_order_relaxed) >= max_threads_) return;
        threads_.fetch_add(1, std::memory_order_relaxed);
    }
    try {
        std::thread([this] { worker_main(); }).detach();
    } catch (...) {
        std::lock_guard<std::mutex> lock(mutex_);
        threads_.fetch_sub(1, std::memory_order_relaxed);
        exit_cv_.notify_all();
    }
}

void BlockingPool::worker_main() {
    std::unique_lock<std::mutex> lock(mutex_);
    bool retire = false;

    while (!retire) {
        while (head_ == nullptr) {
            if (stopping_) {
                retire = true;
                break;
            }
            ++idle_;
            const bool timed_out = cv_.wait_for(lock, kIdleKeepAlive) == std::cv_status::timeout;
            --idle_;
            if (timed_out && head_ == nullptr && !stopping_) {
                retire = true;
                break;
            }
        }
        if (retire) break;

        BlockingJob* job = head_;
        head_ = job->next;
        if (head_ == nullptr) tail_ = nullptr;

        lock.unlock();
        // run() executes the user callable, stores the result into the awaiter,
        // and reschedules the parked task onto the runtime.
        job->run(job);
        lock.lock();
    }

    threads_.fetch_sub(1, std::memory_order_relaxed);
    exit_cv_.notify_all();
}

void BlockingPool::shutdown() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
    cv_.notify_all();
    exit_cv_.wait(lock, [this] { return threads_.load(std::memory_order_relaxed) == 0; });
}

}  // namespace cio::detail
