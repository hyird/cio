#include "cio/detail/blocking_pool.hpp"

#include <chrono>

#include "cio/detail/scheduler.hpp"

namespace cio::detail {
namespace {
constexpr auto kIdleKeepAlive = std::chrono::seconds(10);
}

BlockingPool::BlockingPool(std::size_t max_threads,
                           std::size_t max_queue,
                           BlockingThreadLauncher thread_launcher)
    : BlockingPool(BlockingLimits{max_threads, max_queue, 0, 0},
                   thread_launcher) {}

BlockingPool::BlockingPool(BlockingLimits limits,
                           BlockingThreadLauncher thread_launcher)
    : limits_(limits),
      thread_launcher_(thread_launcher == nullptr
                           ? &BlockingPool::launch_thread
                           : thread_launcher) {
    if (limits_.max_threads == 0) limits_.max_threads = 512;
    if (limits_.max_queue == 0) limits_.max_queue = 1024;

    class_limit_[index_of(BlockingClass::generic)] = 0;  // unlimited
    class_limit_[index_of(BlockingClass::file)] = limits_.max_file_operations;
    class_limit_[index_of(BlockingClass::resolver)] =
        limits_.max_resolver_operations;
}

BlockingPool::~BlockingPool() { shutdown(); }

std::size_t BlockingPool::inflight(BlockingClass klass) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return class_inflight_[index_of(klass)];
}

std::size_t BlockingPool::awaiting_admission(BlockingClass klass) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return class_waiting_[index_of(klass)];
}

bool BlockingPool::enqueue_locked(BlockingJob* job) noexcept {
    // Provision before publishing the intrusive node. The new worker cannot
    // enter worker_main() until this lock is released. If no existing worker
    // can eventually service the queue and creation fails, rejection leaves the
    // caller's coroutine frame unretained.
    const bool need_thread =
        queued_ + 1 > idle_ &&
        threads_.load(std::memory_order_relaxed) < limits_.max_threads;
    if (need_thread && !try_spawn_thread_locked() &&
        threads_.load(std::memory_order_relaxed) == 0) {
        return false;
    }

    job->next = nullptr;
    if (tail_ != nullptr) {
        tail_->next = job;
        tail_ = job;
    } else {
        head_ = tail_ = job;
    }
    ++queued_;
    return true;
}

void BlockingPool::release_admission_locked(BlockingClass klass) noexcept {
    const unsigned index = index_of(klass);
    if (class_limit_[index] == 0) return;

    --class_inflight_[index];

    BlockingJob* next = admission_head_[index];
    if (next == nullptr) return;

    // Promote exactly one waiter into the slot just freed. If it cannot be
    // enqueued the pool has no thread at all, so completing it with the
    // shutdown path is the only way to release its caller.
    admission_head_[index] = next->next;
    if (admission_head_[index] == nullptr) admission_tail_[index] = nullptr;
    --class_waiting_[index];
    --admission_queued_;
    ++class_inflight_[index];

    if (!enqueue_locked(next)) {
        --class_inflight_[index];
        if (next->fail != nullptr) next->fail(next);
        return;
    }
    cv_.notify_one();
}

BlockingSubmitResult BlockingPool::submit(BlockingJob* job) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return BlockingSubmitResult::shutdown;
        // Admission waiters retain coroutine frames just like queued jobs, so
        // they count against the same global bound.
        if (queued_ + admission_queued_ >= limits_.max_queue) {
            return BlockingSubmitResult::overloaded;
        }

        const unsigned index = index_of(job->klass);
        if (class_limit_[index] != 0 &&
            class_inflight_[index] >= class_limit_[index]) {
            // Park for admission. The task is already suspended; it simply is
            // not handed to a pool thread yet, so it occupies no thread.
            job->next = nullptr;
            if (admission_tail_[index] != nullptr) {
                admission_tail_[index]->next = job;
                admission_tail_[index] = job;
            } else {
                admission_head_[index] = admission_tail_[index] = job;
            }
            ++class_waiting_[index];
            ++admission_queued_;
            return BlockingSubmitResult::accepted;
        }

        if (class_limit_[index] != 0) ++class_inflight_[index];
        if (!enqueue_locked(job)) {
            if (class_limit_[index] != 0) --class_inflight_[index];
            return BlockingSubmitResult::overloaded;
        }
    }
    cv_.notify_one();
    return BlockingSubmitResult::accepted;
}

bool BlockingPool::launch_thread(BlockingPool* pool) noexcept {
    try {
        std::thread([pool] { pool->worker_main(); }).detach();
        return true;
    } catch (...) {
        return false;
    }
}

bool BlockingPool::try_spawn_thread_locked() noexcept {
    threads_.fetch_add(1, std::memory_order_relaxed);
    if (thread_launcher_(this)) return true;

    threads_.fetch_sub(1, std::memory_order_relaxed);
    exit_cv_.notify_all();
    return false;
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
        --queued_;

        // run() resumes the parked task, which may destroy the frame this node
        // lives in. Read the class before releasing the lock, never after.
        const BlockingClass klass = job->klass;

        lock.unlock();
        // run() executes the user callable, stores the result into the awaiter,
        // and reschedules the parked task onto the runtime.
        job->run(job);
        lock.lock();

        release_admission_locked(klass);
    }

    threads_.fetch_sub(1, std::memory_order_relaxed);
    exit_cv_.notify_all();
}

void BlockingPool::shutdown() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;

    // Admission waiters never started, so they are completed with a shutdown
    // error rather than being left parked forever. Detach the lists first: a
    // failed job resumes its task, which must not re-enter the pool under this
    // lock.
    BlockingJob* pending = nullptr;
    for (unsigned index = 0; index < kBlockingClassCount; ++index) {
        BlockingJob* job = admission_head_[index];
        while (job != nullptr) {
            BlockingJob* const next = job->next;
            job->next = pending;
            pending = job;
            job = next;
        }
        admission_head_[index] = admission_tail_[index] = nullptr;
        class_waiting_[index] = 0;
    }
    admission_queued_ = 0;

    if (pending != nullptr) {
        lock.unlock();
        while (pending != nullptr) {
            BlockingJob* const next = pending->next;
            if (pending->fail != nullptr) pending->fail(pending);
            pending = next;
        }
        lock.lock();
    }

    cv_.notify_all();
    exit_cv_.wait(lock, [this] { return threads_.load(std::memory_order_relaxed) == 0; });
}

}  // namespace cio::detail
