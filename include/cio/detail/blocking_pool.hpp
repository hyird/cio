// The blocking pool.
//
// Anything that would park an OS thread — file I/O, getaddrinfo, a third-party
// library that calls read() on a regular fd, CPU-bound work a user wants off
// the runtime — runs here instead of on a worker. Workers must never block,
// because blocking one removes 1/N of the runtime's ability to make progress
// and (unlike Go) we have no preemption to paper over it.
//
// Threads are created lazily and retire after an idle keepalive, so a program
// that never calls into it pays nothing.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <vector>

#include "cio/config.hpp"

namespace cio::detail {

// Operation classes with independent admission limits.
//
// Built-in I/O gets its own budget so that a burst of one kind cannot occupy
// the whole pool: a directory walk saturating file admission must not stop name
// resolution from making progress. Admission bounds concurrently *running*
// operations, not threads.
enum class BlockingClass : unsigned {
    generic = 0,   // user cio::blocking(); unlimited by default
    file = 1,
    resolver = 2,
};
inline constexpr unsigned kBlockingClassCount = 3;

// Intrusive job node. Lives in the awaiter, which lives in the coroutine frame.
struct BlockingJob {
    // Runs the user's callable, stores its result, then reschedules the waiter.
    void (*run)(BlockingJob*) noexcept = nullptr;
    // Completes the waiter with Errc::shutdown without running the callable.
    // Used for jobs still waiting for admission when the pool stops.
    void (*fail)(BlockingJob*) noexcept = nullptr;
    BlockingJob* next = nullptr;
    BlockingClass klass = BlockingClass::generic;
};

class BlockingPool;
using BlockingThreadLauncher =
    bool (*)(BlockingPool*) noexcept;

enum class BlockingSubmitResult {
    accepted,
    overloaded,
    shutdown,
};

struct BlockingLimits {
    std::size_t max_threads = 512;
    std::size_t max_queue = 1024;
    // 0 means the class is not separately limited.
    std::size_t max_file_operations = 32;
    std::size_t max_resolver_operations = 8;
};

class BlockingPool {
public:
    explicit BlockingPool(std::size_t max_threads,
                          std::size_t max_queue = 1024,
                          BlockingThreadLauncher thread_launcher = nullptr);
    explicit BlockingPool(BlockingLimits limits,
                          BlockingThreadLauncher thread_launcher = nullptr);
    ~BlockingPool();

    BlockingPool(const BlockingPool&) = delete;
    BlockingPool& operator=(const BlockingPool&) = delete;

    // Enqueues a job, growing the pool when queued demand exceeds idle
    // capacity. Rejected jobs remain owned by the caller and are never run.
    BlockingSubmitResult submit(BlockingJob* job);

    void shutdown();

    std::size_t thread_count() const noexcept {
        return threads_.load(std::memory_order_relaxed);
    }

    // Operations of `klass` currently admitted (running or queued to run).
    std::size_t inflight(BlockingClass klass) const;
    // Operations of `klass` parked waiting for an admission slot.
    std::size_t awaiting_admission(BlockingClass klass) const;

private:
    static bool launch_thread(BlockingPool* pool) noexcept;
    // mutex_ is held. A successful launcher must start exactly one detached
    // call to worker_main(); a failed launcher must start none.
    bool try_spawn_thread_locked() noexcept;
    // mutex_ is held. Appends to the run queue and provisions a thread.
    // Returns false only when no thread can ever service the job.
    bool enqueue_locked(BlockingJob* job) noexcept;
    // mutex_ is held. Releases one slot of `klass` and promotes the next
    // admission waiter of that class, if any.
    void release_admission_locked(BlockingClass klass) noexcept;
    void worker_main();

    static constexpr unsigned index_of(BlockingClass klass) noexcept {
        return static_cast<unsigned>(klass);
    }

    BlockingLimits limits_;
    BlockingThreadLauncher thread_launcher_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;       // work available / stopping
    std::condition_variable exit_cv_;  // a thread retired
    BlockingJob* head_ = nullptr;
    BlockingJob* tail_ = nullptr;
    std::size_t queued_ = 0;
    std::size_t idle_ = 0;
    bool stopping_ = false;

    // Per-class admission. A separate FIFO per class is what stops a long run
    // of file admissions from sitting in front of every resolver admission.
    std::size_t class_limit_[kBlockingClassCount]{};
    std::size_t class_inflight_[kBlockingClassCount]{};
    BlockingJob* admission_head_[kBlockingClassCount]{};
    BlockingJob* admission_tail_[kBlockingClassCount]{};
    std::size_t class_waiting_[kBlockingClassCount]{};
    std::size_t admission_queued_ = 0;

    // Threads are detached and retire on their own after an idle keepalive, so
    // a burst of blocking work does not leave hundreds of threads parked for
    // the life of the process.
    std::atomic<std::size_t> threads_{0};
};

}  // namespace cio::detail
