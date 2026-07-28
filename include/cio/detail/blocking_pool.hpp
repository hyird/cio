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

// Intrusive job node. Lives in the awaiter, which lives in the coroutine frame.
struct BlockingJob {
    // Runs the user's callable, stores its result, then reschedules the waiter.
    void (*run)(BlockingJob*) noexcept = nullptr;
    BlockingJob* next = nullptr;
};

class BlockingPool;
using BlockingThreadLauncher =
    bool (*)(BlockingPool*) noexcept;

enum class BlockingSubmitResult {
    accepted,
    overloaded,
    shutdown,
};

class BlockingPool {
public:
    explicit BlockingPool(std::size_t max_threads,
                          std::size_t max_queue = 1024,
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

private:
    static bool launch_thread(BlockingPool* pool) noexcept;
    // mutex_ is held. A successful launcher must start exactly one detached
    // call to worker_main(); a failed launcher must start none.
    bool try_spawn_thread_locked() noexcept;
    void worker_main();

    std::size_t max_threads_;
    std::size_t max_queue_;
    BlockingThreadLauncher thread_launcher_;

    std::mutex mutex_;
    std::condition_variable cv_;       // work available / stopping
    std::condition_variable exit_cv_;  // a thread retired
    BlockingJob* head_ = nullptr;
    BlockingJob* tail_ = nullptr;
    std::size_t queued_ = 0;
    std::size_t idle_ = 0;
    bool stopping_ = false;

    // Threads are detached and retire on their own after an idle keepalive, so
    // a burst of blocking work does not leave hundreds of threads parked for
    // the life of the process.
    std::atomic<std::size_t> threads_{0};
};

}  // namespace cio::detail
