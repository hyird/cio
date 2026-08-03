// The runtime handle.
//
// Users see threads exactly once — when they choose how many the runtime gets.
// Everything past this point is tasks.
#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <type_traits>

#include "cio/detail/scheduler.hpp"
#include "cio/group.hpp"
#include "cio/spawn.hpp"
#include "cio/task.hpp"

namespace cio {

struct RuntimeOptions {
    // 0 means one worker per hardware thread.
    std::size_t worker_threads = 0;
    // Ceiling on threads used for blocking work. These are not schedulers; they
    // exist so a blocking call cannot take a worker out of circulation.
    std::size_t max_blocking_threads = 512;
    // Maximum blocking jobs waiting to start. A full queue rejects new work
    // with Errc::overloaded instead of retaining coroutine frames without
    // bound. 0 selects the default limit of 1024.
    std::size_t max_blocking_queue = 1024;
    // Concurrently admitted built-in file operations. These bound operations,
    // not threads: a task waiting for admission is parked and occupies no pool
    // thread. Each class has its own wait queue, so a burst of one kind cannot
    // sit in front of the other. 0 means the class is not separately limited.
    std::size_t max_file_operations = 32;
    // Concurrently admitted name lookups.
    std::size_t max_resolver_operations = 8;
};

class Runtime {
public:
    explicit Runtime(RuntimeOptions options = {});
    ~Runtime();

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    // Runs `task` on the runtime and blocks the calling thread until it
    // finishes. The calling thread is not a worker; it just waits.
    template<typename T>
    T block_on(Task<T> task);

    template<typename T>
    JoinHandle<T> spawn(Task<T> task) {
        return detail::spawn_on(*sched_, std::move(task), true);
    }

    template<typename T>
    void go(Task<T> task) {
        detail::go_on(*sched_, std::move(task), true);
    }

    std::size_t worker_count() const noexcept { return sched_->worker_count(); }

    // Blocks until every worker has stopped. Calling this from a task running
    // on this same Runtime would have to join its own worker and is rejected
    // with std::logic_error.
    void shutdown();

    // Cooperative graceful shutdown. New roots submitted from outside this
    // Runtime are rejected, shutdown_token() is cancelled, and this call waits
    // for roots submitted through this Runtime. Structured cleanup children
    // remain covered while their root joins them. A root that ignores the
    // token can keep this call waiting.
    void graceful_shutdown();
    CancelToken shutdown_token() const noexcept {
        return shutdown_source_.token();
    }

    detail::Scheduler& scheduler() noexcept { return *sched_; }

private:
    std::shared_ptr<detail::Scheduler> sched_;
    CancelSource shutdown_source_;
};

namespace detail {

template<typename T>
struct BlockOnSync {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    ValueSlot<T> slot;
    std::exception_ptr exception;

    void finish() {
        // notify while holding the lock: the waiter destroys this object as
        // soon as it wakes, and a spurious wakeup must not let it race ahead of
        // our last touch.
        std::lock_guard<std::mutex> lock(mutex);
        done = true;
        cv.notify_one();
    }
};

template<typename T>
Task<void> block_on_runner(Task<T> task, BlockOnSync<T>* sync) {
    try {
        if constexpr (std::is_void_v<T>) {
            co_await std::move(task);
        } else {
            sync->slot.set(co_await std::move(task));
        }
    } catch (...) {
        sync->exception = std::current_exception();
    }
    sync->finish();
}

}  // namespace detail

template<typename T>
T Runtime::block_on(Task<T> task) {
    detail::BlockOnSync<T> sync;
    detail::go_on(*sched_, detail::block_on_runner<T>(std::move(task), &sync),
                  true);

    {
        std::unique_lock<std::mutex> lock(sync.mutex);
        sync.cv.wait(lock, [&sync] { return sync.done; });
    }

    if (sync.exception) std::rethrow_exception(sync.exception);
    return sync.slot.take();
}

// Convenience: build a runtime, run one task on it, tear it down.
template<typename T>
T run(Task<T> task, RuntimeOptions options = {}) {
    Runtime runtime(options);
    return runtime.block_on(std::move(task));
}

}  // namespace cio
