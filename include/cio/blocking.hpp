// Escaping to a real thread.
//
//     auto contents = co_await cio::blocking([&] { return read_whole_file(path); });
//
// Workers must never block. There is no preemption here — a worker stuck in a
// syscall is 1/N of the runtime gone until it returns. Anything that can block
// (file I/O, getaddrinfo, a C library that does its own socket calls, a long
// CPU crunch you want off the critical path) belongs here.
#pragma once

#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

#include "cio/detail/blocking_pool.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

template <typename F>
class [[nodiscard]] BlockingAwaiter {
public:
    using Result = std::invoke_result_t<F&>;

    explicit BlockingAwaiter(F fn) : fn_(std::move(fn)) {}

    BlockingAwaiter(const BlockingAwaiter&) = delete;
    BlockingAwaiter& operator=(const BlockingAwaiter&) = delete;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        handle_ = h;
        sched_ = current_scheduler();
        job_.self = this;
        job_.run = &BlockingAwaiter::run_job;
        sched_->blocking().submit(&job_);
    }

    Result await_resume() {
        if (exception_) std::rethrow_exception(exception_);
        return result_.take();
    }

private:
    struct Job : BlockingJob {
        BlockingAwaiter* self = nullptr;
    };

    static void run_job(BlockingJob* job) noexcept {
        auto* self = static_cast<Job*>(job)->self;
        try {
            if constexpr (std::is_void_v<Result>) {
                self->fn_();
            } else {
                self->result_.set(self->fn_());
            }
        } catch (...) {
            self->exception_ = std::current_exception();
        }
        // Read everything we need before scheduling: resuming the task lets it
        // destroy the frame this awaiter lives in.
        Scheduler* sched = self->sched_;
        const std::coroutine_handle<> handle = self->handle_;
        if (sched != nullptr) sched->schedule(handle);
    }

    F fn_;
    Job job_{};
    ValueSlot<Result> result_{};
    std::exception_ptr exception_;
    std::coroutine_handle<> handle_{};
    Scheduler* sched_ = nullptr;
};

}  // namespace detail

// Runs `fn` on a pool thread and resumes the task with its result.
template <typename F>
[[nodiscard]] auto blocking(F fn) {
    return detail::BlockingAwaiter<F>{std::move(fn)};
}

}  // namespace cio
