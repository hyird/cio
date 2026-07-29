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
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/detail/blocking_pool.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

template <typename F>
class [[nodiscard]] BlockingAwaiter {
public:
    using Result = std::invoke_result_t<F&>;

    explicit BlockingAwaiter(F fn,
                             BlockingClass klass = BlockingClass::generic)
        : fn_(std::move(fn)), klass_(klass) {}

    BlockingAwaiter(const BlockingAwaiter&) = delete;
    BlockingAwaiter& operator=(const BlockingAwaiter&) = delete;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        handle_ = h;
        Scheduler* const scheduler = current_scheduler();
        if (scheduler == nullptr) {
            throw std::logic_error(
                "cio: no runtime is active on this thread");
        }
        sched_ =
            scheduler->completion_target();
        preferred_worker_ = current_worker_id(scheduler);
        job_.self = this;
        job_.run = &BlockingAwaiter::run_job;
        job_.fail = &BlockingAwaiter::fail_job;
        job_.klass = klass_;
        switch (scheduler->blocking().submit(&job_)) {
            case BlockingSubmitResult::accepted:
                return;
            case BlockingSubmitResult::overloaded:
                throw SystemError{Error{Errc::overloaded}};
            case BlockingSubmitResult::shutdown:
                throw SystemError{Error{Errc::shutdown}};
        }
        throw std::logic_error("cio: invalid blocking submission result");
    }

    Result await_resume() {
        if (exception_) std::rethrow_exception(exception_);
        return result_.take();
    }

private:
    struct Job : BlockingJob {
        BlockingAwaiter* self = nullptr;
    };

    // The pool stopped while this job was still waiting for an admission
    // slot. Its callable never ran, so the task is resumed with the same
    // Errc::shutdown a rejected submission would have thrown.
    static void fail_job(BlockingJob* job) noexcept {
        auto* self = static_cast<Job*>(job)->self;
        self->exception_ =
            std::make_exception_ptr(SystemError{Error{Errc::shutdown}});

        const SchedulerTarget sched = self->sched_;
        const std::coroutine_handle<> handle = self->handle_;
        const WorkerId preferred_worker = self->preferred_worker_;
        SchedulerTarget::dispatch_completion(
            sched, handle, preferred_worker);
    }

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
        const SchedulerTarget sched = self->sched_;
        const std::coroutine_handle<> handle = self->handle_;
        const WorkerId preferred_worker = self->preferred_worker_;
        SchedulerTarget::dispatch_completion(
            sched, handle, preferred_worker);
    }

    F fn_;
    BlockingClass klass_ = BlockingClass::generic;
    Job job_{};
    ValueSlot<Result> result_{};
    std::exception_ptr exception_;
    std::coroutine_handle<> handle_{};
    SchedulerTarget sched_;
    WorkerId preferred_worker_ = kInvalidWorkerId;
};

}  // namespace detail

// Runs `fn` on a pool thread and resumes the task with its result.
template <typename F>
[[nodiscard]] auto blocking(F fn) {
    return detail::BlockingAwaiter<F>{std::move(fn)};
}

namespace detail {

// Built-in I/O submits under its own admission class so that one kind of
// blocking work cannot starve another. Not public: applications choose the
// limits through RuntimeOptions, not the class per call site.
template <typename F>
[[nodiscard]] auto blocking_in_class(F fn, BlockingClass klass) {
    return BlockingAwaiter<F>{std::move(fn), klass};
}

}  // namespace detail

}  // namespace cio
