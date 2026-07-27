// spawn / go — putting a task on the scheduler.
//
// Two flavours, because they have genuinely different costs:
//
//   go(t)     fire and forget, exactly like `go f()`. The task's own frame is
//             the only allocation; it destroys itself on completion.
//   spawn(t)  returns a JoinHandle you can co_await for the result. This one
//             needs a shared completion slot, so it costs a refcounted state
//             plus a small wrapper frame. You only pay for it if you join.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <utility>

#include "cio/detail/frame_pool.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

inline Scheduler& require_scheduler() {
    Scheduler* sched = current_scheduler();
    if (sched == nullptr) {
        throw std::logic_error("cio: no runtime is active on this thread");
    }
    return *sched;
}

// Completion rendezvous between a spawned task and its JoinHandle.
//
// `waiter_` is a single word with three states: nullptr (no joiner yet), a
// parked joiner's frame address, or kDone. The CAS is what makes "join before
// completion" and "join after completion" the same code path.
template <typename T>
class JoinState {
public:
    // Same pool as the coroutine frames: spawn() allocates one of these per
    // call, on the same threads and with the same lifetime pattern.
    static void* operator new(std::size_t size) { return FramePool::allocate(size); }
    static void operator delete(void* state, std::size_t size) noexcept {
        FramePool::deallocate(state, size);
    }

    void add_ref() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
    void release() noexcept {
        if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }

    template <typename... U>
    void set_value(U&&... value) {
        slot_.set(std::forward<U>(value)...);
        complete();
    }

    void set_exception(std::exception_ptr e) noexcept {
        exception_ = std::move(e);
        complete();
    }

    bool completed() const noexcept {
        return waiter_.load(std::memory_order_acquire) == done_sentinel();
    }

    // Returns true if the joiner parked; false if the task already finished.
    bool try_park(std::coroutine_handle<> joiner) noexcept {
        void* expected = nullptr;
        return waiter_.compare_exchange_strong(expected, joiner.address(),
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire);
    }

    T take() {
        if (exception_) std::rethrow_exception(exception_);
        return slot_.take();
    }

private:
    static void* done_sentinel() noexcept {
        return reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));
    }

    void complete() noexcept {
        void* previous = waiter_.exchange(done_sentinel(), std::memory_order_acq_rel);
        if (previous != nullptr && previous != done_sentinel()) {
            Scheduler* sched = current_scheduler();
            if (sched != nullptr) {
                sched->schedule(std::coroutine_handle<>::from_address(previous));
            }
        }
    }

    std::atomic<void*> waiter_{nullptr};
    std::atomic<std::uint32_t> refs_{0};
    ValueSlot<T> slot_;
    std::exception_ptr exception_;
};

template <typename T>
class StateRef {
public:
    StateRef() noexcept = default;
    explicit StateRef(JoinState<T>* state) noexcept : state_(state) {
        if (state_) state_->add_ref();
    }
    StateRef(const StateRef& other) noexcept : state_(other.state_) {
        if (state_) state_->add_ref();
    }
    StateRef(StateRef&& other) noexcept : state_(std::exchange(other.state_, nullptr)) {}
    StateRef& operator=(StateRef other) noexcept {
        std::swap(state_, other.state_);
        return *this;
    }
    ~StateRef() {
        if (state_) state_->release();
    }

    JoinState<T>* get() const noexcept { return state_; }
    JoinState<T>* operator->() const noexcept { return state_; }
    explicit operator bool() const noexcept { return state_ != nullptr; }
    void reset() noexcept {
        if (state_) state_->release();
        state_ = nullptr;
    }

private:
    JoinState<T>* state_ = nullptr;
};

}  // namespace detail

// A handle to a spawned task's eventual result.
//
// Dropping it without joining detaches the task, which keeps running — the
// goroutine model, not the structured-concurrency one. Use TaskGroup when you
// want children bounded by a scope.
template <typename T = void>
class [[nodiscard]] JoinHandle {
public:
    JoinHandle() noexcept = default;
    explicit JoinHandle(detail::StateRef<T> state) noexcept : state_(std::move(state)) {}

    JoinHandle(JoinHandle&&) noexcept = default;
    JoinHandle& operator=(JoinHandle&&) noexcept = default;
    JoinHandle(const JoinHandle&) = delete;
    JoinHandle& operator=(const JoinHandle&) = delete;

    bool valid() const noexcept { return static_cast<bool>(state_); }
    bool done() const noexcept { return state_ && state_->completed(); }

    void detach() noexcept { state_.reset(); }

    auto operator co_await() noexcept {
        struct Awaiter {
            detail::JoinState<T>* state;

            bool await_ready() const noexcept { return state->completed(); }
            bool await_suspend(std::coroutine_handle<> joiner) noexcept {
                return state->try_park(joiner);
            }
            T await_resume() { return state->take(); }
        };
        return Awaiter{state_.get()};
    }

private:
    detail::StateRef<T> state_;
};

// Fire and forget. The task owns itself and disappears when it finishes; an
// exception that escapes it terminates the process, as an unrecovered panic in
// a goroutine does.
template <typename T>
void go(Task<T> task) {
    detail::Scheduler& sched = detail::require_scheduler();
    auto handle = task.release();
    handle.promise().detached = true;
    sched.schedule(handle);
}

namespace detail {

template <typename T>
inline void go_on(Scheduler& sched, Task<T> task) {
    auto handle = task.release();
    handle.promise().detached = true;
    sched.schedule(handle);
}

template <typename T>
Task<void> root_runner(Task<T> task, StateRef<T> state) {
    try {
        if constexpr (std::is_void_v<T>) {
            co_await std::move(task);
            state->set_value();
        } else {
            state->set_value(co_await std::move(task));
        }
    } catch (...) {
        state->set_exception(std::current_exception());
    }
}

template <typename T>
JoinHandle<T> spawn_on(Scheduler& sched, Task<T> task) {
    detail::StateRef<T> state{new detail::JoinState<T>()};
    JoinHandle<T> handle{state};
    // `state` is copied into the runner's frame, which is what keeps the shared
    // state alive for exactly as long as either side still needs it.
    go_on(sched, root_runner<T>(std::move(task), state));
    return handle;
}

}  // namespace detail

// Spawn and keep a handle on the result.
template <typename T>
JoinHandle<T> spawn(Task<T> task) {
    return detail::spawn_on(detail::require_scheduler(), std::move(task));
}

// Yield the worker to whatever else is runnable — Go's runtime.Gosched().
// The task goes to the tail of the local queue, so everything already queued
// runs before it does.
inline auto yield() noexcept {
    struct Awaiter {
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const noexcept {
            detail::current_scheduler()->reschedule_self(h);
        }
        void await_resume() const noexcept {}
    };
    return Awaiter{};
}

}  // namespace cio
