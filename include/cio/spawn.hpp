// spawn / go — putting a task on the scheduler.
//
// Two flavours, because they have genuinely different costs:
//
//   go(t)     fire and forget, exactly like `go f()`. The task's own frame is
//             the only allocation; it destroys itself on completion.
//   spawn(t)  returns a JoinHandle you can co_await for the result. This one
//             needs a shared completion slot. The task's final suspend writes
//             that slot directly, so joining does not add a wrapper frame.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/detail/frame_pool.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

struct JoinWait {
    std::coroutine_handle<> handle{};
    SchedulerTarget sched;
    WorkerId preferred_worker = kInvalidWorkerId;
};

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
class JoinState : public DetachedTaskCompletion {
public:
    explicit JoinState(std::uint32_t initial_refs) noexcept
        : DetachedTaskCompletion{&complete_task},
          refs_(initial_refs) {}

    // Same pool as the coroutine frames: spawn() allocates one of these per
    // call, on the same threads and with the same lifetime pattern.
    static void* operator new(std::size_t size) { return FramePool::allocate(size); }
    static void operator delete(void* state, std::size_t size) noexcept {
        FramePool::deallocate(state, size);
    }

    void add_ref() noexcept {
        const std::uint32_t previous =
            refs_.fetch_add(1, std::memory_order_relaxed);
        if (previous == std::numeric_limits<std::uint32_t>::max()) {
            std::terminate();
        }
    }
    void release() noexcept {
        const std::uint32_t previous =
            refs_.fetch_sub(1, std::memory_order_acq_rel);
        if (previous == 1) delete this;
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

    bool completed_successfully() const noexcept {
        // set_value()/set_exception() publishes all result state before
        // complete() release-publishes kDone. The acquire in completed()
        // therefore makes this non-atomic exception read race-free.
        return completed() && !exception_;
    }

    enum class ParkResult { kParked, kCompleted, kAlreadyWaiting };

    ParkResult try_park(JoinWait* wait, std::coroutine_handle<> joiner) noexcept {
        wait->handle = joiner;
        Scheduler* const scheduler = current_scheduler();
        wait->sched =
            scheduler == nullptr
                ? SchedulerTarget{}
                : scheduler->completion_target();
        wait->preferred_worker = current_worker_id(scheduler);
        void* expected = nullptr;
        if (waiter_.compare_exchange_strong(expected, wait,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
            return ParkResult::kParked;
        }
        return expected == done_sentinel() ? ParkResult::kCompleted
                                           : ParkResult::kAlreadyWaiting;
    }

    T take() {
        if (exception_) std::rethrow_exception(exception_);
        return slot_.take();
    }

private:
    static void complete_task(
        TaskPromiseBase& base,
        DetachedTaskCompletion& completion) noexcept {
        auto& self = static_cast<JoinState&>(completion);
        auto& promise = static_cast<TaskPromise<T>&>(base);

        try {
            if (promise.exception) {
                self.set_exception(promise.exception);
            } else if constexpr (std::is_void_v<T>) {
                self.set_value();
            } else {
                self.set_value(std::move(*promise.value));
            }
        } catch (...) {
            // Moving the completed result into shared state is part of the
            // spawned task's result path, just as it was in the former wrapper
            // coroutine. Preserve that exception for the JoinHandle.
            self.set_exception(std::current_exception());
        }

        // The scheduled task owns one of the two seeded references. Its frame
        // is destroyed immediately after this callback returns.
        self.release();
    }

    static void* done_sentinel() noexcept {
        return reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));
    }

    void complete() noexcept {
        void* previous = waiter_.exchange(done_sentinel(), std::memory_order_acq_rel);
        if (previous != nullptr && previous != done_sentinel()) {
            auto* const wait = static_cast<JoinWait*>(previous);
            const SchedulerTarget sched = wait->sched;
            const WorkerId worker = wait->preferred_worker;
            const std::coroutine_handle<> handle = wait->handle;
            // Same-runtime completion stays local and publishes normally so
            // peers may steal. Cross-runtime completion targets the
            // Scheduler captured by the waiter and treats its old worker only
            // as an affinity hint.
            SchedulerTarget::dispatch(
                sched, handle, worker);
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
    struct AdoptTag {
        explicit AdoptTag() = default;
    };
    static constexpr AdoptTag adopt{};

    StateRef() noexcept = default;
    explicit StateRef(JoinState<T>* state) noexcept : state_(state) {
        if (state_) state_->add_ref();
    }
    StateRef(JoinState<T>* state, AdoptTag) noexcept : state_(state) {}
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
            detail::StateRef<T> state;
            detail::JoinWait wait{};
            bool rejected = false;
            bool snapshot_success = false;

            bool await_ready() const noexcept {
                return snapshot_success || rejected || !state ||
                       state->completed();
            }
            bool await_suspend(std::coroutine_handle<> joiner) noexcept {
                if (snapshot_success) return false;
                if (!state) return false;
                switch (state->try_park(&wait, joiner)) {
                    case detail::JoinState<T>::ParkResult::kParked:
                        return true;
                    case detail::JoinState<T>::ParkResult::kCompleted:
                        return false;
                    case detail::JoinState<T>::ParkResult::kAlreadyWaiting:
                        rejected = true;
                        return false;
                }
                return false;
            }
            T await_resume() {
                if constexpr (std::is_void_v<T>) {
                    if (snapshot_success) return;
                }
                if (rejected) {
                    throw std::logic_error("cio: JoinHandle already has a waiter");
                }
                // A default-constructed or detached JoinHandle has no state.
                if (!state) {
                    throw std::logic_error("cio: awaited an invalid JoinHandle");
                }
                return state->take();
            }
        };
        detail::JoinState<T>* const state = state_.get();
        if (state == nullptr) return Awaiter{{}, {}, false, false};
        if constexpr (std::is_void_v<T>) {
            // No result object needs to stay alive after a successful void
            // completion. Snapshotting that fact makes a saved awaiter
            // independent of the handle without paying two refcount RMWs.
            // Exceptional and incomplete states retain the owning path.
            if (state->completed_successfully()) {
                return Awaiter{{}, {}, false, true};
            }
        }
        // The awaiter owns a reference independently of the handle, so a saved
        // awaiter remains valid if the originating handle is detached. A
        // completed handle may still be awaited again, preserving the original
        // observable behaviour; only two simultaneously parked joiners are
        // rejected by try_park().
        return Awaiter{
            detail::StateRef<T>{state}, {}, false, false};
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
    if (!handle) throw std::invalid_argument("cio: cannot schedule an invalid Task");
    // A scheduler-detached task has no awaiting continuation. Clearing the
    // shared slot also makes accidental prior awaiter misuse fail closed
    // instead of being interpreted as a detached completion callback.
    handle.promise().continuation_or_completion = nullptr;
    handle.promise().detached = true;
    sched.schedule(handle);
}

namespace detail {

template <typename T>
inline void go_on(Scheduler& sched, Task<T> task) {
    auto handle = task.release();
    if (!handle) throw std::invalid_argument("cio: cannot schedule an invalid Task");
    handle.promise().continuation_or_completion = nullptr;
    handle.promise().detached = true;
    sched.schedule(handle);
}

// Cold compatibility path for an invalid Task or one that has already reached
// final_suspend. A done coroutine must never be resumed directly; composing it
// through this wrapper preserves spawn()'s established asynchronous completion
// and exception behaviour without taxing the ordinary lazy-task path.
template <typename T>
Task<void> completed_spawn_runner(Task<T> task, StateRef<T> state) {
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
    // Exactly two owners exist: the returned handle and the scheduled task.
    // Seed both references in the allocation, then let each StateRef adopt one.
    auto* raw = new detail::JoinState<T>(2);
    detail::StateRef<T> handle_state{raw, detail::StateRef<T>::adopt};
    JoinHandle<T> handle{std::move(handle_state)};

    if (!task.valid() || task.done()) {
        detail::StateRef<T> runner_state{
            raw, detail::StateRef<T>::adopt};
        go_on(
            sched,
            completed_spawn_runner(
                std::move(task), std::move(runner_state)));
        return handle;
    }

    // A spawned task is scheduler-detached, but unlike go() its exception and
    // result complete JoinState instead of taking the process down. The raw
    // completion pointer owns the second seeded reference until final suspend.
    auto child = task.release();
    child.promise().continuation_or_completion =
        static_cast<DetachedTaskCompletion*>(raw);
    child.promise().detached = true;
    sched.schedule(child);
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
