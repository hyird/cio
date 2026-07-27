// Task<T> — a lazy coroutine.
//
// Awaiting a Task starts it by symmetric transfer: the awaiting coroutine
// tail-calls straight into the child, and the child tail-calls straight back on
// completion. Neither transition goes through a run queue, so composing tasks
// costs a jump, not a scheduling round-trip. Only spawn/go put a task on the
// scheduler; plain composition never does.
//
// That "tail-call" is a requirement, not an optimisation: without it a loop
// like `for (;;) co_await handle(co_await listener.accept());` grows the stack
// on every iteration and eventually overflows. GCC needs
// -foptimize-sibling-calls for it (see CMakeLists.txt); clang emits a musttail
// unconditionally. Sanitizer builds disable sibling calls outright, so they
// cannot run unbounded coroutine loops — keep sanitizer runs short.
#pragma once

#include <concepts>
#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

#include "cio/config.hpp"
#include "cio/detail/frame_pool.hpp"

namespace cio {

template <typename T = void>
class Task;

namespace detail {

// Installed by the runtime; called when an exception escapes a detached task.
// Go's rule: an unrecovered panic in a goroutine takes down the process, and
// silently swallowing it is worse than crashing.
[[noreturn]] void abort_on_unhandled_exception(std::exception_ptr e) noexcept;

struct TaskPromiseBase {
    // Route every task frame through the per-thread pool. Only the sized delete
    // is declared: the coroutine machinery always knows the frame size, and
    // offering only this form means a compiler that wanted the unsized one
    // would fail loudly rather than silently bypassing the pool.
    static void* operator new(std::size_t size) { return FramePool::allocate(size); }
    static void operator delete(void* frame, std::size_t size) noexcept {
        FramePool::deallocate(frame, size);
    }

    struct FinalAwaiter {
        TaskPromiseBase* promise;

        bool await_ready() noexcept {
            if (promise->detached) {
                if (promise->exception) abort_on_unhandled_exception(promise->exception);
                // Not suspending here completes the coroutine, which destroys
                // its own frame. That is exactly what a detached task wants,
                // and it means go() costs no extra bookkeeping object.
                return true;
            }
            return false;
        }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<>) noexcept {
            return promise->continuation ? promise->continuation : std::noop_coroutine();
        }

        void await_resume() noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return FinalAwaiter{this}; }
    void unhandled_exception() noexcept { exception = std::current_exception(); }

    void rethrow_if_failed() {
        if (exception) std::rethrow_exception(exception);
    }

    std::coroutine_handle<> continuation{};
    std::exception_ptr exception{};
    bool detached = false;
};

template <typename T>
struct TaskPromise final : TaskPromiseBase {
    Task<T> get_return_object() noexcept;

    template <typename U = T>
        requires std::convertible_to<U&&, T>
    void return_value(U&& v) {
        value.emplace(std::forward<U>(v));
    }

    T&& result() {
        rethrow_if_failed();
        return std::move(*value);
    }

    std::optional<T> value;
};

template <>
struct TaskPromise<void> final : TaskPromiseBase {
    Task<void> get_return_object() noexcept;
    void return_void() noexcept {}
    void result() { rethrow_if_failed(); }
};

}  // namespace detail

template <typename T>
class [[nodiscard]] Task {
public:
    using promise_type = detail::TaskPromise<T>;
    using value_type = T;
    using handle_type = std::coroutine_handle<promise_type>;

    Task() noexcept = default;
    explicit Task(handle_type handle) noexcept : handle_(handle) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() { destroy(); }

    bool valid() const noexcept { return static_cast<bool>(handle_); }
    bool done() const noexcept { return handle_ && handle_.done(); }

    // Relinquishes ownership of the frame. Used by go()/spawn(), which hand the
    // frame to the scheduler.
    handle_type release() noexcept { return std::exchange(handle_, {}); }

    auto operator co_await() const& noexcept { return Awaiter{handle_}; }
    auto operator co_await() const&& noexcept { return Awaiter{handle_}; }

private:
    struct Awaiter {
        handle_type coro;

        bool await_ready() const noexcept { return !coro || coro.done(); }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
            coro.promise().continuation = awaiting;
            return coro;  // tail-call into the child
        }

        decltype(auto) await_resume() { return coro.promise().result(); }
    };

    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    handle_type handle_{};
};

namespace detail {

template <typename T>
inline Task<T> TaskPromise<T>::get_return_object() noexcept {
    return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
    return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

// Holds a T, or nothing when T is void. Used wherever a result has to be moved
// across a suspension boundary.
template <typename T>
struct ValueSlot {
    std::optional<T> value;

    template <typename U>
    void set(U&& v) {
        value.emplace(std::forward<U>(v));
    }
    T take() { return std::move(*value); }
};

template <>
struct ValueSlot<void> {
    void set() noexcept {}
    void take() const noexcept {}
};

}  // namespace detail
}  // namespace cio
