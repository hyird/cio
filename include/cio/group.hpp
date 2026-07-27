// TaskGroup — structured concurrency on top of the goroutine model.
//
//     cio::TaskGroup group;
//     for (auto& item : work) group.spawn(handle(item, group.token()));
//     co_await group.join();     // rethrows the first failure
//
// spawn/go are unstructured by design (that is what makes them feel like Go).
// TaskGroup is the opt-in that gives you a scope: join() does not return until
// every child has finished, and the first child to throw cancels the rest.
//
// Cancellation is cooperative and travels through a channel, exactly like
// Go's context.Done(). That means it composes with select for free:
//
//     switch (co_await cio::select(cio::recv(jobs), cio::recv(token.done()))) {
//         case 0: ...
//         case 1: co_return;   // cancelled
//     }
#pragma once

#include <atomic>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

#include "cio/chan.hpp"
#include "cio/spawn.hpp"
#include "cio/sync.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

struct CancelState {
    std::atomic<bool> cancelled{false};
    Chan<Unit> done = make_chan<Unit>(0);

    void cancel() {
        if (cancelled.exchange(true, std::memory_order_acq_rel)) return;
        // Closing wakes every parked receiver, including select cases.
        done.close();
    }
};

}  // namespace detail

// The read side of a cancellation signal. Copyable and cheap; pass it by value
// into child tasks the way you would pass a context.Context.
class CancelToken {
public:
    CancelToken() = default;
    explicit CancelToken(std::shared_ptr<detail::CancelState> state) noexcept
        : state_(std::move(state)) {}

    bool cancelled() const noexcept {
        return state_ != nullptr && state_->cancelled.load(std::memory_order_acquire);
    }

    // A channel that is closed when cancellation is requested. Put it in a
    // select to make any blocking operation cancellable.
    Chan<Unit> done() const {
        return state_ != nullptr ? state_->done : Chan<Unit>{};
    }

    explicit operator bool() const noexcept { return state_ != nullptr; }

private:
    std::shared_ptr<detail::CancelState> state_;
};

// The write side. Independent of TaskGroup so it can be used on its own.
class CancelSource {
public:
    CancelSource() : state_(std::make_shared<detail::CancelState>()) {}

    CancelToken token() const noexcept { return CancelToken{state_}; }
    void cancel() const { state_->cancel(); }
    bool cancelled() const noexcept {
        return state_->cancelled.load(std::memory_order_acquire);
    }

private:
    std::shared_ptr<detail::CancelState> state_;
};

namespace detail {

// Shared between the group and its children. Refcounted so that a group
// destroyed without joining leaks nothing and crashes nothing — the children
// simply finish into a state that no longer has a listener.
struct GroupState {
    std::mutex mutex;
    std::size_t outstanding = 0;
    WaitNode* waiters = nullptr;
    std::exception_ptr first_exception;
    CancelSource cancel;

    void child_started() {
        std::lock_guard<std::mutex> lock(mutex);
        ++outstanding;
    }

    void child_failed(std::exception_ptr e) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (first_exception) return;  // keep the first failure, like Go's errgroup
            first_exception = std::move(e);
        }
        // One child failing makes the rest pointless; tell them to stop.
        cancel.cancel();
    }

    void child_finished() {
        WaitNode* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (--outstanding > 0) return;
            to_wake = std::exchange(waiters, nullptr);
        }
        wake_list(to_wake);
    }
};

template <typename T>
Task<void> group_child(Task<T> task, std::shared_ptr<GroupState> state) {
    try {
        co_await std::move(task);
    } catch (...) {
        state->child_failed(std::current_exception());
    }
    state->child_finished();
}

}  // namespace detail

class TaskGroup {
public:
    TaskGroup() : state_(std::make_shared<detail::GroupState>()) {}

    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    ~TaskGroup() {
        // Not joining is a bug, but crashing here would be worse: cancel what
        // is left and let the refcounted state outlive us.
        if (!joined_) state_->cancel.cancel();
    }

    template <typename T>
    void spawn(Task<T> task) {
        state_->child_started();
        go(detail::group_child<T>(std::move(task), state_));
    }

    // Pass this into children so they can observe cancellation.
    CancelToken token() const noexcept { return state_->cancel.token(); }

    // Ask every child to stop. Cooperative: children must actually check.
    void cancel() const { state_->cancel.cancel(); }

    // Waits for every child. Rethrows the first exception a child raised, after
    // all of them have finished — never leaves a child running past the scope.
    [[nodiscard]] auto join() noexcept {
        struct Awaiter {
            TaskGroup* group;
            detail::WaitNode node{};

            bool await_ready() const noexcept {
                std::lock_guard<std::mutex> lock(group->state_->mutex);
                return group->state_->outstanding == 0;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(group->state_->mutex);
                if (group->state_->outstanding == 0) return false;
                node.arm(h);
                node.next = group->state_->waiters;
                group->state_->waiters = &node;
                return true;
            }
            void await_resume() const {
                group->joined_ = true;
                std::exception_ptr failure;
                {
                    std::lock_guard<std::mutex> lock(group->state_->mutex);
                    failure = group->state_->first_exception;
                }
                if (failure) std::rethrow_exception(failure);
            }
        };
        return Awaiter{this, {}};
    }

private:
    std::shared_ptr<detail::GroupState> state_;
    bool joined_ = false;
};

}  // namespace cio
