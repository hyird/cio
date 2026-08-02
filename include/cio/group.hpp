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
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "cio/chan.hpp"
#include "cio/clock.hpp"
#include "cio/spawn.hpp"
#include "cio/sync.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

struct CancelAccess;

template<typename T>
class GroupAllocator {
public:
    using value_type = T;

    GroupAllocator() noexcept = default;
    template<typename U>
    GroupAllocator(const GroupAllocator<U>&) noexcept {}

    T* allocate(std::size_t count) {
        static_assert(alignof(T) <= alignof(std::max_align_t));
        if (count > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length{};
        }
        return static_cast<T*>(FramePool::allocate(count * sizeof(T)));
    }

    void deallocate(T* block, std::size_t count) noexcept {
        FramePool::deallocate(block, count * sizeof(T));
    }

    template<typename U>
    friend bool operator==(GroupAllocator, GroupAllocator<U>) noexcept {
        return true;
    }
};

// Something to run when cancellation fires, for waiters that cannot select on
// a channel. A parked socket read is the motivating case: the flag alone gates
// the next syscall, but an operation already blocked in epoll has to be woken.
//
// Hooks are shared-owned so a hook outlives the object that registered it; a
// detached hook goes inert rather than being unlinked from under a firing
// cancel.
struct CancelHook {
    virtual ~CancelHook() = default;
    virtual void on_cancel() noexcept = 0;
};

struct CancelState {
    std::atomic<bool> cancelled{false};
    Chan<Unit> done;
    std::atomic<bool> done_ready{false};

    // When this state was given a deadline, the instant it fires. Zero means
    // none. Stored here so a token can report it without a second handle.
    std::atomic<std::int64_t> deadline_ns{0};
    // Type-erased slot for whatever a derived construction needs to keep alive
    // — a timer, a link to a parent. Held here rather than in the source so a
    // moved or copied token cannot outlive it.
    std::shared_ptr<void> keepalive;

    std::mutex hooks_mutex;
    std::vector<std::shared_ptr<CancelHook>> hooks;
    bool fired = false;

    Chan<Unit> done_channel() {
        if (done_ready.load(std::memory_order_acquire)) return done;

        Chan<Unit> result;
        {
            std::lock_guard<std::mutex> lock(hooks_mutex);
            if (!done_ready.load(std::memory_order_relaxed)) {
                done = make_chan<Unit>(0);
                // If cancellation already completed, establish the channel's
                // final state before publishing it to lock-free readers.
                if (fired) done.close();
                done_ready.store(true, std::memory_order_release);
            }
            result = done;
        }
        return result;
    }

    // False when cancellation already fired; the caller must act immediately
    // instead of waiting for a callback that will never come.
    bool add_hook(std::shared_ptr<CancelHook> hook) {
        std::lock_guard<std::mutex> lock(hooks_mutex);
        if (fired) return false;
        hooks.push_back(std::move(hook));
        return true;
    }

    void remove_hook(const std::shared_ptr<CancelHook>& hook) {
        std::lock_guard<std::mutex> lock(hooks_mutex);
        for (auto it = hooks.begin(); it != hooks.end(); ++it) {
            if (*it == hook) {
                hooks.erase(it);
                return;
            }
        }
    }

    void cancel() {
        if (cancelled.exchange(true, std::memory_order_acq_rel)) return;

        // Detach the list before running anything: a hook may re-enter this
        // state, and none of them may run under the lock.
        std::vector<std::shared_ptr<CancelHook>> to_fire;
        Chan<Unit> done_to_close;
        {
            std::lock_guard<std::mutex> lock(hooks_mutex);
            fired = true;
            to_fire.swap(hooks);
            done_to_close = done;
        }
        for (const auto& hook : to_fire) hook->on_cancel();

        // Closing wakes every parked receiver, including select cases.
        done_to_close.close();
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
        return state_ != nullptr &&
               state_->cancelled.load(std::memory_order_acquire);
    }

    // A channel that is closed when cancellation is requested. Put it in a
    // select to make any blocking operation cancellable. Go's ctx.Done().
    Chan<Unit> done() const {
        return state_ != nullptr ? state_->done_channel() : Chan<Unit>{};
    }

    // Why the token fired, or a success Error while it has not. Go's ctx.Err().
    // A token with no source never fires, so it reports success.
    //
    // A deadline that has elapsed reports Errc::timed_out rather than
    // Errc::cancelled, matching context.DeadlineExceeded: the two are different
    // outcomes and a caller decides differently on each.
    Error err() const noexcept {
        if (!cancelled()) return Error{};
        const std::int64_t at =
            state_ != nullptr
                ? state_->deadline_ns.load(std::memory_order_acquire)
                : 0;
        if (at != 0 && now_ns() >= at) return Error{Errc::timed_out};
        return Error{Errc::cancelled};
    }

    // The deadline this token carries, if any. Go's ctx.Deadline().
    std::optional<TimePoint> deadline() const noexcept {
        if (state_ == nullptr) return std::nullopt;
        const std::int64_t at =
            state_->deadline_ns.load(std::memory_order_acquire);
        if (at == 0) return std::nullopt;
        return TimePoint{std::chrono::nanoseconds{at}};
    }

    explicit operator bool() const noexcept { return state_ != nullptr; }

private:
    friend struct detail::CancelAccess;
    std::shared_ptr<detail::CancelState> state_;
};

namespace detail {

// Lets the runtime bind a token's flag to a descriptor without making the
// shared state public.
struct CancelAccess {
    static const std::shared_ptr<CancelState>& state(
        const CancelToken& token) noexcept {
        return token.state_;
    }
};

}  // namespace detail

// The write side. Independent of TaskGroup so it can be used on its own.
class CancelSource {
public:
    CancelSource() : state_(std::make_shared<detail::CancelState>()) {}

    CancelToken token() const noexcept { return CancelToken{state_}; }

    // Exposed so the context helpers can attach a deadline and a parent link
    // without making CancelState public.
    const std::shared_ptr<detail::CancelState>& state() const noexcept {
        return state_;
    }
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
    std::atomic<std::size_t> outstanding{0};
    WaitNode* waiters = nullptr;
    std::exception_ptr first_exception;
    std::atomic<bool> has_exception{false};
    CancelState cancel;

    void child_started() {
        // The finishing sentinel closes the only dangerous zero-transition
        // window: a last child that has decided to detach joiners but has not
        // acquired their lock yet. Every ordinary start, including 0 -> 1,
        // therefore needs only this CAS.
        std::size_t observed = outstanding.load(std::memory_order_relaxed);
        for (;;) {
            if (observed == kFinishing) {
                std::lock_guard<std::mutex> lock(mutex);
                observed = outstanding.load(std::memory_order_acquire);
                if (observed == kFinishing) {
                    // We reached the lock before the finishing child. Keep the
                    // existing generation alive and let it observe our start.
                    outstanding.store(1, std::memory_order_release);
                    return;
                }
                outstanding.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            if (outstanding.compare_exchange_weak(observed, observed + 1,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
                return;
            }
        }
    }

    void child_failed(std::exception_ptr e) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (first_exception)
                return;  // keep the first failure, like Go's errgroup
            first_exception = std::move(e);
            has_exception.store(true, std::memory_order_release);
        }
        // One child failing makes the rest pointless; tell them to stop.
        cancel.cancel();
    }

    void child_finished() {
        const std::size_t previous =
            outstanding.fetch_sub(1, std::memory_order_acq_rel);
        if (previous > 1) return;

        std::size_t expected = 0;
        if (!outstanding.compare_exchange_strong(expected, kFinishing,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
            return;
        }
        WaitNode* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex);
            // A child that reached this lock first replaced the sentinel with
            // one, cancelling this completion boundary.
            if (outstanding.load(std::memory_order_acquire) != kFinishing)
                return;
            to_wake = std::exchange(waiters, nullptr);
            outstanding.store(0, std::memory_order_release);
        }
        if (to_wake != nullptr && to_wake->next == nullptr) {
            // A TaskGroup join is released only by the final child. With one
            // joiner there is no child batch left to drain, so hand the worker
            // directly back to the structured parent.
            to_wake->wake_handoff();
        } else {
            wake_list(to_wake);
        }
    }

    static constexpr std::size_t kFinishing = ~std::size_t{0};
};

// The ordinary TaskGroup path does not need a wrapper coroutine: the child's
// final suspend can report failure and completion straight into the group.
// Each record owns one state reference so destroying the TaskGroup before its
// children finish remains safe.
struct GroupCompletion final : DetachedTaskCompletion {
    explicit GroupCompletion(std::shared_ptr<GroupState> group_state) noexcept
        : DetachedTaskCompletion{&complete_task},
          owner(std::move(group_state)) {}

    static void* operator new(std::size_t size) {
        return FramePool::allocate(size);
    }
    static void operator delete(void* completion, std::size_t size) noexcept {
        FramePool::deallocate(completion, size);
    }

    static void complete_task(TaskPromiseBase& promise,
                              DetachedTaskCompletion& completion) noexcept {
        auto& self = static_cast<GroupCompletion&>(completion);
        std::shared_ptr<GroupState> state = std::move(self.owner);
        delete &self;

        if (promise.exception) state->child_failed(promise.exception);
        state->child_finished();
    }

    std::shared_ptr<GroupState> owner;
};

template<typename T>
Task<void> group_child(Task<T> task, std::shared_ptr<GroupState> state) {
    try {
        co_await std::move(task);
    } catch (...) {
        state->child_failed(std::current_exception());
    }
    state->child_finished();
}

template<typename T>
void start_group_child(Task<T> task, std::shared_ptr<GroupState> state) {
    Scheduler& scheduler = require_scheduler();

    // Preserve Task's established invalid/completed semantics without ever
    // resuming a coroutine that is already at final suspend.
    if (!task.valid() || task.done()) {
        go_on(scheduler, group_child<T>(std::move(task), std::move(state)));
        return;
    }

    auto completion = std::make_unique<GroupCompletion>(std::move(state));
    auto child = task.release();
    child.promise().continuation_or_completion = completion.release();
    child.promise().detached = true;
    scheduler.schedule(child);
}

}  // namespace detail

class TaskGroup {
public:
    TaskGroup()
        : state_(std::allocate_shared<detail::GroupState>(
              detail::GroupAllocator<detail::GroupState>{})) {}

    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    ~TaskGroup() {
        // Not joining is a bug, but crashing here would be worse: cancel what
        // is left and let the refcounted state outlive us.
        if (!joined_) state_->cancel.cancel();
    }

    template<typename T>
    void spawn(Task<T> task) {
        state_->child_started();
        detail::start_group_child<T>(std::move(task), state_);
    }

    // Pass this into children so they can observe cancellation.
    CancelToken token() const noexcept {
        // The token aliases the group's control block, so cancellation state
        // stays alive independently without a second allocation or refcount.
        return CancelToken{
            std::shared_ptr<detail::CancelState>{state_, &state_->cancel}};
    }

    // Ask every child to stop. Cooperative: children must actually check.
    void cancel() const { state_->cancel.cancel(); }

    // Waits for every child. Rethrows the first exception a child raised, after
    // all of them have finished — never leaves a child running past the scope.
    [[nodiscard]] auto join() noexcept {
        struct Awaiter {
            explicit Awaiter(TaskGroup* owner) noexcept : group(owner) {}

            TaskGroup* group;
            detail::WaitNodeStorage node_storage;

            bool await_ready() const noexcept {
                return group->state_->outstanding.load(
                           std::memory_order_acquire) == 0;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(group->state_->mutex);
                if (group->state_->outstanding.load(
                        std::memory_order_acquire) == 0) {
                    return false;
                }
                detail::WaitNode* const node = node_storage.construct();
                node->arm(h);
                node->next = group->state_->waiters;
                group->state_->waiters = node;
                return true;
            }
            void await_resume() const {
                group->joined_ = true;
                if (!group->state_->has_exception.load(
                        std::memory_order_acquire)) {
                    return;
                }
                std::exception_ptr failure;
                {
                    std::lock_guard<std::mutex> lock(group->state_->mutex);
                    failure = group->state_->first_exception;
                }
                if (failure) std::rethrow_exception(failure);
            }
        };
        return Awaiter{this};
    }

private:
    std::shared_ptr<detail::GroupState> state_;
    bool joined_ = false;
};

}  // namespace cio
