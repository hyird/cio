// Synchronisation primitives that suspend a task instead of blocking a thread.
//
// Reach for a channel first — these exist for the cases where a channel is the
// wrong shape, not as the default way to coordinate.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cio/detail/scheduler.hpp"

namespace cio {

namespace detail {

// Intrusive waiter for the primitives below; lives in the coroutine frame.
struct WaitNode {
    WaitNode* next = nullptr;
    std::coroutine_handle<> handle{};
    Scheduler* sched = nullptr;

    void arm(std::coroutine_handle<> h) noexcept {
        handle = h;
        sched = current_scheduler();
    }
    void wake() noexcept {
        if (sched != nullptr) sched->schedule(handle);
    }
};

// Wakes an intrusive list, reading each `next` before the wake that may free it.
inline void wake_list(WaitNode* node) noexcept {
    while (node != nullptr) {
        WaitNode* next = node->next;
        node->wake();
        node = next;
    }
}

}  // namespace detail

// Go's sync.WaitGroup.
//
//     cio::WaitGroup wg;
//     wg.add(n);
//     for (...) cio::go(worker(wg));   // each calls wg.done()
//     co_await wg.wait();
class WaitGroup {
public:
    WaitGroup() = default;
    WaitGroup(const WaitGroup&) = delete;
    WaitGroup& operator=(const WaitGroup&) = delete;

    void add(std::ptrdiff_t delta = 1) {
        // Atomic fast path. done() is called once per task, so with a fan-out
        // of a million this is the hottest line in the program; taking a mutex
        // here turns a work-stealing scheduler into a queue at one lock.
        const std::ptrdiff_t updated =
            count_.fetch_add(delta, std::memory_order_acq_rel) + delta;
        if (updated > 0) return;
        if (updated < 0) throw std::logic_error("cio: WaitGroup counter went negative");

        detail::WaitNode* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            to_wake = std::exchange(waiters_, nullptr);
        }
        detail::wake_list(to_wake);
    }

    void done() { add(-1); }

    [[nodiscard]] auto wait() noexcept {
        struct Awaiter {
            WaitGroup* group;
            detail::WaitNode node{};

            bool await_ready() const noexcept {
                return group->count_.load(std::memory_order_acquire) == 0;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                // The lock orders us against the zero-transition in add(),
                // which collects waiters under the same lock: either we see
                // zero here, or add() sees us in the list.
                std::lock_guard<std::mutex> lock(group->mutex_);
                if (group->count_.load(std::memory_order_acquire) == 0) return false;
                node.arm(h);
                node.next = group->waiters_;
                group->waiters_ = &node;
                return true;
            }
            void await_resume() const noexcept {}
        };
        return Awaiter{this, {}};
    }

    std::ptrdiff_t count() const noexcept {
        return count_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::ptrdiff_t> count_{0};
    std::mutex mutex_;
    detail::WaitNode* waiters_ = nullptr;
};

// A mutex that suspends the task rather than the worker.
//
// Unlock hands ownership straight to the next waiter instead of releasing and
// letting everyone re-contend, which is what keeps a hot critical section from
// convoying under a work-stealing scheduler.
class Mutex {
public:
    Mutex() = default;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;

    class Guard {
    public:
        Guard() noexcept = default;
        explicit Guard(Mutex* mutex) noexcept : mutex_(mutex) {}
        Guard(Guard&& other) noexcept : mutex_(std::exchange(other.mutex_, nullptr)) {}
        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                release();
                mutex_ = std::exchange(other.mutex_, nullptr);
            }
            return *this;
        }
        ~Guard() { release(); }

        void release() noexcept {
            if (mutex_ != nullptr) {
                mutex_->unlock();
                mutex_ = nullptr;
            }
        }

    private:
        Mutex* mutex_ = nullptr;
    };

    bool try_lock() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (held_) return false;
        held_ = true;
        return true;
    }

    // co_await m.lock() -> Guard
    [[nodiscard]] auto lock() noexcept {
        struct Awaiter {
            Mutex* owner;
            detail::WaitNode node{};

            bool await_ready() const noexcept { return false; }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(owner->mutex_);
                if (!owner->held_) {
                    owner->held_ = true;
                    return false;
                }
                node.arm(h);
                node.next = nullptr;
                if (owner->tail_ != nullptr) {
                    owner->tail_->next = &node;
                } else {
                    owner->head_ = &node;
                }
                owner->tail_ = &node;
                return true;
            }
            Guard await_resume() const noexcept { return Guard{owner}; }
        };
        return Awaiter{this, {}};
    }

    void unlock() {
        detail::WaitNode* next = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            next = head_;
            if (next != nullptr) {
                head_ = next->next;
                if (head_ == nullptr) tail_ = nullptr;
                // held_ stays true: ownership transfers directly to `next`.
            } else {
                held_ = false;
            }
        }
        if (next != nullptr) next->wake();
    }

private:
    std::mutex mutex_;
    bool held_ = false;
    detail::WaitNode* head_ = nullptr;
    detail::WaitNode* tail_ = nullptr;
};

}  // namespace cio
