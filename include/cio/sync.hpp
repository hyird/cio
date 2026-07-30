// Synchronisation primitives that suspend a task instead of blocking a thread.
//
// Reach for a channel first — these exist for the cases where a channel is the
// wrong shape, not as the default way to coordinate.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <mutex>
#include <utility>
#include <vector>
#include <stdexcept>
#include <utility>

#include "cio/detail/scheduler.hpp"

namespace cio {

namespace detail {

// Intrusive waiter for the primitives below; lives in the coroutine frame.
struct WaitNode {
    WaitNode* next = nullptr;
    std::coroutine_handle<> handle{};
    SchedulerTarget sched;
    WorkerId preferred_worker = kInvalidWorkerId;

    void arm(std::coroutine_handle<> h) noexcept {
        handle = h;
        Scheduler* const scheduler = current_scheduler();
        sched =
            scheduler == nullptr
                ? SchedulerTarget{}
                : scheduler->completion_target();
        preferred_worker = current_worker_id(scheduler);
    }
    void wake() noexcept {
        SchedulerTarget::dispatch_completion(
            sched, handle, preferred_worker);
    }
    void wake_handoff() noexcept {
        SchedulerTarget::dispatch_next(sched, handle);
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

    bool try_lock() { return try_acquire(); }

    // co_await m.lock() -> Guard
    [[nodiscard]] auto lock() noexcept {
        struct Awaiter {
            Mutex* owner;
            detail::WaitNode node{};

            bool await_ready() const noexcept {
                // Once a queue exists, ownership is handed directly between
                // waiters. Avoid hammering the held_ cache line with a CAS that
                // cannot succeed until the queue drains.
                return !owner->has_waiters_.load(std::memory_order_relaxed) &&
                       owner->try_acquire();
            }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(owner->mutex_);
                // unlock() clears held_ while holding this same queue lock.
                // Retrying after taking it closes the race between losing the
                // lock-free attempt and publishing our waiter.
                if (!owner->has_waiters_.load(std::memory_order_relaxed) &&
                    owner->try_acquire()) {
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
                owner->has_waiters_.store(true, std::memory_order_relaxed);
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
                if (head_ == nullptr) {
                    tail_ = nullptr;
                    has_waiters_.store(false, std::memory_order_relaxed);
                }
                // held_ stays true: ownership transfers directly to `next`.
            } else {
                held_.store(false, std::memory_order_release);
            }
        }
        if (next != nullptr) next->wake_handoff();
    }

private:
    bool try_acquire() noexcept {
        bool expected = false;
        return held_.compare_exchange_strong(expected, true, std::memory_order_acquire,
                                             std::memory_order_relaxed);
    }

    std::mutex mutex_;
    std::atomic<bool> held_{false};
    std::atomic<bool> has_waiters_{false};
    detail::WaitNode* head_ = nullptr;
    detail::WaitNode* tail_ = nullptr;
};


// Go's sync.RWMutex.
//
// Writer-preferring: a waiting writer blocks new readers, so a steady stream of
// readers cannot starve it. Go makes the same choice, and the alternative turns
// a read-heavy workload into a writer that never runs.
class RWMutex {
public:
    RWMutex() = default;
    RWMutex(const RWMutex&) = delete;
    RWMutex& operator=(const RWMutex&) = delete;

    class [[nodiscard]] ReadGuard {
    public:
        ReadGuard() = default;
        explicit ReadGuard(RWMutex* owner) noexcept : owner_(owner) {}
        ~ReadGuard() { if (owner_ != nullptr) owner_->unlock_read(); }

        ReadGuard(ReadGuard&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)) {}
        ReadGuard& operator=(ReadGuard&& other) noexcept {
            if (this != &other) {
                if (owner_ != nullptr) owner_->unlock_read();
                owner_ = std::exchange(other.owner_, nullptr);
            }
            return *this;
        }
        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

    private:
        RWMutex* owner_ = nullptr;
    };

    class [[nodiscard]] WriteGuard {
    public:
        WriteGuard() = default;
        explicit WriteGuard(RWMutex* owner) noexcept : owner_(owner) {}
        ~WriteGuard() { if (owner_ != nullptr) owner_->unlock(); }

        WriteGuard(WriteGuard&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)) {}
        WriteGuard& operator=(WriteGuard&& other) noexcept {
            if (this != &other) {
                if (owner_ != nullptr) owner_->unlock();
                owner_ = std::exchange(other.owner_, nullptr);
            }
            return *this;
        }
        WriteGuard(const WriteGuard&) = delete;
        WriteGuard& operator=(const WriteGuard&) = delete;

    private:
        RWMutex* owner_ = nullptr;
    };

    // Go's RLock. Suspends while a writer holds or is waiting for the lock.
    [[nodiscard]] auto lock_read() noexcept {
        struct Awaiter {
            RWMutex* self;
            detail::WaitNode node{};

            bool await_ready() const noexcept { return false; }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(self->mutex_);
                if (!self->writing_ && self->waiting_writers_ == 0) {
                    ++self->readers_;
                    return false;
                }
                node.arm(h);
                node.next = self->read_waiters_;
                self->read_waiters_ = &node;
                return true;
            }
            ReadGuard await_resume() const noexcept { return ReadGuard{self}; }
        };
        return Awaiter{this, {}};
    }

    // Go's Lock.
    [[nodiscard]] auto lock() noexcept {
        struct Awaiter {
            RWMutex* self;
            detail::WaitNode node{};

            bool await_ready() const noexcept { return false; }
            bool await_suspend(std::coroutine_handle<> h) {
                std::lock_guard<std::mutex> lock(self->mutex_);
                if (!self->writing_ && self->readers_ == 0) {
                    self->writing_ = true;
                    return false;
                }
                ++self->waiting_writers_;
                node.arm(h);
                node.next = self->write_waiters_;
                self->write_waiters_ = &node;
                return true;
            }
            WriteGuard await_resume() const noexcept { return WriteGuard{self}; }
        };
        return Awaiter{this, {}};
    }

    bool try_lock_read() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (writing_ || waiting_writers_ != 0) return false;
        ++readers_;
        return true;
    }

    bool try_lock() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (writing_ || readers_ != 0) return false;
        writing_ = true;
        return true;
    }

    void unlock_read() {
        detail::WaitNode* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (readers_ == 0) return;
            if (--readers_ != 0) return;
            to_wake = take_next_writer_locked();
        }
        detail::wake_list(to_wake);
    }

    void unlock() {
        detail::WaitNode* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!writing_) return;
            writing_ = false;
            // A waiting writer goes first; only when none is left do readers
            // proceed, which is what makes this writer-preferring.
            to_wake = take_next_writer_locked();
            if (to_wake == nullptr) {
                to_wake = std::exchange(read_waiters_, nullptr);
                for (detail::WaitNode* n = to_wake; n != nullptr; n = n->next) {
                    ++readers_;
                }
            }
        }
        detail::wake_list(to_wake);
    }

private:
    detail::WaitNode* take_next_writer_locked() noexcept {
        if (write_waiters_ == nullptr) return nullptr;
        detail::WaitNode* next = write_waiters_;
        write_waiters_ = next->next;
        next->next = nullptr;
        --waiting_writers_;
        writing_ = true;
        return next;
    }

    std::mutex mutex_;
    std::size_t readers_ = 0;
    std::size_t waiting_writers_ = 0;
    bool writing_ = false;
    detail::WaitNode* read_waiters_ = nullptr;
    detail::WaitNode* write_waiters_ = nullptr;
};

// Go's sync.Once.
//
// The callable may suspend, which sync.Once cannot express: every other task
// awaiting the same Once waits for the first to finish rather than racing past a
// half-initialised value.
class Once {
public:
    Once() = default;
    Once(const Once&) = delete;
    Once& operator=(const Once&) = delete;

    bool done() const noexcept { return done_.load(std::memory_order_acquire); }

    template <typename F>
    Task<void> call(F fn) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_.load(std::memory_order_acquire)) co_return;
            if (running_) {
                // Someone else is initialising. Wait for them rather than
                // running it a second time.
                auto gate = gate_;
                lock.unlock();
                (void)co_await gate.recv();
                co_return;
            }
            // The gate must exist before running_ is published, or a second
            // caller could observe running_ and find nothing to wait on.
            if (!static_cast<bool>(gate_)) gate_ = make_chan<Unit>(0);
            running_ = true;
        }

        // Outside the lock: fn may suspend, and holding a std::mutex across a
        // suspension would block a worker rather than the task.
        co_await std::move(fn)();

        Chan<Unit> gate;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            done_.store(true, std::memory_order_release);
            running_ = false;
            gate = gate_;
        }
        // Closing releases every waiter at once.
        gate.close();
        co_return;
    }

private:
    std::mutex mutex_;
    std::atomic<bool> done_{false};
    bool running_ = false;
    Chan<Unit> gate_{};
};

// Go's sync.Cond, over a cio Mutex.
//
// wait() releases the mutex, suspends, and reacquires it before returning, which
// is the contract that makes a condition variable usable. Spurious wakeups are
// possible, so callers must re-check their predicate in a loop, exactly as with
// sync.Cond or std::condition_variable.
class Cond {
public:
    explicit Cond(Mutex& mutex) noexcept : mutex_(&mutex) {}
    Cond(const Cond&) = delete;
    Cond& operator=(const Cond&) = delete;

    // Requires the mutex to be held; returns holding it again.
    Task<void> wait(Mutex::Guard& guard) {
        auto waiter = make_chan<Unit>(0);
        {
            std::lock_guard<std::mutex> lock(list_mutex_);
            waiters_.push_back(waiter);
        }
        guard = Mutex::Guard{};       // release
        (void)co_await waiter.recv();  // woken by notify
        guard = co_await mutex_->lock();
        co_return;
    }

    void notify_one() {
        Chan<Unit> next;
        {
            std::lock_guard<std::mutex> lock(list_mutex_);
            if (waiters_.empty()) return;
            next = waiters_.front();
            waiters_.erase(waiters_.begin());
        }
        next.close();
    }

    void notify_all() {
        std::vector<Chan<Unit>> all;
        {
            std::lock_guard<std::mutex> lock(list_mutex_);
            all.swap(waiters_);
        }
        for (auto& waiter : all) waiter.close();
    }

private:
    Mutex* mutex_ = nullptr;
    std::mutex list_mutex_;
    std::vector<Chan<Unit>> waiters_;
};

}  // namespace cio
