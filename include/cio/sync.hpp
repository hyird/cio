// Synchronisation primitives that suspend a task instead of blocking a thread.
//
// Reach for a channel first — these exist for the cases where a channel is the
// wrong shape, not as the default way to coordinate.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/chan.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

// Intrusive waiter for the primitives below; lives in the coroutine frame.
struct WaitNode {
    struct ArmTag {
        explicit ArmTag() = default;
    };

    WaitNode() noexcept = default;

    WaitNode(ArmTag, std::coroutine_handle<> h) noexcept : handle(h) {
        capture_current_scheduler(sched, preferred_worker);
    }

    WaitNode* next = nullptr;
    std::coroutine_handle<> handle{};
    SchedulerTarget sched;
    WorkerId preferred_worker = kInvalidWorkerId;

    void arm(std::coroutine_handle<> h) noexcept {
        handle = h;
        capture_current_scheduler(sched, preferred_worker);
    }
    void wake() noexcept {
        SchedulerTarget::dispatch_completion(sched, handle, preferred_worker);
    }
    void wake_handoff() noexcept {
        SchedulerTarget::dispatch_next(sched, handle);
    }
};

// WaitGroup, Mutex, and RWMutex construct this node in coroutine-frame storage
// only after their lock-free ready path misses. A trivial destructor lets the
// frame reclaim that storage without tracking whether await_suspend built it.
static_assert(std::is_trivially_destructible_v<WaitNode>);

inline constexpr std::uintptr_t kWaiterOwnerFlag = 1;
static_assert((alignof(std::mutex) & kWaiterOwnerFlag) == 0);

// Raw coroutine-frame storage for a waiter that is constructed only after an
// awaiter's lock-free path misses. Copying or moving an unpublished awaiter
// produces fresh empty storage instead of reading indeterminate bytes.
class WaitNodeStorage {
public:
    WaitNodeStorage() noexcept {}
    WaitNodeStorage(const WaitNodeStorage&) noexcept {}
    WaitNodeStorage(WaitNodeStorage&&) noexcept {}
    WaitNodeStorage& operator=(const WaitNodeStorage&) noexcept {
        return *this;
    }
    WaitNodeStorage& operator=(WaitNodeStorage&&) noexcept { return *this; }

    WaitNode* construct(std::coroutine_handle<> h) noexcept {
        return ::new (static_cast<void*>(storage_))
            WaitNode(WaitNode::ArmTag{}, h);
    }
    WaitNode* construct() noexcept {
        return ::new (static_cast<void*>(storage_)) WaitNode;
    }

private:
    alignas(WaitNode) std::byte storage_[sizeof(WaitNode)];
};

// These primitives protect only short intrusive waiter lists and ownership
// hand-offs. Keep their public objects' established layouts while avoiding
// pthread mutex machinery on each queue transition.
class alignas(std::mutex) WaiterQueueMutex : public ChannelMutex {
private:
    [[maybe_unused]] std::byte
        layout_padding_[sizeof(std::mutex) - sizeof(ChannelMutex)]{};
};
static_assert(sizeof(WaiterQueueMutex) == sizeof(std::mutex));
static_assert(alignof(WaiterQueueMutex) == alignof(std::mutex));

// Wakes an intrusive list, reading each `next` before the wake that may free
// it.
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
        if (delta == 0) return;

        if (delta > 0) {
            add_positive(static_cast<State>(delta));
            return;
        }

        const State magnitude = State{0} - static_cast<State>(delta);
        if (magnitude > kMaxCount)
            throw std::logic_error("cio: WaitGroup counter went negative");

        // Encoding positive count N as N+1 reserves state 1 for the final
        // zero transition. done() therefore remains one atomic RMW even with
        // many producers, without ever publishing zero before its cleanup.
        const State prior =
            state_.fetch_sub(magnitude, std::memory_order_acq_rel);
        const State prior_count = prior > kFinishing ? prior - 1 : 0;
        if (prior_count > magnitude) return;
        if (prior_count < magnitude)
            throw std::logic_error("cio: WaitGroup counter went negative");

        detail::WaitNode* to_wake = nullptr;
        {
            std::lock_guard<detail::ChannelMutex> lock(mutex_);
            if (waiters_ != &drained_marker_) to_wake = waiters_;
            waiters_ = &drained_marker_;
        }

        // State 1 means zero is being finalized. Publish stable zero only
        // after releasing mutex_, so a resumed waiter can immediately reuse
        // or destroy the group without racing this transition.
        state_.store(0, std::memory_order_release);
        if (to_wake != nullptr && to_wake->next == nullptr) {
            // The zero transition is the exact completion event this sole
            // waiter needs; retain its cache-hot continuation in runnext.
            to_wake->wake_handoff();
        } else {
            detail::wake_list(to_wake);
        }
    }

    void done() { add(-1); }

    [[nodiscard]] auto wait() noexcept {
        struct Awaiter {
            WaitGroup* group;
            detail::WaitNodeStorage node_storage;

            bool await_ready() const noexcept {
                return group->state_.load(std::memory_order_acquire) == 0;
            }
            bool await_suspend(std::coroutine_handle<> h) {
                return group->park_wait(node_storage, h);
            }
            void await_resume() const noexcept {}
        };
        return Awaiter{this, {}};
    }

    std::ptrdiff_t count() const noexcept {
        const State state = state_.load(std::memory_order_acquire);
        return state > kFinishing ? static_cast<std::ptrdiff_t>(state - 1) : 0;
    }

private:
    using State = std::uintptr_t;
    static constexpr State kFinishing = 1;
    static constexpr State kMaxCount =
        static_cast<State>(std::numeric_limits<std::ptrdiff_t>::max());

    CIO_NOINLINE bool park_wait(detail::WaitNodeStorage& storage,
                                std::coroutine_handle<> h) {
        detail::WaitNode* const node = storage.construct();
        std::unique_lock<detail::ChannelMutex> lock(mutex_);
        const State state = state_.load(std::memory_order_acquire);
        if (state <= kFinishing) {
            if (state == 0) return false;
            if (waiters_ != &drained_marker_) {
                // done() published its finishing state but has not yet
                // acquired mutex_. Join the batch it is about to extract
                // instead of spinning until completion.
                node->arm(h);
                node->next = waiters_;
                waiters_ = node;
                return true;
            }

            // The last done() has claimed the waiter list but has not yet
            // published stable zero. It cannot finish while we hold mutex_.
            // Release it before waiting for that very short transition.
            lock.unlock();
            while (state_.load(std::memory_order_acquire) == kFinishing) {
                cpu_relax();
            }
            return false;
        }
        if (waiters_ == &drained_marker_) waiters_ = nullptr;
        node->arm(h);
        node->next = waiters_;
        waiters_ = node;
        return true;
    }

    void add_positive(State amount) {
        State state = state_.load(std::memory_order_acquire);
        for (;;) {
            if (state == kFinishing) {
                throw std::logic_error(
                    "cio: WaitGroup add called concurrently with wait");
            }
            const State count = state == 0 ? 0 : state - 1;
            if (amount > kMaxCount - count) {
                throw std::logic_error("cio: WaitGroup counter went negative");
            }
            const State desired = state == 0 ? amount + 1 : state + amount;
            if (state_.compare_exchange_weak(state, desired,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
                return;
            }
        }
    }

    // Zero is stable, one is the final zero transition, and larger values
    // encode count+1. This keeps done() to one RMW while preventing wait() from
    // observing zero until the last done() has stopped touching the group.
    std::atomic<State> state_{0};
    detail::WaiterQueueMutex mutex_;
    detail::WaitNode* waiters_ = nullptr;
    inline static detail::WaitNode drained_marker_{};
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
        explicit Guard(Mutex* mutex) noexcept : mutex_(to_bits(mutex)) {}
        Guard(Guard&& other) noexcept
            : mutex_(std::exchange(other.mutex_, 0)) {}
        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                release();
                mutex_ = std::exchange(other.mutex_, 0);
            }
            return *this;
        }
        ~Guard() { release(); }

        void release() noexcept {
            const std::uintptr_t tagged = std::exchange(mutex_, 0);
            if (tagged == 0) return;
            Mutex* const mutex = reinterpret_cast<Mutex*>(tagged & ~kContended);
            if ((tagged & kContended) != 0) {
                mutex->unlock_contended();
            } else {
                mutex->unlock();
            }
        }

    private:
        friend class Mutex;

        Guard(Mutex* mutex, bool contended) noexcept
            : mutex_(to_bits(mutex) | (contended ? kContended : 0)) {
            static_assert(alignof(std::mutex) > kContended);
            if ((to_bits(mutex) & kContended) != 0) std::terminate();
        }

        static std::uintptr_t to_bits(Mutex* mutex) noexcept {
            return reinterpret_cast<std::uintptr_t>(mutex);
        }

        static constexpr std::uintptr_t kContended = 1;
        std::uintptr_t mutex_ = 0;
    };

    bool try_lock() { return try_acquire(); }

    // co_await m.lock() -> Guard
    [[nodiscard]] auto lock() noexcept {
        struct Awaiter {
            explicit Awaiter(Mutex* mutex) noexcept
                : owner_bits(reinterpret_cast<std::uintptr_t>(mutex)) {}

            std::uintptr_t owner_bits;
            detail::WaitNodeStorage node_storage;

            Mutex* owner() const noexcept {
                return reinterpret_cast<Mutex*>(owner_bits &
                                                ~detail::kWaiterOwnerFlag);
            }

            bool await_ready() noexcept { return owner()->try_acquire(); }
            bool await_suspend(std::coroutine_handle<> h) {
                detail::WaitNode* const node = node_storage.construct();
                Mutex* const mutex = owner();
                std::lock_guard<detail::ChannelMutex> lock(mutex->mutex_);
                // Publish waiter intent before enqueueing. unlock() can take
                // its lock-free path only while the state is exactly locked;
                // once this bit is visible it must enter the same queue lock,
                // closing the publication-versus-unlock race.
                std::uint8_t previous =
                    mutex->state_.load(std::memory_order_relaxed);
                if ((previous & kContended) == 0) {
                    previous = mutex->state_.fetch_or(
                        kContended, std::memory_order_acquire);
                }
                if ((previous & kLocked) == 0) {
                    // unlock() won before our waiter publication. The waiter
                    // bit excludes a barging fast-path acquirer while we turn
                    // that released state into ownership.
                    mutex->state_.store(kLocked | kContended,
                                        std::memory_order_relaxed);
                    owner_bits |= detail::kWaiterOwnerFlag;
                    return false;
                }
                owner_bits |= detail::kWaiterOwnerFlag;
                node->arm(h);
                node->next = nullptr;
                if (mutex->tail_ != nullptr) {
                    mutex->tail_->next = node;
                } else {
                    mutex->head_ = node;
                }
                mutex->tail_ = node;
                return true;
            }
            Guard await_resume() const noexcept {
                return owner()->make_guard(
                    (owner_bits & detail::kWaiterOwnerFlag) != 0);
            }
        };
        return Awaiter{this};
    }

    void unlock() {
        if ((state_.load(std::memory_order_relaxed) & kContended) == 0) {
            std::uint8_t expected = kLocked;
            if (state_.compare_exchange_strong(expected, 0,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                return;
            }
        }

        unlock_contended();
    }

private:
    static constexpr std::uint8_t kLocked = 1;
    static constexpr std::uint8_t kContended = 2;

    Guard make_guard(bool contended) noexcept { return Guard{this, contended}; }

    void unlock_contended() {
        detail::WaitNode* next = nullptr;
        {
            std::lock_guard<detail::ChannelMutex> lock(mutex_);
            next = head_;
            if (next != nullptr) {
                head_ = next->next;
                if (head_ == nullptr) {
                    tail_ = nullptr;
                }
            } else {
                // No ownership is being handed off. Re-enable the lock-free
                // path; a concurrent first waiter holds this queue lock before
                // publishing its contended bit, so it will observe or race
                // this release without falling between state and queue.
                state_.store(0, std::memory_order_release);
            }
            // During direct handoff state_ remains locked|contended, so no
            // atomic write is needed while a waiter exists.
        }
        if (next != nullptr) next->wake_handoff();
    }

    bool try_acquire() noexcept {
        // kContended without kLocked is the first waiter publishing intent
        // under mutex_; it is not a stable unlocked state and must not be
        // acquired by a barging try_lock(). Only exact zero is available.
        if (state_.load(std::memory_order_relaxed) != 0) return false;
        std::uint8_t expected = 0;
        return state_.compare_exchange_strong(expected, kLocked,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed);
    }

    detail::WaiterQueueMutex mutex_;
    std::atomic<std::uint8_t> state_{0};
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
        ~ReadGuard() {
            if (owner_ != nullptr) owner_->runlock();
        }

        ReadGuard(ReadGuard&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)) {}
        ReadGuard& operator=(ReadGuard&& other) noexcept {
            if (this != &other) {
                if (owner_ != nullptr) owner_->runlock();
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
        ~WriteGuard() {
            if (owner_ != nullptr) owner_->unlock();
        }

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

    // Go's RLock, snake-cased as one word the way the Go community
    // pronounces it. Suspends while a writer holds or is waiting for the lock.
    [[nodiscard]] auto rlock() noexcept {
        struct Awaiter {
            explicit Awaiter(RWMutex* mutex) noexcept
                : self_bits(reinterpret_cast<std::uintptr_t>(mutex)) {}

            std::uintptr_t self_bits;
            detail::WaitNodeStorage node_storage;

            RWMutex* self() const noexcept {
                return reinterpret_cast<RWMutex*>(self_bits &
                                                  ~detail::kWaiterOwnerFlag);
            }

            bool await_ready() const noexcept {
                return self()->try_acquire_read();
            }
            bool await_suspend(std::coroutine_handle<> h) {
                RWMutex* const mutex = self();
                std::lock_guard<detail::ChannelMutex> lock(mutex->mutex_);
                // Retrying under the queue lock closes the gap between the
                // lock-free attempt and publishing this waiter. Publishing
                // queue intent in the same atomic state also forces a racing
                // writer unlock onto this queue lock before we enqueue.
                if (mutex->acquire_read_or_publish_pending()) return false;
                detail::WaitNode* const node = node_storage.construct(h);
                node->next = mutex->read_waiters_;
                mutex->read_waiters_ = node;
                self_bits |= detail::kWaiterOwnerFlag;
                return true;
            }
            ReadGuard await_resume() const noexcept {
                RWMutex* const mutex = self();
                if ((self_bits & detail::kWaiterOwnerFlag) != 0) {
                    (void)mutex->state_.load(std::memory_order_acquire);
                }
                return ReadGuard{mutex};
            }
        };
        return Awaiter{this};
    }

    // Go's Lock.
    [[nodiscard]] auto lock() noexcept {
        struct Awaiter {
            explicit Awaiter(RWMutex* mutex) noexcept
                : self_bits(reinterpret_cast<std::uintptr_t>(mutex)) {}

            std::uintptr_t self_bits;
            detail::WaitNodeStorage node_storage;

            RWMutex* self() const noexcept {
                return reinterpret_cast<RWMutex*>(self_bits &
                                                  ~detail::kWaiterOwnerFlag);
            }

            bool await_ready() const noexcept {
                return self()->try_acquire_write();
            }
            bool await_suspend(std::coroutine_handle<> h) {
                RWMutex* const mutex = self();
                std::lock_guard<detail::ChannelMutex> lock(mutex->mutex_);
                // Either take a lock that became idle since await_ready(), or
                // atomically block the last active reader from leaving before
                // it observes that a writer needs the hand-off.
                if (mutex->acquire_write_or_publish_pending()) return false;
                ++mutex->waiting_writers_;
                detail::WaitNode* const node = node_storage.construct(h);
                node->next = mutex->write_waiters_;
                mutex->write_waiters_ = node;
                self_bits |= detail::kWaiterOwnerFlag;
                return true;
            }
            WriteGuard await_resume() const noexcept {
                RWMutex* const mutex = self();
                if ((self_bits & detail::kWaiterOwnerFlag) != 0) {
                    (void)mutex->state_.load(std::memory_order_acquire);
                }
                return WriteGuard{mutex};
            }
        };
        return Awaiter{this};
    }

    bool try_rlock() noexcept { return try_acquire_read(); }

    bool try_lock() noexcept { return try_acquire_write(); }

    void runlock() {
        std::size_t observed = state_.load(std::memory_order_relaxed);
        std::size_t next = 0;
        for (;;) {
            if ((observed & kWriter) != 0 || (observed & kReaderMask) == 0) {
                return;
            }
            next = observed - kReaderOne;
            if (state_.compare_exchange_weak(observed, next,
                                             std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
                break;
            }
        }
        if (next != kWriterPending) return;

        detail::WaitNode* to_wake;
        {
            std::lock_guard<detail::ChannelMutex> lock(mutex_);
            to_wake = take_next_writer_locked();
            if (to_wake == nullptr) {
                // A pending bit without its waiter would otherwise strand
                // future readers. Defensive only: writers publish both while
                // holding this same queue lock.
                state_.store(0, std::memory_order_release);
            } else {
                publish_writer_handoff();
            }
        }
        detail::wake_list(to_wake);
    }

    void unlock() {
        std::size_t expected = kWriter;
        if (state_.compare_exchange_strong(expected, 0,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
            return;
        }

        detail::WaitNode* to_wake = nullptr;
        {
            std::lock_guard<detail::ChannelMutex> lock(mutex_);
            if ((state_.load(std::memory_order_relaxed) & kWriter) == 0) {
                return;
            }
            // A waiting writer goes first; only when none is left do readers
            // proceed, which is what makes this writer-preferring.
            to_wake = take_next_writer_locked();
            if (to_wake != nullptr) {
                publish_writer_handoff();
            } else {
                to_wake = std::exchange(read_waiters_, nullptr);
                std::size_t readers = 0;
                for (detail::WaitNode* n = to_wake; n != nullptr; n = n->next) {
                    ++readers;
                }
                state_.store(readers * kReaderOne, std::memory_order_release);
            }
        }
        detail::wake_list(to_wake);
    }

private:
    static constexpr std::size_t kWriter = 1;
    static constexpr std::size_t kWriterPending = 2;
    static constexpr std::size_t kReaderOne = 4;
    static constexpr std::size_t kReaderMask = ~std::size_t{3};

    bool try_acquire_read() noexcept {
        std::size_t observed = state_.load(std::memory_order_relaxed);
        while ((observed & (kWriter | kWriterPending)) == 0) {
            if (state_.compare_exchange_weak(observed, observed + kReaderOne,
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    bool try_acquire_write() noexcept {
        std::size_t expected = 0;
        return state_.compare_exchange_strong(expected, kWriter,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed);
    }

    // Called with mutex_ held. Either acquire a lock released before waiter
    // publication, or publish queue intent before returning false. A writer
    // unlock therefore cannot take its exact-kWriter fast path while this
    // reader is between deciding to wait and entering read_waiters_.
    bool acquire_read_or_publish_pending() noexcept {
        std::size_t observed = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((observed & (kWriter | kWriterPending)) == 0) {
                if (state_.compare_exchange_weak(
                        observed, observed + kReaderOne,
                        std::memory_order_acquire, std::memory_order_relaxed)) {
                    return true;
                }
                continue;
            }
            if ((observed & kWriterPending) != 0) return false;
            if (state_.compare_exchange_weak(
                    observed, observed | kWriterPending,
                    std::memory_order_release, std::memory_order_relaxed)) {
                return false;
            }
        }
    }

    // Called with mutex_ held. A separate retry followed by fetch_or() has a
    // lost-wakeup window: the final reader can publish zero between them and
    // return before the pending bit appears. This loop makes zero-to-writer
    // and active-to-pending the only two possible outcomes.
    bool acquire_write_or_publish_pending() noexcept {
        std::size_t observed = state_.load(std::memory_order_relaxed);
        for (;;) {
            if (observed == 0) {
                if (state_.compare_exchange_weak(observed, kWriter,
                                                 std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    return true;
                }
                continue;
            }
            if (state_.compare_exchange_weak(
                    observed, observed | kWriterPending,
                    std::memory_order_release, std::memory_order_relaxed)) {
                return false;
            }
        }
    }

    void publish_writer_handoff() noexcept {
        const bool has_waiters =
            waiting_writers_ != 0 || read_waiters_ != nullptr;
        state_.store(kWriter | (has_waiters ? kWriterPending : 0),
                     std::memory_order_release);
    }

    detail::WaitNode* take_next_writer_locked() noexcept {
        if (write_waiters_ == nullptr) return nullptr;
        detail::WaitNode* next = write_waiters_;
        write_waiters_ = next->next;
        next->next = nullptr;
        --waiting_writers_;
        return next;
    }

    detail::WaiterQueueMutex mutex_;
    std::atomic<std::size_t> state_{0};
    std::size_t waiting_writers_ = 0;
    // Kept to preserve this public type's established size and member offsets.
    [[maybe_unused]] bool writing_ = false;
    detail::WaitNode* read_waiters_ = nullptr;
    detail::WaitNode* write_waiters_ = nullptr;
};

// Go's sync.Once.
//
// The callable may suspend, which sync.Once cannot express: every other task
// awaiting the same Once waits for the first to finish rather than racing past
// a half-initialised value.
class Once {
public:
    Once() = default;
    Once(const Once&) = delete;
    Once& operator=(const Once&) = delete;

    bool done() const noexcept { return done_.load(std::memory_order_acquire); }

    template<typename F>
    Task<void> call(F fn) {
        // Keep this entry point a non-coroutine dispatcher. Otherwise every
        // completed call reserves the mutex and gate state in its frame even
        // though the permanent done bit makes that state unreachable.
        // Completion is permanent. Keep the common repeated-call path off the
        // queue mutex; the acquire pairs with the initialiser's release store
        // so callers also observe everything it published.
        if (done_.load(std::memory_order_acquire)) {
            return completed_call(std::move(fn));
        }
        return call_slow(std::move(fn));
    }

private:
    template<typename F>
    static Task<void> completed_call(F fn) {
        (void)fn;
        co_return;
    }

    template<typename F>
    Task<void> call_slow(F fn) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (done_.load(std::memory_order_relaxed)) co_return;
            if (running_) {
                // Someone else is initialising. Wait for them rather than
                // running it a second time. The first uncontended call does
                // not need a gate at all; construct it only when a waiter
                // actually appears. Both publication and the initialiser's
                // completion copy happen under this lock, so no wake can be
                // missed.
                if (!static_cast<bool>(gate_)) gate_ = make_chan<Unit>(0);
                auto gate = gate_;
                lock.unlock();
                (void)co_await gate.recv();
                co_return;
            }
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

    std::mutex mutex_;
    std::atomic<bool> done_{false};
    bool running_ = false;
    Chan<Unit> gate_{};
};

// Go's sync.Cond, over a cio Mutex.
//
// wait() releases the mutex, suspends, and reacquires it before returning,
// which is the contract that makes a condition variable usable. Spurious
// wakeups are possible, so callers must re-check their predicate in a loop,
// exactly as with sync.Cond. wait() takes the guard because C++ scopes the lock
// in an object where Go scopes it in convention; signal() and broadcast() are
// Go's names.
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
        guard = Mutex::Guard{};        // release
        (void)co_await waiter.recv();  // woken by notify
        guard = co_await mutex_->lock();
        co_return;
    }

    // Go's Signal.
    void signal() {
        Chan<Unit> next;
        {
            std::lock_guard<std::mutex> lock(list_mutex_);
            if (waiters_.empty()) return;
            next = waiters_.front();
            waiters_.erase(waiters_.begin());
        }
        next.close();
    }

    // Go's Broadcast.
    void broadcast() {
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
