// Chan<T> — a Go channel.
//
// Value semantics on purpose: a Chan is a cheap refcounted handle, so it is
// passed by value into tasks exactly the way `chan int` is passed into a
// goroutine. Copying is one relaxed increment.
//
//     auto jobs = cio::make_chan<int>(64);   // buffered
//     auto done = cio::make_chan<>();        // unbuffered, Chan<Unit>
//
//     co_await jobs.send(7);                 // -> bool, false if closed
//     while (auto job = co_await jobs.recv()) { ... }   // nullopt when drained
//
// Waiter nodes are intrusive and live in the coroutine frames of the parked
// tasks, so a blocked send or receive allocates nothing.
//
// LIFETIME CONTRACT, which everything here depends on: a waker must decide
// whether it owns a waiter (ChanWaiter::try_acquire) while holding the channel
// lock, and must not touch that waiter again after releasing the lock unless it
// won. The winner may touch it until it schedules it, and must schedule it
// last. This is what lets select retract its unfired cases safely with nothing
// but the channel lock — no refcounting on the wakeup path.
#pragma once

#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/detail/scheduler.hpp"

namespace cio {

// The payload for signal-only channels: `Chan<>` / `make_chan<>()`.
struct Unit {
    friend bool operator==(Unit, Unit) noexcept { return true; }
};

namespace detail {

inline constexpr std::uint32_t kNoWinner = 0xFFFFFFFFu;

// Publication phases of a select. A waker that wins a case while the select is
// still registering its other cases must NOT resume it — the select is still
// touching its own frame. It hands responsibility back by leaving the phase at
// kSelectResumed, which the setup code detects and resumes itself.
inline constexpr std::uint32_t kSelectSetup = 0;
inline constexpr std::uint32_t kSelectParked = 1;
inline constexpr std::uint32_t kSelectResumed = 2;

// A parked sender or receiver. Lives in the coroutine frame.
struct ChanWaiter {
    ChanWaiter* next = nullptr;
    ChanWaiter* prev = nullptr;
    std::coroutine_handle<> handle{};
    Scheduler* sched = nullptr;

    // Sender: points at the T being sent. Receiver: points at the
    // std::optional<T> to fill.
    void* slot = nullptr;
    bool success = false;
    bool queued = false;

    // Non-null when this waiter belongs to a select. Waking it then requires
    // winning the select's single claim; losers must move on to the next
    // waiter in the queue.
    std::atomic<std::uint32_t>* select_winner = nullptr;
    std::atomic<std::uint32_t>* select_phase = nullptr;
    std::uint32_t case_index = 0;

    // Must be called with the owning channel's lock held.
    bool try_acquire() noexcept {
        if (select_winner == nullptr) return true;
        std::uint32_t expected = kNoWinner;
        return select_winner->compare_exchange_strong(expected, case_index,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_relaxed);
    }

    // Returns false if resumption is not ours to perform (a select that has not
    // finished publishing itself yet).
    bool take_resume_ownership() noexcept {
        if (select_phase == nullptr) return true;
        return select_phase->exchange(kSelectResumed, std::memory_order_acq_rel) ==
               kSelectParked;
    }

    void wake() noexcept {
        if (!take_resume_ownership()) return;
        if (sched != nullptr) sched->schedule(handle);
    }

    // Used for a direct hand-off, where the waker has just produced exactly the
    // value this task was waiting for: run it next on this worker so the value
    // is still in L1 when it does.
    void wake_handoff() noexcept {
        if (!take_resume_ownership()) return;
        if (sched != nullptr) sched->schedule_next(handle);
    }
};

class WaiterList {
public:
    void push_back(ChanWaiter* w) noexcept {
        w->next = nullptr;
        w->prev = tail_;
        if (tail_ != nullptr) {
            tail_->next = w;
        } else {
            head_ = w;
        }
        tail_ = w;
        w->queued = true;
    }

    ChanWaiter* pop_front() noexcept {
        ChanWaiter* w = head_;
        if (w == nullptr) return nullptr;
        head_ = w->next;
        if (head_ != nullptr) {
            head_->prev = nullptr;
        } else {
            tail_ = nullptr;
        }
        w->next = w->prev = nullptr;
        w->queued = false;
        return w;
    }

    void remove(ChanWaiter* w) noexcept {
        if (!w->queued) return;
        if (w->prev != nullptr) {
            w->prev->next = w->next;
        } else {
            head_ = w->next;
        }
        if (w->next != nullptr) {
            w->next->prev = w->prev;
        } else {
            tail_ = w->prev;
        }
        w->next = w->prev = nullptr;
        w->queued = false;
    }

    bool empty() const noexcept { return head_ == nullptr; }

private:
    ChanWaiter* head_ = nullptr;
    ChanWaiter* tail_ = nullptr;
};

enum class OpStatus { kDone, kClosed, kBlocked };

// Type-erased part of a channel. select needs to lock a heterogeneous set of
// channels in address order, which it can only do through a common base.
class ChannelBase {
public:
    std::mutex mutex;
    WaiterList senders;
    WaiterList receivers;
    std::size_t capacity = 0;
    std::size_t count = 0;
    std::size_t send_index = 0;
    std::size_t recv_index = 0;
    bool closed = false;
    std::atomic<std::uint32_t> refs{1};
};

template <typename T>
class Channel final : public ChannelBase {
public:
    static Channel* create(std::size_t capacity) {
        // unique_ptr so a throwing buffer allocation does not leak the control
        // block along the way.
        auto channel = std::unique_ptr<Channel>(new Channel());
        channel->ChannelBase::capacity = capacity;
        if (capacity > 0) {
            // capacity * sizeof(T) wrapping would allocate a ring far smaller
            // than the capacity recorded above, and every buffered send after
            // that writes out of bounds.
            if (capacity > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
                throw std::bad_array_new_length{};
            }
            channel->buffer_ = static_cast<T*>(
                ::operator new(capacity * sizeof(T), std::align_val_t{alignof(T)}));
        }
        return channel.release();
    }

    void add_ref() noexcept { refs.fetch_add(1, std::memory_order_relaxed); }

    void release() noexcept {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }

    // --- locked primitives ---------------------------------------------------
    //
    // `to_wake` receives the peer that this operation unblocked, if any. The
    // caller wakes it after releasing the lock; waking under the lock would put
    // a scheduler round-trip inside the channel's critical section.

    OpStatus try_send_locked(T* value, ChanWaiter*& to_wake) {
        to_wake = nullptr;
        if (closed) return OpStatus::kClosed;

        // A waiting receiver takes the value directly — it never touches the
        // ring buffer, even on a buffered channel.
        while (ChanWaiter* receiver = receivers.pop_front()) {
            if (!receiver->try_acquire()) continue;  // lost a select race
            static_cast<std::optional<T>*>(receiver->slot)->emplace(std::move(*value));
            receiver->success = true;
            to_wake = receiver;
            return OpStatus::kDone;
        }

        if (count < capacity) {
            ::new (static_cast<void*>(buffer_ + send_index)) T(std::move(*value));
            send_index = next_index(send_index);
            ++count;
            return OpStatus::kDone;
        }

        return OpStatus::kBlocked;
    }

    OpStatus try_recv_locked(std::optional<T>* out, ChanWaiter*& to_wake) {
        to_wake = nullptr;

        if (count > 0) {
            T* cell = buffer_ + recv_index;
            out->emplace(std::move(*cell));
            cell->~T();
            recv_index = next_index(recv_index);
            --count;

            // The ring just gained a free slot; hand it to a blocked sender so
            // the queue stays full and the sender does not have to re-contend.
            while (ChanWaiter* sender = senders.pop_front()) {
                if (!sender->try_acquire()) continue;
                ::new (static_cast<void*>(buffer_ + send_index))
                    T(std::move(*static_cast<T*>(sender->slot)));
                send_index = next_index(send_index);
                ++count;
                sender->success = true;
                to_wake = sender;
                break;
            }
            return OpStatus::kDone;
        }

        // Unbuffered, or buffered-but-empty with a sender parked.
        while (ChanWaiter* sender = senders.pop_front()) {
            if (!sender->try_acquire()) continue;
            out->emplace(std::move(*static_cast<T*>(sender->slot)));
            sender->success = true;
            to_wake = sender;
            return OpStatus::kDone;
        }

        if (closed) {
            out->reset();
            return OpStatus::kClosed;
        }
        return OpStatus::kBlocked;
    }

    void close() {
        // Collected under the lock, woken after it: waking runs scheduler code,
        // and no channel lock should be held across that.
        ChanWaiter* wake_list = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) return;
            closed = true;

            while (ChanWaiter* receiver = receivers.pop_front()) {
                if (!receiver->try_acquire()) continue;
                static_cast<std::optional<T>*>(receiver->slot)->reset();
                receiver->success = false;
                receiver->next = wake_list;
                wake_list = receiver;
            }
            while (ChanWaiter* sender = senders.pop_front()) {
                if (!sender->try_acquire()) continue;
                sender->success = false;
                sender->next = wake_list;
                wake_list = sender;
            }
        }
        while (wake_list != nullptr) {
            ChanWaiter* waiter = wake_list;
            wake_list = waiter->next;  // read before waking: wake() may free it
            waiter->wake();
        }
    }

    ~Channel() {
        for (std::size_t i = 0; i < count; ++i) {
            buffer_[(recv_index + i) % capacity].~T();
        }
        if (buffer_ != nullptr) {
            ::operator delete(buffer_, std::align_val_t{alignof(T)});
        }
    }

private:
    Channel() = default;

    std::size_t next_index(std::size_t i) const noexcept {
        const std::size_t n = i + 1;
        return n == capacity ? 0 : n;
    }

    T* buffer_ = nullptr;
};

template <typename T>
class [[nodiscard]] SendAwaiter {
public:
    SendAwaiter(Channel<T>* channel, T value)
        : channel_(channel), value_(std::move(value)) {}

    SendAwaiter(const SendAwaiter&) = delete;
    SendAwaiter& operator=(const SendAwaiter&) = delete;

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
        ChanWaiter* to_wake = nullptr;
        {
            std::unique_lock<std::mutex> lock(channel_->mutex);
            const OpStatus status = channel_->try_send_locked(&value_, to_wake);
            if (status == OpStatus::kBlocked) {
                waiter_.handle = h;
                waiter_.sched = current_scheduler();
                waiter_.slot = &value_;
                channel_->senders.push_back(&waiter_);
                // The lock is released by the unique_lock destructor, which
                // lives on the stack, not in the frame — safe even though the
                // frame may be resumed the instant the lock drops.
                return true;
            }
            waiter_.success = status == OpStatus::kDone;
        }
        if (to_wake != nullptr) to_wake->wake_handoff();
        return false;
    }

    bool await_resume() const noexcept { return waiter_.success; }

private:
    Channel<T>* channel_;
    T value_;
    ChanWaiter waiter_{};
};

template <typename T>
class [[nodiscard]] RecvAwaiter {
public:
    explicit RecvAwaiter(Channel<T>* channel) : channel_(channel) {}

    RecvAwaiter(const RecvAwaiter&) = delete;
    RecvAwaiter& operator=(const RecvAwaiter&) = delete;

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
        ChanWaiter* to_wake = nullptr;
        {
            std::unique_lock<std::mutex> lock(channel_->mutex);
            const OpStatus status = channel_->try_recv_locked(&value_, to_wake);
            if (status == OpStatus::kBlocked) {
                waiter_.handle = h;
                waiter_.sched = current_scheduler();
                waiter_.slot = &value_;
                channel_->receivers.push_back(&waiter_);
                return true;
            }
        }
        if (to_wake != nullptr) to_wake->wake_handoff();
        return false;
    }

    std::optional<T> await_resume() noexcept { return std::move(value_); }

private:
    Channel<T>* channel_;
    std::optional<T> value_{};
    ChanWaiter waiter_{};
};

}  // namespace detail

template <typename T = Unit>
class Chan {
public:
    using value_type = T;

    Chan() noexcept = default;  // nil channel

    Chan(const Chan& other) noexcept : impl_(other.impl_) {
        if (impl_ != nullptr) impl_->add_ref();
    }
    Chan(Chan&& other) noexcept : impl_(std::exchange(other.impl_, nullptr)) {}
    Chan& operator=(Chan other) noexcept {
        std::swap(impl_, other.impl_);
        return *this;
    }
    ~Chan() {
        if (impl_ != nullptr) impl_->release();
    }

    explicit operator bool() const noexcept { return impl_ != nullptr; }

    // co_await ch.send(v) -> bool. false means the channel was closed and the
    // value was not delivered. (Go panics here; returning is friendlier and
    // composes with Result-style error handling.)
    [[nodiscard]] detail::SendAwaiter<T> send(T value) const {
        require();
        return detail::SendAwaiter<T>{impl_, std::move(value)};
    }

    // co_await ch.recv() -> std::optional<T>. nullopt means closed and drained.
    [[nodiscard]] detail::RecvAwaiter<T> recv() const {
        require();
        return detail::RecvAwaiter<T>{impl_};
    }

    // Non-blocking variants, for the rare places that genuinely cannot suspend.
    bool try_send(T value) const {
        require();
        detail::ChanWaiter* to_wake = nullptr;
        detail::OpStatus status;
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);
            status = impl_->try_send_locked(&value, to_wake);
        }
        if (to_wake != nullptr) to_wake->wake();
        return status == detail::OpStatus::kDone;
    }

    std::optional<T> try_recv() const {
        require();
        std::optional<T> out;
        detail::ChanWaiter* to_wake = nullptr;
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);
            impl_->try_recv_locked(&out, to_wake);
        }
        if (to_wake != nullptr) to_wake->wake();
        return out;
    }

    // Idempotent, unlike Go's close() which panics on a second call.
    void close() const {
        if (impl_ != nullptr) impl_->close();
    }

    bool is_closed() const {
        if (impl_ == nullptr) return false;
        std::lock_guard<std::mutex> lock(impl_->mutex);
        return impl_->closed;
    }

    std::size_t size() const {
        if (impl_ == nullptr) return 0;
        std::lock_guard<std::mutex> lock(impl_->mutex);
        return impl_->count;
    }

    std::size_t capacity() const noexcept {
        return impl_ == nullptr ? 0 : impl_->capacity;
    }

    detail::Channel<T>* native() const noexcept { return impl_; }

private:
    template <typename U>
    friend Chan<U> make_chan(std::size_t);

    explicit Chan(detail::Channel<T>* impl) noexcept : impl_(impl) {}

    void require() const {
        if (impl_ == nullptr) {
            // Go would block forever here. In C++ that reads as a hang with no
            // explanation, so say what happened instead.
            throw std::logic_error("cio: operation on a nil Chan");
        }
    }

    detail::Channel<T>* impl_ = nullptr;
};

// capacity 0 gives an unbuffered channel: send and recv rendezvous.
template <typename T = Unit>
Chan<T> make_chan(std::size_t capacity = 0) {
    return Chan<T>{detail::Channel<T>::create(capacity)};
}

}  // namespace cio
