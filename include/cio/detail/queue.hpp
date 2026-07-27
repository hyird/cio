// Run queues.
//
// The layout mirrors Go's P run queue, which has held up well in practice:
//
//   * a bounded lock-free ring per worker, FIFO for the owner (fairness within
//     a worker) and stealable in halves by other workers,
//   * plus a single `runnext` slot the worker owns (handled in Worker, not
//     here) that gives LIFO hand-off so a ping-pong pair stays on one core,
//   * plus a shared overflow queue behind a mutex, deliberately the slow path.
//
// Items are type-erased coroutine frame addresses (void*). Keeping them
// pointer-sized is what lets the ring be a plain array with no indirection.
#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>

#include "cio/config.hpp"

namespace cio::detail {

inline constexpr std::uint32_t kLocalQueueMask = kLocalQueueCapacity - 1;
static_assert((kLocalQueueCapacity & kLocalQueueMask) == 0,
              "local queue capacity must be a power of two");

// Any-producer, owning-worker-only-consumer bounded inbox.
//
// Each slot carries a monotonically increasing sequence. Producers reserve a
// position with CAS, write the frame, then publish the slot with release. The
// owner consumes in position order and advances that slot's sequence by one
// capacity before it can be reused. A failed full check does not reserve a
// position, so overflow can safely fall back to GlobalRunQueue without leaving
// a permanent hole.
//
// This is intentionally not MPMC. Remote producers must not write a worker's
// owner-optimized local FIFO, and thieves must not consume its inbox; the owner
// promotes inbox items into the stealable local FIFO in bounded batches.
template <std::size_t Capacity>
class BoundedMpscQueue {
    static_assert(Capacity >= 2, "MPSC queue needs at least two slots");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "MPSC queue capacity must be a power of two");

    struct Slot {
        std::atomic<std::uint64_t> sequence{0};
        void* frame = nullptr;
    };

public:
    BoundedMpscQueue() noexcept {
        for (std::uint64_t i = 0; i < Capacity; ++i) {
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    BoundedMpscQueue(const BoundedMpscQueue&) = delete;
    BoundedMpscQueue& operator=(const BoundedMpscQueue&) = delete;

    // Any producer. Returns false immediately when the bounded ring is full.
    bool try_push(void* frame) noexcept {
        std::uint64_t pos = producer_pos_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[pos & kMask];
            const std::uint64_t sequence =
                slot.sequence.load(std::memory_order_acquire);
            const auto difference =
                static_cast<std::int64_t>(sequence - pos);

            if (difference == 0) {
                if (producer_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    slot.frame = frame;
                    slot.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                continue;
            }
            if (difference < 0) return false;
            pos = producer_pos_.load(std::memory_order_relaxed);
        }
    }

    // Owning worker only.
    void* pop() noexcept {
        const std::uint64_t pos = consumer_pos_;
        Slot& slot = slots_[pos & kMask];
        const std::uint64_t sequence =
            slot.sequence.load(std::memory_order_acquire);
        const auto difference =
            static_cast<std::int64_t>(sequence - (pos + 1));
        if (difference != 0) return nullptr;

        void* const frame = slot.frame;
        slot.sequence.store(pos + Capacity, std::memory_order_release);
        consumer_pos_ = pos + 1;
        return frame;
    }

    // Owning worker only. A reserved-but-not-yet-published producer slot is
    // conservatively reported empty for this instant; the producer's directed
    // wake/final parking recheck is what makes that transient safe.
    bool empty() noexcept {
        const std::uint64_t pos = consumer_pos_;
        const std::uint64_t sequence =
            slots_[pos & kMask].sequence.load(std::memory_order_acquire);
        return static_cast<std::int64_t>(sequence - (pos + 1)) != 0;
    }

private:
    static constexpr std::uint64_t kMask = Capacity - 1;

    CIO_CACHE_ALIGNED std::atomic<std::uint64_t> producer_pos_{0};
    CIO_CACHE_ALIGNED std::uint64_t consumer_pos_ = 0;
    CIO_CACHE_ALIGNED std::array<Slot, Capacity> slots_{};
};

inline constexpr std::size_t kRemoteInboxCapacity = 256;
using RemoteInbox = BoundedMpscQueue<kRemoteInboxCapacity>;

// Single-producer (the owning worker) multi-consumer (thieves) bounded ring.
//
// head_ and tail_ are free-running 32-bit counters; only their difference is
// meaningful, so wraparound is harmless. The owner is the only writer of tail_;
// head_ is CAS'd by both the owner (pop) and thieves (grab).
class CIO_CACHE_ALIGNED LocalRunQueue {
public:
    // Owner only. Returns false when full; the caller is expected to spill half
    // the queue to the global queue and retry.
    bool push(void* item) noexcept {
        const std::uint32_t t = tail_.load(std::memory_order_relaxed);
        const std::uint32_t h = head_.load(std::memory_order_acquire);
        if (t - h >= kLocalQueueCapacity) return false;
        buf_[t & kLocalQueueMask].store(item, std::memory_order_relaxed);
        // Release: publishes the slot write to any thief that observes tail_.
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    // Owner only. FIFO — takes from the head, same end thieves take from, so it
    // must CAS.
    void* pop() noexcept {
        for (;;) {
            std::uint32_t h = head_.load(std::memory_order_acquire);
            const std::uint32_t t = tail_.load(std::memory_order_relaxed);
            if (t == h) return nullptr;
            void* item = buf_[h & kLocalQueueMask].load(std::memory_order_relaxed);
            // If the CAS succeeds, head_ did not move, so no thief took this
            // slot and no push could have wrapped onto it (that would need
            // kLocalQueueCapacity pushes without head_ advancing, which the
            // capacity check in push() forbids).
            if (head_.compare_exchange_strong(h, h + 1, std::memory_order_seq_cst,
                                              std::memory_order_relaxed)) {
                return item;
            }
        }
    }

    // Any thread. Copies up to half the queue into `out`, returning the count.
    // Steals half rather than one item so that a thief that wins a race does
    // not immediately have to steal again.
    std::uint32_t grab(void** out, std::uint32_t max) noexcept {
        for (;;) {
            const std::uint32_t h = head_.load(std::memory_order_acquire);
            const std::uint32_t t = tail_.load(std::memory_order_acquire);
            std::uint32_t n = t - h;
            n = n - n / 2;  // half, rounded up
            if (n == 0) return 0;
            // Torn snapshot (the owner raced us); retry with fresh values.
            if (n > kLocalQueueCapacity / 2) continue;
            if (n > max) n = max;
            for (std::uint32_t i = 0; i < n; ++i) {
                out[i] = buf_[(h + i) & kLocalQueueMask].load(std::memory_order_relaxed);
            }
            std::uint32_t expected = h;
            if (head_.compare_exchange_strong(expected, h + n, std::memory_order_seq_cst,
                                              std::memory_order_relaxed)) {
                return n;
            }
        }
    }

    // Owner only. Used on overflow: takes half the queue so it can be spilled
    // to the global queue in one lock acquisition.
    std::uint32_t pop_half(void** out) noexcept {
        const std::uint32_t t = tail_.load(std::memory_order_relaxed);
        std::uint32_t h = head_.load(std::memory_order_acquire);
        const std::uint32_t n = (t - h) / 2;
        if (n == 0) return 0;
        for (std::uint32_t i = 0; i < n; ++i) {
            out[i] = buf_[(h + i) & kLocalQueueMask].load(std::memory_order_relaxed);
        }
        if (!head_.compare_exchange_strong(h, h + n, std::memory_order_seq_cst,
                                           std::memory_order_relaxed)) {
            return 0;  // a thief beat us to it; it already took work off our hands
        }
        return n;
    }

    std::uint32_t size() const noexcept {
        const std::uint32_t t = tail_.load(std::memory_order_acquire);
        const std::uint32_t h = head_.load(std::memory_order_acquire);
        const std::uint32_t n = t - h;
        return n > kLocalQueueCapacity ? 0 : n;  // torn read
    }

    bool empty() const noexcept { return size() == 0; }

    // Conservative predicate for published-stealable-bit repair. A torn
    // snapshot is reported as non-empty: a stale set bit costs one failed
    // steal, while a false clear can strand runnable work.
    bool maybe_nonempty() const noexcept {
        const std::uint32_t first_head = head_.load(std::memory_order_acquire);
        const std::uint32_t tail = tail_.load(std::memory_order_acquire);
        const std::uint32_t second_head = head_.load(std::memory_order_acquire);
        if (first_head != second_head) return true;
        const std::uint32_t n = tail - second_head;
        return n != 0 || n > kLocalQueueCapacity;
    }

private:
    CIO_CACHE_ALIGNED std::atomic<std::uint32_t> head_{0};
    CIO_CACHE_ALIGNED std::atomic<std::uint32_t> tail_{0};
    // Relaxed atomics rather than plain pointers. On x86/ARM these compile to
    // exactly the same load and store instructions, but they make the slot
    // accesses race-free by definition: a thief reading a slot can otherwise
    // overlap the owner's write to it once the counters wrap, which is a real
    // data race even though the CAS makes the *value* harmless.
    CIO_CACHE_ALIGNED std::atomic<void*> buf_[kLocalQueueCapacity]{};
};

// Overflow / cross-thread submission queue. Contended by design but rarely on
// the hot path: workers take a batch proportional to their share, not one item.
class GlobalRunQueue {
public:
    void push(void* item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back(item);
        }
        // SC pairs with the worker's final park observation. If a producer's
        // subsequent idle-bitmap scan misses the worker, that worker's earlier
        // SC idle publication forces its size load to observe this update (or
        // a later consume).
        size_.fetch_add(1, std::memory_order_seq_cst);
    }

    void push_batch(void* const* items, std::uint32_t n) {
        if (n == 0) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.insert(queue_.end(), items, items + n);
        }
        size_.fetch_add(n, std::memory_order_seq_cst);
    }

    void* pop() {
        if (empty()) return nullptr;
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return nullptr;
        void* item = queue_.front();
        queue_.pop_front();
        size_.fetch_sub(1, std::memory_order_seq_cst);
        return item;
    }

    // Takes `1/share` of the queue (capped by `max`) and returns the count.
    // `share` is normally the worker count, so N workers draining concurrently
    // do not all take the same items and do not all take everything.
    std::uint32_t pop_batch(void** out, std::uint32_t max, std::uint32_t share) {
        if (empty()) return 0;
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint32_t available = static_cast<std::uint32_t>(queue_.size());
        if (available == 0) return 0;
        std::uint32_t n = available / (share ? share : 1) + 1;
        if (n > available) n = available;
        if (n > max) n = max;
        for (std::uint32_t i = 0; i < n; ++i) {
            out[i] = queue_.front();
            queue_.pop_front();
        }
        size_.fetch_sub(n, std::memory_order_seq_cst);
        return n;
    }

    std::uint32_t size() const noexcept {
        return size_.load(std::memory_order_seq_cst);
    }
    bool empty() const noexcept { return size() == 0; }

private:
    CIO_CACHE_ALIGNED std::atomic<std::uint32_t> size_{0};
    std::mutex mutex_;
    std::deque<void*> queue_;
};

}  // namespace cio::detail
