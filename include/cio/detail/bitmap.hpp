// Cache-line-separated atomic worker-state bitmaps.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "cio/config.hpp"
#include "cio/detail/worker_id.hpp"

namespace cio::detail {

class AtomicWorkerBitmap {
    struct CIO_CACHE_ALIGNED Word {
        std::atomic<std::uint64_t> bits{0};
    };

public:
    explicit AtomicWorkerBitmap(std::size_t bit_count)
        : bit_count_(bit_count),
          word_count_((bit_count + 63) / 64),
          words_(word_count_ == 0 ? nullptr : std::make_unique<Word[]>(word_count_)) {}

    AtomicWorkerBitmap(const AtomicWorkerBitmap&) = delete;
    AtomicWorkerBitmap& operator=(const AtomicWorkerBitmap&) = delete;

    // Returns true when this call changed a clear bit to set.
    bool set(WorkerId worker) noexcept {
        if (worker >= bit_count_) return false;
        const std::uint64_t mask = bit(worker);
        // Always perform the RMW, even when the bit is already set. A racing
        // clearer reads this release sequence before its final queue recheck;
        // a read-only early return could otherwise let that recheck miss the
        // just-published queue tail and clear the only work indication. A
        // caller may avoid this RMW only with an equivalent external
        // publication/clear handshake.
        return (words_[worker / 64].bits.fetch_or(
                    mask, std::memory_order_seq_cst) &
                mask) == 0;
    }

    // Returns true when this call claimed a previously-set bit.
    bool clear(WorkerId worker) noexcept {
        if (worker >= bit_count_) return false;
        const std::uint64_t mask = bit(worker);
        return (words_[worker / 64].bits.fetch_and(~mask, std::memory_order_seq_cst) &
                mask) != 0;
    }

    bool test(WorkerId worker) const noexcept {
        if (worker >= bit_count_) return false;
        return (words_[worker / 64].bits.load(std::memory_order_acquire) &
                bit(worker)) != 0;
    }

    // SC counterpart used when a read participates in a cross-atomic
    // publication protocol rather than merely consuming a bitmap hint.
    bool test_seq_cst(WorkerId worker) const noexcept {
        if (worker >= bit_count_) return false;
        return (words_[worker / 64].bits.load(std::memory_order_seq_cst) &
                bit(worker)) != 0;
    }

    bool any() const noexcept {
        for (std::size_t i = 0; i < word_count_; ++i) {
            if (words_[i].bits.load(std::memory_order_acquire) != 0) return true;
        }
        return false;
    }

    // SC counterpart for the final park check. Producers publish a victim bit
    // before scanning the SC idle bitmap; the worker publishes its idle bit
    // before this read. Both sides therefore cannot miss each other in the SC
    // order.
    bool any_seq_cst() const noexcept {
        for (std::size_t i = 0; i < word_count_; ++i) {
            if (words_[i].bits.load(std::memory_order_seq_cst) != 0) {
                return true;
            }
        }
        return false;
    }

    bool all() const noexcept {
        if (bit_count_ == 0) return true;
        for (std::size_t i = 0; i + 1 < word_count_; ++i) {
            if (words_[i].bits.load(std::memory_order_acquire) !=
                ~std::uint64_t{0}) {
                return false;
            }
        }
        const unsigned tail_bits = static_cast<unsigned>(bit_count_ % 64);
        const std::uint64_t tail_mask =
            tail_bits == 0 ? ~std::uint64_t{0}
                           : (std::uint64_t{1} << tail_bits) - 1;
        return (words_[word_count_ - 1].bits.load(
                    std::memory_order_acquire) &
                tail_mask) == tail_mask;
    }

    // Finds a set worker without changing the bitmap. `start` rotates both the
    // first word and the bit choice within it so concurrent thieves do not all
    // converge on the lowest-numbered victim.
    WorkerId find_from(WorkerId start) const noexcept {
        return find_from_impl(start, std::memory_order_acquire);
    }

    // SC counterpart used when an empty search participates in the park /
    // publication handshake. A victim publisher reads idle after publishing
    // its victim bit, while a worker reads victims after publishing idle; both
    // sides must be in the single SC order so they cannot both miss.
    WorkerId find_from_seq_cst(WorkerId start) const noexcept {
        return find_from_impl(start, std::memory_order_seq_cst);
    }

private:
    WorkerId find_from_impl(
        WorkerId start, std::memory_order order) const noexcept {
        if (bit_count_ == 0) return kInvalidWorkerId;
        start %= static_cast<WorkerId>(bit_count_);
        const std::size_t first_word = start / 64;
        const unsigned first_bit = start % 64;

        const std::uint64_t first =
            words_[first_word].bits.load(order);
        const std::uint64_t high_mask = ~std::uint64_t{0} << first_bit;
        if (const std::uint64_t high = first & high_mask; high != 0) {
            return static_cast<WorkerId>(
                first_word * 64 + std::countr_zero(high));
        }

        for (std::size_t offset = 1; offset < word_count_; ++offset) {
            const std::size_t word_index = (first_word + offset) % word_count_;
            const std::uint64_t value =
                words_[word_index].bits.load(order);
            if (value == 0) continue;
            const WorkerId result = static_cast<WorkerId>(
                word_index * 64 + std::countr_zero(value));
            if (result < bit_count_) return result;
        }

        if (first_bit != 0) {
            const std::uint64_t low_mask =
                (std::uint64_t{1} << first_bit) - 1;
            if (const std::uint64_t low = first & low_mask; low != 0) {
                return static_cast<WorkerId>(
                    first_word * 64 + std::countr_zero(low));
            }
        }
        return kInvalidWorkerId;
    }

public:
    // Atomically takes one set bit, used by producers to claim an idle worker.
    WorkerId claim_from(WorkerId start) noexcept {
        if (bit_count_ == 0) return kInvalidWorkerId;
        start %= static_cast<WorkerId>(bit_count_);
        const std::size_t first_word = start / 64;
        const unsigned first_bit = start % 64;

        if (const WorkerId claimed =
                claim_from_word(first_word, ~std::uint64_t{0} << first_bit);
            claimed != kInvalidWorkerId) {
            return claimed;
        }
        for (std::size_t offset = 1; offset < word_count_; ++offset) {
            const std::size_t word_index = (first_word + offset) % word_count_;
            if (const WorkerId claimed =
                    claim_from_word(word_index, ~std::uint64_t{0});
                claimed != kInvalidWorkerId) {
                return claimed;
            }
        }
        if (first_bit != 0) {
            const std::uint64_t low_mask =
                (std::uint64_t{1} << first_bit) - 1;
            return claim_from_word(first_word, low_mask);
        }
        return kInvalidWorkerId;
    }

private:
    static std::uint64_t bit(WorkerId worker) noexcept {
        return std::uint64_t{1} << (worker % 64);
    }

    WorkerId claim_from_word(std::size_t word_index,
                             std::uint64_t allowed) noexcept {
        std::uint64_t observed =
            words_[word_index].bits.load(std::memory_order_seq_cst);
        for (;;) {
            const std::uint64_t candidates = observed & allowed;
            if (candidates == 0) return kInvalidWorkerId;
            const unsigned selected = std::countr_zero(candidates);
            const std::uint64_t desired =
                observed & ~(std::uint64_t{1} << selected);
            if (words_[word_index].bits.compare_exchange_weak(
                    observed, desired, std::memory_order_seq_cst,
                    std::memory_order_seq_cst)) {
                const WorkerId result =
                    static_cast<WorkerId>(word_index * 64 + selected);
                return result < bit_count_ ? result : kInvalidWorkerId;
            }
        }
    }

    std::size_t bit_count_;
    std::size_t word_count_;
    std::unique_ptr<Word[]> words_;
};

}  // namespace cio::detail
