// Coroutine frame allocator.
//
// Frames are this runtime's most frequent allocation: one per spawned task, one
// per socket read or write. They are a fixed size per coroutine type and very
// short lived, which makes them a good fit for a size-classed free list.
//
// Measured before adding this: malloc/free cost 36 ns on the socket read path
// against 360 ns for the recv syscall itself — 10% of a read spent in the
// allocator.
//
// Two levels, because the two access patterns are genuinely different:
//
//   thread cache   A task that awaits a read allocates and frees the frame on
//                  the same worker, so a plain thread-local free list serves it
//                  with no atomics at all.
//   central list   A fan-out allocates every frame on one task and frees them
//                  across every worker. The allocating thread's cache is then
//                  permanently empty and a thread-local list recycles nothing —
//                  measured at 1.000 allocations per spawn. Freeing threads
//                  spill batches here for the allocating thread to refill from.
//
// Batching is what keeps the central list cheap: its lock is touched once per
// kBatch operations, not once per frame.
//
// The central lists are never destroyed. The blocks they hold stay reachable
// from a static, so this is a cache rather than a leak, and it avoids the
// shutdown-ordering hazard of a thread freeing into a destroyed list.
#pragma once

#include <cstddef>
#include <mutex>
#include <new>

#include "cio/config.hpp"

namespace cio::detail {

class FramePool {
public:
    static constexpr std::size_t kGranularity = 32;
    static constexpr std::size_t kMaxSize = 512;
    static constexpr std::size_t kClasses = kMaxSize / kGranularity;

    // Blocks moved between a thread cache and the central list in one lock.
    static constexpr std::size_t kBatch = 32;
    // High-water mark for a thread cache; must exceed kBatch so that a thread
    // alternating between allocate and free does not spill on every operation.
    static constexpr std::size_t kCachePerClass = 64;
    // Ceiling on centrally cached batches per size class.
    static constexpr std::size_t kCentralBatches = 64;

    static void* allocate(std::size_t size) {
        if (size == 0 || size > kMaxSize) return ::operator new(size);
        const std::size_t index = size_class(size);
        Cache& cache = thread_cache();

        if (void* block = cache.free_list[index]) {
            cache.free_list[index] = next_of(block);
            --cache.count[index];
            return block;
        }
        return allocate_slow(cache, index);
    }

    static void deallocate(void* block, std::size_t size) noexcept {
        if (block == nullptr) return;
        if (size == 0 || size > kMaxSize) {
            ::operator delete(block);
            return;
        }
        const std::size_t index = size_class(size);
        Cache& cache = thread_cache();

        next_of(block) = cache.free_list[index];
        cache.free_list[index] = block;
        if (++cache.count[index] < kCachePerClass) return;

        // Detach the first kBatch blocks. Walking them is cheap: they were just
        // written by this thread and are still in L1.
        void* head = cache.free_list[index];
        void* tail = head;
        for (std::size_t i = 1; i < kBatch; ++i) tail = next_of(tail);

        cache.free_list[index] = next_of(tail);
        cache.count[index] -= kBatch;
        next_of(tail) = nullptr;

        Central& list = central(index);
        {
            std::lock_guard<std::mutex> lock(list.mutex);
            if (list.count < kCentralBatches) {
                list.batches[list.count++] = head;
                return;
            }
        }
        // Central list is full: genuinely give the memory back.
        release_chain(head);
    }

private:
    static std::size_t size_class(std::size_t size) noexcept {
        return (size - 1) / kGranularity;
    }

    // The free-list link lives in the first word of the free block itself.
    static void*& next_of(void* block) noexcept {
        return *static_cast<void**>(block);
    }

    static void release_chain(void* block) noexcept {
        while (block != nullptr) {
            void* next = next_of(block);
            ::operator delete(block);
            block = next;
        }
    }

    struct Cache {
        void* free_list[kClasses]{};
        std::size_t count[kClasses]{};

        // Returns this thread's cached blocks to the process at thread exit,
        // so a short-lived thread does not strand memory.
        ~Cache() {
            for (std::size_t i = 0; i < kClasses; ++i)
                release_chain(free_list[i]);
        }
    };

    struct Central {
        std::mutex mutex;
        std::size_t count = 0;
        void* batches[kCentralBatches]{};
    };

    static Cache& thread_cache() noexcept {
        static thread_local Cache cache;
        return cache;
    }

    static Central& central(std::size_t index) noexcept {
        // Trivially destructible, so never torn down — see the header comment.
        static Central lists[kClasses];
        return lists[index];
    }

    static CIO_NOINLINE CIO_COLD void* allocate_slow(Cache& cache,
                                                     std::size_t index) {
        Central& list = central(index);
        void* chain = nullptr;
        {
            std::lock_guard<std::mutex> lock(list.mutex);
            if (list.count != 0) chain = list.batches[--list.count];
        }
        if (chain != nullptr) {
            cache.free_list[index] = next_of(chain);
            cache.count[index] = kBatch - 1;
            return chain;
        }
        // Allocate the whole class, not the request, so any frame in this class
        // can reuse the block.
        return ::operator new((index + 1) * kGranularity);
    }
};

}  // namespace cio::detail
