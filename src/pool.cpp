#include "cio/pool.hpp"

#include <bit>
#include <new>

namespace cio {

void PooledBuffer::release() noexcept {
    if (data_ == nullptr) return;
    BufferPool* const pool = std::exchange(pool_, nullptr);
    std::byte* const data = std::exchange(data_, nullptr);
    const std::size_t size = std::exchange(size_, 0);
    if (pool != nullptr) {
        pool->put(data, size);
    } else {
        // An oversized buffer was never pooled; free it directly.
        ::operator delete(data, std::align_val_t{alignof(std::max_align_t)});
    }
}

unsigned BufferPool::class_of(std::size_t bytes) noexcept {
    std::size_t rounded = kMinBufferBytes;
    unsigned index = 0;
    while (rounded < bytes && index + 1 < kClassCount) {
        rounded <<= 1;
        ++index;
    }
    return index;
}

std::size_t BufferPool::class_bytes(unsigned index) noexcept {
    return kMinBufferBytes << index;
}

// Frees a cache's blocks outright.
//
// Deliberately not "give them back to the owning pool". A thread cache can
// outlive the pool it borrowed from — a pool with automatic storage is destroyed
// while this thread's cache still names it — and it can also be destroyed at
// thread exit, after the pool. Dereferencing `owner` on either path is a
// use-after-free, which is exactly what ASan caught here. Releasing the memory
// directly is always safe, and both paths are cold: a re-homed cache and a
// dying thread are not events worth retaining a few buffers for.
void BufferPool::drain_cache(ThreadCache& local) noexcept {
    for (unsigned i = 0; i < kClassCount; ++i) {
        for (std::byte* block : local.free_list[i]) {
            ::operator delete(block,
                              std::align_val_t{alignof(std::max_align_t)});
        }
        local.free_list[i].clear();
    }
}

BufferPool::ThreadCache& BufferPool::cache() {
    thread_local ThreadCache local;
    if (local.owner != this) {
        // A different pool used this thread before. Its blocks are released
        // rather than handed over: `owner` may already be destroyed, and it is
        // never dereferenced here for that reason.
        drain_cache(local);
        local.owner = this;
    }
    return local;
}

BufferPool::ThreadCache::~ThreadCache() {
    // Runs at thread exit, possibly after the pool. Free directly; never touch
    // `owner`.
    for (unsigned i = 0; i < kClassCount; ++i) {
        for (std::byte* block : free_list[i]) {
            ::operator delete(block,
                              std::align_val_t{alignof(std::max_align_t)});
        }
        free_list[i].clear();
    }
    owner = nullptr;
}

PooledBuffer BufferPool::get(std::size_t bytes) {
    if (bytes == 0) bytes = 1;

    if (bytes > kMaxPooledBytes) {
        // Above the pooled range: allocate directly and let the handle free it.
        // Retaining a multi-megabyte buffer to save one allocation is the wrong
        // trade, so this deliberately does not pool.
        void* raw = ::operator new(bytes,
                                   std::align_val_t{alignof(std::max_align_t)});
        return PooledBuffer{nullptr, static_cast<std::byte*>(raw), bytes};
    }

    const unsigned index = class_of(bytes);
    const std::size_t size = class_bytes(index);

    ThreadCache& local = cache();
    if (!local.free_list[index].empty()) {
        std::byte* block = local.free_list[index].back();
        local.free_list[index].pop_back();
        return PooledBuffer{this, block, size};
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& shared = central_[index];
        if (!shared.empty()) {
            // Refill a batch, so a thread that only frees does not force a lock
            // acquisition per take.
            const std::size_t move =
                std::min(shared.size(), kThreadCacheDepth);
            for (std::size_t i = 0; i < move; ++i) {
                local.free_list[index].push_back(shared.back());
                shared.pop_back();
            }
        }
    }
    if (!local.free_list[index].empty()) {
        std::byte* block = local.free_list[index].back();
        local.free_list[index].pop_back();
        return PooledBuffer{this, block, size};
    }

    void* raw = ::operator new(size,
                               std::align_val_t{alignof(std::max_align_t)});
    return PooledBuffer{this, static_cast<std::byte*>(raw), size};
}

void BufferPool::put(std::byte* data, std::size_t size) noexcept {
    if (data == nullptr) return;
    if (size > kMaxPooledBytes) {
        ::operator delete(data, std::align_val_t{alignof(std::max_align_t)});
        return;
    }

    const unsigned index = class_of(size);
    ThreadCache& local = cache();
    if (local.free_list[index].size() < kThreadCacheDepth) {
        local.free_list[index].push_back(data);
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    central_[index].push_back(data);
}

std::size_t BufferPool::retained() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t total = 0;
    for (unsigned i = 0; i < kClassCount; ++i) total += central_[i].size();
    return total;
}

BufferPool::~BufferPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (unsigned i = 0; i < kClassCount; ++i) {
        for (std::byte* block : central_[i]) {
            ::operator delete(block,
                              std::align_val_t{alignof(std::max_align_t)});
        }
        central_[i].clear();
    }
}

BufferPool& buffer_pool() {
    // Process-lifetime, like the frame pool: a thread cache must not outlive the
    // pool it spills into, and static destruction order cannot guarantee that.
    static BufferPool* pool = new BufferPool();
    return *pool;
}

}  // namespace cio
