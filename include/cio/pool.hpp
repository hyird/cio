// Reusable byte buffers, and a pool for any type.
//
//     {
//         auto buffer = cio::buffer_pool().take(64 * 1024);
//         auto n = co_await stream.read(buffer.bytes());
//     }                                   // returned to the pool here
//
//     static cio::Pool<Parser> parsers;
//     auto parser = parsers.take();
//     parser->reset();
//
// Coroutine frames already have a pool: they are this runtime's most frequent
// allocation and a size-classed free list measurably paid for itself. I/O
// buffers are the second most frequent and had none, so every `io::copy()`
// without a caller-supplied span heap-allocated 64 KiB per call.
//
// Same two-level shape as the frame pool, for the same reason: a buffer taken
// and returned on one worker needs no atomics, while a buffer that crosses
// workers must not leave the taking worker's cache permanently empty.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

namespace cio {

class BufferPool;

// A borrowed buffer. Move-only; returns itself on destruction.
class [[nodiscard]] PooledBuffer {
public:
    PooledBuffer() = default;
    ~PooledBuffer() { release(); }

    PooledBuffer(PooledBuffer&& other) noexcept
        : pool_(std::exchange(other.pool_, nullptr)),
          data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)) {}
    PooledBuffer& operator=(PooledBuffer&& other) noexcept {
        if (this != &other) {
            release();
            pool_ = std::exchange(other.pool_, nullptr);
            data_ = std::exchange(other.data_, nullptr);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }
    PooledBuffer(const PooledBuffer&) = delete;
    PooledBuffer& operator=(const PooledBuffer&) = delete;

    bool valid() const noexcept { return data_ != nullptr; }
    std::span<std::byte> bytes() const noexcept { return {data_, size_}; }
    std::size_t size() const noexcept { return size_; }

private:
    friend class BufferPool;
    PooledBuffer(BufferPool* pool, std::byte* data, std::size_t size) noexcept
        : pool_(pool), data_(data), size_(size) {}

    void release() noexcept;

    BufferPool* pool_ = nullptr;
    std::byte* data_ = nullptr;
    std::size_t size_ = 0;
};

// Size-classed byte-buffer pool.
//
// Requests are rounded up to a power of two, so a pool serves a range of sizes
// without fragmenting into a class per exact request. Buffers above the largest
// class are allocated and freed directly rather than retained, because keeping a
// multi-megabyte buffer alive to save one allocation is the wrong trade.
class BufferPool {
public:
    static constexpr std::size_t kMinBufferBytes = 512;
    static constexpr std::size_t kMaxPooledBytes = 1u << 20;  // 1 MiB
    // Buffers retained per class per thread before spilling to the shared list.
    static constexpr std::size_t kThreadCacheDepth = 8;

    BufferPool() = default;
    ~BufferPool();

    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // Go's sync.Pool.Get shape; Put is the handle's destructor, because RAII
    // is how C++ spells "give it back". Never returns an invalid buffer: a
    // size above the pooled range is served by a direct allocation that the
    // handle frees instead of returning.
    PooledBuffer get(std::size_t bytes);

    // Retained buffers, for tests and diagnostics.
    std::size_t retained() const;

private:
    friend class PooledBuffer;

    static constexpr unsigned kClassCount = 12;  // 512 B .. 1 MiB
    static unsigned class_of(std::size_t bytes) noexcept;
    static std::size_t class_bytes(unsigned index) noexcept;

    void put(std::byte* data, std::size_t size) noexcept;

    struct ThreadCache {
        std::vector<std::byte*> free_list[kClassCount];
        // Identity only, for detecting a re-home. Never dereferenced: a cache
        // can outlive the pool it names.
        const BufferPool* owner = nullptr;
        ~ThreadCache();
    };
    ThreadCache& cache();
    static void drain_cache(ThreadCache& local) noexcept;

    mutable std::mutex mutex_;
    std::vector<std::byte*> central_[kClassCount];
};

// The runtime-wide buffer pool, used by io::copy() when the caller supplies no
// scratch span.
BufferPool& buffer_pool();

// A pool for any default-constructible type, in the spirit of Go's sync.Pool.
//
// Deliberately not a cache with an eviction policy: it retains what is given
// back and hands it out again, and the caller is responsible for resetting an
// object's state before reuse. A pooled object with stale state is the classic
// sync.Pool bug, so `take()` does not pretend to have cleaned it.
template <typename T>
class Pool {
public:
    class [[nodiscard]] Handle {
    public:
        Handle() = default;
        ~Handle() {
            if (pool_ != nullptr && value_) pool_->put(std::move(value_));
        }

        Handle(Handle&& other) noexcept
            : pool_(std::exchange(other.pool_, nullptr)),
              value_(std::move(other.value_)) {}
        Handle& operator=(Handle&& other) noexcept {
            if (this != &other) {
                if (pool_ != nullptr && value_) {
                    pool_->put(std::move(value_));
                }
                pool_ = std::exchange(other.pool_, nullptr);
                value_ = std::move(other.value_);
            }
            return *this;
        }
        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        bool valid() const noexcept { return static_cast<bool>(value_); }
        T& operator*() const noexcept { return *value_; }
        T* operator->() const noexcept { return value_.get(); }

    private:
        friend class Pool;
        Handle(Pool* pool, std::unique_ptr<T> value) noexcept
            : pool_(pool), value_(std::move(value)) {}

        Pool* pool_ = nullptr;
        std::unique_ptr<T> value_;
    };

    // Go's Get; Put is the handle going out of scope.
    Handle get() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!free_.empty()) {
                auto value = std::move(free_.back());
                free_.pop_back();
                return Handle{this, std::move(value)};
            }
        }
        return Handle{this, std::make_unique<T>()};
    }

    std::size_t retained() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return free_.size();
    }

private:
    friend class Handle;
    void put(std::unique_ptr<T> value) {
        std::lock_guard<std::mutex> lock(mutex_);
        free_.push_back(std::move(value));
    }

    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<T>> free_;
};

}  // namespace cio
