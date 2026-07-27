// The reactor: readiness notification for the runtime.
//
// epoll, edge-triggered. The backend-independent half of the reactor lives in
// src/reactor_common.cpp and the epoll-specific half in src/reactor_epoll.cpp,
// which is where another backend would go — but none is claimed.
//
// Per-fd, per-direction state follows Go's netpoll protocol. Each direction is
// a single atomic word with three states:
//
//   nullptr    idle
//   kIoReady   an edge arrived while nobody was waiting; the next waiter
//              consumes it instead of parking
//   IoWait*    exactly one task is parked on this direction
//
// The three-state machine is what makes edge-triggered polling safe: the race
// between "syscall returned EAGAIN, about to park" and "readiness arrived" is
// resolved by the CAS, never by a lock.
//
// Deadlines live on the descriptor, not on the awaiter — Go's SetReadDeadline
// model. That is not just API mimicry: a timer that can fire concurrently with
// the task it is timing out must outlive the coroutine frame, and a descriptor
// in the reactor's slab does while an awaiter in a frame does not.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <mutex>

#include "cio/config.hpp"
#include "cio/detail/timer.hpp"
#include "cio/result.hpp"

namespace cio::detail {

class Scheduler;
class Reactor;

enum class Dir : unsigned { kRead = 0, kWrite = 1 };
inline constexpr unsigned kDirCount = 2;

// Sentinel meaning "readiness arrived, no waiter". Never dereferenced.
inline void* const kIoReady = reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));

// The parked-task record stored in an IoDesc slot. Lives in the awaiter, which
// lives in the coroutine frame: parking on I/O never allocates.
//
// Exactly one party ever takes an IoWait out of its slot (the CAS decides), and
// the rule for whoever takes it is: touch the frame only until you schedule it,
// and schedule it last. That is what makes a refcount unnecessary here.
struct IoWait {
    std::coroutine_handle<> handle{};
    Scheduler* sched = nullptr;
    Error err{};  // written by the waker before scheduling
};

struct IoDesc;

// A deadline timer owned by a descriptor. Derives from Timer so it can sit in
// the timer heaps while carrying the back-pointers its callback needs.
struct IoTimer : Timer {
    IoDesc* desc = nullptr;
    Reactor* reactor = nullptr;
    Dir dir = Dir::kRead;
    std::uint32_t seq = 0;
};

// Per-file-descriptor registration. Allocated from a slab that never frees, so
// a stale event dequeued by another thread can always dereference it safely;
// the generation tag tells us whether the contents are still ours.
struct CIO_CACHE_ALIGNED IoDesc {
    std::atomic<void*> slot[kDirCount]{{nullptr}, {nullptr}};
    int fd = -1;
    std::uint32_t index = 0;
    std::atomic<std::uint32_t> generation{0};
    std::atomic<bool> closing{false};
    Reactor* owner = nullptr;
    IoDesc* free_next = nullptr;

    IoTimer deadline_timer[kDirCount];
    // A direction is "timed out" while expired_seq == deadline_seq. Stamping
    // the state with the sequence instead of using a bool is what makes
    // rearming a deadline race-free against the previous timer firing.
    std::atomic<std::uint32_t> deadline_seq[kDirCount]{{1}, {1}};
    std::atomic<std::uint32_t> expired_seq[kDirCount]{{0}, {0}};

    std::atomic<void*>& dir_slot(Dir d) noexcept { return slot[static_cast<unsigned>(d)]; }

    bool timed_out(Dir d) const noexcept {
        const unsigned i = static_cast<unsigned>(d);
        return expired_seq[i].load(std::memory_order_acquire) ==
               deadline_seq[i].load(std::memory_order_acquire);
    }
};

// Awaits readiness in one direction.
//
// Always used inside the readiness loop; a single await is never a complete
// operation, because edge-triggered readiness can be spurious:
//
//     for (;;) {
//         n = ::read(fd, ...);
//         if (n >= 0) return n;
//         if (errno != EAGAIN) return Error::from_errno();
//         if (auto r = co_await IoAwaiter(desc, Dir::kRead); !r) return r.error();
//     }
class IoAwaiter {
public:
    IoAwaiter(IoDesc* desc, Dir dir) noexcept : desc_(desc), dir_(dir) {}

    IoAwaiter(const IoAwaiter&) = delete;
    IoAwaiter& operator=(const IoAwaiter&) = delete;

    bool await_ready() noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    Result<void> await_resume() noexcept;

private:
    IoDesc* desc_;
    Dir dir_;
    IoWait wait_{};
};

class Reactor {
public:
    explicit Reactor(Scheduler& sched);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    // Registers `fd` for edge-triggered read+write notification. The fd must
    // already be non-blocking. The descriptor is released with detach().
    Result<IoDesc*> attach(int fd);

    // Unregisters the fd, waking any parked task with Errc::closed. Must be
    // called before the fd itself is closed.
    void detach(IoDesc* desc);

    // Waits for events and schedules the tasks they unblock.
    //   timeout_ns < 0  block until an event or wake()
    //   timeout_ns == 0 non-blocking drain
    void poll(std::int64_t timeout_ns);

    // Makes a blocked poll() return promptly. Safe from any thread.
    void wake() noexcept;

    // Arms (or with deadline_ns == 0, clears) the deadline for one direction.
    // Only the task that owns that direction may call this.
    void set_deadline(IoDesc* desc, Dir dir, std::int64_t deadline_ns);

    std::size_t registered() const noexcept {
        return registered_.load(std::memory_order_relaxed);
    }

    // Wakes the task parked on (desc, dir), or records readiness if none.
    // A default-constructed `err` means "ready".
    void unblock(IoDesc* desc, Dir dir, Error err) noexcept {
        unblock_impl(desc, dir, err, nullptr);
    }

    Scheduler& scheduler() noexcept { return sched_; }

private:
    static std::coroutine_handle<> on_deadline(Timer* timer) noexcept;

    IoDesc* alloc_desc();
    void free_desc(IoDesc* desc) noexcept;
    IoDesc* desc_at(std::uint32_t index) noexcept;

    // When `deferred` is non-null the woken tasks are queued without waking a
    // worker each, and the count is accumulated there so poll() can issue one
    // batched wake for the whole readiness burst.
    void unblock_impl(IoDesc* desc, Dir dir, Error err, std::uint32_t* deferred) noexcept;
    void dispatch(std::uint64_t token, unsigned dirs, std::uint32_t* deferred) noexcept;

    // Slab: fixed-size chunks, never freed, so pointers stay stable and a stale
    // event is always safe to dereference. Recycling is detected by generation,
    // not by pointer validity.
    static constexpr std::uint32_t kChunkShift = 9;
    static constexpr std::uint32_t kChunkSize = 1u << kChunkShift;
    static constexpr std::uint32_t kChunkMask = kChunkSize - 1;
    static constexpr std::uint32_t kMaxChunks = 8192;  // ~4M descriptors

    Scheduler& sched_;
    int backend_fd_ = -1;  // epoll fd / kqueue fd
    int wake_fd_ = -1;     // eventfd (Linux) or pipe read end (BSD)
    int wake_write_fd_ = -1;
    std::atomic<bool> wake_pending_{false};

    std::atomic<IoDesc*> chunks_[kMaxChunks]{};
    std::atomic<std::uint32_t> chunk_count_{0};
    std::atomic<std::uint32_t> registered_{0};
    std::mutex slab_mutex_;
    IoDesc* free_list_ = nullptr;
};

}  // namespace cio::detail
