// Lifetime-safe scheduler completion targets.
#pragma once

#include <coroutine>
#include <cstdint>

#include "cio/detail/worker_id.hpp"

namespace cio::detail {

class Scheduler;
struct CompletionEndpoint;

enum class IoCompletionRoute : std::uint8_t {
    kSharedFallback,
    kRunnext,
    kLocalFifo,
};

// A short counted lease obtained by an explicit lock or a foreign/cross-
// runtime dispatch. Same-runtime dispatch helpers need no endpoint reference:
// Scheduler shutdown joins those workers before destroying owned storage.
// A lease is for short scheduling/inspection only; calling Scheduler::shutdown
// through it would wait for that same lease and is therefore invalid.
class SchedulerLease {
public:
    SchedulerLease() noexcept = default;
    SchedulerLease(const SchedulerLease&) = delete;
    SchedulerLease& operator=(const SchedulerLease&) = delete;
    SchedulerLease(SchedulerLease&& other) noexcept;
    SchedulerLease& operator=(SchedulerLease&& other) noexcept;
    ~SchedulerLease();

    Scheduler* get() const noexcept { return scheduler_; }
    Scheduler* operator->() const noexcept { return scheduler_; }
    explicit operator bool() const noexcept {
        return scheduler_ != nullptr;
    }

private:
    friend struct SchedulerTarget;
    SchedulerLease(Scheduler* scheduler,
                   CompletionEndpoint* endpoint) noexcept
        : scheduler_(scheduler), endpoint_(endpoint) {}
    void reset() noexcept;

    Scheduler* scheduler_ = nullptr;
    CompletionEndpoint* endpoint_ = nullptr;
};

// A stable route to a Scheduler. Each endpoint address is unique for the
// process lifetime, so waiters copy one pointer without a shared-refcount RMW
// or an ABA-prone recycled identity. The dispatch helpers are free of atomic
// RMWs when the waker already runs on the target Scheduler; foreign/cross-
// runtime wakers acquire an endpoint lease that Scheduler::shutdown() waits
// out. Explicit lock() always returns a counted lease with a self-contained
// lifetime.
struct SchedulerTarget {
    SchedulerLease lock() const noexcept;
    // Join completion stays local when it originates on the awaiting
    // Scheduler; a foreign completion uses the captured worker only as a soft
    // affinity hint.
    static void dispatch(
        SchedulerTarget target,
        std::coroutine_handle<> handle,
        WorkerId preferred_worker) noexcept;
    // Direct hand-off for channel/synchronisation rendezvous.
    static void dispatch_next(
        SchedulerTarget target,
        std::coroutine_handle<> handle) noexcept;
    // Wakeups which may originate on a pool thread, monitor, or another
    // runtime. A same-runtime preferred worker may retain the continuation;
    // every other path publishes it where an idle worker can take it.
    static void dispatch_completion(
        SchedulerTarget target,
        std::coroutine_handle<> handle,
        WorkerId preferred_worker) noexcept;
    // Reactor batches need to distinguish unpublished local FIFO placement
    // from runnext/shared placement. Returns false when the target has already
    // closed and no scheduling occurred.
    static bool dispatch_io(
        SchedulerTarget target,
        std::coroutine_handle<> handle,
        WorkerId preferred_worker,
        IoCompletionRoute& route) noexcept;
    bool operator==(const SchedulerTarget&) const noexcept = default;

    CompletionEndpoint* endpoint = nullptr;
};

}  // namespace cio::detail
