#include "cio/detail/timer.hpp"

#include <algorithm>
#include <cassert>

#include "cio/detail/scheduler.hpp"

namespace cio::detail {
namespace {

// 4-ary heaps beat binary heaps here: timer heaps are pop-heavy and shallow,
// and a 4-way fan-out cuts the depth (and therefore the cache misses on the
// sift path) by half while the extra comparisons stay in registers.
constexpr std::size_t kFanOut = 4;

inline std::size_t parent_of(std::size_t i) noexcept { return (i - 1) / kFanOut; }
inline std::size_t first_child_of(std::size_t i) noexcept { return i * kFanOut + 1; }

constexpr std::uint32_t kNotInHeap = ~0u;

}  // namespace

TimerService::TimerService(Scheduler& sched, std::size_t shard_count) : sched_(sched) {
    if (shard_count == 0) shard_count = 1;
    shards_.reserve(shard_count);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
}

TimerService::~TimerService() = default;

void TimerService::sift_up(std::vector<Timer*>& heap, std::size_t i) noexcept {
    Timer* node = heap[i];
    while (i > 0) {
        const std::size_t p = parent_of(i);
        if (heap[p]->deadline_ns <= node->deadline_ns) break;
        heap[i] = heap[p];
        heap[i]->heap_index = static_cast<std::uint32_t>(i);
        i = p;
    }
    heap[i] = node;
    node->heap_index = static_cast<std::uint32_t>(i);
}

void TimerService::sift_down(std::vector<Timer*>& heap, std::size_t i) noexcept {
    const std::size_t n = heap.size();
    Timer* node = heap[i];
    for (;;) {
        const std::size_t first = first_child_of(i);
        if (first >= n) break;
        const std::size_t last = std::min(first + kFanOut, n);

        std::size_t best = first;
        for (std::size_t c = first + 1; c < last; ++c) {
            if (heap[c]->deadline_ns < heap[best]->deadline_ns) best = c;
        }
        if (heap[best]->deadline_ns >= node->deadline_ns) break;

        heap[i] = heap[best];
        heap[i]->heap_index = static_cast<std::uint32_t>(i);
        i = best;
    }
    heap[i] = node;
    node->heap_index = static_cast<std::uint32_t>(i);
}

void TimerService::heap_remove(std::vector<Timer*>& heap, std::size_t i) noexcept {
    const std::size_t last = heap.size() - 1;
    if (i == last) {
        heap.pop_back();
        return;
    }
    heap[i] = heap[last];
    heap[i]->heap_index = static_cast<std::uint32_t>(i);
    heap.pop_back();
    // The replacement can violate the invariant in either direction.
    sift_down(heap, i);
    sift_up(heap, heap[i]->heap_index);
}

void TimerService::republish(Shard& shard) noexcept {
    shard.earliest.store(shard.heap.empty() ? INT64_MAX : shard.heap[0]->deadline_ns,
                         std::memory_order_release);
}

void TimerService::arm(Timer* timer) {
    Worker* worker = current_worker();
    // A timer armed by one of this scheduler's workers maps directly to that
    // worker's shard; the scheduler constructs exactly one shard per worker.
    // Avoid a hardware divide on every arm. A socket can exceptionally be
    // manipulated from a different runtime, so retain shard zero as the safe
    // foreign-thread fallback.
    const std::uint32_t shard_index =
        worker != nullptr && &worker->scheduler() == &sched_ ? worker->index() : 0u;
    Shard& shard = *shards_[shard_index];
    timer->preferred_worker = shard_index;

    // Arming a node that is already linked into a heap corrupts it: heap_index
    // gets overwritten and the old slot keeps pointing here. Callers must
    // disarm() first, unconditionally.
    assert(timer->heap_index == kNotInHeap && "cio: timer armed while already in a heap");

    timer->shard = shard_index;
    timer->state.store(Timer::kArmed, std::memory_order_relaxed);

    // Read the deadline before publishing: once the shard lock is dropped the
    // timer may fire, resume its task, and destroy the coroutine frame the
    // Timer node lives in.
    const std::int64_t deadline_ns = timer->deadline_ns;

    bool now_earliest = false;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        try {
            shard.heap.push_back(timer);
        } catch (...) {
            // state was published before taking the shard lock so a concurrent
            // disarm waits for us. Restore an unlinked state before propagating
            // allocation failure; heap_index is still the sentinel.
            timer->state.store(Timer::kIdle, std::memory_order_release);
            throw;
        }
        timer->heap_index = static_cast<std::uint32_t>(shard.heap.size() - 1);
        sift_up(shard.heap, timer->heap_index);
        now_earliest = shard.heap[0] == timer;
        republish(shard);
    }

    // Only disturb the parked poller if this timer would fire before it plans
    // to wake up anyway.
    if (now_earliest) sched_.nudge_poller(shard_index, deadline_ns);
}

bool TimerService::disarm(Timer* timer) {
    // Safe to call in any state, and callers must call it unconditionally.
    //
    // Testing `state == kArmed` at the call site and skipping this is a trap:
    // it silently skips the kFiring wait below, so the caller can go on to
    // recycle or re-arm the node while the firing callback is still running.
    // That callback's final `state = kFired` then lands on the node's next
    // incarnation, and the next disarm sees a state that does not match
    // reality — which ends with the same Timer linked into a heap twice and
    // heap_index pointing at the wrong slot.
    for (;;) {
        const std::uint32_t state = timer->state.load(std::memory_order_acquire);

        if (state == Timer::kIdle || state == Timer::kCancelled ||
            state == Timer::kFired) {
            // Not in any heap, and nobody is touching it.
            return false;
        }

        if (state == Timer::kFiring) {
            // The callback is still reading this node and our caller is about
            // to destroy or re-arm it. Wait for it to publish kFired. Bounded
            // and rare: a callback is a compare-exchange, not work.
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#endif
            continue;
        }

        // kArmed. The shard lock is what serialises us against run_expired():
        // it moves the timer out of kArmed while holding the lock, so exactly
        // one of disarm and fire can win.
        Shard& shard = *shards_[timer->shard];
        std::lock_guard<std::mutex> lock(shard.mutex);

        std::uint32_t expected = Timer::kArmed;
        if (timer->state.compare_exchange_strong(expected, Timer::kCancelled,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
            if (timer->heap_index != kNotInHeap) {
                heap_remove(shard.heap, timer->heap_index);
                timer->heap_index = kNotInHeap;
            }
            republish(shard);
            return true;
        }
        // Lost the race; re-examine (it is now kFiring or kFired).
    }
}

std::int64_t TimerService::next_deadline_ns() const noexcept {
    std::int64_t earliest = INT64_MAX;
    for (const auto& shard : shards_) {
        const std::int64_t d = shard->earliest.load(std::memory_order_acquire);
        if (d < earliest) earliest = d;
    }
    return earliest;
}

std::int64_t TimerService::next_timeout_ns() const noexcept {
    const std::int64_t deadline = next_deadline_ns();
    if (deadline == INT64_MAX) return -1;
    const std::int64_t delta = deadline - now_ns();
    return delta > 0 ? delta : 0;
}

std::int64_t TimerService::next_deadline_ns(WorkerId worker) const noexcept {
    if (worker >= shards_.size()) return INT64_MAX;
    return shards_[worker]->earliest.load(std::memory_order_acquire);
}

std::int64_t TimerService::next_timeout_ns(WorkerId worker) const noexcept {
    const std::int64_t deadline = next_deadline_ns(worker);
    if (deadline == INT64_MAX) return -1;
    const std::int64_t delta = deadline - now_ns();
    return delta > 0 ? delta : 0;
}

bool TimerService::empty() const noexcept { return next_deadline_ns() == INT64_MAX; }

std::size_t TimerService::run_expired_shard(Shard& shard, std::int64_t now) {
    constexpr std::size_t kBatch = 64;

    // Everything the firing loop needs is copied out under the lock. For a
    // plain sleep the node is already logically done once we leave the lock, so
    // reading `waiter` afterwards would be a use-after-free the moment the
    // resumed task destroyed its frame.
    struct Fired {
        Timer* timer;
        std::coroutine_handle<> waiter;
        Timer::FireFn on_fire;
        WorkerId preferred_worker;
    };
    Fired ready[kBatch];
    std::size_t total = 0;

    for (;;) {
        std::size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            while (n < kBatch && !shard.heap.empty() && shard.heap[0]->deadline_ns <= now) {
                Timer* timer = shard.heap[0];
                heap_remove(shard.heap, 0);
                timer->heap_index = kNotInHeap;

                const Timer::FireFn on_fire = timer->on_fire;
                std::uint32_t expected = Timer::kArmed;
                const std::uint32_t next = on_fire != nullptr ? Timer::kFiring : Timer::kFired;
                if (timer->state.compare_exchange_strong(expected, next,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_relaxed)) {
                    ready[n++] = Fired{timer, timer->waiter, on_fire,
                                       timer->preferred_worker};
                }
                // Otherwise it was disarmed concurrently and its owner has
                // already taken responsibility for the waiter.
            }
            republish(shard);
        }

        if (n == 0) break;

        // Fire outside the lock. A fired timer resumes a task, and that task
        // may arm another timer on this very shard the moment it runs.
        void* resumable[kBatch];
        std::uint32_t resumable_count = 0;
        WorkerId target = kInvalidWorkerId;
        for (std::size_t i = 0; i < n; ++i) {
            const Fired& fired = ready[i];
            std::coroutine_handle<> resume = fired.waiter;
            if (fired.on_fire != nullptr) {
                resume = fired.on_fire(fired.timer);
                // Publish "nobody is touching this node any more" before the
                // task can possibly resume and destroy the frame holding it.
                fired.timer->state.store(Timer::kFired, std::memory_order_release);
            }
            if (resume) {
                resumable[resumable_count++] = resume.address();
                target = fired.preferred_worker;
            }
        }
        if (resumable_count > 0) {
            sched_.schedule_batch_to(resumable, resumable_count, target);
        }
        total += n;
        if (n < kBatch) break;
    }
    return total;
}

std::size_t TimerService::run_expired() {
    const std::int64_t now = now_ns();
    std::size_t fired = 0;
    for (auto& shard : shards_) {
        // The published earliest deadline lets us skip a shard without taking
        // its lock, which is the whole point of sharding.
        if (shard->earliest.load(std::memory_order_acquire) > now) continue;
        fired += run_expired_shard(*shard, now);
    }
    return fired;
}

std::size_t TimerService::run_expired(WorkerId worker) {
    if (worker >= shards_.size()) return 0;
    const std::int64_t now = now_ns();
    Shard& shard = *shards_[worker];
    if (shard.earliest.load(std::memory_order_acquire) > now) return 0;
    return run_expired_shard(shard, now);
}

void TimerService::drain_all() {
    // Shutdown path. Like Go, we do not unwind tasks that are parked when the
    // runtime stops; we just make sure nothing is left pointing into the heaps.
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mutex);
        for (Timer* timer : shard->heap) {
            timer->state.store(Timer::kCancelled, std::memory_order_release);
            timer->heap_index = kNotInHeap;
        }
        shard->heap.clear();
        republish(*shard);
    }
}

}  // namespace cio::detail
