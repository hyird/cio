// Backend-independent half of the reactor: the descriptor slab, the readiness
// state machine, deadlines, and the awaiter.
#include <new>

#include "cio/detail/reactor.hpp"
#include "cio/detail/scheduler.hpp"

namespace cio::detail {

// ------------------------------------------------------------ descriptors ---

IoDesc* Reactor::desc_at(std::uint32_t index) noexcept {
    const std::uint32_t chunk = index >> kChunkShift;
    if (chunk >= kMaxChunks) return nullptr;
    IoDesc* base = chunks_[chunk].load(std::memory_order_acquire);
    if (base == nullptr) return nullptr;
    return base + (index & kChunkMask);
}

IoDesc* Reactor::alloc_desc() {
    std::lock_guard<std::mutex> lock(slab_mutex_);
    if (free_list_ != nullptr) {
        IoDesc* desc = free_list_;
        free_list_ = desc->free_next;
        desc->free_next = nullptr;
        return desc;
    }

    const std::uint32_t chunk = chunk_count_.load(std::memory_order_relaxed);
    if (chunk >= kMaxChunks) return nullptr;

    auto* base = new (std::nothrow) IoDesc[kChunkSize];
    if (base == nullptr) return nullptr;
    for (std::uint32_t i = 0; i < kChunkSize; ++i) {
        base[i].index = (chunk << kChunkShift) | i;
    }
    chunks_[chunk].store(base, std::memory_order_release);
    chunk_count_.store(chunk + 1, std::memory_order_release);

    // Everything except the one we are handing out goes on the free list.
    for (std::uint32_t i = kChunkSize; i-- > 1;) {
        base[i].free_next = free_list_;
        free_list_ = &base[i];
    }
    return &base[0];
}

void Reactor::free_desc(IoDesc* desc) noexcept {
    // Bumping the generation before recycling is what makes a stale event safe:
    // a poller that dequeued an event for the old incarnation will fail the
    // generation check and drop it.
    //
    // A poller can still slip through if it passes the check and then the
    // descriptor is recycled before it calls unblock(). The consequence is a
    // spurious readiness edge on an unrelated socket, and every operation in
    // this runtime treats readiness as a hint that must be confirmed by the
    // syscall — so it costs one EAGAIN, never correctness.
    desc->generation.fetch_add(1, std::memory_order_acq_rel);
    desc->fd = -1;
    desc->owner = nullptr;
    desc->closing.store(false, std::memory_order_relaxed);
    desc->slot[0].store(nullptr, std::memory_order_relaxed);
    desc->slot[1].store(nullptr, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(slab_mutex_);
    desc->free_next = free_list_;
    free_list_ = desc;
}

// --------------------------------------------------------- readiness core ---

void Reactor::unblock(IoDesc* desc, Dir dir, Error err) noexcept {
    std::atomic<void*>& slot = desc->dir_slot(dir);
    for (;;) {
        void* old = slot.load(std::memory_order_acquire);

        if (old == kIoReady) return;  // an edge is already recorded

        if (old == nullptr) {
            // Nobody is waiting. Record the edge so the next waiter consumes it
            // instead of parking — without this, edge-triggered polling loses
            // the wakeup for any operation that was between its EAGAIN and its
            // park when the event arrived.
            if (slot.compare_exchange_weak(old, kIoReady, std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                return;
            }
            continue;
        }

        if (!slot.compare_exchange_weak(old, nullptr, std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
            continue;
        }

        auto* waiter = static_cast<IoWait*>(old);
        waiter->err = err;
        Scheduler* sched = waiter->sched;
        std::coroutine_handle<> handle = waiter->handle;
        // Nothing below may touch `waiter`: scheduling it hands the frame to
        // another thread, which may resume and destroy it immediately.
        if (sched != nullptr) sched->schedule(handle);
        return;
    }
}

void Reactor::dispatch(std::uint64_t token, unsigned dirs) noexcept {
    const auto index = static_cast<std::uint32_t>(token & 0xFFFFFFFFu);
    const auto generation = static_cast<std::uint32_t>(token >> 32);

    IoDesc* desc = desc_at(index);
    if (desc == nullptr) return;
    if (desc->generation.load(std::memory_order_acquire) != generation) return;

    if (dirs & 1u) unblock(desc, Dir::kRead, Error{});
    if (dirs & 2u) unblock(desc, Dir::kWrite, Error{});
}

// -------------------------------------------------------------- deadlines ---

std::coroutine_handle<> Reactor::on_deadline(Timer* timer) noexcept {
    // Safe to keep touching this node after it fires: unlike a select's timer,
    // an IoTimer lives in the reactor's descriptor slab, not in a coroutine
    // frame, so nothing can free it out from under us.
    auto* self = static_cast<IoTimer*>(timer);
    IoDesc* desc = self->desc;
    const auto i = static_cast<unsigned>(self->dir);

    // Stamp the expiry with the sequence this timer was armed under. If a
    // set_deadline() raced us it has already bumped deadline_seq, so this stamp
    // never compares equal and the stale expiry is simply invisible.
    desc->expired_seq[i].store(self->seq, std::memory_order_release);
    self->reactor->unblock(desc, self->dir, Error{Errc::timed_out});
    return {};  // unblock() already scheduled whoever was parked
}

void Reactor::set_deadline(IoDesc* desc, Dir dir, std::int64_t deadline_ns) {
    const auto i = static_cast<unsigned>(dir);
    IoTimer& timer = desc->deadline_timer[i];

    // Unconditionally: disarm() is what waits out a callback that is still
    // running on this node. Guarding it with a state check would let us re-arm
    // underneath that callback.
    sched_.timers().disarm(&timer);

    const std::uint32_t seq =
        desc->deadline_seq[i].fetch_add(1, std::memory_order_acq_rel) + 1;

    if (deadline_ns == 0) return;  // deadline cleared

    timer.desc = desc;
    timer.reactor = this;
    timer.dir = dir;
    timer.seq = seq;
    timer.deadline_ns = deadline_ns;
    timer.waiter = {};
    timer.on_fire = &Reactor::on_deadline;
    sched_.timers().arm(&timer);
}

// ---------------------------------------------------------------- awaiter ---

bool IoAwaiter::await_ready() noexcept {
    if (desc_->timed_out(dir_)) {
        wait_.err = Error{Errc::timed_out};
        return true;
    }
    if (desc_->closing.load(std::memory_order_acquire)) {
        wait_.err = Error{Errc::closed};
        return true;
    }

    std::atomic<void*>& slot = desc_->dir_slot(dir_);
    void* old = slot.load(std::memory_order_acquire);
    if (old == kIoReady && slot.compare_exchange_strong(old, nullptr, std::memory_order_acq_rel,
                                                        std::memory_order_relaxed)) {
        wait_.err = Error{};
        return true;
    }
    return false;
}

bool IoAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    // Copies, because once we publish &wait_ the frame belongs to whoever wakes
    // it and every member of *this becomes unreadable.
    IoDesc* const desc = desc_;
    const Dir dir = dir_;
    IoWait* const self = &wait_;

    self->handle = h;
    self->sched = current_scheduler();
    self->err = Error{};

    std::atomic<void*>& slot = desc->dir_slot(dir);
    for (;;) {
        void* old = slot.load(std::memory_order_acquire);

        if (old == kIoReady) {
            if (slot.compare_exchange_weak(old, nullptr, std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                return false;  // readiness arrived between await_ready and now
            }
            continue;
        }

        if (old != nullptr) {
            // Two tasks awaiting the same direction of the same descriptor.
            // Go panics on this; we surface it as an error rather than let the
            // second one silently overwrite the first.
            self->err = Error{Errc::broken};
            return false;
        }

        void* expected = nullptr;
        if (slot.compare_exchange_weak(expected, self, std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
            // Published. From here we may only touch locals.
            //
            // Re-check for a close that ran before we were visible: detach()
            // sets `closing` and then unblocks, so if it did both before our
            // CAS we would park forever. Re-running the unblock protocol is
            // idempotent, and whichever side takes us out of the slot resumes
            // us exactly once.
            if (desc->closing.load(std::memory_order_acquire) && desc->owner != nullptr) {
                desc->owner->unblock(desc, dir, Error{Errc::closed});
            }
            return true;
        }
    }
}

Result<void> IoAwaiter::await_resume() noexcept {
    if (wait_.err) return wait_.err;
    return ok();
}

}  // namespace cio::detail
