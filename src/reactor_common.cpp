// Backend-independent half of the reactor: the descriptor slab, the readiness
// state machine, deadlines, and the awaiter.
#include <cassert>
#include <exception>
#include <limits>
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
    // dispatch() pins the descriptor after its preliminary generation check
    // and validates the incarnation again, so a dequeued old token cannot
    // reach the waiter slots of a recycled descriptor.
    desc->lock_lifecycle();
    desc->generation.fetch_add(1, std::memory_order_acq_rel);
    desc->fd = -1;
    // owner and home_worker are properties of this shard's slab slot and stay
    // immutable across fd incarnations. Keeping closing=true until attach()
    // also prevents an old awaiter from mistaking a free node for a live one.
    desc->closing.store(true, std::memory_order_relaxed);
    desc->slot[0].store(nullptr, std::memory_order_relaxed);
    desc->slot[1].store(nullptr, std::memory_order_relaxed);
    assert(!desc->syscall_active[0].load(std::memory_order_relaxed));
    assert(!desc->syscall_active[1].load(std::memory_order_relaxed));
    desc->unlock_lifecycle();

    std::lock_guard<std::mutex> lock(slab_mutex_);
    desc->free_next = free_list_;
    free_list_ = desc;
}

void Reactor::release_desc(IoDesc* desc) noexcept {
    const std::uint32_t previous =
        desc->refs.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous > 0 && "cio: descriptor reference underflow");
    if (previous == 1) free_desc(desc);
}

// --------------------------------------------------------- readiness core ---

void Reactor::unblock_impl(IoDesc* desc, Dir dir, Error err,
                           ReadyBatch* batch) noexcept {
    // Kernel readiness means a syscall may succeed again. Close/deadline are
    // error completions, not readiness, and must not make the I/O loop attempt
    // another native call.
    if (!err) desc->note_readable(dir);

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
        const SchedulerTarget sched = waiter->sched;
        const WorkerId preferred_worker = waiter->preferred_worker;
        std::coroutine_handle<> handle = waiter->handle;
        // Nothing below may touch `waiter`: scheduling it hands the frame to
        // another thread, which may resume and destroy it immediately.
        if (batch != nullptr) {
            Scheduler::IoCompletionRoute route =
                Scheduler::IoCompletionRoute::kSharedFallback;
            if (SchedulerTarget::dispatch_io(
                    sched, handle, preferred_worker, route)) {
                if (route ==
                    Scheduler::IoCompletionRoute::kLocalFifo) {
                    ++batch->unpublished_local_fifo;
                }
                ++batch->total;
            }
        } else {
            SchedulerTarget::dispatch_completion(
                sched, handle, preferred_worker);
        }
        return;
    }
}

void Reactor::dispatch(std::uint64_t token, unsigned dirs,
                       ReadyBatch* batch) noexcept {
    const auto index = static_cast<std::uint32_t>(token & 0xFFFFFFFFu);
    const auto generation = static_cast<std::uint32_t>(token >> 32);

    IoDesc* desc = desc_at(index);
    if (desc == nullptr) return;
    if (desc->generation.load(std::memory_order_acquire) != generation) return;

    // Pin this incarnation before touching its waiter slots. A token can pass
    // the cheap generation check above, pause, and then outlive
    // detach/free/attach of the same slab address. Incrementing only while the
    // count is non-zero prevents resurrecting a free descriptor; if the count
    // made a 1 -> 0 -> 1 ABA because a new incarnation was attached, the
    // generation/closing check after the CAS rejects it and drops the new
    // incarnation's temporary reference.
    std::uint32_t refs = desc->refs.load(std::memory_order_acquire);
    for (;;) {
        if (refs == 0 ||
            refs == std::numeric_limits<std::uint32_t>::max()) {
            return;
        }
        if (desc->refs.compare_exchange_weak(
                refs, refs + 1, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }

    if (desc->generation.load(std::memory_order_acquire) != generation ||
        desc->closing.load(std::memory_order_acquire)) {
        release_desc(desc);
        return;
    }

    if (dirs & 1u) unblock_impl(desc, Dir::kRead, Error{}, batch);
    if (dirs & 2u) unblock_impl(desc, Dir::kWrite, Error{}, batch);
    release_desc(desc);
}

// -------------------------------------------------------------- deadlines ---

std::coroutine_handle<> Reactor::on_deadline(Timer* timer) noexcept {
    // Safe to keep touching this node after it fires: unlike a select's timer,
    // an IoTimer lives in the reactor's descriptor slab, not in a coroutine
    // frame, so nothing can free it out from under us.
    auto* self = static_cast<IoTimer*>(timer);
    IoDesc* desc = self->desc;
    const auto i = static_cast<unsigned>(self->dir);

    // Serialize expiry publication with await_suspend's
    // "check expiry + consume readiness/publish waiter" transaction. Without
    // this lock the callback can stamp the timeout after await_suspend's check
    // but before it consumes the kIoReady written by unblock(); that turns a
    // persistent deadline into a successful readiness result.
    desc->lock_lifecycle();

    // Stamp only the configuration this node was armed for. clear/rearm bumps
    // deadline_seq after waiting out an in-flight callback, and this explicit
    // check also makes a stale callback harmless if a future implementation
    // changes that waiting strategy.
    if (!desc->closing.load(std::memory_order_acquire) &&
        self->seq ==
            desc->deadline_seq[i].load(std::memory_order_acquire)) {
        desc->expired_seq[i].store(self->seq, std::memory_order_release);
        self->reactor->unblock(desc, self->dir,
                              Error{Errc::timed_out});
    }
    desc->unlock_lifecycle();
    return {};  // unblock() already scheduled whoever was parked
}

void Reactor::set_deadline(IoDesc* desc, Dir dir,
                           std::uint32_t expected_generation,
                           std::int64_t deadline_ns) {
    if (desc == nullptr) return;
    const auto i = static_cast<unsigned>(dir);
    IoTimer& timer = desc->deadline_timer[i];
    std::lock_guard<std::mutex> update_lock(
        desc->deadline_update_mutex[i]);

    // Pin the validated incarnation before dropping lifecycle_lock for
    // disarm(). disarm may wait for a callback that itself needs the lifecycle
    // lock, so holding that lock across the wait would deadlock. The reference
    // prevents close/recycle from reusing this slab node in the gap.
    desc->lock_lifecycle();
    if (desc->generation.load(std::memory_order_acquire) !=
            expected_generation ||
        desc->closing.load(std::memory_order_acquire) ||
        desc->runtime_stopping()) {
        desc->unlock_lifecycle();
        return;
    }
    const std::uint32_t refs =
        desc->refs.fetch_add(1, std::memory_order_relaxed);
    if (refs == std::numeric_limits<std::uint32_t>::max()) {
        std::terminate();
    }
    desc->unlock_lifecycle();

    // Unconditionally: disarm() is what waits out a callback that is still
    // running on this node. Guarding it with a state check would let us re-arm
    // underneath that callback.
    sched_.timers().disarm(&timer);

    desc->lock_lifecycle();
    if (desc->generation.load(std::memory_order_acquire) !=
            expected_generation ||
        desc->closing.load(std::memory_order_acquire) ||
        desc->runtime_stopping()) {
        desc->unlock_lifecycle();
        release_desc(desc);
        return;
    }

    // Clear the absolute state while this configuration is being replaced.
    // The final release store below is the new deadline's commit point.
    desc->absolute_deadline_ns[i].store(0, std::memory_order_release);
    const std::uint32_t seq =
        desc->deadline_seq[i].fetch_add(1, std::memory_order_acq_rel) + 1;

    if (deadline_ns == 0) {
        desc->unlock_lifecycle();
        release_desc(desc);
        return;  // deadline cleared
    }

    timer.desc = desc;
    timer.reactor = this;
    timer.dir = dir;
    timer.seq = seq;
    timer.deadline_ns = deadline_ns;
    timer.waiter = {};
    timer.on_fire = &Reactor::on_deadline;

    if (deadline_ns <= now_ns()) {
        // Publish expiry synchronously. This closes the ready-syscall window:
        // set_deadline(now) has taken effect before the setter returns even if
        // no worker has had a chance to execute a timer callback.
        desc->absolute_deadline_ns[i].store(deadline_ns,
                                            std::memory_order_release);
        desc->expired_seq[i].store(seq, std::memory_order_release);
        unblock(desc, dir, Error{Errc::timed_out});
        desc->unlock_lifecycle();
        release_desc(desc);
        return;
    }

    try {
        // Arm while lifecycle is locked. If close wins afterward it observes
        // closing in order and disarms this node; it cannot finish a disarm and
        // then have this setter link the timer back into a heap.
        sched_.timers().arm(&timer);
    } catch (...) {
        desc->unlock_lifecycle();
        release_desc(desc);
        throw;
    }
    desc->absolute_deadline_ns[i].store(deadline_ns,
                                        std::memory_order_release);
    desc->unlock_lifecycle();
    release_desc(desc);
}

// ---------------------------------------------------------------- awaiter ---

IoAwaiter::IoAwaiter(IoDesc* desc, Dir dir,
                     std::uint32_t generation) noexcept
    : desc_(desc), dir_(dir), generation_(generation) {}

IoAwaiter::~IoAwaiter() {
    if (retained_) desc_->owner->release_desc(desc_);
}

bool IoAwaiter::await_ready() noexcept {
    // A default-constructed, moved-from or closed socket hands us no
    // descriptor. Completing with EBADF is the only thing that is not a crash.
    if (desc_ == nullptr) {
        wait_.err = Error{EBADF};
        return true;
    }
    // Incarnation validation and kIoReady consumption must be serialized with
    // detach/reuse. They are therefore unified in await_suspend's lifecycle
    // critical section instead of racing here.
    return false;
}

bool IoAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    // Copies, because once we publish &wait_ the frame belongs to whoever wakes
    // it and every member of *this becomes unreadable.
    IoDesc* const desc = desc_;
    const Dir dir = dir_;
    const std::uint32_t generation = generation_;
    IoWait* const self = &wait_;

    if (desc == nullptr) {
        self->err = Error{EBADF};
        return false;
    }

    self->handle = h;
    Scheduler* const scheduler = current_scheduler();
    self->sched =
        scheduler == nullptr
            ? SchedulerTarget{}
            : scheduler->completion_target();
    self->preferred_worker = current_worker_id(scheduler);
    self->err = Error{};

    desc->lock_lifecycle();
    if (Error state = desc->io_error(dir, generation); state) {
        desc->unlock_lifecycle();
        self->err = state;
        return false;
    }

    source_lifetime_ =
        desc->owner->scheduler().shared_handle();

    // Acquire the operation reference only when the coroutine really reaches
    // suspension setup. The slab address itself is stable, and the generation
    // check above rejects close/reuse between awaiter construction and here.
    // Publish retained_ before the waiter slot: readiness may resume and
    // destroy this frame as soon as the CAS succeeds.
    desc->refs.fetch_add(1, std::memory_order_relaxed);
    retained_ = true;

    std::atomic<void*>& slot = desc->dir_slot(dir);
    for (;;) {
        void* old = slot.load(std::memory_order_acquire);

        if (old == kIoReady) {
            if (slot.compare_exchange_weak(old, nullptr, std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                desc->unlock_lifecycle();
                return false;  // readiness arrived between await_ready and now
            }
            continue;
        }

        if (old != nullptr) {
            // Two tasks awaiting the same direction of the same descriptor.
            // Go panics on this; we surface it as an error rather than let the
            // second one silently overwrite the first.
            self->err = Error{Errc::broken};
            desc->unlock_lifecycle();
            return false;
        }

        void* expected = nullptr;
        if (slot.compare_exchange_weak(expected, self, std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
            // Published. From here we may only touch locals.
            //
            // detach() takes this lifecycle lock before publishing closing, so
            // it can neither miss this waiter nor recycle the descriptor until
            // publication is complete.
            desc->unlock_lifecycle();
            return true;
        }
    }
}

Result<void> IoAwaiter::await_resume() noexcept {
    // The party that actually took this waiter out of the slot wins over
    // state published later: timeout-then-close remains timeout, and
    // close-then-timeout remains closed.
    if (wait_.err) return wait_.err;

    // The awaiter's descriptor reference prevents recycle until this object is
    // destroyed. Re-check both close and persistent deadline state: readiness
    // may have won the slot CAS just before the deadline callback stamped the
    // descriptor, but that raw edge is not allowed to turn an expired complete
    // operation into success.
    if (retained_) {
        if (Error state = desc_->io_error(dir_, generation_); state) {
            return state;
        }
        // An EPOLLET edge may set the hint before claiming this waiter while
        // the operation's preceding EAGAIN/short-I/O path writes false in
        // between. Every successful readiness consumption converges here, so
        // make this the final hint writer before the I/O loop retries its
        // syscall. Error completions returned above never masquerade as ready.
        desc_->note_readable(dir_);
    }
    return ok();
}

}  // namespace cio::detail
