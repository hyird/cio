// select — Go's select statement.
//
//     auto sel = cio::select(cio::recv(jobs), cio::recv(quit), cio::after(1s));
//     switch (co_await sel) {
//         case 0: if (auto job = sel.get<0>()) handle(*job); break;
//         case 1: co_return;
//         case 2: log("idle for a second");
//     }
//
// Case bodies are ordinary code in the enclosing coroutine, so they can
// co_await freely — unlike a callback-per-case design.
//
// Semantics that match Go: a case on a nil channel is never ready (so you can
// disable a case by nulling its channel), ready cases are chosen uniformly at
// random, and adding cio::otherwise() makes the whole select non-blocking.
//
// Implementation notes, in the order they matter:
//
//  1. All involved channels are locked in address order before the ready scan
//     and released only after every waiter has been published. Without that,
//     a case could fire while we were still registering later cases — and
//     resuming a coroutine whose frame is mid-registration is fatal.
//  2. Exactly one waker wins, via a single CAS on `winner`.
//  3. The timer, unlike the channels, has no lock we can share, so publication
//     is handshaked through `phase`: a waker that wins before setup finishes
//     leaves the resume to the setup code.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "cio/chan.hpp"
#include "cio/clock.hpp"
#include "cio/detail/scheduler.hpp"
#include "cio/detail/timer.hpp"

namespace cio {

namespace detail {

struct SelectShared {
    std::atomic<std::uint32_t> winner{kNoWinner};
    std::atomic<std::uint32_t> phase{kSelectSetup};
    std::coroutine_handle<> handle{};
};

// TimeoutCase is often only a fallback for a channel that is already ready.
// Keep the full timer node as inert bytes until select actually has to park.
struct SelectTimer {
    struct Node : Timer {
        Node(std::int64_t deadline, SelectShared* owner,
             std::uint32_t case_index, FireFn fire) noexcept
            : Timer(ArmTag{}, deadline, {}, fire),
              shared(owner),
              index(case_index) {}

        SelectShared* shared;
        std::uint32_t index;
    };

    SelectTimer() noexcept {}
    SelectTimer(const SelectTimer&) noexcept {}
    SelectTimer(SelectTimer&&) noexcept {}
    SelectTimer& operator=(const SelectTimer&) noexcept { return *this; }
    SelectTimer& operator=(SelectTimer&&) noexcept { return *this; }

    Node& construct(std::int64_t deadline, SelectShared* shared,
                    std::uint32_t index, Timer::FireFn fire) noexcept {
        return *::new (static_cast<void*>(storage_))
            Node(deadline, shared, index, fire);
    }

    Node* get() noexcept {
        return std::launder(reinterpret_cast<Node*>(storage_));
    }

private:
    static_assert(std::is_trivially_destructible_v<Node>);
    alignas(Node) std::byte storage_[sizeof(Node)];
};

// Cheap per-thread randomness for choosing among ready cases. Go randomises the
// poll order for exactly this reason: a fixed order lets a hot case starve the
// ones after it forever.
inline std::uint32_t select_rand(std::uint32_t n) noexcept {
    static thread_local std::uint64_t state = 0x2545F4914F6CDD1Dull;
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return n == 0 ? 0 : static_cast<std::uint32_t>((state >> 33) % n);
}

}  // namespace detail

// ------------------------------------------------------------------ cases ---

template<typename T>
struct RecvCase {
    using result_type = std::optional<T>;

    Chan<T> chan;
    std::optional<T> value{};
    detail::ChanWaiter waiter{};

    result_type take() { return std::move(value); }
};

template<typename T>
struct SendCase {
    using result_type = bool;

    Chan<T> chan;
    T value;
    detail::ChanWaiter waiter{};

    result_type take() const noexcept { return waiter.success; }
};

struct TimeoutCase {
    using result_type = void;

    std::int64_t deadline_ns = 0;
    detail::SelectTimer timer{};
    bool armed = false;

    TimeoutCase() = default;
    explicit TimeoutCase(std::int64_t deadline) noexcept
        : deadline_ns(deadline) {}

    // A Timer holds an atomic and so is not movable. Cases are only ever moved
    // while being collected into the Selector — before anything is armed — so
    // moving the deadline and leaving a fresh timer behind is exactly right.
    TimeoutCase(TimeoutCase&& other) noexcept
        : deadline_ns(other.deadline_ns) {}
    TimeoutCase& operator=(TimeoutCase&&) = delete;
    TimeoutCase(const TimeoutCase&) = delete;
    TimeoutCase& operator=(const TimeoutCase&) = delete;

    void take() const noexcept {}
};

struct DefaultCase {
    using result_type = void;
    void take() const noexcept {}
};

// `case v := <-ch:`
template<typename T>
RecvCase<T> recv(Chan<T> chan) {
    return RecvCase<T>{std::move(chan), {}, {}};
}

// `case ch <- v:`
template<typename T>
SendCase<T> send(Chan<T> chan, T value) {
    return SendCase<T>{std::move(chan), std::move(value), {}};
}

// `case <-time.After(d):`
inline TimeoutCase after(Duration duration) {
    return TimeoutCase{deadline_from_now(duration)};
}

inline TimeoutCase after_deadline(TimePoint deadline) {
    return TimeoutCase{std::chrono::duration_cast<std::chrono::nanoseconds>(
                           deadline.time_since_epoch())
                           .count()};
}

// `default:` — makes the select non-blocking.
inline DefaultCase otherwise() {
    return DefaultCase{};
}

namespace detail {

// --- per-case operations, dispatched by overload ---------------------------

template<typename T>
ChannelBase* case_channel(RecvCase<T>& c) noexcept {
    return c.chan.native();
}
template<typename T>
ChannelBase* case_channel(SendCase<T>& c) noexcept {
    return c.chan.native();
}
inline ChannelBase* case_channel(TimeoutCase&) noexcept {
    return nullptr;
}
inline ChannelBase* case_channel(DefaultCase&) noexcept {
    return nullptr;
}

template<typename T>
OpStatus case_try(RecvCase<T>& c, ChanWaiter*& to_wake) {
    if (c.chan.native() == nullptr) return OpStatus::kBlocked;  // nil channel
    return static_cast<Channel<T>*>(c.chan.native())
        ->try_recv_locked(&c.value, to_wake);
}
template<typename T>
OpStatus case_try(SendCase<T>& c, ChanWaiter*& to_wake) {
    if (c.chan.native() == nullptr) return OpStatus::kBlocked;
    const OpStatus status = static_cast<Channel<T>*>(c.chan.native())
                                ->try_send_locked(&c.value, to_wake);
    if (status != OpStatus::kBlocked)
        c.waiter.success = status == OpStatus::kDone;
    return status;
}
inline OpStatus case_try(TimeoutCase&, ChanWaiter*&) noexcept {
    return OpStatus::kBlocked;
}
inline OpStatus case_try(DefaultCase&, ChanWaiter*&) noexcept {
    return OpStatus::kBlocked;
}

template<typename Case>
void init_waiter(Case& c, SelectShared& shared, std::uint32_t index,
                 std::coroutine_handle<> h) {
    c.waiter.handle = h;
    Scheduler* const scheduler = current_scheduler();
    c.waiter.sched = scheduler == nullptr ? SchedulerTarget{}
                                          : scheduler->completion_target();
    c.waiter.select_winner = &shared.winner;
    c.waiter.select_phase = &shared.phase;
    c.waiter.case_index = index;
}

template<typename T>
void case_enqueue(RecvCase<T>& c, SelectShared& shared, std::uint32_t index,
                  std::coroutine_handle<> h) {
    if (c.chan.native() == nullptr) return;
    init_waiter(c, shared, index, h);
    c.waiter.slot = &c.value;
    c.chan.native()->receivers.push_back(&c.waiter);
}
template<typename T>
void case_enqueue(SendCase<T>& c, SelectShared& shared, std::uint32_t index,
                  std::coroutine_handle<> h) {
    if (c.chan.native() == nullptr) return;
    init_waiter(c, shared, index, h);
    c.waiter.slot = &c.value;
    c.chan.native()->senders.push_back(&c.waiter);
}
inline void case_enqueue(TimeoutCase&, SelectShared&, std::uint32_t,
                         std::coroutine_handle<>) noexcept {}
inline void case_enqueue(DefaultCase&, SelectShared&, std::uint32_t,
                         std::coroutine_handle<>) noexcept {}

template<typename T>
void case_retract(RecvCase<T>& c, bool won) {
    // The winning waiter was already popped while the channel lock was held.
    // Re-locking that channel can only discover queued == false.
    if (won) return;
    ChannelBase* channel = c.chan.native();
    if (channel == nullptr) return;
    // Taking the lock is what makes this safe: any waker that popped our node
    // did so under this lock and stopped touching it before releasing.
    std::lock_guard<ChannelMutex> lock(channel->mutex);
    channel->receivers.remove(&c.waiter);
}
template<typename T>
void case_retract(SendCase<T>& c, bool won) {
    if (won) return;
    ChannelBase* channel = c.chan.native();
    if (channel == nullptr) return;
    std::lock_guard<ChannelMutex> lock(channel->mutex);
    channel->senders.remove(&c.waiter);
}
inline void case_retract(TimeoutCase& c, bool /*won*/) {
    if (!c.armed) return;
    c.armed = false;
    // disarm() does not return until a firing callback has stopped touching the
    // node, so it is safe to destroy this frame afterwards.
    current_scheduler()->timers().disarm(c.timer.get());
}
inline void case_retract(DefaultCase&, bool /*won*/) noexcept {}

inline std::coroutine_handle<> select_timeout_fired(Timer* timer) noexcept {
    auto* self = static_cast<SelectTimer::Node*>(timer);
    SelectShared* shared = self->shared;

    std::uint32_t expected = kNoWinner;
    if (!shared->winner.compare_exchange_strong(expected, self->index,
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed)) {
        return {};  // a channel case already won
    }
    if (shared->phase.exchange(kSelectResumed, std::memory_order_acq_rel) !=
        kSelectParked) {
        return {};  // setup is still running and will resume itself
    }
    return shared->handle;
}

inline void case_arm(TimeoutCase& c, SelectShared& shared,
                     std::uint32_t index) {
    SelectTimer::Node& timer =
        c.timer.construct(c.deadline_ns, &shared, index, &select_timeout_fired);
    c.armed = true;
    current_scheduler()->timers().arm(&timer);
}
template<typename Case>
void case_arm(Case&, SelectShared&, std::uint32_t) noexcept {}

template<typename Case>
inline constexpr bool is_default_case =
    std::is_same_v<std::remove_cvref_t<Case>, DefaultCase>;

template<typename Case>
inline constexpr bool is_channel_case = false;
template<typename T>
inline constexpr bool is_channel_case<RecvCase<T>> = true;
template<typename T>
inline constexpr bool is_channel_case<SendCase<T>> = true;

}  // namespace detail

template<typename... Cases>
class [[nodiscard]] Selector {
    static_assert(sizeof...(Cases) > 0, "select needs at least one case");
    static constexpr std::size_t kCount = sizeof...(Cases);

public:
    explicit Selector(Cases... cases) : cases_(std::move(cases)...) {}

    Selector(const Selector&) = delete;
    Selector& operator=(const Selector&) = delete;

    // The result of case I: std::optional<T> for recv, bool for send, void for
    // after()/otherwise().
    template<std::size_t I>
    auto get() {
        return std::get<I>(cases_).take();
    }

    std::size_t index() const noexcept {
        return shared_.winner.load(std::memory_order_acquire);
    }

    auto operator co_await() & noexcept {
        struct Awaiter {
            Selector& selector;

            bool await_ready() const noexcept { return false; }
            bool await_suspend(std::coroutine_handle<> h) {
                return selector.setup(h);
            }
            std::size_t await_resume() { return selector.finish(); }
        };
        return Awaiter{*this};
    }

private:
    static constexpr std::size_t kDefaultCount =
        (std::size_t{0} + ... +
         static_cast<std::size_t>(detail::is_default_case<Cases>));
    static constexpr bool kHasDefault = kDefaultCount != 0;
    static constexpr std::size_t kChannelCount =
        (std::size_t{0} + ... +
         static_cast<std::size_t>(detail::is_channel_case<Cases>));

    // Returns true if the task parked.
    bool setup(std::coroutine_handle<> h) {
        detail::ChannelBase* locked[kCount];
        std::size_t lock_count = 0;
        collect_channels(locked, lock_count);

        // Sort by address, so two selects over the same channels can never
        // deadlock against each other, then drop duplicates so a select that
        // names one channel twice does not try to lock it twice.
        //
        // Insertion sort rather than std::sort: the case count is a
        // compile-time constant in the low single digits, where this is both
        // faster and simpler than an introsort.
        for (std::size_t i = 1; i < lock_count; ++i) {
            detail::ChannelBase* key = locked[i];
            std::size_t j = i;
            while (j > 0 && locked[j - 1] > key) {
                locked[j] = locked[j - 1];
                --j;
            }
            locked[j] = key;
        }
        std::size_t unique_count = 0;
        detail::ChannelBase* previous = nullptr;
        for (std::size_t i = 0; i < lock_count; ++i) {
            if (locked[i] != previous) {
                previous = locked[i];
                locked[unique_count++] = previous;
            }
        }
        lock_count = unique_count;

        std::uint8_t poll_order[kCount];
        if constexpr (kChannelCount == kCount) {
            if constexpr (kChannelCount != 2) fill_poll_order(poll_order);
        } else {
            // The permutation is independent of channel state. Pick it before
            // entering the multi-channel critical section so contending
            // senders and receivers do not wait behind the shuffle.
            fill_channel_poll_order(poll_order);
        }

        for (std::size_t i = 0; i < lock_count; ++i) locked[i]->mutex.lock();

        auto unlock_all = [&] {
            for (std::size_t i = lock_count; i-- > 0;)
                locked[i]->mutex.unlock();
        };

        // Phase 1: is anything ready right now?
        detail::ChanWaiter* to_wake = nullptr;
        if constexpr (kChannelCount == kCount && kChannelCount == 2)
            fill_poll_order(poll_order);

        std::size_t ready = kCount;
        if constexpr (kChannelCount == kCount) {
            for (const std::size_t i : poll_order) {
                visit_case(i, [&](auto& target) {
                    detail::ChanWaiter* woken = nullptr;
                    if (detail::case_try(target, woken) !=
                        detail::OpStatus::kBlocked) {
                        ready = i;
                        to_wake = woken;
                    }
                });
                if (ready != kCount) break;
            }
        } else {
            for (std::size_t position = 0; position < kChannelCount;
                 ++position) {
                const std::size_t i = poll_order[position];
                visit_case(i, [&](auto& target) {
                    detail::ChanWaiter* woken = nullptr;
                    if (detail::case_try(target, woken) !=
                        detail::OpStatus::kBlocked) {
                        ready = i;
                        to_wake = woken;
                    }
                });
                if (ready != kCount) break;
            }
        }

        if (ready != kCount) {
            shared_.winner.store(static_cast<std::uint32_t>(ready),
                                 std::memory_order_release);
            unlock_all();
            if (to_wake != nullptr) to_wake->wake_handoff();
            return false;
        }

        if constexpr (kHasDefault) {
            shared_.winner.store(static_cast<std::uint32_t>(default_index()),
                                 std::memory_order_release);
            unlock_all();
            return false;
        }

        // Phase 2: publish a waiter on every case, still holding every lock, so
        // nothing can fire until we are completely registered.
        shared_.handle = h;
        for_each_case([&](auto& c, std::size_t i) {
            detail::case_enqueue(c, shared_, static_cast<std::uint32_t>(i), h);
        });
        enqueued_ = true;
        unlock_all();

        // Phase 3: the timer. It is armed after the channels because it is the
        // one waker that cannot be held off by a lock; the phase handshake
        // below is what covers the window it opens.
        for_each_case([&](auto& c, std::size_t i) {
            detail::case_arm(c, shared_, static_cast<std::uint32_t>(i));
        });

        // Phase 4: hand ourselves over. If a waker already won, it deliberately
        // did not resume us — we are still on this stack — so resume inline.
        std::uint32_t expected = detail::kSelectSetup;
        return shared_.phase.compare_exchange_strong(
            expected, detail::kSelectParked, std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    std::size_t finish() {
        if (enqueued_) {
            const std::size_t winner =
                shared_.winner.load(std::memory_order_acquire);
            for_each_case([winner](auto& c, std::size_t i) {
                detail::case_retract(c, i == winner);
            });
            enqueued_ = false;
        }
        return shared_.winner.load(std::memory_order_acquire);
    }

    void collect_channels(detail::ChannelBase** out, std::size_t& n) {
        for_each_case([&](auto& c, std::size_t) {
            if (detail::ChannelBase* channel = detail::case_channel(c))
                out[n++] = channel;
        });
    }

    static constexpr std::size_t default_index() {
        std::size_t index = 0;
        std::size_t found = 0;
        ((detail::is_default_case<Cases> ? (found = index++) : (index++, 0)),
         ...);
        return found;
    }

    template<typename F>
    void for_each_case(F&& fn) {
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (fn(std::get<I>(cases_), I), ...);
        }(std::make_index_sequence<kCount>{});
    }

    // A uniform random permutation of [0, kCount), written into `out`. This
    // direct form keeps the all-channel specialization compact.
    static void fill_poll_order(std::uint8_t (&out)[kCount]) noexcept {
        static_assert(kCount <= 255, "poll order indices are uint8_t");
        if constexpr (kCount == 1) {
            out[0] = 0;
        } else if constexpr (kCount == 2) {
            const auto first =
                static_cast<std::uint8_t>(detail::select_rand(2));
            out[0] = first;
            out[1] = static_cast<std::uint8_t>(1 - first);
        } else if constexpr (kCount == 3) {
            static constexpr std::uint8_t kOrders[6][3] = {
                {0, 1, 2}, {0, 2, 1}, {1, 0, 2},
                {1, 2, 0}, {2, 0, 1}, {2, 1, 0},
            };
            const std::uint8_t* picked = kOrders[detail::select_rand(6)];
            out[0] = picked[0];
            out[1] = picked[1];
            out[2] = picked[2];
        } else {
            for (std::size_t i = 0; i < kCount; ++i)
                out[i] = static_cast<std::uint8_t>(i);
            for (std::size_t i = kCount; i > 1; --i) {
                const std::size_t j =
                    detail::select_rand(static_cast<std::uint32_t>(i));
                std::swap(out[i - 1], out[j]);
            }
        }
    }

    // Builds a uniform random permutation of the channel-case tuple indices.
    // Timeout and default cases are control flow during the ready scan: their
    // case_try overloads can only report blocked, so dispatching them cannot
    // select a winner.
    //
    // It has to be a full permutation, not a random rotation. A rotation only
    // randomises where the scan starts, so a case that is never ready still
    // hands its rotations to whichever case follows it: with [ready, ready,
    // nil] the first case wins two rotations out of three, a 2:1 bias against
    // the uniform choice this is documented to make. Go's selectgo shuffles
    // pollorder for the same reason.
    //
    // Fisher-Yates is the general answer, but it is not free and the channel
    // count is almost always 2 or 3. A permutation of n items is just a number
    // in [0, n!), so at those sizes one draw picks the whole permutation and
    // the loop, swaps and n-1 bounded draws disappear.
    static void fill_channel_poll_order(std::uint8_t (&out)[kCount]) noexcept {
        static_assert(kCount <= 255, "poll order indices are uint8_t");
        std::size_t position = 0;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    using Case = std::tuple_element_t<I, std::tuple<Cases...>>;
                    if constexpr (detail::is_channel_case<Case>)
                        out[position++] = static_cast<std::uint8_t>(I);
                }(),
                ...);
        }(std::make_index_sequence<kCount>{});

        if constexpr (kChannelCount == 2) {
            if (detail::select_rand(2) != 0) std::swap(out[0], out[1]);
        } else if constexpr (kChannelCount == 3) {
            static constexpr std::uint8_t kOrders[6][3] = {
                {0, 1, 2}, {0, 2, 1}, {1, 0, 2},
                {1, 2, 0}, {2, 0, 1}, {2, 1, 0},
            };
            const std::uint8_t* picked = kOrders[detail::select_rand(6)];
            const std::uint8_t cases[3] = {out[0], out[1], out[2]};
            out[0] = cases[picked[0]];
            out[1] = cases[picked[1]];
            out[2] = cases[picked[2]];
        } else if constexpr (kChannelCount > 3) {
            for (std::size_t i = kChannelCount; i > 1; --i) {
                const std::size_t j =
                    detail::select_rand(static_cast<std::uint32_t>(i));
                std::swap(out[i - 1], out[j]);
            }
        }
    }

    // Applies `fn` to the case at a runtime index.
    template<typename F>
    void visit_case(std::size_t index, F&& fn) {
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            ((I == index ? (fn(std::get<I>(cases_)), void()) : void()), ...);
        }(std::make_index_sequence<kCount>{});
    }

    std::tuple<Cases...> cases_;
    detail::SelectShared shared_;
    bool enqueued_ = false;
};

template<typename... Cases>
Selector<Cases...> select(Cases... cases) {
    return Selector<Cases...>{std::move(cases)...};
}

}  // namespace cio
