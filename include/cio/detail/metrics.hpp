// Runtime counters, compiled out unless CIO_METRICS is defined.
//
// This exists because the questions worth asking about a scheduler are not the
// ones a sampling profiler answers. "Which symbol has the most cycles" does not
// tell you how many futex wakes a request costs, how many events an epoll_wait
// returns, or how often a burst overflows a worker's queue into the global one
// — and those are the numbers that decide what to change.
//
// Every counter is a relaxed atomic on its own cache line, so turning this on
// perturbs the thing being measured as little as a counter can. It is still
// off by default: a benchmark and a production build should not differ.
//
//     cmake -S . -B build-metrics -DCIO_METRICS=ON -DCMAKE_BUILD_TYPE=Release
//
// runtime_metrics() links in either build — it just returns zeroes when the
// counters are compiled out — so a diagnostic can be written once and pointed
// at whichever build is interesting.
#pragma once

#include <atomic>
#include <cstdint>

#include "cio/config.hpp"

namespace cio {

// A snapshot of the counters. All zero when metrics are compiled out.
struct RuntimeMetrics {
    std::uint64_t tasks_run = 0;           // coroutine resumptions
    std::uint64_t polls_blocking = 0;      // epoll_wait with a timeout
    std::uint64_t polls_nonblocking = 0;   // opportunistic epoll_wait(0)
    std::uint64_t poll_events = 0;         // events returned by the kernel
    std::uint64_t poll_wakeups = 0;        // tasks those events made runnable
    std::uint64_t parks = 0;               // entries into park()
    std::uint64_t park_cv_waits = 0;       // parks that actually slept on the cv
    std::uint64_t wake_single = 0;         // notify() that posted a token
    std::uint64_t wake_batch_calls = 0;    // notify_batch() that granted > 0
    std::uint64_t wake_batch_workers = 0;  // workers granted by those calls
    std::uint64_t reactor_wakes = 0;       // eventfd writes to interrupt a poll
    std::uint64_t steal_attempts = 0;
    std::uint64_t steal_hits = 0;
    std::uint64_t local_overflow = 0;      // local ring full, half spilled to global
    std::uint64_t global_batch_pops = 0;   // batched takes from the global queue
    std::uint64_t global_batch_items = 0;
};

// Zeroes unless CIO_METRICS is on.
RuntimeMetrics runtime_metrics() noexcept;

namespace detail {

// Internal diagnostic snapshot for the work-aware cooperative-I/O completion
// quota. Keep this separate from the public RuntimeMetrics layout: these
// counters exist to accept or reject scheduler experiments, not as a stable
// API commitment.
//
// In a quiescent snapshot:
//   exhaustions =
//       renew_no_demand + deferred_local_only + forced_yields
//   ticket_polls = ticket_polls_empty + ticket_polls_productive
// The fields are independent relaxed atomics, so an in-flight checkpoint can
// make a live snapshot temporarily violate either identity. "Empty" and
// "productive" describe runnable debt immediately after the ticket poll; a
// later timer/stop check at the same boundary can still force a yield. The
// local/inbox/global reason counters do not partition forced_yields: inbox and
// global can overlap, and a stop-only yield has no reason counter.
struct CooperativeIoMetrics {
    std::uint64_t exhaustions = 0;
    std::uint64_t renew_no_demand = 0;
    std::uint64_t deferred_local_only = 0;
    std::uint64_t forced_yields = 0;
    std::uint64_t yield_local_only = 0;
    std::uint64_t yield_inbox = 0;
    std::uint64_t yield_global = 0;
    std::uint64_t ticket_polls = 0;
    std::uint64_t ticket_polls_empty = 0;
    std::uint64_t ticket_polls_productive = 0;
    std::uint64_t timer_checks = 0;
    std::uint64_t timer_productive = 0;
};

CooperativeIoMetrics cooperative_io_metrics() noexcept;

#if defined(CIO_METRICS)

struct CIO_CACHE_ALIGNED MetricCounter {
    std::atomic<std::uint64_t> value{0};
    void add(std::uint64_t n) noexcept { value.fetch_add(n, std::memory_order_relaxed); }
    std::uint64_t load() const noexcept { return value.load(std::memory_order_relaxed); }
};

struct Metrics {
    MetricCounter tasks_run;
    MetricCounter polls_blocking;
    MetricCounter polls_nonblocking;
    MetricCounter poll_events;
    MetricCounter poll_wakeups;
    MetricCounter parks;
    MetricCounter park_cv_waits;
    MetricCounter wake_single;
    MetricCounter wake_batch_calls;
    MetricCounter wake_batch_workers;
    MetricCounter reactor_wakes;
    MetricCounter steal_attempts;
    MetricCounter steal_hits;
    MetricCounter local_overflow;
    MetricCounter global_batch_pops;
    MetricCounter global_batch_items;
    MetricCounter cooperative_io_exhaustions;
    MetricCounter cooperative_io_renew_no_demand;
    MetricCounter cooperative_io_deferred_local_only;
    MetricCounter cooperative_io_forced_yields;
    MetricCounter cooperative_io_yield_local_only;
    MetricCounter cooperative_io_yield_inbox;
    MetricCounter cooperative_io_yield_global;
    MetricCounter cooperative_io_ticket_polls;
    MetricCounter cooperative_io_ticket_polls_empty;
    MetricCounter cooperative_io_ticket_polls_productive;
    MetricCounter cooperative_io_timer_checks;
    MetricCounter cooperative_io_timer_productive;
};

Metrics& metrics() noexcept;

#define CIO_METRIC(field, n) ::cio::detail::metrics().field.add(n)

#else

#define CIO_METRIC(field, n) ((void)0)

#endif  // CIO_METRICS

}  // namespace detail
}  // namespace cio
