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
};

Metrics& metrics() noexcept;

#define CIO_METRIC(field, n) ::cio::detail::metrics().field.add(n)

#else

#define CIO_METRIC(field, n) ((void)0)

#endif  // CIO_METRICS

}  // namespace detail
}  // namespace cio
