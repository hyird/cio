// Definitions for the runtime counters declared in detail/metrics.hpp.
//
// Compiled unconditionally so that cio::runtime_metrics() always links; what
// CIO_METRICS changes is whether anything ever increments the counters. A
// build without it returns an all-zero snapshot rather than failing to link,
// which is what lets a diagnostic tool be written once and pointed at either
// build.
//
// CIO_METRICS is deliberately a PRIVATE compile definition on the library
// target, and every CIO_METRIC() call site lives in a .cpp file here. Putting
// one in a header would make an inline function's body depend on a macro the
// consumer does not define — an ODR violation that links cleanly and then
// misbehaves.
#include "cio/detail/metrics.hpp"

namespace cio {

#if defined(CIO_METRICS)

namespace detail {

Metrics& metrics() noexcept {
    // Function-local static: the counters must be alive for any thread that
    // touches them, including one retiring after main() has returned.
    static Metrics instance;
    return instance;
}

}  // namespace detail

RuntimeMetrics runtime_metrics() noexcept {
    const detail::Metrics& m = detail::metrics();
    RuntimeMetrics out;
    out.tasks_run = m.tasks_run.load();
    out.polls_blocking = m.polls_blocking.load();
    out.polls_nonblocking = m.polls_nonblocking.load();
    out.poll_events = m.poll_events.load();
    out.poll_wakeups = m.poll_wakeups.load();
    out.parks = m.parks.load();
    out.park_cv_waits = m.park_cv_waits.load();
    out.wake_single = m.wake_single.load();
    out.wake_batch_calls = m.wake_batch_calls.load();
    out.wake_batch_workers = m.wake_batch_workers.load();
    out.reactor_wakes = m.reactor_wakes.load();
    out.steal_attempts = m.steal_attempts.load();
    out.steal_hits = m.steal_hits.load();
    out.local_overflow = m.local_overflow.load();
    out.global_batch_pops = m.global_batch_pops.load();
    out.global_batch_items = m.global_batch_items.load();
    return out;
}

detail::CooperativeIoMetrics
detail::cooperative_io_metrics() noexcept {
    const Metrics& m = metrics();
    CooperativeIoMetrics out;
    out.exhaustions =
        m.cooperative_io_exhaustions.load();
    out.renew_no_demand =
        m.cooperative_io_renew_no_demand.load();
    out.deferred_local_only =
        m.cooperative_io_deferred_local_only.load();
    out.forced_yields =
        m.cooperative_io_forced_yields.load();
    out.yield_local_only =
        m.cooperative_io_yield_local_only.load();
    out.yield_inbox =
        m.cooperative_io_yield_inbox.load();
    out.yield_global =
        m.cooperative_io_yield_global.load();
    out.ticket_polls =
        m.cooperative_io_ticket_polls.load();
    out.ticket_polls_empty =
        m.cooperative_io_ticket_polls_empty.load();
    out.ticket_polls_productive =
        m.cooperative_io_ticket_polls_productive.load();
    out.timer_checks =
        m.cooperative_io_timer_checks.load();
    out.timer_productive =
        m.cooperative_io_timer_productive.load();
    return out;
}

#else

RuntimeMetrics runtime_metrics() noexcept { return RuntimeMetrics{}; }

detail::CooperativeIoMetrics
detail::cooperative_io_metrics() noexcept {
    return {};
}

#endif  // CIO_METRICS

}  // namespace cio
