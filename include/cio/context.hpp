// Cancellation scopes that nest and expire, shaped like Go's context package.
//
//     auto request = cio::with_timeout(parent.token(), 5s);
//     auto db = cio::with_cancel(request.token());
//     co_await query(db.token());
//     // cancelling `request`, or its 5s elapsing, cancels `db` too
//
// CancelSource and CancelToken on their own are flat: a token fires when its own
// source is cancelled and never because of anything upstream. That is enough for
// a single scope and not enough for a request, where a deadline on the whole
// operation has to reach every sub-operation without being threaded through by
// hand.
//
// These are free functions rather than a Context type, because cio's token
// already carries what context.Context carries here — a done channel, an error
// and now a deadline. There are no context values: a map keyed by opaque types
// is a dependency-injection mechanism, not a cancellation one, and C++ has
// better ways to pass a request-scoped value.
#pragma once

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include "cio/clock.hpp"
#include "cio/group.hpp"
#include "cio/timer.hpp"

namespace cio {

// Go's context.WithCancel: a scope cancelled by its own cancel() or by `parent`.
//
// The returned source must be kept alive for the link to hold; dropping it
// cancels the scope, which is the same discipline as Go's `defer cancel()`.
CancelSource with_cancel(CancelToken parent);

// Go's context.WithDeadline and context.WithTimeout.
//
// The scope is cancelled at `deadline`, when `parent` is cancelled, or when
// cancel() is called — whichever happens first. A deadline later than the
// parent's is clamped to the parent's, so a child can never outlive the budget
// it was given.
CancelSource with_deadline(CancelToken parent, TimePoint deadline);
CancelSource with_timeout(CancelToken parent, Duration timeout);

// The same three without a parent, for a top-level scope. Go spells the absent
// parent context.Background().
inline CancelSource with_cancel() { return CancelSource{}; }
CancelSource with_deadline(TimePoint deadline);
CancelSource with_timeout(Duration timeout);

}  // namespace cio
