// Private worker identity used by scheduler-owned shards and wake routing.
//
// This never appears in a public signature. Internal wait records may carry it
// as a transient locality hint, not task affinity: stealing may move a task and
// the next suspension records whichever worker it is then running on.
#pragma once

#include <cstdint>
#include <limits>

namespace cio::detail {

using WorkerId = std::uint32_t;

inline constexpr WorkerId kInvalidWorkerId =
    std::numeric_limits<WorkerId>::max();

}  // namespace cio::detail
