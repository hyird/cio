#pragma once

#include <chrono>
#include <memory>

namespace cio::detail {

class RuntimeState;

[[nodiscard]] std::chrono::steady_clock::time_point current_time();
[[nodiscard]] std::shared_ptr<RuntimeState> require_time_runtime();

}  // namespace cio::detail
