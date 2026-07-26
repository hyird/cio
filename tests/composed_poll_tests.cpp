#include <atomic>
#include <barrier>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/detail/composed_poll.hpp"
#include "cio/detail/cooperative.hpp"

namespace {

using cio::Task;
using cio::detail::ComposedWakeGate;
using cio::detail::TaskPollLane;
using cio::runtime::Runtime;
using cio::sync::Notify;

static_assert(std::is_move_constructible_v<
              cio::detail::CooperativeProgressTicket>);
static_assert(!std::is_move_assignable_v<
              cio::detail::CooperativeProgressTicket>);

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<int> ready_value(int value) {
  co_return value;
}

Task<void> ready_void() {
  co_return;
}

Task<int> wait_value(Notify gate, int value) {
  co_await gate.notified();
  co_return value;
}

struct LifetimeProbe final {
  explicit LifetimeProbe(
      std::shared_ptr<std::atomic<std::size_t>> live_count)
      : live{std::move(live_count)} {
    live->fetch_add(1, std::memory_order_relaxed);
  }

  LifetimeProbe(const LifetimeProbe&) = delete;
  LifetimeProbe& operator=(const LifetimeProbe&) = delete;

  LifetimeProbe(LifetimeProbe&& other) noexcept
      : live{std::move(other.live)} {}

  LifetimeProbe& operator=(LifetimeProbe&&) = delete;

  ~LifetimeProbe() {
    if (live) {
      live->fetch_sub(1, std::memory_order_relaxed);
    }
  }

  std::shared_ptr<std::atomic<std::size_t>> live;
};

Task<int> pending_with_probe(
    Notify gate,
    std::shared_ptr<std::atomic<std::size_t>> live_count) {
  LifetimeProbe probe{std::move(live_count)};
  co_await gate.notified();
  (void)probe;
  co_return 9;
}

Task<int> failing_lane() {
  throw std::runtime_error{"lane failure"};
  co_return 0;
}

Task<bool> immediate_ready_root() {
  ComposedWakeGate gate;
  TaskPollLane<int> lane{ready_value(17), gate};
  auto result = lane.poll_once();
  if (!result.is_ready() || std::move(result).take_value() != 17 ||
      lane.active()) {
    co_return false;
  }

  bool rejected = false;
  try {
    (void)lane.poll_once();
  } catch (const std::logic_error&) {
    rejected = true;
  }
  if (!rejected) {
    co_return false;
  }

  TaskPollLane<void> void_lane{ready_void(), gate};
  auto void_result = void_lane.poll_once();
  if (!void_result.is_ready()) {
    co_return false;
  }
  void_result.take_value();
  co_return !void_lane.active();
}

Task<bool> wake_before_park_root() {
  ComposedWakeGate gate;
  auto registration = gate.register_lane();
  const auto token = registration.activate();
  const auto observed = gate.sequence();
  token.wake();
  co_await gate.wait_after(observed);
  co_return gate.sequence() == observed + 1;
}

Task<bool> stale_generation_root() {
  ComposedWakeGate gate;
  auto stale_registration = gate.register_lane();
  const auto stale = stale_registration.activate();
  stale_registration.invalidate();
  auto fresh_registration = gate.register_lane();
  const auto fresh = fresh_registration.activate();
  if (stale.key() == fresh.key()) {
    co_return false;
  }

  const auto observed = gate.sequence();
  stale.wake();
  if (gate.sequence() != observed) {
    co_return false;
  }
  fresh.wake();
  co_await gate.wait_after(observed);
  co_return gate.sequence() == observed + 1;
}

Task<bool> pending_then_ready_root() {
  Notify source;
  ComposedWakeGate gate;
  TaskPollLane<int> lane{wait_value(source, 23), gate};
  const auto observed = gate.sequence();
  if (lane.poll_once().is_ready()) {
    co_return false;
  }

  source.notify_one();
  co_await gate.wait_after(observed);
  auto completed = lane.poll_once();
  co_return completed.is_ready() &&
      std::move(completed).take_value() == 23;
}

Task<bool> two_lanes_do_not_overwrite_root() {
  Notify first_source;
  Notify second_source;
  ComposedWakeGate gate;
  TaskPollLane<int> first{wait_value(first_source, 31), gate};
  TaskPollLane<int> second{wait_value(second_source, 47), gate};
  const auto observed = gate.sequence();
  if (first.poll_once().is_ready() ||
      second.poll_once().is_ready()) {
    co_return false;
  }

  first_source.notify_one();
  second_source.notify_one();
  co_await gate.wait_after(observed);

  auto first_result = first.poll_once();
  auto second_result = second.poll_once();
  co_return gate.sequence() == observed + 2 &&
      first_result.is_ready() &&
      second_result.is_ready() &&
      std::move(first_result).take_value() == 31 &&
      std::move(second_result).take_value() == 47;
}

Task<bool> one_lane_wake_does_not_resume_peer_root() {
  Notify first_source;
  Notify second_source;
  ComposedWakeGate gate;
  TaskPollLane<int> first{wait_value(first_source, 37), gate};
  TaskPollLane<int> second{wait_value(second_source, 41), gate};
  auto observed = gate.sequence();
  if (first.poll_once().is_ready() ||
      second.poll_once().is_ready()) {
    co_return false;
  }

  first_source.notify_one();
  co_await gate.wait_after(observed);
  auto first_result = first.poll_once();
  auto second_pending = second.poll_once();
  if (!first_result.is_ready() ||
      std::move(first_result).take_value() != 37 ||
      second_pending.is_ready()) {
    co_return false;
  }

  observed = gate.sequence();
  second_source.notify_one();
  co_await gate.wait_after(observed);
  auto second_result = second.poll_once();
  co_return second_result.is_ready() &&
      std::move(second_result).take_value() == 41;
}

Task<bool> move_pending_lane_root() {
  Notify source;
  ComposedWakeGate gate;
  TaskPollLane<int> original{wait_value(source, 59), gate};
  const auto observed = gate.sequence();
  if (original.poll_once().is_ready()) {
    co_return false;
  }

  TaskPollLane<int> moved{std::move(original)};
  source.notify_one();
  co_await gate.wait_after(observed);
  auto result = moved.poll_once();
  co_return result.is_ready() &&
      std::move(result).take_value() == 59;
}

Task<bool> cancel_destroys_frame_and_ignores_late_wake_root() {
  Notify source;
  ComposedWakeGate gate;
  auto live_count = std::make_shared<std::atomic<std::size_t>>(0);
  TaskPollLane<int> lane{
      pending_with_probe(source, live_count),
      gate};
  if (lane.poll_once().is_ready() ||
      live_count->load(std::memory_order_relaxed) != 1) {
    co_return false;
  }

  lane.cancel_now();
  if (lane.active() ||
      live_count->load(std::memory_order_relaxed) != 0) {
    co_return false;
  }
  const auto observed = gate.sequence();
  source.notify_one();
  co_await cio::task::yield_now();
  co_return gate.sequence() == observed;
}

Task<bool> exception_cleans_lane_root() {
  ComposedWakeGate gate;
  TaskPollLane<int> lane{failing_lane(), gate};
  bool observed = false;
  try {
    (void)lane.poll_once();
  } catch (const std::runtime_error& error) {
    observed = std::string_view{error.what()} == "lane failure";
  }
  co_return observed && !lane.active();
}

cio::task::JoinHandle<void> spawn_budget_peer(
    std::shared_ptr<std::atomic<bool>> ran) {
  return cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<std::atomic<bool>> owned_ran) -> Task<void> {
        owned_ran->store(true, std::memory_order_release);
        co_return;
      },
      std::move(ran)));
}

Task<void> consume_child_budget(std::size_t units) {
  for (std::size_t index = 0; index < units; ++index) {
    auto ticket =
        co_await cio::detail::acquire_cooperative_progress();
    ticket.made_progress();
  }
}

Task<std::size_t> first_peer_iteration_after_lane_budget(
    bool commit_outer) {
  co_await cio::task::yield_now();
  {
    auto outer =
        co_await cio::detail::acquire_cooperative_progress();
    ComposedWakeGate gate;
    TaskPollLane<void> lane{consume_child_budget(7), gate};
    if (!lane.poll_once().is_ready()) {
      co_return 0;
    }
    if (commit_outer) {
      outer.made_progress();
    }
  }

  const auto peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto peer = spawn_budget_peer(peer_ran);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    auto ticket =
        co_await cio::detail::acquire_cooperative_progress();
    ticket.made_progress();
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() ? first_peer_iteration : 0;
}

Task<std::size_t> first_peer_iteration_after_moved_refund() {
  co_await cio::task::yield_now();
  {
    auto original =
        co_await cio::detail::acquire_cooperative_progress();
    auto moved = std::move(original);
    (void)moved;
  }

  const auto peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto peer = spawn_budget_peer(peer_ran);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    auto ticket =
        co_await cio::detail::acquire_cooperative_progress();
    ticket.made_progress();
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() ? first_peer_iteration : 0;
}

Task<bool> cooperative_ticket_refund_and_commit_root() {
  const auto refund_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto refund_peer = spawn_budget_peer(refund_peer_ran);
  for (std::size_t iteration = 0; iteration < 512; ++iteration) {
    auto ticket =
        co_await cio::detail::acquire_cooperative_progress();
    (void)ticket;
    if (refund_peer_ran->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  const auto refund_joined = co_await refund_peer;
  if (!refund_joined.has_value()) {
    co_return false;
  }

  const auto committed_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto committed_peer = spawn_budget_peer(committed_peer_ran);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    auto ticket =
        co_await cio::detail::acquire_cooperative_progress();
    ticket.made_progress();
    if (first_peer_iteration == 0 &&
        committed_peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto committed_joined = co_await committed_peer;
  if (!committed_joined.has_value() ||
      first_peer_iteration != 129) {
    co_return false;
  }

  // 子 lane 与父组合操作共享同一份 budget；外层 Pending 退款必须恢复
  // 扣费前的完整快照，抹除 lane 内部已经提交的七次扣费。
  const auto restored_boundary =
      co_await first_peer_iteration_after_lane_budget(false);
  if (restored_boundary != 129) {
    co_return false;
  }

  if (co_await first_peer_iteration_after_moved_refund() != 129) {
    co_return false;
  }

  // 外层提交时，父 ticket 与 lane 的七次扣费都必须保留。
  const auto committed_boundary =
      co_await first_peer_iteration_after_lane_budget(true);
  co_return committed_boundary == 121;
}

Task<bool> external_wake_root(
    Notify source,
    std::shared_ptr<std::atomic<bool>> waiting) {
  ComposedWakeGate gate;
  TaskPollLane<int> lane{wait_value(source, 71), gate};
  const auto observed = gate.sequence();
  if (lane.poll_once().is_ready()) {
    co_return false;
  }
  waiting->store(true, std::memory_order_release);
  co_await gate.wait_after(observed);
  auto result = lane.poll_once();
  co_return result.is_ready() &&
      std::move(result).take_value() == 71;
}

void multi_thread_external_wake_test() {
  Notify source;
  auto waiting = std::make_shared<std::atomic<bool>>(false);
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  std::jthread waker{[source, waiting] {
    while (!waiting->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    source.notify_one();
  }};
  check(runtime.block_on(cio::task::assume_portable(
            external_wake_root(source, waiting))),
        "multi-thread 外部 lane wake 没有恢复父组合 task");
}

void simultaneous_wake_test() {
  ComposedWakeGate gate;
  auto first_registration = gate.register_lane();
  auto second_registration = gate.register_lane();
  const auto first = first_registration.activate();
  const auto second = second_registration.activate();
  const auto observed = gate.sequence();
  std::barrier start{3};

  std::jthread first_thread{[&] {
    start.arrive_and_wait();
    first.wake();
  }};
  std::jthread second_thread{[&] {
    start.arrive_and_wait();
    second.wake();
  }};
  start.arrive_and_wait();
  first_thread.join();
  second_thread.join();

  check(gate.sequence() == observed + 2,
        "两个线程同时 wake 必须各发布一次 sequence");
}

void run_async_cases() {
  simultaneous_wake_test();
  multi_thread_external_wake_test();
  Runtime runtime;
  check(runtime.block_on(immediate_ready_root()),
        "立即完成 lane 状态错误");
  check(runtime.block_on(wake_before_park_root()),
        "wake-before-park 丢失");
  check(runtime.block_on(stale_generation_root()),
        "迟到 generation wake 未失效");
  check(runtime.block_on(pending_then_ready_root()),
        "Pending lane 恢复错误");
  check(runtime.block_on(two_lanes_do_not_overwrite_root()),
        "两个 Pending lane 的 resumable 被覆盖");
  check(runtime.block_on(
            one_lane_wake_does_not_resume_peer_root()),
        "单 lane wake 错误恢复了未通知 peer lane");
  check(runtime.block_on(move_pending_lane_root()),
        "Pending lane 移动后所有权错误");
  check(runtime.block_on(
            cancel_destroys_frame_and_ignores_late_wake_root()),
        "lane 取消未同步销毁 frame 或屏蔽迟到 wake");
  check(runtime.block_on(exception_cleans_lane_root()),
        "lane 异常未清理 owning frame");
  check(runtime.block_on(
            cooperative_ticket_refund_and_commit_root()),
        "cooperative ticket Pending 退款或 progress 提交边界错误");
}

}  // namespace

int main() {
  try {
    run_async_cases();
  } catch (const std::exception& error) {
    std::cerr << "[FAIL] composed poll: " << error.what() << '\n';
    return 1;
  }

  std::cout << "composed poll tests passed: 13/13\n";
  return 0;
}
