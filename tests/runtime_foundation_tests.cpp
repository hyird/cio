#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/detail/waker.hpp"

struct MultiThreadProbe final {
  void record_thread() {
    std::lock_guard lock{mutex};
    threads.insert(std::this_thread::get_id());
  }

  [[nodiscard]] std::size_t thread_count() const {
    std::lock_guard lock{mutex};
    return threads.size();
  }

  mutable std::mutex mutex;
  std::set<std::thread::id> threads;
  std::atomic<int> completions{0};
  std::atomic<int> spinner_progress{0};
  std::atomic<int> sentinel_observed{-1};
};

struct MutexRecord final {
  struct Nested final {
    int value{0};
  };

  int first{0};
  Nested nested;
};

namespace cio {
template <> struct sync_traits<::MultiThreadProbe> : std::true_type {};

template <> struct send_traits<::MutexRecord> : std::true_type {};

template <> struct sync_traits<::MutexRecord> : std::true_type {};
} // namespace cio

namespace {

using cio::Task;
using cio::runtime::Runtime;
using namespace std::chrono_literals;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<int> immediate_value(int value) { co_return value; }

Task<int> nested_value() {
  const auto first = co_await immediate_value(20);
  const auto second = co_await immediate_value(22);
  co_return first + second;
}

Task<std::size_t> deep_task_chain(std::size_t remaining) {
  if (remaining == 0) {
    co_return 0;
  }
  co_return 1 + co_await deep_task_chain(remaining - 1);
}

Task<void> record_immediate(const std::shared_ptr<std::vector<int>> &order,
                            int value) {
  order->push_back(value);
  co_return;
}

Task<bool> verify_nested_task_stays_in_current_poll() {
  const auto order = std::make_shared<std::vector<int>>();
  auto spawned = cio::task::spawn(record_immediate(order, 3));
  order->push_back(1);
  co_await record_immediate(order, 2);
  const bool composed_inline = *order == std::vector<int>({1, 2});
  auto result = co_await spawned;
  co_return composed_inline &&result.has_value() &&
      *order == std::vector<int>({1, 2, 3});
}

Task<void> mark_started(const std::shared_ptr<std::atomic<int>> &polls) {
  polls->fetch_add(1, std::memory_order_relaxed);
  co_return;
}

Task<bool>
verify_spawn_is_deferred(const std::shared_ptr<std::atomic<int>> &polls) {
  auto handle = cio::task::spawn(mark_started(polls));
  const bool deferred = polls->load(std::memory_order_relaxed) == 0;
  auto result = co_await handle;
  co_return deferred &&result.has_value() &&
      polls->load(std::memory_order_relaxed) == 1;
}

Task<void>
detached_worker(const std::shared_ptr<std::atomic<bool>> &completed) {
  co_await cio::task::yield_now();
  completed->store(true, std::memory_order_release);
}

Task<void> detach_parent(const std::shared_ptr<std::atomic<bool>> &completed) {
  {
    auto handle = cio::task::spawn(detached_worker(completed));
    (void)handle;
  }
  co_await cio::task::yield_now();
  co_await cio::task::yield_now();
}

struct DestructionProbe final {
  std::shared_ptr<std::atomic<bool>> destroyed;

  ~DestructionProbe() { destroyed->store(true, std::memory_order_release); }
};

Task<void>
cancellable_task(const std::shared_ptr<std::atomic<bool>> &started,
                 const std::shared_ptr<std::atomic<bool>> &destroyed) {
  DestructionProbe probe{destroyed};
  started->store(true, std::memory_order_release);
  while (true) {
    co_await cio::task::yield_now();
  }
}

Task<bool>
abort_after_first_poll(const std::shared_ptr<std::atomic<bool>> &started,
                       const std::shared_ptr<std::atomic<bool>> &destroyed) {
  auto handle = cio::task::spawn(cancellable_task(started, destroyed));
  co_await cio::task::yield_now();
  handle.abort();
  handle.abort();
  auto result = co_await handle;
  co_return started->load(std::memory_order_acquire) &&
      destroyed->load(std::memory_order_acquire) && !result.has_value() &&
      result.error().is_cancelled();
}

Task<int>
never_polled_after_abort(const std::shared_ptr<std::atomic<bool>> &started) {
  started->store(true, std::memory_order_release);
  co_return 7;
}

Task<bool> await_cancelled(cio::task::JoinHandle<int> handle) {
  auto result = co_await handle;
  co_return !result.has_value() && result.error().is_cancelled();
}

Task<bool> await_void_cancelled(cio::task::JoinHandle<void> handle) {
  auto result = co_await handle;
  co_return !result.has_value() && result.error().is_cancelled();
}

Task<int> throwing_task() {
  throw std::runtime_error{"spawned boom"};
  co_return 0;
}

Task<bool> verify_spawned_exception() {
  auto handle = cio::task::spawn(throwing_task());
  auto result = co_await handle;
  if (result.has_value() || !result.error().is_panic() ||
      !result.error().exception()) {
    co_return false;
  }

  try {
    std::rethrow_exception(result.error().exception());
  } catch (const std::runtime_error &error) {
    co_return std::string_view{error.what()} == "spawned boom";
  }
  co_return false;
}

class Never final {
public:
  [[nodiscard]] bool await_ready() const noexcept { return false; }

  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise>) const noexcept {
    // 测试专用永久挂起点；句柄不保存，也不产生 wake。
  }

  void await_resume() const noexcept {}
};

Task<void>
suspended_until_shutdown(const std::shared_ptr<std::atomic<bool>> &started,
                         const std::shared_ptr<std::atomic<bool>> &destroyed) {
  DestructionProbe probe{destroyed};
  started->store(true, std::memory_order_release);
  co_await Never{};
}

Task<void> empty_root() { co_return; }

Task<cio::detail::TaskKey> capture_current_task_key() {
  co_return cio::detail::require_execution_context()->task_key();
}

Task<void> stale_key_target(const std::shared_ptr<std::atomic<int>> &polls,
                            const std::shared_ptr<std::atomic<bool>> &resumed) {
  polls->fetch_add(1, std::memory_order_relaxed);
  co_await Never{};
  resumed->store(true, std::memory_order_release);
}

class SlotWait final {
public:
  explicit SlotWait(std::shared_ptr<cio::detail::WakerSlot> slot)
      : slot_{std::move(slot)} {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }

  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    return slot_->register_waiter(
        cio::detail::require_execution_context(),
        cio::detail::CoroutineRef::from_abi(coroutine));
  }

  void await_resume() const noexcept {}

private:
  std::shared_ptr<cio::detail::WakerSlot> slot_;
};

Task<void> wait_for_slot(const std::shared_ptr<cio::detail::WakerSlot> &slot,
                         const std::shared_ptr<std::atomic<bool>> &resumed) {
  co_await SlotWait{slot};
  resumed->store(true, std::memory_order_release);
}

Task<void>
wait_for_external_wake(const std::shared_ptr<cio::detail::WakerSlot> &slot,
                       const std::shared_ptr<std::atomic<bool>> &registering,
                       const std::shared_ptr<std::atomic<bool>> &resumed) {
  registering->store(true, std::memory_order_release);
  co_await SlotWait{slot};
  resumed->store(true, std::memory_order_release);
}

Task<bool>
verify_wait_then_wake(const std::shared_ptr<cio::detail::WakerSlot> &slot,
                      const std::shared_ptr<std::atomic<bool>> &resumed) {
  auto handle = cio::task::spawn(wait_for_slot(slot, resumed));
  co_await cio::task::yield_now();
  const bool suspended = !resumed->load(std::memory_order_acquire);
  slot->wake();
  auto result = co_await handle;
  co_return suspended &&result.has_value() &&
      resumed->load(std::memory_order_acquire);
}

Task<void>
delayed_detached_child(const std::shared_ptr<std::atomic<bool>> &completed) {
  for (int step = 0; step < 4; ++step) {
    co_await cio::task::yield_now();
  }
  completed->store(true, std::memory_order_release);
}

Task<void> parent_waiting_on_child(
    const std::shared_ptr<std::atomic<bool>> &child_completed) {
  auto child = cio::task::spawn(delayed_detached_child(child_completed));
  (void)co_await child;
}

Task<bool> cancel_join_waiter_case(
    const std::shared_ptr<std::atomic<bool>> &child_completed) {
  auto parent = cio::task::spawn(parent_waiting_on_child(child_completed));
  co_await cio::task::yield_now();
  co_await cio::task::yield_now();
  parent.abort();
  auto parent_result = co_await parent;

  while (!child_completed->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_return !parent_result.has_value() && parent_result.error().is_cancelled();
}

Task<void>
nested_block_on_attempt(const std::shared_ptr<Runtime> &runtime,
                        const std::shared_ptr<std::atomic<bool>> &rejected) {
  try {
    (void)runtime->block_on(immediate_value(1));
  } catch (const std::logic_error &) {
    rejected->store(true, std::memory_order_release);
  }
  co_return;
}

Task<void> attempt_sleep_without_driver() { co_await cio::time::sleep(1ms); }

Task<bool> paused_sleep_rounding_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::sleep(1ns);
  const bool initially_pending = !timer.is_elapsed();
  co_await timer;
  const auto elapsed = cio::time::Instant::now() - start;
  co_return initially_pending &&timer.is_elapsed() &&
      elapsed == std::chrono::duration_cast<cio::time::Duration>(1ms);
}

Task<void>
wait_on_shared_sleep(const std::shared_ptr<cio::time::Sleep> &timer,
                     const std::shared_ptr<std::atomic<bool>> &completed) {
  co_await *timer;
  completed->store(true, std::memory_order_release);
}

Task<bool> reset_suspended_sleep_case() {
  const auto start = cio::time::Instant::now();
  const auto completed = std::make_shared<std::atomic<bool>>(false);
  const auto timer = std::make_shared<cio::time::Sleep>(cio::time::sleep(5s));
  auto waiter = cio::task::spawn(wait_on_shared_sleep(timer, completed));

  co_await cio::task::yield_now();
  timer->reset(start + std::chrono::duration_cast<cio::time::Duration>(10s));
  const auto result = co_await waiter;

  co_return result.has_value() && completed->load(std::memory_order_acquire) &&
      cio::time::Instant::now() - start ==
          std::chrono::duration_cast<cio::time::Duration>(10s);
}

Task<void> long_sleep() { co_await cio::time::sleep(100s); }

Task<bool> cancelling_sleep_releases_timer_case() {
  auto sleeper = cio::task::spawn(long_sleep());
  co_await cio::task::yield_now();

  const auto runtime = cio::detail::require_execution_context()->runtime();
  const bool registered = runtime && runtime->active_timer_count() == 1;
  sleeper.abort();
  const auto result = co_await sleeper;
  co_return registered && !result.has_value() &&
      result.error().is_cancelled() && runtime->active_timer_count() == 0;
}

Task<bool> reset_elapsed_sleep_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::sleep(2s);
  co_await timer;
  const bool first_elapsed = timer.is_elapsed();

  timer.reset(cio::time::Instant::now() +
              std::chrono::duration_cast<cio::time::Duration>(3s));
  const bool pending_again = !timer.is_elapsed();
  co_await timer;

  co_return first_elapsed && pending_again && timer.is_elapsed() &&
      cio::time::Instant::now() - start ==
          std::chrono::duration_cast<cio::time::Duration>(5s);
}

Task<void>
mark_after_five_seconds(const std::shared_ptr<std::atomic<bool>> &completed) {
  co_await cio::time::sleep(5s);
  completed->store(true, std::memory_order_release);
}

Task<bool> manual_advance_case() {
  const auto start = cio::time::Instant::now();
  const auto completed = std::make_shared<std::atomic<bool>>(false);
  auto sleeper = cio::task::spawn(mark_after_five_seconds(completed));
  co_await cio::task::yield_now();

  co_await cio::time::advance(4s);
  const bool pending_at_four =
      !completed->load(std::memory_order_acquire) &&
      cio::time::Instant::now() - start ==
          std::chrono::duration_cast<cio::time::Duration>(4s);

  co_await cio::time::advance(1s);
  const bool advance_did_not_join = !completed->load(std::memory_order_acquire);
  const auto result = co_await sleeper;

  co_return pending_at_four && advance_did_not_join && result.has_value() &&
      completed->load(std::memory_order_acquire) &&
      cio::time::Instant::now() - start ==
          std::chrono::duration_cast<cio::time::Duration>(5s);
}

Task<bool> overflow_wheel_case() {
  const auto start = cio::time::Instant::now();
  const auto duration = std::chrono::duration_cast<cio::time::Duration>(
      std::chrono::hours{24 * 365 * 3});
  co_await cio::time::sleep(duration);
  co_return cio::time::Instant::now() - start == duration;
}

Task<bool> pause_resume_error_case() {
  bool duplicate_pause_rejected = false;
  bool duplicate_resume_rejected = false;
  try {
    cio::time::pause();
  } catch (const std::logic_error &) {
    duplicate_pause_rejected = true;
  }

  cio::time::resume();
  try {
    cio::time::resume();
  } catch (const std::logic_error &) {
    duplicate_resume_rejected = true;
  }
  cio::time::pause();
  co_return duplicate_pause_rejected &&duplicate_resume_rejected;
}

Task<bool> cross_thread_reset_case() {
  const auto start = cio::time::Instant::now();
  const auto timer = std::make_shared<cio::time::Sleep>(cio::time::sleep(5s));
  std::jthread resetter{[timer] {
    std::this_thread::sleep_for(10ms);
    timer->reset(cio::time::Instant::now() +
                 std::chrono::duration_cast<cio::time::Duration>(5ms));
  }};

  co_await *timer;
  const auto elapsed = cio::time::Instant::now() - start;
  co_return elapsed >= std::chrono::duration_cast<cio::time::Duration>(10ms) &&
      elapsed < std::chrono::duration_cast<cio::time::Duration>(1s);
}

Task<bool> timeout_immediate_future_wins_case() {
  auto result = co_await cio::time::timeout(cio::time::Duration::zero(),
                                            immediate_value(42));
  co_return result.has_value() && result.value() == 42;
}

Task<int> value_after_five_seconds() {
  co_await cio::time::sleep(5s);
  co_return 42;
}

Task<bool> timeout_same_deadline_prefers_value_case() {
  auto result = co_await cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(5s),
      value_after_five_seconds());
  co_return result.has_value() && result.value() == 42;
}

Task<int> timeout_value_with_destruction(
    const std::shared_ptr<std::atomic<bool>> &destroyed) {
  DestructionProbe probe{destroyed};
  co_await cio::time::sleep(5s);
  co_return 42;
}

Task<bool> timeout_elapsed_destroys_value_case() {
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto result = co_await cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(3s),
      timeout_value_with_destruction(destroyed));
  const auto runtime = cio::detail::require_execution_context()->runtime();
  co_return !result.has_value() &&
      result.error().message() == "deadline 已到" &&
      destroyed->load(std::memory_order_acquire) && runtime &&
      runtime->active_timer_count() == 0;
}

Task<bool> timeout_exception_propagates_case() {
  try {
    (void)co_await cio::time::timeout(
        std::chrono::duration_cast<cio::time::Duration>(5s), throwing_task());
  } catch (const std::runtime_error &error) {
    co_return std::string_view{error.what()} == "spawned boom";
  }
  co_return false;
}

Task<bool> timeout_into_inner_case() {
  auto wrapped = cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(5s), immediate_value(42));
  auto inner = std::move(wrapped).into_inner();
  co_return co_await std::move(inner) == 42;
}

Task<void>
timeout_parent_for_abort(const std::shared_ptr<std::atomic<bool>> &started,
                         const std::shared_ptr<std::atomic<bool>> &destroyed) {
  auto value = [started, destroyed]() -> Task<void> {
    DestructionProbe probe{destroyed};
    started->store(true, std::memory_order_release);
    co_await cio::time::sleep(100s);
  };
  (void)co_await cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(50s), value());
}

Task<bool> timeout_parent_abort_case() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto parent = cio::task::spawn(timeout_parent_for_abort(started, destroyed));

  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  parent.abort();
  const auto result = co_await parent;
  const auto runtime = cio::detail::require_execution_context()->runtime();
  co_return !result.has_value() && result.error().is_cancelled() &&
      destroyed->load(std::memory_order_acquire) && runtime &&
      runtime->active_timer_count() == 0;
}

Task<bool> interval_basic_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::interval(2s);
  const auto first = co_await timer.tick();
  const auto second = co_await timer.tick();
  const auto third = co_await timer.tick();
  const auto runtime = cio::detail::require_execution_context()->runtime();

  co_return first == start &&second - start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      third - start == std::chrono::duration_cast<cio::time::Duration>(4s) &&
      cio::time::Instant::now() == third && runtime &&
      runtime->active_timer_count() == 0;
}

Task<bool> interval_missed_tick_cases() {
  const auto burst_start = cio::time::Instant::now();
  auto burst = cio::time::interval(2s);
  (void)co_await burst.tick();
  co_await cio::time::advance(10s);
  const auto burst_missed = co_await burst.tick();
  const auto burst_next = co_await burst.tick();
  const bool burst_ok =
      burst_missed - burst_start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      burst_next - burst_start ==
          std::chrono::duration_cast<cio::time::Duration>(4s) &&
      cio::time::Instant::now() - burst_start ==
          std::chrono::duration_cast<cio::time::Duration>(10s);

  const auto delay_start = cio::time::Instant::now();
  auto delay = cio::time::interval(2s);
  delay.set_missed_tick_behavior(cio::time::MissedTickBehavior::delay);
  (void)co_await delay.tick();
  co_await cio::time::advance(10s);
  const auto delay_missed = co_await delay.tick();
  const auto delay_next = co_await delay.tick();
  const bool delay_ok =
      delay_missed - delay_start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      delay_next - delay_start ==
          std::chrono::duration_cast<cio::time::Duration>(12s);

  const auto skip_start = cio::time::Instant::now();
  auto skip = cio::time::interval(2s);
  skip.set_missed_tick_behavior(cio::time::MissedTickBehavior::skip);
  (void)co_await skip.tick();
  co_await cio::time::advance(9s);
  const auto skip_missed = co_await skip.tick();
  const auto skip_next = co_await skip.tick();
  const bool skip_ok =
      skip_missed - skip_start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      skip_next - skip_start ==
          std::chrono::duration_cast<cio::time::Duration>(10s);

  co_return burst_ok && delay_ok && skip_ok;
}

Task<bool> interval_cancel_safe_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::interval_at(
      start + std::chrono::duration_cast<cio::time::Duration>(5s),
      std::chrono::duration_cast<cio::time::Duration>(2s));
  auto first_attempt = cio::task::spawn(timer.tick());
  co_await cio::task::yield_now();
  first_attempt.abort();
  const auto cancelled = co_await first_attempt;
  const auto first = co_await timer.tick();
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      first - start == std::chrono::duration_cast<cio::time::Duration>(5s);
}

Task<void> multi_thread_child(std::shared_ptr<MultiThreadProbe> probe,
                              int yields) {
  for (int step = 0; step < yields; ++step) {
    probe->record_thread();
    co_await cio::task::yield_now();
  }
  probe->record_thread();
  probe->completions.fetch_add(1, std::memory_order_relaxed);
}

Task<bool> multi_thread_fanout_root(std::shared_ptr<MultiThreadProbe> probe) {
  std::vector<cio::task::JoinHandle<void>> handles;
  constexpr int task_count = 600;
  handles.reserve(task_count);
  for (int index = 0; index < task_count; ++index) {
    handles.push_back(cio::task::spawn(cio::task::owned(
        [](std::shared_ptr<MultiThreadProbe> child_probe,
           int yields) -> Task<void> {
          co_await multi_thread_child(std::move(child_probe), yields);
        },
        probe, 8)));
  }

  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return probe->completions.load(std::memory_order_acquire) == task_count &&
      probe->thread_count() >= 2;
}

Task<void> multi_thread_spinner(std::shared_ptr<MultiThreadProbe> probe) {
  for (int step = 0; step < 1'000; ++step) {
    probe->spinner_progress.store(step + 1, std::memory_order_release);
    co_await cio::task::yield_now();
  }
}

Task<void> multi_thread_sentinel(std::shared_ptr<MultiThreadProbe> probe) {
  probe->sentinel_observed.store(
      probe->spinner_progress.load(std::memory_order_acquire),
      std::memory_order_release);
  co_return;
}

Task<bool> multi_thread_fairness_root(std::shared_ptr<MultiThreadProbe> probe) {
  auto spinner = cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<MultiThreadProbe> child_probe) -> Task<void> {
        co_await multi_thread_spinner(std::move(child_probe));
      },
      probe));
  auto sentinel = cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<MultiThreadProbe> child_probe) -> Task<void> {
        co_await multi_thread_sentinel(std::move(child_probe));
      },
      probe));

  const auto sentinel_result = co_await sentinel;
  const auto spinner_result = co_await spinner;
  const auto observed =
      probe->sentinel_observed.load(std::memory_order_acquire);
  co_return sentinel_result.has_value() && spinner_result.has_value() &&
      observed >= 0 && observed < 1'000;
}

Task<void> multi_thread_budget_hog(std::shared_ptr<MultiThreadProbe> probe) {
  for (int step = 0; step < 1'000; ++step) {
    probe->spinner_progress.store(step + 1, std::memory_order_release);
    co_await cio::task::consume_budget();
  }
}

Task<bool>
multi_thread_cooperative_budget_root(std::shared_ptr<MultiThreadProbe> probe) {
  auto hog = cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<MultiThreadProbe> child_probe) -> Task<void> {
        co_await multi_thread_budget_hog(std::move(child_probe));
      },
      probe));
  auto sentinel = cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<MultiThreadProbe> child_probe) -> Task<void> {
        co_await multi_thread_sentinel(std::move(child_probe));
      },
      probe));

  const auto sentinel_result = co_await sentinel;
  const auto observed =
      probe->sentinel_observed.load(std::memory_order_acquire);
  const auto hog_result = co_await hog;
  co_return sentinel_result.has_value() && hog_result.has_value() &&
      observed >= 128 && observed <= 512;
}

Task<bool> multi_thread_timer_root() {
  const auto start = cio::time::Instant::now();
  co_await cio::time::sleep(5ms);
  co_return cio::time::Instant::now() - start >=
      std::chrono::duration_cast<cio::time::Duration>(5ms);
}

Task<bool> blocking_basic_root() {
  const auto caller_thread =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  auto handle = cio::task::spawn_blocking(
      [](std::size_t caller) {
        return std::hash<std::thread::id>{}(std::this_thread::get_id()) !=
               caller;
      },
      caller_thread);
  const auto result = co_await handle;
  co_return result.has_value() && result.value();
}

Task<bool>
blocking_queued_abort_root(std::shared_ptr<std::atomic<bool>> first_started,
                           std::shared_ptr<std::atomic<bool>> release_first,
                           std::shared_ptr<std::atomic<int>> second_calls) {
  auto first = cio::task::spawn_blocking(
      [](std::shared_ptr<std::atomic<bool>> started,
         std::shared_ptr<std::atomic<bool>> release) {
        started->store(true, std::memory_order_release);
        while (!release->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        return 1;
      },
      first_started, release_first);
  while (!first_started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }

  auto second = cio::task::spawn_blocking(
      [](std::shared_ptr<std::atomic<int>> calls) {
        calls->fetch_add(1, std::memory_order_relaxed);
        return 2;
      },
      second_calls);
  second.abort();
  release_first->store(true, std::memory_order_release);

  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return first_result.has_value() && first_result.value() == 1 &&
      !second_result.has_value() && second_result.error().is_cancelled() &&
      second_calls->load(std::memory_order_acquire) == 0;
}

Task<bool>
blocking_running_abort_root(std::shared_ptr<std::atomic<bool>> started,
                            std::shared_ptr<std::atomic<bool>> release) {
  auto handle = cio::task::spawn_blocking(
      [](std::shared_ptr<std::atomic<bool>> job_started,
         std::shared_ptr<std::atomic<bool>> job_release) {
        job_started->store(true, std::memory_order_release);
        while (!job_release->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        return 42;
      },
      started, release);
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  handle.abort();
  release->store(true, std::memory_order_release);
  const auto result = co_await handle;
  co_return result.has_value() && result.value() == 42;
}

Task<bool>
blocking_thread_cap_root(std::shared_ptr<std::atomic<int>> running,
                         std::shared_ptr<std::atomic<int>> maximum,
                         std::shared_ptr<std::atomic<bool>> release) {
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(3);
  for (int index = 0; index < 3; ++index) {
    handles.push_back(cio::task::spawn_blocking(
        [](std::shared_ptr<std::atomic<int>> active,
           std::shared_ptr<std::atomic<int>> observed_maximum,
           std::shared_ptr<std::atomic<bool>> job_release) {
          const auto now = active->fetch_add(1, std::memory_order_acq_rel) + 1;
          auto previous = observed_maximum->load(std::memory_order_acquire);
          while (previous < now && !observed_maximum->compare_exchange_weak(
                                       previous, now, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          }
          while (!job_release->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          active->fetch_sub(1, std::memory_order_acq_rel);
        },
        running, maximum, release));
  }
  while (maximum->load(std::memory_order_acquire) < 2) {
    co_await cio::task::yield_now();
  }
  release->store(true, std::memory_order_release);
  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return maximum->load(std::memory_order_acquire) == 2 &&
      running->load(std::memory_order_acquire) == 0;
}

Task<bool> blocking_exception_root() {
  auto handle = cio::task::spawn_blocking(
      []() -> int { throw std::runtime_error{"blocking boom"}; });
  const auto id = handle.id();
  const auto result = co_await handle;
  co_return !result.has_value() && result.error().is_panic() &&
      result.error().id() == id && result.error().exception() != nullptr;
}

Task<bool> nested_blocking_root() {
  auto outer = cio::task::spawn_blocking(
      [] { return cio::task::spawn_blocking([] { return 17; }); });
  auto outer_result = co_await outer;
  if (!outer_result.has_value()) {
    co_return false;
  }
  auto inner = std::move(outer_result).value();
  const auto inner_result = co_await inner;
  co_return inner_result.has_value() && inner_result.value() == 17;
}

Task<bool> blocking_inhibits_paused_time_root() {
  const auto start = cio::time::Instant::now();
  const auto timer_completed = std::make_shared<std::atomic<bool>>(false);
  auto timer = cio::task::spawn(mark_after_five_seconds(timer_completed));
  auto blocking =
      cio::task::spawn_blocking([] { std::this_thread::sleep_for(20ms); });
  const auto blocking_result = co_await blocking;
  const bool stayed_frozen = cio::time::Instant::now() == start &&
                             !timer_completed->load(std::memory_order_acquire);
  timer.abort();
  const auto timer_result = co_await timer;
  co_return blocking_result.has_value() && stayed_frozen &&
      !timer_result.has_value() && timer_result.error().is_cancelled();
}

Task<bool> notify_permit_coalescing_root() {
  cio::sync::Notify notify;
  notify.notify_one();
  notify.notify_one();

  auto first = notify.notified();
  const bool first_ready = first.enable();
  co_await first;

  auto second = notify.notified();
  const bool second_waited = !second.enable();
  notify.notify_one();
  co_await second;
  co_return first_ready &&second_waited;
}

Task<bool> notify_fifo_lifo_root() {
  cio::sync::Notify notify;
  auto first = notify.notified();
  auto second = notify.notified();
  auto third = notify.notified();
  const bool registered =
      !first.enable() && !second.enable() && !third.enable();

  notify.notify_one();
  const bool fifo_first = first.enable() && !second.enable() && !third.enable();

  notify.notify_last();
  const bool lifo_third = third.enable() && !second.enable();

  notify.notify_one();
  co_await second;
  co_return registered && fifo_first && lifo_third;
}

Task<bool> notify_waiters_snapshot_root() {
  cio::sync::Notify notify;
  notify.notify_one();
  auto first = notify.notified();
  auto second = notify.notified_owned();
  notify.notify_waiters();

  const bool broadcast_seen = first.enable() && second.enable();
  co_await first;
  co_await second;

  auto permit = notify.notified();
  const bool permit_retained = permit.enable();
  co_await permit;

  cio::sync::Notify no_stored_broadcast;
  no_stored_broadcast.notify_waiters();
  auto after = no_stored_broadcast.notified();
  const bool no_permit_stored = !after.enable();
  no_stored_broadcast.notify_one();
  co_await after;

  co_return broadcast_seen && permit_retained && no_permit_stored;
}

Task<bool> notify_cancel_transfers_root() {
  cio::sync::Notify notify;
  std::optional<cio::sync::Notify::Notified> first;
  first.emplace(notify.notified());
  auto second = notify.notified();
  const bool registered = !first->enable() && !second.enable();
  notify.notify_one();
  first.reset();
  const bool transferred = second.enable();
  co_await second;
  co_return registered &&transferred;
}

Task<bool> notify_cancel_unnotified_root() {
  cio::sync::Notify notify;
  {
    auto cancelled = notify.notified();
    if (cancelled.enable()) {
      co_return false;
    }
  }
  auto next = notify.notified();
  const bool still_waiting = !next.enable();
  notify.notify_one();
  co_await next;
  co_return still_waiting;
}

Task<bool> notify_cross_thread_root(cio::sync::Notify notify) {
  for (int iteration = 0; iteration < 100; ++iteration) {
    auto waiter = notify.notified();
    if (waiter.enable()) {
      co_return false;
    }
    std::jthread notifier{[notify] { notify.notify_one(); }};
    co_await waiter;
  }
  co_return true;
}

Task<void> notify_fanout_child(cio::sync::Notify notify,
                               std::shared_ptr<std::atomic<int>> completed) {
  co_await notify.notified();
  completed->fetch_add(1, std::memory_order_relaxed);
}

Task<bool> notify_cancel_race_root() {
  for (int iteration = 0; iteration < 100; ++iteration) {
    cio::sync::Notify notify;
    const auto completed = std::make_shared<std::atomic<bool>>(false);
    auto child = cio::task::spawn(cio::task::owned(
        [](cio::sync::Notify child_notify,
           std::shared_ptr<std::atomic<bool>> child_completed) -> Task<void> {
          co_await child_notify.notified();
          child_completed->store(true, std::memory_order_release);
        },
        notify, completed));
    co_await cio::task::yield_now();

    std::jthread notifier{[notify] { notify.notify_one(); }};
    child.abort();
    const auto result = co_await child;
    if (result.has_value() != completed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> notify_retired_waiter_lifetime_root() {
  constexpr int iterations = 64;
  for (int iteration = 0; iteration < iterations; ++iteration) {
    {
      cio::sync::Notify notify;
      std::optional<cio::sync::Notify::Notified> cancelled;
      cancelled.emplace(notify.notified());
      auto survivor = notify.notified();
      if (cancelled->enable() || survivor.enable()) {
        co_return false;
      }

      const auto start = std::make_shared<std::atomic<bool>>(false);
      std::jthread notifier{[notify, start] {
        while (!start->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        notify.notify_one();
      }};
      start->store(true, std::memory_order_release);
      cancelled.reset();
      notifier.join();
      if (!survivor.enable()) {
        co_return false;
      }
      co_await survivor;
    }

    {
      cio::sync::Notify notify;
      auto survivor = notify.notified();
      std::optional<cio::sync::Notify::Notified> cancelled;
      cancelled.emplace(notify.notified());
      if (survivor.enable() || cancelled->enable()) {
        co_return false;
      }

      const auto start = std::make_shared<std::atomic<bool>>(false);
      std::jthread notifier{[notify, start] {
        while (!start->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        notify.notify_last();
      }};
      start->store(true, std::memory_order_release);
      cancelled.reset();
      notifier.join();
      if (!survivor.enable()) {
        co_return false;
      }
      co_await survivor;
    }

    {
      cio::sync::Notify notify;
      std::optional<cio::sync::Notify::Notified> cancelled;
      cancelled.emplace(notify.notified());
      auto survivor = notify.notified();
      if (cancelled->enable() || survivor.enable()) {
        co_return false;
      }

      const auto start = std::make_shared<std::atomic<bool>>(false);
      std::jthread notifier{[notify, start] {
        while (!start->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        notify.notify_waiters();
      }};
      start->store(true, std::memory_order_release);
      cancelled.reset();
      notifier.join();
      if (!survivor.enable()) {
        co_return false;
      }
      co_await survivor;
    }
  }
  co_return true;
}

Task<bool> notify_waiters_fanout_root() {
  cio::sync::Notify notify;
  const auto completed = std::make_shared<std::atomic<int>>(0);
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(100);
  for (int index = 0; index < 100; ++index) {
    handles.push_back(cio::task::spawn(notify_fanout_child(notify, completed)));
  }
  co_await cio::task::yield_now();
  notify.notify_waiters();
  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return completed->load(std::memory_order_acquire) == 100;
}

Task<void> semaphore_order_child(cio::sync::Semaphore semaphore,
                                 std::uint32_t permits, int label,
                                 std::shared_ptr<std::vector<int>> order,
                                 cio::sync::Notify release,
                                 std::shared_ptr<std::atomic<bool>> failed) {
  auto result = co_await semaphore.acquire_many(permits);
  if (!result.has_value()) {
    failed->store(true, std::memory_order_release);
    co_return;
  }
  auto permit = std::move(result).value();
  order->push_back(label);
  co_await release.notified();
}

Task<bool> semaphore_fifo_head_blocking_root() {
  cio::sync::Semaphore semaphore{0};
  cio::sync::Notify release;
  const auto order = std::make_shared<std::vector<int>>();
  const auto failed = std::make_shared<std::atomic<bool>>(false);

  auto first = cio::task::spawn(
      semaphore_order_child(semaphore, 3, 1, order, release, failed));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(
      semaphore_order_child(semaphore, 1, 2, order, release, failed));
  co_await cio::task::yield_now();
  auto third = cio::task::spawn(
      semaphore_order_child(semaphore, 1, 3, order, release, failed));
  co_await cio::task::yield_now();

  semaphore.add_permits(2);
  co_await cio::task::yield_now();
  const bool head_blocked =
      order->empty() && semaphore.available_permits() == 0;

  semaphore.add_permits(1);
  for (int attempt = 0; attempt < 20 && order->size() < 1; ++attempt) {
    co_await cio::task::yield_now();
  }
  const bool first_woke = *order == std::vector<int>({1});

  semaphore.add_permits(1);
  for (int attempt = 0; attempt < 20 && order->size() < 2; ++attempt) {
    co_await cio::task::yield_now();
  }
  const bool second_woke = *order == std::vector<int>({1, 2});

  semaphore.add_permits(1);
  for (int attempt = 0; attempt < 20 && order->size() < 3; ++attempt) {
    co_await cio::task::yield_now();
  }
  const bool third_woke = *order == std::vector<int>({1, 2, 3});

  release.notify_waiters();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  const auto third_result = co_await third;
  co_return head_blocked && first_woke && second_woke && third_woke &&
      first_result.has_value() && second_result.has_value() &&
      third_result.has_value() && !failed->load(std::memory_order_acquire) &&
      semaphore.available_permits() == 5;
}

Task<void> semaphore_hold_child(cio::sync::Semaphore semaphore,
                                std::uint32_t permits,
                                std::shared_ptr<std::atomic<bool>> acquired,
                                cio::sync::Notify release) {
  auto result = co_await semaphore.acquire_many(permits);
  if (!result.has_value()) {
    co_return;
  }
  auto permit = std::move(result).value();
  acquired->store(true, std::memory_order_release);
  co_await release.notified();
}

Task<bool> semaphore_cancel_partial_root() {
  cio::sync::Semaphore semaphore{0};
  cio::sync::Notify release;
  const auto first_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto second_acquired = std::make_shared<std::atomic<bool>>(false);

  auto first = cio::task::spawn(
      semaphore_hold_child(semaphore, 3, first_acquired, release));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(
      semaphore_hold_child(semaphore, 1, second_acquired, release));
  co_await cio::task::yield_now();

  semaphore.add_permits(2);
  first.abort();
  const auto first_result = co_await first;
  for (int attempt = 0;
       attempt < 20 && !second_acquired->load(std::memory_order_acquire);
       ++attempt) {
    co_await cio::task::yield_now();
  }

  const bool transferred = !first_result.has_value() &&
                           first_result.error().is_cancelled() &&
                           !first_acquired->load(std::memory_order_acquire) &&
                           second_acquired->load(std::memory_order_acquire) &&
                           semaphore.available_permits() == 1;
  release.notify_waiters();
  const auto second_result = co_await second;
  co_return transferred &&second_result.has_value() &&
      semaphore.available_permits() == 2;
}

Task<bool> semaphore_cancel_completed_root() {
  cio::sync::Semaphore semaphore{0};
  cio::sync::Notify release;
  const auto first_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto second_acquired = std::make_shared<std::atomic<bool>>(false);

  auto first = cio::task::spawn(
      semaphore_hold_child(semaphore, 1, first_acquired, release));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(
      semaphore_hold_child(semaphore, 1, second_acquired, release));
  co_await cio::task::yield_now();

  semaphore.add_permits(1);
  first.abort();
  const auto first_result = co_await first;
  for (int attempt = 0;
       attempt < 20 && !second_acquired->load(std::memory_order_acquire);
       ++attempt) {
    co_await cio::task::yield_now();
  }

  const bool transferred = !first_result.has_value() &&
                           first_result.error().is_cancelled() &&
                           !first_acquired->load(std::memory_order_acquire) &&
                           second_acquired->load(std::memory_order_acquire) &&
                           semaphore.available_permits() == 0;
  release.notify_waiters();
  const auto second_result = co_await second;
  co_return transferred &&second_result.has_value() &&
      semaphore.available_permits() == 1;
}

Task<bool> semaphore_expect_closed(cio::sync::Semaphore semaphore,
                                   std::uint32_t permits) {
  auto result = co_await semaphore.acquire_many(permits);
  co_return !result.has_value() && result.error().is_closed();
}

Task<bool> semaphore_close_root() {
  cio::sync::Semaphore semaphore{1};
  auto waiter = cio::task::spawn(semaphore_expect_closed(semaphore, 2));
  co_await cio::task::yield_now();
  const bool partially_assigned = semaphore.available_permits() == 0;

  semaphore.close();
  semaphore.close();
  const auto waiter_result = co_await waiter;
  const auto after_close = semaphore.try_acquire_many(0);
  semaphore.add_permits(2);
  co_return partially_assigned &&waiter_result.has_value() &&
      waiter_result.value() && semaphore.is_closed() &&
      semaphore.available_permits() == 3 && !after_close.has_value() &&
      after_close.error() == cio::sync::TryAcquireError::closed;
}

Task<bool> semaphore_cross_thread_root(cio::sync::Semaphore semaphore) {
  for (int iteration = 0; iteration < 100; ++iteration) {
    std::jthread releaser{[semaphore] { semaphore.add_permits(1); }};
    auto result = co_await semaphore.acquire_owned();
    if (!result.has_value()) {
      co_return false;
    }
    {
      auto permit = std::move(result).value();
      if (permit.num_permits() != 1) {
        co_return false;
      }
    }
    if (semaphore.available_permits() != 1 ||
        semaphore.forget_permits(1) != 1) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> semaphore_cancel_release_race_root() {
  for (int iteration = 0; iteration < 100; ++iteration) {
    cio::sync::Semaphore semaphore{0};
    auto child = cio::task::spawn(cio::task::owned(
        [](cio::sync::Semaphore child_semaphore) -> Task<void> {
          auto result = co_await child_semaphore.acquire();
          if (result.has_value()) {
            auto permit = std::move(result).value();
          }
        },
        semaphore));
    co_await cio::task::yield_now();

    std::jthread releaser{[semaphore] { semaphore.add_permits(1); }};
    child.abort();
    const auto result = co_await child;
    if (result.has_value() == false && !result.error().is_cancelled()) {
      co_return false;
    }
    releaser.join();
    if (semaphore.available_permits() != 1) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> semaphore_multi_contention_root() {
  cio::sync::Semaphore semaphore{4};
  const auto active = std::make_shared<std::atomic<int>>(0);
  const auto maximum = std::make_shared<std::atomic<int>>(0);
  const auto completed = std::make_shared<std::atomic<int>>(0);
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(100);

  for (int index = 0; index < 100; ++index) {
    handles.push_back(cio::task::spawn(cio::task::owned(
        [](cio::sync::Semaphore child_semaphore,
           std::shared_ptr<std::atomic<int>> child_active,
           std::shared_ptr<std::atomic<int>> child_maximum,
           std::shared_ptr<std::atomic<int>> child_completed) -> Task<void> {
          auto result = co_await child_semaphore.acquire_owned();
          if (!result.has_value()) {
            co_return;
          }
          auto permit = std::move(result).value();
          const auto now =
              child_active->fetch_add(1, std::memory_order_acq_rel) + 1;
          auto previous = child_maximum->load(std::memory_order_acquire);
          while (previous < now && !child_maximum->compare_exchange_weak(
                                       previous, now, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          }
          while (child_maximum->load(std::memory_order_acquire) < 4) {
            co_await cio::task::yield_now();
          }
          child_active->fetch_sub(1, std::memory_order_acq_rel);
          child_completed->fetch_add(1, std::memory_order_release);
        },
        semaphore, active, maximum, completed)));
  }

  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return active->load(std::memory_order_acquire) == 0 &&
      maximum->load(std::memory_order_acquire) == 4 &&
      completed->load(std::memory_order_acquire) == 100 &&
      semaphore.available_permits() == 4;
}

Task<bool> mutex_basic_root() {
  cio::sync::Mutex<int> mutex{1};
  {
    auto result = mutex.try_lock_owned();
    if (!result.has_value()) {
      co_return false;
    }
    auto guard = std::move(result).value();
    const auto blocked = mutex.try_lock();
    if (blocked.has_value() || blocked.error().message() != "操作将阻塞") {
      co_return false;
    }
    co_await cio::task::yield_now();
    *guard = 2;
    if (guard.mutex().try_lock().has_value()) {
      co_return false;
    }
  }

  {
    auto guard = co_await mutex.lock_owned();
    if (*guard != 2) {
      co_return false;
    }
    *guard = 3;
  }
  const auto final = mutex.try_lock();
  co_return final.has_value() && *final.value() == 3;
}

Task<void> mutex_order_child(cio::sync::Mutex<int> mutex, int label,
                             std::shared_ptr<std::vector<int>> order) {
  auto guard = co_await mutex.lock();
  order->push_back(label);
  ++*guard;
  co_await cio::task::yield_now();
}

Task<bool> mutex_fifo_root() {
  cio::sync::Mutex<int> mutex{0};
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());
  const auto order = std::make_shared<std::vector<int>>();

  auto first = cio::task::spawn(mutex_order_child(mutex, 1, order));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(mutex_order_child(mutex, 2, order));
  co_await cio::task::yield_now();
  auto third = cio::task::spawn(mutex_order_child(mutex, 3, order));
  co_await cio::task::yield_now();
  const bool all_waited = order->empty();

  owner.reset();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  const auto third_result = co_await third;
  auto final = co_await mutex.lock();
  co_return all_waited &&first_result.has_value() &&
      second_result.has_value() && third_result.has_value() &&
      *order == std::vector<int>({1, 2, 3}) && *final == 3;
}

Task<bool> mutex_cancel_waiter_root() {
  cio::sync::Mutex<int> mutex{0};
  const auto order = std::make_shared<std::vector<int>>();
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());

  auto first = cio::task::spawn(mutex_order_child(mutex, 1, order));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(mutex_order_child(mutex, 2, order));
  co_await cio::task::yield_now();
  first.abort();
  owner.reset();

  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return !first_result.has_value() && first_result.error().is_cancelled() &&
      second_result.has_value() && *order == std::vector<int>({2}) &&
      mutex.try_lock().has_value();
}

Task<bool> mutex_cancel_acquired_root() {
  cio::sync::Mutex<int> mutex{0};
  const auto order = std::make_shared<std::vector<int>>();
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());

  auto first = cio::task::spawn(mutex_order_child(mutex, 1, order));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(mutex_order_child(mutex, 2, order));
  co_await cio::task::yield_now();
  owner.reset();
  first.abort();

  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return !first_result.has_value() && first_result.error().is_cancelled() &&
      second_result.has_value() && *order == std::vector<int>({2}) &&
      mutex.try_lock().has_value();
}

Task<void> mutex_throwing_child(cio::sync::Mutex<int> mutex) {
  auto guard = co_await mutex.lock_owned();
  *guard = 7;
  throw std::runtime_error{"mutex holder panic"};
}

Task<bool> mutex_no_poison_root() {
  cio::sync::Mutex<int> mutex{0};
  auto child = cio::task::spawn(mutex_throwing_child(mutex));
  const auto child_result = co_await child;
  auto after = mutex.try_lock();
  co_return !child_result.has_value() && child_result.error().is_panic() &&
      after.has_value() && *after.value() == 7;
}

Task<bool> mutex_map_root() {
  cio::sync::Mutex<MutexRecord> mutex{MutexRecord{1, MutexRecord::Nested{2}}};
  {
    auto guard = co_await mutex.lock_owned();
    auto nested = cio::sync::MutexGuard<MutexRecord>::map(
        std::move(guard), [](MutexRecord &value) -> MutexRecord::Nested & {
          return value.nested;
        });
    auto mapped =
        cio::sync::MappedMutexGuard<MutexRecord, MutexRecord::Nested>::map(
            std::move(nested),
            [](MutexRecord::Nested &value) -> int & { return value.value; });
    *mapped = 9;
    if (mutex.try_lock().has_value()) {
      co_return false;
    }
  }

  {
    auto guard = co_await mutex.lock();
    auto failed = cio::sync::MutexGuard<MutexRecord>::try_map(
        std::move(guard), [](MutexRecord &) { return false; },
        [](MutexRecord &value) -> int & { return value.first; });
    if (failed.has_value()) {
      co_return false;
    }
    auto original = std::move(failed).error();
    (*original).first = 4;
  }

  {
    auto guard = co_await mutex.lock();
    auto succeeded = cio::sync::MutexGuard<MutexRecord>::try_map(
        std::move(guard), [](MutexRecord &value) { return value.first == 4; },
        [](MutexRecord &value) -> int & { return value.first; });
    if (!succeeded.has_value()) {
      co_return false;
    }
    auto mapped = std::move(succeeded).value();
    auto failed_nested = cio::sync::MappedMutexGuard<MutexRecord, int>::try_map(
        std::move(mapped), [](int &) { return false; },
        [](int &value) -> int & { return value; });
    if (failed_nested.has_value()) {
      co_return false;
    }
    auto original_mapped = std::move(failed_nested).error();
    *original_mapped = 5;
  }

  auto final = co_await mutex.lock();
  co_return (*final).first == 5 && (*final).nested.value == 9;
}

Task<bool> mutex_blocking_rejected_root() {
  cio::sync::Mutex<int> mutex{0};
  try {
    auto guard = mutex.blocking_lock();
    (void)guard;
  } catch (const std::logic_error &) {
    co_return true;
  }
  co_return false;
}

Task<bool> mutex_blocking_bridge_root() {
  cio::sync::Mutex<int> mutex{1};
  std::optional<cio::sync::MutexGuard<int>> owner;
  owner.emplace(co_await mutex.lock());
  const auto started = std::make_shared<std::atomic<bool>>(false);
  auto blocking = cio::task::spawn_blocking(
      [](cio::sync::Mutex<int> child_mutex,
         std::shared_ptr<std::atomic<bool>> child_started) {
        child_started->store(true, std::memory_order_release);
        auto guard = child_mutex.blocking_lock_owned();
        *guard = 9;
      },
      mutex, started);
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool stayed_locked = **owner == 1;
  owner.reset();
  const auto blocking_result = co_await blocking;
  auto final = co_await mutex.lock();
  co_return stayed_locked &&blocking_result.has_value() && *final == 9;
}

Task<bool> mutex_release_abort_race_root() {
  for (int iteration = 0; iteration < 100; ++iteration) {
    cio::sync::Mutex<int> mutex{0};
    std::optional<cio::sync::MutexGuard<int>> owner;
    auto owner_result = mutex.try_lock();
    owner.emplace(std::move(owner_result).value());
    auto child = cio::task::spawn(cio::task::owned(
        [](cio::sync::Mutex<int> child_mutex) -> Task<void> {
          auto guard = co_await child_mutex.lock_owned();
          ++*guard;
        },
        mutex));
    co_await cio::task::yield_now();

    std::jthread releaser{
        [held = std::move(owner)]() mutable { held.reset(); }};
    child.abort();
    const auto child_result = co_await child;
    releaser.join();
    if (!child_result.has_value() && !child_result.error().is_cancelled()) {
      co_return false;
    }
    auto final = mutex.try_lock();
    if (!final.has_value() || (*final.value() != 0 && *final.value() != 1)) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> mutex_multi_contention_root() {
  cio::sync::Mutex<int> mutex{0};
  const auto active = std::make_shared<std::atomic<int>>(0);
  const auto maximum = std::make_shared<std::atomic<int>>(0);
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(100);
  for (int index = 0; index < 100; ++index) {
    handles.push_back(cio::task::spawn(cio::task::owned(
        [](cio::sync::Mutex<int> child_mutex,
           std::shared_ptr<std::atomic<int>> child_active,
           std::shared_ptr<std::atomic<int>> child_maximum) -> Task<void> {
          for (int iteration = 0; iteration < 10; ++iteration) {
            auto guard = co_await child_mutex.lock_owned();
            const auto now =
                child_active->fetch_add(1, std::memory_order_acq_rel) + 1;
            auto previous = child_maximum->load(std::memory_order_acquire);
            while (previous < now &&
                   !child_maximum->compare_exchange_weak(
                       previous, now, std::memory_order_acq_rel,
                       std::memory_order_acquire)) {
            }
            const auto value = *guard;
            co_await cio::task::yield_now();
            *guard = value + 1;
            child_active->fetch_sub(1, std::memory_order_acq_rel);
          }
        },
        mutex, active, maximum)));
  }

  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  auto final = co_await mutex.lock();
  co_return *final == 1'000 && active->load(std::memory_order_acquire) == 0 &&
      maximum->load(std::memory_order_acquire) == 1;
}

Task<bool> multi_thread_pause_rejected_root() {
  try {
    cio::time::pause();
  } catch (const std::logic_error &) {
    co_return true;
  }
  co_return false;
}

Task<bool>
multi_thread_abort_root(std::shared_ptr<std::atomic<bool>> started,
                        std::shared_ptr<std::atomic<bool>> destroyed) {
  auto child = cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<std::atomic<bool>> child_started,
         std::shared_ptr<std::atomic<bool>> child_destroyed) -> Task<void> {
        co_await cancellable_task(std::move(child_started),
                                  std::move(child_destroyed));
      },
      started, destroyed));
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  child.abort();
  const auto result = co_await child;
  co_return !result.has_value() && result.error().is_cancelled() &&
      destroyed->load(std::memory_order_acquire);
}

Task<void> wait_until_started(std::shared_ptr<std::atomic<bool>> started) {
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
}

Task<bool> await_multi_wake_handle(cio::task::JoinHandle<void> handle,
                                   std::shared_ptr<std::atomic<bool>> resumed) {
  const auto result = co_await handle;
  co_return result.has_value() && resumed->load(std::memory_order_acquire);
}

void test_result_contract() {
  auto success = cio::Result<int, std::string>::success(42);
  check(success.has_value(), "Result 成功状态丢失");
  check(success.value() == 42, "Result 成功值错误");

  auto failure = cio::Result<int, std::string>::failure(std::string{"failure"});
  check(!failure.has_value(), "Result 错误状态丢失");
  check(failure.error() == "failure", "Result 错误值错误");
}

void test_task_and_block_on() {
  Runtime runtime;
  check(runtime.block_on(nested_value()) == 42, "嵌套 Task 结果错误");
}

void test_symmetric_transfer_has_bounded_stack() {
  Runtime runtime;
  constexpr std::size_t depth = 20'000;
  check(runtime.block_on(deep_task_chain(depth)) == depth,
        "深层 Task symmetric transfer 结果错误");
}

void test_spawn_join_and_deferred_poll() {
  Runtime runtime;
  const auto polls = std::make_shared<std::atomic<int>>(0);
  check(runtime.block_on(verify_spawn_is_deferred(polls)),
        "spawn 同步 poll 或 JoinHandle 未返回结果");
}

void test_nested_task_composition_order() {
  Runtime runtime;
  check(runtime.block_on(verify_nested_task_stays_in_current_poll()),
        "普通子 Task 被错误地作为独立 spawned task 排队");
}

void test_join_handle_drop_detaches() {
  Runtime runtime;
  const auto completed = std::make_shared<std::atomic<bool>>(false);
  runtime.block_on(detach_parent(completed));
  check(completed->load(std::memory_order_acquire),
        "JoinHandle 析构错误地取消了背景 task");
}

void test_abort_is_idempotent_and_destroys_before_join() {
  Runtime runtime;
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  check(runtime.block_on(abort_after_first_poll(started, destroyed)),
        "abort 语义、幂等性或析构顺序错误");
}

void test_abort_before_first_poll() {
  Runtime runtime;
  const auto started = std::make_shared<std::atomic<bool>>(false);
  auto handle = runtime.spawn(never_polled_after_abort(started));
  handle.abort();
  check(runtime.block_on(await_cancelled(std::move(handle))),
        "启动前 abort 未返回 cancelled");
  check(!started->load(std::memory_order_acquire),
        "启动前 abort 后仍 poll 了用户 task");
}

void test_spawned_exception_becomes_join_error() {
  Runtime runtime;
  check(runtime.block_on(verify_spawned_exception()),
        "spawn task 异常没有变成 JoinError::panic");
}

void test_root_exception_rethrows_at_block_on() {
  Runtime runtime;
  bool observed = false;
  try {
    (void)runtime.block_on(throwing_task());
  } catch (const std::runtime_error &error) {
    observed = std::string_view{error.what()} == "spawned boom";
  }
  check(observed, "block_on 没有在同步边界重抛根 task 异常");
}

void test_runtime_shutdown_cancels_and_destroys() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  cio::task::JoinHandle<void> handle;
  {
    Runtime runtime;
    handle = runtime.spawn(suspended_until_shutdown(started, destroyed));
    runtime.block_on(empty_root());
    check(started->load(std::memory_order_acquire),
          "关闭测试 task 没有进入暂停点");
    check(!handle.is_finished(), "暂停 task 被错误标记为完成");
  }

  check(destroyed->load(std::memory_order_acquire),
        "runtime 析构未销毁暂停 task 的局部对象");
  check(handle.is_finished(), "runtime 析构未发布 task 取消完成");
}

void test_waker_wake_before_wait() {
  Runtime runtime;
  const auto slot = std::make_shared<cio::detail::WakerSlot>();
  const auto resumed = std::make_shared<std::atomic<bool>>(false);
  slot->wake();
  runtime.block_on(wait_for_slot(slot, resumed));
  check(resumed->load(std::memory_order_acquire), "wake-before-wait 丢失通知");
}

void test_waker_wait_before_wake() {
  Runtime runtime;
  const auto slot = std::make_shared<cio::detail::WakerSlot>();
  const auto resumed = std::make_shared<std::atomic<bool>>(false);
  check(runtime.block_on(verify_wait_then_wake(slot, resumed)),
        "wait-before-wake 没有恢复等待 task");
}

void test_nested_block_on_is_rejected() {
  const auto runtime = std::make_shared<Runtime>();
  const auto rejected = std::make_shared<std::atomic<bool>>(false);
  runtime->block_on(nested_block_on_attempt(runtime, rejected));
  check(rejected->load(std::memory_order_acquire),
        "异步上下文内嵌套 block_on 没有被拒绝");
}

void test_cross_thread_abort() {
  Runtime runtime;
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto handle = runtime.spawn(cancellable_task(started, destroyed));
  const auto abort = handle.abort_handle();

  std::jthread canceller{[started, abort] {
    while (!started->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    for (int attempt = 0; attempt < 64; ++attempt) {
      abort.abort();
    }
  }};

  check(runtime.block_on(await_void_cancelled(std::move(handle))),
        "跨线程 abort 没有发布 cancelled");
  check(destroyed->load(std::memory_order_acquire),
        "跨线程 abort 的 join 完成早于局部对象析构");
}

void test_cross_thread_wake_race() {
  for (int iteration = 0; iteration < 200; ++iteration) {
    Runtime runtime;
    const auto slot = std::make_shared<cio::detail::WakerSlot>();
    const auto registering = std::make_shared<std::atomic<bool>>(false);
    const auto resumed = std::make_shared<std::atomic<bool>>(false);

    std::jthread waker{[slot, registering] {
      while (!registering->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      slot->wake();
    }};

    runtime.block_on(wait_for_external_wake(slot, registering, resumed));
    check(resumed->load(std::memory_order_acquire),
          "跨线程 register/wake 竞态丢失通知");
  }
}

void test_cancelled_join_waiter_ignores_stale_wake() {
  Runtime runtime;
  const auto child_completed = std::make_shared<std::atomic<bool>>(false);
  check(runtime.block_on(cancel_join_waiter_case(child_completed)),
        "等待方取消后的旧 Join wake 恢复了已销毁协程");
  check(child_completed->load(std::memory_order_acquire),
        "JoinHandle detach 后子 task 没有继续完成");
}

void test_empty_task_is_rejected() {
  Runtime runtime;
  bool rejected = false;
  try {
    (void)runtime.spawn(Task<void>{});
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  check(rejected, "空 Task 被 runtime 接受");
}

void test_stale_task_key_cannot_resume_reused_slot() {
  Runtime runtime;
  const auto stale_key = runtime.block_on(capture_current_task_key());

  const auto polls = std::make_shared<std::atomic<int>>(0);
  const auto resumed = std::make_shared<std::atomic<bool>>(false);
  auto target = runtime.spawn(stale_key_target(polls, resumed));
  runtime.block_on(empty_root());
  check(polls->load(std::memory_order_relaxed) == 1,
        "generation 测试目标没有进入暂停点");

  cio::detail::RuntimeAccess::state(runtime)->schedule(stale_key);
  runtime.block_on(empty_root());
  check(polls->load(std::memory_order_relaxed) == 1 &&
            !resumed->load(std::memory_order_acquire),
        "stale TaskKey 错误地恢复了复用 slot 中的新 task");

  target.abort();
  check(runtime.block_on(await_void_cancelled(std::move(target))),
        "generation 测试目标未正常取消");
}

void test_task_key_cannot_cross_runtime() {
  Runtime source_runtime;
  const auto foreign_key = source_runtime.block_on(capture_current_task_key());

  Runtime target_runtime;
  const auto polls = std::make_shared<std::atomic<int>>(0);
  const auto resumed = std::make_shared<std::atomic<bool>>(false);
  auto target = target_runtime.spawn(stale_key_target(polls, resumed));
  target_runtime.block_on(empty_root());

  cio::detail::RuntimeAccess::state(target_runtime)->schedule(foreign_key);
  target_runtime.block_on(empty_root());
  check(polls->load(std::memory_order_relaxed) == 1 &&
            !resumed->load(std::memory_order_acquire),
        "其他 runtime 的 TaskKey 错误地恢复了本 runtime task");

  target.abort();
  check(target_runtime.block_on(await_void_cancelled(std::move(target))),
        "跨 runtime key 测试目标未正常取消");
}

void test_sleep_requires_enabled_runtime() {
  auto runtime = cio::runtime::Builder::new_current_thread().build();
  bool rejected = false;
  try {
    runtime.block_on(attempt_sleep_without_driver());
  } catch (const std::logic_error &) {
    rejected = true;
  }
  check(rejected, "未启用 time 的 Builder runtime 接受了 Sleep");
}

void test_sleep_creation_outside_runtime_is_rejected() {
  bool rejected = false;
  try {
    (void)cio::time::sleep(1ms);
  } catch (const std::logic_error &) {
    rejected = true;
  }
  check(rejected, "runtime 外创建 Sleep 没有失败");
}

void test_paused_sleep_auto_advance_and_rounding() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(paused_sleep_rounding_case()),
        "暂停时钟没有按 1 ms 粒度自动推进");
}

void test_sleep_reset_invalidates_old_generation() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(reset_suspended_sleep_case()),
        "Sleep::reset 的旧 TimerKey 提前完成了新 deadline");
}

void test_cancelled_sleep_releases_timer() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(cancelling_sleep_releases_timer_case()),
        "取消 Sleep 后 timer slot 未释放");
}

void test_elapsed_sleep_can_be_reset() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(reset_elapsed_sleep_case()),
        "已完成 Sleep 不能按新 deadline 再次等待");
}

void test_manual_advance_yields_without_joining_timers() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(manual_advance_case()),
        "time::advance 的跳跃或 yield 语义错误");
}

void test_timer_overflow_reenters_wheel() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(overflow_wheel_case()),
        "超出六层时间轮范围的 timer 没有完成");
}

void test_pause_resume_state_errors() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(pause_resume_error_case()),
        "pause/resume 没有拒绝非法时钟状态");
}

void test_cross_thread_sleep_reset_wakes_driver() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().build();
  check(runtime.block_on(cross_thread_reset_case()),
        "跨线程 reset 没有唤醒或重排 timer driver");
}

void test_timeout_immediate_future_beats_zero_deadline() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_immediate_future_wins_case()),
        "立即完成 Task 没有优先于零时长 timeout");
}

void test_timeout_value_wins_at_same_deadline() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_same_deadline_prefers_value_case()),
        "相同 deadline 时没有先 poll 被包装 Task");
}

void test_timeout_elapsed_destroys_loser_before_result() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_elapsed_destroys_value_case()),
        "timeout 返回前未销毁失败分支或释放 timer");
}

void test_timeout_propagates_task_exception() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_exception_propagates_case()),
        "Timeout 吞掉了被包装 Task 的异常");
}

void test_timeout_into_inner_before_poll() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_into_inner_case()),
        "Timeout::into_inner 没有返回未启动 Task");
}

void test_aborting_timeout_destroys_children_before_join() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(timeout_parent_abort_case()),
        "取消 Timeout 后子协程析构或 timer 清理晚于父 join");
}

void test_interval_ticks_on_original_schedule() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(interval_basic_case()),
        "Interval 首 tick、固定周期或惰性重注册语义错误");
}

void test_interval_missed_tick_behaviors() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(interval_missed_tick_cases()),
        "Interval Burst/Delay/Skip missed-tick 语义错误");
}

void test_interval_tick_is_cancel_safe() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.enable_time().start_paused().build();
  check(runtime.block_on(interval_cancel_safe_case()),
        "取消 tick 消费了 Interval 的计划时间点");
}

void test_interval_rejects_zero_period() {
  bool rejected = false;
  try {
    (void)cio::time::interval(cio::time::Duration::zero());
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  check(rejected, "Interval 接受了零 period");
}

void test_multi_thread_rejects_unreviewed_task() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(2).build();
  bool rejected = false;
  try {
    (void)runtime.block_on(immediate_value(42));
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  check(rejected, "multi-thread runtime 接受了未审计 Task");
}

void test_multi_thread_owned_fanout_and_steal() {
  const auto probe = std::make_shared<MultiThreadProbe>();
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  check(cio::detail::RuntimeAccess::state(runtime)->worker_count() == 4,
        "Builder::worker_threads 没有配置准确 worker 数");
  auto root = cio::task::owned(
      [](std::shared_ptr<MultiThreadProbe> root_probe) -> Task<bool> {
        co_return co_await multi_thread_fanout_root(std::move(root_probe));
      },
      probe);
  check(runtime.block_on(std::move(root)),
        "multi-thread fanout 未完成或工作窃取未使用多个 worker");
}

void test_multi_thread_fairness_bound() {
  const auto probe = std::make_shared<MultiThreadProbe>();
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(2).build();
  check(runtime.block_on(cio::task::owned(
            [](std::shared_ptr<MultiThreadProbe> root_probe) -> Task<bool> {
              co_return co_await multi_thread_fairness_root(
                  std::move(root_probe));
            },
            probe)),
        "runnext/local 链饿死了其他 ready task");
}

void test_multi_thread_cooperative_budget() {
  const auto probe = std::make_shared<MultiThreadProbe>();
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(1).build();
  check(runtime.block_on(cio::task::owned(
            [](std::shared_ptr<MultiThreadProbe> root_probe) -> Task<bool> {
              co_return co_await multi_thread_cooperative_budget_root(
                  std::move(root_probe));
            },
            probe)),
        "consume_budget 没有在硬上限内让出 worker");
}

void test_multi_thread_real_timer() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(2).enable_time().build();
  check(runtime.block_on(cio::task::assume_portable(multi_thread_timer_root())),
        "multi-thread worker 没有驱动实时 timer");
}

void test_multi_thread_rejects_paused_clock() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  bool rejected = false;
  try {
    (void)builder.worker_threads(2).enable_time().start_paused().build();
  } catch (const std::logic_error &) {
    rejected = true;
  }
  check(rejected, "multi-thread runtime 接受了 start_paused");

  auto runtime_builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = runtime_builder.worker_threads(2).enable_time().build();
  check(runtime.block_on(
            cio::task::assume_portable(multi_thread_pause_rejected_root())),
        "multi-thread runtime 接受了 time::pause");
}

void test_multi_thread_abort_destroys_before_join() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  check(
      runtime.block_on(cio::task::owned(
          [](std::shared_ptr<std::atomic<bool>> root_started,
             std::shared_ptr<std::atomic<bool>> root_destroyed) -> Task<bool> {
            co_return co_await multi_thread_abort_root(
                std::move(root_started), std::move(root_destroyed));
          },
          started, destroyed)),
      "multi-thread abort 的析构晚于 join 结果");
}

void test_multi_thread_shutdown_cancels_suspended_task() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  cio::task::JoinHandle<void> handle;
  {
    auto builder = cio::runtime::Builder::new_multi_thread();
    auto runtime = builder.worker_threads(2).build();
    handle = runtime.spawn(cio::task::assume_portable(
        suspended_until_shutdown(started, destroyed)));
    runtime.block_on(cio::task::owned(
        [](std::shared_ptr<std::atomic<bool>> root_started) -> Task<void> {
          co_await wait_until_started(std::move(root_started));
        },
        started));
    check(handle.is_finished() == false, "multi shutdown 测试 task 提前完成");
  }
  check(destroyed->load(std::memory_order_acquire),
        "multi-thread runtime 析构未销毁暂停 task");
  check(handle.is_finished(), "multi-thread runtime 析构未发布 cancelled");
}

void test_multi_thread_cross_thread_wake() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  for (int iteration = 0; iteration < 100; ++iteration) {
    const auto slot = std::make_shared<cio::detail::WakerSlot>();
    const auto registering = std::make_shared<std::atomic<bool>>(false);
    const auto resumed = std::make_shared<std::atomic<bool>>(false);
    auto target = runtime.spawn(cio::task::assume_portable(
        wait_for_external_wake(slot, registering, resumed)));

    std::jthread waker{[slot, registering] {
      while (!registering->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      slot->wake();
    }};

    check(runtime.block_on(cio::task::assume_portable(
              await_multi_wake_handle(std::move(target), resumed))),
          "multi-thread 外部 wake 丢失或未恢复 task");
  }
}

void test_blocking_builder_and_basic_execution() {
  bool zero_rejected = false;
  try {
    auto invalid = cio::runtime::Builder::new_current_thread();
    (void)invalid.max_blocking_threads(0);
  } catch (const std::invalid_argument &) {
    zero_rejected = true;
  }
  check(zero_rejected, "max_blocking_threads 接受了零");

  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.max_blocking_threads(3).build();
  const auto state = cio::detail::RuntimeAccess::state(runtime);
  check(state->max_blocking_threads() == 3,
        "Builder 未保留 blocking worker 上限");
  check(runtime.block_on(blocking_basic_root()),
        "spawn_blocking 没有在专用线程执行");
}

void test_multi_thread_spawn_blocking() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(2).max_blocking_threads(2).build();
  check(runtime.block_on(cio::task::assume_portable(blocking_basic_root())),
        "multi-thread runtime 的 spawn_blocking 失败");
}

void test_blocking_queued_abort() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  const auto calls = std::make_shared<std::atomic<int>>(0);
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.max_blocking_threads(1).build();
  check(runtime.block_on(blocking_queued_abort_root(started, release, calls)),
        "排队 blocking job 的 abort 语义错误");
}

void test_blocking_running_abort_is_noop() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.max_blocking_threads(1).build();
  check(runtime.block_on(blocking_running_abort_root(started, release)),
        "已开始 blocking job 被 abort 中断");
}

void test_blocking_thread_cap() {
  const auto running = std::make_shared<std::atomic<int>>(0);
  const auto maximum = std::make_shared<std::atomic<int>>(0);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.max_blocking_threads(2).build();
  check(runtime.block_on(blocking_thread_cap_root(running, maximum, release)),
        "blocking pool 未遵守 max_blocking_threads");
}

void test_blocking_exception_and_nested_spawn() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime = builder.max_blocking_threads(1).build();
  check(runtime.block_on(blocking_exception_root()),
        "blocking 异常未转成 JoinError::panic");
  check(runtime.block_on(nested_blocking_root()),
        "blocking worker 上下文不能嵌套 spawn_blocking");
}

void test_blocking_inhibits_paused_auto_advance() {
  auto builder = cio::runtime::Builder::new_current_thread();
  auto runtime =
      builder.enable_time().start_paused().max_blocking_threads(1).build();
  check(runtime.block_on(blocking_inhibits_paused_time_root()),
        "运行中的 blocking job 未抑制冻结时钟自动推进");
}

void test_blocking_shutdown_waits_and_cancels_queue() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  const auto queued_calls = std::make_shared<std::atomic<int>>(0);
  cio::task::JoinHandle<int> running;
  cio::task::JoinHandle<int> queued;
  std::jthread releaser{[started, release] {
    while (!started->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    std::this_thread::sleep_for(10ms);
    release->store(true, std::memory_order_release);
  }};

  {
    auto builder = cio::runtime::Builder::new_current_thread();
    auto runtime = builder.max_blocking_threads(1).build();
    running = runtime.spawn_blocking(
        [](std::shared_ptr<std::atomic<bool>> job_started,
           std::shared_ptr<std::atomic<bool>> job_release) {
          job_started->store(true, std::memory_order_release);
          while (!job_release->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          return 7;
        },
        started, release);
    while (!started->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    queued = runtime.spawn_blocking(
        [](std::shared_ptr<std::atomic<int>> calls) {
          calls->fetch_add(1, std::memory_order_relaxed);
          return 9;
        },
        queued_calls);
  }

  check(running.is_finished(), "runtime 析构未等待运行中 blocking job");
  check(queued.is_finished(), "runtime 析构未取消排队 blocking job");
  auto running_result = cio::detail::JoinHandleAccess::take(running);
  auto queued_result = cio::detail::JoinHandleAccess::take(queued);
  check(running_result.has_value() && running_result.value() == 7,
        "运行中 blocking job 未在 shutdown 前完成");
  check(!queued_result.has_value() && queued_result.error().is_cancelled() &&
            queued_calls->load(std::memory_order_acquire) == 0,
        "shutdown 执行了排队的非 mandatory blocking job");
}

void test_notify_permit_and_ordering() {
  Runtime runtime;
  check(runtime.block_on(notify_permit_coalescing_root()),
        "Notify permit 未折叠为一个");
  check(runtime.block_on(notify_fifo_lifo_root()), "Notify FIFO/LIFO 选择错误");
}

void test_notify_waiters_snapshot_and_fanout() {
  Runtime runtime;
  check(runtime.block_on(notify_waiters_snapshot_root()),
        "notify_waiters 创建快照或 permit 保留语义错误");
  check(runtime.block_on(notify_waiters_fanout_root()),
        "notify_waiters 未唤醒全部已登记 waiter");
}

void test_notify_cancellation() {
  Runtime runtime;
  check(runtime.block_on(notify_cancel_transfers_root()),
        "取消已获 notify_one 的 waiter 未转交通知");
  check(runtime.block_on(notify_cancel_unnotified_root()),
        "取消未通知 waiter 意外产生 permit");
}

void test_notify_cross_thread_current_and_multi() {
  Runtime current;
  check(current.block_on(notify_cross_thread_root(cio::sync::Notify{})),
        "current-thread Notify 跨线程 wake 失败");
  check(current.block_on(notify_cancel_race_root()),
        "current-thread Notify notify/abort 竞态错误");

  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(2).build();
  check(multi.block_on(cio::task::owned(
            [](cio::sync::Notify notify) -> Task<bool> {
              co_return co_await notify_cross_thread_root(std::move(notify));
            },
            cio::sync::Notify{})),
        "multi-thread Notify 跨线程 wake 失败");
  check(multi.block_on(cio::task::assume_portable(notify_cancel_race_root())),
        "multi-thread Notify notify/abort 竞态错误");
}

void test_notify_retired_waiter_lifetime() {
  Runtime current;
  check(current.block_on(notify_retired_waiter_lifetime_root()),
        "current-thread Notify retired waiter 生命周期或 FIFO/LIFO 转交错误");

  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(2).build();
  check(multi.block_on(
            cio::task::assume_portable(notify_retired_waiter_lifetime_root())),
        "multi-thread Notify retired waiter 生命周期或 FIFO/LIFO 转交错误");
}

Task<bool> semaphore_immediate_root() {
  cio::sync::Semaphore semaphore{2};
  {
    auto first_result = co_await semaphore.acquire();
    auto second_result = co_await semaphore.acquire_many_owned(1);
    if (!first_result.has_value() || !second_result.has_value()) {
      co_return false;
    }
    auto first = std::move(first_result).value();
    auto second = std::move(second_result).value();
    if (first.num_permits() != 1 || second.num_permits() != 1 ||
        semaphore.available_permits() != 0) {
      co_return false;
    }
  }
  co_return semaphore.available_permits() == 2;
}

void test_semaphore_immediate_try_and_guards() {
  Runtime runtime;
  check(runtime.block_on(semaphore_immediate_root()),
        "Semaphore 立即异步获取或 guard 析构错误");

  cio::sync::Semaphore semaphore{5};
  auto zero_result = semaphore.try_acquire_many(0);
  check(zero_result.has_value() && zero_result.value().num_permits() == 0 &&
            semaphore.available_permits() == 5,
        "Semaphore 零许可获取错误");

  auto many_result = semaphore.try_acquire_many_owned(3);
  check(many_result.has_value(), "try_acquire_many 未立即成功");
  auto permit = std::move(many_result).value();
  auto split = permit.split(1);
  check(split.has_value() && permit.num_permits() == 2 &&
            split->num_permits() == 1,
        "SemaphorePermit split 错误");
  permit.merge(std::move(*split));
  check(permit.num_permits() == 3 &&
            permit.semaphore().available_permits() == 2,
        "SemaphorePermit merge 或 semaphore() 错误");

  cio::sync::Semaphore foreign{1};
  auto foreign_result = foreign.try_acquire();
  auto foreign_permit = std::move(foreign_result).value();
  bool rejected_foreign = false;
  try {
    permit.merge(std::move(foreign_permit));
  } catch (const std::invalid_argument &) {
    rejected_foreign = true;
  }
  check(rejected_foreign && permit.num_permits() == 3 &&
            foreign.available_permits() == 1,
        "不同 Semaphore 的 permit 合并未拒绝或未归还");

  permit.forget();
  check(semaphore.available_permits() == 2,
        "SemaphorePermit forget 意外归还许可");
  semaphore.add_permits(3);
  check(semaphore.available_permits() == 5 &&
            semaphore.forget_permits(9) == 5 &&
            semaphore.available_permits() == 0,
        "add_permits/forget_permits 计数错误");

  cio::sync::Semaphore moved{5};
  {
    auto first_result = moved.try_acquire();
    auto second_result = moved.try_acquire_many(2);
    auto first = std::move(first_result).value();
    auto second = std::move(second_result).value();
    first = std::move(second);
    check(first.num_permits() == 2 && moved.available_permits() == 3,
          "permit 移动赋值未归还原许可");
  }
  check(moved.available_permits() == 5, "移动后的 permit 析构归还错误");

  cio::sync::Semaphore empty{0};
  const auto no_permits = empty.try_acquire();
  check(!no_permits.has_value() &&
            no_permits.error() == cio::sync::TryAcquireError::no_permits,
        "try_acquire 未返回 no_permits");

  bool constructor_overflow = false;
  try {
    const cio::sync::Semaphore invalid{cio::sync::Semaphore::MAX_PERMITS + 1};
    (void)invalid;
  } catch (const std::invalid_argument &) {
    constructor_overflow = true;
  }
  cio::sync::Semaphore maximum{cio::sync::Semaphore::MAX_PERMITS};
  bool add_overflow = false;
  try {
    maximum.add_permits(1);
  } catch (const std::overflow_error &) {
    add_overflow = true;
  }
  check(constructor_overflow && add_overflow &&
            maximum.available_permits() == cio::sync::Semaphore::MAX_PERMITS,
        "Semaphore MAX_PERMITS 边界错误");
}

void test_semaphore_fairness_and_cancellation() {
  Runtime runtime;
  check(runtime.block_on(semaphore_fifo_head_blocking_root()),
        "Semaphore FIFO 或 acquire_many 头阻塞错误");
  check(runtime.block_on(semaphore_cancel_partial_root()),
        "Semaphore 部分许可取消未转交");
  check(runtime.block_on(semaphore_cancel_completed_root()),
        "Semaphore 已满足未恢复的获取取消未转交");
}

void test_semaphore_close() {
  Runtime runtime;
  check(runtime.block_on(semaphore_close_root()),
        "Semaphore close 唤醒、部分返还或关闭后行为错误");
}

void test_semaphore_cross_thread_current_and_multi() {
  Runtime current;
  check(current.block_on(semaphore_cross_thread_root(cio::sync::Semaphore{0})),
        "current-thread Semaphore 跨线程释放失败");
  check(current.block_on(semaphore_cancel_release_race_root()),
        "current-thread Semaphore release/abort 竞态丢失许可");

  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(4).build();
  check(multi.block_on(cio::task::owned(
            [](cio::sync::Semaphore semaphore) -> Task<bool> {
              co_return co_await semaphore_cross_thread_root(
                  std::move(semaphore));
            },
            cio::sync::Semaphore{0})),
        "multi-thread Semaphore 跨线程释放失败");
  check(multi.block_on(
            cio::task::assume_portable(semaphore_cancel_release_race_root())),
        "multi-thread Semaphore release/abort 竞态丢失许可");
  check(multi.block_on(
            cio::task::assume_portable(semaphore_multi_contention_root())),
        "multi-thread Semaphore 高竞争容量或许可守恒错误");
}

void test_mutex_immediate_unique_and_blocking() {
  Runtime runtime;
  check(runtime.block_on(mutex_basic_root()),
        "Mutex 立即 lock/try_lock、持锁暂停或 guard 访问错误");

  auto unique = cio::sync::Mutex<int>::const_new(1);
  bool guarded_copy_blocked = false;
  {
    auto unique_guard = unique.get_mut();
    *unique_guard = 4;
    auto copy_while_guarded = unique;
    guarded_copy_blocked = !copy_while_guarded.try_lock().has_value();
  }
  bool shared_get_rejected = false;
  bool shared_into_rejected = false;
  {
    auto copy = unique;
    try {
      (void)unique.get_mut();
    } catch (const std::logic_error &) {
      shared_get_rejected = true;
    }
    try {
      (void)std::move(unique).into_inner();
    } catch (const std::logic_error &) {
      shared_into_rejected = true;
    }
  }
  check(guarded_copy_blocked && shared_get_rejected && shared_into_rejected &&
            std::move(unique).into_inner() == 4,
        "Mutex get_mut/into_inner 唯一性校验错误");

  cio::sync::Mutex<int> mutex{1};
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto acquired = std::make_shared<std::atomic<bool>>(false);
  std::jthread waiter{[mutex, started, acquired] {
    started->store(true, std::memory_order_release);
    auto guard = mutex.blocking_lock_owned();
    *guard = 8;
    acquired->store(true, std::memory_order_release);
  }};
  while (!started->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(10ms);
  check(!acquired->load(std::memory_order_acquire),
        "blocking_lock 在原 guard 释放前取得锁");
  owner.reset();
  waiter.join();
  const auto final = mutex.try_lock();
  check(final.has_value() && *final.value() == 8,
        "blocking_lock 跨线程等待或写入错误");
}

void test_mutex_fifo_cancellation_and_no_poison() {
  Runtime runtime;
  check(runtime.block_on(mutex_fifo_root()), "Mutex 未严格按 FIFO 唤醒");
  check(runtime.block_on(mutex_cancel_waiter_root()),
        "Mutex 取消排队 waiter 未转交锁");
  check(runtime.block_on(mutex_cancel_acquired_root()),
        "Mutex 取消已满足未恢复 waiter 未转交锁");
  check(runtime.block_on(mutex_no_poison_root()),
        "Mutex 持锁 task 异常后发生 poison 或未解锁");
}

void test_mutex_mapping_and_blocking_bridge() {
  Runtime runtime;
  check(runtime.block_on(mutex_map_root()),
        "Mutex map/嵌套 map/try_map 所有权错误");
  check(runtime.block_on(mutex_blocking_rejected_root()),
        "异步上下文未拒绝 blocking_lock");
  check(runtime.block_on(mutex_blocking_bridge_root()),
        "spawn_blocking 与 Mutex blocking_lock 桥接错误");
}

void test_mutex_cross_thread_and_multi() {
  Runtime current;
  check(current.block_on(mutex_release_abort_race_root()),
        "current-thread Mutex release/abort 竞态错误");

  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(4).build();
  check(multi.block_on(
            cio::task::assume_portable(mutex_release_abort_race_root())),
        "multi-thread Mutex release/abort 竞态错误");
  check(
      multi.block_on(cio::task::assume_portable(mutex_multi_contention_root())),
      "multi-thread Mutex 高竞争互斥或计数错误");
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"Result contract", test_result_contract},
      {"Task and block_on", test_task_and_block_on},
      {"bounded symmetric transfer", test_symmetric_transfer_has_bounded_stack},
      {"spawn and join", test_spawn_join_and_deferred_poll},
      {"nested composition", test_nested_task_composition_order},
      {"JoinHandle detach", test_join_handle_drop_detaches},
      {"abort after poll", test_abort_is_idempotent_and_destroys_before_join},
      {"abort before poll", test_abort_before_first_poll},
      {"spawn exception", test_spawned_exception_becomes_join_error},
      {"root exception", test_root_exception_rethrows_at_block_on},
      {"runtime shutdown", test_runtime_shutdown_cancels_and_destroys},
      {"waker early", test_waker_wake_before_wait},
      {"waker registered", test_waker_wait_before_wake},
      {"nested block_on", test_nested_block_on_is_rejected},
      {"cross-thread abort", test_cross_thread_abort},
      {"cross-thread wake", test_cross_thread_wake_race},
      {"stale join wake", test_cancelled_join_waiter_ignores_stale_wake},
      {"empty task", test_empty_task_is_rejected},
      {"stale task key", test_stale_task_key_cannot_resume_reused_slot},
      {"foreign task key", test_task_key_cannot_cross_runtime},
      {"time driver required", test_sleep_requires_enabled_runtime},
      {"sleep outside runtime",
       test_sleep_creation_outside_runtime_is_rejected},
      {"paused sleep rounding", test_paused_sleep_auto_advance_and_rounding},
      {"sleep reset generation", test_sleep_reset_invalidates_old_generation},
      {"sleep cancellation", test_cancelled_sleep_releases_timer},
      {"sleep reset elapsed", test_elapsed_sleep_can_be_reset},
      {"manual time advance",
       test_manual_advance_yields_without_joining_timers},
      {"timer wheel overflow", test_timer_overflow_reenters_wheel},
      {"pause resume errors", test_pause_resume_state_errors},
      {"cross-thread sleep reset", test_cross_thread_sleep_reset_wakes_driver},
      {"timeout immediate", test_timeout_immediate_future_beats_zero_deadline},
      {"timeout same deadline", test_timeout_value_wins_at_same_deadline},
      {"timeout elapsed cleanup",
       test_timeout_elapsed_destroys_loser_before_result},
      {"timeout exception", test_timeout_propagates_task_exception},
      {"timeout into inner", test_timeout_into_inner_before_poll},
      {"timeout parent abort",
       test_aborting_timeout_destroys_children_before_join},
      {"interval basic", test_interval_ticks_on_original_schedule},
      {"interval missed ticks", test_interval_missed_tick_behaviors},
      {"interval cancellation", test_interval_tick_is_cancel_safe},
      {"interval zero period", test_interval_rejects_zero_period},
      {"multi rejects raw task", test_multi_thread_rejects_unreviewed_task},
      {"multi fanout steal", test_multi_thread_owned_fanout_and_steal},
      {"multi fairness", test_multi_thread_fairness_bound},
      {"multi cooperative budget", test_multi_thread_cooperative_budget},
      {"multi timer", test_multi_thread_real_timer},
      {"multi paused clock", test_multi_thread_rejects_paused_clock},
      {"multi abort cleanup", test_multi_thread_abort_destroys_before_join},
      {"multi shutdown", test_multi_thread_shutdown_cancels_suspended_task},
      {"multi cross-thread wake", test_multi_thread_cross_thread_wake},
      {"blocking builder basic", test_blocking_builder_and_basic_execution},
      {"multi spawn blocking", test_multi_thread_spawn_blocking},
      {"blocking queued abort", test_blocking_queued_abort},
      {"blocking running abort", test_blocking_running_abort_is_noop},
      {"blocking thread cap", test_blocking_thread_cap},
      {"blocking exception nested", test_blocking_exception_and_nested_spawn},
      {"blocking paused time", test_blocking_inhibits_paused_auto_advance},
      {"blocking shutdown", test_blocking_shutdown_waits_and_cancels_queue},
      {"notify permit ordering", test_notify_permit_and_ordering},
      {"notify waiters fanout", test_notify_waiters_snapshot_and_fanout},
      {"notify cancellation", test_notify_cancellation},
      {"notify cross-thread", test_notify_cross_thread_current_and_multi},
      {"notify retired waiter", test_notify_retired_waiter_lifetime},
      {"semaphore immediate guards", test_semaphore_immediate_try_and_guards},
      {"semaphore fairness cancellation",
       test_semaphore_fairness_and_cancellation},
      {"semaphore close", test_semaphore_close},
      {"semaphore cross-thread", test_semaphore_cross_thread_current_and_multi},
      {"mutex immediate unique blocking",
       test_mutex_immediate_unique_and_blocking},
      {"mutex fifo cancellation", test_mutex_fifo_cancellation_and_no_poison},
      {"mutex mapping blocking bridge", test_mutex_mapping_and_blocking_bridge},
      {"mutex cross-thread", test_mutex_cross_thread_and_multi},
  };

  std::size_t passed = 0;
  for (const auto &[name, test] : tests) {
    try {
      test();
      ++passed;
      std::cout << "[通过] " << name << '\n';
    } catch (const std::exception &error) {
      std::cerr << "[失败] " << name << ": " << error.what() << '\n';
      return 1;
    }
  }

  std::cout << "全部通过：" << passed << " 项\n";
  return 0;
}
