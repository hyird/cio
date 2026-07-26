#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <sstream>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/broadcast.hpp"
#include "cio/sync/mpsc.hpp"
#include "cio/sync/oneshot.hpp"
#include "cio/sync/set_once.hpp"
#include "cio/sync/watch.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;
using namespace std::chrono_literals;

std::vector<std::byte> io_bytes(std::string_view text) {
  std::vector<std::byte> bytes;
  bytes.reserve(text.size());
  for (const char value : text) {
    bytes.push_back(
        static_cast<std::byte>(static_cast<unsigned char>(value)));
  }
  return bytes;
}

cio::io::SharedBuffer io_buffer(std::string_view text) {
  const auto bytes = io_bytes(text);
  return cio::io::SharedBuffer::copy_from(
      std::span<const std::byte>{bytes});
}

Task<void> set_flag(const std::shared_ptr<std::atomic<bool>> &flag) {
  flag->store(true, std::memory_order_release);
  co_return;
}

Task<bool> spawn_deferred_case(const std::shared_ptr<std::atomic<bool>> &flag) {
  auto handle = cio::task::spawn(set_flag(flag));
  const bool deferred = !flag->load(std::memory_order_acquire);
  auto result = co_await handle;
  co_return deferred &&result.has_value() &&
      flag->load(std::memory_order_acquire);
}

Task<bool> await_cancelled(cio::task::JoinHandle<void> handle,
                           const std::shared_ptr<std::atomic<bool>> &polled) {
  auto result = co_await handle;
  co_return !result.has_value() && result.error().is_cancelled() &&
      !polled->load(std::memory_order_acquire);
}

Task<void>
detached_sender(const std::shared_ptr<std::atomic<bool>> &completed) {
  co_await cio::task::yield_now();
  completed->store(true, std::memory_order_release);
}

Task<bool> detach_case(const std::shared_ptr<std::atomic<bool>> &completed) {
  {
    auto handle = cio::task::spawn(detached_sender(completed));
    (void)handle;
  }
  while (!completed->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_return true;
}

Task<void> panic_task() {
  throw 42;
  co_return;
}

Task<bool> panic_case() {
  auto handle = cio::task::spawn(panic_task());
  auto result = co_await handle;
  co_return !result.has_value() && result.error().is_panic();
}

struct Probe final {
  std::shared_ptr<std::atomic<bool>> destroyed;

  ~Probe() { destroyed->store(true, std::memory_order_release); }
};

struct MutexProbeRecord final {
  struct Nested final {
    int value{0};
  };

  int first{0};
  Nested nested;
};

struct RwLockProbeRecord final {
  struct Nested final {
    int value{0};
  };

  Nested nested;
};

Task<void> cancellable(const std::shared_ptr<std::atomic<bool>> &started,
                       const std::shared_ptr<std::atomic<bool>> &destroyed) {
  Probe probe{destroyed};
  started->store(true, std::memory_order_release);
  while (true) {
    co_await cio::task::yield_now();
  }
}

Task<bool>
abort_destruction_case(const std::shared_ptr<std::atomic<bool>> &started,
                       const std::shared_ptr<std::atomic<bool>> &destroyed) {
  auto handle = cio::task::spawn(cancellable(started, destroyed));
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  handle.abort();
  auto result = co_await handle;
  co_return !result.has_value() && result.error().is_cancelled() &&
      destroyed->load(std::memory_order_acquire);
}

Task<void> append(const std::shared_ptr<std::vector<int>> &order, int value) {
  order->push_back(value);
  co_return;
}

Task<bool> composition_case() {
  const auto order = std::make_shared<std::vector<int>>();
  auto spawned = cio::task::spawn(append(order, 3));
  order->push_back(1);
  co_await append(order, 2);
  const bool before_spawned_poll = *order == std::vector<int>({1, 2});
  auto result = co_await spawned;
  co_return before_spawned_poll &&result.has_value() &&
      *order == std::vector<int>({1, 2, 3});
}

Task<bool> paused_sleep_rounding_case() {
  const auto start = cio::time::Instant::now();
  co_await cio::time::sleep(1ns);
  co_return cio::time::Instant::now() - start ==
      std::chrono::duration_cast<cio::time::Duration>(1ms);
}

Task<bool> timeout_immediate_zero_case() {
  auto result = co_await cio::time::timeout(
      cio::time::Duration::zero(), []() -> Task<int> { co_return 42; }());
  co_return result.has_value() && result.value() == 42;
}

Task<int> value_at_five_seconds() {
  co_await cio::time::sleep(5s);
  co_return 42;
}

Task<bool> timeout_same_deadline_case() {
  auto result = co_await cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(5s),
      value_at_five_seconds());
  co_return result.has_value() && result.value() == 42;
}

Task<int> timeout_loser(const std::shared_ptr<std::atomic<bool>> &destroyed) {
  Probe probe{destroyed};
  co_await cio::time::sleep(5s);
  co_return 42;
}

Task<bool> timeout_drops_loser_case() {
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  auto result = co_await cio::time::timeout(
      std::chrono::duration_cast<cio::time::Duration>(3s),
      timeout_loser(destroyed));
  co_return !result.has_value() && destroyed->load(std::memory_order_acquire);
}

Task<bool> sleep_reset_after_elapsed_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::sleep(2s);
  co_await timer;
  timer.reset(cio::time::Instant::now() +
              std::chrono::duration_cast<cio::time::Duration>(3s));
  co_await timer;
  co_return cio::time::Instant::now() - start ==
      std::chrono::duration_cast<cio::time::Duration>(5s);
}

Task<bool> interval_basic_case() {
  const auto start = cio::time::Instant::now();
  auto timer = cio::time::interval(2s);
  const auto first = co_await timer.tick();
  const auto second = co_await timer.tick();
  const auto third = co_await timer.tick();
  co_return first == start &&second - start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      third - start == std::chrono::duration_cast<cio::time::Duration>(4s);
}

Task<bool> interval_missed_ticks_case() {
  const auto delay_start = cio::time::Instant::now();
  auto delay = cio::time::interval(2s);
  delay.set_missed_tick_behavior(cio::time::MissedTickBehavior::delay);
  (void)co_await delay.tick();
  co_await cio::time::advance(10s);
  const auto missed = co_await delay.tick();
  const auto next = co_await delay.tick();
  const bool delay_ok =
      missed - delay_start ==
          std::chrono::duration_cast<cio::time::Duration>(2s) &&
      next - delay_start ==
          std::chrono::duration_cast<cio::time::Duration>(12s);

  const auto skip_start = cio::time::Instant::now();
  auto skip = cio::time::interval(2s);
  skip.set_missed_tick_behavior(cio::time::MissedTickBehavior::skip);
  (void)co_await skip.tick();
  co_await cio::time::advance(9s);
  (void)co_await skip.tick();
  const auto skip_next = co_await skip.tick();

  co_return delay_ok &&skip_next - skip_start ==
      std::chrono::duration_cast<cio::time::Duration>(10s);
}

Task<void> budget_hog(const std::shared_ptr<std::atomic<int>> &progress) {
  for (int step = 0; step < 1'000; ++step) {
    progress->store(step + 1, std::memory_order_release);
    co_await cio::task::consume_budget();
  }
}

Task<bool> consume_budget_yields_case() {
  const auto progress = std::make_shared<std::atomic<int>>(0);
  const auto observed = std::make_shared<std::atomic<int>>(-1);
  auto hog = cio::task::spawn(budget_hog(progress));
  auto sentinel = cio::task::spawn([progress, observed]() -> Task<void> {
    observed->store(progress->load(std::memory_order_acquire),
                    std::memory_order_release);
    co_return;
  }());
  const auto sentinel_result = co_await sentinel;
  const auto hog_result = co_await hog;
  co_return sentinel_result.has_value() && hog_result.has_value() &&
      observed->load(std::memory_order_acquire) < 1'000;
}

Task<bool> blocking_running_abort_noop_case() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto release = std::make_shared<std::atomic<bool>>(false);
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

Task<bool> blocking_queued_abort_case() {
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  const auto calls = std::make_shared<std::atomic<int>>(0);
  auto first = cio::task::spawn_blocking(
      [](std::shared_ptr<std::atomic<bool>> job_started,
         std::shared_ptr<std::atomic<bool>> job_release) {
        job_started->store(true, std::memory_order_release);
        while (!job_release->load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      },
      started, release);
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  auto second = cio::task::spawn_blocking(
      [](std::shared_ptr<std::atomic<int>> job_calls) {
        job_calls->fetch_add(1, std::memory_order_relaxed);
      },
      calls);
  second.abort();
  release->store(true, std::memory_order_release);
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return first_result.has_value() && !second_result.has_value() &&
      second_result.error().is_cancelled() &&
      calls->load(std::memory_order_acquire) == 0;
}

Task<void> blocking_timer(std::shared_ptr<std::atomic<bool>> completed) {
  co_await cio::time::sleep(5s);
  completed->store(true, std::memory_order_release);
}

Task<bool> blocking_paused_inhibits_time_case() {
  const auto start = cio::time::Instant::now();
  const auto timer_completed = std::make_shared<std::atomic<bool>>(false);
  auto timer = cio::task::spawn(blocking_timer(timer_completed));
  auto blocking =
      cio::task::spawn_blocking([] { std::this_thread::sleep_for(10ms); });
  const auto blocking_result = co_await blocking;
  const bool stayed_frozen = cio::time::Instant::now() == start &&
                             !timer_completed->load(std::memory_order_acquire);
  timer.abort();
  const auto timer_result = co_await timer;
  co_return blocking_result.has_value() && stayed_frozen &&
      !timer_result.has_value() && timer_result.error().is_cancelled();
}

Task<bool> notify_permit_coalesces_case() {
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

Task<bool> notify_fifo_lifo_case() {
  cio::sync::Notify notify;
  auto first = notify.notified();
  auto second = notify.notified();
  auto third = notify.notified();
  const bool registered =
      !first.enable() && !second.enable() && !third.enable();
  notify.notify_one();
  const bool fifo = first.enable() && !second.enable() && !third.enable();
  notify.notify_last();
  const bool lifo = third.enable() && !second.enable();
  notify.notify_one();
  co_await second;
  co_return registered && fifo && lifo;
}

Task<bool> notify_waiters_snapshot_case() {
  cio::sync::Notify notify;
  notify.notify_one();
  auto first = notify.notified();
  auto second = notify.notified();
  notify.notify_waiters();
  const bool broadcast = first.enable() && second.enable();
  co_await first;
  co_await second;
  auto permit = notify.notified();
  const bool retained = permit.enable();
  co_await permit;

  cio::sync::Notify fresh;
  fresh.notify_waiters();
  auto after = fresh.notified();
  const bool no_stored_broadcast = !after.enable();
  fresh.notify_one();
  co_await after;
  co_return broadcast && retained && no_stored_broadcast;
}

Task<bool> notify_cancel_transfers_case() {
  cio::sync::Notify notify;
  auto first = std::make_optional(notify.notified());
  auto second = notify.notified();
  const bool registered = !first->enable() && !second.enable();
  notify.notify_one();
  first.reset();
  const bool transferred = second.enable();
  co_await second;
  co_return registered &&transferred;
}

Task<void> semaphore_order_probe_child(cio::sync::Semaphore semaphore,
                                       std::uint32_t permits, int label,
                                       std::shared_ptr<std::vector<int>> order,
                                       cio::sync::Notify release) {
  auto result = co_await semaphore.acquire_many(permits);
  if (!result.has_value()) {
    co_return;
  }
  auto permit = std::move(result).value();
  order->push_back(label);
  co_await release.notified();
}

Task<bool> semaphore_fifo_head_blocking_case() {
  cio::sync::Semaphore semaphore{0};
  cio::sync::Notify release;
  const auto order = std::make_shared<std::vector<int>>();
  auto first = cio::task::spawn(
      semaphore_order_probe_child(semaphore, 3, 1, order, release));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(
      semaphore_order_probe_child(semaphore, 1, 2, order, release));
  co_await cio::task::yield_now();

  semaphore.add_permits(2);
  co_await cio::task::yield_now();
  const bool head_blocked = order->empty();
  semaphore.add_permits(1);
  while (order->size() < 1) {
    co_await cio::task::yield_now();
  }
  const bool fifo_first = *order == std::vector<int>({1});
  semaphore.add_permits(1);
  while (order->size() < 2) {
    co_await cio::task::yield_now();
  }
  const bool fifo_second = *order == std::vector<int>({1, 2});

  release.notify_waiters();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return head_blocked && fifo_first &&
      fifo_second &&first_result.has_value() && second_result.has_value() &&
      semaphore.available_permits() == 4;
}

Task<void> semaphore_hold_probe_child(
    cio::sync::Semaphore semaphore, std::uint32_t permits,
    std::shared_ptr<std::atomic<bool>> acquired, cio::sync::Notify release) {
  auto result = co_await semaphore.acquire_many(permits);
  if (!result.has_value()) {
    co_return;
  }
  auto permit = std::move(result).value();
  acquired->store(true, std::memory_order_release);
  co_await release.notified();
}

Task<bool> semaphore_cancel_partial_case() {
  cio::sync::Semaphore semaphore{0};
  cio::sync::Notify release;
  const auto first_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto second_acquired = std::make_shared<std::atomic<bool>>(false);
  auto first = cio::task::spawn(
      semaphore_hold_probe_child(semaphore, 3, first_acquired, release));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(
      semaphore_hold_probe_child(semaphore, 1, second_acquired, release));
  co_await cio::task::yield_now();

  semaphore.add_permits(2);
  first.abort();
  const auto first_result = co_await first;
  while (!second_acquired->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  const bool transferred = !first_result.has_value() &&
                           first_result.error().is_cancelled() &&
                           !first_acquired->load(std::memory_order_acquire) &&
                           semaphore.available_permits() == 1;
  release.notify_waiters();
  const auto second_result = co_await second;
  co_return transferred &&second_result.has_value() &&
      semaphore.available_permits() == 2;
}

Task<bool> semaphore_close_case() {
  cio::sync::Semaphore semaphore{1};
  auto waiter =
      cio::task::spawn([](cio::sync::Semaphore child_semaphore) -> Task<bool> {
        auto result = co_await child_semaphore.acquire_many(2);
        co_return !result.has_value() && result.error().is_closed();
      }(semaphore));
  co_await cio::task::yield_now();
  const bool partial = semaphore.available_permits() == 0;
  semaphore.close();
  const auto waiter_result = co_await waiter;
  const auto zero = semaphore.try_acquire_many(0);
  semaphore.add_permits(2);
  co_return partial &&waiter_result.has_value() && waiter_result.value() &&
      semaphore.is_closed() && semaphore.available_permits() == 3 &&
      !zero.has_value() && zero.error() == cio::sync::TryAcquireError::closed;
}

Task<bool> semaphore_permit_ops_case() {
  cio::sync::Semaphore semaphore{5};
  auto result = semaphore.try_acquire_many_owned(3);
  if (!result.has_value()) {
    co_return false;
  }
  auto permit = std::move(result).value();
  auto split = permit.split(1);
  if (!split.has_value() || permit.num_permits() != 2 ||
      split->num_permits() != 1) {
    co_return false;
  }
  permit.merge(std::move(*split));
  const bool merged =
      permit.num_permits() == 3 && permit.semaphore().available_permits() == 2;
  permit.forget();
  semaphore.add_permits(3);
  const auto forgotten = semaphore.forget_permits(9);
  co_return merged &&forgotten == 5 && semaphore.available_permits() == 0;
}

Task<void> mutex_order_probe_child(cio::sync::Mutex<int> mutex, int label,
                                   std::shared_ptr<std::vector<int>> order) {
  auto guard = co_await mutex.lock_owned();
  order->push_back(label);
  ++*guard;
  co_await cio::task::yield_now();
}

Task<bool> mutex_fifo_case() {
  cio::sync::Mutex<int> mutex{0};
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());
  const auto order = std::make_shared<std::vector<int>>();
  auto first = cio::task::spawn(mutex_order_probe_child(mutex, 1, order));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(mutex_order_probe_child(mutex, 2, order));
  co_await cio::task::yield_now();
  auto third = cio::task::spawn(mutex_order_probe_child(mutex, 3, order));
  co_await cio::task::yield_now();
  const bool waited = order->empty();
  owner.reset();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  const auto third_result = co_await third;
  auto final = co_await mutex.lock();
  co_return waited &&first_result.has_value() && second_result.has_value() &&
      third_result.has_value() && *order == std::vector<int>({1, 2, 3}) &&
      *final == 3;
}

Task<bool> mutex_cancel_transfers_case() {
  cio::sync::Mutex<int> mutex{0};
  const auto order = std::make_shared<std::vector<int>>();
  std::optional<cio::sync::MutexGuard<int>> owner;
  auto owner_result = mutex.try_lock();
  owner.emplace(std::move(owner_result).value());
  auto first = cio::task::spawn(mutex_order_probe_child(mutex, 1, order));
  co_await cio::task::yield_now();
  auto second = cio::task::spawn(mutex_order_probe_child(mutex, 2, order));
  co_await cio::task::yield_now();
  owner.reset();
  first.abort();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  co_return !first_result.has_value() && first_result.error().is_cancelled() &&
      second_result.has_value() && *order == std::vector<int>({2}) &&
      mutex.try_lock().has_value();
}

Task<void> mutex_panic_probe_child(cio::sync::Mutex<int> mutex) {
  auto guard = co_await mutex.lock();
  *guard = 7;
  throw 42;
}

Task<bool> mutex_no_poison_case() {
  cio::sync::Mutex<int> mutex{0};
  auto child = cio::task::spawn(mutex_panic_probe_child(mutex));
  const auto result = co_await child;
  auto after = mutex.try_lock();
  co_return !result.has_value() && result.error().is_panic() &&
      after.has_value() && *after.value() == 7;
}

Task<bool> mutex_owned_map_case() {
  cio::sync::Mutex<MutexProbeRecord> mutex{
      MutexProbeRecord{1, MutexProbeRecord::Nested{2}}};
  {
    auto guard = co_await mutex.lock_owned();
    auto nested = cio::sync::MutexGuard<MutexProbeRecord>::map(
        std::move(guard),
        [](MutexProbeRecord &value) -> MutexProbeRecord::Nested & {
          return value.nested;
        });
    auto value = cio::sync::
        MappedMutexGuard<MutexProbeRecord, MutexProbeRecord::Nested>::map(
            std::move(nested),
            [](MutexProbeRecord::Nested &nested_value) -> int & {
              return nested_value.value;
            });
    *value = 9;
  }
  {
    auto guard = co_await mutex.lock_owned();
    auto failed = cio::sync::MutexGuard<MutexProbeRecord>::try_map(
        std::move(guard), [](MutexProbeRecord &) { return false; },
        [](MutexProbeRecord &value) -> int & { return value.first; });
    if (failed.has_value()) {
      co_return false;
    }
    auto original = std::move(failed).error();
    (*original).first = 4;
  }
  auto final = co_await mutex.lock();
  co_return (*final).first == 4 && (*final).nested.value == 9;
}

Task<bool> mutex_blocking_bridge_case() {
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

Task<void>
rwlock_hold_reader_child(cio::sync::RwLock<int> rwlock,
                         const std::shared_ptr<std::atomic<bool>> &acquired,
                         cio::sync::Notify release) {
  auto guard = co_await rwlock.read_owned();
  acquired->store(true, std::memory_order_release);
  co_await release.notified();
}

Task<bool> rwlock_shared_max_readers_case() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(7, 2);
  std::optional<cio::sync::RwLockReadGuard<int>> first;
  std::optional<cio::sync::RwLockReadGuard<int>> second;
  first.emplace(co_await rwlock.read_owned());
  second.emplace(co_await rwlock.read_owned());

  cio::sync::Notify release;
  const auto third_acquired = std::make_shared<std::atomic<bool>>(false);
  auto third = cio::task::spawn(
      rwlock_hold_reader_child(rwlock, third_acquired, release));
  co_await cio::task::yield_now();
  const bool capped = !third_acquired->load(std::memory_order_acquire) &&
                      !rwlock.try_write().has_value();

  first.reset();
  while (!third_acquired->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  const bool shared =
      second.has_value() && **second == 7 && !rwlock.try_write().has_value();

  release.notify_waiters();
  const auto third_result = co_await third;
  second.reset();
  auto writer = rwlock.try_write();
  co_return capped && shared && third_result.has_value() &&
      writer.has_value() && *writer.value() == 7;
}

Task<void>
rwlock_writer_order_child(cio::sync::RwLock<int> rwlock,
                          const std::shared_ptr<std::vector<int>> &order,
                          cio::sync::Notify release) {
  auto guard = co_await rwlock.write_owned();
  order->push_back(1);
  ++*guard;
  co_await release.notified();
}

Task<void>
rwlock_reader_order_child(cio::sync::RwLock<int> rwlock,
                          const std::shared_ptr<std::vector<int>> &order,
                          cio::sync::Notify release) {
  auto guard = co_await rwlock.read_owned();
  order->push_back(2);
  co_await release.notified();
}

Task<bool> rwlock_writer_priority_fifo_case() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 3);
  std::optional<cio::sync::RwLockReadGuard<int>> owner;
  owner.emplace(co_await rwlock.read_owned());
  const auto order = std::make_shared<std::vector<int>>();
  cio::sync::Notify release_writer;
  cio::sync::Notify release_reader;

  auto writer = cio::task::spawn(
      rwlock_writer_order_child(rwlock, order, release_writer));
  co_await cio::task::yield_now();
  auto reader = cio::task::spawn(
      rwlock_reader_order_child(rwlock, order, release_reader));
  co_await cio::task::yield_now();
  const bool both_waited = order->empty();

  owner.reset();
  while (order->empty()) {
    co_await cio::task::yield_now();
  }
  const bool writer_first = *order == std::vector<int>({1});
  release_writer.notify_waiters();
  while (order->size() < 2) {
    co_await cio::task::yield_now();
  }
  const bool reader_second = *order == std::vector<int>({1, 2});
  release_reader.notify_waiters();

  const auto writer_result = co_await writer;
  const auto reader_result = co_await reader;
  auto final = co_await rwlock.read();
  co_return both_waited && writer_first &&
      reader_second &&writer_result.has_value() && reader_result.has_value() &&
      *final == 1;
}

Task<void>
rwlock_cancel_writer_child(cio::sync::RwLock<int> rwlock,
                           const std::shared_ptr<std::atomic<bool>> &acquired) {
  auto guard = co_await rwlock.write_owned();
  acquired->store(true, std::memory_order_release);
  ++*guard;
}

Task<bool> rwlock_cancel_partial_writer_case() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 3);
  std::optional<cio::sync::RwLockReadGuard<int>> owner;
  owner.emplace(co_await rwlock.read_owned());
  const auto writer_acquired = std::make_shared<std::atomic<bool>>(false);
  auto writer =
      cio::task::spawn(rwlock_cancel_writer_child(rwlock, writer_acquired));
  co_await cio::task::yield_now();

  cio::sync::Notify release_reader;
  const auto reader_acquired = std::make_shared<std::atomic<bool>>(false);
  auto reader = cio::task::spawn(
      rwlock_hold_reader_child(rwlock, reader_acquired, release_reader));
  co_await cio::task::yield_now();
  const bool partial_blocked =
      !writer_acquired->load(std::memory_order_acquire) &&
      !reader_acquired->load(std::memory_order_acquire);

  writer.abort();
  const auto writer_result = co_await writer;
  while (!reader_acquired->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  const bool transferred = !writer_result.has_value() &&
                           writer_result.error().is_cancelled() &&
                           !writer_acquired->load(std::memory_order_acquire) &&
                           owner.has_value() && **owner == 0;

  release_reader.notify_waiters();
  const auto reader_result = co_await reader;
  owner.reset();
  auto final = rwlock.try_write();
  co_return partial_blocked && transferred && reader_result.has_value() &&
      final.has_value() && *final.value() == 0;
}

Task<void> rwlock_panic_probe_child(cio::sync::RwLock<int> rwlock) {
  auto guard = co_await rwlock.write_owned();
  *guard = 7;
  throw 42;
}

Task<bool> rwlock_no_poison_case() {
  cio::sync::RwLock<int> rwlock{0};
  auto child = cio::task::spawn(rwlock_panic_probe_child(rwlock));
  const auto result = co_await child;
  if (result.has_value() || !result.error().is_panic()) {
    co_return false;
  }
  bool readable = false;
  {
    auto after_read = rwlock.try_read();
    readable = after_read.has_value() && *after_read.value() == 7;
  }
  auto after_write = rwlock.try_write();
  co_return readable &&after_write.has_value() && *after_write.value() == 7;
}

Task<bool> rwlock_owned_mapping_case() {
  cio::sync::RwLock<RwLockProbeRecord> rwlock{
      RwLockProbeRecord{RwLockProbeRecord::Nested{2}}};
  {
    auto guard = co_await rwlock.write_owned();
    auto nested = cio::sync::RwLockWriteGuard<RwLockProbeRecord>::map(
        std::move(guard),
        [](RwLockProbeRecord &value) -> RwLockProbeRecord::Nested & {
          return value.nested;
        });
    auto value = cio::sync::RwLockMappedWriteGuard<RwLockProbeRecord,
                                                   RwLockProbeRecord::Nested>::
        map(std::move(nested),
            [](RwLockProbeRecord::Nested &nested_value) -> int & {
              return nested_value.value;
            });
    *value = 9;
  }
  auto read = co_await rwlock.read_owned();
  auto nested = cio::sync::RwLockReadGuard<RwLockProbeRecord>::map(
      std::move(read),
      [](const RwLockProbeRecord &value) -> const RwLockProbeRecord::Nested & {
        return value.nested;
      });
  auto value =
      cio::sync::RwLockReadGuard<RwLockProbeRecord, RwLockProbeRecord::Nested>::
          map(std::move(nested),
              [](const RwLockProbeRecord::Nested &nested_value) -> const int & {
                return nested_value.value;
              });
  co_return *value == 9;
}

Task<void> rwlock_downgrade_writer_child(
    cio::sync::RwLock<int> rwlock,
    const std::shared_ptr<std::atomic<bool>> &acquired) {
  auto guard = co_await rwlock.write_owned();
  acquired->store(true, std::memory_order_release);
  *guard = 2;
}

Task<bool> rwlock_atomic_downgrade_case() {
  cio::sync::RwLock<int> rwlock{1};
  auto write = co_await rwlock.write_owned();
  const auto writer_acquired = std::make_shared<std::atomic<bool>>(false);
  auto writer =
      cio::task::spawn(rwlock_downgrade_writer_child(rwlock, writer_acquired));
  co_await cio::task::yield_now();

  std::optional<cio::sync::RwLockReadGuard<int>> read;
  read.emplace(cio::sync::RwLockWriteGuard<int>::downgrade(std::move(write)));
  co_await cio::task::yield_now();
  const bool stayed_read_locked =
      **read == 1 && !writer_acquired->load(std::memory_order_acquire);
  read.reset();

  const auto writer_result = co_await writer;
  auto final = co_await rwlock.read();
  co_return stayed_read_locked &&writer_result.has_value() &&
      writer_acquired->load(std::memory_order_acquire) && *final == 2;
}

Task<void> barrier_wait_probe_child(cio::sync::Barrier barrier,
                                    std::shared_ptr<std::atomic<int>> completed,
                                    std::shared_ptr<std::atomic<int>> leaders) {
  const auto result = co_await barrier.wait();
  if (result.is_leader()) {
    leaders->fetch_add(1, std::memory_order_relaxed);
  }
  completed->fetch_add(1, std::memory_order_release);
}

Task<bool> barrier_zero_single_leader_case() {
  cio::sync::Barrier zero{0};
  cio::sync::Barrier single{1};
  const auto zero_result = co_await zero.wait();
  const auto single_result = co_await single.wait();
  co_return zero_result.is_leader() && single_result.is_leader();
}

Task<bool> barrier_lazy_unpolled_case() {
  cio::sync::Barrier barrier{2};
  auto unpolled = barrier.wait();
  const auto completed = std::make_shared<std::atomic<int>>(0);
  const auto leaders = std::make_shared<std::atomic<int>>(0);

  auto first =
      cio::task::spawn(barrier_wait_probe_child(barrier, completed, leaders));
  co_await cio::task::yield_now();
  const bool unpolled_not_counted =
      completed->load(std::memory_order_acquire) == 0;

  auto second =
      cio::task::spawn(barrier_wait_probe_child(barrier, completed, leaders));
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  (void)unpolled;
  co_return unpolled_not_counted &&first_result.has_value() &&
      second_result.has_value() &&
      completed->load(std::memory_order_acquire) == 2 &&
      leaders->load(std::memory_order_acquire) == 1;
}

Task<bool> barrier_reusable_unique_leader_case() {
  cio::sync::Barrier barrier{3};
  for (int generation = 0; generation < 4; ++generation) {
    const auto completed = std::make_shared<std::atomic<int>>(0);
    const auto leaders = std::make_shared<std::atomic<int>>(0);
    std::vector<cio::task::JoinHandle<void>> waiters;
    waiters.reserve(3);
    for (int participant = 0; participant < 3; ++participant) {
      waiters.push_back(cio::task::spawn(
          barrier_wait_probe_child(barrier, completed, leaders)));
    }
    for (auto &waiter : waiters) {
      const auto result = co_await waiter;
      if (!result.has_value()) {
        co_return false;
      }
    }
    if (completed->load(std::memory_order_acquire) != 3 ||
        leaders->load(std::memory_order_acquire) != 1) {
      co_return false;
    }
  }
  co_return true;
}

Task<void>
barrier_cancelled_arrival_child(cio::sync::Barrier barrier,
                                std::shared_ptr<std::atomic<bool>> started) {
  started->store(true, std::memory_order_release);
  (void)co_await barrier.wait();
}

Task<bool> barrier_cancelled_arrival_retained_case() {
  cio::sync::Barrier barrier{3};
  const auto started = std::make_shared<std::atomic<bool>>(false);
  auto cancelled =
      cio::task::spawn(barrier_cancelled_arrival_child(barrier, started));
  while (!started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  cancelled.abort();
  const auto cancelled_result = co_await cancelled;

  const auto completed = std::make_shared<std::atomic<int>>(0);
  const auto leaders = std::make_shared<std::atomic<int>>(0);
  auto second =
      cio::task::spawn(barrier_wait_probe_child(barrier, completed, leaders));
  auto third =
      cio::task::spawn(barrier_wait_probe_child(barrier, completed, leaders));
  const auto second_result = co_await second;
  const auto third_result = co_await third;

  co_return !cancelled_result.has_value() &&
      cancelled_result.error().is_cancelled() && second_result.has_value() &&
      third_result.has_value() &&
      completed->load(std::memory_order_acquire) == 2 &&
      leaders->load(std::memory_order_acquire) == 1;
}

Task<int> once_cell_counted_value(std::shared_ptr<std::atomic<int>> calls,
                                  int value) {
  calls->fetch_add(1, std::memory_order_relaxed);
  co_await cio::task::yield_now();
  co_return value;
}

struct OnceCellCountedFactory final {
  std::shared_ptr<std::atomic<int>> calls;
  int value{0};
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const { return once_cell_counted_value(calls, value); }
};

Task<int> once_cell_waiting_value(std::shared_ptr<cio::sync::Semaphore> gate,
                                  std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  auto permit = co_await gate->acquire_owned();
  if (!permit.has_value()) {
    throw std::runtime_error{"OnceCell 差分 gate 被关闭"};
  }
  co_return 1;
}

struct OnceCellWaitingFactory final {
  std::shared_ptr<cio::sync::Semaphore> gate;
  std::shared_ptr<std::atomic<bool>> entered;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const {
    return once_cell_waiting_value(gate, entered);
  }
};

Task<cio::Result<int, int>> once_cell_try_value(bool fail, int value) {
  co_await cio::task::yield_now();
  if (fail) {
    co_return cio::Result<int, int>::failure(value);
  }
  co_return cio::Result<int, int>::success(value);
}

struct OnceCellTryFactory final {
  bool fail{false};
  int value{0};
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<cio::Result<int, int>> operator()() const {
    return once_cell_try_value(fail, value);
  }
};

Task<bool> once_cell_single_initializer_case() {
  cio::sync::OnceCell<int> cell;
  const auto calls = std::make_shared<std::atomic<int>>(0);
  std::vector<cio::task::JoinHandle<std::shared_ptr<const int>>> handles;
  handles.reserve(16);
  for (int index = 0; index < 16; ++index) {
    handles.push_back(
        cio::task::spawn(cell.get_or_init(OnceCellCountedFactory{calls, 7})));
  }
  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value() || !result.value() || *result.value() != 7) {
      co_return false;
    }
  }
  co_return calls->load(std::memory_order_relaxed) == 1;
}

Task<bool> once_cell_cancel_retry_case() {
  cio::sync::OnceCell<int> cell;
  const auto gate = std::make_shared<cio::sync::Semaphore>(0);
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto owner =
      cio::task::spawn(cell.get_or_init(OnceCellWaitingFactory{gate, entered}));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }

  auto set_result = cell.set(9);
  const bool initializing_error = !set_result.has_value() &&
                                  set_result.error().is_initializing_err() &&
                                  set_result.error().value() == 9;
  owner.abort();
  const auto cancelled = co_await owner;
  const auto retry = co_await cell.get_or_init(
      OnceCellCountedFactory{std::make_shared<std::atomic<int>>(0), 2});
  co_return initializing_error && !cancelled.has_value() &&
      cancelled.error().is_cancelled() && retry && *retry == 2;
}

Task<bool> once_cell_try_error_retry_case() {
  cio::sync::OnceCell<int> cell;
  const auto failed =
      co_await cell.get_or_try_init(OnceCellTryFactory{true, 5});
  const auto succeeded =
      co_await cell.get_or_try_init(OnceCellTryFactory{false, 8});
  co_return !failed.has_value() && failed.error() == 5 &&
      succeeded.has_value() && succeeded.value() && *succeeded.value() == 8;
}

Task<bool> once_cell_clone_independent_case() {
  cio::sync::OnceCell<int> cell;
  if (!cell.set(1).has_value()) {
    co_return false;
  }
  auto cloned = cell.clone();
  const auto taken = cell.take();
  if (!taken || *taken != 1 || !cell.set(2).has_value()) {
    co_return false;
  }
  const auto original = cell.get();
  const auto copy = cloned.get();
  co_return original && copy && *original == 2 && *copy == 1;
}

Task<bool> once_cell_debug_format_case() {
  cio::sync::OnceCell<int> cell;
  std::ostringstream empty_stream;
  empty_stream << cell;
  const bool empty = cell.debug_string() == "OnceCell { value: None }" &&
                     empty_stream.str() == "OnceCell { value: None }";
  if (!cell.set(7).has_value()) {
    co_return false;
  }
  std::ostringstream initialized_stream;
  initialized_stream << cell;
  co_return empty &&cell.debug_string() == "OnceCell { value: Some(7) }" &&
      initialized_stream.str() == "OnceCell { value: Some(7) }";
}

Task<bool> once_cell_set_error_format_case() {
  cio::sync::OnceCell<int> initialized;
  if (!initialized.set(1).has_value()) {
    co_return false;
  }
  auto already = initialized.set(5);
  if (already.has_value()) {
    co_return false;
  }
  std::ostringstream already_display;
  already_display << already.error();
  const bool already_ok =
      already.error().is_already_init_err() && already.error().value() == 5 &&
      already.error().debug_string() == "AlreadyInitializedError(5)" &&
      already_display.str() == "AlreadyInitializedError";

  cio::sync::OnceCell<int> initializing;
  const auto gate = std::make_shared<cio::sync::Semaphore>(0);
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto owner = cio::task::spawn(
      initializing.get_or_init(OnceCellWaitingFactory{gate, entered}));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  auto during = initializing.set(9);
  if (during.has_value()) {
    owner.abort();
    (void)co_await owner;
    co_return false;
  }
  std::ostringstream initializing_display;
  initializing_display << during.error();
  const bool initializing_ok =
      during.error().is_initializing_err() && during.error().value() == 9 &&
      during.error().debug_string() == "InitializingError(9)" &&
      initializing_display.str() == "InitializingError";
  owner.abort();
  const auto cancelled = co_await owner;
  co_return already_ok && initializing_ok && !cancelled.has_value() &&
      cancelled.error().is_cancelled();
}

Task<std::shared_ptr<const int>>
set_once_wait_child(std::shared_ptr<cio::sync::SetOnce<int>> set_once,
                    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_return co_await set_once->wait();
}

Task<bool> set_once_wait_unblocks_case() {
  const auto set_once = std::make_shared<cio::sync::SetOnce<int>>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto waiter = cio::task::spawn(set_once_wait_child(set_once, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const auto set = set_once->set(11);
  const auto joined = co_await waiter;
  co_return set.has_value() && joined.has_value() && joined.value() &&
      *joined.value() == 11;
}

Task<int> set_once_contender(std::shared_ptr<cio::sync::SetOnce<int>> set_once,
                             int value) {
  co_await cio::task::yield_now();
  auto result = set_once->set(value);
  if (result.has_value()) {
    co_return -value;
  }
  co_return std::move(result).error().into_value();
}

Task<bool> set_once_single_winner_values_case() {
  const auto set_once = std::make_shared<cio::sync::SetOnce<int>>();
  std::vector<cio::task::JoinHandle<int>> contenders;
  contenders.reserve(16);
  for (int value = 1; value <= 16; ++value) {
    contenders.push_back(cio::task::spawn(set_once_contender(set_once, value)));
  }

  int winners = 0;
  int observed_sum = 0;
  for (auto &contender : contenders) {
    const auto result = co_await contender;
    if (!result.has_value()) {
      co_return false;
    }
    if (result.value() < 0) {
      ++winners;
      observed_sum += -result.value();
    } else {
      observed_sum += result.value();
    }
  }
  const auto winner = set_once->get();
  co_return winners == 1 && observed_sum == 136 && winner && *winner >= 1 &&
      *winner <= 16;
}

Task<bool> set_once_cancel_safe_case() {
  const auto set_once = std::make_shared<cio::sync::SetOnce<int>>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto cancelled = cio::task::spawn(set_once_wait_child(set_once, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  cancelled.abort();
  const auto cancelled_result = co_await cancelled;
  const auto set = set_once->set(13);
  const auto retry = co_await set_once->wait();
  co_return !cancelled_result.has_value() &&
      cancelled_result.error().is_cancelled() && set.has_value() && retry &&
      *retry == 13;
}

Task<bool> set_once_clone_independent_case() {
  cio::sync::SetOnce<int> original;
  auto cloned = original.clone();
  const auto first = original.set(17);
  const auto second = cloned.set(23);
  const auto original_value = original.get();
  const auto cloned_value = cloned.get();
  co_return first.has_value() && second.has_value() && original_value &&
      cloned_value && *original_value == 17 && *cloned_value == 23;
}

Task<cio::Result<int, cio::sync::oneshot::error::RecvError>>
oneshot_marked_receive(cio::sync::oneshot::Receiver<int>::Receive receive,
                       std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_return co_await std::move(receive);
}

Task<void> oneshot_closed(cio::sync::oneshot::Sender<int>::Closed closed) {
  co_await std::move(closed);
}

Task<bool> oneshot_send_receive_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  const auto sent = std::move(sender).send(11);
  const auto received = co_await receiver;
  co_return sent.has_value() && received.has_value() && received.value() == 11;
}

Task<bool> oneshot_sender_drop_recv_error_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  std::optional<cio::sync::oneshot::Sender<int>> owned_sender{
      std::move(sender)};
  owned_sender.reset();
  const auto received = co_await receiver;
  if (received.has_value()) {
    co_return false;
  }
  const auto &error = received.error();
  std::ostringstream display;
  display << error;
  co_return error.message() == "channel closed" &&
      error.debug_string() == "RecvError(())" &&
      display.str() == "channel closed";
}

Task<bool> oneshot_receiver_drop_returns_value_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  std::optional<cio::sync::oneshot::Receiver<int>> owned_receiver{
      std::move(receiver)};
  owned_receiver.reset();
  const auto sent = std::move(sender).send(17);
  co_return !sent.has_value() && sent.error() == 17;
}

Task<bool> oneshot_close_preserves_sent_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  const auto sent = std::move(sender).send(23);
  receiver.close();
  const auto received = receiver.try_recv();
  co_return sent.has_value() && received.has_value() &&
      received.value() == 23 && receiver.is_empty() && receiver.is_terminated();
}

Task<bool> oneshot_close_rejects_late_send_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  receiver.close();
  const auto sent = std::move(sender).send(29);
  const auto received = co_await receiver;
  co_return !sent.has_value() && sent.error() == 29 && !received.has_value() &&
      received.error().message() == "channel closed";
}

Task<bool> oneshot_try_recv_empty_closed_case() {
  using cio::sync::oneshot::error::TryRecvError;
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  const auto empty = receiver.try_recv();
  std::optional<cio::sync::oneshot::Sender<int>> owned_sender{
      std::move(sender)};
  owned_sender.reset();
  const auto closed = receiver.try_recv();
  if (empty.has_value() || closed.has_value()) {
    co_return false;
  }
  std::ostringstream empty_display;
  empty_display << empty.error();
  std::ostringstream closed_display;
  closed_display << closed.error();
  co_return empty.error() == TryRecvError::empty &&closed.error() ==
          TryRecvError::closed &&cio::sync::oneshot::error::debug_string(
              empty.error()) == "Empty" &&
      cio::sync::oneshot::error::debug_string(closed.error()) == "Closed" &&
      empty_display.str() == "channel empty" &&
      closed_display.str() == "channel closed";
}

Task<bool> oneshot_receive_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto cancelled =
      cio::task::spawn(oneshot_marked_receive(receiver.receive(), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  cancelled.abort();
  const auto cancelled_result = co_await cancelled;
  const auto sent = std::move(sender).send(31);
  const auto retried = co_await receiver;
  co_return !cancelled_result.has_value() &&
      cancelled_result.error().is_cancelled() && sent.has_value() &&
      retried.has_value() && retried.value() == 31;
}

Task<bool> oneshot_sender_closed_wakes_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  auto waiting = cio::task::spawn(oneshot_closed(sender.closed()));
  co_await cio::task::yield_now();
  receiver.close();
  const auto joined = co_await waiting;
  co_return joined.has_value() && sender.is_closed();
}

Task<bool> oneshot_empty_terminated_transitions_case() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  const bool initial = receiver.is_empty() && !receiver.is_terminated();
  const auto sent = std::move(sender).send(37);
  const bool published = !receiver.is_empty() && !receiver.is_terminated();
  const auto received = receiver.try_recv();
  const bool consumed = receiver.is_empty() && receiver.is_terminated();
  co_return initial &&sent.has_value() && published &&received.has_value() &&
      received.value() == 37 && consumed;
}

struct OneshotDropProbe final {
  std::shared_ptr<std::atomic<int>> drops;
  bool active{true};

  OneshotDropProbe(std::shared_ptr<std::atomic<int>> counter)
      : drops{std::move(counter)} {}

  OneshotDropProbe(const OneshotDropProbe &) = delete;
  OneshotDropProbe &operator=(const OneshotDropProbe &) = delete;

  OneshotDropProbe(OneshotDropProbe &&other) noexcept
      : drops{std::move(other.drops)},
        active{std::exchange(other.active, false)} {}

  OneshotDropProbe &operator=(OneshotDropProbe &&) = delete;

  ~OneshotDropProbe() {
    if (active) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

Task<bool> oneshot_value_drop_once_case() {
  const auto drops = std::make_shared<std::atomic<int>>(0);
  {
    auto [sender, receiver] = cio::sync::oneshot::channel<OneshotDropProbe>();
    const auto sent = std::move(sender).send(OneshotDropProbe{drops});
    if (!sent.has_value()) {
      co_return false;
    }
  }
  co_return drops->load(std::memory_order_relaxed) == 1;
}

Task<bool> oneshot_ready_budget_yields_case() {
  constexpr std::size_t bound = 512;

  const auto receive_peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto receive_peer = cio::task::spawn(set_flag(receive_peer_ran));
  std::size_t receive_peer_iteration = 0;
  std::uint64_t checksum = 0;
  for (std::size_t iteration = 1; iteration <= bound; ++iteration) {
    auto [sender, receiver] = cio::sync::oneshot::channel<std::uint64_t>();
    const auto sent =
        std::move(sender).send(static_cast<std::uint64_t>(iteration));
    const auto received = co_await receiver;
    if (!sent.has_value() || !received.has_value() ||
        received.value() != iteration) {
      co_return false;
    }
    checksum += received.value();
    if (receive_peer_iteration == 0 &&
        receive_peer_ran->load(std::memory_order_acquire)) {
      receive_peer_iteration = iteration;
    }
  }
  const auto receive_peer_joined = co_await receive_peer;

  const auto closed_peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto closed_peer = cio::task::spawn(set_flag(closed_peer_ran));
  std::size_t closed_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= bound; ++iteration) {
    auto [sender, receiver] = cio::sync::oneshot::channel<std::uint64_t>();
    receiver.close();
    co_await sender.closed();
    if (!sender.is_closed()) {
      co_return false;
    }
    if (closed_peer_iteration == 0 &&
        closed_peer_ran->load(std::memory_order_acquire)) {
      closed_peer_iteration = iteration;
    }
  }
  const auto closed_peer_joined = co_await closed_peer;

  const auto expected_checksum = static_cast<std::uint64_t>(bound) *
                                 static_cast<std::uint64_t>(bound + 1) / 2;
  co_return receive_peer_joined.has_value() && closed_peer_joined.has_value() &&
      receive_peer_iteration != 0 &&
      receive_peer_iteration <= bound &&closed_peer_iteration != 0 &&
      closed_peer_iteration <= bound &&checksum == expected_checksum;
}

Task<cio::Result<void, cio::sync::mpsc::error::SendError<int>>>
mpsc_marked_send(cio::sync::mpsc::Sender<int> sender, int value,
                 std::shared_ptr<std::atomic<bool>> entered,
                 std::shared_ptr<std::atomic<bool>> completed = {}) {
  entered->store(true, std::memory_order_release);
  auto result = co_await sender.send(value);
  if (completed) {
    completed->store(true, std::memory_order_release);
  }
  co_return result;
}

Task<bool>
mpsc_marked_reserve_send(cio::sync::mpsc::Sender<int> sender, int value,
                         std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  auto permit = std::move(reserved).value();
  std::move(permit).send(value);
  co_return true;
}

Task<bool> mpsc_fifo_backpressure_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  auto first_sent = sender.try_send(1);
  auto second_sent = sender.try_send(2);
  auto full = sender.try_send(3);
  if (!first_sent.has_value() || !second_sent.has_value() ||
      full.has_value() || !full.error().is_full() ||
      full.error().value() != 3) {
    co_return false;
  }

  const auto entered = std::make_shared<std::atomic<bool>>(false);
  const auto completed = std::make_shared<std::atomic<bool>>(false);
  auto pending =
      cio::task::spawn(mpsc_marked_send(sender, 3, entered, completed));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool observed_backpressure =
      !completed->load(std::memory_order_acquire);

  const auto first = co_await receiver.recv();
  const auto joined = co_await pending;
  const auto second = co_await receiver.recv();
  const auto third = co_await receiver.recv();
  co_return observed_backpressure &&first == std::optional<int>{1} &&
      second == std::optional<int>{2} && third == std::optional<int>{3} &&
      joined.has_value() && joined.value().has_value();
}

Task<bool> mpsc_send_reserve_fairness_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(0).has_value()) {
    co_return false;
  }

  const auto first_entered = std::make_shared<std::atomic<bool>>(false);
  const auto reserve_entered = std::make_shared<std::atomic<bool>>(false);
  const auto third_entered = std::make_shared<std::atomic<bool>>(false);

  auto first =
      cio::task::spawn(mpsc_marked_send(sender, 1, first_entered));
  while (!first_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();

  auto reserved = cio::task::spawn(
      mpsc_marked_reserve_send(sender, 2, reserve_entered));
  while (!reserve_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();

  auto third =
      cio::task::spawn(mpsc_marked_send(sender, 3, third_entered));
  while (!third_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();

  const auto zero = co_await receiver.recv();
  const auto first_joined = co_await first;
  const auto one = co_await receiver.recv();
  const auto reserve_joined = co_await reserved;
  const auto two = co_await receiver.recv();
  const auto third_joined = co_await third;
  const auto three = co_await receiver.recv();

  co_return zero == std::optional<int>{0} && one == std::optional<int>{1} &&
      two == std::optional<int>{2} && three == std::optional<int>{3} &&
      first_joined.has_value() && first_joined.value().has_value() &&
      reserve_joined.has_value() && reserve_joined.value() &&
      third_joined.has_value() && third_joined.value().has_value();
}

Task<bool> mpsc_cancel_send_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }

  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending = cio::task::spawn(mpsc_marked_send(sender, 2, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  pending.abort();
  const auto cancelled = co_await pending;

  const auto first = co_await receiver.recv();
  const auto retry = sender.try_send(3);
  const auto third = co_await receiver.recv();
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      first == std::optional<int>{1} && retry.has_value() &&
      third == std::optional<int>{3};
}

Task<bool> mpsc_cancel_reserve_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }

  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending =
      cio::task::spawn(mpsc_marked_reserve_send(sender, 2, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  pending.abort();
  const auto cancelled = co_await pending;

  const auto first = co_await receiver.recv();
  const auto retry = sender.try_send(3);
  const auto third = co_await receiver.recv();
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      first == std::optional<int>{1} && retry.has_value() &&
      third == std::optional<int>{3};
}

Task<bool> mpsc_permit_capacity_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  const bool initial =
      sender.capacity() == 2 && sender.max_capacity() == 2;

  auto reserved = co_await sender.reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::Permit<int>> held{
      std::move(reserved).value()};
  const bool held_capacity = sender.capacity() == 1;
  held.reset();
  const bool restored = sender.capacity() == 2;

  auto second = co_await sender.reserve();
  if (!second.has_value()) {
    co_return false;
  }
  auto permit = std::move(second).value();
  std::move(permit).send(5);
  const bool sent_capacity = sender.capacity() == 1;
  const auto received = co_await receiver.recv();
  co_return initial && held_capacity && restored && sent_capacity &&
      received == std::optional<int>{5} && sender.capacity() == 2;
}

Task<bool> mpsc_close_drain_permit_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value() || sender.capacity() != 0) {
    co_return false;
  }
  auto permit = std::move(reserved).value();

  receiver.close();
  auto rejected = sender.try_send(3);
  if (rejected.has_value() || !rejected.error().is_closed() ||
      rejected.error().value() != 3) {
    co_return false;
  }
  std::move(permit).send(2);

  const auto first = co_await receiver.recv();
  const auto second = co_await receiver.recv();
  const auto finished = co_await receiver.recv();
  co_return sender.is_closed() &&first == std::optional<int>{1} &&
      second == std::optional<int>{2} && !finished.has_value();
}

Task<bool> mpsc_try_errors_case() {
  using cio::sync::mpsc::error::TryRecvError;
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto empty = receiver.try_recv();
  auto first = sender.try_send(1);
  auto full = sender.try_send(2);
  receiver.close();
  auto closed = sender.try_send(3);
  const auto received = co_await receiver.recv();
  const auto finished = co_await receiver.recv();
  auto disconnected = receiver.try_recv();

  co_return !empty.has_value() &&
      empty.error() == TryRecvError::empty &&first.has_value() &&
      !full.has_value() && full.error().is_full() &&
      full.error().value() == 2 && !closed.has_value() &&
      closed.error().is_closed() && closed.error().value() == 3 &&
      received == std::optional<int>{1} && !finished.has_value() &&
      !disconnected.has_value() &&
      disconnected.error() == TryRecvError::disconnected;
}

Task<bool> mpsc_receiver_drop_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(7).has_value()) {
    co_return false;
  }
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending = cio::task::spawn(mpsc_marked_send(sender, 8, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();

  std::optional<cio::sync::mpsc::Receiver<int>> owned_receiver{
      std::move(receiver)};
  owned_receiver.reset();
  const auto joined = co_await pending;
  co_return joined.has_value() && !joined.value().has_value() &&
      joined.value().error().value() == 8 && sender.is_closed();
}

Task<bool> mpsc_last_sender_weak_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  std::optional<cio::sync::mpsc::Sender<int>> strong{std::move(sender)};
  auto weak = strong->downgrade();
  std::optional<cio::sync::mpsc::Sender<int>> clone{*strong};
  clone.reset();
  strong.reset();

  const bool cannot_revive = !weak.upgrade().has_value() &&
                             weak.strong_count() == 0 &&
                             weak.weak_count() == 1;
  const auto finished = co_await receiver.recv();
  co_return cannot_revive && !finished.has_value();
}

Task<bool> mpsc_sender_counts_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  const bool initial =
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0;

  bool expanded = false;
  bool after_upgrade = false;
  {
    auto clone = sender;
    auto weak = sender.downgrade();
    auto weak_clone = weak;
    auto upgraded = weak.upgrade();
    expanded = upgraded.has_value() && sender.strong_count() == 3 &&
               sender.weak_count() == 2 &&
               receiver.sender_strong_count() == 3 &&
               receiver.sender_weak_count() == 2;
    upgraded.reset();
    after_upgrade = sender.strong_count() == 2 &&
                    weak.strong_count() == 2 && weak.weak_count() == 2;
  }

  co_return initial && expanded && after_upgrade &&
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0;
}

Task<bool> mpsc_error_format_case() {
  using cio::sync::mpsc::error::TryRecvError;

  auto [send_sender, send_receiver] =
      cio::sync::mpsc::channel<int>(1);
  std::optional<cio::sync::mpsc::Receiver<int>> dropped_receiver{
      std::move(send_receiver)};
  dropped_receiver.reset();
  auto send_result = co_await send_sender.send(41);
  if (send_result.has_value()) {
    co_return false;
  }
  std::ostringstream send_display;
  send_display << send_result.error();

  auto [reserve_sender, reserve_receiver] =
      cio::sync::mpsc::channel<int>(1);
  reserve_receiver.close();
  auto reserve_result = co_await reserve_sender.reserve();
  if (reserve_result.has_value()) {
    co_return false;
  }
  std::ostringstream reserve_display;
  reserve_display << reserve_result.error();

  auto [try_sender, try_receiver] =
      cio::sync::mpsc::channel<int>(1);
  if (!try_sender.try_send(1).has_value()) {
    co_return false;
  }
  auto full = try_sender.try_send(2);
  try_receiver.close();
  auto closed = try_sender.try_send(3);
  if (full.has_value() || closed.has_value()) {
    co_return false;
  }
  std::ostringstream full_display;
  full_display << full.error();
  std::ostringstream closed_display;
  closed_display << closed.error();

  auto [recv_sender, recv_receiver] =
      cio::sync::mpsc::channel<int>(1);
  auto empty = recv_receiver.try_recv();
  std::optional<cio::sync::mpsc::Sender<int>> dropped_sender{
      std::move(recv_sender)};
  dropped_sender.reset();
  auto disconnected = recv_receiver.try_recv();
  if (empty.has_value() || disconnected.has_value()) {
    co_return false;
  }
  std::ostringstream empty_display;
  empty_display << empty.error();
  std::ostringstream disconnected_display;
  disconnected_display << disconnected.error();

  co_return send_result.error().value() == 41 &&
      send_result.error().debug_string() == "SendError { .. }" &&
      send_display.str() == "channel closed" &&
      reserve_result.error().debug_string() == "SendError { .. }" &&
      reserve_display.str() == "channel closed" &&
      full.error().debug_string() == "\"Full(..)\"" &&
      full_display.str() == "no available capacity" &&
      closed.error().debug_string() == "\"Closed(..)\"" &&
      closed_display.str() == "channel closed" &&
      cio::sync::mpsc::error::debug_string(empty.error()) == "Empty" &&
      empty_display.str() == "receiving on an empty channel" &&
      cio::sync::mpsc::error::debug_string(disconnected.error()) ==
          "Disconnected" &&
      disconnected_display.str() == "receiving on a closed channel";
}

Task<bool>
mpsc_marked_closed(cio::sync::mpsc::Sender<int> sender,
                   std::shared_ptr<std::atomic<bool>> entered,
                   std::shared_ptr<std::atomic<bool>> completed = {}) {
  entered->store(true, std::memory_order_release);
  co_await sender.closed();
  if (completed) {
    completed->store(true, std::memory_order_release);
  }
  co_return sender.is_closed();
}

Task<bool>
mpsc_marked_reserve_owned(cio::sync::mpsc::Sender<int> sender,
                          std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  auto reserved = co_await std::move(sender).reserve_owned();
  if (!reserved.has_value()) {
    co_return false;
  }
  auto permit = std::move(reserved).value();
  auto returned = std::move(permit).release();
  co_return !returned.is_closed();
}

Task<bool> mpsc_closed_wakes_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  const auto first_entered = std::make_shared<std::atomic<bool>>(false);
  const auto second_entered = std::make_shared<std::atomic<bool>>(false);
  const auto first_completed = std::make_shared<std::atomic<bool>>(false);
  const auto second_completed = std::make_shared<std::atomic<bool>>(false);
  auto first = cio::task::spawn(mpsc_marked_closed(
      sender, first_entered, first_completed));
  auto second = cio::task::spawn(mpsc_marked_closed(
      sender, second_entered, second_completed));
  while (!first_entered->load(std::memory_order_acquire) ||
         !second_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool pending =
      !first_completed->load(std::memory_order_acquire) &&
      !second_completed->load(std::memory_order_acquire) &&
      !sender.is_closed();

  receiver.close();
  const auto first_joined = co_await first;
  const auto second_joined = co_await second;
  co_await sender.closed();
  co_return pending && first_joined.has_value() && first_joined.value() &&
      second_joined.has_value() && second_joined.value() &&
      sender.is_closed();
}

Task<bool> mpsc_closed_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto cancelled =
      cio::task::spawn(mpsc_marked_closed(sender, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  cancelled.abort();
  const auto cancelled_result = co_await cancelled;
  if (cancelled_result.has_value() ||
      !cancelled_result.error().is_cancelled() || sender.is_closed()) {
    co_return false;
  }

  const auto retry_entered = std::make_shared<std::atomic<bool>>(false);
  auto retry =
      cio::task::spawn(mpsc_marked_closed(sender, retry_entered));
  while (!retry_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  receiver.close();
  const auto retried = co_await retry;
  co_return retried.has_value() && retried.value() && sender.is_closed();
}

Task<bool> mpsc_same_channel_case() {
  auto [first, first_receiver] =
      cio::sync::mpsc::channel<int>(1);
  auto clone = first;
  auto [other, other_receiver] =
      cio::sync::mpsc::channel<int>(1);
  co_return first.same_channel(clone) && clone.same_channel(first) &&
      !first.same_channel(other) && !other.same_channel(clone);
}

Task<bool> mpsc_receiver_len_empty_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(3);
  const bool initial = receiver.is_empty() && receiver.len() == 0;
  auto reserved = sender.try_reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::Permit<int>> unpublished{
      std::move(reserved).value()};
  const bool reservation_is_not_message =
      receiver.is_empty() && receiver.len() == 0;
  unpublished.reset();
  if (!sender.try_send(1).has_value() ||
      !sender.try_send(2).has_value()) {
    co_return false;
  }
  const bool buffered = !receiver.is_empty() && receiver.len() == 2;
  const auto first = co_await receiver.recv();
  const bool one_left = !receiver.is_empty() && receiver.len() == 1;
  const auto second = co_await receiver.recv();
  const bool drained = receiver.is_empty() && receiver.len() == 0;
  receiver.close();
  const auto finished = co_await receiver.recv();
  co_return initial && reservation_is_not_message && buffered &&
      first == std::optional<int>{1} && one_left &&
      second == std::optional<int>{2} && drained && !finished.has_value() &&
      receiver.is_empty() && receiver.len() == 0;
}

Task<bool> mpsc_try_reserve_errors_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto first = sender.try_reserve();
  if (!first.has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::Permit<int>> permit{
      std::move(first).value()};
  auto full = sender.try_reserve();
  const bool held = sender.capacity() == 0 && !full.has_value() &&
                    full.error().is_full();
  permit.reset();
  const bool restored = sender.capacity() == 1;
  receiver.close();
  auto closed = sender.try_reserve();
  co_return held && restored && !closed.has_value() &&
      closed.error().is_closed();
}

Task<bool> mpsc_owned_permit_send_release_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  auto first_sender = sender;
  auto first_reserved =
      co_await std::move(first_sender).reserve_owned();
  if (!first_reserved.has_value()) {
    co_return false;
  }
  auto first_permit = std::move(first_reserved).value();
  auto returned = std::move(first_permit).send(10);
  const bool send_returned_sender =
      returned.same_channel(sender) && receiver.len() == 1 &&
      sender.capacity() == 1;

  auto second_reserved =
      co_await std::move(returned).reserve_owned();
  if (!second_reserved.has_value()) {
    co_return false;
  }
  auto second_permit = std::move(second_reserved).value();
  auto returned_again = std::move(second_permit).release();
  const bool release_restored =
      returned_again.same_channel(sender) && sender.capacity() == 1 &&
      receiver.len() == 1;
  const auto second_sent = returned_again.try_send(11);
  const auto first = co_await receiver.recv();
  const auto second = co_await receiver.recv();
  co_return send_returned_sender && release_restored &&
      second_sent.has_value() && first == std::optional<int>{10} &&
      second == std::optional<int>{11} && receiver.is_empty();
}

Task<bool> mpsc_owned_permit_same_channel_case() {
  auto [first_sender, first_receiver] =
      cio::sync::mpsc::channel<int>(2);
  auto [other_sender, other_receiver] =
      cio::sync::mpsc::channel<int>(1);

  auto first_owner = first_sender;
  auto first_reserved =
      co_await std::move(first_owner).reserve_owned();
  auto second_owner = first_sender;
  auto second_reserved =
      co_await std::move(second_owner).reserve_owned();
  auto other_owner = other_sender;
  auto other_reserved =
      co_await std::move(other_owner).reserve_owned();
  if (!first_reserved.has_value() || !second_reserved.has_value() ||
      !other_reserved.has_value()) {
    co_return false;
  }

  auto first = std::move(first_reserved).value();
  auto second = std::move(second_reserved).value();
  auto other = std::move(other_reserved).value();
  co_return first.same_channel(second) && second.same_channel(first) &&
      !first.same_channel(other) &&
      first.same_channel_as_sender(first_sender) &&
      !first.same_channel_as_sender(other_sender) &&
      other.same_channel_as_sender(other_sender);
}

Task<bool> mpsc_owned_permit_lifetime_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto weak = sender.downgrade();
  auto reserved = co_await std::move(sender).reserve_owned();
  if (!reserved.has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::OwnedPermit<int>> permit{
      std::move(reserved).value()};
  const bool held_open = weak.strong_count() == 1 &&
                         weak.weak_count() == 1 &&
                         !receiver.is_closed();
  permit.reset();
  const bool closed =
      weak.strong_count() == 0 && !weak.upgrade().has_value();
  const auto finished = co_await receiver.recv();
  co_return held_open && closed && !finished.has_value();
}

Task<bool> mpsc_reserve_owned_closed_consumes_sender_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto witness = sender;
  receiver.close();
  auto failed = co_await std::move(sender).reserve_owned();
  if (failed.has_value()) {
    co_return false;
  }
  std::ostringstream display;
  display << failed.error();
  co_return witness.strong_count() == 1 && witness.is_closed() &&
      failed.error().debug_string() == "SendError { .. }" &&
      display.str() == "channel closed";
}

Task<bool> mpsc_try_reserve_owned_errors_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }
  auto candidate = sender;
  auto full = std::move(candidate).try_reserve_owned();
  if (full.has_value() || !full.error().is_full()) {
    co_return false;
  }
  auto returned = std::move(full).error().into_inner();
  const bool full_returned_sender = returned.same_channel(sender);
  const auto first = co_await receiver.recv();
  if (first != std::optional<int>{1} ||
      !returned.try_send(2).has_value()) {
    co_return false;
  }
  const auto second = co_await receiver.recv();

  auto [closed_sender, closed_receiver] =
      cio::sync::mpsc::channel<int>(1);
  auto witness = closed_sender;
  closed_receiver.close();
  auto closed = std::move(closed_sender).try_reserve_owned();
  if (closed.has_value() || !closed.error().is_closed()) {
    co_return false;
  }
  auto returned_closed = std::move(closed).error().into_inner();
  co_return full_returned_sender && second == std::optional<int>{2} &&
      returned_closed.same_channel(witness) &&
      returned_closed.is_closed();
}

Task<bool> mpsc_reserve_owned_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending =
      cio::task::spawn(mpsc_marked_reserve_owned(sender, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool pending_owns_one_sender = sender.strong_count() == 2;
  pending.abort();
  const auto cancelled = co_await pending;

  const auto first = co_await receiver.recv();
  const auto retry = sender.try_send(3);
  const auto third = co_await receiver.recv();
  co_return !cancelled.has_value() &&
      cancelled.error().is_cancelled() && pending_owns_one_sender &&
      sender.strong_count() == 1 && first == std::optional<int>{1} &&
      retry.has_value() && third == std::optional<int>{3};
}

Task<bool> mpsc_unbounded_fifo_multi_sender_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto second = sender;

  if (!sender.send(1).has_value() || !second.send(2).has_value() ||
      !sender.send(3).has_value() || !second.send(4).has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::UnboundedSender<int>> first_owner{
      std::move(sender)};
  std::optional<cio::sync::mpsc::UnboundedSender<int>> second_owner{
      std::move(second)};
  first_owner.reset();
  second_owner.reset();

  const auto first = co_await receiver.recv();
  const auto second_value = co_await receiver.recv();
  const auto third = co_await receiver.recv();
  const auto fourth = co_await receiver.recv();
  const auto finished = co_await receiver.recv();
  co_return first == std::optional<int>{1} &&
      second_value == std::optional<int>{2} &&
      third == std::optional<int>{3} && fourth == std::optional<int>{4} &&
      !finished.has_value();
}

Task<bool> mpsc_unbounded_send_try_errors_case() {
  using cio::sync::mpsc::error::TryRecvError;

  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  const auto empty = receiver.try_recv();
  if (!sender.send(7).has_value()) {
    co_return false;
  }
  const auto received = receiver.try_recv();
  const auto empty_again = receiver.try_recv();
  std::optional<cio::sync::mpsc::UnboundedSender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  const auto disconnected = receiver.try_recv();

  auto [closed_sender, closed_receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  std::optional<cio::sync::mpsc::UnboundedReceiver<int>> receiver_owner{
      std::move(closed_receiver)};
  receiver_owner.reset();
  const auto closed = closed_sender.send(41);
  if (closed.has_value()) {
    co_return false;
  }
  std::ostringstream closed_display;
  closed_display << closed.error();

  co_return !empty.has_value() &&
      empty.error() == TryRecvError::empty && received.has_value() &&
      received.value() == 7 && !empty_again.has_value() &&
      empty_again.error() == TryRecvError::empty &&
      !disconnected.has_value() &&
      disconnected.error() == TryRecvError::disconnected &&
      closed.error().value() == 41 &&
      closed.error().debug_string() == "SendError { .. }" &&
      closed_display.str() == "channel closed";
}

Task<bool> mpsc_unbounded_close_drain_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  if (!sender.send(1).has_value() || !sender.send(2).has_value()) {
    co_return false;
  }
  receiver.close();
  const auto rejected = sender.send(3);
  const auto first = co_await receiver.recv();
  const auto second = co_await receiver.recv();
  const auto finished = co_await receiver.recv();

  co_return sender.is_closed() && !rejected.has_value() &&
      rejected.error().value() == 3 &&
      first == std::optional<int>{1} &&
      second == std::optional<int>{2} && !finished.has_value() &&
      receiver.is_closed();
}

Task<bool> mpsc_unbounded_receiver_drop_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  if (!sender.send(7).has_value()) {
    co_return false;
  }
  std::optional<cio::sync::mpsc::UnboundedReceiver<int>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  const auto rejected = sender.send(8);
  co_return !rejected.has_value() && rejected.error().value() == 8 &&
      sender.is_closed();
}

Task<bool> mpsc_unbounded_marked_closed(
    cio::sync::mpsc::UnboundedSender<int> sender,
    std::shared_ptr<std::atomic<bool>> entered,
    std::shared_ptr<std::atomic<bool>> completed = {}) {
  entered->store(true, std::memory_order_release);
  co_await sender.closed();
  if (completed) {
    completed->store(true, std::memory_order_release);
  }
  co_return sender.is_closed();
}

Task<bool> mpsc_unbounded_closed_wakes_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  const auto first_entered = std::make_shared<std::atomic<bool>>(false);
  const auto second_entered = std::make_shared<std::atomic<bool>>(false);
  const auto first_completed = std::make_shared<std::atomic<bool>>(false);
  const auto second_completed = std::make_shared<std::atomic<bool>>(false);
  auto first = cio::task::spawn(mpsc_unbounded_marked_closed(
      sender, first_entered, first_completed));
  auto second = cio::task::spawn(mpsc_unbounded_marked_closed(
      sender, second_entered, second_completed));
  while (!first_entered->load(std::memory_order_acquire) ||
         !second_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool pending =
      !first_completed->load(std::memory_order_acquire) &&
      !second_completed->load(std::memory_order_acquire) &&
      !sender.is_closed();

  receiver.close();
  const auto first_joined = co_await first;
  const auto second_joined = co_await second;
  co_await sender.closed();
  co_return pending && first_joined.has_value() && first_joined.value() &&
      second_joined.has_value() && second_joined.value() &&
      sender.is_closed();
}

Task<bool> mpsc_unbounded_closed_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto cancelled =
      cio::task::spawn(mpsc_unbounded_marked_closed(sender, entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  cancelled.abort();
  const auto cancelled_result = co_await cancelled;
  if (cancelled_result.has_value() ||
      !cancelled_result.error().is_cancelled() || sender.is_closed()) {
    co_return false;
  }

  const auto retry_entered = std::make_shared<std::atomic<bool>>(false);
  auto retry =
      cio::task::spawn(mpsc_unbounded_marked_closed(sender, retry_entered));
  while (!retry_entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  receiver.close();
  const auto retried = co_await retry;
  co_return retried.has_value() && retried.value() && sender.is_closed();
}

Task<bool> mpsc_unbounded_last_sender_weak_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  std::optional<cio::sync::mpsc::UnboundedSender<int>> strong{
      std::move(sender)};
  auto weak = strong->downgrade();
  std::optional<cio::sync::mpsc::UnboundedSender<int>> clone{*strong};
  clone.reset();
  strong.reset();

  const bool cannot_revive = !weak.upgrade().has_value() &&
                             weak.strong_count() == 0 &&
                             weak.weak_count() == 1;
  const auto finished = co_await receiver.recv();
  co_return cannot_revive && !finished.has_value();
}

Task<bool> mpsc_unbounded_same_channel_counts_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto [other, other_receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  const bool initial =
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0 &&
      !sender.same_channel(other);

  bool expanded = false;
  bool after_upgrade = false;
  {
    auto clone = sender;
    auto weak = sender.downgrade();
    auto weak_clone = weak;
    auto upgraded = weak.upgrade();
    expanded = sender.same_channel(clone) && upgraded.has_value() &&
               sender.same_channel(*upgraded) &&
               sender.strong_count() == 3 &&
               sender.weak_count() == 2 &&
               receiver.sender_strong_count() == 3 &&
               receiver.sender_weak_count() == 2;
    upgraded.reset();
    after_upgrade = sender.strong_count() == 2 &&
                    weak.strong_count() == 2 && weak.weak_count() == 2;
  }

  co_return initial && expanded && after_upgrade &&
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0;
}

Task<bool> mpsc_unbounded_receiver_len_empty_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  const bool initial = receiver.is_empty() && receiver.len() == 0;
  if (!sender.send(1).has_value() || !sender.send(2).has_value()) {
    co_return false;
  }
  const bool buffered = !receiver.is_empty() && receiver.len() == 2;
  const auto first = co_await receiver.recv();
  const bool one_left = !receiver.is_empty() && receiver.len() == 1;
  const auto second = co_await receiver.recv();
  const bool drained = receiver.is_empty() && receiver.len() == 0;
  std::optional<cio::sync::mpsc::UnboundedSender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  const auto finished = co_await receiver.recv();

  co_return initial && buffered && first == std::optional<int>{1} &&
      one_left && second == std::optional<int>{2} && drained &&
      !finished.has_value() && receiver.is_empty() && receiver.len() == 0;
}

Task<bool> mpsc_unbounded_ready_recv_budget_case() {
  constexpr std::size_t bound = 512;
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  for (std::size_t value = 1; value <= bound; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }
  std::optional<cio::sync::mpsc::UnboundedSender<std::size_t>>
      sender_owner{std::move(sender)};
  sender_owner.reset();

  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(set_flag(peer_ran));
  std::size_t peer_iteration = 0;
  std::size_t checksum = 0;
  for (std::size_t iteration = 1; iteration <= bound; ++iteration) {
    const auto received = co_await receiver.recv();
    if (!received.has_value()) {
      co_return false;
    }
    checksum += received.value();
    if (peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      peer_iteration = iteration;
    }
  }
  const auto finished = co_await receiver.recv();
  const auto peer_joined = co_await peer;
  const auto expected = bound * (bound + 1) / 2;
  co_return peer_joined.has_value() && peer_iteration != 0 &&
      peer_iteration <= bound && checksum == expected &&
      !finished.has_value();
}

struct MpscUnboundedDropProbe final {
  std::shared_ptr<std::atomic<std::size_t>> drops;
  bool active{true};

  explicit MpscUnboundedDropProbe(
      std::shared_ptr<std::atomic<std::size_t>> counter)
      : drops{std::move(counter)} {}

  MpscUnboundedDropProbe(const MpscUnboundedDropProbe &) = delete;
  MpscUnboundedDropProbe &
  operator=(const MpscUnboundedDropProbe &) = delete;

  MpscUnboundedDropProbe(MpscUnboundedDropProbe &&other) noexcept
      : drops{std::move(other.drops)},
        active{std::exchange(other.active, false)} {}

  MpscUnboundedDropProbe &
  operator=(MpscUnboundedDropProbe &&) = delete;

  ~MpscUnboundedDropProbe() {
    if (active) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

Task<bool> mpsc_unbounded_value_drop_once_case() {
  const auto drops =
      std::make_shared<std::atomic<std::size_t>>(0);
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<MpscUnboundedDropProbe>();
  for (std::size_t index = 0; index < 3; ++index) {
    if (!sender.send(MpscUnboundedDropProbe{drops}).has_value()) {
      co_return false;
    }
  }

  auto first = co_await receiver.recv();
  if (!first.has_value()) {
    co_return false;
  }
  first.reset();
  const bool first_dropped =
      drops->load(std::memory_order_relaxed) == 1;
  std::optional<
      cio::sync::mpsc::UnboundedReceiver<MpscUnboundedDropProbe>>
      receiver_owner{std::move(receiver)};
  receiver_owner.reset();
  const bool buffered_dropped =
      drops->load(std::memory_order_relaxed) == 3;

  bool rejected_still_owned = false;
  {
    const auto rejected =
        sender.send(MpscUnboundedDropProbe{drops});
    rejected_still_owned =
        !rejected.has_value() &&
        drops->load(std::memory_order_relaxed) == 3;
  }

  co_return first_dropped && buffered_dropped &&
      rejected_still_owned &&
      drops->load(std::memory_order_relaxed) == 4;
}

Task<bool> mpsc_unbounded_weak_upgrade_closed_case() {
  auto [closed_sender, closed_receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto closed_weak = closed_sender.downgrade();
  closed_receiver.close();
  auto closed_upgraded = closed_weak.upgrade();
  if (!closed_upgraded.has_value()) {
    co_return false;
  }
  const auto close_rejected = closed_upgraded->send(11);
  const bool close_preserves_strong =
      closed_sender.strong_count() == 2 &&
      closed_sender.same_channel(*closed_upgraded) &&
      closed_upgraded->is_closed() && !close_rejected.has_value() &&
      close_rejected.error().value() == 11;

  auto [dropped_sender, dropped_receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto dropped_weak = dropped_sender.downgrade();
  std::optional<cio::sync::mpsc::UnboundedReceiver<int>>
      dropped_receiver_owner{std::move(dropped_receiver)};
  dropped_receiver_owner.reset();
  auto dropped_upgraded = dropped_weak.upgrade();
  if (!dropped_upgraded.has_value()) {
    co_return false;
  }
  const auto drop_rejected = dropped_upgraded->send(12);
  const bool drop_preserves_strong =
      dropped_sender.strong_count() == 2 &&
      dropped_sender.same_channel(*dropped_upgraded) &&
      dropped_upgraded->is_closed() && !drop_rejected.has_value() &&
      drop_rejected.error().value() == 12;

  co_return close_preserves_strong && drop_preserves_strong;
}

Task<std::optional<int>> mpsc_unbounded_marked_recv(
    Task<std::optional<int>> operation,
    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_return co_await std::move(operation);
}

Task<bool> mpsc_unbounded_recv_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending = cio::task::spawn(
      mpsc_unbounded_marked_recv(receiver.recv(), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  pending.abort();
  const auto cancelled = co_await pending;
  if (cancelled.has_value() ||
      !cancelled.error().is_cancelled() ||
      !sender.send(29).has_value()) {
    co_return false;
  }
  const auto received = co_await receiver.recv();
  co_return received == std::optional<int>{29};
}

Task<bool> mpsc_unbounded_noncoop_send_closed_case() {
  constexpr std::size_t bound = 512;

  const auto send_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto send_peer = cio::task::spawn(set_flag(send_peer_ran));
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  for (std::size_t value = 0; value < bound; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }
  const bool send_did_not_yield =
      !send_peer_ran->load(std::memory_order_acquire);
  const auto send_peer_joined = co_await send_peer;
  std::optional<cio::sync::mpsc::UnboundedReceiver<std::size_t>>
      receiver_owner{std::move(receiver)};
  receiver_owner.reset();

  auto [closed_sender, closed_receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  closed_receiver.close();
  const auto closed_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto closed_peer = cio::task::spawn(set_flag(closed_peer_ran));
  for (std::size_t iteration = 0; iteration < bound; ++iteration) {
    co_await closed_sender.closed();
  }
  const bool closed_did_not_yield =
      !closed_peer_ran->load(std::memory_order_acquire);
  const auto closed_peer_joined = co_await closed_peer;

  co_return send_did_not_yield && send_peer_joined.has_value() &&
      closed_did_not_yield && closed_peer_joined.has_value();
}

[[nodiscard]] bool watch_changed_is(
    const cio::Result<bool, cio::sync::watch::error::RecvError> &result,
    bool expected) {
  return result.has_value() && result.value() == expected;
}

Task<bool> watch_initial_borrow_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(7);
  const auto receiver_snapshot = receiver.borrow();
  const auto sender_snapshot = sender.borrow();
  co_return receiver_snapshot.value() == 7 &&
      !receiver_snapshot.has_changed() && sender_snapshot.value() == 7 &&
      !sender_snapshot.has_changed() &&
      watch_changed_is(receiver.has_changed(), false);
}

Task<bool> watch_send_changed_borrow_update_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(1);
  if (!sender.send(2).has_value()) {
    co_return false;
  }
  const auto borrowed = receiver.borrow();
  const bool borrow_does_not_mark =
      borrowed.value() == 2 && borrowed.has_changed() &&
      watch_changed_is(receiver.has_changed(), true);
  const auto changed = co_await receiver.changed();
  if (!changed.has_value() ||
      !watch_changed_is(receiver.has_changed(), false) ||
      !sender.send(3).has_value()) {
    co_return false;
  }
  const auto updated = receiver.borrow_and_update();
  co_return borrow_does_not_mark && updated.value() == 3 &&
      updated.has_changed() &&
      watch_changed_is(receiver.has_changed(), false);
}

Task<bool> watch_marks_and_has_changed_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(5);
  receiver.mark_changed();
  if (!watch_changed_is(receiver.has_changed(), true) ||
      !(co_await receiver.changed()).has_value()) {
    co_return false;
  }
  receiver.mark_changed();
  receiver.mark_unchanged();
  const bool unchanged = watch_changed_is(receiver.has_changed(), false);
  if (!sender.send(6).has_value()) {
    co_return false;
  }
  const bool unseen_before_close =
      watch_changed_is(receiver.has_changed(), true);
  std::optional<cio::sync::watch::Sender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  co_return unchanged && unseen_before_close &&
      !receiver.has_changed().has_value();
}

Task<bool> watch_independent_receivers_subscribe_case() {
  auto [sender, receiver_one] = cio::sync::watch::channel<int>(10);
  auto receiver_two = receiver_one;
  if (!sender.send(11).has_value() ||
      !(co_await receiver_one.changed()).has_value()) {
    co_return false;
  }
  const bool independent_seen =
      watch_changed_is(receiver_one.has_changed(), false) &&
      watch_changed_is(receiver_two.has_changed(), true) &&
      receiver_one.same_channel(receiver_two);
  auto subscribed = sender.subscribe();
  const auto snapshot = subscribed.borrow();
  co_return independent_seen && snapshot.value() == 11 &&
      watch_changed_is(subscribed.has_changed(), false) &&
      subscribed.same_channel(receiver_two);
}

Task<bool> watch_last_sender_close_retains_value_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(0);
  if (!sender.send(9).has_value()) {
    co_return false;
  }
  std::optional<cio::sync::watch::Sender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  const bool unseen_delivered =
      (co_await receiver.changed()).has_value();
  const auto retained = receiver.borrow();
  const bool closed_after_seen =
      !(co_await receiver.changed()).has_value();
  co_return unseen_delivered && retained.value() == 9 &&
      closed_after_seen;
}

Task<bool> watch_last_receiver_closes_sender_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(3);
  std::optional<cio::sync::watch::Receiver<int>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  const bool closed_now = sender.is_closed();
  co_await sender.closed();
  const auto rejected = sender.send(4);
  const bool send_error_preserves_value =
      !rejected.has_value() && rejected.error().value() == 4;

  auto subscribed = sender.subscribe();
  const bool sent_reopened = sender.send(5).has_value();
  const auto reopened_snapshot = subscribed.borrow();
  const bool reopened = !sender.is_closed() && sent_reopened &&
                        reopened_snapshot.value() == 5 &&
                        watch_changed_is(subscribed.has_changed(), true);
  co_return closed_now && send_error_preserves_value && reopened;
}

Task<cio::Result<void, cio::sync::watch::error::RecvError>>
watch_marked_changed(
    Task<cio::Result<void, cio::sync::watch::error::RecvError>> operation,
    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_return co_await std::move(operation);
}

Task<bool> watch_changed_cancel_safe_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(0);
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto pending =
      cio::task::spawn(watch_marked_changed(receiver.changed(), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  pending.abort();
  const auto cancelled = co_await pending;
  if (cancelled.has_value() ||
      !cancelled.error().is_cancelled() ||
      !sender.send(1).has_value()) {
    co_return false;
  }
  const auto changed = co_await receiver.changed();
  const auto snapshot = receiver.borrow();
  co_return changed.has_value() && snapshot.value() == 1 &&
      watch_changed_is(receiver.has_changed(), false);
}

Task<bool> watch_same_channel_counts_case() {
  auto [sender_one, receiver_one] =
      cio::sync::watch::channel<int>(1);
  auto sender_two = sender_one;
  auto receiver_two = receiver_one;
  auto subscribed = sender_one.subscribe();
  auto [other_sender, other_receiver] =
      cio::sync::watch::channel<int>(1);
  co_return sender_one.sender_count() == 2 &&
      sender_two.sender_count() == 2 &&
      sender_one.receiver_count() == 3 &&
      sender_one.same_channel(sender_two) &&
      !sender_one.same_channel(other_sender) &&
      receiver_one.same_channel(receiver_two) &&
      receiver_one.same_channel(subscribed) &&
      !receiver_one.same_channel(other_receiver);
}

Task<bool> watch_send_replace_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(1);
  const auto old = sender.send_replace(2);
  const bool first_changed =
      watch_changed_is(receiver.has_changed(), true);
  const auto first_snapshot = receiver.borrow_and_update();
  const bool first_replace =
      old == 1 && first_changed && first_snapshot.value() == 2;
  const auto second_old = sender.send_replace(3);
  const auto second_snapshot = receiver.borrow();
  co_return first_replace && second_old == 2 &&
      watch_changed_is(receiver.has_changed(), true) &&
      second_snapshot.value() == 3;
}

Task<bool> watch_wait_for_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(2);
  auto immediate =
      co_await receiver.wait_for([](const int &value) {
        return value == 2;
      });
  if (!immediate.has_value() || immediate.value().value() != 2 ||
      !sender.send(3).has_value() || !sender.send(4).has_value()) {
    co_return false;
  }
  auto latest =
      co_await receiver.wait_for([](const int &value) {
        return value >= 4;
      });
  if (!latest.has_value() || latest.value().value() != 4 ||
      !latest.value().has_changed()) {
    co_return false;
  }
  std::optional<cio::sync::watch::Sender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  auto closed_true =
      co_await receiver.wait_for([](const int &value) {
        return value == 4;
      });
  auto closed =
      co_await receiver.wait_for([](const int &value) {
        return value == 99;
      });
  co_return closed_true.has_value() &&
      closed_true.value().value() == 4 && !closed.has_value();
}

struct WatchDropProbe final {
  std::shared_ptr<std::atomic<std::size_t>> drops;
  std::shared_ptr<std::atomic<std::size_t>> copies;
  int value{0};
  bool active{true};

  WatchDropProbe(std::shared_ptr<std::atomic<std::size_t>> drop_counter,
                 std::shared_ptr<std::atomic<std::size_t>> copy_counter,
                 int initial)
      : drops{std::move(drop_counter)}, copies{std::move(copy_counter)},
        value{initial} {}

  WatchDropProbe(const WatchDropProbe &other)
      : drops{other.drops}, copies{other.copies}, value{other.value} {
    copies->fetch_add(1, std::memory_order_relaxed);
  }

  WatchDropProbe &operator=(const WatchDropProbe &) = delete;

  WatchDropProbe(WatchDropProbe &&other) noexcept
      : drops{std::move(other.drops)}, copies{std::move(other.copies)},
        value{other.value}, active{std::exchange(other.active, false)} {}

  WatchDropProbe &operator=(WatchDropProbe &&) = delete;

  ~WatchDropProbe() {
    if (active) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

Task<bool> watch_value_drop_and_clone_case() {
  const auto drops =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto copies =
      std::make_shared<std::atomic<std::size_t>>(0);
  {
    auto [sender, receiver] = cio::sync::watch::channel<WatchDropProbe>(
        WatchDropProbe{drops, copies, 1});
    auto sender_two = sender;
    auto receiver_two = receiver;
    auto subscribed = sender.subscribe();
    const auto initial_snapshot = receiver.borrow();
    auto old =
        sender.send_replace(WatchDropProbe{drops, copies, 2});
    const bool sent_third =
        sender.send(WatchDropProbe{drops, copies, 3}).has_value();
    const auto receiver_snapshot = receiver.borrow();
    const auto receiver_two_snapshot = receiver_two.borrow();
    const auto subscribed_snapshot = subscribed.borrow();
    if (initial_snapshot.value().value != 1 || old.value != 1 ||
        !sent_third ||
        receiver_snapshot.value().value != 3 ||
        receiver_two_snapshot.value().value != 3 ||
        subscribed_snapshot.value().value != 3) {
      co_return false;
    }
  }
  co_return drops->load(std::memory_order_relaxed) ==
      copies->load(std::memory_order_relaxed) + 3;
}

Task<bool> watch_error_format_case() {
  auto [sender, receiver] = cio::sync::watch::channel<int>(1);
  std::optional<cio::sync::watch::Receiver<int>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  const auto send_error = sender.send(2);
  const bool send_format =
      !send_error.has_value() &&
      send_error.error().message() == "channel closed" &&
      send_error.error().debug_string() == "SendError { .. }";

  auto [closed_sender, closed_receiver] =
      cio::sync::watch::channel<int>(3);
  std::optional<cio::sync::watch::Sender<int>> sender_owner{
      std::move(closed_sender)};
  sender_owner.reset();
  const auto recv_error = co_await closed_receiver.changed();
  co_return send_format && !recv_error.has_value() &&
      recv_error.error().message() == "channel closed" &&
      recv_error.error().debug_string() == "RecvError(())";
}

Task<bool> watch_cooperative_ready_paths_case() {
  constexpr std::size_t bound = 512;

  const auto changed_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto changed_peer = cio::task::spawn(set_flag(changed_peer_ran));
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  for (std::size_t value = 1; value <= bound; ++value) {
    if (!sender.send(value).has_value() ||
        !(co_await receiver.changed()).has_value()) {
      co_return false;
    }
  }
  const bool changed_yielded =
      changed_peer_ran->load(std::memory_order_acquire);
  const auto changed_joined = co_await changed_peer;

  const auto error_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto error_peer = cio::task::spawn(set_flag(error_peer_ran));
  auto [closed_sender, closed_receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  std::optional<cio::sync::watch::Sender<std::size_t>>
      closed_sender_owner{std::move(closed_sender)};
  closed_sender_owner.reset();
  for (std::size_t index = 0; index < bound; ++index) {
    if ((co_await closed_receiver.changed()).has_value()) {
      co_return false;
    }
  }
  const bool error_yielded =
      error_peer_ran->load(std::memory_order_acquire);
  const auto error_joined = co_await error_peer;

  const auto closed_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto closed_peer = cio::task::spawn(set_flag(closed_peer_ran));
  auto [closed_wait_sender, closed_wait_receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  std::optional<cio::sync::watch::Receiver<std::size_t>>
      closed_receiver_owner{std::move(closed_wait_receiver)};
  closed_receiver_owner.reset();
  for (std::size_t index = 0; index < bound; ++index) {
    co_await closed_wait_sender.closed();
  }
  const bool closed_yielded =
      closed_peer_ran->load(std::memory_order_acquire);
  const auto closed_joined = co_await closed_peer;

  const auto wait_peer_ran =
      std::make_shared<std::atomic<bool>>(false);
  auto wait_peer = cio::task::spawn(set_flag(wait_peer_ran));
  auto [wait_sender, wait_receiver] =
      cio::sync::watch::channel<std::size_t>(7);
  for (std::size_t index = 0; index < bound; ++index) {
    auto result =
        co_await wait_receiver.wait_for([](const std::size_t &value) {
          return value == 7;
        });
    if (!result.has_value()) {
      co_return false;
    }
  }
  const bool wait_yielded =
      wait_peer_ran->load(std::memory_order_acquire);
  const auto wait_joined = co_await wait_peer;

  co_return changed_yielded && changed_joined.has_value() &&
      error_yielded && error_joined.has_value() && closed_yielded &&
      closed_joined.has_value() && wait_yielded &&
      wait_joined.has_value();
}

cio::task::JoinHandle<void>
watch_spawn_budget_peer(std::shared_ptr<std::atomic<bool>> flag) {
  return cio::task::spawn(cio::task::owned(
      [](std::shared_ptr<std::atomic<bool>> owned_flag) -> Task<void> {
        owned_flag->store(true, std::memory_order_release);
        co_return;
      },
      std::move(flag)));
}

Task<bool> watch_coop_changed_success_boundary_case() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    if (!sender.send(iteration).has_value() ||
        !(co_await receiver.changed()).has_value()) {
      co_return false;
    }
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 129;
}

Task<bool> watch_coop_changed_error_boundary_case() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  std::optional<cio::sync::watch::Sender<std::size_t>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    if ((co_await receiver.changed()).has_value()) {
      co_return false;
    }
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 129;
}

Task<bool> watch_coop_closed_boundary_case() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  std::optional<cio::sync::watch::Receiver<std::size_t>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    co_await sender.closed();
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 129;
}

Task<bool> watch_coop_wait_for_success_boundary_case() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(7);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    auto result =
        co_await receiver.wait_for([](const std::size_t &value) {
          return value == 7;
        });
    if (!result.has_value()) {
      co_return false;
    }
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 129;
}

Task<bool> watch_coop_wait_for_error_boundary_case() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(7);
  std::optional<cio::sync::watch::Sender<std::size_t>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 129; ++iteration) {
    auto result =
        co_await receiver.wait_for([](const std::size_t &value) {
          return value == 99;
        });
    if (result.has_value()) {
      co_return false;
    }
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 129;
}

Task<bool> watch_expect_fresh_poll_debit() {
  const auto peer_ran = std::make_shared<std::atomic<bool>>(false);
  auto peer = watch_spawn_budget_peer(peer_ran);
  std::size_t first_peer_iteration = 0;
  for (std::size_t iteration = 1; iteration <= 128; ++iteration) {
    co_await cio::task::consume_budget();
    if (first_peer_iteration == 0 &&
        peer_ran->load(std::memory_order_acquire)) {
      first_peer_iteration = iteration;
    }
  }
  const auto joined = co_await peer;
  co_return joined.has_value() && first_peer_iteration == 128;
}

Task<bool> watch_coop_changed_fresh_wake_budget_case() {
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  auto watcher = cio::task::spawn(cio::task::owned(
      [](cio::sync::watch::Receiver<std::size_t> owned_receiver,
         std::shared_ptr<std::atomic<bool>> owned_entered) -> Task<bool> {
        owned_entered->store(true, std::memory_order_release);
        if (!(co_await owned_receiver.changed()).has_value()) {
          co_return false;
        }
        co_return co_await watch_expect_fresh_poll_debit();
      },
      std::move(receiver), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  if (!sender.send(1).has_value()) {
    co_return false;
  }
  const auto joined = co_await watcher;
  co_return joined.has_value() && joined.value();
}

Task<bool> watch_coop_wait_for_fresh_wake_budget_case() {
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  auto watcher = cio::task::spawn(cio::task::owned(
      [](cio::sync::watch::Receiver<std::size_t> owned_receiver,
         std::shared_ptr<std::atomic<bool>> owned_entered) -> Task<bool> {
        owned_entered->store(true, std::memory_order_release);
        auto result =
            co_await owned_receiver.wait_for(
                [](const std::size_t &value) { return value == 1; });
        if (!result.has_value()) {
          co_return false;
        }
        co_return co_await watch_expect_fresh_poll_debit();
      },
      std::move(receiver), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  if (!sender.send(1).has_value()) {
    co_return false;
  }
  const auto joined = co_await watcher;
  co_return joined.has_value() && joined.value();
}

Task<bool> watch_coop_closed_fresh_wake_budget_case() {
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto [sender, receiver] =
      cio::sync::watch::channel<std::size_t>(0);
  auto watcher = cio::task::spawn(cio::task::owned(
      [](cio::sync::watch::Sender<std::size_t> owned_sender,
         std::shared_ptr<std::atomic<bool>> owned_entered) -> Task<bool> {
        owned_entered->store(true, std::memory_order_release);
        co_await owned_sender.closed();
        co_return co_await watch_expect_fresh_poll_debit();
      },
      std::move(sender), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  std::optional<cio::sync::watch::Receiver<std::size_t>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  const auto joined = co_await watcher;
  co_return joined.has_value() && joined.value();
}

Task<bool> broadcast_capacity_rounding_lag_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<std::size_t>(3);
  for (std::size_t value = 1; value <= 5; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }
  const auto lagged = co_await receiver.recv();
  if (lagged.has_value() || !lagged.error().is_lagged() ||
      lagged.error().lagged() != 1) {
    co_return false;
  }
  for (std::size_t expected = 2; expected <= 5; ++expected) {
    const auto received = co_await receiver.recv();
    if (!received.has_value() || received.value() != expected) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> broadcast_failed_send_then_subscribe_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<int>(4);
  std::optional<cio::sync::broadcast::Receiver<int>> receiver_owner{
      std::move(receiver)};
  receiver_owner.reset();
  const auto failed = sender.send(10);
  auto subscribed = sender.subscribe();
  const auto sent = sender.send(20);
  const auto received = co_await subscribed.recv();
  co_return !failed.has_value() && failed.error().value() == 10 &&
      sent.has_value() && sent.value() == 1 && received.has_value() &&
      received.value() == 20;
}

Task<bool> broadcast_independent_receivers_case() {
  auto [sender, first] =
      cio::sync::broadcast::channel<int>(4);
  auto second = sender.subscribe();
  const auto first_send = sender.send(10);
  const auto second_send = sender.send(20);
  if (!first_send.has_value() || first_send.value() != 2 ||
      !second_send.has_value() || second_send.value() != 2) {
    co_return false;
  }
  const auto first_one = co_await first.recv();
  const auto first_two = co_await first.recv();
  const auto second_one = co_await second.recv();
  const auto second_two = co_await second.recv();
  co_return first_one.has_value() && first_one.value() == 10 &&
      first_two.has_value() && first_two.value() == 20 &&
      second_one.has_value() && second_one.value() == 10 &&
      second_two.has_value() && second_two.value() == 20;
}

Task<bool> broadcast_resubscribe_skips_backlog_case() {
  auto [sender, original] =
      cio::sync::broadcast::channel<int>(4);
  if (!sender.send(1).has_value()) {
    co_return false;
  }
  auto resubscribed = original.resubscribe();
  const auto sent = sender.send(2);
  if (!sent.has_value() || sent.value() != 2) {
    co_return false;
  }
  const auto new_value = co_await resubscribed.recv();
  const auto old_first = co_await original.recv();
  const auto old_second = co_await original.recv();
  co_return new_value.has_value() && new_value.value() == 2 &&
      old_first.has_value() && old_first.value() == 1 &&
      old_second.has_value() && old_second.value() == 2;
}

Task<bool> broadcast_drain_then_closed_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<int>(4);
  if (!sender.send(1).has_value() ||
      !sender.send(2).has_value()) {
    co_return false;
  }
  std::optional<cio::sync::broadcast::Sender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  const auto first = co_await receiver.recv();
  const auto second = co_await receiver.recv();
  const auto closed = co_await receiver.recv();
  co_return first.has_value() && first.value() == 1 &&
      second.has_value() && second.value() == 2 &&
      !closed.has_value() && closed.error().is_closed();
}

Task<bool> broadcast_lagged_exact_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<std::size_t>(2);
  for (std::size_t value = 1; value <= 5; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }
  const auto lagged = co_await receiver.recv();
  const auto fourth = co_await receiver.recv();
  const auto fifth = co_await receiver.recv();
  co_return !lagged.has_value() && lagged.error().is_lagged() &&
      lagged.error().lagged() == 3 && fourth.has_value() &&
      fourth.value() == 4 && fifth.has_value() &&
      fifth.value() == 5;
}

Task<bool> broadcast_try_recv_empty_closed_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<int>(2);
  const auto initial = receiver.try_recv();
  if (!sender.send(7).has_value()) {
    co_return false;
  }
  const auto received = receiver.try_recv();
  const auto empty_again = receiver.try_recv();
  std::optional<cio::sync::broadcast::Sender<int>> sender_owner{
      std::move(sender)};
  sender_owner.reset();
  const auto closed = receiver.try_recv();
  co_return !initial.has_value() && initial.error().is_empty() &&
      received.has_value() && received.value() == 7 &&
      !empty_again.has_value() && empty_again.error().is_empty() &&
      !closed.has_value() && closed.error().is_closed();
}

Task<bool> broadcast_send_receiver_count_case() {
  auto [sender, first] =
      cio::sync::broadcast::channel<int>(2);
  auto second = sender.subscribe();
  const auto sent_two = sender.send(1);
  std::optional<cio::sync::broadcast::Receiver<int>> second_owner{
      std::move(second)};
  second_owner.reset();
  const auto sent_one = sender.send(2);
  std::optional<cio::sync::broadcast::Receiver<int>> first_owner{
      std::move(first)};
  first_owner.reset();
  const auto failed = sender.send(3);
  co_return sent_two.has_value() && sent_two.value() == 2 &&
      sent_one.has_value() && sent_one.value() == 1 &&
      !failed.has_value() && failed.error().value() == 3;
}

Task<bool> broadcast_counts_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<std::size_t>(4);
  const bool initial =
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      sender.receiver_count() == 1 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0;

  auto second_sender = sender;
  auto weak = sender.downgrade();
  auto weak_clone = weak;
  auto second_receiver = sender.subscribe();
  const bool expanded =
      sender.strong_count() == 2 && sender.weak_count() == 2 &&
      sender.receiver_count() == 2 &&
      receiver.sender_strong_count() == 2 &&
      receiver.sender_weak_count() == 2 &&
      weak.strong_count() == 2 && weak.weak_count() == 2;

  std::optional<cio::sync::broadcast::Sender<std::size_t>>
      second_sender_owner{std::move(second_sender)};
  second_sender_owner.reset();
  std::optional<cio::sync::broadcast::WeakSender<std::size_t>>
      weak_clone_owner{std::move(weak_clone)};
  weak_clone_owner.reset();
  std::optional<cio::sync::broadcast::WeakSender<std::size_t>>
      weak_owner{std::move(weak)};
  weak_owner.reset();
  std::optional<cio::sync::broadcast::Receiver<std::size_t>>
      second_receiver_owner{std::move(second_receiver)};
  second_receiver_owner.reset();
  const bool restored =
      sender.strong_count() == 1 && sender.weak_count() == 0 &&
      sender.receiver_count() == 1 &&
      receiver.sender_strong_count() == 1 &&
      receiver.sender_weak_count() == 0;
  co_return initial && expanded && restored;
}

Task<bool> broadcast_weak_upgrade_case() {
  auto [sender, receiver] =
      cio::sync::broadcast::channel<std::size_t>(4);
  auto weak = sender.downgrade();
  auto upgraded = weak.upgrade();
  if (!upgraded.has_value()) {
    co_return false;
  }
  const bool live = sender.strong_count() == 2 &&
                    weak.strong_count() == 2 &&
                    weak.weak_count() == 1;
  upgraded.reset();
  std::optional<cio::sync::broadcast::Sender<std::size_t>>
      sender_owner{std::move(sender)};
  sender_owner.reset();
  co_return live && weak.strong_count() == 0 &&
      weak.weak_count() == 1 && !weak.upgrade().has_value() &&
      receiver.is_closed();
}

struct BroadcastCopyProbe final {
  std::size_t value{0};
  std::shared_ptr<std::atomic<bool>> throw_once;

  BroadcastCopyProbe(
      std::size_t initial,
      std::shared_ptr<std::atomic<bool>> should_throw)
      : value{initial}, throw_once{std::move(should_throw)} {}

  BroadcastCopyProbe(const BroadcastCopyProbe &other)
      : value{other.value}, throw_once{other.throw_once} {
    if (throw_once->exchange(false, std::memory_order_acq_rel)) {
      throw std::runtime_error{
          "broadcast 差分测试的预期 copy exception"};
    }
  }

  BroadcastCopyProbe &
  operator=(const BroadcastCopyProbe &other) {
    auto replacement_flag = other.throw_once;
    if (replacement_flag->exchange(
            false, std::memory_order_acq_rel)) {
      throw std::runtime_error{
          "broadcast 差分测试的预期 copy exception"};
    }
    value = other.value;
    throw_once = std::move(replacement_flag);
    return *this;
  }

  BroadcastCopyProbe(BroadcastCopyProbe &&) noexcept = default;
  BroadcastCopyProbe &
  operator=(BroadcastCopyProbe &&) noexcept = default;
};

Task<bool> broadcast_copy_exception_advances_cursor_case() {
  const auto throw_once =
      std::make_shared<std::atomic<bool>>(true);
  auto [sender, receiver] =
      cio::sync::broadcast::channel<BroadcastCopyProbe>(4);
  if (!sender.send(BroadcastCopyProbe{1, throw_once}).has_value() ||
      !sender.send(BroadcastCopyProbe{2, throw_once}).has_value()) {
    co_return false;
  }
  bool threw = false;
  try {
    (void)(co_await receiver.recv());
  } catch (const std::runtime_error &) {
    threw = true;
  }
  const auto second = co_await receiver.recv();
  co_return threw && second.has_value() &&
      second.value().value == 2;
}

Task<bool> broadcast_recv_cooperative_ready_budget_case() {
  auto child = cio::task::spawn(cio::task::owned(
      []() -> Task<bool> {
        const auto peer_ran =
            std::make_shared<std::atomic<bool>>(false);
        auto peer = watch_spawn_budget_peer(peer_ran);
        auto [sender, receiver] =
            cio::sync::broadcast::channel<std::size_t>(256);
        for (std::size_t value = 1; value <= 129; ++value) {
          if (!sender.send(value).has_value()) {
            co_return false;
          }
        }
        std::size_t first_peer_iteration = 0;
        for (std::size_t expected = 1; expected <= 129;
             ++expected) {
          const auto received = co_await receiver.recv();
          if (!received.has_value() ||
              received.value() != expected) {
            co_return false;
          }
          if (first_peer_iteration == 0 &&
              peer_ran->load(std::memory_order_acquire)) {
            first_peer_iteration = expected;
          }
        }
        const auto joined = co_await peer;
        co_return joined.has_value() &&
            first_peer_iteration == 129;
      }));
  const auto joined = co_await child;
  co_return joined.has_value() && joined.value();
}

Task<bool> broadcast_recv_cooperative_pending_budget_case() {
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto [sender, receiver] =
      cio::sync::broadcast::channel<std::size_t>(1);
  auto watcher = cio::task::spawn(cio::task::owned(
      [](cio::sync::broadcast::Receiver<std::size_t>
             owned_receiver,
         std::shared_ptr<std::atomic<bool>> owned_entered)
          -> Task<bool> {
        owned_entered->store(true, std::memory_order_release);
        const auto received = co_await owned_receiver.recv();
        if (!received.has_value() || received.value() != 7) {
          co_return false;
        }
        co_return co_await watch_expect_fresh_poll_debit();
      },
      std::move(receiver), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  if (!sender.send(7).has_value()) {
    co_return false;
  }
  const auto joined = co_await watcher;
  co_return joined.has_value() && joined.value();
}

Task<bool> broadcast_closed_noncooperative_case() {
  auto child = cio::task::spawn(cio::task::owned(
      []() -> Task<bool> {
        const auto peer_ran =
            std::make_shared<std::atomic<bool>>(false);
        auto peer = watch_spawn_budget_peer(peer_ran);
        auto [sender, receiver] =
            cio::sync::broadcast::channel<std::size_t>(1);
        std::optional<
            cio::sync::broadcast::Receiver<std::size_t>>
            receiver_owner{std::move(receiver)};
        receiver_owner.reset();
        for (std::size_t index = 0; index < 512; ++index) {
          co_await sender.closed();
        }
        const bool did_not_yield =
            !peer_ran->load(std::memory_order_acquire);
        const auto joined = co_await peer;
        co_return did_not_yield && joined.has_value();
      }));
  const auto joined = co_await child;
  co_return joined.has_value() && joined.value();
}

bool io_readbuf_regions_clear_case() {
  auto buffer = cio::io::ReadBuf::with_capacity(5);
  const bool initial =
      buffer.filled_size() == 0 &&
      buffer.initialized_size() == 5 &&
      buffer.remaining() == 5;
  const auto bytes = io_bytes("ab");
  buffer.put_slice(std::span<const std::byte>{bytes});
  const bool filled =
      buffer.filled_snapshot() == bytes &&
      buffer.initialized_size() == 5 &&
      buffer.remaining() == 3;
  buffer.clear();
  const bool cleared =
      buffer.filled_size() == 0 &&
      buffer.initialized_size() == 5 &&
      buffer.remaining() == 5;
  buffer.set_filled(2);
  return initial && filled && cleared &&
      buffer.filled_snapshot() == bytes;
}

Task<bool> io_partial_read_eof_zero_capacity_case() {
  auto reader = cio::io::MemoryReader::from(io_buffer("abc"), 2);
  auto buffer = cio::io::ReadBuf::with_capacity(2);

  const auto first = co_await cio::io::read(reader, buffer);
  const bool first_ok =
      first.has_value() &&
      buffer.filled_snapshot() == io_bytes("ab") &&
      reader.position() == 2;
  buffer.clear();
  const auto second = co_await cio::io::read(reader, buffer);
  const bool second_ok =
      second.has_value() &&
      buffer.filled_snapshot() == io_bytes("c") &&
      reader.position() == 3;
  buffer.clear();
  const auto eof = co_await cio::io::read(reader, buffer);
  const bool eof_ok =
      eof.has_value() && buffer.filled_size() == 0 &&
      reader.remaining() == 0;

  auto zero_reader = cio::io::MemoryReader::from(io_buffer("z"));
  auto empty = cio::io::ReadBuf::with_capacity(0);
  const auto zero = co_await cio::io::read(zero_reader, empty);
  const bool zero_ok =
      zero.has_value() && empty.filled_size() == 0 &&
      zero_reader.position() == 0;
  auto remaining = cio::io::ReadBuf::with_capacity(1);
  const auto final_read =
      co_await cio::io::read(zero_reader, remaining);

  co_return first_ok && second_ok && eof_ok && zero_ok &&
      final_read.has_value() &&
      remaining.filled_snapshot() == io_bytes("z");
}

struct IoExactErrorReaderState final {
  IoExactErrorReaderState(
      cio::io::SharedBuffer input,
      bool should_block_second)
      : source{std::move(input)},
        block_second{should_block_second} {}

  mutable std::mutex mutex;
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
  cio::io::SharedBuffer source;
  std::size_t position{0};
  std::size_t calls{0};
  bool block_second{false};
  cio::sync::Notify gate;
};

class IoExactErrorReader final {
 private:
  using EndpointSession =
      cio::io::detail::EndpointSessionState<
          IoExactErrorReaderState>;

 public:
  class ReadSession final {
   public:
    static constexpr bool cio_async_read_session = true;

    ReadSession(const ReadSession&) = delete;
    ReadSession& operator=(const ReadSession&) = delete;
    ReadSession(ReadSession&&) noexcept = default;
    ReadSession& operator=(ReadSession&&) noexcept = default;
    ~ReadSession() = default;

    [[nodiscard]] Task<
        cio::io::IoResult<cio::io::MutableBufferLease>>
    read(cio::io::MutableBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<
              IoExactErrorReaderState>::acquire(session_);
      return read_impl(
          session_,
          std::move(primitive),
          std::move(buffer));
    }

   private:
    explicit ReadSession(
        std::shared_ptr<EndpointSession> session) noexcept
        : session_{std::move(session)} {}

    [[nodiscard]] static Task<
        cio::io::IoResult<cio::io::MutableBufferLease>>
    read_impl(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<
            IoExactErrorReaderState> primitive,
        cio::io::MutableBufferLease buffer) {
      const auto state = session->endpoint;
      std::size_t call = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        call = ++state->calls;
      }
      if (call == 3) {
        co_return cio::io::IoResult<
            cio::io::MutableBufferLease>::failure(
            cio::io::Error::other(
                "注入 read 错误",
                std::make_error_code(std::errc::io_error)));
      }
      if (call == 2 && state->block_second) {
        co_await state->gate.notified();
      }

      std::size_t position = 0;
      std::size_t amount = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        position = state->position;
        amount = std::min(
            {state->source.size() - position,
             buffer.remaining(),
             std::size_t{2}});
      }
      const auto input =
          state->source.subbuffer(position, amount).snapshot();
      buffer.put_slice(std::span<const std::byte>{input});
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->position += amount;
      }
      (void)primitive;
      co_return cio::io::IoResult<
          cio::io::MutableBufferLease>::success(
          std::move(buffer));
    }

    std::shared_ptr<EndpointSession> session_;

    friend class IoExactErrorReader;
  };

  static constexpr bool cio_async_read_endpoint = true;

  explicit IoExactErrorReader(
      cio::io::SharedBuffer source,
      bool block_second = false)
      : state_{std::make_shared<IoExactErrorReaderState>(
            std::move(source),
            block_second)} {}

  [[nodiscard]] ReadSession open_read_session() const {
    auto guard =
        cio::io::detail::EndpointOperationGuard<
            IoExactErrorReaderState>::acquire(state_);
    return ReadSession{
        std::make_shared<EndpointSession>(
            state_,
            std::move(guard))};
  }

  [[nodiscard]] std::size_t position() const {
    const std::lock_guard lock{state_->mutex};
    return state_->position;
  }

  [[nodiscard]] std::size_t calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->calls;
  }

  [[nodiscard]] bool active() const noexcept {
    return state_->operation_active.load(
        std::memory_order_acquire);
  }

  void wake_late() const {
    state_->gate.notify_one();
  }

 private:
  std::shared_ptr<IoExactErrorReaderState> state_;
};

struct IoExactErrorWriterState final {
  explicit IoExactErrorWriterState(bool should_block_second)
      : block_second{should_block_second} {}

  mutable std::mutex mutex;
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
  std::vector<std::byte> bytes;
  std::size_t calls{0};
  bool block_second{false};
  cio::sync::Notify gate;
};

class IoExactErrorWriter final {
 private:
  using EndpointSession =
      cio::io::detail::EndpointSessionState<
          IoExactErrorWriterState>;

 public:
  class WriteSession final {
   public:
    static constexpr bool cio_async_write_session = true;

    WriteSession(const WriteSession&) = delete;
    WriteSession& operator=(const WriteSession&) = delete;
    WriteSession(WriteSession&&) noexcept = default;
    WriteSession& operator=(WriteSession&&) noexcept = default;
    ~WriteSession() = default;

    [[nodiscard]] Task<cio::io::IoResult<std::size_t>> write(
        cio::io::ConstBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<
              IoExactErrorWriterState>::acquire(session_);
      return write_impl(
          session_,
          std::move(primitive),
          std::move(buffer));
    }

    [[nodiscard]] Task<cio::io::IoResult<void>> flush() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<
              IoExactErrorWriterState>::acquire(session_);
      return complete(session_, std::move(primitive));
    }

    [[nodiscard]] Task<cio::io::IoResult<void>> shutdown() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<
              IoExactErrorWriterState>::acquire(session_);
      return complete(session_, std::move(primitive));
    }

   private:
    explicit WriteSession(
        std::shared_ptr<EndpointSession> session) noexcept
        : session_{std::move(session)} {}

    [[nodiscard]] static Task<
        cio::io::IoResult<std::size_t>>
    write_impl(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<
            IoExactErrorWriterState> primitive,
        cio::io::ConstBufferLease buffer) {
      const auto state = session->endpoint;
      std::size_t call = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        call = ++state->calls;
      }
      if (call == 3) {
        co_return cio::io::IoResult<std::size_t>::failure(
            cio::io::Error::other(
                "注入 write 错误",
                std::make_error_code(std::errc::io_error)));
      }
      if (call == 2 && state->block_second) {
        co_await state->gate.notified();
      }

      const auto amount =
          std::min(buffer.size(), std::size_t{2});
      const auto input =
          buffer.sublease(0, amount).snapshot();
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->bytes.insert(
            state->bytes.end(),
            input.begin(),
            input.end());
      }
      (void)primitive;
      co_return cio::io::IoResult<std::size_t>::success(amount);
    }

    [[nodiscard]] static Task<cio::io::IoResult<void>> complete(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<
            IoExactErrorWriterState> primitive) {
      {
        const std::lock_guard lock{session->endpoint->mutex};
        session->endpoint_guard.require_current(
            *session->endpoint);
      }
      (void)primitive;
      co_return cio::io::IoResult<void>::success();
    }

    std::shared_ptr<EndpointSession> session_;

    friend class IoExactErrorWriter;
  };

  static constexpr bool cio_async_write_endpoint = true;

  explicit IoExactErrorWriter(bool block_second = false)
      : state_{std::make_shared<IoExactErrorWriterState>(
            block_second)} {}

  [[nodiscard]] WriteSession open_write_session() const {
    auto guard =
        cio::io::detail::EndpointOperationGuard<
            IoExactErrorWriterState>::acquire(state_);
    return WriteSession{
        std::make_shared<EndpointSession>(
            state_,
            std::move(guard))};
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const std::lock_guard lock{state_->mutex};
    return state_->bytes;
  }

  [[nodiscard]] std::size_t calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->calls;
  }

  [[nodiscard]] bool active() const noexcept {
    return state_->operation_active.load(
        std::memory_order_acquire);
  }

  void wake_late() const {
    state_->gate.notify_one();
  }

 private:
  std::shared_ptr<IoExactErrorWriterState> state_;
};

Task<bool> io_read_exact_partial_success_case() {
  auto reader = cio::io::MemoryReader::from(
      io_buffer("abcde"),
      2);
  auto output = cio::io::ReadBuf::with_capacity(5);
  const auto result =
      co_await cio::io::read_exact(reader, output);
  co_return result.has_value() && result.value() == 5 &&
      output.filled_snapshot() == io_bytes("abcde") &&
      reader.position() == 5;
}

Task<bool> io_read_exact_early_eof_case() {
  auto reader = cio::io::MemoryReader::from(
      io_buffer("abc"),
      2);
  auto output = cio::io::ReadBuf::with_capacity(5);
  const auto result =
      co_await cio::io::read_exact(reader, output);
  co_return !result.has_value() &&
      result.error().kind() == cio::io::ErrorKind::unexpected_eof &&
      output.filled_snapshot() == io_bytes("abc") &&
      reader.position() == 3;
}

Task<bool> io_read_exact_partial_error_case() {
  IoExactErrorReader reader{io_buffer("abcdef")};
  auto output = cio::io::ReadBuf::with_capacity(6);
  const auto result =
      co_await cio::io::read_exact(reader, output);
  co_return !result.has_value() &&
      result.error().kind() == cio::io::ErrorKind::other &&
      result.error().native_code() ==
          std::make_error_code(std::errc::io_error) &&
      output.filled_snapshot() == io_bytes("abcd") &&
      reader.position() == 4 && reader.calls() == 3;
}

Task<bool> io_write_all_partial_zero_case() {
  auto writer = cio::io::MemoryWriter::with_max_chunk(2);
  const auto complete =
      co_await cio::io::write_all(
          writer,
          io_buffer("abcde").lease());
  const bool partial_ok =
      complete.has_value() &&
      writer.snapshot() == io_bytes("abcde") &&
      writer.flush_count() == 0;

  auto zero = cio::io::MemoryWriter::zero_writer();
  const auto zero_result =
      co_await cio::io::write_all(
          zero,
          io_buffer("x").lease());
  co_return partial_ok && !zero_result.has_value() &&
      zero_result.error().kind() == cio::io::ErrorKind::write_zero &&
      zero.snapshot().empty() && zero.flush_count() == 0;
}

Task<bool> io_write_all_partial_error_case() {
  IoExactErrorWriter writer;
  const auto result =
      co_await cio::io::write_all(
          writer,
          io_buffer("abcdef").lease());
  co_return !result.has_value() &&
      result.error().kind() == cio::io::ErrorKind::other &&
      result.error().native_code() ==
          std::make_error_code(std::errc::io_error) &&
      writer.snapshot() == io_bytes("abcd") &&
      writer.calls() == 3;
}

Task<bool> io_exact_cancel_partial_late_wake_case() {
  IoExactErrorReader reader{io_buffer("abcd"), true};
  auto output = cio::io::ReadBuf::with_capacity(4);
  auto read_handle =
      cio::task::spawn(cio::io::read_exact(reader, output));
  co_await cio::task::yield_now();
  if (read_handle.is_finished() || reader.calls() != 2 ||
      !reader.active()) {
    co_return false;
  }
  read_handle.abort();
  const auto read_joined = co_await read_handle;
  if (read_joined.has_value() ||
      !read_joined.error().is_cancelled() ||
      reader.active() ||
      output.filled_snapshot() != io_bytes("ab")) {
    co_return false;
  }
  reader.wake_late();
  co_await cio::task::yield_now();
  if (reader.calls() != 2 ||
      output.filled_snapshot() != io_bytes("ab")) {
    co_return false;
  }

  IoExactErrorWriter writer{true};
  auto write_handle =
      cio::task::spawn(cio::io::write_all(
          writer,
          io_buffer("abcd").lease()));
  co_await cio::task::yield_now();
  if (write_handle.is_finished() || writer.calls() != 2 ||
      !writer.active() ||
      writer.snapshot() != io_bytes("ab")) {
    co_return false;
  }
  write_handle.abort();
  const auto write_joined = co_await write_handle;
  if (write_joined.has_value() ||
      !write_joined.error().is_cancelled() ||
      writer.active() ||
      writer.snapshot() != io_bytes("ab")) {
    co_return false;
  }
  writer.wake_late();
  co_await cio::task::yield_now();
  co_return writer.calls() == 2 &&
      writer.snapshot() == io_bytes("ab");
}

Task<bool> io_exact_empty_no_poll_case() {
  auto reader = cio::io::MemoryReader::from(io_buffer("x"));
  auto output = cio::io::ReadBuf::with_capacity(0);
  const auto read =
      co_await cio::io::read_exact(reader, output);
  auto writer = cio::io::MemoryWriter::with_max_chunk(1);
  const auto write =
      co_await cio::io::write_all(
          writer,
          io_buffer("").lease());
  co_return read.has_value() && read.value() == 0 &&
      write.has_value() && reader.position() == 0 &&
      writer.snapshot().empty() && writer.flush_count() == 0;
}

struct IoTraceWriterState final {
  explicit IoTraceWriterState(std::size_t maximum_write)
      : max_write{maximum_write} {}

  mutable std::mutex mutex;
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
  std::vector<std::byte> bytes;
  std::vector<std::string_view> events;
  std::size_t max_write;
  std::size_t write_calls{0};
  bool shutdown{false};
};

class IoTraceWriterSession final {
 public:
  static constexpr bool cio_async_write_session = true;

  IoTraceWriterSession(const IoTraceWriterSession&) = delete;
  IoTraceWriterSession& operator=(const IoTraceWriterSession&) = delete;
  IoTraceWriterSession(IoTraceWriterSession&&) noexcept = default;
  IoTraceWriterSession& operator=(IoTraceWriterSession&&) noexcept = default;
  ~IoTraceWriterSession() = default;

  [[nodiscard]] static IoTraceWriterSession acquire(
      const std::shared_ptr<IoTraceWriterState>& state) {
    auto endpoint_guard =
        cio::io::detail::EndpointOperationGuard<
            IoTraceWriterState>::acquire(state);
    return IoTraceWriterSession{
        std::make_shared<SessionState>(
            state,
            std::move(endpoint_guard))};
  }

  [[nodiscard]] Task<cio::io::IoResult<std::size_t>> write(
      cio::io::ConstBufferLease buffer) {
    auto primitive =
        cio::io::detail::SessionPrimitiveGuard<
            IoTraceWriterState>::acquire(session_);
    return write_impl(
        session_,
        std::move(primitive),
        std::move(buffer));
  }

  [[nodiscard]] Task<cio::io::IoResult<void>> flush() {
    auto primitive =
        cio::io::detail::SessionPrimitiveGuard<
            IoTraceWriterState>::acquire(session_);
    return flush_impl(session_, std::move(primitive));
  }

  [[nodiscard]] Task<cio::io::IoResult<void>> shutdown() {
    auto primitive =
        cio::io::detail::SessionPrimitiveGuard<
            IoTraceWriterState>::acquire(session_);
    return shutdown_impl(session_, std::move(primitive));
  }

 private:
  using SessionState =
      cio::io::detail::EndpointSessionState<IoTraceWriterState>;

  explicit IoTraceWriterSession(
      std::shared_ptr<SessionState> session) noexcept
      : session_{std::move(session)} {}

  static Task<cio::io::IoResult<std::size_t>> write_impl(
      std::shared_ptr<SessionState> session,
      cio::io::detail::SessionPrimitiveGuard<
          IoTraceWriterState> primitive,
      cio::io::ConstBufferLease buffer) {
    const auto state = session->endpoint;
    const auto input = buffer.snapshot();
    std::size_t amount = 0;
    {
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      ++state->write_calls;
      if (state->shutdown) {
        state->events.push_back("write_after_shutdown");
        co_return cio::io::IoResult<std::size_t>::failure(
            cio::io::Error::broken_pipe());
      }
      state->events.push_back("write");
      amount = std::min(input.size(), state->max_write);
      state->bytes.insert(
          state->bytes.end(), input.begin(),
          input.begin() + static_cast<std::ptrdiff_t>(amount));
    }
    (void)primitive;
    co_return cio::io::IoResult<std::size_t>::success(amount);
  }

  static Task<cio::io::IoResult<void>> flush_impl(
      std::shared_ptr<SessionState> session,
      cio::io::detail::SessionPrimitiveGuard<
          IoTraceWriterState> primitive) {
    {
      const std::lock_guard lock{session->endpoint->mutex};
      session->endpoint_guard.require_current(*session->endpoint);
      session->endpoint->events.push_back("flush");
    }
    (void)primitive;
    co_return cio::io::IoResult<void>::success();
  }

  static Task<cio::io::IoResult<void>> shutdown_impl(
      std::shared_ptr<SessionState> session,
      cio::io::detail::SessionPrimitiveGuard<
          IoTraceWriterState> primitive) {
    {
      const std::lock_guard lock{session->endpoint->mutex};
      session->endpoint_guard.require_current(*session->endpoint);
      if (!session->endpoint->shutdown) {
        session->endpoint->events.push_back("flush_in_shutdown");
        session->endpoint->events.push_back("shutdown");
        session->endpoint->shutdown = true;
      }
    }
    (void)primitive;
    co_return cio::io::IoResult<void>::success();
  }

  std::shared_ptr<SessionState> session_;
};

class IoTraceWriter final {
 public:
  static constexpr bool cio_async_write_endpoint = true;
  using WriteSession = IoTraceWriterSession;

  [[nodiscard]] static IoTraceWriter with_max_write(
      std::size_t maximum_write) {
    return IoTraceWriter{
        std::make_shared<IoTraceWriterState>(maximum_write)};
  }

  [[nodiscard]] WriteSession open_write_session() const {
    return WriteSession::acquire(state_);
  }

  [[nodiscard]] std::size_t write_calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->write_calls;
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const std::lock_guard lock{state_->mutex};
    return state_->bytes;
  }

  [[nodiscard]] std::vector<std::string_view> events() const {
    const std::lock_guard lock{state_->mutex};
    return state_->events;
  }

 private:
  explicit IoTraceWriter(
      std::shared_ptr<IoTraceWriterState> state) noexcept
      : state_{std::move(state)} {}

  std::shared_ptr<IoTraceWriterState> state_;
};

static_assert(cio::io::AsyncWrite<IoTraceWriter>);

Task<bool> io_partial_write_single_attempt_case() {
  auto writer = IoTraceWriter::with_max_write(2);
  const auto result =
      co_await cio::io::write(writer, io_buffer("abcd").lease());
  co_return result.has_value() && result.value() == 2 &&
      writer.write_calls() == 1 &&
      writer.snapshot() == io_bytes("ab") &&
      writer.events() ==
          std::vector<std::string_view>{"write"};
}

Task<bool> io_write_zero_success_case() {
  auto writer = cio::io::MemoryWriter::zero_writer();
  const auto result =
      co_await cio::io::write(writer, io_buffer("x").lease());
  co_return result.has_value() && result.value() == 0 &&
      writer.snapshot().empty();
}

Task<bool> io_write_vectored_default_first_nonempty_case() {
  auto writer = IoTraceWriter::with_max_write(
      std::numeric_limits<std::size_t>::max());
  cio::io::ConstBufferSequence buffers;
  buffers.push(io_buffer("").lease());
  buffers.push(io_buffer("ab").lease());
  buffers.push(io_buffer("cd").lease());
  const auto result =
      co_await cio::io::write_vectored(writer, std::move(buffers));
  co_return result.has_value() && result.value() == 2 &&
      writer.write_calls() == 1 &&
      writer.snapshot() == io_bytes("ab") &&
      writer.events() ==
          std::vector<std::string_view>{"write"} &&
      !cio::io::is_write_vectored(writer);
}

Task<bool> io_flush_shutdown_order_case() {
  auto writer = IoTraceWriter::with_max_write(
      std::numeric_limits<std::size_t>::max());
  const auto write =
      co_await cio::io::write(writer, io_buffer("x").lease());
  const auto flush = co_await cio::io::flush(writer);
  const auto shutdown = co_await cio::io::shutdown(writer);
  co_return write.has_value() && write.value() == 1 &&
      flush.has_value() && shutdown.has_value() &&
      writer.write_calls() == 1 &&
      writer.snapshot() == io_bytes("x") &&
      writer.events() == std::vector<std::string_view>{
          "write", "flush", "flush_in_shutdown", "shutdown"};
}

Task<bool> io_shutdown_terminal_case() {
  auto writer = cio::io::MemoryWriter::with_max_chunk();
  const auto first = co_await cio::io::shutdown(writer);
  const auto second = co_await cio::io::shutdown(writer);
  const auto write =
      co_await cio::io::write(writer, io_buffer("x").lease());
  co_return first.has_value() && second.has_value() &&
      writer.flush_count() == 1 && writer.is_shutdown() &&
      !write.has_value() &&
      write.error().kind() == cio::io::ErrorKind::broken_pipe;
}

Task<bool> io_ready_ext_noncooperative_case() {
  auto child = cio::task::spawn(cio::task::owned(
      []() -> Task<bool> {
        const auto peer_ran =
            std::make_shared<std::atomic<bool>>(false);
        auto peer = watch_spawn_budget_peer(peer_ran);
        std::vector<std::byte> source(
            512,
            static_cast<std::byte>(static_cast<unsigned char>('x')));
        auto reader = cio::io::MemoryReader::from(
            cio::io::SharedBuffer::copy_from(
                std::span<const std::byte>{source}),
            1);
        auto writer = cio::io::MemoryWriter::with_max_chunk(1);
        auto buffer = cio::io::ReadBuf::with_capacity(1);
        const auto byte = io_buffer("x").lease();
        for (std::size_t index = 0; index < 512; ++index) {
          const auto read =
              co_await cio::io::read(reader, buffer);
          const auto write =
              co_await cio::io::write(writer, byte);
          const auto flush = co_await cio::io::flush(writer);
          if (!read.has_value() || buffer.filled_size() != 1 ||
              !write.has_value() || write.value() != 1 ||
              !flush.has_value()) {
            co_return false;
          }
          buffer.clear();
        }
        const bool did_not_yield =
            !peer_ran->load(std::memory_order_acquire);
        const auto joined = co_await peer;
        co_return did_not_yield && joined.has_value();
      }));
  const auto joined = co_await child;
  co_return joined.has_value() && joined.value();
}

Task<bool> multi_spawn_join_case(std::shared_ptr<std::atomic<int>> completed) {
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(100);
  for (int index = 0; index < 100; ++index) {
    handles.push_back(cio::task::spawn(cio::task::owned(
        [](std::shared_ptr<std::atomic<int>> counter) -> Task<void> {
          co_await cio::task::yield_now();
          counter->fetch_add(1, std::memory_order_relaxed);
        },
        completed)));
  }
  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return completed->load(std::memory_order_acquire) == 100;
}

void emit(std::string_view name, bool value) {
  std::cout << name << '=' << (value ? 1 : 0) << '\n';
}

} // namespace

int main() {
  Runtime runtime;
  bool all_passed = true;

  const auto deferred_flag = std::make_shared<std::atomic<bool>>(false);
  const bool spawn_deferred =
      runtime.block_on(spawn_deferred_case(deferred_flag));
  emit("spawn_deferred", spawn_deferred);
  all_passed = all_passed && spawn_deferred;

  const auto polled = std::make_shared<std::atomic<bool>>(false);
  auto aborted = runtime.spawn(set_flag(polled));
  aborted.abort();
  const bool abort_before_poll =
      runtime.block_on(await_cancelled(std::move(aborted), polled));
  emit("abort_before_poll", abort_before_poll);
  all_passed = all_passed && abort_before_poll;

  const auto detached = std::make_shared<std::atomic<bool>>(false);
  const bool join_drop_detaches = runtime.block_on(detach_case(detached));
  emit("join_drop_detaches", join_drop_detaches);
  all_passed = all_passed && join_drop_detaches;

  const bool panic_join_error = runtime.block_on(panic_case());
  emit("panic_join_error", panic_join_error);
  all_passed = all_passed && panic_join_error;

  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto destroyed = std::make_shared<std::atomic<bool>>(false);
  const bool abort_destroys_before_join =
      runtime.block_on(abort_destruction_case(started, destroyed));
  emit("abort_destroys_before_join", abort_destroys_before_join);
  all_passed = all_passed && abort_destroys_before_join;

  const bool nested_future_current_poll = runtime.block_on(composition_case());
  emit("nested_future_current_poll", nested_future_current_poll);
  all_passed = all_passed && nested_future_current_poll;

  auto builder = cio::runtime::Builder::new_current_thread();
  auto time_runtime =
      builder.enable_time().start_paused().max_blocking_threads(1).build();

  const bool paused_sleep_rounding =
      time_runtime.block_on(paused_sleep_rounding_case());
  emit("paused_sleep_rounding", paused_sleep_rounding);
  all_passed = all_passed && paused_sleep_rounding;

  const bool timeout_immediate_zero =
      time_runtime.block_on(timeout_immediate_zero_case());
  emit("timeout_immediate_zero", timeout_immediate_zero);
  all_passed = all_passed && timeout_immediate_zero;

  const bool timeout_same_deadline =
      time_runtime.block_on(timeout_same_deadline_case());
  emit("timeout_same_deadline", timeout_same_deadline);
  all_passed = all_passed && timeout_same_deadline;

  const bool timeout_drops_loser =
      time_runtime.block_on(timeout_drops_loser_case());
  emit("timeout_drops_loser", timeout_drops_loser);
  all_passed = all_passed && timeout_drops_loser;

  const bool sleep_reset_after_elapsed =
      time_runtime.block_on(sleep_reset_after_elapsed_case());
  emit("sleep_reset_after_elapsed", sleep_reset_after_elapsed);
  all_passed = all_passed && sleep_reset_after_elapsed;

  const bool interval_basic = time_runtime.block_on(interval_basic_case());
  emit("interval_basic", interval_basic);
  all_passed = all_passed && interval_basic;

  const bool interval_missed_ticks =
      time_runtime.block_on(interval_missed_ticks_case());
  emit("interval_missed_ticks", interval_missed_ticks);
  all_passed = all_passed && interval_missed_ticks;

  const bool consume_budget_yields =
      time_runtime.block_on(consume_budget_yields_case());
  emit("consume_budget_yields", consume_budget_yields);
  all_passed = all_passed && consume_budget_yields;

  const bool blocking_running_abort_noop =
      time_runtime.block_on(blocking_running_abort_noop_case());
  emit("blocking_running_abort_noop", blocking_running_abort_noop);
  all_passed = all_passed && blocking_running_abort_noop;

  const bool blocking_queued_abort =
      time_runtime.block_on(blocking_queued_abort_case());
  emit("blocking_queued_abort", blocking_queued_abort);
  all_passed = all_passed && blocking_queued_abort;

  const bool blocking_paused_inhibits_time =
      time_runtime.block_on(blocking_paused_inhibits_time_case());
  emit("blocking_paused_inhibits_time", blocking_paused_inhibits_time);
  all_passed = all_passed && blocking_paused_inhibits_time;

  const bool notify_permit_coalesces =
      time_runtime.block_on(notify_permit_coalesces_case());
  emit("notify_permit_coalesces", notify_permit_coalesces);
  all_passed = all_passed && notify_permit_coalesces;

  const bool notify_fifo_lifo = time_runtime.block_on(notify_fifo_lifo_case());
  emit("notify_fifo_lifo", notify_fifo_lifo);
  all_passed = all_passed && notify_fifo_lifo;

  const bool notify_waiters_snapshot =
      time_runtime.block_on(notify_waiters_snapshot_case());
  emit("notify_waiters_snapshot", notify_waiters_snapshot);
  all_passed = all_passed && notify_waiters_snapshot;

  const bool notify_cancel_transfers =
      time_runtime.block_on(notify_cancel_transfers_case());
  emit("notify_cancel_transfers", notify_cancel_transfers);
  all_passed = all_passed && notify_cancel_transfers;

  const bool semaphore_fifo_head_blocking =
      time_runtime.block_on(semaphore_fifo_head_blocking_case());
  emit("semaphore_fifo_head_blocking", semaphore_fifo_head_blocking);
  all_passed = all_passed && semaphore_fifo_head_blocking;

  const bool semaphore_cancel_partial =
      time_runtime.block_on(semaphore_cancel_partial_case());
  emit("semaphore_cancel_partial", semaphore_cancel_partial);
  all_passed = all_passed && semaphore_cancel_partial;

  const bool semaphore_close = time_runtime.block_on(semaphore_close_case());
  emit("semaphore_close", semaphore_close);
  all_passed = all_passed && semaphore_close;

  const bool semaphore_permit_ops =
      time_runtime.block_on(semaphore_permit_ops_case());
  emit("semaphore_permit_ops", semaphore_permit_ops);
  all_passed = all_passed && semaphore_permit_ops;

  const bool mutex_fifo = time_runtime.block_on(mutex_fifo_case());
  emit("mutex_fifo", mutex_fifo);
  all_passed = all_passed && mutex_fifo;

  const bool mutex_cancel_transfers =
      time_runtime.block_on(mutex_cancel_transfers_case());
  emit("mutex_cancel_transfers", mutex_cancel_transfers);
  all_passed = all_passed && mutex_cancel_transfers;

  const bool mutex_no_poison = time_runtime.block_on(mutex_no_poison_case());
  emit("mutex_no_poison", mutex_no_poison);
  all_passed = all_passed && mutex_no_poison;

  const bool mutex_owned_map = time_runtime.block_on(mutex_owned_map_case());
  emit("mutex_owned_map", mutex_owned_map);
  all_passed = all_passed && mutex_owned_map;

  const bool mutex_blocking_bridge =
      time_runtime.block_on(mutex_blocking_bridge_case());
  emit("mutex_blocking_bridge", mutex_blocking_bridge);
  all_passed = all_passed && mutex_blocking_bridge;

  const bool rwlock_shared_max_readers =
      time_runtime.block_on(rwlock_shared_max_readers_case());
  emit("rwlock_shared_max_readers", rwlock_shared_max_readers);
  all_passed = all_passed && rwlock_shared_max_readers;

  const bool rwlock_writer_priority_fifo =
      time_runtime.block_on(rwlock_writer_priority_fifo_case());
  emit("rwlock_writer_priority_fifo", rwlock_writer_priority_fifo);
  all_passed = all_passed && rwlock_writer_priority_fifo;

  const bool rwlock_cancel_partial_writer =
      time_runtime.block_on(rwlock_cancel_partial_writer_case());
  emit("rwlock_cancel_partial_writer", rwlock_cancel_partial_writer);
  all_passed = all_passed && rwlock_cancel_partial_writer;

  const bool rwlock_no_poison = time_runtime.block_on(rwlock_no_poison_case());
  emit("rwlock_no_poison", rwlock_no_poison);
  all_passed = all_passed && rwlock_no_poison;

  const bool rwlock_owned_mapping =
      time_runtime.block_on(rwlock_owned_mapping_case());
  emit("rwlock_owned_mapping", rwlock_owned_mapping);
  all_passed = all_passed && rwlock_owned_mapping;

  const bool rwlock_atomic_downgrade =
      time_runtime.block_on(rwlock_atomic_downgrade_case());
  emit("rwlock_atomic_downgrade", rwlock_atomic_downgrade);
  all_passed = all_passed && rwlock_atomic_downgrade;

  const bool barrier_zero_single_leader =
      time_runtime.block_on(barrier_zero_single_leader_case());
  emit("barrier_zero_single_leader", barrier_zero_single_leader);
  all_passed = all_passed && barrier_zero_single_leader;

  const bool barrier_lazy_unpolled =
      time_runtime.block_on(barrier_lazy_unpolled_case());
  emit("barrier_lazy_unpolled", barrier_lazy_unpolled);
  all_passed = all_passed && barrier_lazy_unpolled;

  const bool barrier_reusable_unique_leader =
      time_runtime.block_on(barrier_reusable_unique_leader_case());
  emit("barrier_reusable_unique_leader", barrier_reusable_unique_leader);
  all_passed = all_passed && barrier_reusable_unique_leader;

  const bool barrier_cancelled_arrival_retained =
      time_runtime.block_on(barrier_cancelled_arrival_retained_case());
  emit("barrier_cancelled_arrival_retained",
       barrier_cancelled_arrival_retained);
  all_passed = all_passed && barrier_cancelled_arrival_retained;

  const bool once_cell_single_initializer =
      time_runtime.block_on(once_cell_single_initializer_case());
  emit("once_cell_single_initializer", once_cell_single_initializer);
  all_passed = all_passed && once_cell_single_initializer;

  const bool once_cell_cancel_retry =
      time_runtime.block_on(once_cell_cancel_retry_case());
  emit("once_cell_cancel_retry", once_cell_cancel_retry);
  all_passed = all_passed && once_cell_cancel_retry;

  const bool once_cell_try_error_retry =
      time_runtime.block_on(once_cell_try_error_retry_case());
  emit("once_cell_try_error_retry", once_cell_try_error_retry);
  all_passed = all_passed && once_cell_try_error_retry;

  const bool once_cell_clone_independent =
      time_runtime.block_on(once_cell_clone_independent_case());
  emit("once_cell_clone_independent", once_cell_clone_independent);
  all_passed = all_passed && once_cell_clone_independent;

  const bool once_cell_debug_format =
      time_runtime.block_on(once_cell_debug_format_case());
  emit("once_cell_debug_format", once_cell_debug_format);
  all_passed = all_passed && once_cell_debug_format;

  const bool once_cell_set_error_format =
      time_runtime.block_on(once_cell_set_error_format_case());
  emit("once_cell_set_error_format", once_cell_set_error_format);
  all_passed = all_passed && once_cell_set_error_format;

  const bool set_once_wait_unblocks =
      time_runtime.block_on(set_once_wait_unblocks_case());
  emit("set_once_wait_unblocks", set_once_wait_unblocks);
  all_passed = all_passed && set_once_wait_unblocks;

  const bool set_once_single_winner_values =
      time_runtime.block_on(set_once_single_winner_values_case());
  emit("set_once_single_winner_values", set_once_single_winner_values);
  all_passed = all_passed && set_once_single_winner_values;

  const bool set_once_cancel_safe =
      time_runtime.block_on(set_once_cancel_safe_case());
  emit("set_once_cancel_safe", set_once_cancel_safe);
  all_passed = all_passed && set_once_cancel_safe;

  const bool set_once_clone_independent =
      time_runtime.block_on(set_once_clone_independent_case());
  emit("set_once_clone_independent", set_once_clone_independent);
  all_passed = all_passed && set_once_clone_independent;

  const bool oneshot_send_receive =
      time_runtime.block_on(oneshot_send_receive_case());
  emit("oneshot_send_receive", oneshot_send_receive);
  all_passed = all_passed && oneshot_send_receive;

  const bool oneshot_sender_drop_recv_error =
      time_runtime.block_on(oneshot_sender_drop_recv_error_case());
  emit("oneshot_sender_drop_recv_error", oneshot_sender_drop_recv_error);
  all_passed = all_passed && oneshot_sender_drop_recv_error;

  const bool oneshot_receiver_drop_returns_value =
      time_runtime.block_on(oneshot_receiver_drop_returns_value_case());
  emit("oneshot_receiver_drop_returns_value",
       oneshot_receiver_drop_returns_value);
  all_passed = all_passed && oneshot_receiver_drop_returns_value;

  const bool oneshot_close_preserves_sent =
      time_runtime.block_on(oneshot_close_preserves_sent_case());
  emit("oneshot_close_preserves_sent", oneshot_close_preserves_sent);
  all_passed = all_passed && oneshot_close_preserves_sent;

  const bool oneshot_close_rejects_late_send =
      time_runtime.block_on(oneshot_close_rejects_late_send_case());
  emit("oneshot_close_rejects_late_send", oneshot_close_rejects_late_send);
  all_passed = all_passed && oneshot_close_rejects_late_send;

  const bool oneshot_try_recv_empty_closed =
      time_runtime.block_on(oneshot_try_recv_empty_closed_case());
  emit("oneshot_try_recv_empty_closed", oneshot_try_recv_empty_closed);
  all_passed = all_passed && oneshot_try_recv_empty_closed;

  const bool oneshot_receive_cancel_safe =
      time_runtime.block_on(oneshot_receive_cancel_safe_case());
  emit("oneshot_receive_cancel_safe", oneshot_receive_cancel_safe);
  all_passed = all_passed && oneshot_receive_cancel_safe;

  const bool oneshot_sender_closed_wakes =
      time_runtime.block_on(oneshot_sender_closed_wakes_case());
  emit("oneshot_sender_closed_wakes", oneshot_sender_closed_wakes);
  all_passed = all_passed && oneshot_sender_closed_wakes;

  const bool oneshot_empty_terminated_transitions =
      time_runtime.block_on(oneshot_empty_terminated_transitions_case());
  emit("oneshot_empty_terminated_transitions",
       oneshot_empty_terminated_transitions);
  all_passed = all_passed && oneshot_empty_terminated_transitions;

  const bool oneshot_value_drop_once =
      time_runtime.block_on(oneshot_value_drop_once_case());
  emit("oneshot_value_drop_once", oneshot_value_drop_once);
  all_passed = all_passed && oneshot_value_drop_once;

  const bool oneshot_ready_budget_yields =
      time_runtime.block_on(oneshot_ready_budget_yields_case());
  emit("oneshot_ready_budget_yields", oneshot_ready_budget_yields);
  all_passed = all_passed && oneshot_ready_budget_yields;

  const bool mpsc_fifo_backpressure =
      time_runtime.block_on(mpsc_fifo_backpressure_case());
  emit("mpsc_fifo_backpressure", mpsc_fifo_backpressure);
  all_passed = all_passed && mpsc_fifo_backpressure;

  const bool mpsc_send_reserve_fairness =
      time_runtime.block_on(mpsc_send_reserve_fairness_case());
  emit("mpsc_send_reserve_fairness", mpsc_send_reserve_fairness);
  all_passed = all_passed && mpsc_send_reserve_fairness;

  const bool mpsc_cancel_send =
      time_runtime.block_on(mpsc_cancel_send_case());
  emit("mpsc_cancel_send", mpsc_cancel_send);
  all_passed = all_passed && mpsc_cancel_send;

  const bool mpsc_cancel_reserve =
      time_runtime.block_on(mpsc_cancel_reserve_case());
  emit("mpsc_cancel_reserve", mpsc_cancel_reserve);
  all_passed = all_passed && mpsc_cancel_reserve;

  const bool mpsc_permit_capacity =
      time_runtime.block_on(mpsc_permit_capacity_case());
  emit("mpsc_permit_capacity", mpsc_permit_capacity);
  all_passed = all_passed && mpsc_permit_capacity;

  const bool mpsc_close_drain_permit =
      time_runtime.block_on(mpsc_close_drain_permit_case());
  emit("mpsc_close_drain_permit", mpsc_close_drain_permit);
  all_passed = all_passed && mpsc_close_drain_permit;

  const bool mpsc_try_errors =
      time_runtime.block_on(mpsc_try_errors_case());
  emit("mpsc_try_errors", mpsc_try_errors);
  all_passed = all_passed && mpsc_try_errors;

  const bool mpsc_receiver_drop =
      time_runtime.block_on(mpsc_receiver_drop_case());
  emit("mpsc_receiver_drop", mpsc_receiver_drop);
  all_passed = all_passed && mpsc_receiver_drop;

  const bool mpsc_last_sender_weak =
      time_runtime.block_on(mpsc_last_sender_weak_case());
  emit("mpsc_last_sender_weak", mpsc_last_sender_weak);
  all_passed = all_passed && mpsc_last_sender_weak;

  const bool mpsc_sender_counts =
      time_runtime.block_on(mpsc_sender_counts_case());
  emit("mpsc_sender_counts", mpsc_sender_counts);
  all_passed = all_passed && mpsc_sender_counts;

  const bool mpsc_error_format =
      time_runtime.block_on(mpsc_error_format_case());
  emit("mpsc_error_format", mpsc_error_format);
  all_passed = all_passed && mpsc_error_format;

  const bool mpsc_closed_wakes =
      time_runtime.block_on(mpsc_closed_wakes_case());
  emit("mpsc_closed_wakes", mpsc_closed_wakes);
  all_passed = all_passed && mpsc_closed_wakes;

  const bool mpsc_closed_cancel_safe =
      time_runtime.block_on(mpsc_closed_cancel_safe_case());
  emit("mpsc_closed_cancel_safe", mpsc_closed_cancel_safe);
  all_passed = all_passed && mpsc_closed_cancel_safe;

  const bool mpsc_same_channel =
      time_runtime.block_on(mpsc_same_channel_case());
  emit("mpsc_same_channel", mpsc_same_channel);
  all_passed = all_passed && mpsc_same_channel;

  const bool mpsc_receiver_len_empty =
      time_runtime.block_on(mpsc_receiver_len_empty_case());
  emit("mpsc_receiver_len_empty", mpsc_receiver_len_empty);
  all_passed = all_passed && mpsc_receiver_len_empty;

  const bool mpsc_try_reserve_errors =
      time_runtime.block_on(mpsc_try_reserve_errors_case());
  emit("mpsc_try_reserve_errors", mpsc_try_reserve_errors);
  all_passed = all_passed && mpsc_try_reserve_errors;

  const bool mpsc_owned_permit_send_release =
      time_runtime.block_on(mpsc_owned_permit_send_release_case());
  emit("mpsc_owned_permit_send_release",
       mpsc_owned_permit_send_release);
  all_passed = all_passed && mpsc_owned_permit_send_release;

  const bool mpsc_owned_permit_same_channel =
      time_runtime.block_on(mpsc_owned_permit_same_channel_case());
  emit("mpsc_owned_permit_same_channel",
       mpsc_owned_permit_same_channel);
  all_passed = all_passed && mpsc_owned_permit_same_channel;

  const bool mpsc_owned_permit_lifetime =
      time_runtime.block_on(mpsc_owned_permit_lifetime_case());
  emit("mpsc_owned_permit_lifetime", mpsc_owned_permit_lifetime);
  all_passed = all_passed && mpsc_owned_permit_lifetime;

  const bool mpsc_reserve_owned_closed_consumes_sender =
      time_runtime.block_on(
          mpsc_reserve_owned_closed_consumes_sender_case());
  emit("mpsc_reserve_owned_closed_consumes_sender",
       mpsc_reserve_owned_closed_consumes_sender);
  all_passed =
      all_passed && mpsc_reserve_owned_closed_consumes_sender;

  const bool mpsc_try_reserve_owned_errors =
      time_runtime.block_on(mpsc_try_reserve_owned_errors_case());
  emit("mpsc_try_reserve_owned_errors", mpsc_try_reserve_owned_errors);
  all_passed = all_passed && mpsc_try_reserve_owned_errors;

  const bool mpsc_reserve_owned_cancel_safe =
      time_runtime.block_on(mpsc_reserve_owned_cancel_safe_case());
  emit("mpsc_reserve_owned_cancel_safe",
       mpsc_reserve_owned_cancel_safe);
  all_passed = all_passed && mpsc_reserve_owned_cancel_safe;

  const bool mpsc_unbounded_fifo_multi_sender =
      time_runtime.block_on(mpsc_unbounded_fifo_multi_sender_case());
  emit("mpsc_unbounded_fifo_multi_sender",
       mpsc_unbounded_fifo_multi_sender);
  all_passed = all_passed && mpsc_unbounded_fifo_multi_sender;

  const bool mpsc_unbounded_send_try_errors =
      time_runtime.block_on(mpsc_unbounded_send_try_errors_case());
  emit("mpsc_unbounded_send_try_errors",
       mpsc_unbounded_send_try_errors);
  all_passed = all_passed && mpsc_unbounded_send_try_errors;

  const bool mpsc_unbounded_close_drain =
      time_runtime.block_on(mpsc_unbounded_close_drain_case());
  emit("mpsc_unbounded_close_drain", mpsc_unbounded_close_drain);
  all_passed = all_passed && mpsc_unbounded_close_drain;

  const bool mpsc_unbounded_receiver_drop =
      time_runtime.block_on(mpsc_unbounded_receiver_drop_case());
  emit("mpsc_unbounded_receiver_drop", mpsc_unbounded_receiver_drop);
  all_passed = all_passed && mpsc_unbounded_receiver_drop;

  const bool mpsc_unbounded_closed_wakes =
      time_runtime.block_on(mpsc_unbounded_closed_wakes_case());
  emit("mpsc_unbounded_closed_wakes", mpsc_unbounded_closed_wakes);
  all_passed = all_passed && mpsc_unbounded_closed_wakes;

  const bool mpsc_unbounded_closed_cancel_safe =
      time_runtime.block_on(mpsc_unbounded_closed_cancel_safe_case());
  emit("mpsc_unbounded_closed_cancel_safe",
       mpsc_unbounded_closed_cancel_safe);
  all_passed = all_passed && mpsc_unbounded_closed_cancel_safe;

  const bool mpsc_unbounded_last_sender_weak =
      time_runtime.block_on(mpsc_unbounded_last_sender_weak_case());
  emit("mpsc_unbounded_last_sender_weak",
       mpsc_unbounded_last_sender_weak);
  all_passed = all_passed && mpsc_unbounded_last_sender_weak;

  const bool mpsc_unbounded_same_channel_counts =
      time_runtime.block_on(
          mpsc_unbounded_same_channel_counts_case());
  emit("mpsc_unbounded_same_channel_counts",
       mpsc_unbounded_same_channel_counts);
  all_passed = all_passed && mpsc_unbounded_same_channel_counts;

  const bool mpsc_unbounded_receiver_len_empty =
      time_runtime.block_on(
          mpsc_unbounded_receiver_len_empty_case());
  emit("mpsc_unbounded_receiver_len_empty",
       mpsc_unbounded_receiver_len_empty);
  all_passed = all_passed && mpsc_unbounded_receiver_len_empty;

  const bool mpsc_unbounded_ready_recv_budget =
      time_runtime.block_on(
          mpsc_unbounded_ready_recv_budget_case());
  emit("mpsc_unbounded_ready_recv_budget",
       mpsc_unbounded_ready_recv_budget);
  all_passed = all_passed && mpsc_unbounded_ready_recv_budget;

  const bool mpsc_unbounded_value_drop_once =
      time_runtime.block_on(mpsc_unbounded_value_drop_once_case());
  emit("mpsc_unbounded_value_drop_once",
       mpsc_unbounded_value_drop_once);
  all_passed = all_passed && mpsc_unbounded_value_drop_once;

  const bool mpsc_unbounded_weak_upgrade_closed =
      time_runtime.block_on(
          mpsc_unbounded_weak_upgrade_closed_case());
  emit("mpsc_unbounded_weak_upgrade_closed",
       mpsc_unbounded_weak_upgrade_closed);
  all_passed = all_passed && mpsc_unbounded_weak_upgrade_closed;

  const bool mpsc_unbounded_recv_cancel_safe =
      time_runtime.block_on(
          mpsc_unbounded_recv_cancel_safe_case());
  emit("mpsc_unbounded_recv_cancel_safe",
       mpsc_unbounded_recv_cancel_safe);
  all_passed = all_passed && mpsc_unbounded_recv_cancel_safe;

  const bool mpsc_unbounded_noncoop_send_closed =
      time_runtime.block_on(
          mpsc_unbounded_noncoop_send_closed_case());
  emit("mpsc_unbounded_noncoop_send_closed",
       mpsc_unbounded_noncoop_send_closed);
  all_passed = all_passed && mpsc_unbounded_noncoop_send_closed;

  const bool watch_initial_borrow =
      time_runtime.block_on(watch_initial_borrow_case());
  emit("watch_initial_borrow", watch_initial_borrow);
  all_passed = all_passed && watch_initial_borrow;

  const bool watch_send_changed_borrow_update =
      time_runtime.block_on(watch_send_changed_borrow_update_case());
  emit("watch_send_changed_borrow_update",
       watch_send_changed_borrow_update);
  all_passed = all_passed && watch_send_changed_borrow_update;

  const bool watch_marks_and_has_changed =
      time_runtime.block_on(watch_marks_and_has_changed_case());
  emit("watch_marks_and_has_changed", watch_marks_and_has_changed);
  all_passed = all_passed && watch_marks_and_has_changed;

  const bool watch_independent_receivers_subscribe =
      time_runtime.block_on(watch_independent_receivers_subscribe_case());
  emit("watch_independent_receivers_subscribe",
       watch_independent_receivers_subscribe);
  all_passed = all_passed && watch_independent_receivers_subscribe;

  const bool watch_last_sender_close_retains_value =
      time_runtime.block_on(
          watch_last_sender_close_retains_value_case());
  emit("watch_last_sender_close_retains_value",
       watch_last_sender_close_retains_value);
  all_passed = all_passed && watch_last_sender_close_retains_value;

  const bool watch_last_receiver_closes_sender =
      time_runtime.block_on(watch_last_receiver_closes_sender_case());
  emit("watch_last_receiver_closes_sender",
       watch_last_receiver_closes_sender);
  all_passed = all_passed && watch_last_receiver_closes_sender;

  const bool watch_changed_cancel_safe =
      time_runtime.block_on(watch_changed_cancel_safe_case());
  emit("watch_changed_cancel_safe", watch_changed_cancel_safe);
  all_passed = all_passed && watch_changed_cancel_safe;

  const bool watch_same_channel_counts =
      time_runtime.block_on(watch_same_channel_counts_case());
  emit("watch_same_channel_counts", watch_same_channel_counts);
  all_passed = all_passed && watch_same_channel_counts;

  const bool watch_send_replace =
      time_runtime.block_on(watch_send_replace_case());
  emit("watch_send_replace", watch_send_replace);
  all_passed = all_passed && watch_send_replace;

  const bool watch_wait_for =
      time_runtime.block_on(watch_wait_for_case());
  emit("watch_wait_for", watch_wait_for);
  all_passed = all_passed && watch_wait_for;

  const bool watch_value_drop_and_clone =
      time_runtime.block_on(watch_value_drop_and_clone_case());
  emit("watch_value_drop_and_clone", watch_value_drop_and_clone);
  all_passed = all_passed && watch_value_drop_and_clone;

  const bool watch_error_format =
      time_runtime.block_on(watch_error_format_case());
  emit("watch_error_format", watch_error_format);
  all_passed = all_passed && watch_error_format;

  const bool watch_cooperative_ready_paths =
      time_runtime.block_on(watch_cooperative_ready_paths_case());
  emit("watch_cooperative_ready_paths",
       watch_cooperative_ready_paths);
  all_passed = all_passed && watch_cooperative_ready_paths;

  const bool watch_coop_changed_success_boundary =
      time_runtime.block_on(
          watch_coop_changed_success_boundary_case());
  emit("watch_coop_changed_success_boundary",
       watch_coop_changed_success_boundary);
  all_passed =
      all_passed && watch_coop_changed_success_boundary;

  const bool watch_coop_changed_error_boundary =
      time_runtime.block_on(
          watch_coop_changed_error_boundary_case());
  emit("watch_coop_changed_error_boundary",
       watch_coop_changed_error_boundary);
  all_passed =
      all_passed && watch_coop_changed_error_boundary;

  const bool watch_coop_closed_boundary =
      time_runtime.block_on(watch_coop_closed_boundary_case());
  emit("watch_coop_closed_boundary", watch_coop_closed_boundary);
  all_passed = all_passed && watch_coop_closed_boundary;

  const bool watch_coop_wait_for_success_boundary =
      time_runtime.block_on(
          watch_coop_wait_for_success_boundary_case());
  emit("watch_coop_wait_for_success_boundary",
       watch_coop_wait_for_success_boundary);
  all_passed =
      all_passed && watch_coop_wait_for_success_boundary;

  const bool watch_coop_wait_for_error_boundary =
      time_runtime.block_on(
          watch_coop_wait_for_error_boundary_case());
  emit("watch_coop_wait_for_error_boundary",
       watch_coop_wait_for_error_boundary);
  all_passed =
      all_passed && watch_coop_wait_for_error_boundary;

  const bool watch_coop_changed_fresh_wake_budget =
      time_runtime.block_on(
          watch_coop_changed_fresh_wake_budget_case());
  emit("watch_coop_changed_fresh_wake_budget",
       watch_coop_changed_fresh_wake_budget);
  all_passed =
      all_passed && watch_coop_changed_fresh_wake_budget;

  const bool watch_coop_wait_for_fresh_wake_budget =
      time_runtime.block_on(
          watch_coop_wait_for_fresh_wake_budget_case());
  emit("watch_coop_wait_for_fresh_wake_budget",
       watch_coop_wait_for_fresh_wake_budget);
  all_passed =
      all_passed && watch_coop_wait_for_fresh_wake_budget;

  const bool watch_coop_closed_fresh_wake_budget =
      time_runtime.block_on(
          watch_coop_closed_fresh_wake_budget_case());
  emit("watch_coop_closed_fresh_wake_budget",
       watch_coop_closed_fresh_wake_budget);
  all_passed =
      all_passed && watch_coop_closed_fresh_wake_budget;

  const bool broadcast_capacity_rounding_lag =
      time_runtime.block_on(
          broadcast_capacity_rounding_lag_case());
  emit("broadcast_capacity_rounding_lag",
       broadcast_capacity_rounding_lag);
  all_passed =
      all_passed && broadcast_capacity_rounding_lag;

  const bool broadcast_failed_send_then_subscribe =
      time_runtime.block_on(
          broadcast_failed_send_then_subscribe_case());
  emit("broadcast_failed_send_then_subscribe",
       broadcast_failed_send_then_subscribe);
  all_passed =
      all_passed && broadcast_failed_send_then_subscribe;

  const bool broadcast_independent_receivers =
      time_runtime.block_on(
          broadcast_independent_receivers_case());
  emit("broadcast_independent_receivers",
       broadcast_independent_receivers);
  all_passed =
      all_passed && broadcast_independent_receivers;

  const bool broadcast_resubscribe_skips_backlog =
      time_runtime.block_on(
          broadcast_resubscribe_skips_backlog_case());
  emit("broadcast_resubscribe_skips_backlog",
       broadcast_resubscribe_skips_backlog);
  all_passed =
      all_passed && broadcast_resubscribe_skips_backlog;

  const bool broadcast_drain_then_closed =
      time_runtime.block_on(broadcast_drain_then_closed_case());
  emit("broadcast_drain_then_closed",
       broadcast_drain_then_closed);
  all_passed = all_passed && broadcast_drain_then_closed;

  const bool broadcast_lagged_exact =
      time_runtime.block_on(broadcast_lagged_exact_case());
  emit("broadcast_lagged_exact", broadcast_lagged_exact);
  all_passed = all_passed && broadcast_lagged_exact;

  const bool broadcast_try_recv_empty_closed =
      time_runtime.block_on(
          broadcast_try_recv_empty_closed_case());
  emit("broadcast_try_recv_empty_closed",
       broadcast_try_recv_empty_closed);
  all_passed =
      all_passed && broadcast_try_recv_empty_closed;

  const bool broadcast_send_receiver_count =
      time_runtime.block_on(
          broadcast_send_receiver_count_case());
  emit("broadcast_send_receiver_count",
       broadcast_send_receiver_count);
  all_passed =
      all_passed && broadcast_send_receiver_count;

  const bool broadcast_counts =
      time_runtime.block_on(broadcast_counts_case());
  emit("broadcast_counts", broadcast_counts);
  all_passed = all_passed && broadcast_counts;

  const bool broadcast_weak_upgrade =
      time_runtime.block_on(broadcast_weak_upgrade_case());
  emit("broadcast_weak_upgrade", broadcast_weak_upgrade);
  all_passed = all_passed && broadcast_weak_upgrade;

  const bool broadcast_copy_panic_advances_cursor =
      time_runtime.block_on(
          broadcast_copy_exception_advances_cursor_case());
  emit("broadcast_copy_panic_advances_cursor",
       broadcast_copy_panic_advances_cursor);
  all_passed =
      all_passed && broadcast_copy_panic_advances_cursor;

  const bool broadcast_recv_cooperative_ready_budget =
      time_runtime.block_on(
          broadcast_recv_cooperative_ready_budget_case());
  emit("broadcast_recv_cooperative_ready_budget",
       broadcast_recv_cooperative_ready_budget);
  all_passed =
      all_passed && broadcast_recv_cooperative_ready_budget;

  const bool broadcast_recv_cooperative_pending_budget =
      time_runtime.block_on(
          broadcast_recv_cooperative_pending_budget_case());
  emit("broadcast_recv_cooperative_pending_budget",
       broadcast_recv_cooperative_pending_budget);
  all_passed =
      all_passed && broadcast_recv_cooperative_pending_budget;

  const bool broadcast_closed_noncooperative =
      time_runtime.block_on(
          broadcast_closed_noncooperative_case());
  emit("broadcast_closed_noncooperative",
       broadcast_closed_noncooperative);
  all_passed =
      all_passed && broadcast_closed_noncooperative;

  const bool io_readbuf_regions_clear =
      io_readbuf_regions_clear_case();
  emit("io_readbuf_regions_clear", io_readbuf_regions_clear);
  all_passed = all_passed && io_readbuf_regions_clear;

  const bool io_partial_read_eof_zero_capacity =
      time_runtime.block_on(
          io_partial_read_eof_zero_capacity_case());
  emit("io_partial_read_eof_zero_capacity",
       io_partial_read_eof_zero_capacity);
  all_passed =
      all_passed && io_partial_read_eof_zero_capacity;

  const bool io_read_exact_partial_success =
      time_runtime.block_on(
          io_read_exact_partial_success_case());
  emit("io_read_exact_partial_success",
       io_read_exact_partial_success);
  all_passed =
      all_passed && io_read_exact_partial_success;

  const bool io_read_exact_early_eof =
      time_runtime.block_on(
          io_read_exact_early_eof_case());
  emit("io_read_exact_early_eof",
       io_read_exact_early_eof);
  all_passed = all_passed && io_read_exact_early_eof;

  const bool io_read_exact_partial_error =
      time_runtime.block_on(
          io_read_exact_partial_error_case());
  emit("io_read_exact_partial_error",
       io_read_exact_partial_error);
  all_passed =
      all_passed && io_read_exact_partial_error;

  const bool io_write_all_partial_zero =
      time_runtime.block_on(
          io_write_all_partial_zero_case());
  emit("io_write_all_partial_zero",
       io_write_all_partial_zero);
  all_passed = all_passed && io_write_all_partial_zero;

  const bool io_write_all_partial_error =
      time_runtime.block_on(
          io_write_all_partial_error_case());
  emit("io_write_all_partial_error",
       io_write_all_partial_error);
  all_passed =
      all_passed && io_write_all_partial_error;

  const bool io_exact_cancel_partial_late_wake =
      time_runtime.block_on(
          io_exact_cancel_partial_late_wake_case());
  emit("io_exact_cancel_partial_late_wake",
       io_exact_cancel_partial_late_wake);
  all_passed =
      all_passed && io_exact_cancel_partial_late_wake;

  const bool io_exact_empty_no_poll =
      time_runtime.block_on(
          io_exact_empty_no_poll_case());
  emit("io_exact_empty_no_poll",
       io_exact_empty_no_poll);
  all_passed = all_passed && io_exact_empty_no_poll;

  const bool io_partial_write_single_attempt =
      time_runtime.block_on(
          io_partial_write_single_attempt_case());
  emit("io_partial_write_single_attempt",
       io_partial_write_single_attempt);
  all_passed =
      all_passed && io_partial_write_single_attempt;

  const bool io_write_zero_success =
      time_runtime.block_on(io_write_zero_success_case());
  emit("io_write_zero_success", io_write_zero_success);
  all_passed = all_passed && io_write_zero_success;

  const bool io_write_vectored_default_first_nonempty =
      time_runtime.block_on(
          io_write_vectored_default_first_nonempty_case());
  emit("io_write_vectored_default_first_nonempty",
       io_write_vectored_default_first_nonempty);
  all_passed =
      all_passed && io_write_vectored_default_first_nonempty;

  const bool io_flush_shutdown_order =
      time_runtime.block_on(io_flush_shutdown_order_case());
  emit("io_flush_shutdown_order", io_flush_shutdown_order);
  all_passed = all_passed && io_flush_shutdown_order;

  const bool io_shutdown_terminal =
      time_runtime.block_on(io_shutdown_terminal_case());
  emit("io_shutdown_terminal", io_shutdown_terminal);
  all_passed = all_passed && io_shutdown_terminal;

  const bool io_ready_ext_noncooperative =
      time_runtime.block_on(io_ready_ext_noncooperative_case());
  emit("io_ready_ext_noncooperative",
       io_ready_ext_noncooperative);
  all_passed = all_passed && io_ready_ext_noncooperative;

  const auto multi_completed = std::make_shared<std::atomic<int>>(0);
  auto multi_builder = cio::runtime::Builder::new_multi_thread();
  auto multi_runtime = multi_builder.worker_threads(4).build();
  const bool multi_spawn_join = multi_runtime.block_on(cio::task::owned(
      [](std::shared_ptr<std::atomic<int>> completed) -> Task<bool> {
        co_return co_await multi_spawn_join_case(std::move(completed));
      },
      multi_completed));
  emit("multi_spawn_join", multi_spawn_join);
  all_passed = all_passed && multi_spawn_join;

  return all_passed ? 0 : 1;
}
