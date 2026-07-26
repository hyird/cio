#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"

namespace {

using namespace std::chrono_literals;
using cio::Task;
using cio::runtime::Runtime;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

void update_maximum(const std::shared_ptr<std::atomic<int>> &maximum,
                    int value) {
  auto previous = maximum->load(std::memory_order_acquire);
  while (previous < value && !maximum->compare_exchange_weak(
                                 previous, value, std::memory_order_acq_rel,
                                 std::memory_order_acquire)) {
  }
}

Task<bool> rwlock_basic_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(1, 2);
  {
    auto first = co_await rwlock.read_owned();
    auto second_result = rwlock.try_read();
    if (!second_result.has_value()) {
      co_return false;
    }
    auto second = std::move(second_result).value();
    if (*first != 1 || *second != 1 || rwlock.try_read().has_value() ||
        rwlock.try_write().has_value()) {
      co_return false;
    }
    co_await cio::task::yield_now();
  }

  {
    auto write = co_await rwlock.write_owned();
    if (rwlock.try_read().has_value() || rwlock.try_write_owned().has_value()) {
      co_return false;
    }
    co_await cio::task::yield_now();
    *write = 7;
  }

  auto final = rwlock.try_read_owned();
  co_return final.has_value() && *final.value() == 7 &&
      final.value().rwlock().try_write().has_value() == false;
}

Task<bool> rwlock_blocking_rejected_root() {
  cio::sync::RwLock<int> rwlock{0};
  bool read_rejected = false;
  bool write_rejected = false;
  try {
    auto guard = rwlock.blocking_read();
    (void)guard;
  } catch (const std::logic_error &) {
    read_rejected = true;
  }
  try {
    auto guard = rwlock.blocking_write();
    (void)guard;
  } catch (const std::logic_error &) {
    write_rejected = true;
  }
  co_return read_rejected &&write_rejected;
}

Task<void>
rwlock_reader_hold_child(cio::sync::RwLock<int> rwlock,
                         std::shared_ptr<std::atomic<int>> active,
                         std::shared_ptr<std::atomic<int>> maximum,
                         std::shared_ptr<std::atomic<int>> started,
                         std::shared_ptr<std::atomic<bool>> release) {
  auto guard = co_await rwlock.read_owned();
  const auto now = active->fetch_add(1, std::memory_order_acq_rel) + 1;
  update_maximum(maximum, now);
  started->fetch_add(1, std::memory_order_release);
  while (!release->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  if (*guard != 5) {
    throw std::runtime_error{"读 guard 数据错误"};
  }
  active->fetch_sub(1, std::memory_order_acq_rel);
}

Task<bool> rwlock_max_readers_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(5, 3);
  const auto active = std::make_shared<std::atomic<int>>(0);
  const auto maximum = std::make_shared<std::atomic<int>>(0);
  const auto started = std::make_shared<std::atomic<int>>(0);
  const auto release = std::make_shared<std::atomic<bool>>(false);
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(4);
  for (int index = 0; index < 4; ++index) {
    handles.push_back(cio::task::spawn(
        rwlock_reader_hold_child(rwlock, active, maximum, started, release)));
  }
  while (started->load(std::memory_order_acquire) < 3) {
    co_await cio::task::yield_now();
  }
  for (int index = 0; index < 8; ++index) {
    co_await cio::task::yield_now();
  }
  const bool capped = started->load(std::memory_order_acquire) == 3 &&
                      maximum->load(std::memory_order_acquire) == 3 &&
                      rwlock.try_read().has_value() == false;
  release->store(true, std::memory_order_release);
  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  co_return capped && started->load(std::memory_order_acquire) == 4 &&
      active->load(std::memory_order_acquire) == 0 &&
      rwlock.try_write().has_value();
}

Task<void> rwlock_writer_order_child(cio::sync::RwLock<int> rwlock, int label,
                                     std::shared_ptr<std::vector<int>> order) {
  auto guard = co_await rwlock.write_owned();
  order->push_back(label);
  ++*guard;
  co_await cio::task::yield_now();
}

Task<void> rwlock_reader_order_child(cio::sync::RwLock<int> rwlock, int label,
                                     std::shared_ptr<std::vector<int>> order) {
  auto guard = co_await rwlock.read_owned();
  order->push_back(label);
  if (*guard < 0) {
    throw std::runtime_error{"不可能的读值"};
  }
  co_await cio::task::yield_now();
}

Task<bool> rwlock_writer_preference_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 4);
  std::optional<cio::sync::RwLockReadGuard<int>> owner;
  owner.emplace(co_await rwlock.read_owned());
  const auto order = std::make_shared<std::vector<int>>();

  auto writer = cio::task::spawn(rwlock_writer_order_child(rwlock, 1, order));
  co_await cio::task::yield_now();
  const bool late_try_read_blocked = !rwlock.try_read().has_value();
  auto reader = cio::task::spawn(rwlock_reader_order_child(rwlock, 2, order));
  co_await cio::task::yield_now();
  const bool both_waited = order->empty();
  owner.reset();

  const auto writer_result = co_await writer;
  const auto reader_result = co_await reader;
  auto final = co_await rwlock.read();
  co_return late_try_read_blocked && both_waited && writer_result.has_value() &&
      reader_result.has_value() && *order == std::vector<int>({1, 2}) &&
      *final == 1;
}

Task<void>
rwlock_writer_flag_child(cio::sync::RwLock<int> rwlock,
                         std::shared_ptr<std::atomic<bool>> acquired) {
  auto guard = co_await rwlock.write_owned();
  acquired->store(true, std::memory_order_release);
  ++*guard;
}

Task<void>
rwlock_reader_flag_child(cio::sync::RwLock<int> rwlock,
                         std::shared_ptr<std::atomic<bool>> acquired) {
  auto guard = co_await rwlock.read_owned();
  if (*guard < 0) {
    throw std::runtime_error{"不可能的读值"};
  }
  acquired->store(true, std::memory_order_release);
}

Task<bool> rwlock_cancel_partial_writer_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 4);
  std::optional<cio::sync::RwLockReadGuard<int>> owner;
  owner.emplace(co_await rwlock.read());
  const auto writer_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto reader_acquired = std::make_shared<std::atomic<bool>>(false);
  auto writer =
      cio::task::spawn(rwlock_writer_flag_child(rwlock, writer_acquired));
  co_await cio::task::yield_now();
  auto reader =
      cio::task::spawn(rwlock_reader_flag_child(rwlock, reader_acquired));
  co_await cio::task::yield_now();

  writer.abort();
  const auto writer_result = co_await writer;
  while (!reader_acquired->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  const auto reader_result = co_await reader;
  const bool transferred_while_owner_held =
      !writer_result.has_value() && writer_result.error().is_cancelled() &&
      !writer_acquired->load(std::memory_order_acquire) &&
      reader_result.has_value() && **owner == 0;
  owner.reset();
  co_return transferred_while_owner_held &&rwlock.try_write().has_value();
}

Task<bool> rwlock_cancel_acquired_writer_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 2);
  std::optional<cio::sync::RwLockWriteGuard<int>> owner;
  owner.emplace(co_await rwlock.write_owned());
  const auto first_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto second_acquired = std::make_shared<std::atomic<bool>>(false);
  auto first =
      cio::task::spawn(rwlock_writer_flag_child(rwlock, first_acquired));
  co_await cio::task::yield_now();
  auto second =
      cio::task::spawn(rwlock_writer_flag_child(rwlock, second_acquired));
  co_await cio::task::yield_now();
  owner.reset();
  first.abort();
  const auto first_result = co_await first;
  const auto second_result = co_await second;
  auto final = co_await rwlock.read();
  co_return !first_result.has_value() && first_result.error().is_cancelled() &&
      !first_acquired->load(std::memory_order_acquire) &&
      second_result.has_value() &&
      second_acquired->load(std::memory_order_acquire) && *final == 1;
}

Task<bool> rwlock_cancel_pending_reader_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 2);
  std::optional<cio::sync::RwLockWriteGuard<int>> owner;
  owner.emplace(co_await rwlock.write_owned());
  const auto reader_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto writer_acquired = std::make_shared<std::atomic<bool>>(false);

  auto reader =
      cio::task::spawn(rwlock_reader_flag_child(rwlock, reader_acquired));
  co_await cio::task::yield_now();
  reader.abort();
  const auto reader_result = co_await reader;

  auto writer =
      cio::task::spawn(rwlock_writer_flag_child(rwlock, writer_acquired));
  co_await cio::task::yield_now();
  owner.reset();
  const auto writer_result = co_await writer;

  co_return !reader_result.has_value() &&
      reader_result.error().is_cancelled() &&
      !reader_acquired->load(std::memory_order_acquire) &&
      writer_result.has_value() &&
      writer_acquired->load(std::memory_order_acquire) &&
      rwlock.try_write().has_value();
}

Task<bool> rwlock_cancel_acquired_reader_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 2);
  std::optional<cio::sync::RwLockWriteGuard<int>> owner;
  owner.emplace(co_await rwlock.write_owned());
  const auto reader_acquired = std::make_shared<std::atomic<bool>>(false);
  const auto writer_acquired = std::make_shared<std::atomic<bool>>(false);

  auto reader =
      cio::task::spawn(rwlock_reader_flag_child(rwlock, reader_acquired));
  co_await cio::task::yield_now();
  auto writer =
      cio::task::spawn(rwlock_writer_flag_child(rwlock, writer_acquired));
  co_await cio::task::yield_now();

  owner.reset();
  reader.abort();
  const auto reader_result = co_await reader;
  const auto writer_result = co_await writer;

  auto final = co_await rwlock.read();
  co_return !reader_result.has_value() &&
      reader_result.error().is_cancelled() &&
      !reader_acquired->load(std::memory_order_acquire) &&
      writer_result.has_value() &&
      writer_acquired->load(std::memory_order_acquire) && *final == 1;
}

Task<void> rwlock_throwing_writer(cio::sync::RwLock<int> rwlock) {
  auto guard = co_await rwlock.write_owned();
  *guard = 7;
  throw std::runtime_error{"预期的 RwLock 写 task 异常"};
}

Task<bool> rwlock_no_poison_root() {
  cio::sync::RwLock<int> rwlock{0};
  auto child = cio::task::spawn(rwlock_throwing_writer(rwlock));
  const auto result = co_await child;
  auto after = rwlock.try_read();
  co_return !result.has_value() && result.error().is_panic() &&
      after.has_value() && *after.value() == 7;
}

struct RwLockRecord final {
  struct Nested final {
    int value;
  };

  int first;
  Nested nested;
};

struct UnknownMobility final {};

Task<bool> rwlock_mapping_root() {
  cio::sync::RwLock<RwLockRecord> rwlock{
      RwLockRecord{1, RwLockRecord::Nested{2}}};
  {
    const auto projection_calls = std::make_shared<std::atomic<int>>(0);
    auto guard = co_await rwlock.read_owned();
    auto nested = cio::sync::RwLockReadGuard<RwLockRecord>::map(
        std::move(guard),
        [projection_calls](
            const RwLockRecord &value) -> const RwLockRecord::Nested & {
          projection_calls->fetch_add(1, std::memory_order_relaxed);
          return value.nested;
        });
    co_await cio::task::yield_now();
    auto value =
        cio::sync::RwLockReadGuard<RwLockRecord, RwLockRecord::Nested>::map(
            std::move(nested),
            [](const RwLockRecord::Nested &nested_value) -> const int & {
              return nested_value.value;
            });
    if (*value != 2 || *value != 2 ||
        projection_calls->load(std::memory_order_relaxed) != 1 ||
        value.rwlock().try_write().has_value()) {
      co_return false;
    }
  }

  {
    auto guard = co_await rwlock.read();
    auto failed = cio::sync::RwLockReadGuard<RwLockRecord>::try_map(
        std::move(guard), [](const RwLockRecord &) { return false; },
        [](const RwLockRecord &value) -> const int & { return value.first; });
    if (failed.has_value()) {
      co_return false;
    }
    auto original = std::move(failed).error();
    if ((*original).first != 1) {
      co_return false;
    }
  }

  {
    const auto projection_calls = std::make_shared<std::atomic<int>>(0);
    auto guard = co_await rwlock.write_owned();
    auto nested = cio::sync::RwLockWriteGuard<RwLockRecord>::map(
        std::move(guard),
        [projection_calls](RwLockRecord &value) -> RwLockRecord::Nested & {
          projection_calls->fetch_add(1, std::memory_order_relaxed);
          return value.nested;
        });
    co_await cio::task::yield_now();
    auto value = cio::sync::
        RwLockMappedWriteGuard<RwLockRecord, RwLockRecord::Nested>::map(
            std::move(nested), [](RwLockRecord::Nested &nested_value) -> int & {
              return nested_value.value;
            });
    *value = 9;
    if (projection_calls->load(std::memory_order_relaxed) != 1) {
      co_return false;
    }
  }

  {
    auto guard = co_await rwlock.write();
    auto failed = cio::sync::RwLockWriteGuard<RwLockRecord>::try_map(
        std::move(guard), [](RwLockRecord &) { return false; },
        [](RwLockRecord &value) -> int & { return value.first; });
    if (failed.has_value()) {
      co_return false;
    }
    auto original = std::move(failed).error();
    (*original).first = 4;
  }

  {
    auto guard = co_await rwlock.write_owned();
    auto mapped = cio::sync::RwLockWriteGuard<RwLockRecord>::into_mapped(
        std::move(guard));
    mapped.get().first = 5;
  }

  bool projection_threw = false;
  try {
    auto guard = co_await rwlock.write_owned();
    (void)cio::sync::RwLockWriteGuard<RwLockRecord>::map(
        std::move(guard), [](RwLockRecord &) -> int & {
          throw std::runtime_error{"预期的 RwLock 投影异常"};
        });
  } catch (const std::runtime_error &) {
    projection_threw = true;
  }

  auto final = co_await rwlock.read();
  co_return projection_threw && (*final).first == 5 &&
      (*final).nested.value == 9;
}

Task<void> rwlock_set_writer(cio::sync::RwLock<int> rwlock,
                             std::shared_ptr<std::atomic<bool>> completed) {
  auto guard = co_await rwlock.write_owned();
  *guard = 2;
  completed->store(true, std::memory_order_release);
}

Task<bool> rwlock_downgrade_root() {
  cio::sync::RwLock<int> rwlock{0};
  auto write = co_await rwlock.write_owned();
  *write = 1;
  const auto writer_completed = std::make_shared<std::atomic<bool>>(false);
  auto writer = cio::task::spawn(rwlock_set_writer(rwlock, writer_completed));
  co_await cio::task::yield_now();

  std::optional<cio::sync::RwLockReadGuard<int>> read;
  read.emplace(cio::sync::RwLockWriteGuard<int>::downgrade(std::move(write)));
  co_await cio::task::yield_now();
  const bool atomic = **read == 1 &&
                      !writer_completed->load(std::memory_order_acquire) &&
                      !rwlock.try_read().has_value();
  read.reset();
  const auto writer_result = co_await writer;
  auto final = co_await rwlock.read();
  co_return atomic &&writer_result.has_value() &&
      writer_completed->load(std::memory_order_acquire) && *final == 2;
}

Task<bool> rwlock_downgrade_mapping_root() {
  cio::sync::RwLock<RwLockRecord> rwlock{
      RwLockRecord{1, RwLockRecord::Nested{2}}};
  {
    auto write = co_await rwlock.write_owned();
    auto failed = cio::sync::RwLockWriteGuard<RwLockRecord>::try_downgrade_map(
        std::move(write), [](const RwLockRecord &) { return false; },
        [](const RwLockRecord &value) -> const int & { return value.first; });
    if (failed.has_value()) {
      co_return false;
    }
    auto original = std::move(failed).error();
    (*original).first = 4;
  }
  {
    auto write = co_await rwlock.write();
    auto read = cio::sync::RwLockWriteGuard<RwLockRecord>::downgrade_map(
        std::move(write),
        [](const RwLockRecord &value) -> const int & { return value.first; });
    if (*read != 4 || !rwlock.try_read().has_value()) {
      co_return false;
    }
  }
  {
    auto write = co_await rwlock.write_owned();
    auto succeeded =
        cio::sync::RwLockWriteGuard<RwLockRecord>::try_downgrade_map(
            std::move(write),
            [](const RwLockRecord &value) { return value.nested.value == 2; },
            [](const RwLockRecord &value) -> const int & {
              return value.nested.value;
            });
    if (!succeeded.has_value() || *succeeded.value() != 2) {
      co_return false;
    }
  }
  co_return rwlock.try_write().has_value();
}

Task<bool> rwlock_blocking_bridge_root() {
  cio::sync::RwLock<int> rwlock{1};
  std::optional<cio::sync::RwLockWriteGuard<int>> writer;
  writer.emplace(co_await rwlock.write());
  const auto read_started = std::make_shared<std::atomic<bool>>(false);
  const auto read_value = std::make_shared<std::atomic<int>>(0);
  auto blocking_read = cio::task::spawn_blocking(
      [](cio::sync::RwLock<int> child_rwlock,
         std::shared_ptr<std::atomic<bool>> started,
         std::shared_ptr<std::atomic<int>> value) {
        started->store(true, std::memory_order_release);
        auto guard = child_rwlock.blocking_read_owned();
        value->store(*guard, std::memory_order_release);
      },
      rwlock, read_started, read_value);
  while (!read_started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool read_waited = read_value->load(std::memory_order_acquire) == 0;
  writer.reset();
  const auto read_result = co_await blocking_read;

  std::optional<cio::sync::RwLockReadGuard<int>> reader;
  reader.emplace(co_await rwlock.read_owned());
  const auto write_started = std::make_shared<std::atomic<bool>>(false);
  auto blocking_write = cio::task::spawn_blocking(
      [](cio::sync::RwLock<int> child_rwlock,
         std::shared_ptr<std::atomic<bool>> started) {
        started->store(true, std::memory_order_release);
        auto guard = child_rwlock.blocking_write_owned();
        *guard = 9;
      },
      rwlock, write_started);
  while (!write_started->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const bool write_waited = **reader == 1;
  reader.reset();
  const auto write_result = co_await blocking_write;
  auto final = co_await rwlock.read();
  co_return read_waited &&read_result.has_value() &&
      read_value->load(std::memory_order_acquire) == 1 &&
      write_waited &&write_result.has_value() && *final == 9;
}

Task<void> rwlock_shutdown_reader(cio::sync::RwLock<int> rwlock,
                                  std::shared_ptr<std::atomic<bool>> entered,
                                  std::shared_ptr<std::atomic<bool>> acquired) {
  entered->store(true, std::memory_order_release);
  auto guard = co_await rwlock.read_owned();
  acquired->store(true, std::memory_order_release);
  if (*guard < 0) {
    throw std::runtime_error{"不可能的读值"};
  }
}

Task<void> rwlock_shutdown_writer(cio::sync::RwLock<int> rwlock,
                                  std::shared_ptr<std::atomic<bool>> entered,
                                  std::shared_ptr<std::atomic<bool>> acquired) {
  entered->store(true, std::memory_order_release);
  auto guard = co_await rwlock.write_owned();
  acquired->store(true, std::memory_order_release);
  ++*guard;
}

template <typename Factory>
Task<bool>
establish_shutdown_waiter(Factory factory,
                          std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(factory());
  (void)detached;
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return true;
}

Task<bool> rwlock_release_abort_race_root() {
  for (int iteration = 0; iteration < 100; ++iteration) {
    auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 4);
    std::optional<cio::sync::RwLockReadGuard<int>> owner;
    owner.emplace(co_await rwlock.read_owned());
    auto child = cio::task::spawn(cio::task::owned(
        [](cio::sync::RwLock<int> child_rwlock) -> Task<void> {
          auto guard = co_await child_rwlock.write_owned();
          ++*guard;
        },
        rwlock));
    co_await cio::task::yield_now();

    std::jthread releaser{
        [held = std::move(owner)]() mutable { held.reset(); }};
    child.abort();
    const auto child_result = co_await child;
    releaser.join();
    if (!child_result.has_value() && !child_result.error().is_cancelled()) {
      co_return false;
    }
    auto final = rwlock.try_write();
    if (!final.has_value() || (*final.value() != 0 && *final.value() != 1)) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> rwlock_multi_contention_root() {
  auto rwlock = cio::sync::RwLock<int>::with_max_readers(0, 64);
  const auto active_readers = std::make_shared<std::atomic<int>>(0);
  const auto maximum_readers = std::make_shared<std::atomic<int>>(0);
  const auto active_writers = std::make_shared<std::atomic<int>>(0);
  const auto violation = std::make_shared<std::atomic<bool>>(false);
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(64);

  for (int index = 0; index < 64; ++index) {
    if (index % 4 == 0) {
      handles.push_back(cio::task::spawn(cio::task::owned(
          [](cio::sync::RwLock<int> child_rwlock,
             std::shared_ptr<std::atomic<int>> readers,
             std::shared_ptr<std::atomic<int>> writers,
             std::shared_ptr<std::atomic<bool>> failed) -> Task<void> {
            for (int iteration = 0; iteration < 10; ++iteration) {
              auto guard = co_await child_rwlock.write_owned();
              const auto now =
                  writers->fetch_add(1, std::memory_order_acq_rel) + 1;
              if (now != 1 || readers->load(std::memory_order_acquire) != 0) {
                failed->store(true, std::memory_order_release);
              }
              co_await cio::task::yield_now();
              ++*guard;
              writers->fetch_sub(1, std::memory_order_acq_rel);
              co_await cio::task::yield_now();
            }
          },
          rwlock, active_readers, active_writers, violation)));
    } else {
      handles.push_back(cio::task::spawn(cio::task::owned(
          [](cio::sync::RwLock<int> child_rwlock,
             std::shared_ptr<std::atomic<int>> readers,
             std::shared_ptr<std::atomic<int>> maximum,
             std::shared_ptr<std::atomic<int>> writers,
             std::shared_ptr<std::atomic<bool>> failed) -> Task<void> {
            for (int iteration = 0; iteration < 10; ++iteration) {
              auto guard = co_await child_rwlock.read_owned();
              const auto now =
                  readers->fetch_add(1, std::memory_order_acq_rel) + 1;
              update_maximum(maximum, now);
              if (writers->load(std::memory_order_acquire) != 0 || *guard < 0) {
                failed->store(true, std::memory_order_release);
              }
              co_await cio::task::yield_now();
              readers->fetch_sub(1, std::memory_order_acq_rel);
              co_await cio::task::yield_now();
            }
          },
          rwlock, active_readers, maximum_readers, active_writers, violation)));
    }
  }

  for (auto &handle : handles) {
    const auto result = co_await handle;
    if (!result.has_value()) {
      co_return false;
    }
  }
  auto final = co_await rwlock.read();
  co_return *final == 160 &&
      active_readers->load(std::memory_order_acquire) == 0 &&
      active_writers->load(std::memory_order_acquire) == 0 &&
      maximum_readers->load(std::memory_order_acquire) > 1 &&
      !violation->load(std::memory_order_acquire);
}

void test_rwlock_basic_unique_and_blocking() {
  Runtime runtime;
  check(runtime.block_on(rwlock_basic_root()),
        "RwLock 基本 read/write/try/max_readers 语义错误");
  check(runtime.block_on(rwlock_blocking_rejected_root()),
        "异步上下文未拒绝 RwLock blocking 操作");

  bool zero_rejected = false;
  bool oversized_rejected = false;
  try {
    (void)cio::sync::RwLock<int>::with_max_readers(0, 0);
  } catch (const std::invalid_argument &) {
    zero_rejected = true;
  }
  try {
    (void)cio::sync::RwLock<int>::with_max_readers(
        0, std::numeric_limits<std::uint32_t>::max());
  } catch (const std::invalid_argument &) {
    oversized_rejected = true;
  }

  auto unique = cio::sync::RwLock<int>::const_with_max_readers(1, 2);
  bool guarded_copy_blocked = false;
  {
    auto unique_guard = unique.get_mut();
    *unique_guard = 4;
    auto copy_while_guarded = unique;
    guarded_copy_blocked = !copy_while_guarded.try_read().has_value();
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
  check(zero_rejected && oversized_rejected && shared_get_rejected &&
            shared_into_rejected && guarded_copy_blocked &&
            std::move(unique).into_inner() == 4,
        "RwLock 构造边界或 get_mut/into_inner 唯一性错误");

  cio::sync::RwLock<int> rwlock{1};
  std::optional<cio::sync::RwLockWriteGuard<int>> owner;
  owner.emplace(std::move(rwlock.try_write()).value());
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto acquired = std::make_shared<std::atomic<bool>>(false);
  std::jthread waiter{[rwlock, started, acquired] {
    started->store(true, std::memory_order_release);
    auto guard = rwlock.blocking_read_owned();
    if (*guard == 1) {
      acquired->store(true, std::memory_order_release);
    }
  }};
  while (!started->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(10ms);
  check(!acquired->load(std::memory_order_acquire),
        "blocking_read 在写 guard 释放前取得锁");
  owner.reset();
  waiter.join();
  check(acquired->load(std::memory_order_acquire),
        "blocking_read 跨线程等待失败");
}

void test_rwlock_readers_and_writer_preference() {
  Runtime runtime;
  check(runtime.block_on(rwlock_max_readers_root()),
        "RwLock 未限制并发读者或未允许共享读取");
  check(runtime.block_on(rwlock_writer_preference_root()),
        "RwLock 写者优先/FIFO 队列语义错误");
}

void test_rwlock_cancellation_and_no_poison() {
  Runtime runtime;
  check(runtime.block_on(rwlock_cancel_partial_writer_root()),
        "RwLock 取消部分取得许可的写者未转交许可");
  check(runtime.block_on(rwlock_cancel_acquired_writer_root()),
        "RwLock 取消已满足未恢复写者未转交许可");
  check(runtime.block_on(rwlock_cancel_pending_reader_root()),
        "RwLock 取消排队读者未清理等待状态");
  check(runtime.block_on(rwlock_cancel_acquired_reader_root()),
        "RwLock 取消已满足未恢复读者未转交许可");
  check(runtime.block_on(rwlock_no_poison_root()),
        "RwLock 写 task 异常后 poison 或未释放");
}

void test_rwlock_mapping_and_downgrade() {
  Runtime runtime;
  check(runtime.block_on(rwlock_mapping_root()),
        "RwLock read/write map、nested map 或 try_map 错误");
  check(runtime.block_on(rwlock_downgrade_root()),
        "RwLock write->read downgrade 不是原子转换");
  check(runtime.block_on(rwlock_downgrade_mapping_root()),
        "RwLock downgrade_map/try_downgrade_map 错误");
}

void test_rwlock_blocking_bridge() {
  Runtime runtime;
  check(runtime.block_on(rwlock_blocking_bridge_root()),
        "spawn_blocking 与 RwLock blocking read/write 桥接错误");
}

void test_rwlock_shutdown_cleanup() {
  {
    cio::sync::RwLock<int> rwlock{0};
    std::optional<cio::sync::RwLockWriteGuard<int>> owner;
    owner.emplace(std::move(rwlock.try_write()).value());
    const auto entered = std::make_shared<std::atomic<bool>>(false);
    const auto acquired = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime runtime;
      check(runtime.block_on(establish_shutdown_waiter(
                [rwlock, entered, acquired] {
                  return rwlock_shutdown_reader(rwlock, entered, acquired);
                },
                entered)),
            "运行时关闭前未建立 RwLock 读等待者");
    }
    check(!acquired->load(std::memory_order_acquire),
          "运行时关闭时排队 RwLock 读者错误取得锁");
    owner.reset();
    check(rwlock.try_write().has_value(),
          "运行时关闭未清理排队 RwLock 读者或泄漏许可");
  }

  {
    cio::sync::RwLock<int> rwlock{0};
    std::optional<cio::sync::RwLockReadGuard<int>> owner;
    owner.emplace(std::move(rwlock.try_read()).value());
    const auto entered = std::make_shared<std::atomic<bool>>(false);
    const auto acquired = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime runtime;
      check(runtime.block_on(establish_shutdown_waiter(
                [rwlock, entered, acquired] {
                  return rwlock_shutdown_writer(rwlock, entered, acquired);
                },
                entered)),
            "运行时关闭前未建立 RwLock 写等待者");
    }
    check(!acquired->load(std::memory_order_acquire),
          "运行时关闭时排队 RwLock 写者错误取得锁");
    owner.reset();
    check(rwlock.try_write().has_value(),
          "运行时关闭未清理排队 RwLock 写者或泄漏许可");
  }
}

void test_rwlock_cross_thread_and_multi() {
  Runtime current;
  check(current.block_on(rwlock_release_abort_race_root()),
        "current-thread RwLock release/abort 竞态错误");

  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(4).build();
  check(multi.block_on(
            cio::task::assume_portable(rwlock_release_abort_race_root())),
        "multi-thread RwLock release/abort 竞态错误");
  check(multi.block_on(
            cio::task::assume_portable(rwlock_multi_contention_root())),
        "multi-thread RwLock 读写排他、并发读或计数错误");
}

static_assert(cio::Send<cio::sync::RwLock<int>>);
static_assert(cio::Sync<cio::sync::RwLock<int>>);
static_assert(cio::Send<cio::sync::RwLockReadGuard<int>>);
static_assert(cio::Sync<cio::sync::RwLockReadGuard<int>>);
static_assert(cio::Send<cio::sync::RwLockWriteGuard<int>>);
static_assert(!cio::Sync<cio::sync::RwLockWriteGuard<int>>);
static_assert(cio::Send<cio::sync::RwLockMappedWriteGuard<int, int>>);
static_assert(!cio::Sync<cio::sync::RwLockMappedWriteGuard<int, int>>);
static_assert(cio::Send<cio::sync::RwLock<int>::Read>);
static_assert(!cio::Sync<cio::sync::RwLock<int>::Read>);
static_assert(cio::Send<cio::sync::RwLock<int>::Read::Awaiter>);
static_assert(!cio::Sync<cio::sync::RwLock<int>::Read::Awaiter>);
static_assert(cio::Send<cio::sync::RwLock<int>::Write>);
static_assert(!cio::Sync<cio::sync::RwLock<int>::Write>);
static_assert(cio::Send<cio::sync::RwLock<int>::Write::Awaiter>);
static_assert(!cio::Sync<cio::sync::RwLock<int>::Write::Awaiter>);
static_assert(!cio::Send<cio::sync::RwLock<UnknownMobility>::Read>);
static_assert(!cio::Send<cio::sync::RwLock<UnknownMobility>::Write>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"rwlock basic unique blocking", test_rwlock_basic_unique_and_blocking},
      {"rwlock readers writer preference",
       test_rwlock_readers_and_writer_preference},
      {"rwlock cancellation no poison", test_rwlock_cancellation_and_no_poison},
      {"rwlock mapping downgrade", test_rwlock_mapping_and_downgrade},
      {"rwlock blocking bridge", test_rwlock_blocking_bridge},
      {"rwlock shutdown cleanup", test_rwlock_shutdown_cleanup},
      {"rwlock cross-thread multi", test_rwlock_cross_thread_and_multi},
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
  std::cout << "RwLock 全部通过：" << passed << " 项\n";
  return 0;
}
