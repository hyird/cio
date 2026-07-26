#include <array>
#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/barrier.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<bool> barrier_immediate_root(std::size_t participants) {
  cio::sync::Barrier barrier{participants};
  const auto first = co_await barrier.wait();
  const auto second = co_await barrier.wait();
  co_return first.is_leader() && second.is_leader();
}

Task<cio::sync::BarrierWaitResult>
barrier_wait_child(cio::sync::Barrier barrier,
                   std::shared_ptr<std::atomic<int>> entered,
                   std::shared_ptr<std::atomic<int>> completed) {
  entered->fetch_add(1, std::memory_order_release);
  auto result = co_await barrier.wait();
  completed->fetch_add(1, std::memory_order_release);
  co_return result;
}

Task<bool> barrier_basic_root() {
  cio::sync::Barrier barrier{3};
  const auto entered = std::make_shared<std::atomic<int>>(0);
  const auto completed = std::make_shared<std::atomic<int>>(0);
  auto first =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));
  auto second =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));

  while (entered->load(std::memory_order_acquire) != 2) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  if (completed->load(std::memory_order_acquire) != 0) {
    co_return false;
  }

  const auto third = co_await barrier.wait();
  const auto first_join = co_await first;
  const auto second_join = co_await second;
  if (!first_join.has_value() || !second_join.has_value()) {
    co_return false;
  }

  const int leaders = static_cast<int>(third.is_leader()) +
                      static_cast<int>(first_join.value().is_leader()) +
                      static_cast<int>(second_join.value().is_leader());
  co_return leaders == 1 && completed->load(std::memory_order_acquire) == 2;
}

Task<bool> barrier_unpolled_wait_root() {
  cio::sync::Barrier barrier{2};
  {
    auto never_polled = barrier.wait();
    (void)never_polled;
  }

  const auto entered = std::make_shared<std::atomic<int>>(0);
  const auto completed = std::make_shared<std::atomic<int>>(0);
  auto first =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));
  while (entered->load(std::memory_order_acquire) != 1) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  if (completed->load(std::memory_order_acquire) != 0) {
    co_return false;
  }

  const auto second = co_await barrier.wait();
  const auto joined = co_await first;
  co_return joined.has_value() && second.is_leader() &&
      !joined.value().is_leader() &&
      completed->load(std::memory_order_acquire) == 1;
}

template <std::size_t Generations> struct BarrierCounters final {
  std::array<std::atomic<int>, Generations> arrivals{};
  std::array<std::atomic<int>, Generations> leaders{};
  std::atomic<bool> visibility_failure{false};
};

template <std::size_t Generations>
Task<void> barrier_generation_child(
    cio::sync::Barrier barrier, std::size_t participants,
    std::shared_ptr<BarrierCounters<Generations>> counters) {
  for (std::size_t generation = 0; generation < Generations; ++generation) {
    counters->arrivals[generation].fetch_add(1, std::memory_order_release);
    const auto result = co_await barrier.wait();
    if (counters->arrivals[generation].load(std::memory_order_acquire) !=
        static_cast<int>(participants)) {
      counters->visibility_failure.store(true, std::memory_order_release);
    }
    if (result.is_leader()) {
      counters->leaders[generation].fetch_add(1, std::memory_order_relaxed);
    }
    co_await cio::task::yield_now();
  }
}

template <std::size_t Generations>
Task<bool> barrier_reuse_root(std::size_t participants) {
  cio::sync::Barrier barrier{participants};
  const auto counters = std::make_shared<BarrierCounters<Generations>>();
  std::vector<cio::task::JoinHandle<void>> handles;
  handles.reserve(participants);
  for (std::size_t index = 0; index < participants; ++index) {
    handles.push_back(cio::task::spawn(
        cio::task::assume_portable(barrier_generation_child<Generations>(
            barrier, participants, counters))));
  }

  for (auto &handle : handles) {
    const auto joined = co_await handle;
    if (!joined.has_value()) {
      co_return false;
    }
  }

  if (counters->visibility_failure.load(std::memory_order_acquire)) {
    co_return false;
  }
  for (std::size_t generation = 0; generation < Generations; ++generation) {
    if (counters->arrivals[generation].load(std::memory_order_acquire) !=
            static_cast<int>(participants) ||
        counters->leaders[generation].load(std::memory_order_acquire) != 1) {
      co_return false;
    }
  }
  co_return true;
}

Task<bool> barrier_cancellation_root() {
  cio::sync::Barrier barrier{3};
  const auto entered = std::make_shared<std::atomic<int>>(0);
  const auto completed = std::make_shared<std::atomic<int>>(0);
  auto survivor =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));
  auto cancelled =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));
  while (entered->load(std::memory_order_acquire) != 2) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();

  cancelled.abort();
  const auto cancelled_join = co_await cancelled;
  if (cancelled_join.has_value() || !cancelled_join.error().is_cancelled()) {
    co_return false;
  }

  auto replacement =
      cio::task::spawn(barrier_wait_child(barrier, entered, completed));
  const auto survivor_join = co_await survivor;
  const auto replacement_join = co_await replacement;
  if (!survivor_join.has_value() || !replacement_join.has_value()) {
    co_return false;
  }

  const int observable_leaders =
      static_cast<int>(survivor_join.value().is_leader()) +
      static_cast<int>(replacement_join.value().is_leader());
  co_return observable_leaders == 1 &&
      completed->load(std::memory_order_acquire) == 2;
}

Task<void> barrier_detached_waiter(cio::sync::Barrier barrier,
                                   std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  (void)co_await barrier.wait();
}

Task<bool>
launch_barrier_waiter_for_shutdown(cio::sync::Barrier barrier,
                                   std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(barrier_detached_waiter(barrier, entered));
  (void)detached;
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return true;
}

Task<bool> finish_cancelled_generation(cio::sync::Barrier barrier) {
  const auto result = co_await barrier.wait();
  co_return result.is_leader();
}

void test_barrier_immediate_and_basic() {
  Runtime runtime;
  check(runtime.block_on(barrier_immediate_root(0)),
        "Barrier(0) 未按 Tokio 的 Barrier(1) 语义处理");
  check(runtime.block_on(barrier_immediate_root(1)),
        "Barrier(1) 未立即完成或 leader 不唯一");
  check(runtime.block_on(barrier_basic_root()),
        "Barrier 基本 rendezvous 或 leader 语义错误");
  check(runtime.block_on(barrier_unpolled_wait_root()),
        "未 poll 的 Barrier Wait 错误计入到达数");
}

void test_barrier_reuse_and_visibility() {
  Runtime runtime;
  check(runtime.block_on(barrier_reuse_root<32>(8)),
        "current-thread Barrier 重复代、唯一 leader 或可见性错误");
}

void test_barrier_cancellation_and_shutdown() {
  Runtime runtime;
  check(runtime.block_on(barrier_cancellation_root()),
        "Barrier 取消未保持 Tokio 的非 cancel-safe 到达计数");

  cio::sync::Barrier shutdown_barrier{2};
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  {
    Runtime shutting_down;
    check(shutting_down.block_on(
              launch_barrier_waiter_for_shutdown(shutdown_barrier, entered)),
          "运行时关闭场景未建立 Barrier 等待者");
  }
  Runtime continuation;
  check(continuation.block_on(finish_cancelled_generation(shutdown_barrier)),
        "运行时关闭后 Barrier 取消到达数被错误回滚");
}

void test_barrier_multi_thread_contention() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  check(
      runtime.block_on(cio::task::assume_portable(barrier_reuse_root<100>(48))),
      "multi-thread Barrier 高竞争重复代语义错误");
}

static_assert(cio::Send<cio::sync::Barrier>);
static_assert(cio::Sync<cio::sync::Barrier>);
static_assert(cio::Send<cio::sync::Barrier::Wait>);
static_assert(!cio::Sync<cio::sync::Barrier::Wait>);
static_assert(cio::Send<cio::sync::Barrier::Wait::Awaiter>);
static_assert(!cio::Sync<cio::sync::Barrier::Wait::Awaiter>);
static_assert(cio::Send<cio::sync::BarrierWaitResult>);
static_assert(cio::Sync<cio::sync::BarrierWaitResult>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"barrier immediate basic", test_barrier_immediate_and_basic},
      {"barrier reuse visibility", test_barrier_reuse_and_visibility},
      {"barrier cancellation shutdown", test_barrier_cancellation_and_shutdown},
      {"barrier multi-thread contention", test_barrier_multi_thread_contention},
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

  std::cout << "Barrier 全部通过：" << passed << " 项\n";
  return 0;
}
