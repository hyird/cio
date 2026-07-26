#include <atomic>
#include <barrier>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/once_cell.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<int> immediate_value(int value) { co_return value; }

Task<int> yielding_value(int value, std::shared_ptr<std::atomic<int>> calls) {
  calls->fetch_add(1, std::memory_order_relaxed);
  co_await cio::task::yield_now();
  co_return value;
}

Task<int> waiting_value(int value, std::shared_ptr<cio::sync::Semaphore> gate,
                        std::shared_ptr<std::atomic<int>> entered) {
  entered->fetch_add(1, std::memory_order_release);
  auto permit = co_await gate->acquire_owned();
  if (!permit.has_value()) {
    throw std::runtime_error{"OnceCell 测试 gate 被意外关闭"};
  }
  co_return value;
}

Task<int> throwing_value() {
  co_await cio::task::yield_now();
  throw std::runtime_error{"预期的 OnceCell initializer 异常"};
  co_return 0;
}

Task<cio::Result<int, std::string>> failing_value(std::string error) {
  co_await cio::task::yield_now();
  co_return cio::Result<int, std::string>::failure(std::move(error));
}

Task<cio::Result<int, std::string>> successful_try_value(int value) {
  co_return cio::Result<int, std::string>::success(value);
}

struct ImmediateFactory final {
  int value{0};
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const { return immediate_value(value); }
};

struct YieldingFactory final {
  int value{0};
  std::shared_ptr<std::atomic<int>> calls;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const { return yielding_value(value, calls); }
};

struct WaitingFactory final {
  int value{0};
  std::shared_ptr<cio::sync::Semaphore> gate;
  std::shared_ptr<std::atomic<int>> entered;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const { return waiting_value(value, gate, entered); }
};

struct ThrowingFactory final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const { return throwing_value(); }
};

struct SynchronousThrowFactory final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<int> operator()() const {
    throw std::runtime_error{"预期的 OnceCell factory 同步异常"};
  }
};

struct FailingFactory final {
  std::string error;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<cio::Result<int, std::string>> operator()() const {
    return failing_value(error);
  }
};

struct SuccessfulTryFactory final {
  int value{0};
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  Task<cio::Result<int, std::string>> operator()() const {
    return successful_try_value(value);
  }
};

struct TransferCounters final {
  std::atomic<int> constructions{0};
  std::atomic<int> destructions{0};
  std::atomic<int> live{0};
  std::atomic<int> copy_attempts{0};
  std::atomic<int> move_attempts{0};
  std::atomic<bool> throw_on_copy{false};
  std::atomic<bool> throw_on_move{false};
};

struct ThrowingTransferValue final {
  std::shared_ptr<TransferCounters> counters;
  int value{0};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  ThrowingTransferValue(std::shared_ptr<TransferCounters> observed, int stored)
      : counters{std::move(observed)}, value{stored} {
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingTransferValue(const ThrowingTransferValue &other)
      : counters{other.counters}, value{other.value} {
    counters->copy_attempts.fetch_add(1, std::memory_order_relaxed);
    if (counters->throw_on_copy.load(std::memory_order_relaxed)) {
      throw std::runtime_error{"预期的 OnceCell 值复制异常"};
    }
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingTransferValue(ThrowingTransferValue &&other)
      : counters{other.counters}, value{other.value} {
    counters->move_attempts.fetch_add(1, std::memory_order_relaxed);
    if (counters->throw_on_move.load(std::memory_order_relaxed)) {
      throw std::runtime_error{"预期的 OnceCell 值移动异常"};
    }
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingTransferValue &operator=(const ThrowingTransferValue &) = delete;
  ThrowingTransferValue &operator=(ThrowingTransferValue &&) = delete;

  ~ThrowingTransferValue() {
    counters->destructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_sub(1, std::memory_order_relaxed);
  }
};

struct TrackedMoveOnlyValue final {
  std::shared_ptr<TransferCounters> counters;
  int value{0};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  TrackedMoveOnlyValue(std::shared_ptr<TransferCounters> observed, int stored)
      : counters{std::move(observed)}, value{stored} {
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  TrackedMoveOnlyValue(const TrackedMoveOnlyValue &) = delete;
  TrackedMoveOnlyValue &operator=(const TrackedMoveOnlyValue &) = delete;

  TrackedMoveOnlyValue(TrackedMoveOnlyValue &&other) noexcept
      : counters{other.counters}, value{other.value} {
    counters->move_attempts.fetch_add(1, std::memory_order_relaxed);
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  TrackedMoveOnlyValue &operator=(TrackedMoveOnlyValue &&) = delete;

  ~TrackedMoveOnlyValue() {
    counters->destructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_sub(1, std::memory_order_relaxed);
  }
};

void check_balanced_lifetime(const std::shared_ptr<TransferCounters> &counters,
                             std::string_view message) {
  check(counters->live.load(std::memory_order_relaxed) == 0 &&
            counters->constructions.load(std::memory_order_relaxed) ==
                counters->destructions.load(std::memory_order_relaxed),
        message);
}

template <typename T>
bool once_cell_source_is_invalid(cio::sync::OnceCell<T> &cell) {
  try {
    (void)cell.initialized();
  } catch (const std::logic_error &) {
    return true;
  }
  return false;
}

Task<bool> once_cell_async_basic_root() {
  cio::sync::OnceCell<int> cell;
  const auto first = co_await cell.get_or_init(ImmediateFactory{41});
  const auto calls = std::make_shared<std::atomic<int>>(0);
  const auto second = co_await cell.get_or_init(YieldingFactory{99, calls});
  co_return first && second && *first == 41 && *second == 41 &&
      calls->load(std::memory_order_relaxed) == 0;
}

Task<bool> once_cell_try_error_and_exception_root() {
  cio::sync::OnceCell<int> cell;
  const auto failed = co_await cell.get_or_try_init(FailingFactory{"expected"});
  if (failed.has_value() || failed.error() != "expected" ||
      cell.initialized()) {
    co_return false;
  }

  bool async_threw = false;
  try {
    (void)co_await cell.get_or_init(ThrowingFactory{});
  } catch (const std::runtime_error &) {
    async_threw = true;
  }
  if (!async_threw || cell.initialized()) {
    co_return false;
  }

  bool synchronous_threw = false;
  try {
    (void)co_await cell.get_or_init(SynchronousThrowFactory{});
  } catch (const std::runtime_error &) {
    synchronous_threw = true;
  }
  if (!synchronous_threw || cell.initialized()) {
    co_return false;
  }

  const auto succeeded =
      co_await cell.get_or_try_init(SuccessfulTryFactory{73});
  co_return succeeded.has_value() && succeeded.value() &&
      *succeeded.value() == 73 && cell.initialized();
}

Task<bool> once_cell_cancel_before_poll_root() {
  cio::sync::OnceCell<int> cell;
  const auto calls = std::make_shared<std::atomic<int>>(0);
  auto cancelled =
      cio::task::spawn(cell.get_or_init(YieldingFactory{1, calls}));
  cancelled.abort();
  const auto joined = co_await cancelled;
  const auto retry = co_await cell.get_or_init(ImmediateFactory{2});
  co_return !joined.has_value() && joined.error().is_cancelled() &&
      calls->load(std::memory_order_relaxed) == 0 && retry && *retry == 2;
}

Task<bool> once_cell_cancel_initializer_root() {
  cio::sync::OnceCell<int> cell;
  const auto gate = std::make_shared<cio::sync::Semaphore>(0);
  const auto entered = std::make_shared<std::atomic<int>>(0);
  auto initializer =
      cio::task::spawn(cell.get_or_init(WaitingFactory{1, gate, entered}));
  while (entered->load(std::memory_order_acquire) != 1) {
    co_await cio::task::yield_now();
  }

  auto set_while_initializing = cell.set(8);
  if (set_while_initializing.has_value() ||
      !set_while_initializing.error().is_initializing_err() ||
      set_while_initializing.error().value() != 8) {
    co_return false;
  }

  auto survivor = cio::task::spawn(cell.get_or_init(ImmediateFactory{2}));
  co_await cio::task::yield_now();
  initializer.abort();
  const auto cancelled = co_await initializer;
  const auto survived = co_await survivor;
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      survived.has_value() && survived.value() && *survived.value() == 2 &&
      cell.initialized();
}

Task<bool> once_cell_cancel_queued_waiter_root() {
  cio::sync::OnceCell<int> cell;
  const auto gate = std::make_shared<cio::sync::Semaphore>(0);
  const auto entered = std::make_shared<std::atomic<int>>(0);
  auto owner =
      cio::task::spawn(cell.get_or_init(WaitingFactory{1, gate, entered}));
  while (entered->load(std::memory_order_acquire) != 1) {
    co_await cio::task::yield_now();
  }
  auto queued = cio::task::spawn(cell.get_or_init(ImmediateFactory{2}));
  co_await cio::task::yield_now();
  queued.abort();
  const auto cancelled_waiter = co_await queued;
  owner.abort();
  const auto cancelled_owner = co_await owner;
  const auto retry = co_await cell.get_or_init(ImmediateFactory{3});
  co_return !cancelled_waiter.has_value() &&
      cancelled_waiter.error().is_cancelled() && !cancelled_owner.has_value() &&
      cancelled_owner.error().is_cancelled() && retry && *retry == 3;
}

Task<bool> once_cell_contention_root(std::size_t participants) {
  cio::sync::OnceCell<int> cell;
  const auto calls = std::make_shared<std::atomic<int>>(0);
  std::vector<cio::task::JoinHandle<std::shared_ptr<const int>>> handles;
  handles.reserve(participants);
  for (std::size_t index = 0; index < participants; ++index) {
    handles.push_back(cio::task::spawn(cio::task::assume_portable(
        cell.get_or_init(YieldingFactory{97, calls}))));
  }
  for (auto &handle : handles) {
    const auto joined = co_await handle;
    if (!joined.has_value() || !joined.value() || *joined.value() != 97) {
      co_return false;
    }
  }
  co_return calls->load(std::memory_order_relaxed) == 1;
}

Task<bool>
launch_detached_initializer(std::shared_ptr<cio::sync::OnceCell<int>> cell,
                            std::shared_ptr<cio::sync::Semaphore> gate,
                            std::shared_ptr<std::atomic<int>> entered) {
  auto detached =
      cio::task::spawn(cell->get_or_init(WaitingFactory{1, gate, entered}));
  (void)detached;
  while (entered->load(std::memory_order_acquire) != 1) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return true;
}

Task<bool>
retry_after_shutdown(std::shared_ptr<cio::sync::OnceCell<int>> cell) {
  const auto value = co_await cell->get_or_init(ImmediateFactory{11});
  co_return value && *value == 11;
}

void test_once_cell_constructors_set_and_owned_views() {
  cio::sync::OnceCell<int> empty;
  check(!empty.initialized() && !empty.get(), "OnceCell::new/get 空状态错误");
  auto set = empty.set(1);
  check(set.has_value() && empty.initialized() && *empty.get() == 1,
        "OnceCell::set 未发布值");

  auto duplicate = empty.set(2);
  check(!duplicate.has_value() && duplicate.error().is_already_init_err() &&
            duplicate.error().value() == 2 &&
            duplicate.error().message() == "AlreadyInitializedError" &&
            duplicate.error().debug_string() == "AlreadyInitializedError(2)",
        "OnceCell::set 重复初始化错误分类、格式或值所有权错误");
  std::ostringstream display;
  display << duplicate.error();
  check(display.str() == "AlreadyInitializedError",
        "OnceCell SetError Display 映射错误");

  auto cloned = empty.clone();
  check(cloned == empty &&
            cloned.debug_string() == "OnceCell { value: Some(1) }",
        "OnceCell clone/equality/debug 映射错误");

  auto none = cio::sync::OnceCell<int>::new_with(std::nullopt);
  auto some = cio::sync::OnceCell<int>::new_with(3);
  auto const_empty = cio::sync::OnceCell<int>::const_new();
  auto const_some = cio::sync::OnceCell<int>::const_new_with(4);
  auto from = cio::sync::OnceCell<int>::from(5);
  check(!none.initialized() && !const_empty.initialized() && *some.get() == 3 &&
            *const_some.get() == 4 && *from.get() == 5,
        "OnceCell 构造工厂语义错误");

  auto snapshot = empty.get();
  bool snapshot_rejected_mutation = false;
  try {
    (void)empty.get_mut();
  } catch (const std::logic_error &) {
    snapshot_rejected_mutation = true;
  }
  snapshot.reset();

  {
    auto guard = empty.get_mut();
    check(guard.has_value(), "OnceCell::get_mut 未返回初始化值");
    guard->update([](int &value) { value += 9; });
    check(guard->copy() == 10, "OnceCellMutGuard 修改或读取错误");
  }
  check(snapshot_rejected_mutation && *empty.get() == 10,
        "OnceCell::get_mut owning snapshot 边界错误");

  auto taken = empty.take();
  check(taken && *taken == 10 && !empty.initialized() &&
            empty.set(12).has_value(),
        "OnceCell::take 未恢复为空或无法再次 set");
  auto inner = std::move(empty).into_inner();
  bool consumed_source_rejected = false;
  try {
    (void)empty.initialized();
  } catch (const std::logic_error &) {
    consumed_source_rejected = true;
  }
  check(inner && *inner == 12 && consumed_source_rejected,
        "OnceCell::into_inner 未消费源对象或返回值错误");

  cio::sync::OnceCell<std::unique_ptr<int>> move_only;
  check(move_only.set(std::make_unique<int>(21)).has_value(),
        "OnceCell 未接受 move-only 值");
  auto move_only_snapshot = move_only.get();
  check(move_only_snapshot && **move_only_snapshot == 21,
        "OnceCell move-only owning snapshot 错误");
  move_only_snapshot.reset();
  auto move_only_inner = move_only.take();
  check(move_only_inner && **move_only_inner == 21 && !move_only.initialized(),
        "OnceCell move-only take 错误");
}

void test_once_cell_take_into_inner_exception_safety() {
  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 11});
      const auto attempts_before =
          counters->move_attempts.load(std::memory_order_relaxed);
      counters->throw_on_move.store(true, std::memory_order_relaxed);
      bool move_threw = false;
      try {
        (void)cell.take();
      } catch (const std::runtime_error &) {
        move_threw = true;
      }
      counters->throw_on_move.store(false, std::memory_order_relaxed);

      check(move_threw &&
                counters->move_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                !cell.initialized() && !cell.get(),
            "OnceCell::take 移动异常未传播、未释放锁或未恢复为空");
      check(cell.set(ThrowingTransferValue{counters, 12}).has_value(),
            "OnceCell::take 移动异常后无法再次 set");
      auto recovered = cell.take();
      check(recovered && recovered->value == 12 && !cell.initialized(),
            "OnceCell::take 移动异常后状态无法安全复用");
    }
    check_balanced_lifetime(counters,
                            "OnceCell::take 移动异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 21});
      auto snapshot = cell.get();
      const auto attempts_before =
          counters->copy_attempts.load(std::memory_order_relaxed);
      counters->throw_on_copy.store(true, std::memory_order_relaxed);
      bool copy_threw = false;
      try {
        (void)cell.take();
      } catch (const std::runtime_error &) {
        copy_threw = true;
      }
      counters->throw_on_copy.store(false, std::memory_order_relaxed);

      check(copy_threw &&
                counters->copy_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                !cell.initialized() && !cell.get() && snapshot &&
                snapshot->value == 21,
            "OnceCell::take 复制异常未传播、未释放锁或破坏 owning snapshot");
      check(cell.set(ThrowingTransferValue{counters, 22}).has_value(),
            "OnceCell::take 复制异常后无法再次 set");
      auto recovered = cell.take();
      check(recovered && recovered->value == 22 && !cell.initialized(),
            "OnceCell::take 复制异常后状态无法安全复用");
      snapshot.reset();
    }
    check_balanced_lifetime(counters,
                            "OnceCell::take 复制异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 31});
      const auto attempts_before =
          counters->move_attempts.load(std::memory_order_relaxed);
      counters->throw_on_move.store(true, std::memory_order_relaxed);
      bool move_threw = false;
      try {
        (void)std::move(cell).into_inner();
      } catch (const std::runtime_error &) {
        move_threw = true;
      }
      counters->throw_on_move.store(false, std::memory_order_relaxed);
      check(move_threw &&
                counters->move_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                once_cell_source_is_invalid(cell),
            "OnceCell::into_inner 移动异常未传播或源对象仍然有效");
    }
    check_balanced_lifetime(counters,
                            "OnceCell::into_inner 移动异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 41});
      auto snapshot = cell.get();
      const auto attempts_before =
          counters->copy_attempts.load(std::memory_order_relaxed);
      counters->throw_on_copy.store(true, std::memory_order_relaxed);
      bool copy_threw = false;
      try {
        (void)std::move(cell).into_inner();
      } catch (const std::runtime_error &) {
        copy_threw = true;
      }
      counters->throw_on_copy.store(false, std::memory_order_relaxed);
      check(copy_threw &&
                counters->copy_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                once_cell_source_is_invalid(cell) && snapshot &&
                snapshot->value == 41,
            "OnceCell::into_inner 复制异常未传播、源仍有效或 snapshot 损坏");
      snapshot.reset();
    }
    check_balanced_lifetime(counters,
                            "OnceCell::into_inner 复制异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<TrackedMoveOnlyValue>::from(
          TrackedMoveOnlyValue{counters, 51});
      auto snapshot = cell.get();
      bool snapshot_rejected = false;
      try {
        (void)cell.take();
      } catch (const std::logic_error &) {
        snapshot_rejected = true;
      }
      check(snapshot_rejected && cell.initialized() && snapshot &&
                snapshot->value == 51,
            "OnceCell::take 未拒绝 move-only owning snapshot 或破坏源值");
      snapshot.reset();
      auto taken = cell.take();
      check(taken && taken->value == 51 && !cell.initialized(),
            "OnceCell::take 拒绝 snapshot 后未保持可重试状态");
    }
    check_balanced_lifetime(
        counters, "OnceCell move-only take 边界造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto cell = cio::sync::OnceCell<TrackedMoveOnlyValue>::from(
          TrackedMoveOnlyValue{counters, 61});
      auto snapshot = cell.get();
      bool snapshot_rejected = false;
      try {
        (void)std::move(cell).into_inner();
      } catch (const std::logic_error &) {
        snapshot_rejected = true;
      }
      check(snapshot_rejected && once_cell_source_is_invalid(cell) &&
                snapshot && snapshot->value == 61,
            "OnceCell::into_inner 未拒绝 move-only snapshot 或源仍有效");
      snapshot.reset();
    }
    check_balanced_lifetime(
        counters, "OnceCell move-only into_inner 边界造成泄漏或重复析构");
  }
}

void test_once_cell_async_and_retry() {
  Runtime runtime;
  check(runtime.block_on(once_cell_async_basic_root()),
        "OnceCell get_or_init 快路径或单次调用错误");
  check(runtime.block_on(once_cell_try_error_and_exception_root()),
        "OnceCell error/异常后未允许重试或发布半成品");
}

void test_once_cell_cancellation_boundaries() {
  Runtime runtime;
  check(runtime.block_on(once_cell_cancel_before_poll_root()),
        "OnceCell 初始化 task 在首次 poll 前取消错误");
  check(runtime.block_on(once_cell_cancel_initializer_root()),
        "OnceCell 活跃 initializer 取消未转交许可");
  check(runtime.block_on(once_cell_cancel_queued_waiter_root()),
        "OnceCell 排队 waiter 取消未清理或泄漏许可");
}

void test_once_cell_shutdown_cleanup() {
  const auto cell = std::make_shared<cio::sync::OnceCell<int>>();
  const auto gate = std::make_shared<cio::sync::Semaphore>(0);
  const auto entered = std::make_shared<std::atomic<int>>(0);
  {
    Runtime shutting_down;
    check(shutting_down.block_on(
              launch_detached_initializer(cell, gate, entered)),
          "运行时关闭前未建立 OnceCell initializer");
  }
  check(!cell->initialized(), "运行时关闭错误发布 OnceCell 半成品");
  Runtime continuation;
  check(continuation.block_on(retry_after_shutdown(cell)),
        "运行时关闭未释放 OnceCell 初始化许可");
}

void test_once_cell_multi_thread_contention_and_restart() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto multi = builder.worker_threads(4).build();
  check(
      multi.block_on(cio::task::assume_portable(once_cell_contention_root(64))),
      "multi-thread OnceCell 竞争出现多个 initializer 或错误结果");

  for (int iteration = 0; iteration < 20; ++iteration) {
    Runtime runtime;
    check(runtime.block_on(once_cell_contention_root(12)),
          "OnceCell runtime 反复启停竞争错误");
  }
}

void test_once_cell_concurrent_set_take_linearization() {
  constexpr int iterations = 2'000;
  for (int iteration = 0; iteration < iterations; ++iteration) {
    cio::sync::OnceCell<int> cell;
    std::barrier start{3};
    bool set_succeeded = false;
    std::optional<int> taken;
    {
      std::jthread setter{[&] {
        start.arrive_and_wait();
        set_succeeded = cell.set(1).has_value();
      }};
      std::jthread taker{[&] {
        start.arrive_and_wait();
        taken = cell.take();
      }};
      start.arrive_and_wait();
    }

    const auto current = cell.get();
    const bool exactly_one_location =
        static_cast<bool>(taken) != static_cast<bool>(current);
    check(set_succeeded && exactly_one_location && (!taken || *taken == 1) &&
              (!current || *current == 1),
          "OnceCell 并发 set/take 未形成安全线性化结果");
  }
}

static_assert(cio::Send<cio::sync::OnceCell<int>>);
static_assert(cio::Sync<cio::sync::OnceCell<int>>);
static_assert(cio::Send<cio::sync::SetError<int>>);
static_assert(cio::Sync<cio::sync::SetError<int>>);
static_assert(!cio::Send<cio::sync::OnceCellMutGuard<int>>);
static_assert(!cio::Sync<cio::sync::OnceCellMutGuard<int>>);
static_assert(std::copy_constructible<cio::sync::OnceCell<int>>);

struct MoveOnly final {
  MoveOnly() = default;
  MoveOnly(const MoveOnly &) = delete;
  MoveOnly &operator=(const MoveOnly &) = delete;
  MoveOnly(MoveOnly &&) noexcept = default;
  MoveOnly &operator=(MoveOnly &&) noexcept = default;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;
};

static_assert(!std::copy_constructible<cio::sync::OnceCell<MoveOnly>>);
static_assert(cio::Send<cio::sync::OnceCell<MoveOnly>>);
static_assert(cio::Sync<cio::sync::OnceCell<MoveOnly>>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"once_cell constructors set owned views",
       test_once_cell_constructors_set_and_owned_views},
      {"once_cell take into_inner exceptions",
       test_once_cell_take_into_inner_exception_safety},
      {"once_cell async retry", test_once_cell_async_and_retry},
      {"once_cell cancellation boundaries",
       test_once_cell_cancellation_boundaries},
      {"once_cell shutdown cleanup", test_once_cell_shutdown_cleanup},
      {"once_cell multi contention restart",
       test_once_cell_multi_thread_contention_and_restart},
      {"once_cell concurrent set take linearization",
       test_once_cell_concurrent_set_take_linearization},
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
  std::cout << "OnceCell 全部通过：" << passed << " 项\n";
  return 0;
}
