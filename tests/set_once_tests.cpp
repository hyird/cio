#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/set_once.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

struct VisibilityValue final {
  int sequence{0};
  int payload{0};
  int checksum{0};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  friend bool operator==(const VisibilityValue &,
                         const VisibilityValue &) = default;

  friend std::ostream &operator<<(std::ostream &stream,
                                  const VisibilityValue &value) {
    return stream << value.sequence << ':' << value.payload << ':'
                  << value.checksum;
  }
};

struct DropProbe final {
  std::shared_ptr<std::atomic<int>> drops;
  int value{0};
  bool active{true};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  DropProbe(std::shared_ptr<std::atomic<int>> counter, int stored)
      : drops{std::move(counter)}, value{stored} {}

  DropProbe(const DropProbe &) = delete;
  DropProbe &operator=(const DropProbe &) = delete;

  DropProbe(DropProbe &&other) noexcept
      : drops{std::move(other.drops)}, value{other.value},
        active{std::exchange(other.active, false)} {}

  DropProbe &operator=(DropProbe &&) = delete;

  ~DropProbe() {
    if (active) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

struct MoveBudgetValue final {
  std::shared_ptr<std::atomic<int>> moves;
  int value{0};
  int allowed_moves{0};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  MoveBudgetValue(std::shared_ptr<std::atomic<int>> counter, int stored,
                  int allowed)
      : moves{std::move(counter)}, value{stored}, allowed_moves{allowed} {}

  MoveBudgetValue(const MoveBudgetValue &) = delete;
  MoveBudgetValue &operator=(const MoveBudgetValue &) = delete;

  MoveBudgetValue(MoveBudgetValue &&other)
      : moves{other.moves}, value{other.value},
        allowed_moves{other.allowed_moves} {
    const auto observed = moves->fetch_add(1, std::memory_order_relaxed) + 1;
    if (observed > allowed_moves) {
      throw std::runtime_error{"SetOnce 重复 set 执行了额外移动"};
    }
  }

  MoveBudgetValue &operator=(MoveBudgetValue &&) = delete;
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
      throw std::runtime_error{"预期的 SetOnce 值复制异常"};
    }
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingTransferValue(ThrowingTransferValue &&other)
      : counters{other.counters}, value{other.value} {
    counters->move_attempts.fetch_add(1, std::memory_order_relaxed);
    if (counters->throw_on_move.load(std::memory_order_relaxed)) {
      throw std::runtime_error{"预期的 SetOnce 值移动异常"};
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
bool set_once_source_is_invalid(cio::sync::SetOnce<T> &set_once) {
  try {
    (void)set_once.initialized();
  } catch (const std::logic_error &) {
    return true;
  }
  return false;
}

template <typename T>
Task<std::shared_ptr<const T>>
await_set_once(typename cio::sync::SetOnce<T>::Wait wait) {
  co_return co_await wait;
}

Task<bool> set_once_immediate_root() {
  auto set_once = cio::sync::SetOnce<int>::from(7);
  const auto value = co_await set_once.wait();
  co_return value && *value == 7;
}

Task<bool> set_once_wait_then_set_root() {
  cio::sync::SetOnce<VisibilityValue> set_once;
  auto waiter =
      cio::task::spawn(await_set_once<VisibilityValue>(set_once.wait()));
  co_await cio::task::yield_now();
  const VisibilityValue expected{17, 31, 48};
  auto set = set_once.set(expected);
  const auto joined = co_await waiter;
  co_return set.has_value() && joined.has_value() && joined.value() &&
      *joined.value() == expected &&joined.value()->checksum ==
          joined.value()->sequence + joined.value()->payload;
}

Task<bool> set_once_wait_is_single_use_root() {
  cio::sync::SetOnce<int> set_once;
  auto wait = set_once.wait();
  auto awaiter = wait.operator co_await();
  bool duplicate_rejected = false;
  try {
    (void)wait.operator co_await();
  } catch (const std::logic_error &) {
    duplicate_rejected = true;
  }
  const auto set = set_once.set(19);
  const auto value = co_await std::move(awaiter);
  co_return duplicate_rejected &&set.has_value() && value && *value == 19;
}

Task<bool> set_once_cancel_pending_root() {
  cio::sync::SetOnce<int> set_once;
  auto before_poll = cio::task::spawn(await_set_once<int>(set_once.wait()));
  before_poll.abort();
  const auto before_poll_join = co_await before_poll;
  if (before_poll_join.has_value() ||
      !before_poll_join.error().is_cancelled()) {
    co_return false;
  }

  auto cancelled = cio::task::spawn(await_set_once<int>(set_once.wait()));
  co_await cio::task::yield_now();
  cancelled.abort();
  const auto cancelled_join = co_await cancelled;

  auto survivor = cio::task::spawn(await_set_once<int>(set_once.wait()));
  co_await cio::task::yield_now();
  const auto set = set_once.set(9);
  const auto survivor_join = co_await survivor;
  co_return !cancelled_join.has_value() &&
      cancelled_join.error().is_cancelled() && set.has_value() &&
      survivor_join.has_value() && survivor_join.value() &&
      *survivor_join.value() == 9;
}

Task<bool> set_once_cancel_notified_root() {
  cio::sync::SetOnce<int> set_once;
  auto cancelled = cio::task::spawn(await_set_once<int>(set_once.wait()));
  auto survivor = cio::task::spawn(await_set_once<int>(set_once.wait()));
  co_await cio::task::yield_now();
  const auto set = set_once.set(12);
  cancelled.abort();
  const auto cancelled_join = co_await cancelled;
  const auto survivor_join = co_await survivor;
  co_return set.has_value() && !cancelled_join.has_value() &&
      cancelled_join.error().is_cancelled() && survivor_join.has_value() &&
      survivor_join.value() && *survivor_join.value() == 12;
}

Task<bool> set_once_no_lost_wake_root(int iterations) {
  for (int iteration = 0; iteration < iterations; ++iteration) {
    cio::sync::SetOnce<int> set_once;
    auto waiter = cio::task::spawn(await_set_once<int>(set_once.wait()));
    if ((iteration % 2) == 0) {
      co_await cio::task::yield_now();
    }
    const auto set = set_once.set(iteration);
    if ((iteration % 3) == 0) {
      co_await cio::task::yield_now();
    }
    const auto joined = co_await waiter;
    if (!set.has_value() || !joined.has_value() || !joined.value() ||
        *joined.value() != iteration) {
      co_return false;
    }
  }
  co_return true;
}

Task<void>
set_once_waiter(std::shared_ptr<cio::sync::SetOnce<VisibilityValue>> set_once,
                std::shared_ptr<std::atomic<int>> entered,
                std::shared_ptr<std::atomic<int>> completed,
                std::shared_ptr<std::atomic<bool>> visibility_failure) {
  entered->fetch_add(1, std::memory_order_release);
  const auto value = co_await set_once->wait();
  if (!value || value->sequence != 100 || value->payload != 23 ||
      value->checksum != 123) {
    visibility_failure->store(true, std::memory_order_release);
  }
  completed->fetch_add(1, std::memory_order_release);
}

Task<bool> set_once_many_waiters_root(std::size_t participants) {
  const auto set_once = std::make_shared<cio::sync::SetOnce<VisibilityValue>>();
  const auto entered = std::make_shared<std::atomic<int>>(0);
  const auto completed = std::make_shared<std::atomic<int>>(0);
  const auto visibility_failure = std::make_shared<std::atomic<bool>>(false);
  std::vector<cio::task::JoinHandle<void>> waiters;
  waiters.reserve(participants);
  for (std::size_t index = 0; index < participants; ++index) {
    waiters.push_back(cio::task::spawn(cio::task::assume_portable(
        set_once_waiter(set_once, entered, completed, visibility_failure))));
  }
  while (entered->load(std::memory_order_acquire) !=
         static_cast<int>(participants)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  const auto set = set_once->set(VisibilityValue{100, 23, 123});
  for (auto &waiter : waiters) {
    const auto joined = co_await waiter;
    if (!joined.has_value()) {
      co_return false;
    }
  }
  co_return set.has_value() &&
      completed->load(std::memory_order_acquire) ==
          static_cast<int>(participants) &&
      !visibility_failure->load(std::memory_order_acquire);
}

Task<bool>
launch_shutdown_waiter(std::shared_ptr<cio::sync::SetOnce<int>> set_once,
                       std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(await_set_once<int>(set_once->wait()));
  (void)detached;
  entered->store(true, std::memory_order_release);
  co_await cio::task::yield_now();
  co_return true;
}

Task<bool>
wait_after_shutdown(std::shared_ptr<cio::sync::SetOnce<int>> set_once) {
  const auto value = co_await set_once->wait();
  co_return value && *value == 44;
}

void test_set_once_construction_value_semantics_error() {
  cio::sync::SetOnce<int> empty;
  check(!empty.initialized() && !empty.get(), "SetOnce 默认构造空状态错误");

  auto const_empty = cio::sync::SetOnce<int>::const_new();
  auto new_some = cio::sync::SetOnce<int>::new_with(3);
  auto new_none = cio::sync::SetOnce<int>::new_with(std::nullopt);
  auto const_some = cio::sync::SetOnce<int>::const_new_with(4);
  auto from = cio::sync::SetOnce<int>::from(5);
  check(!const_empty.initialized() && !new_none.initialized() &&
            *new_some.get() == 3 && *const_some.get() == 4 && *from.get() == 5,
        "SetOnce 构造工厂语义错误");

  check(empty.set(8).has_value() && *empty.get() == 8, "SetOnce 首次 set 失败");
  auto duplicate = empty.set(9);
  check(!duplicate.has_value() && duplicate.error().value() == 9 &&
            duplicate.error().message() == "SetOnceError" &&
            duplicate.error().debug_string() == "SetOnceError(9)",
        "SetOnceError 值、Display 或 Debug 能力错误");
  std::ostringstream display;
  display << duplicate.error();
  check(display.str() == "SetOnceError",
        "SetOnceError operator<< 未对齐 Display");

  auto clone = empty.clone();
  check(clone == empty && clone.debug_string() == "SetOnce { value: Some(8) }",
        "SetOnce Clone/PartialEq/Eq/Debug 映射错误");

  auto snapshot = empty.get();
  auto inner = std::move(empty).into_inner();
  bool consumed_source_rejected = false;
  try {
    (void)empty.initialized();
  } catch (const std::logic_error &) {
    consumed_source_rejected = true;
  }
  check(inner && *inner == 8 && snapshot && *snapshot == 8 &&
            consumed_source_rejected,
        "SetOnce::into_inner 未消费源对象或 owning snapshot 生命周期错误");

  cio::sync::SetOnce<std::unique_ptr<int>> move_only;
  check(move_only.set(std::make_unique<int>(21)).has_value(),
        "SetOnce 未接受 move-only 值");
  auto move_snapshot = move_only.get();
  check(move_snapshot && **move_snapshot == 21, "SetOnce move-only get 错误");
  move_snapshot.reset();
  auto move_inner = std::move(move_only).into_inner();
  check(move_inner && **move_inner == 21, "SetOnce move-only into_inner 错误");

  const auto initial_moves = std::make_shared<std::atomic<int>>(0);
  auto fast_failure = cio::sync::SetOnce<MoveBudgetValue>::from(
      MoveBudgetValue{initial_moves, 1, 8});
  const auto duplicate_moves = std::make_shared<std::atomic<int>>(0);
  auto fast_duplicate =
      fast_failure.set(MoveBudgetValue{duplicate_moves, 2, 3});
  check(!fast_duplicate.has_value() &&
            fast_duplicate.error().value().value == 2 &&
            duplicate_moves->load(std::memory_order_relaxed) == 3,
        "SetOnce 重复 set 快路径仍分配候选值或执行额外移动");
}

void test_set_once_into_inner_exception_safety() {
  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto set_once = cio::sync::SetOnce<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 11});
      const auto attempts_before =
          counters->move_attempts.load(std::memory_order_relaxed);
      counters->throw_on_move.store(true, std::memory_order_relaxed);
      bool move_threw = false;
      try {
        (void)std::move(set_once).into_inner();
      } catch (const std::runtime_error &) {
        move_threw = true;
      }
      counters->throw_on_move.store(false, std::memory_order_relaxed);
      check(move_threw &&
                counters->move_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                set_once_source_is_invalid(set_once),
            "SetOnce::into_inner 移动异常未传播或源对象仍然有效");
    }
    check_balanced_lifetime(counters,
                            "SetOnce::into_inner 移动异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto set_once = cio::sync::SetOnce<ThrowingTransferValue>::from(
          ThrowingTransferValue{counters, 21});
      auto snapshot = set_once.get();
      const auto attempts_before =
          counters->copy_attempts.load(std::memory_order_relaxed);
      counters->throw_on_copy.store(true, std::memory_order_relaxed);
      bool copy_threw = false;
      try {
        (void)std::move(set_once).into_inner();
      } catch (const std::runtime_error &) {
        copy_threw = true;
      }
      counters->throw_on_copy.store(false, std::memory_order_relaxed);
      check(copy_threw &&
                counters->copy_attempts.load(std::memory_order_relaxed) ==
                    attempts_before + 1 &&
                set_once_source_is_invalid(set_once) && snapshot &&
                snapshot->value == 21,
            "SetOnce::into_inner 复制异常未传播、源仍有效或 snapshot 损坏");
      snapshot.reset();
    }
    check_balanced_lifetime(counters,
                            "SetOnce::into_inner 复制异常造成泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto set_once = cio::sync::SetOnce<TrackedMoveOnlyValue>::from(
          TrackedMoveOnlyValue{counters, 31});
      auto snapshot = set_once.get();
      bool snapshot_rejected = false;
      try {
        (void)std::move(set_once).into_inner();
      } catch (const std::logic_error &) {
        snapshot_rejected = true;
      }
      check(snapshot_rejected && set_once_source_is_invalid(set_once) &&
                snapshot && snapshot->value == 31,
            "SetOnce::into_inner 未拒绝 move-only snapshot 或源仍有效");
      snapshot.reset();
    }
    check_balanced_lifetime(
        counters, "SetOnce move-only into_inner 边界造成泄漏或重复析构");
  }
}

void test_set_once_wait_and_visibility() {
  Runtime runtime;
  check(runtime.block_on(set_once_immediate_root()),
        "SetOnce 已设置 wait 未立即完成");
  check(runtime.block_on(set_once_wait_then_set_root()),
        "SetOnce wait-before-set 或内存可见性错误");
  check(runtime.block_on(set_once_wait_is_single_use_root()),
        "SetOnce 同一 wait 未拒绝重复等待");
  check(runtime.block_on(set_once_no_lost_wake_root(300)),
        "SetOnce set/wait 竞态丢失唤醒");
}

void test_set_once_cancellation() {
  Runtime runtime;
  check(runtime.block_on(set_once_cancel_pending_root()),
        "SetOnce pending waiter 取消不安全");
  check(runtime.block_on(set_once_cancel_notified_root()),
        "SetOnce 已通知未恢复 waiter 取消影响其他等待者");
}

void test_set_once_concurrent_set_and_drop() {
  constexpr int contenders = 64;
  const auto set_once = std::make_shared<cio::sync::SetOnce<int>>();
  const auto successes = std::make_shared<std::atomic<int>>(0);
  const auto returned_sum = std::make_shared<std::atomic<int>>(0);
  std::vector<std::jthread> threads;
  threads.reserve(contenders);
  for (int value = 1; value <= contenders; ++value) {
    threads.emplace_back([set_once, successes, returned_sum, value] {
      auto result = set_once->set(value);
      if (result.has_value()) {
        successes->fetch_add(1, std::memory_order_relaxed);
      } else {
        returned_sum->fetch_add(result.error().value(),
                                std::memory_order_relaxed);
      }
    });
  }
  threads.clear();
  const auto winner = set_once->get();
  const int all_values_sum = contenders * (contenders + 1) / 2;
  check(successes->load(std::memory_order_relaxed) == 1 && winner &&
            returned_sum->load(std::memory_order_relaxed) + *winner ==
                all_values_sum,
        "SetOnce 并发 set 不止一个成功或错误值所有权丢失");

  constexpr int drop_contenders = 32;
  const auto drops = std::make_shared<std::atomic<int>>(0);
  auto drop_once = std::make_shared<cio::sync::SetOnce<DropProbe>>();
  std::vector<std::jthread> drop_threads;
  drop_threads.reserve(drop_contenders);
  for (int value = 0; value < drop_contenders; ++value) {
    drop_threads.emplace_back([drop_once, drops, value] {
      auto result = drop_once->set(DropProbe{drops, value});
      (void)result;
    });
  }
  drop_threads.clear();
  check(drops->load(std::memory_order_relaxed) == drop_contenders - 1,
        "SetOnce loser 值未恰好析构一次");
  drop_once.reset();
  check(drops->load(std::memory_order_relaxed) == drop_contenders,
        "SetOnce winner 值未随 cell 恰好析构一次");
}

void test_set_once_shutdown_and_restart() {
  const auto set_once = std::make_shared<cio::sync::SetOnce<int>>();
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  {
    Runtime shutting_down;
    check(shutting_down.block_on(launch_shutdown_waiter(set_once, entered)),
          "运行时关闭前未建立 SetOnce waiter");
  }
  check(!set_once->initialized() && set_once->set(44).has_value(),
        "运行时关闭取消 waiter 后 SetOnce 无法设置");
  Runtime continuation;
  check(continuation.block_on(wait_after_shutdown(set_once)),
        "运行时关闭后 SetOnce wait 无法立即观察设置值");

  for (int iteration = 0; iteration < 20; ++iteration) {
    Runtime runtime;
    check(runtime.block_on(set_once_no_lost_wake_root(30)),
          "SetOnce runtime 反复启停竞态错误");
  }
}

void test_set_once_multi_thread_waiters() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();
  check(runtime.block_on(
            cio::task::assume_portable(set_once_many_waiters_root(96))),
        "multi-thread SetOnce 未唤醒全部 waiter 或发布不可见");
}

static_assert(cio::Send<cio::sync::SetOnce<int>>);
static_assert(cio::Sync<cio::sync::SetOnce<int>>);
static_assert(cio::Send<cio::sync::SetOnceError<int>>);
static_assert(cio::Sync<cio::sync::SetOnceError<int>>);
static_assert(std::copy_constructible<cio::sync::SetOnce<int>>);
static_assert(cio::Send<cio::sync::SetOnce<int>::Wait>);
static_assert(!cio::Sync<cio::sync::SetOnce<int>::Wait>);
static_assert(cio::Send<cio::sync::SetOnce<int>::Wait::Awaiter>);
static_assert(!cio::Sync<cio::sync::SetOnce<int>::Wait::Awaiter>);
static_assert(!std::copy_constructible<cio::sync::SetOnce<int>::Wait::Awaiter>);

struct MoveOnly final {
  MoveOnly() = default;
  MoveOnly(const MoveOnly &) = delete;
  MoveOnly &operator=(const MoveOnly &) = delete;
  MoveOnly(MoveOnly &&) noexcept = default;
  MoveOnly &operator=(MoveOnly &&) noexcept = default;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;
};

static_assert(!std::copy_constructible<cio::sync::SetOnce<MoveOnly>>);
static_assert(cio::Send<cio::sync::SetOnce<MoveOnly>>);
static_assert(cio::Sync<cio::sync::SetOnce<MoveOnly>>);

struct SendOnly final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
};

static_assert(cio::Send<cio::sync::SetOnce<SendOnly>>);
static_assert(!cio::Sync<cio::sync::SetOnce<SendOnly>>);
static_assert(!cio::Send<cio::sync::SetOnce<SendOnly>::Wait>);
static_assert(!cio::Sync<cio::sync::SetOnce<SendOnly>::Wait>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"set_once construction value error",
       test_set_once_construction_value_semantics_error},
      {"set_once into_inner exceptions",
       test_set_once_into_inner_exception_safety},
      {"set_once wait visibility", test_set_once_wait_and_visibility},
      {"set_once cancellation", test_set_once_cancellation},
      {"set_once concurrent set drop", test_set_once_concurrent_set_and_drop},
      {"set_once shutdown restart", test_set_once_shutdown_and_restart},
      {"set_once multi waiters", test_set_once_multi_thread_waiters},
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
  std::cout << "SetOnce 全部通过：" << passed << " 项\n";
  return 0;
}
