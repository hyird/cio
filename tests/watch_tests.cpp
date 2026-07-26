#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/watch.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;
using cio::sync::watch::Receiver;
using cio::sync::watch::Sender;
using cio::sync::watch::Snapshot;
using cio::sync::watch::error::RecvError;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<bool> changed_after_send_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto waiting = cio::task::spawn(receiver.changed());
  co_await cio::task::yield_now();
  const auto sent = sender.send(7);
  const auto joined = co_await waiting;
  co_return sent.has_value() && joined.has_value() &&
      joined.value().has_value() && receiver.borrow().value() == 7 &&
      !receiver.borrow().has_changed();
}

Task<bool> unseen_then_closed_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto sent = sender.send(1);
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();

  const auto unseen = co_await receiver.changed();
  const auto closed = co_await receiver.changed();
  co_return sent.has_value() && unseen.has_value() &&
      !closed.has_value() && receiver.borrow().value() == 1;
}

Task<bool> pending_close_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto waiting = cio::task::spawn(receiver.changed());
  co_await cio::task::yield_now();
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && !joined.value().has_value();
}

Task<bool> cancellation_reuse_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto changed_wait = cio::task::spawn(receiver.changed());
  co_await cio::task::yield_now();
  changed_wait.abort();
  const auto changed_join = co_await changed_wait;
  if (changed_join.has_value()) {
    co_return false;
  }

  const auto predicate_calls = std::make_shared<std::atomic<int>>(0);
  auto predicate_wait = cio::task::spawn(receiver.wait_for(
      [predicate_calls](const int &) {
        predicate_calls->fetch_add(1, std::memory_order_relaxed);
        return false;
      }));
  co_await cio::task::yield_now();
  predicate_wait.abort();
  const auto predicate_join = co_await predicate_wait;
  if (predicate_join.has_value() ||
      predicate_calls->load(std::memory_order_relaxed) != 1) {
    co_return false;
  }

  auto closed_wait = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  closed_wait.abort();
  const auto closed_join = co_await closed_wait;
  if (closed_join.has_value()) {
    co_return false;
  }

  const auto sent = sender.send(6);
  const auto changed = co_await receiver.changed();
  co_return sent.has_value() && changed.has_value() &&
      receiver.borrow().value() == 6 && sender.sender_count() == 1 &&
      sender.receiver_count() == 1;
}

Task<bool> multiple_receivers_root() {
  auto [sender, first] = cio::sync::watch::channel(0);
  auto second = first;
  auto third = sender.subscribe();
  auto first_wait = cio::task::spawn(first.changed());
  auto second_wait = cio::task::spawn(second.changed());
  auto third_wait = cio::task::spawn(third.changed());
  co_await cio::task::yield_now();
  const auto sent = sender.send(9);
  const auto first_join = co_await first_wait;
  const auto second_join = co_await second_wait;
  const auto third_join = co_await third_wait;
  co_return sent.has_value() && first_join.has_value() &&
      second_join.has_value() && third_join.has_value() &&
      first_join.value().has_value() && second_join.value().has_value() &&
      third_join.value().has_value() && first.borrow().value() == 9 &&
      second.borrow().value() == 9 && third.borrow().value() == 9;
}

Task<bool> sender_closed_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto waiting = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  const auto count_while_waiting = sender.sender_count();
  std::optional<Receiver<int>> owner{std::move(receiver)};
  owner.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && sender.is_closed() &&
      count_while_waiting == 1;
}

Task<bool> wait_for_updates_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto calls = std::make_shared<std::vector<int>>();
  auto waiting = cio::task::spawn(receiver.wait_for(
      [calls](const int &value) {
        calls->push_back(value);
        return value == 2;
      }));
  co_await cio::task::yield_now();
  const auto first = sender.send(1);
  co_await cio::task::yield_now();
  const auto second = sender.send(2);
  const auto joined = co_await waiting;
  co_return first.has_value() && second.has_value() &&
      joined.has_value() && joined.value().has_value() &&
      joined.value().value().value() == 2 &&
      *calls == std::vector<int>({0, 1, 2}) &&
      !receiver.borrow().has_changed();
}

Task<bool> wait_for_closed_root() {
  auto [sender, receiver] = cio::sync::watch::channel(4);
  const auto calls = std::make_shared<std::atomic<int>>(0);
  auto waiting = cio::task::spawn(receiver.wait_for(
      [calls](const int &) {
        calls->fetch_add(1, std::memory_order_relaxed);
        return false;
      }));
  co_await cio::task::yield_now();
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && !joined.value().has_value() &&
      calls->load(std::memory_order_relaxed) == 1;
}

Task<bool> predicate_reentry_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto calls = std::make_shared<std::atomic<int>>(0);
  auto result = co_await receiver.wait_for(
      [sender, calls](const int &value) mutable {
        calls->fetch_add(1, std::memory_order_relaxed);
        if (value == 0) {
          const auto old = sender.send_replace(5);
          if (old != 0) {
            throw std::runtime_error{"send_replace 返回旧值错误"};
          }
          return false;
        }
        return value == 5;
      });
  co_return result.has_value() && result.value().value() == 5 &&
      calls->load(std::memory_order_relaxed) == 2;
}

Task<bool> cross_thread_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  std::thread producer{[sender, entered] {
    while (!entered->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    const auto sent = sender.send(33);
    if (!sent.has_value()) {
      std::terminate();
    }
  }};
  entered->store(true, std::memory_order_release);
  const auto changed = co_await receiver.changed();
  producer.join();
  co_return changed.has_value() && receiver.borrow().value() == 33;
}

Task<void>
mark_peer_progress(std::shared_ptr<std::atomic<bool>> progressed) {
  progressed->store(true, std::memory_order_release);
  co_return;
}

Task<bool> changed_cooperative_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  std::size_t completed = 0;
  while (!progressed->load(std::memory_order_acquire) && completed < 256) {
    receiver.mark_changed();
    const auto result = co_await receiver.changed();
    if (!result.has_value()) {
      co_return false;
    }
    ++completed;
  }
  const auto joined = co_await peer;
  co_return joined.has_value() &&
      progressed->load(std::memory_order_acquire) && completed < 256 &&
      sender.receiver_count() == 1;
}

Task<bool> closed_cooperative_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  std::optional<Receiver<int>> owner{std::move(receiver)};
  owner.reset();
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  std::size_t completed = 0;
  while (!progressed->load(std::memory_order_acquire) && completed < 256) {
    co_await sender.closed();
    ++completed;
  }
  const auto joined = co_await peer;
  co_return joined.has_value() &&
      progressed->load(std::memory_order_acquire) && completed < 256;
}

Task<bool> wait_for_immediate_loop_single_budget_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  for (std::size_t unit = 0; unit < 127; ++unit) {
    co_await cio::task::consume_budget();
  }
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  const auto calls = std::make_shared<std::atomic<int>>(0);
  const auto result = co_await receiver.wait_for(
      [sender, calls](const int &value) mutable {
        calls->fetch_add(1, std::memory_order_relaxed);
        if (value == 0) {
          (void)sender.send_replace(1);
          return false;
        }
        return value == 1;
      });
  const bool completed_before_peer =
      result.has_value() &&
      !progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return completed_before_peer && joined.has_value() &&
      calls->load(std::memory_order_relaxed) == 2;
}

Task<bool> changed_ready_exact_budget_root(bool closed) {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  if (closed) {
    std::optional<Sender<int>> owner{std::move(sender)};
    owner.reset();
  }
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    if (!closed) {
      receiver.mark_changed();
    }
    const auto result = co_await receiver.changed();
    if (result.has_value() == closed ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  if (!closed) {
    receiver.mark_changed();
  }
  const auto result = co_await receiver.changed();
  const bool yielded =
      result.has_value() != closed &&
      progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<bool> closed_ready_exact_budget_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  std::optional<Receiver<int>> owner{std::move(receiver)};
  owner.reset();
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    co_await sender.closed();
    if (progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  co_await sender.closed();
  const bool yielded = progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<bool> wait_for_ready_exact_budget_root(bool closed) {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  if (closed) {
    std::optional<Sender<int>> owner{std::move(sender)};
    owner.reset();
  }
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    const auto result = co_await receiver.wait_for(
        [closed](const int &) { return !closed; });
    if (result.has_value() == closed ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  const auto result = co_await receiver.wait_for(
      [closed](const int &) { return !closed; });
  const bool yielded =
      result.has_value() != closed &&
      progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<void> publish_watch_once(Sender<int> sender, int value) {
  const auto sent = sender.send(value);
  if (!sent.has_value()) {
    throw std::runtime_error{"fresh budget watch send 失败"};
  }
  co_return;
}

Task<void> drop_watch_receiver_once(Receiver<int> receiver) {
  (void)receiver;
  co_return;
}

Task<bool> fresh_budget_tail(
    std::shared_ptr<std::atomic<bool>> progressed) {
  for (std::size_t unit = 0; unit < 127; ++unit) {
    co_await cio::task::consume_budget();
    if (progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  co_await cio::task::consume_budget();
  co_return progressed->load(std::memory_order_acquire);
}

Task<bool> changed_fresh_notification_budget_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto producer = cio::task::spawn(publish_watch_once(sender, 1));
  const auto changed = co_await receiver.changed();
  if (!changed.has_value()) {
    co_return false;
  }
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  const bool boundary = co_await fresh_budget_tail(progressed);
  const auto producer_join = co_await producer;
  const auto peer_join = co_await peer;
  co_return boundary && producer_join.has_value() &&
      peer_join.has_value();
}

Task<bool> wait_for_fresh_notification_budget_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto producer = cio::task::spawn(publish_watch_once(sender, 1));
  const auto waited =
      co_await receiver.wait_for([](const int &value) { return value == 1; });
  if (!waited.has_value()) {
    co_return false;
  }
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  const bool boundary = co_await fresh_budget_tail(progressed);
  const auto producer_join = co_await producer;
  const auto peer_join = co_await peer;
  co_return boundary && producer_join.has_value() &&
      peer_join.has_value();
}

Task<bool> closed_fresh_notification_budget_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto dropper =
      cio::task::spawn(drop_watch_receiver_once(std::move(receiver)));
  co_await sender.closed();
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  const bool boundary = co_await fresh_budget_tail(progressed);
  const auto dropper_join = co_await dropper;
  const auto peer_join = co_await peer;
  co_return boundary && dropper_join.has_value() &&
      peer_join.has_value();
}

Task<void>
detach_pending_receiver(std::shared_ptr<Receiver<int>> receiver) {
  auto detached = cio::task::spawn(receiver->changed());
  co_await cio::task::yield_now();
  (void)detached;
}

struct LifetimeState final {
  std::atomic<int> destroyed{0};
  std::atomic<int> reentered{0};
  std::function<void()> callback;
};

struct ReentrantValue final {
  std::shared_ptr<LifetimeState> state;
  int value{0};
  bool active{true};

  ReentrantValue(std::shared_ptr<LifetimeState> observed, int stored)
      : state{std::move(observed)}, value{stored} {}

  ReentrantValue(const ReentrantValue &other)
      : state{other.state}, value{other.value} {}

  ReentrantValue(ReentrantValue &&other) noexcept
      : state{std::move(other.state)}, value{other.value},
        active{std::exchange(other.active, false)} {}

  ReentrantValue &operator=(const ReentrantValue &) = delete;
  ReentrantValue &operator=(ReentrantValue &&) = delete;

  ~ReentrantValue() {
    if (!active) {
      return;
    }
    state->destroyed.fetch_add(1, std::memory_order_relaxed);
    if (state->callback) {
      state->callback();
    }
  }
};

struct ThrowOnCopy final {
  std::shared_ptr<std::atomic<bool>> throw_copy;
  int value{0};

  ThrowOnCopy(std::shared_ptr<std::atomic<bool>> flag, int stored)
      : throw_copy{std::move(flag)}, value{stored} {}

  ThrowOnCopy(const ThrowOnCopy &other)
      : throw_copy{other.throw_copy}, value{other.value} {
    if (throw_copy->load(std::memory_order_relaxed)) {
      throw std::runtime_error{"预期的 watch 复制异常"};
    }
  }

  ThrowOnCopy(ThrowOnCopy &&) noexcept = default;
  ThrowOnCopy &operator=(const ThrowOnCopy &) = delete;
  ThrowOnCopy &operator=(ThrowOnCopy &&) = delete;
};

struct MoveOnlyWatchValue final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  explicit MoveOnlyWatchValue(int stored) noexcept : value{stored} {}

  MoveOnlyWatchValue(const MoveOnlyWatchValue &) = delete;
  MoveOnlyWatchValue &operator=(const MoveOnlyWatchValue &) = delete;
  MoveOnlyWatchValue(MoveOnlyWatchValue &&) noexcept = default;
  MoveOnlyWatchValue &operator=(MoveOnlyWatchValue &&) noexcept = default;

  int value{0};
};

template <typename T>
concept HasRvalueSnapshotValue =
    requires(T snapshot) { std::move(snapshot).value(); };

template <typename T>
concept HasRvalueErrorValue =
    requires(T error) { std::move(error).value(); };

template <typename T>
concept HasRvalueErrorIntoInner =
    requires(T error) { std::move(error).into_inner(); };

struct PredicateDestructorState final {
  std::shared_ptr<Receiver<int>> receiver;
  std::atomic<int> succeeded{0};
  std::atomic<int> failed{0};
};

enum class PredicateExit {
  accept,
  pending,
  throwing,
};

struct ReentrantPredicateDestructor final {
  std::shared_ptr<PredicateDestructorState> state;
  PredicateExit exit{PredicateExit::accept};
  bool active{true};

  ReentrantPredicateDestructor(
      std::shared_ptr<PredicateDestructorState> observed,
      PredicateExit requested) noexcept
      : state{std::move(observed)}, exit{requested} {}

  ReentrantPredicateDestructor(const ReentrantPredicateDestructor &) = delete;
  ReentrantPredicateDestructor &
  operator=(const ReentrantPredicateDestructor &) = delete;

  ReentrantPredicateDestructor(ReentrantPredicateDestructor &&other) noexcept
      : state{std::move(other.state)}, exit{other.exit},
        active{std::exchange(other.active, false)} {}

  ReentrantPredicateDestructor &
  operator=(ReentrantPredicateDestructor &&) = delete;

  ~ReentrantPredicateDestructor() {
    if (!active) {
      return;
    }
    try {
      (void)state->receiver->borrow();
      state->succeeded.fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
      state->failed.fetch_add(1, std::memory_order_relaxed);
    }
  }

  bool operator()(const int &) const {
    if (exit == PredicateExit::throwing) {
      throw std::runtime_error{"预期的 predicate 析构顺序异常"};
    }
    return exit == PredicateExit::accept;
  }
};

Task<bool> predicate_destructor_order_root() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  const auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  const auto state = std::make_shared<PredicateDestructorState>();
  state->receiver = shared_receiver;

  const auto accepted = co_await shared_receiver->wait_for(
      ReentrantPredicateDestructor{state, PredicateExit::accept});
  if (!accepted.has_value()) {
    co_return false;
  }

  bool threw = false;
  try {
    (void)co_await shared_receiver->wait_for(
        ReentrantPredicateDestructor{state, PredicateExit::throwing});
  } catch (const std::runtime_error &) {
    threw = true;
  }
  if (!threw) {
    co_return false;
  }

  auto pending = cio::task::spawn(shared_receiver->wait_for(
      ReentrantPredicateDestructor{state, PredicateExit::pending}));
  co_await cio::task::yield_now();
  pending.abort();
  const auto joined = co_await pending;

  co_return !joined.has_value() &&
      state->succeeded.load(std::memory_order_relaxed) == 3 &&
      state->failed.load(std::memory_order_relaxed) == 0 &&
      sender.receiver_count() == 1;
}

struct DestructionToken final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  explicit DestructionToken(std::shared_ptr<std::atomic<int>> observed)
      : destroyed{std::move(observed)} {}

  ~DestructionToken() {
    destroyed->fetch_add(1, std::memory_order_relaxed);
  }

  std::shared_ptr<std::atomic<int>> destroyed;
};

Task<bool> false_snapshot_released_before_wait_root() {
  const auto old_destroyed = std::make_shared<std::atomic<int>>(0);
  const auto next_destroyed = std::make_shared<std::atomic<int>>(0);
  auto [sender, receiver] = cio::sync::watch::channel(
      std::make_shared<DestructionToken>(old_destroyed));
  const auto entered = std::make_shared<std::atomic<bool>>(false);
  auto waiting = cio::task::spawn(receiver.wait_for(
      [entered](const std::shared_ptr<DestructionToken> &) {
        entered->store(true, std::memory_order_release);
        return false;
      }));
  co_await cio::task::yield_now();
  if (!entered->load(std::memory_order_acquire)) {
    co_return false;
  }

  const auto sent =
      sender.send(std::make_shared<DestructionToken>(next_destroyed));
  const bool released_before_resume =
      sent.has_value() &&
      old_destroyed->load(std::memory_order_relaxed) == 1;
  waiting.abort();
  const auto joined = co_await waiting;
  co_return released_before_resume && !joined.has_value();
}

void test_sync_versions_snapshots_and_marking() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  check(sender.receiver_count() == 1 && sender.sender_count() == 1,
        "watch 初始计数错误");
  check(receiver.borrow().value() == 0 &&
            !receiver.borrow().has_changed(),
        "watch 初值快照错误");
  check(!receiver.has_changed().value(), "watch 初值不应标记 changed");

  const auto retained = receiver.borrow();
  check(sender.send(1).has_value(), "watch send 失败");
  check(retained.value() == 0, "拥有 Snapshot 未保持旧版本");
  check(receiver.borrow().value() == 1 &&
            receiver.borrow().has_changed(),
        "borrow 未保留未读版本");
  const auto updated = receiver.borrow_and_update();
  check(updated.value() == 1 && updated.has_changed(),
        "borrow_and_update 未返回 changed");
  check(!receiver.has_changed().value(), "版本未标成已读");

  check(sender.send(1).has_value(), "相同值 send 失败");
  check(receiver.has_changed().value(),
        "watch 以版本而不是值相等性判断 changed");
  receiver.mark_unchanged();
  check(!receiver.has_changed().value(), "mark_unchanged 失败");
  receiver.mark_changed();
  check(receiver.has_changed().value(), "mark_changed 失败");
  receiver.mark_unchanged();

  check(sender.send_replace(2) == 1, "send_replace 旧值错误");
  check(receiver.borrow_and_update().value() == 2,
        "send_replace 新值错误");
  check(!sender.borrow().has_changed() && sender.borrow().value() == 2,
        "Sender borrow 语义错误");
}

void test_sender_construction_and_count_overflow_guard() {
  auto sender = Sender<int>::new_sender(4);
  check(sender.receiver_count() == 0 && sender.sender_count() == 1,
        "Sender::new_sender 初始计数错误");
  const auto failed = sender.send(5);
  check(!failed.has_value() && failed.error().value() == 5,
        "Sender::new_sender 无 Receiver send 未返还值");
  check(sender.send_replace(6) == 4,
        "Sender::new_sender send_replace 旧值错误");
  auto receiver = sender.subscribe();
  check(receiver.borrow().value() == 6 &&
            !receiver.has_changed().value(),
        "Sender::new_sender subscribe 未把当前值标为已读");

  Sender<int> default_sender;
  check(default_sender.receiver_count() == 0 &&
            default_sender.borrow().value() == 0,
        "watch Sender 默认构造错误");

  auto count = std::numeric_limits<std::size_t>::max();
  bool overflow_rejected = false;
  try {
    cio::detail::watch_checked_increment(count, "预期的 watch 计数溢出");
  } catch (const std::length_error &) {
    overflow_rejected = true;
  }
  check(overflow_rejected &&
            count == std::numeric_limits<std::size_t>::max(),
        "watch 逻辑计数溢出未保持原值");
}

void test_counts_subscribe_close_and_errors() {
  auto [sender, receiver] = cio::sync::watch::channel(std::string{"a"});
  auto sender_copy = sender;
  auto receiver_copy = receiver;
  check(sender.sender_count() == 2 && sender.receiver_count() == 2,
        "watch copy 计数错误");
  check(sender.same_channel(sender_copy) &&
            receiver.same_channel(receiver_copy),
        "watch same_channel 同 channel 错误");
  auto [other_sender, other_receiver] =
      cio::sync::watch::channel(std::string{"x"});
  check(!sender.same_channel(other_sender) &&
            !receiver.same_channel(other_receiver),
        "watch same_channel 异 channel 错误");

  receiver_copy = Receiver<std::string>{std::move(other_receiver)};
  check(sender.receiver_count() == 1, "Receiver 赋值未减少旧计数");
  std::optional<Receiver<std::string>> owner{std::move(receiver)};
  owner.reset();
  check(sender.is_closed(), "最后 Receiver 析构未关闭发送方向");
  auto failed = sender.send(std::string{"b"});
  check(!failed.has_value() && failed.error().value() == "b",
        "无 Receiver send 未返还原值");

  check(sender.send_replace(std::string{"c"}) == "a",
        "无 Receiver send_replace 未更新值");
  auto reopened = sender.subscribe();
  check(!sender.is_closed() && reopened.borrow().value() == "c" &&
            !reopened.has_changed().value(),
        "subscribe 未按最新已读版本重开");
}

void test_async_changed_close_and_multi_receiver() {
  Runtime runtime;
  check(runtime.block_on(changed_after_send_root()),
        "changed wait-before-send 失败");
  check(runtime.block_on(unseen_then_closed_root()),
        "关闭前未读版本没有先成功");
  check(runtime.block_on(pending_close_root()),
        "最后 Sender drop 未唤醒 changed");
  check(runtime.block_on(multiple_receivers_root()),
        "多 Receiver 没有独立观察版本");
}

void test_cancellation_and_exclusive_operation() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  {
    auto pending = receiver.changed();
    bool rejected = false;
    try {
      (void)receiver.borrow();
    } catch (const std::logic_error &) {
      rejected = true;
    }
    check(rejected, "未拒绝同 Receiver 的并发 cursor 操作");
    (void)pending;
  }
  check(receiver.borrow().value() == 0,
        "取消未 poll changed 后 Receiver 未恢复");

  {
    auto pending = sender.closed();
    check(sender.sender_count() == 1,
          "closed 内部借用不应改变公开 sender_count");
    (void)pending;
  }
  check(sender.send(1).has_value(), "取消 closed 后 Sender 不可复用");

  Runtime runtime;
  check(runtime.block_on(cancellation_reuse_root()),
        "pending changed/wait_for/closed 取消后句柄不可复用");
}

void test_sender_closed_wait_for_and_reentry() {
  Runtime runtime;
  check(runtime.block_on(sender_closed_root()), "Sender::closed 等待失败");
  check(runtime.block_on(wait_for_updates_root()), "wait_for 更新序列失败");
  check(runtime.block_on(wait_for_closed_root()), "wait_for 关闭语义失败");
  check(runtime.block_on(predicate_reentry_root()),
        "wait_for predicate 锁外重入失败");
}

void test_cooperative_budget() {
  {
    Runtime runtime;
    check(runtime.block_on(changed_cooperative_root()),
          "watch changed ready 路径 cooperative 边界错误");
  }
  {
    Runtime runtime;
    check(runtime.block_on(closed_cooperative_root()),
          "watch closed ready 路径 cooperative 边界错误");
  }
  {
    Runtime runtime;
    check(runtime.block_on(wait_for_immediate_loop_single_budget_root()),
          "watch wait_for 同 poll 立即通知重复扣预算");
  }
  {
    Runtime runtime;
    check(runtime.block_on(changed_ready_exact_budget_root(false)),
          "watch changed success 未在第 129 次 ready 尝试让出");
  }
  {
    Runtime runtime;
    check(runtime.block_on(changed_ready_exact_budget_root(true)),
          "watch changed closed 未在第 129 次 ready 尝试让出");
  }
  {
    Runtime runtime;
    check(runtime.block_on(closed_ready_exact_budget_root()),
          "watch closed 未在第 129 次 ready 尝试让出");
  }
  {
    Runtime runtime;
    check(runtime.block_on(wait_for_ready_exact_budget_root(false)),
          "watch wait_for success 未在第 129 次 ready 尝试让出");
  }
  {
    Runtime runtime;
    check(runtime.block_on(wait_for_ready_exact_budget_root(true)),
          "watch wait_for closed 未在第 129 次 ready 尝试让出");
  }
  {
    Runtime runtime;
    check(runtime.block_on(changed_fresh_notification_budget_root()),
          "watch changed 真实通知恢复未精确扣一个 fresh budget");
  }
  {
    Runtime runtime;
    check(runtime.block_on(wait_for_fresh_notification_budget_root()),
          "watch wait_for 真实通知恢复未精确扣一个 fresh budget");
  }
  {
    Runtime runtime;
    check(runtime.block_on(closed_fresh_notification_budget_root()),
          "watch closed 真实通知恢复未精确扣一个 fresh budget");
  }
}

void test_predicate_exception_and_cursor_state() {
  auto [sender, receiver] = cio::sync::watch::channel(0);
  Runtime runtime;
  bool threw = false;
  try {
    (void)runtime.block_on(receiver.wait_for([](const int &) -> bool {
      throw std::runtime_error{"预期的 watch predicate 异常"};
    }));
  } catch (const std::runtime_error &) {
    threw = true;
  }
  check(threw, "wait_for 未传播 predicate 异常");
  check(!receiver.borrow().has_changed(),
        "predicate 异常前观察版本未标成已读");
  check(sender.send(8).has_value(), "predicate 异常后 Sender 不可复用");
  check(runtime.block_on(receiver.changed()).has_value() &&
            receiver.borrow().value() == 8,
        "predicate 异常后 Receiver 不可复用");
}

void test_wait_for_owned_lifetime_order() {
  Runtime runtime;
  check(runtime.block_on(predicate_destructor_order_root()),
        "wait_for 未先释放 Receiver operation 再析构 Predicate");
  check(runtime.block_on(false_snapshot_released_before_wait_root()),
        "wait_for 在真正等待通知前未释放 false Snapshot");
}

void test_cross_thread_and_runtime_shutdown() {
  {
    Runtime runtime;
    check(runtime.block_on(cross_thread_root()),
          "watch 跨线程发布/唤醒不可见");
  }

  auto [sender, receiver] = cio::sync::watch::channel(0);
  auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  {
    Runtime runtime;
    runtime.block_on(detach_pending_receiver(shared_receiver));
  }
  check(shared_receiver->borrow().value() == 0,
        "runtime shutdown 未取消并清理 pending changed");
  check(sender.send(1).has_value(),
        "runtime shutdown 后 watch channel 不可复用");
}

void test_user_lifetime_and_copy_exceptions() {
  const auto lifetime = std::make_shared<LifetimeState>();
  {
    auto [sender, receiver] =
        cio::sync::watch::channel(ReentrantValue{lifetime, 1});
    const auto observed_sender =
        std::make_shared<Sender<ReentrantValue>>(sender);
    const std::weak_ptr<Sender<ReentrantValue>> weak_sender{observed_sender};
    lifetime->callback = [weak_sender, lifetime] {
      const auto locked = weak_sender.lock();
      if (locked && locked->receiver_count() >= 1) {
        lifetime->reentered.fetch_add(1, std::memory_order_relaxed);
      }
    };
    check(sender.send(ReentrantValue{lifetime, 2}).has_value(),
          "ReentrantValue send 失败");
    check(lifetime->reentered.load(std::memory_order_relaxed) >= 1,
          "旧 T 析构未在 channel 锁外执行");
    lifetime->callback = {};
    (void)observed_sender;
    (void)receiver;
  }

  const auto throw_copy = std::make_shared<std::atomic<bool>>(false);
  auto [sender, receiver] =
      cio::sync::watch::channel(ThrowOnCopy{throw_copy, 1});
  throw_copy->store(true, std::memory_order_relaxed);
  bool threw = false;
  try {
    (void)sender.send_replace(ThrowOnCopy{throw_copy, 2});
  } catch (const std::runtime_error &) {
    threw = true;
  }
  check(threw, "send_replace 未传播旧值复制异常");
  throw_copy->store(false, std::memory_order_relaxed);
  check(receiver.borrow().value().value == 2 &&
            receiver.borrow().has_changed(),
        "返回旧值异常后新版本未保持已发布语义");

  auto [move_sender, move_receiver] =
      cio::sync::watch::channel(MoveOnlyWatchValue{1});
  {
    const auto retained = move_receiver.borrow();
    bool rejected = false;
    try {
      (void)move_sender.send_replace(MoveOnlyWatchValue{2});
    } catch (const std::logic_error &) {
      rejected = true;
    }
    const auto current = move_receiver.borrow();
    check(rejected && retained.value().value == 1 &&
              current.value().value == 1,
          "不可复制值存在 Snapshot 时 send_replace 未保持原状态");
  }
  const auto old =
      move_sender.send_replace(MoveOnlyWatchValue{2});
  const auto next = move_receiver.borrow();
  check(old.value == 1 && next.value().value == 2,
        "不可复制值最后 Snapshot 释放后 send_replace 失败");
}

void test_subscribe_drop_send_race() {
  constexpr std::size_t rounds{1000};
  for (std::size_t round = 0; round < rounds; ++round) {
    auto [sender, receiver] = cio::sync::watch::channel(0);
    auto reopened =
        std::make_shared<std::optional<Receiver<int>>>();
    auto send_outcome = std::make_shared<std::atomic<int>>(-1);

    std::thread dropper{
        [owner = std::optional<Receiver<int>>{std::move(receiver)}]() mutable {
          owner.reset();
        }};
    std::thread subscriber{[sender, reopened] {
      reopened->emplace(sender.subscribe());
    }};
    std::thread publisher{[sender, send_outcome] {
      const auto sent = sender.send(1);
      if (sent.has_value()) {
        send_outcome->store(1, std::memory_order_release);
        return;
      }
      send_outcome->store(
          sent.error().value() == 1 ? 0 : -2,
          std::memory_order_release);
    }};
    dropper.join();
    subscriber.join();
    publisher.join();

    const auto outcome = send_outcome->load(std::memory_order_acquire);
    check(reopened->has_value() && sender.receiver_count() == 1 &&
              (outcome == 0 || outcome == 1),
          "drop/subscribe/send 竞态破坏 Receiver 计数或值守恒");
    const auto value = reopened->value().borrow().value();
    check(value == (outcome == 1 ? 1 : 0),
          "drop/subscribe/send 竞态的线性化结果非法");
  }
}

static_assert(std::copy_constructible<Sender<int>>);
static_assert(std::copy_constructible<Receiver<int>>);
static_assert(std::move_constructible<Sender<int>>);
static_assert(std::move_constructible<Receiver<int>>);
static_assert(cio::Send<Sender<int>> && cio::Sync<Sender<int>>);
static_assert(cio::Send<Receiver<int>> && cio::Sync<Receiver<int>>);
static_assert(cio::Send<Snapshot<int>> && cio::Sync<Snapshot<int>>);
static_assert(cio::Send<RecvError> && cio::Sync<RecvError>);
static_assert(HasRvalueSnapshotValue<Snapshot<int>>);
static_assert(
    !HasRvalueSnapshotValue<Snapshot<std::unique_ptr<int>>>);
static_assert(
    !HasRvalueErrorValue<cio::sync::watch::error::SendError<int>>);
static_assert(
    HasRvalueErrorIntoInner<cio::sync::watch::error::SendError<int>>);
static_assert(
    !cio::detail::WatchOwnedValue<std::reference_wrapper<int>>);
static_assert(!cio::detail::WatchOwnedValue<int *>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"watch sync versions snapshots marking",
       test_sync_versions_snapshots_and_marking},
      {"watch counts subscribe close errors",
       test_counts_subscribe_close_and_errors},
      {"watch sender construction count overflow",
       test_sender_construction_and_count_overflow_guard},
      {"watch async changed close multi receiver",
       test_async_changed_close_and_multi_receiver},
      {"watch cancellation exclusive operation",
       test_cancellation_and_exclusive_operation},
      {"watch sender closed wait_for reentry",
       test_sender_closed_wait_for_and_reentry},
      {"watch cooperative budget", test_cooperative_budget},
      {"watch predicate exception cursor",
       test_predicate_exception_and_cursor_state},
      {"watch wait_for owned lifetime order",
       test_wait_for_owned_lifetime_order},
      {"watch cross thread runtime shutdown",
       test_cross_thread_and_runtime_shutdown},
      {"watch user lifetime copy exceptions",
       test_user_lifetime_and_copy_exceptions},
      {"watch subscribe drop send race",
       test_subscribe_drop_send_race},
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

  std::cout << "watch 全部通过：" << passed << " 项\n";
  return 0;
}
