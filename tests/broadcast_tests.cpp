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
#include "cio/sync/broadcast.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;
using cio::sync::broadcast::Receiver;
using cio::sync::broadcast::Sender;
using cio::sync::broadcast::WeakSender;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

Task<void>
mark_peer_progress(std::shared_ptr<std::atomic<bool>> progressed) {
  progressed->store(true, std::memory_order_release);
  co_return;
}

Task<bool> wait_before_send_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(4);
  auto waiting = cio::task::spawn(receiver.recv());
  co_await cio::task::yield_now();
  if (waiting.is_finished()) {
    co_return false;
  }
  const auto sent = sender.send(42);
  const auto joined = co_await waiting;
  co_return sent.has_value() && sent.value() == 1 &&
      joined.has_value() && joined.value().has_value() &&
      joined.value().value() == 42;
}

Task<bool> pending_recv_last_sender_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(4);
  auto waiting = cio::task::spawn(receiver.recv());
  co_await cio::task::yield_now();
  if (waiting.is_finished()) {
    co_return false;
  }
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && !joined.value().has_value() &&
      joined.value().error().is_closed() && receiver.is_closed();
}

Task<bool> last_sender_drains_then_closes_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(4);
  if (!sender.send(1).has_value() || !sender.send(2).has_value()) {
    co_return false;
  }
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();
  if (!receiver.is_closed()) {
    co_return false;
  }
  const auto first = co_await receiver.recv();
  const auto second = co_await receiver.recv();
  const auto closed = co_await receiver.recv();
  co_return first.has_value() && first.value() == 1 &&
      second.has_value() && second.value() == 2 &&
      !closed.has_value() && closed.error().is_closed();
}

Task<bool> sender_closed_reopen_before_repoll_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  auto waiting = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  if (waiting.is_finished()) {
    co_return false;
  }

  std::optional<Receiver<int>> first{std::move(receiver)};
  first.reset();
  auto reopened = sender.subscribe();
  co_await cio::task::yield_now();
  if (waiting.is_finished() || sender.receiver_count() != 1) {
    co_return false;
  }

  std::optional<Receiver<int>> second{std::move(reopened)};
  second.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && sender.receiver_count() == 0;
}

Task<bool> closed_operation_hidden_borrow_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  auto waiting = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  if (waiting.is_finished()) {
    co_return false;
  }

  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();
  const auto before_cancel = receiver.try_recv();
  if (receiver.is_closed() || before_cancel.has_value() ||
      !before_cancel.error().is_empty()) {
    co_return false;
  }

  waiting.abort();
  const auto joined = co_await waiting;
  const auto after_cancel = receiver.try_recv();
  co_return !joined.has_value() && receiver.is_closed() &&
      !after_cancel.has_value() && after_cancel.error().is_closed();
}

Task<bool> receive_cancellation_preserves_cursor_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  auto waiting = cio::task::spawn(receiver.recv());
  co_await cio::task::yield_now();
  waiting.abort();
  const auto joined = co_await waiting;
  if (joined.has_value() || !sender.send(9).has_value()) {
    co_return false;
  }
  const auto received = co_await receiver.recv();
  co_return received.has_value() && received.value() == 9;
}

Task<bool> blocking_recv_rejects_worker_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(1);
  (void)sender;
  bool rejected = false;
  try {
    (void)receiver.blocking_recv();
  } catch (const std::logic_error &) {
    rejected = true;
  }
  co_return rejected;
}

Task<bool> ready_value_exact_budget_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(256);
  for (int value = 0; value < 129; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (int value = 0; value < 128; ++value) {
    const auto received = co_await receiver.recv();
    if (!received.has_value() || received.value() != value ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  const auto received = co_await receiver.recv();
  const bool yielded = received.has_value() && received.value() == 128 &&
      progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<bool> ready_lagged_exact_budget_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(128);
  for (int value = 0; value < 129; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  const auto lagged = co_await receiver.recv();
  if (lagged.has_value() || !lagged.error().is_lagged() ||
      lagged.error().skipped() != 1 ||
      progressed->load(std::memory_order_acquire)) {
    co_return false;
  }
  for (int value = 1; value < 128; ++value) {
    const auto received = co_await receiver.recv();
    if (!received.has_value() || received.value() != value ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  const auto received = co_await receiver.recv();
  const bool yielded = received.has_value() && received.value() == 128 &&
      progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<bool> ready_closed_exact_budget_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  std::optional<Sender<int>> owner{std::move(sender)};
  owner.reset();

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    const auto closed = co_await receiver.recv();
    if (closed.has_value() || !closed.error().is_closed() ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  const auto closed = co_await receiver.recv();
  const bool yielded = !closed.has_value() && closed.error().is_closed() &&
      progressed->load(std::memory_order_acquire);
  const auto joined = co_await peer;
  co_return yielded && joined.has_value();
}

Task<void> publish_once(Sender<int> sender) {
  const auto sent = sender.send(7);
  if (!sent.has_value()) {
    throw std::runtime_error{"broadcast fresh wake send 失败"};
  }
  co_return;
}

Task<bool> fresh_wake_exact_budget_root() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  auto producer = cio::task::spawn(publish_once(sender));
  const auto received = co_await receiver.recv();
  if (!received.has_value() || received.value() != 7) {
    co_return false;
  }

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  for (std::size_t unit = 0; unit < 127; ++unit) {
    co_await cio::task::consume_budget();
    if (progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  co_await cio::task::consume_budget();
  const bool yielded = progressed->load(std::memory_order_acquire);
  const auto producer_join = co_await producer;
  const auto peer_join = co_await peer;
  co_return yielded && producer_join.has_value() && peer_join.has_value();
}

struct CopyFailureState final {
  std::atomic<bool> fail{false};
};

struct ThrowOnCopy final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<CopyFailureState> state;
  int value{0};

  ThrowOnCopy(std::shared_ptr<CopyFailureState> observed, int stored)
      : state{std::move(observed)}, value{stored} {}

  ThrowOnCopy(const ThrowOnCopy &other)
      : state{other.state}, value{other.value} {
    if (state->fail.load(std::memory_order_relaxed)) {
      throw std::runtime_error{"预期的 broadcast 复制异常"};
    }
  }

  ThrowOnCopy(ThrowOnCopy &&) noexcept = default;
  ThrowOnCopy &operator=(const ThrowOnCopy &) = delete;
  ThrowOnCopy &operator=(ThrowOnCopy &&) = delete;
};

struct ReentryState final {
  std::atomic<int> destroyed{0};
  std::atomic<int> reentered{0};
  std::function<void()> callback;
};

struct ReentrantValue final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<ReentryState> state;
  int value{0};
  bool active{true};

  ReentrantValue(std::shared_ptr<ReentryState> observed, int stored)
      : state{std::move(observed)}, value{stored} {}

  ReentrantValue(const ReentrantValue &other)
      : state{other.state}, value{other.value}, active{false} {}

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

void test_capacity_lag_resubscribe_and_counts() {
  bool zero_rejected = false;
  try {
    (void)cio::sync::broadcast::channel<int>(0);
  } catch (const std::invalid_argument &) {
    zero_rejected = true;
  }
  check(zero_rejected, "broadcast capacity=0 未拒绝");

  auto [sender, receiver] = cio::sync::broadcast::channel<int>(3);
  auto second = sender.subscribe();
  auto weak = sender.downgrade();
  auto sender_copy = sender;
  check(sender.strong_count() == 2 && sender.weak_count() == 1 &&
            sender.receiver_count() == 2 &&
            sender.same_channel(sender_copy) &&
            !sender.same_channel(
                cio::sync::broadcast::channel<int>(1).first),
        "broadcast 初始计数或 same_channel 错误");
  check(weak.strong_count() == 2 && weak.weak_count() == 1 &&
            weak.upgrade().has_value(),
        "broadcast WeakSender 初始计数或 upgrade 错误");

  for (int value = 0; value < 5; ++value) {
    check(sender.send(value).has_value(), "broadcast 填充环失败");
  }
  check(receiver.len() == 5 && second.len() == 5,
        "broadcast Receiver::len 未保留 lag 距离");
  auto first_lag = receiver.try_recv();
  check(!first_lag.has_value() && first_lag.error().is_lagged() &&
            first_lag.error().skipped() == 1 && receiver.len() == 4,
        "broadcast capacity=3 未向上取整为4或 Lagged 数错误");
  auto oldest = receiver.try_recv();
  check(oldest.has_value() && oldest.value() == 1,
        "broadcast Lagged 后未从最老保留值继续");

  auto resubscribed = receiver.resubscribe();
  auto resubscribed_empty = resubscribed.try_recv();
  check(!resubscribed_empty.has_value() &&
            resubscribed_empty.error().is_empty(),
        "broadcast resubscribe 错误继承历史 backlog");
}

void test_len_endpoint_queries_and_receiver_drop() {
  auto [sender, first] = cio::sync::broadcast::channel<int>(4);
  auto second = sender.subscribe();
  auto sender_copy = sender;
  auto weak = sender.downgrade();
  auto [foreign_sender, foreign_receiver] =
      cio::sync::broadcast::channel<int>(1);

  check(sender.is_empty() && first.is_empty() && second.is_empty() &&
            first.same_channel(second) &&
            !first.same_channel(foreign_receiver) &&
            first.sender_strong_count() == 2 &&
            first.sender_weak_count() == 1 &&
            weak.strong_count() == 2 && weak.weak_count() == 1,
        "broadcast endpoint 初始查询或计数错误");
  (void)foreign_sender;

  check(sender.send(1).has_value() && sender.len() == 1 &&
            first.len() == 1 && second.len() == 1,
        "broadcast 首条 send 后 len 错误");
  check(first.try_recv().has_value() && sender.len() == 1 &&
            first.is_empty() && !second.is_empty(),
        "broadcast 单 Receiver 领取后 Sender len 提前减少");
  check(second.try_recv().has_value() && sender.is_empty(),
        "broadcast 全部 Receiver 领取后 Sender 未清空");

  check(sender.send(2).has_value() && sender.send(3).has_value(),
        "broadcast len/drop 测试 send 失败");
  check(first.try_recv().has_value() && sender.len() == 2,
        "broadcast 部分领取后 Sender len 错误");
  std::optional<Receiver<int>> first_owner{std::move(first)};
  first_owner.reset();
  check(sender.receiver_count() == 1 && sender.len() == 2,
        "broadcast Receiver drop 错误删除其他 Receiver backlog");
  auto second_value = second.try_recv();
  check(second_value.has_value() && second_value.value() == 2 &&
            sender.len() == 1,
        "broadcast backlog 首条最后领取后的 len 错误");
  auto third_value = second.try_recv();
  check(third_value.has_value() && third_value.value() == 3 &&
            sender.is_empty(),
        "broadcast backlog 全领取后的 len 错误");

  check(sender.send(4).has_value(), "broadcast drop unread send 失败");
  std::optional<Receiver<int>> second_owner{std::move(second)};
  second_owner.reset();
  check(sender.receiver_count() == 0 && sender.is_empty(),
        "broadcast 最后 Receiver drop 未释放 unread backlog");
}

void test_failed_send_and_subscribe_reopen() {
  auto sender = Sender<std::string>::new_sender(2);
  auto failed = sender.send(std::string{"old"});
  check(!failed.has_value() && failed.error().value() == "old" &&
            sender.receiver_count() == 0,
        "broadcast 无 Receiver send 未返还原值");

  auto receiver = sender.subscribe();
  auto empty = receiver.try_recv();
  check(!empty.has_value() && empty.error().is_empty(),
        "broadcast subscribe 错误观察失败 send");
  check(sender.send(std::string{"new"}).has_value(),
        "broadcast subscribe reopen 后 send 失败");
  auto received = receiver.try_recv();
  check(received.has_value() && received.value() == "new",
        "broadcast subscribe reopen 后接收错误");

  auto failed_again = Sender<std::string>::new_sender(1)
                          .send(std::string{"temporary-safe"});
  check(!failed_again.has_value() &&
            std::move(failed_again).error().into_inner() ==
                "temporary-safe",
        "broadcast SendError::into_inner 未安全返还临时错误值");
}

void test_unpolled_receive_exclusion_and_moved_from() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(2);
  {
    auto pending = receiver.recv();
    bool rejected = false;
    try {
      (void)receiver.try_recv();
    } catch (const std::logic_error &) {
      rejected = true;
    }
    check(rejected,
          "broadcast 未 poll recv 未立即独占 Receiver operation");
    (void)pending;
  }
  check(sender.send(5).has_value(),
        "broadcast 未 poll recv 取消后 Sender 不可复用");
  auto recovered = receiver.try_recv();
  check(recovered.has_value() && recovered.value() == 5,
        "broadcast 未 poll recv 取消后 Receiver 不可复用");

  auto moved_sender = std::move(sender);
  bool sender_rejected = false;
  try {
    (void)sender.send(6);
  } catch (const std::logic_error &) {
    sender_rejected = true;
  }
  auto weak = moved_sender.downgrade();
  auto moved_weak = std::move(weak);
  bool weak_rejected = false;
  try {
    (void)weak.upgrade();
  } catch (const std::logic_error &) {
    weak_rejected = true;
  }
  auto moved_receiver = std::move(receiver);
  bool receiver_rejected = false;
  try {
    (void)receiver.try_recv();
  } catch (const std::logic_error &) {
    receiver_rejected = true;
  }
  check(sender_rejected && weak_rejected && receiver_rejected &&
            moved_weak.upgrade().has_value() &&
            moved_sender.send(7).has_value() &&
            moved_receiver.try_recv().has_value(),
        "broadcast moved-from 状态未拒绝或 moved-to 不可复用");
}

void test_copy_exception_advances_cursor() {
  const auto state = std::make_shared<CopyFailureState>();
  auto [sender, receiver] =
      cio::sync::broadcast::channel<ThrowOnCopy>(2);
  check(sender.send(ThrowOnCopy{state, 1}).has_value(),
        "broadcast 复制异常测试首条 send 失败");
  state->fail.store(true, std::memory_order_relaxed);
  bool threw = false;
  try {
    (void)receiver.try_recv();
  } catch (const std::runtime_error &) {
    threw = true;
  }
  state->fail.store(false, std::memory_order_relaxed);
  check(threw && sender.send(ThrowOnCopy{state, 2}).has_value(),
        "broadcast 未传播复制异常或异常后不可复用");
  auto next = receiver.try_recv();
  check(next.has_value() && next.value().value == 2,
        "broadcast 复制异常后游标未推进");
}

void test_destructor_reentry_is_outside_channel_lock() {
  const auto state = std::make_shared<ReentryState>();
  auto channel =
      cio::sync::broadcast::channel<ReentrantValue>(1);
  auto sender =
      std::make_shared<Sender<ReentrantValue>>(std::move(channel.first));
  auto receiver = std::move(channel.second);
  const std::weak_ptr<Sender<ReentrantValue>> weak_sender{sender};
  state->callback = [state, weak_sender] {
    if (const auto locked = weak_sender.lock()) {
      (void)locked->receiver_count();
      state->reentered.fetch_add(1, std::memory_order_relaxed);
    }
  };

  check(sender->send(ReentrantValue{state, 1}).has_value() &&
            sender->send(ReentrantValue{state, 2}).has_value() &&
            state->reentered.load(std::memory_order_relaxed) >= 1,
        "broadcast 覆盖值析构未能锁外重入");
  state->callback = {};
  auto latest_lag = receiver.try_recv();
  check(!latest_lag.has_value() && latest_lag.error().is_lagged() &&
            latest_lag.error().skipped() == 1,
        "broadcast 析构重入测试未产生预期 lag");
  check(receiver.try_recv().has_value(),
        "broadcast 析构重入后 channel 不可继续接收");
}

void test_last_consume_and_receiver_drop_destructor_reentry() {
  {
    const auto state = std::make_shared<ReentryState>();
    auto channel =
        cio::sync::broadcast::channel<ReentrantValue>(2);
    auto sender =
        std::make_shared<Sender<ReentrantValue>>(std::move(channel.first));
    auto receiver = std::move(channel.second);
    const std::weak_ptr<Sender<ReentrantValue>> weak_sender{sender};
    state->callback = [state, weak_sender] {
      if (const auto locked = weak_sender.lock()) {
        (void)locked->len();
        state->reentered.fetch_add(1, std::memory_order_relaxed);
      }
    };
    check(sender->send(ReentrantValue{state, 1}).has_value(),
          "broadcast last-consume 重入 send 失败");
    auto consumed = receiver.try_recv();
    check(consumed.has_value() &&
              state->reentered.load(std::memory_order_relaxed) >= 1,
          "broadcast 最后消费析构未能锁外重入");
    state->callback = {};
  }

  {
    const auto state = std::make_shared<ReentryState>();
    auto channel =
        cio::sync::broadcast::channel<ReentrantValue>(2);
    auto sender =
        std::make_shared<Sender<ReentrantValue>>(std::move(channel.first));
    const std::weak_ptr<Sender<ReentrantValue>> weak_sender{sender};
    state->callback = [state, weak_sender] {
      if (const auto locked = weak_sender.lock()) {
        (void)locked->receiver_count();
        state->reentered.fetch_add(1, std::memory_order_relaxed);
      }
    };
    check(sender->send(ReentrantValue{state, 2}).has_value(),
          "broadcast Receiver-drop 重入 send 失败");
    std::optional<Receiver<ReentrantValue>> receiver{
        std::move(channel.second)};
    receiver.reset();
    check(state->reentered.load(std::memory_order_relaxed) >= 1 &&
              sender->receiver_count() == 0,
          "broadcast Receiver drop 析构未能锁外重入");
    state->callback = {};
  }
}

void test_blocking_recv_bridge() {
  auto [sender, receiver] = cio::sync::broadcast::channel<int>(1);
  const auto started = std::make_shared<std::atomic<bool>>(false);
  const auto result =
      std::make_shared<std::optional<cio::Result<
          int, cio::sync::broadcast::error::RecvError>>>();
  std::thread consumer{
      [receiver = std::move(receiver), started, result]() mutable {
        started->store(true, std::memory_order_release);
        *result = receiver.blocking_recv();
      }};
  while (!started->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  check(sender.send(77).has_value(),
        "broadcast blocking_recv 普通线程 send 失败");
  consumer.join();
  check(result->has_value() && result->value().has_value() &&
            result->value().value() == 77,
        "broadcast blocking_recv 普通线程结果错误");

  Runtime runtime;
  check(runtime.block_on(blocking_recv_rejects_worker_root()),
        "broadcast blocking_recv 未拒绝 runtime worker");
}

void test_multi_sender_total_order() {
  constexpr int per_sender = 200;
  auto [sender, first] =
      cio::sync::broadcast::channel<int>(512);
  auto second = sender.subscribe();
  auto sender_two = sender;
  const auto failed = std::make_shared<std::atomic<bool>>(false);

  std::thread one{[sender, failed]() mutable {
    for (int value = 0; value < per_sender; ++value) {
      if (!sender.send(value).has_value()) {
        failed->store(true, std::memory_order_relaxed);
      }
    }
  }};
  std::thread two{[sender_two, failed]() mutable {
    for (int value = 0; value < per_sender; ++value) {
      if (!sender_two.send(1000 + value).has_value()) {
        failed->store(true, std::memory_order_relaxed);
      }
    }
  }};
  one.join();
  two.join();
  check(!failed->load(std::memory_order_relaxed),
        "broadcast 多 Sender 并发 send 失败");

  std::vector<int> first_order;
  std::vector<int> second_order;
  first_order.reserve(per_sender * 2);
  second_order.reserve(per_sender * 2);
  for (int index = 0; index < per_sender * 2; ++index) {
    auto first_value = first.try_recv();
    auto second_value = second.try_recv();
    check(first_value.has_value() && second_value.has_value(),
          "broadcast 非 lag Receiver 丢失并发消息");
    first_order.push_back(first_value.value());
    second_order.push_back(second_value.value());
  }
  check(first_order == second_order,
        "broadcast 两个非 lag Receiver 未观察同一总序");
}

void test_overflow_helpers() {
  auto count = std::numeric_limits<std::size_t>::max();
  bool count_rejected = false;
  try {
    cio::detail::broadcast_checked_increment(
        count, "预期的 broadcast 计数溢出");
  } catch (const std::length_error &) {
    count_rejected = true;
  }
  check(count_rejected &&
            count == std::numeric_limits<std::size_t>::max(),
        "broadcast 计数溢出未拒绝或改变原值");

  const auto maximum_requested =
      std::numeric_limits<std::size_t>::max() >> 1U;
  const auto normalized =
      cio::detail::broadcast_effective_capacity(maximum_requested);
  bool capacity_rejected = false;
  try {
    (void)cio::detail::broadcast_effective_capacity(
        maximum_requested + 1);
  } catch (const std::length_error &) {
    capacity_rejected = true;
  }
  check(normalized == maximum_requested + 1 && capacity_rejected,
        "broadcast 容量上界 normalization 错误");
}

void test_weak_upgrade_last_sender_race() {
  for (std::size_t round = 0; round < 500; ++round) {
    auto [sender, receiver] = cio::sync::broadcast::channel<int>(1);
    auto weak = sender.downgrade();
    const auto owner =
        std::make_shared<std::optional<Sender<int>>>(std::move(sender));
    const auto upgraded =
        std::make_shared<std::optional<Sender<int>>>();
    const auto start = std::make_shared<std::atomic<bool>>(false);

    std::thread dropper{[owner, start] {
      while (!start->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      owner->reset();
    }};
    std::thread upgrader{[weak, upgraded, start]() mutable {
      start->store(true, std::memory_order_release);
      *upgraded = weak.upgrade();
    }};
    dropper.join();
    upgrader.join();

    if (upgraded->has_value()) {
      check(!receiver.is_closed() && weak.strong_count() == 1,
            "broadcast Weak upgrade 成功却已关闭或强计数错误");
      upgraded->reset();
    }
    check(receiver.is_closed() && !weak.upgrade().has_value(),
          "broadcast 最后强 Sender 后 Weak 复活");
  }
}

void test_async_lost_wake_close_cancel_and_budget() {
  Runtime runtime;
  for (std::size_t round = 0; round < 200; ++round) {
    check(runtime.block_on(wait_before_send_root()),
          "broadcast wait-before-send 丢失唤醒");
    check(runtime.block_on(pending_recv_last_sender_root()),
          "broadcast pending recv 未被最后 Sender 唤醒");
  }
  check(runtime.block_on(last_sender_drains_then_closes_root()),
        "broadcast 最后 Sender 后未先 drain 再 Closed");
  check(runtime.block_on(sender_closed_reopen_before_repoll_root()),
        "broadcast Sender::closed 在 resubscribe 后错误完成");
  check(runtime.block_on(closed_operation_hidden_borrow_root()),
        "broadcast closed operation 未维持隐藏 Sender 借用");
  check(runtime.block_on(receive_cancellation_preserves_cursor_root()),
        "broadcast recv 取消改变游标或未释放 operation");
  check(runtime.block_on(ready_value_exact_budget_root()),
        "broadcast value ready cooperative 精确边界错误");
  check(runtime.block_on(ready_lagged_exact_budget_root()),
        "broadcast Lagged ready cooperative 精确边界错误");
  check(runtime.block_on(ready_closed_exact_budget_root()),
        "broadcast Closed ready cooperative 精确边界错误");
  check(runtime.block_on(fresh_wake_exact_budget_root()),
        "broadcast fresh wake 未精确扣一个 budget");
}

template <typename T>
concept HasRvalueErrorValue =
    requires(T error) { std::move(error).value(); };

template <typename T>
concept HasSenderCapacity =
    requires(const T &sender) { sender.capacity(); };

static_assert(!std::is_copy_constructible_v<Receiver<int>>);
static_assert(!std::is_copy_assignable_v<Receiver<int>>);
static_assert(!HasRvalueErrorValue<
              cio::sync::broadcast::error::SendError<int>>);
static_assert(
    !cio::detail::BroadcastOwnedValue<std::reference_wrapper<int>>);
static_assert(!cio::detail::BroadcastOwnedValue<int *>);
static_assert(!cio::detail::BroadcastOwnedValue<std::span<int>>);
static_assert(
    !cio::detail::BroadcastOwnedValue<std::string_view>);
static_assert(!HasSenderCapacity<Sender<int>>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"broadcast capacity lag resubscribe counts",
       test_capacity_lag_resubscribe_and_counts},
      {"broadcast len endpoint queries receiver drop",
       test_len_endpoint_queries_and_receiver_drop},
      {"broadcast failed send subscribe reopen",
       test_failed_send_and_subscribe_reopen},
      {"broadcast unpolled receive moved-from",
       test_unpolled_receive_exclusion_and_moved_from},
      {"broadcast copy exception cursor",
       test_copy_exception_advances_cursor},
      {"broadcast destructor reentry",
       test_destructor_reentry_is_outside_channel_lock},
      {"broadcast consume drop destructor reentry",
       test_last_consume_and_receiver_drop_destructor_reentry},
      {"broadcast blocking recv bridge", test_blocking_recv_bridge},
      {"broadcast multi sender total order",
       test_multi_sender_total_order},
      {"broadcast weak upgrade race",
       test_weak_upgrade_last_sender_race},
      {"broadcast overflow helpers", test_overflow_helpers},
      {"broadcast async lost wake close cancel budget",
       test_async_lost_wake_close_cancel_and_budget},
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
  std::cout << "broadcast tests passed: " << passed << '/' << tests.size()
            << '\n';
  return 0;
}
