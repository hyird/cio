#include <atomic>
#include <chrono>
#include <concepts>
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

#include "cio/runtime/runtime.hpp"
#include "cio/send.hpp"
#include "cio/sync/mpsc.hpp"
#include "cio/task/spawn.hpp"
#include "cio/task/task.hpp"
#include "cio/task/yield_now.hpp"

namespace {

using namespace std::chrono_literals;

using cio::Task;
using cio::runtime::Runtime;
using cio::sync::mpsc::UnboundedReceiver;
using cio::sync::mpsc::UnboundedSender;
using cio::sync::mpsc::WeakUnboundedSender;
using cio::sync::mpsc::error::SendError;
using cio::sync::mpsc::error::TryRecvError;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

struct DropProbe final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<std::atomic<int>> drops;
  std::shared_ptr<std::function<void()>> on_drop;
  int value{0};
  bool armed{true};

  DropProbe(std::shared_ptr<std::atomic<int>> drops, int value,
            std::shared_ptr<std::function<void()>> on_drop = {})
      : drops{std::move(drops)}, on_drop{std::move(on_drop)}, value{value} {}

  DropProbe(const DropProbe &) = delete;
  DropProbe &operator=(const DropProbe &) = delete;

  DropProbe(DropProbe &&other) noexcept
      : drops{std::move(other.drops)}, on_drop{std::move(other.on_drop)},
        value{other.value}, armed{std::exchange(other.armed, false)} {}

  DropProbe &operator=(DropProbe &&other) noexcept {
    if (this != &other) {
      release();
      drops = std::move(other.drops);
      on_drop = std::move(other.on_drop);
      value = other.value;
      armed = std::exchange(other.armed, false);
    }
    return *this;
  }

  ~DropProbe() { release(); }

private:
  void release() noexcept {
    if (!std::exchange(armed, false)) {
      return;
    }
    if (drops) {
      drops->fetch_add(1, std::memory_order_relaxed);
    }
    if (on_drop && *on_drop) {
      try {
        (*on_drop)();
      } catch (...) {
        std::terminate();
      }
    }
  }
};

struct ThrowingMove final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<std::atomic<int>> moves;
  std::shared_ptr<std::atomic<int>> throw_on;
  int value{0};

  ThrowingMove(std::shared_ptr<std::atomic<int>> moves,
               std::shared_ptr<std::atomic<int>> throw_on, int value)
      : moves{std::move(moves)}, throw_on{std::move(throw_on)},
        value{value} {}

  ThrowingMove(const ThrowingMove &) = delete;
  ThrowingMove &operator=(const ThrowingMove &) = delete;

  ThrowingMove(ThrowingMove &&other)
      : moves{other.moves}, throw_on{other.throw_on}, value{other.value} {
    const int move = moves->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (throw_on->load(std::memory_order_acquire) == move) {
      throw std::runtime_error{"预期的 unbounded mpsc 用户移动异常"};
    }
  }

  ThrowingMove &operator=(ThrowingMove &&) = delete;
};

static_assert(!std::copy_constructible<UnboundedReceiver<int>>);
static_assert(std::move_constructible<UnboundedReceiver<int>>);
static_assert(cio::detail::MpscOwnedValue<int>);
static_assert(!cio::detail::MpscOwnedValue<int *>);
static_assert(!cio::detail::MpscOwnedValue<std::reference_wrapper<int>>);
static_assert(UnboundedSender<int>::cio_send);
static_assert(UnboundedSender<int>::cio_sync);

Task<bool> wake_before_wait_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto sent_one = sender.send(1);
  auto sent_two = sender.send(2);
  auto first = co_await receiver.recv();
  auto second = co_await receiver.recv();
  co_return sent_one.has_value() && sent_two.has_value() &&
      first == std::optional<int>{1} &&
      second == std::optional<int>{2};
}

Task<bool> wait_before_wake_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto producer = std::thread{[sender]() mutable {
    std::this_thread::sleep_for(5ms);
    if (!sender.send(7).has_value()) {
      std::terminate();
    }
  }};
  auto received = co_await receiver.recv();
  producer.join();
  co_return received == std::optional<int>{7};
}

Task<bool> close_drain_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  if (!sender.send(10).has_value() ||
      !sender.send(20).has_value() || receiver.len() != 2) {
    co_return false;
  }
  receiver.close();
  auto rejected = sender.send(30);
  auto first = co_await receiver.recv();
  auto second = co_await receiver.recv();
  auto terminal = co_await receiver.recv();
  co_return !rejected.has_value() &&
      rejected.error().value() == 30 && sender.is_closed() &&
      receiver.is_closed() && first == std::optional<int>{10} &&
      second == std::optional<int>{20} && !terminal &&
      receiver.is_empty() && receiver.len() == 0;
}

Task<bool> closed_waiters_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto first = cio::task::spawn(sender.closed());
  auto second = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  if (first.is_finished() || second.is_finished()) {
    co_return false;
  }
  receiver.close();
  auto first_result = co_await first;
  auto second_result = co_await second;
  co_await sender.closed();
  co_return first_result.has_value() && second_result.has_value() &&
      sender.is_closed();
}

Task<bool> cancelled_closed_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto pending = cio::task::spawn(sender.closed());
  co_await cio::task::yield_now();
  pending.abort();
  auto joined = co_await pending;
  if (joined.has_value() || !joined.error().is_cancelled()) {
    co_return false;
  }
  receiver.close();
  co_await sender.closed();
  co_return true;
}

Task<bool> cancelled_recv_root() {
  auto [sender, receiver] = cio::sync::mpsc::unbounded_channel<int>();
  auto pending = cio::task::spawn(receiver.recv());
  co_await cio::task::yield_now();
  pending.abort();
  auto joined = co_await pending;
  if (joined.has_value() || !joined.error().is_cancelled()) {
    co_return false;
  }
  if (!sender.send(44).has_value()) {
    co_return false;
  }
  auto retry = co_await receiver.recv();
  co_return retry == std::optional<int>{44};
}

Task<bool> blocking_rejected_root(UnboundedReceiver<int> receiver) {
  try {
    (void)receiver.blocking_recv();
  } catch (const std::logic_error &) {
    co_return true;
  }
  co_return false;
}

Task<void> mark_peer(std::shared_ptr<std::atomic<bool>> progressed) {
  progressed->store(true, std::memory_order_release);
  co_return;
}

Task<bool> ready_recv_budget_root() {
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  for (std::size_t value = 0; value < 512; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }

  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer(progressed));
  std::size_t received = 0;
  while (!progressed->load(std::memory_order_acquire) &&
         received < 512) {
    auto value = co_await receiver.recv();
    if (!value || *value != received) {
      co_return false;
    }
    ++received;
  }
  auto peer_result = co_await peer;
  co_return peer_result.has_value() &&
      progressed->load(std::memory_order_acquire) && received < 512;
}

Task<void> notify_receiver(UnboundedSender<std::size_t> sender) {
  if (!sender.send(1).has_value()) {
    throw std::runtime_error{"unbounded 通知预算测试发送失败"};
  }
  co_return;
}

Task<bool> notification_fresh_poll_budget_root() {
  auto [first_sender, first_receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  auto producer =
      cio::task::spawn(notify_receiver(std::move(first_sender)));
  auto first = co_await first_receiver.recv();
  if (first != std::optional<std::size_t>{1}) {
    co_return false;
  }

  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<std::size_t>();
  for (std::size_t value = 0; value < 256; ++value) {
    if (!sender.send(value).has_value()) {
      co_return false;
    }
  }
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer(progressed));
  std::size_t completed = 0;
  while (!progressed->load(std::memory_order_acquire) &&
         completed < 256) {
    auto value = co_await receiver.recv();
    if (!value || *value != completed) {
      co_return false;
    }
    ++completed;
  }
  auto producer_result = co_await producer;
  auto peer_result = co_await peer;
  co_return producer_result.has_value() && peer_result.has_value() &&
      progressed->load(std::memory_order_acquire) && completed == 128;
}

Task<bool> closed_does_not_consume_budget_root() {
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  receiver.close();
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer(progressed));
  for (int iteration = 0; iteration < 512; ++iteration) {
    co_await sender.closed();
    if (progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  auto peer_result = co_await peer;
  co_return peer_result.has_value() &&
      progressed->load(std::memory_order_acquire);
}

Task<bool> synchronous_send_does_not_consume_budget_root() {
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer(progressed));
  for (int value = 0; value < 512; ++value) {
    if (!sender.send(value).has_value() ||
        progressed->load(std::memory_order_acquire)) {
      co_return false;
    }
  }
  auto peer_result = co_await peer;
  co_return peer_result.has_value() &&
      progressed->load(std::memory_order_acquire) &&
      receiver.len() == 512;
}

Task<void> marked_pending_recv(
    Task<std::optional<int>> receive,
    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  (void)co_await std::move(receive);
}

Task<bool> install_pending_recv(
    Task<std::optional<int>> receive,
    std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(
      marked_pending_recv(std::move(receive), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

Task<void> marked_pending_closed(
    UnboundedSender<int> sender,
    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_await sender.closed();
}

Task<bool> install_pending_closed(
    UnboundedSender<int> sender,
    std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(
      marked_pending_closed(std::move(sender), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

void test_basic_api_and_errors() {
  Runtime runtime;
  check(runtime.block_on(wake_before_wait_root()),
        "unbounded mpsc wake-before-wait/FIFO 错误");
  check(runtime.block_on(wait_before_wake_root()),
        "unbounded mpsc wait-before-wake 丢失唤醒");
  check(runtime.block_on(close_drain_root()),
        "unbounded mpsc close/drain/SendError 错误");

  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  check(receiver.is_empty() && receiver.len() == 0 &&
            !sender.is_closed() && !receiver.is_closed(),
        "unbounded channel 初始观察错误");
  check(sender.send(5).has_value() && !receiver.is_empty() &&
            receiver.len() == 1,
        "同步 send 或 len/is_empty 错误");
  auto first = receiver.try_recv();
  auto empty = receiver.try_recv();
  check(first.has_value() && first.value() == 5 &&
            !empty.has_value() &&
            empty.error() == TryRecvError::empty &&
            receiver.is_empty(),
        "try_recv 成功/Empty 错误");

  std::optional<UnboundedReceiver<int>> owned_receiver{
      std::move(receiver)};
  owned_receiver.reset();
  auto rejected = sender.send(9);
  check(!rejected.has_value() && rejected.error().value() == 9 &&
            rejected.error().message() == "channel closed" &&
            SendError<int>::debug_string() == "SendError { .. }",
        "Receiver drop 后 SendError payload/格式错误");
  std::ostringstream display;
  display << rejected.error();
  std::ostringstream recv_display;
  recv_display << TryRecvError::disconnected;
  check(display.str() == "channel closed" &&
            recv_display.str() == "receiving on a closed channel" &&
            cio::sync::mpsc::error::debug_string(
                TryRecvError::empty) == "Empty",
        "unbounded mpsc 错误 Display/Debug 格式错误");
}

void test_closed_cancel_and_drop() {
  Runtime runtime;
  check(runtime.block_on(closed_waiters_root()),
        "UnboundedSender::closed 多 waiter 或永久 ready 错误");
  check(runtime.block_on(cancelled_closed_root()),
        "UnboundedSender::closed 取消/重试错误");
  check(runtime.block_on(cancelled_recv_root()),
        "UnboundedReceiver::recv 取消消费消息或未释放独占状态");

  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto pending = runtime.spawn(sender.closed());
  runtime.block_on([]() -> Task<void> {
    co_await cio::task::yield_now();
  }());
  std::optional<UnboundedReceiver<int>> dropped{std::move(receiver)};
  dropped.reset();
  auto joined = runtime.block_on(
      [&pending]() -> Task<cio::Result<void, cio::task::JoinError>> {
        co_return co_await pending;
      }());
  check(joined.has_value() && sender.is_closed(),
        "Receiver drop 未唤醒 UnboundedSender::closed");
}

void test_counts_weak_and_identity() {
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto sender_two = sender;
  auto weak_one = sender.downgrade();
  auto weak_two = weak_one;
  auto foreign = cio::sync::mpsc::unbounded_channel<int>();
  check(sender.same_channel(sender_two) &&
            !sender.same_channel(foreign.first) &&
            sender.strong_count() == 2 && sender.weak_count() == 2 &&
            weak_one.strong_count() == 2 && weak_two.weak_count() == 2 &&
            receiver.sender_strong_count() == 2 &&
            receiver.sender_weak_count() == 2,
        "unbounded sender identity 或初始计数错误");

  sender_two = UnboundedSender<int>{sender};
  weak_two = WeakUnboundedSender<int>{weak_one};
  check(sender.strong_count() == 2 && sender.weak_count() == 2,
        "unbounded copy assignment 计数错误");
  {
    auto upgraded = weak_one.upgrade();
    check(upgraded.has_value() && sender.strong_count() == 3,
          "WeakUnboundedSender upgrade 未增加强计数");
  }
  check(sender.strong_count() == 2,
        "升级 UnboundedSender drop 未恢复强计数");

  check(sender.send(81).has_value(), "weak drain 测试发送失败");
  std::optional<UnboundedSender<int>> first{std::move(sender)};
  std::optional<UnboundedSender<int>> second{std::move(sender_two)};
  first.reset();
  check(weak_one.strong_count() == 1 && weak_one.upgrade().has_value(),
        "仍有强发送端时 weak upgrade 失败");
  second.reset();
  check(weak_one.strong_count() == 0 && !weak_one.upgrade().has_value() &&
            receiver.is_closed(),
        "强计数归零后 weak 复活 channel");
  auto value = receiver.try_recv();
  auto terminal = receiver.try_recv();
  check(value.has_value() && value.value() == 81 &&
            !terminal.has_value() &&
            terminal.error() == TryRecvError::disconnected,
        "最后 sender drop 后未 drain 或未终止");

  {
    auto closed_channel =
        cio::sync::mpsc::unbounded_channel<int>();
    auto weak = closed_channel.first.downgrade();
    closed_channel.second.close();
    auto upgraded = weak.upgrade();
    check(upgraded.has_value() && upgraded->is_closed() &&
              upgraded->strong_count() == 2,
          "Receiver close 后仍有 strong 时 weak 未升级为 closed Sender");
  }

  {
    auto dropped_channel =
        cio::sync::mpsc::unbounded_channel<int>();
    auto weak = dropped_channel.first.downgrade();
    std::optional<UnboundedReceiver<int>> dropped{
        std::move(dropped_channel.second)};
    dropped.reset();
    auto upgraded = weak.upgrade();
    check(upgraded.has_value() && upgraded->is_closed(),
          "Receiver drop 后仍有 strong 时 weak 未升级为 closed Sender");
  }
}

void test_blocking_and_cross_thread() {
  {
    auto [sender, receiver] =
        cio::sync::mpsc::unbounded_channel<int>();
    auto received = std::make_shared<std::optional<int>>();
    std::thread consumer{
        [receiver = std::move(receiver), received]() mutable {
          *received = receiver.blocking_recv();
        }};
    std::this_thread::sleep_for(5ms);
    check(sender.send(123).has_value(),
          "blocking_recv 跨线程测试发送失败");
    consumer.join();
    check(*received == std::optional<int>{123},
          "blocking_recv 跨线程桥接错误");
  }

  {
    Runtime runtime;
    auto channel = cio::sync::mpsc::unbounded_channel<int>();
    check(runtime.block_on(
              blocking_rejected_root(std::move(channel.second))),
          "blocking_recv 未拒绝 runtime worker");
  }

  {
    constexpr int producer_count = 4;
    constexpr int values_per_producer = 2000;
    auto channel = cio::sync::mpsc::unbounded_channel<int>();
    std::optional<UnboundedSender<int>> root_sender{
        std::move(channel.first)};
    auto receiver = std::move(channel.second);
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int producer = 0; producer < producer_count; ++producer) {
      auto sender = *root_sender;
      producers.emplace_back(
          [sender = std::move(sender), producer]() mutable {
            for (int index = 0; index < values_per_producer; ++index) {
              if (!sender.send(producer * values_per_producer +
                               index + 1)
                       .has_value()) {
                std::terminate();
              }
            }
          });
    }
    for (auto &producer : producers) {
      producer.join();
    }
    root_sender.reset();

    std::size_t count = 0;
    std::int64_t sum = 0;
    for (;;) {
      auto value = receiver.try_recv();
      if (!value.has_value()) {
        check(value.error() == TryRecvError::disconnected,
              "多生产者 drain 意外返回 Empty");
        break;
      }
      ++count;
      sum += value.value();
    }
    constexpr std::int64_t total =
        producer_count * values_per_producer;
    const std::int64_t expected_sum = total * (total + 1) / 2;
    check(count == static_cast<std::size_t>(total) &&
              sum == expected_sum,
          "unbounded 多生产者压力测试值丢失、重复或损坏");
  }
}

void test_close_send_linearization_stress() {
  constexpr int producer_count = 4;
  constexpr int attempts_per_producer = 4000;
  auto [sender, receiver] =
      cio::sync::mpsc::unbounded_channel<int>();
  auto started = std::make_shared<std::atomic<int>>(0);
  auto successes = std::make_shared<std::atomic<int>>(0);
  auto failures = std::make_shared<std::atomic<int>>(0);
  auto outcomes = std::make_shared<std::vector<int>>(
      producer_count * attempts_per_producer, 0);
  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (int producer = 0; producer < producer_count; ++producer) {
    auto clone = sender;
    producers.emplace_back(
        [clone = std::move(clone), started, successes, failures, outcomes,
         producer]() mutable {
          started->fetch_add(1, std::memory_order_release);
          for (int index = 0; index < attempts_per_producer; ++index) {
            const int id =
                producer * attempts_per_producer + index;
            auto result = clone.send(id);
            if (result.has_value()) {
              (*outcomes)[static_cast<std::size_t>(id)] = 1;
              successes->fetch_add(1, std::memory_order_relaxed);
            } else {
              (*outcomes)[static_cast<std::size_t>(id)] = -1;
              failures->fetch_add(1, std::memory_order_relaxed);
            }
          }
        });
  }
  while (started->load(std::memory_order_acquire) != producer_count) {
    std::this_thread::yield();
  }
  std::this_thread::yield();
  receiver.close();
  for (auto &producer : producers) {
    producer.join();
  }

  int drained = 0;
  std::vector<bool> seen(
      producer_count * attempts_per_producer, false);
  bool values_exact = true;
  for (;;) {
    auto value = receiver.try_recv();
    if (!value.has_value()) {
      check(value.error() == TryRecvError::disconnected,
            "close/send 竞态 drain 未终止");
      break;
    }
    const int id = value.value();
    if (id < 0 ||
        id >= producer_count * attempts_per_producer ||
        (*outcomes)[static_cast<std::size_t>(id)] != 1 ||
        seen[static_cast<std::size_t>(id)]) {
      values_exact = false;
    } else {
      seen[static_cast<std::size_t>(id)] = true;
    }
    ++drained;
  }
  for (std::size_t id = 0; id < outcomes->size(); ++id) {
    if (((*outcomes)[id] == 1) != seen[id]) {
      values_exact = false;
    }
  }
  check(drained == successes->load(std::memory_order_relaxed) &&
            values_exact &&
            successes->load(std::memory_order_relaxed) +
                    failures->load(std::memory_order_relaxed) ==
                producer_count * attempts_per_producer,
        "close/send 线性化竞态出现半提交、丢失或重复");
}

void test_exception_drop_and_reentrancy() {
  {
    auto moves = std::make_shared<std::atomic<int>>(0);
    auto throw_on = std::make_shared<std::atomic<int>>(2);
    auto [sender, receiver] =
        cio::sync::mpsc::unbounded_channel<ThrowingMove>();
    ThrowingMove value{moves, throw_on, 9};
    bool threw = false;
    try {
      (void)sender.send(std::move(value));
    } catch (const std::runtime_error &) {
      threw = true;
    }
    check(threw && receiver.is_empty() && receiver.len() == 0,
          "send 用户移动异常后出现半提交");
    throw_on->store(0, std::memory_order_release);
    check(sender.send(ThrowingMove{moves, throw_on, 10}).has_value(),
          "send 异常后 channel 无法复用");
    auto received = receiver.try_recv();
    check(received.has_value() && received.value().value == 10,
          "send 异常后复用消息错误");
  }

  {
    auto moves = std::make_shared<std::atomic<int>>(0);
    auto throw_on = std::make_shared<std::atomic<int>>(0);
    auto [sender, receiver] =
        cio::sync::mpsc::unbounded_channel<ThrowingMove>();
    check(sender.send(ThrowingMove{moves, throw_on, 1}).has_value(),
          "try_recv 异常测试预填充失败");
    throw_on->store(
        moves->load(std::memory_order_acquire) + 1,
        std::memory_order_release);
    bool threw = false;
    try {
      (void)receiver.try_recv();
    } catch (const std::runtime_error &) {
      threw = true;
    }
    throw_on->store(0, std::memory_order_release);
    check(threw && receiver.is_empty() &&
              sender.send(ThrowingMove{moves, throw_on, 2}).has_value(),
          "try_recv 移动异常未消费终态或破坏 receiver");
    auto retry = receiver.try_recv();
    check(retry.has_value() && retry.value().value == 2,
          "try_recv 移动异常后 receiver 无法复用");
  }

  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    auto reentered = std::make_shared<std::atomic<bool>>(false);
    auto channel =
        cio::sync::mpsc::unbounded_channel<DropProbe>();
    auto weak = channel.first.downgrade();
    auto callback = std::make_shared<std::function<void()>>(
        [weak, reentered] {
          if (weak.weak_count() >= 1) {
            reentered->store(true, std::memory_order_release);
          }
        });
    check(channel.first
              .send(DropProbe{drops, 3, std::move(callback)})
              .has_value(),
          "析构重入测试入队失败");
    std::optional<UnboundedReceiver<DropProbe>> receiver{
        std::move(channel.second)};
    receiver.reset();
    check(drops->load(std::memory_order_relaxed) == 1 &&
              reentered->load(std::memory_order_acquire),
          "Receiver drop 在锁内析构、死锁或未恰好一次");
  }

  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    auto channel =
        cio::sync::mpsc::unbounded_channel<DropProbe>();
    std::optional<UnboundedReceiver<DropProbe>> receiver{
        std::move(channel.second)};
    receiver.reset();
    {
      auto rejected = channel.first.send(DropProbe{drops, 4});
      check(!rejected.has_value() &&
                rejected.error().value().value == 4 &&
                drops->load(std::memory_order_relaxed) == 0,
            "关闭 send 未返还 DropProbe 所有权");
    }
    check(drops->load(std::memory_order_relaxed) == 1,
          "SendError payload 未恰好析构一次");
  }
}

void test_runtime_shutdown_and_budget() {
  {
    auto [sender, receiver] =
        cio::sync::mpsc::unbounded_channel<int>();
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_recv(receiver.recv(), entered)),
            "shutdown 前未建立 pending unbounded recv");
    }
    check(sender.send(71).has_value(),
          "shutdown 后发送失败");
    Runtime continuation;
    auto value = continuation.block_on(receiver.recv());
    check(value == std::optional<int>{71},
          "shutdown 未清理 recv guard 或无法重试");
  }

  {
    auto [sender, receiver] =
        cio::sync::mpsc::unbounded_channel<int>();
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_closed(sender, entered)),
            "shutdown 前未建立 pending UnboundedSender::closed");
    }
    receiver.close();
    Runtime continuation;
    continuation.block_on(sender.closed());
    check(sender.is_closed(),
          "shutdown 后 closed waiter 无法重试");
  }

  Runtime runtime;
  check(runtime.block_on(ready_recv_budget_root()),
        "always-ready unbounded recv 未消耗 cooperative budget");
  check(runtime.block_on(notification_fresh_poll_budget_root()),
        "Notify 唤醒后的 unbounded recv fresh poll 未扣预算");
  check(runtime.block_on(closed_does_not_consume_budget_root()),
        "UnboundedSender::closed 错误消耗 cooperative budget");
  check(runtime.block_on(
            synchronous_send_does_not_consume_budget_root()),
        "UnboundedSender::send 错误消耗 cooperative budget");
}

} // namespace

int main() {
  try {
    test_basic_api_and_errors();
    test_closed_cancel_and_drop();
    test_counts_weak_and_identity();
    test_blocking_and_cross_thread();
    test_close_send_linearization_stress();
    test_exception_drop_and_reentrancy();
    test_runtime_shutdown_and_budget();
    std::cout << "unbounded mpsc tests passed\n";
    return 0;
  } catch (const std::exception &exception) {
    std::cerr << "unbounded mpsc tests failed: " << exception.what()
              << '\n';
    return 1;
  }
}
