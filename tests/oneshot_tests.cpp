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
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync/oneshot.hpp"

namespace {

using cio::Task;
using cio::runtime::Runtime;
using cio::sync::oneshot::Receiver;
using cio::sync::oneshot::Sender;
using cio::sync::oneshot::error::RecvError;
using cio::sync::oneshot::error::TryRecvError;

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
  static constexpr bool cio_sync = false;

  friend bool operator==(const VisibilityValue &,
                         const VisibilityValue &) = default;
};

struct DropProbe final {
  std::shared_ptr<std::atomic<int>> drops;
  int value{0};
  bool active{true};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;

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

struct TransferCounters final {
  std::atomic<int> constructions{0};
  std::atomic<int> destructions{0};
  std::atomic<int> live{0};
  std::atomic<int> move_attempts{0};
  std::atomic<int> throw_on_move_attempt{-1};
  std::function<void()> on_destroy;
};

struct ThrowingMoveValue final {
  std::shared_ptr<TransferCounters> counters;
  int value{0};

  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;

  ThrowingMoveValue(std::shared_ptr<TransferCounters> observed, int stored)
      : counters{std::move(observed)}, value{stored} {
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingMoveValue(const ThrowingMoveValue &) = delete;
  ThrowingMoveValue &operator=(const ThrowingMoveValue &) = delete;

  ThrowingMoveValue(ThrowingMoveValue &&other)
      : counters{other.counters}, value{other.value} {
    const int attempt =
        counters->move_attempts.fetch_add(1, std::memory_order_relaxed) + 1;
    if (counters->throw_on_move_attempt.load(std::memory_order_relaxed) ==
        attempt) {
      throw std::runtime_error{"预期的 oneshot 值移动异常"};
    }
    counters->constructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_add(1, std::memory_order_relaxed);
  }

  ThrowingMoveValue &operator=(ThrowingMoveValue &&) = delete;

  ~ThrowingMoveValue() {
    counters->destructions.fetch_add(1, std::memory_order_relaxed);
    counters->live.fetch_sub(1, std::memory_order_relaxed);
    if (counters->on_destroy) {
      counters->on_destroy();
    }
  }
};

struct ReentryObservation final {
  std::atomic<int> calls{0};
  std::atomic<int> successes{0};
  std::atomic<int> failures{0};
};

void throw_on_next_move(const std::shared_ptr<TransferCounters> &counters) {
  counters->throw_on_move_attempt.store(
      counters->move_attempts.load(std::memory_order_relaxed) + 1,
      std::memory_order_relaxed);
}

void disable_move_throw(const std::shared_ptr<TransferCounters> &counters) {
  counters->throw_on_move_attempt.store(-1, std::memory_order_relaxed);
}

void check_balanced_lifetime(
    const std::shared_ptr<TransferCounters> &counters,
    std::string_view message) {
  check(counters->live.load(std::memory_order_relaxed) == 0 &&
            counters->constructions.load(std::memory_order_relaxed) ==
                counters->destructions.load(std::memory_order_relaxed),
        message);
}

void install_receiver_reentry(
    const std::shared_ptr<TransferCounters> &counters,
    const std::shared_ptr<Receiver<ThrowingMoveValue>> &receiver,
    const std::shared_ptr<ReentryObservation> &observation) {
  const std::weak_ptr<Receiver<ThrowingMoveValue>> weak_receiver{receiver};
  counters->on_destroy = [weak_receiver, observation] {
    observation->calls.fetch_add(1, std::memory_order_relaxed);
    try {
      const auto locked = weak_receiver.lock();
      if (locked && locked->is_terminated() && locked->is_empty()) {
        observation->successes.fetch_add(1, std::memory_order_relaxed);
      } else {
        observation->failures.fetch_add(1, std::memory_order_relaxed);
      }
    } catch (...) {
      observation->failures.fetch_add(1, std::memory_order_relaxed);
    }
  };
}

template <typename T>
Task<cio::Result<T, RecvError>>
await_receive(typename Receiver<T>::Receive receive) {
  co_return co_await std::move(receive);
}

template <typename T>
Task<cio::Result<T, RecvError>>
marked_receive(typename Receiver<T>::Receive receive,
               std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_return co_await std::move(receive);
}

template <typename T>
Task<void> await_closed(typename Sender<T>::Closed closed) {
  co_await std::move(closed);
}

template <typename T>
Task<void> marked_closed(typename Sender<T>::Closed closed,
                         std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_await std::move(closed);
}

Task<bool> direct_receive_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  auto sent = std::move(sender).send(17);
  auto received = co_await receiver;
  co_return sent.has_value() && received.has_value() &&
      received.value() == 17 && receiver.is_terminated() && receiver.is_empty();
}

Task<bool> wait_before_send_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<VisibilityValue>();
  auto waiting =
      cio::task::spawn(await_receive<VisibilityValue>(receiver.receive()));
  co_await cio::task::yield_now();
  auto sent = std::move(sender).send(VisibilityValue{19, 23, 42});
  const auto joined = co_await waiting;
  if (!sent.has_value() || !joined.has_value() || !joined.value().has_value()) {
    co_return false;
  }
  const auto &value = joined.value().value();
  co_return value == VisibilityValue{19, 23, 42} &&
      value.checksum ==
          value.sequence + value.payload &&receiver.is_terminated();
}

Task<bool> sender_drop_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  std::optional<Sender<int>> owned_sender{std::move(sender)};
  owned_sender.reset();
  if (receiver.is_terminated() || !receiver.is_empty()) {
    co_return false;
  }
  auto received = co_await receiver;
  co_return !received.has_value() && received.error() == RecvError{} &&
      receiver.is_terminated();
}

Task<bool> closed_wait_and_retry_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();

  auto before_poll = cio::task::spawn(await_closed<int>(sender.closed()));
  before_poll.abort();
  const auto before_poll_join = co_await before_poll;
  if (before_poll_join.has_value() ||
      !before_poll_join.error().is_cancelled()) {
    co_return false;
  }

  auto pending = cio::task::spawn(await_closed<int>(sender.closed()));
  co_await cio::task::yield_now();
  pending.abort();
  const auto pending_join = co_await pending;
  if (pending_join.has_value() || !pending_join.error().is_cancelled()) {
    co_return false;
  }

  auto notified = cio::task::spawn(await_closed<int>(sender.closed()));
  co_await cio::task::yield_now();
  receiver.close();
  notified.abort();
  const auto notified_join = co_await notified;
  if (notified_join.has_value() || !notified_join.error().is_cancelled() ||
      !sender.is_closed()) {
    co_return false;
  }

  auto retry = cio::task::spawn(await_closed<int>(sender.poll_closed()));
  const auto retry_join = co_await retry;
  auto rejected = std::move(sender).send(31);
  auto closed = receiver.try_recv();
  co_return retry_join.has_value() && !rejected.has_value() &&
      rejected.error() == 31 && !closed.has_value() &&
      closed.error() == TryRecvError::closed && receiver.is_terminated();
}

Task<bool> receiver_drop_wakes_closed_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  std::optional<Receiver<int>> owned_receiver{std::move(receiver)};
  auto waiting = cio::task::spawn(await_closed<int>(sender.closed()));
  co_await cio::task::yield_now();
  owned_receiver.reset();
  const auto joined = co_await waiting;
  co_return joined.has_value() && sender.is_closed();
}

Task<bool> receiver_drop_completes_active_receive_root() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  auto waiting = cio::task::spawn(await_receive<int>(receiver.receive()));
  co_await cio::task::yield_now();
  std::optional<Receiver<int>> owned_receiver{std::move(receiver)};
  owned_receiver.reset();
  const auto joined = co_await waiting;
  co_return sender.is_closed() && joined.has_value() &&
      !joined.value().has_value() && joined.value().error() == RecvError{};
}

Task<bool> receive_cancellation_root() {
  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto before_poll = cio::task::spawn(await_receive<int>(receiver.receive()));
    before_poll.abort();
    const auto joined = co_await before_poll;
    if (joined.has_value() || !joined.error().is_cancelled()) {
      co_return false;
    }
    auto sent = std::move(sender).send(3);
    auto recovered = co_await receiver;
    if (!sent.has_value() || !recovered.has_value() || recovered.value() != 3) {
      co_return false;
    }
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto pending = cio::task::spawn(await_receive<int>(receiver.receive()));
    co_await cio::task::yield_now();
    pending.abort();
    const auto joined = co_await pending;
    if (joined.has_value() || !joined.error().is_cancelled()) {
      co_return false;
    }
    auto sent = std::move(sender).send(5);
    auto recovered = receiver.try_recv();
    if (!sent.has_value() || !recovered.has_value() || recovered.value() != 5) {
      co_return false;
    }
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto notified = cio::task::spawn(await_receive<int>(receiver.receive()));
    co_await cio::task::yield_now();
    auto sent = std::move(sender).send(7);
    notified.abort();
    const auto joined = co_await notified;
    auto recovered = receiver.try_recv();
    if (!sent.has_value() || joined.has_value() ||
        !joined.error().is_cancelled() || !recovered.has_value() ||
        recovered.value() != 7) {
      co_return false;
    }
  }

  co_return true;
}

Task<bool> no_lost_wake_root(int iterations) {
  for (int iteration = 0; iteration < iterations; ++iteration) {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto waiting = cio::task::spawn(await_receive<int>(receiver.receive()));
    if ((iteration % 2) == 0) {
      co_await cio::task::yield_now();
    }
    auto sent = std::move(sender).send(iteration);
    if ((iteration % 3) == 0) {
      co_await cio::task::yield_now();
    }
    const auto joined = co_await waiting;
    if (!sent.has_value() || !joined.has_value() ||
        !joined.value().has_value() || joined.value().value() != iteration) {
      co_return false;
    }
  }
  co_return true;
}

Task<void> mark_peer_progress(std::shared_ptr<std::atomic<bool>> progressed) {
  progressed->store(true, std::memory_order_release);
  co_return;
}

Task<bool> ready_receive_cooperative_budget_root() {
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  std::size_t iterations = 0;
  while (!progressed->load(std::memory_order_acquire) && iterations < 512) {
    auto [sender, receiver] = cio::sync::oneshot::channel<std::size_t>();
    auto sent = std::move(sender).send(iterations);
    auto received = co_await receiver;
    if (!sent.has_value() || !received.has_value() ||
        received.value() != iterations) {
      co_return false;
    }
    ++iterations;
  }
  const bool yielded_before_limit =
      progressed->load(std::memory_order_acquire) && iterations < 512;
  const auto joined = co_await peer;
  co_return yielded_before_limit && joined.has_value();
}

Task<bool> ready_closed_cooperative_budget_root() {
  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  std::size_t iterations = 0;
  while (!progressed->load(std::memory_order_acquire) && iterations < 512) {
    auto [sender, receiver] = cio::sync::oneshot::channel<std::size_t>();
    std::optional<Receiver<std::size_t>> owned_receiver{std::move(receiver)};
    owned_receiver.reset();
    co_await sender.closed();
    ++iterations;
  }
  const bool yielded_before_limit =
      progressed->load(std::memory_order_acquire) && iterations < 512;
  const auto joined = co_await peer;
  co_return yielded_before_limit && joined.has_value();
}

Task<bool> fresh_poll_budget_debit_root() {
  constexpr std::size_t initial_progress = 129;
  for (std::size_t iteration = 0; iteration < initial_progress; ++iteration) {
    auto [sender, receiver] =
        cio::sync::oneshot::channel<std::size_t>();
    std::optional<Receiver<std::size_t>> owned_receiver{
        std::move(receiver)};
    owned_receiver.reset();
    co_await sender.closed();
  }

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  if (progressed->load(std::memory_order_acquire)) {
    co_return false;
  }

  std::size_t completed = 0;
  while (!progressed->load(std::memory_order_acquire) && completed < 256) {
    auto [sender, receiver] =
        cio::sync::oneshot::channel<std::size_t>();
    std::optional<Receiver<std::size_t>> owned_receiver{
        std::move(receiver)};
    owned_receiver.reset();
    co_await sender.closed();
    ++completed;
  }

  const auto joined = co_await peer;
  co_return joined.has_value() &&
      progressed->load(std::memory_order_acquire) && completed == 128;
}

Task<void> send_ready_notification(Sender<std::size_t> sender) {
  auto sent = std::move(sender).send(1);
  if (!sent.has_value()) {
    throw std::runtime_error{"oneshot 通知预算测试发送失败"};
  }
  co_return;
}

Task<bool> notification_poll_budget_debit_root() {
  auto [sender, receiver] =
      cio::sync::oneshot::channel<std::size_t>();
  auto producer =
      cio::task::spawn(send_ready_notification(std::move(sender)));
  auto received = co_await receiver;
  if (!received.has_value() || received.value() != 1) {
    co_return false;
  }

  const auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  if (progressed->load(std::memory_order_acquire)) {
    co_return false;
  }

  std::size_t completed = 0;
  while (!progressed->load(std::memory_order_acquire) && completed < 256) {
    auto [closed_sender, closed_receiver] =
        cio::sync::oneshot::channel<std::size_t>();
    std::optional<Receiver<std::size_t>> owned_receiver{
        std::move(closed_receiver)};
    owned_receiver.reset();
    co_await closed_sender.closed();
    ++completed;
  }

  const auto producer_joined = co_await producer;
  const auto peer_joined = co_await peer;
  co_return producer_joined.has_value() && peer_joined.has_value() &&
      progressed->load(std::memory_order_acquire) && completed == 128;
}

Task<bool> blocking_recv_rejected_root(Receiver<int> receiver) {
  try {
    (void)std::move(receiver).blocking_recv();
  } catch (const std::logic_error &) {
    co_return true;
  }
  co_return false;
}

Task<bool> launch_shutdown_receive(Receiver<int>::Receive receive,
                                   std::shared_ptr<std::atomic<bool>> entered) {
  auto detached =
      cio::task::spawn(marked_receive<int>(std::move(receive), entered));
  (void)detached;
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return true;
}

Task<bool> launch_shutdown_closed(Sender<int>::Closed closed,
                                  std::shared_ptr<std::atomic<bool>> entered) {
  auto detached =
      cio::task::spawn(marked_closed<int>(std::move(closed), entered));
  (void)detached;
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return true;
}

void test_construction_errors_traits_and_move() {
  auto [sender, receiver] = cio::sync::oneshot::channel<int>();
  check(!sender.is_closed() && receiver.is_empty() && !receiver.is_terminated(),
        "oneshot 初始状态错误");

  auto empty = receiver.try_recv();
  check(!empty.has_value() && empty.error() == TryRecvError::empty &&
            cio::sync::oneshot::error::message(empty.error()) ==
                "channel empty" &&
            cio::sync::oneshot::error::debug_string(empty.error()) == "Empty" &&
            !receiver.is_terminated(),
        "try_recv Empty 状态或格式错误");

  std::ostringstream recv_display;
  recv_display << RecvError{};
  std::ostringstream empty_display;
  empty_display << TryRecvError::empty;
  check(recv_display.str() == "channel closed" &&
            empty_display.str() == "channel empty" &&
            RecvError{}.debug_string() == "RecvError(())",
        "RecvError/TryRecvError Display 或 Debug 错误");

  std::ostringstream sender_debug;
  sender_debug << sender;
  std::ostringstream receiver_debug;
  receiver_debug << receiver;
  check(sender_debug.str() == "Sender { closed: 0 }" &&
            receiver_debug.str() == "Receiver { terminated: 0, empty: 1 }",
        "Sender/Receiver Debug 映射错误");

  Sender<int> moved_sender{std::move(sender)};
  Receiver<int> moved_receiver{std::move(receiver)};
  check(!moved_sender.is_closed() && moved_receiver.is_empty(),
        "move 目标句柄状态错误");
  bool sender_source_rejected = false;
  bool receiver_source_rejected = false;
  try {
    (void)sender.is_closed();
  } catch (const std::logic_error &) {
    sender_source_rejected = true;
  }
  try {
    (void)receiver.is_empty();
  } catch (const std::logic_error &) {
    receiver_source_rejected = true;
  }
  check(sender_source_rejected && receiver_source_rejected,
        "move 后源句柄未拒绝使用");
}

void test_send_receive_close_and_drop() {
  Runtime runtime;
  check(runtime.block_on(direct_receive_root()),
        "send-before-receive 或 Receiver operator co_await 错误");
  check(runtime.block_on(wait_before_send_root()),
        "wait-before-send、跨暂停发布或异步接收错误");
  check(runtime.block_on(sender_drop_root()),
        "Sender drop 未使 Receiver 收到 RecvError");

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto sent = std::move(sender).send(41);
    receiver.close();
    auto received = receiver.try_recv();
    check(sent.has_value() && received.has_value() && received.value() == 41 &&
              receiver.is_terminated(),
          "send 后 close 未保留已发送值");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    receiver.close();
    check(sender.is_closed(), "Receiver close 未同步发布给 Sender");
    auto rejected = std::move(sender).send(43);
    check(!rejected.has_value() && rejected.error() == 43,
          "Receiver close 后 send 未返还值");
    auto closed = receiver.try_recv();
    check(!closed.has_value() && closed.error() == TryRecvError::closed &&
              receiver.is_terminated(),
          "close-before-send 的 Receiver 终态错误");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<std::string>();
    std::optional<Receiver<std::string>> owned_receiver{std::move(receiver)};
    owned_receiver.reset();
    check(sender.is_closed(), "Receiver drop 未关闭 Sender");
    auto rejected = std::move(sender).send("retained");
    check(!rejected.has_value() && rejected.error() == "retained",
          "Receiver drop 后 send 未完整返还 move-only ownership");
  }
}

void test_closed_wait_and_exclusive_operations() {
  Runtime runtime;
  check(runtime.block_on(closed_wait_and_retry_root()),
        "Sender::closed 取消重试、显式 close 或 poll_closed 错误");
  check(runtime.block_on(receiver_drop_wakes_closed_root()),
        "Receiver drop 未唤醒 Sender::closed");
  check(runtime.block_on(receiver_drop_completes_active_receive_root()),
        "Receiver drop 未让 active receive 稳定观察 RecvError");

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    {
      auto operation = receiver.receive();
      bool second_receive_rejected = false;
      bool try_recv_rejected = false;
      bool close_rejected = false;
      bool blocking_recv_rejected = false;
      try {
        (void)receiver.receive();
      } catch (const std::logic_error &) {
        second_receive_rejected = true;
      }
      try {
        (void)receiver.try_recv();
      } catch (const std::logic_error &) {
        try_recv_rejected = true;
      }
      try {
        receiver.close();
      } catch (const std::logic_error &) {
        close_rejected = true;
      }
      try {
        (void)std::move(receiver).blocking_recv();
      } catch (const std::logic_error &) {
        blocking_recv_rejected = true;
      }

      auto awaiter = operation.operator co_await();
      bool repeated_await_rejected = false;
      try {
        (void)operation.operator co_await();
      } catch (const std::logic_error &) {
        repeated_await_rejected = true;
      }
      check(second_receive_rejected && try_recv_rejected && close_rejected &&
                blocking_recv_rejected && repeated_await_rejected,
            "Receiver 未拒绝并发 operation 或重复 await");
    }
    auto sent = std::move(sender).send(53);
    auto received = receiver.try_recv();
    check(sent.has_value() && received.has_value() && received.value() == 53,
          "receive operation 取消后未释放独占状态");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    {
      auto operation = sender.closed();
      bool second_closed_rejected = false;
      bool send_rejected = false;
      try {
        (void)sender.closed();
      } catch (const std::logic_error &) {
        second_closed_rejected = true;
      }
      try {
        (void)std::move(sender).send(59);
      } catch (const std::logic_error &) {
        send_rejected = true;
      }

      auto awaiter = operation.operator co_await();
      bool repeated_await_rejected = false;
      try {
        (void)operation.operator co_await();
      } catch (const std::logic_error &) {
        repeated_await_rejected = true;
      }
      check(second_closed_rejected && send_rejected && repeated_await_rejected,
            "Sender 未拒绝并发 closed、send 或重复 await");
    }
    auto sent = std::move(sender).send(61);
    auto received = receiver.try_recv();
    check(sent.has_value() && received.has_value() && received.value() == 61,
          "closed operation 取消后未释放 Sender 独占状态");
  }
}

void test_receive_cancellation_and_wake_races() {
  Runtime runtime;
  check(runtime.block_on(receive_cancellation_root()),
        "receive 在 poll 前、pending 或已通知状态取消不安全");
  check(runtime.block_on(no_lost_wake_root(500)),
        "oneshot send/receive 竞态丢失唤醒");
}

void test_cooperative_budget_fairness() {
  {
    Runtime runtime;
    check(runtime.block_on(ready_receive_cooperative_budget_root()),
          "always-ready receive 未在 cooperative budget 耗尽后让出 worker");
  }
  {
    Runtime runtime;
    check(runtime.block_on(ready_closed_cooperative_budget_root()),
          "always-ready Sender::closed 未在 cooperative budget 耗尽后让出 worker");
  }
  {
    Runtime runtime;
    check(runtime.block_on(fresh_poll_budget_debit_root()),
          "oneshot 新 poll 未扣除已完成操作的 cooperative budget");
  }
  {
    Runtime runtime;
    check(runtime.block_on(notification_poll_budget_debit_root()),
          "oneshot 通知唤醒后的新 poll 未扣 cooperative budget");
  }
}

void test_blocking_recv_and_cross_thread_visibility() {
  {
    auto [sender, receiver] = cio::sync::oneshot::channel<VisibilityValue>();
    const auto result = std::make_shared<
        std::optional<cio::Result<VisibilityValue, RecvError>>>();
    std::jthread receiving{[receiver = std::move(receiver), result]() mutable {
      result->emplace(std::move(receiver).blocking_recv());
    }};
    auto sent = std::move(sender).send(VisibilityValue{71, 29, 100});
    receiving.join();
    check(sent.has_value() && result->has_value() &&
              result->value().has_value() &&
              result->value().value() == VisibilityValue{71, 29, 100},
          "blocking_recv 跨线程等待或发布可见性错误");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    auto sent = std::move(sender).send(83);
    auto received = std::move(receiver).blocking_recv();
    bool consumed_receiver_rejected = false;
    try {
      (void)receiver.is_terminated();
    } catch (const std::logic_error &) {
      consumed_receiver_rejected = true;
    }
    check(sent.has_value() && received.has_value() && received.value() == 83 &&
              consumed_receiver_rejected,
          "blocking_recv 未消费 Receiver");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    const auto result =
        std::make_shared<std::optional<cio::Result<int, RecvError>>>();
    std::jthread receiving{[receiver = std::move(receiver), result]() mutable {
      result->emplace(std::move(receiver).blocking_recv());
    }};
    std::optional<Sender<int>> owned_sender{std::move(sender)};
    owned_sender.reset();
    receiving.join();
    check(result->has_value() && !result->value().has_value(),
          "blocking_recv 未被 Sender drop 唤醒");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    Runtime runtime;
    check(runtime.block_on(blocking_recv_rejected_root(std::move(receiver))),
          "blocking_recv 未拒绝 CIO worker 上下文");
    check(sender.is_closed(),
          "worker blocking_recv 拒绝后的 Receiver 生命周期错误");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<VisibilityValue>();
    const auto entered = std::make_shared<std::atomic<bool>>(false);
    std::jthread sending{[sender = std::move(sender), entered]() mutable {
      while (!entered->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      auto result = std::move(sender).send(VisibilityValue{101, 22, 123});
      if (!result.has_value()) {
        std::terminate();
      }
    }};

    auto builder = cio::runtime::Builder::new_multi_thread();
    auto runtime = builder.worker_threads(4).build();
    auto received = runtime.block_on(cio::task::assume_portable(
        marked_receive<VisibilityValue>(receiver.receive(), entered)));
    sending.join();
    check(received.has_value() &&
              received.value() == VisibilityValue{101, 22, 123} &&
              received.value().checksum ==
                  received.value().sequence + received.value().payload,
          "multi-thread runtime 与外部线程之间发布不可见");
  }
}

void test_send_close_race() {
  constexpr int iterations = 300;
  for (int iteration = 0; iteration < iterations; ++iteration) {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    const auto sent = std::make_shared<std::optional<cio::Result<void, int>>>();
    std::jthread sending{
        [sender = std::move(sender), sent, iteration]() mutable {
          sent->emplace(std::move(sender).send(iteration));
        }};
    receiver.close();
    sending.join();
    check(sent->has_value(), "send/close 竞态未产生发送终态");
    auto received = receiver.try_recv();
    if (sent->value().has_value()) {
      check(received.has_value() && received.value() == iteration,
            "send 赢得 close 竞态后消息丢失");
    } else {
      check(sent->value().error() == iteration && !received.has_value() &&
                received.error() == TryRecvError::closed,
            "close 赢得 send 竞态后值未返还或终态错误");
    }
  }
}

void test_shutdown_restart_and_operation_reuse() {
  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    const auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                launch_shutdown_receive(receiver.receive(), entered)),
            "runtime 关闭前未建立 receive waiter");
    }
    auto empty = receiver.try_recv();
    check(!empty.has_value() && empty.error() == TryRecvError::empty,
          "runtime shutdown 未取消 receive operation");
    auto sent = std::move(sender).send(73);
    Runtime continuation;
    auto received =
        continuation.block_on(await_receive<int>(receiver.receive()));
    check(sent.has_value() && received.has_value() && received.value() == 73,
          "runtime shutdown 后 Receiver 无法在新 runtime 重试");
  }

  {
    auto [sender, receiver] = cio::sync::oneshot::channel<int>();
    const auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                launch_shutdown_closed(sender.closed(), entered)),
            "runtime 关闭前未建立 Sender::closed waiter");
    }
    auto sent = std::move(sender).send(79);
    auto received = receiver.try_recv();
    check(sent.has_value() && received.has_value() && received.value() == 79,
          "runtime shutdown 后 Sender::closed 未释放独占状态");
  }

  for (int iteration = 0; iteration < 20; ++iteration) {
    Runtime runtime;
    check(runtime.block_on(no_lost_wake_root(30)),
          "oneshot runtime 反复启停竞态错误");
  }
}

void test_drop_exactly_once() {
  {
    const auto drops = std::make_shared<std::atomic<int>>(0);
    {
      auto [sender, receiver] = cio::sync::oneshot::channel<DropProbe>();
      auto sent = std::move(sender).send(DropProbe{drops, 1});
      check(sent.has_value(), "DropProbe 成功发送失败");
    }
    check(drops->load(std::memory_order_relaxed) == 1,
          "Receiver drop 未恰好析构已发送值一次");
  }

  {
    const auto drops = std::make_shared<std::atomic<int>>(0);
    {
      auto [sender, receiver] = cio::sync::oneshot::channel<DropProbe>();
      std::optional<Receiver<DropProbe>> owned_receiver{std::move(receiver)};
      owned_receiver.reset();
      auto rejected = std::move(sender).send(DropProbe{drops, 2});
      check(!rejected.has_value() && rejected.error().value == 2 &&
                drops->load(std::memory_order_relaxed) == 0,
            "失败发送未返还唯一值所有权");
    }
    check(drops->load(std::memory_order_relaxed) == 1,
          "失败发送返还值未恰好析构一次");
  }

  {
    const auto drops = std::make_shared<std::atomic<int>>(0);
    {
      auto [sender, receiver] = cio::sync::oneshot::channel<DropProbe>();
      auto sent = std::move(sender).send(DropProbe{drops, 3});
      auto received = receiver.try_recv();
      check(sent.has_value() && received.has_value() &&
                received.value().value == 3 &&
                drops->load(std::memory_order_relaxed) == 0,
            "成功 receive 未转移唯一值所有权");
    }
    check(drops->load(std::memory_order_relaxed) == 1,
          "成功接收值未恰好析构一次");
  }
}

void test_user_value_exception_reentrancy_and_conservation() {
  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      ThrowingMoveValue source{counters, 101};
      counters->throw_on_move_attempt.store(2, std::memory_order_relaxed);
      bool candidate_move_threw = false;
      try {
        (void)std::move(sender).send(std::move(source));
      } catch (const std::runtime_error &) {
        candidate_move_threw = true;
      }
      disable_move_throw(counters);

      const auto empty = receiver.try_recv();
      check(candidate_move_threw &&
                counters->move_attempts.load(std::memory_order_relaxed) == 2 &&
                !sender.is_closed() && !empty.has_value() &&
                empty.error() == TryRecvError::empty &&
                !receiver.is_terminated(),
            "send 候选值移动异常未保持 Sender/channel 可重试");

      const auto sent =
          std::move(sender).send(ThrowingMoveValue{counters, 102});
      const auto received = receiver.try_recv();
      check(sent.has_value() && received.has_value() &&
                received.value().value == 102 && receiver.is_terminated(),
            "send 候选值移动异常后的重试未正常交付");
    }
    check_balanced_lifetime(counters,
                            "send 候选移动异常造成值泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    const auto observation = std::make_shared<ReentryObservation>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      const auto receiver_owner =
          std::make_shared<Receiver<ThrowingMoveValue>>(std::move(receiver));
      const auto sent =
          std::move(sender).send(ThrowingMoveValue{counters, 201});
      check(sent.has_value(), "try_recv 移动异常测试发送失败");

      install_receiver_reentry(counters, receiver_owner, observation);
      throw_on_next_move(counters);
      bool receive_move_threw = false;
      try {
        (void)receiver_owner->try_recv();
      } catch (const std::runtime_error &) {
        receive_move_threw = true;
      }
      disable_move_throw(counters);

      const auto repeated = receiver_owner->try_recv();
      check(receive_move_threw && receiver_owner->is_terminated() &&
                receiver_owner->is_empty() && !repeated.has_value() &&
                repeated.error() == TryRecvError::closed &&
                observation->calls.load(std::memory_order_relaxed) >= 1 &&
                observation->successes.load(std::memory_order_relaxed) ==
                    observation->calls.load(std::memory_order_relaxed) &&
                observation->failures.load(std::memory_order_relaxed) == 0,
            "try_recv 最终移动异常未保持终态、发生重复交付或析构锁内重入");
      counters->on_destroy = {};
    }
    check_balanced_lifetime(counters,
                            "try_recv 最终移动异常造成值泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    const auto observation = std::make_shared<ReentryObservation>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      const auto receiver_owner =
          std::make_shared<Receiver<ThrowingMoveValue>>(std::move(receiver));
      const auto sent =
          std::move(sender).send(ThrowingMoveValue{counters, 301});
      check(sent.has_value(), "异步 receive 移动异常测试发送失败");

      install_receiver_reentry(counters, receiver_owner, observation);
      throw_on_next_move(counters);
      bool receive_move_threw = false;
      try {
        Runtime runtime;
        (void)runtime.block_on(
            await_receive<ThrowingMoveValue>(receiver_owner->receive()));
      } catch (const std::runtime_error &) {
        receive_move_threw = true;
      }
      disable_move_throw(counters);

      const auto repeated = receiver_owner->try_recv();
      check(receive_move_threw && receiver_owner->is_terminated() &&
                receiver_owner->is_empty() && !repeated.has_value() &&
                repeated.error() == TryRecvError::closed &&
                observation->calls.load(std::memory_order_relaxed) >= 1 &&
                observation->successes.load(std::memory_order_relaxed) ==
                    observation->calls.load(std::memory_order_relaxed) &&
                observation->failures.load(std::memory_order_relaxed) == 0,
            "异步 receive 最终移动异常未保持终态、发生重复交付或析构锁内重入");
      counters->on_destroy = {};
    }
    check_balanced_lifetime(counters,
                            "异步 receive 移动异常造成值泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    const auto observation = std::make_shared<ReentryObservation>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      const auto receiver_owner =
          std::make_shared<Receiver<ThrowingMoveValue>>(std::move(receiver));
      const auto sent =
          std::move(sender).send(ThrowingMoveValue{counters, 401});
      check(sent.has_value(), "析构重入测试发送失败");

      install_receiver_reentry(counters, receiver_owner, observation);
      {
        const auto received = receiver_owner->try_recv();
        check(received.has_value() && received.value().value == 401,
              "析构重入测试接收值错误");
      }
      check(observation->calls.load(std::memory_order_relaxed) >= 1 &&
                observation->successes.load(std::memory_order_relaxed) ==
                    observation->calls.load(std::memory_order_relaxed) &&
                observation->failures.load(std::memory_order_relaxed) == 0,
            "接收值析构重入同一 Receiver 状态查询失败或发生死锁");
      counters->on_destroy = {};
    }
    check_balanced_lifetime(counters,
                            "接收值析构重入造成值泄漏或重复析构");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      receiver.close();
      const auto rejected =
          std::move(sender).send(ThrowingMoveValue{counters, 501});
      check(!rejected.has_value() && rejected.error().value == 501,
            "move-only 值在失败 send 中未完整返还");
    }
    check_balanced_lifetime(counters,
                            "失败 send 的 move-only 值计数不守恒");
  }

  {
    const auto counters = std::make_shared<TransferCounters>();
    {
      auto [sender, receiver] =
          cio::sync::oneshot::channel<ThrowingMoveValue>();
      const auto sent =
          std::move(sender).send(ThrowingMoveValue{counters, 601});
      check(sent.has_value(), "Receiver drop 值计数测试发送失败");
      std::optional<Receiver<ThrowingMoveValue>> owned_receiver{
          std::move(receiver)};
      owned_receiver.reset();
    }
    check_balanced_lifetime(counters,
                            "Receiver drop 的 move-only 值计数不守恒");
  }
}

struct SendOnly final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
};

struct NotSend final {
  static constexpr bool cio_send = false;
  static constexpr bool cio_sync = false;
};

static_assert(!std::copy_constructible<Sender<int>>);
static_assert(!std::copy_constructible<Receiver<int>>);
static_assert(std::move_constructible<Sender<int>>);
static_assert(std::move_constructible<Receiver<int>>);
static_assert(!std::copy_constructible<ThrowingMoveValue>);
static_assert(std::move_constructible<ThrowingMoveValue>);
static_assert(cio::Send<Sender<SendOnly>>);
static_assert(cio::Sync<Sender<SendOnly>>);
static_assert(cio::Send<Receiver<SendOnly>>);
static_assert(cio::Sync<Receiver<SendOnly>>);
static_assert(!cio::Send<Sender<NotSend>>);
static_assert(!cio::Sync<Sender<NotSend>>);
static_assert(!cio::Send<Receiver<NotSend>>);
static_assert(!cio::Sync<Receiver<NotSend>>);
static_assert(
    !cio::detail::OneshotOwnedValue<std::reference_wrapper<SendOnly>>);
static_assert(cio::Send<Receiver<SendOnly>::Receive>);
static_assert(!cio::Sync<Receiver<SendOnly>::Receive>);
static_assert(cio::Send<Receiver<SendOnly>::Receive::Awaiter>);
static_assert(!cio::Sync<Receiver<SendOnly>::Receive::Awaiter>);
static_assert(cio::Send<Sender<SendOnly>::Closed>);
static_assert(!cio::Sync<Sender<SendOnly>::Closed>);
static_assert(cio::Send<Sender<SendOnly>::Closed::Awaiter>);
static_assert(!cio::Sync<Sender<SendOnly>::Closed::Awaiter>);
static_assert(cio::Send<RecvError>);
static_assert(cio::Sync<RecvError>);
static_assert(cio::Send<TryRecvError>);
static_assert(cio::Sync<TryRecvError>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"oneshot construction error traits move",
       test_construction_errors_traits_and_move},
      {"oneshot send receive close drop", test_send_receive_close_and_drop},
      {"oneshot closed exclusive operations",
       test_closed_wait_and_exclusive_operations},
      {"oneshot cancellation wake races",
       test_receive_cancellation_and_wake_races},
      {"oneshot cooperative budget fairness",
       test_cooperative_budget_fairness},
      {"oneshot blocking cross-thread visibility",
       test_blocking_recv_and_cross_thread_visibility},
      {"oneshot send close race", test_send_close_race},
      {"oneshot shutdown restart", test_shutdown_restart_and_operation_reuse},
      {"oneshot drop exactly once", test_drop_exactly_once},
      {"oneshot user value exceptions reentrancy conservation",
       test_user_value_exception_reentrancy_and_conservation},
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

  std::cout << "oneshot 全部通过：" << passed << " 项\n";
  return 0;
}
