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
#include "cio/task/portable.hpp"
#include "cio/task/spawn.hpp"
#include "cio/task/task.hpp"
#include "cio/task/yield_now.hpp"

namespace {

using namespace std::chrono_literals;

using cio::Task;
using cio::runtime::Runtime;
using cio::sync::mpsc::OwnedPermit;
using cio::sync::mpsc::Permit;
using cio::sync::mpsc::Receiver;
using cio::sync::mpsc::Sender;
using cio::sync::mpsc::WeakSender;
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

struct SendOnly final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
};

struct NotSend final {
  static constexpr bool cio_send = false;
  static constexpr bool cio_sync = false;
};

struct MoveObserved final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<std::atomic<int>> moves;
  int value{0};

  MoveObserved(std::shared_ptr<std::atomic<int>> moves, int value)
      : moves{std::move(moves)}, value{value} {}

  MoveObserved(const MoveObserved &) = delete;
  MoveObserved &operator=(const MoveObserved &) = delete;

  MoveObserved(MoveObserved &&other) noexcept
      : moves{other.moves}, value{other.value} {
    moves->fetch_add(1, std::memory_order_relaxed);
  }

  MoveObserved &operator=(MoveObserved &&) = delete;
};

struct ThrowingMove final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<std::atomic<bool>> throw_now;
  int value{0};

  ThrowingMove(std::shared_ptr<std::atomic<bool>> throw_now, int value)
      : throw_now{std::move(throw_now)}, value{value} {}

  ThrowingMove(const ThrowingMove &) = delete;
  ThrowingMove &operator=(const ThrowingMove &) = delete;

  ThrowingMove(ThrowingMove &&other)
      : throw_now{other.throw_now}, value{other.value} {
    if (throw_now->load(std::memory_order_acquire)) {
      throw std::runtime_error{"预期的 mpsc 用户移动异常"};
    }
  }

  ThrowingMove &operator=(ThrowingMove &&) = delete;
};

struct BlockingThrowingMove final {
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  std::shared_ptr<std::atomic<bool>> entered;
  std::shared_ptr<std::atomic<bool>> proceed;
  std::shared_ptr<std::atomic<int>> moves;

  BlockingThrowingMove(std::shared_ptr<std::atomic<bool>> entered,
                       std::shared_ptr<std::atomic<bool>> proceed,
                       std::shared_ptr<std::atomic<int>> moves)
      : entered{std::move(entered)}, proceed{std::move(proceed)},
        moves{std::move(moves)} {}

  BlockingThrowingMove(const BlockingThrowingMove &) = delete;
  BlockingThrowingMove &operator=(const BlockingThrowingMove &) = delete;

  BlockingThrowingMove(BlockingThrowingMove &&other)
      : entered{other.entered}, proceed{other.proceed},
        moves{other.moves} {
    const auto attempt =
        moves->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (attempt == 2) {
      entered->store(true, std::memory_order_release);
      while (!proceed->load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      throw std::runtime_error{"预期的并发 mpsc 用户移动异常"};
    }
  }

  BlockingThrowingMove &operator=(BlockingThrowingMove &&) = delete;
};

Task<bool> fifo_backpressure_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  const auto first = co_await sender.send(1);
  if (!first.has_value() || sender.capacity() != 0) {
    co_return false;
  }

  auto second = cio::task::spawn(sender.send(2));
  co_await cio::task::yield_now();
  const bool blocked = !second.is_finished();

  auto first_received = co_await receiver.recv();
  const auto second_sent = co_await second;
  auto second_received = co_await receiver.recv();
  co_return blocked && first_received == std::optional<int>{1} &&
      second_sent.has_value() && second_sent.value().has_value() &&
      second_received == std::optional<int>{2} && sender.capacity() == 1;
}

Task<bool>
ordered_send(Sender<int> sender, int value,
             std::shared_ptr<std::vector<int>> completion_order) {
  auto result = co_await sender.send(value);
  if (!result.has_value()) {
    co_return false;
  }
  completion_order->push_back(value);
  co_return true;
}

Task<bool> fifo_fairness_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(0).has_value()) {
    co_return false;
  }
  auto completion_order = std::make_shared<std::vector<int>>();

  auto one =
      cio::task::spawn(ordered_send(sender, 1, completion_order));
  co_await cio::task::yield_now();
  auto two =
      cio::task::spawn(ordered_send(sender, 2, completion_order));
  co_await cio::task::yield_now();
  auto three =
      cio::task::spawn(ordered_send(sender, 3, completion_order));
  co_await cio::task::yield_now();

  std::vector<int> received;
  for (int index = 0; index < 4; ++index) {
    auto value = co_await receiver.recv();
    if (!value) {
      co_return false;
    }
    received.push_back(*value);
    co_await cio::task::yield_now();
  }

  const auto one_result = co_await one;
  const auto two_result = co_await two;
  const auto three_result = co_await three;
  co_return one_result.has_value() && one_result.value() &&
      two_result.has_value() && two_result.value() &&
      three_result.has_value() && three_result.value() &&
      received == std::vector<int>({0, 1, 2, 3}) &&
      *completion_order == std::vector<int>({1, 2, 3});
}

Task<bool> send_cancellation_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }

  auto pending = cio::task::spawn(sender.send(2));
  co_await cio::task::yield_now();
  if (pending.is_finished()) {
    co_return false;
  }
  pending.abort();
  const auto cancelled = co_await pending;
  auto first = co_await receiver.recv();
  auto empty = receiver.try_recv();
  const auto retry = sender.try_send(3);
  auto third = co_await receiver.recv();
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      first == std::optional<int>{1} && !empty.has_value() &&
      empty.error() == TryRecvError::empty && retry.has_value() &&
      third == std::optional<int>{3} && sender.capacity() == 1;
}

Task<bool> reserve_cancellation_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }

  auto pending = cio::task::spawn(sender.reserve());
  co_await cio::task::yield_now();
  if (pending.is_finished()) {
    co_return false;
  }
  pending.abort();
  const auto cancelled = co_await pending;
  auto first = co_await receiver.recv();
  const auto retry = sender.try_send(2);
  auto second = co_await receiver.recv();
  co_return !cancelled.has_value() && cancelled.error().is_cancelled() &&
      first == std::optional<int>{1} && retry.has_value() &&
      second == std::optional<int>{2} && sender.capacity() == 1;
}

Task<bool> permit_capacity_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value() || sender.capacity() != 1) {
    co_return false;
  }

  const auto immediate = sender.try_send(7);
  if (!immediate.has_value() || sender.capacity() != 0) {
    co_return false;
  }

  {
    std::optional<Permit<int>> permit{
        std::move(reserved).value()};
    permit.reset();
  }
  if (sender.capacity() != 1) {
    co_return false;
  }
  auto value = co_await receiver.recv();
  co_return value == std::optional<int>{7} && sender.capacity() == 2;
}

Task<bool> close_drain_outstanding_permit_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  auto permit = std::optional<Permit<int>>{
      std::move(reserved).value()};

  receiver.close();
  const auto rejected = sender.try_send(3);
  auto first = receiver.try_recv();
  auto pending = receiver.try_recv();
  if (!sender.is_closed() || !receiver.is_closed() ||
      rejected.has_value() || !rejected.error().is_closed() ||
      !first.has_value() || first.value() != 1 || pending.has_value() ||
      pending.error() != TryRecvError::empty) {
    co_return false;
  }

  std::move(*permit).send(2);
  permit.reset();
  auto second = co_await receiver.recv();
  auto terminal = co_await receiver.recv();
  co_return second == std::optional<int>{2} && !terminal.has_value() &&
      sender.capacity() == 2;
}

Task<bool> close_waits_for_dropped_permit_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  auto permit = std::optional<Permit<int>>{
      std::move(reserved).value()};
  receiver.close();

  auto waiting = cio::task::spawn(receiver.recv());
  co_await cio::task::yield_now();
  const bool remained_pending = !waiting.is_finished();
  permit.reset();
  const auto terminal = co_await waiting;
  co_return remained_pending && terminal.has_value() &&
      !terminal.value().has_value() && sender.capacity() == 1;
}

Task<bool> weak_permit_liveness_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto weak = sender.downgrade();
  auto reserved = co_await sender.reserve();
  if (!reserved.has_value()) {
    co_return false;
  }
  auto permit = std::optional<Permit<int>>{
      std::move(reserved).value()};

  {
    std::optional<Sender<int>> owner{std::move(sender)};
    owner.reset();
  }
  auto upgraded = weak.upgrade();
  const bool alive_with_permit =
      upgraded.has_value() && weak.strong_count() == 2;
  upgraded.reset();
  permit.reset();
  const bool dead_after_permit =
      weak.strong_count() == 0 && !weak.upgrade().has_value();
  auto terminal = co_await receiver.recv();
  co_return alive_with_permit && dead_after_permit &&
      !terminal.has_value();
}

Task<bool> blocking_rejected_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  bool send_rejected = false;
  bool recv_rejected = false;
  try {
    (void)sender.blocking_send(1);
  } catch (const std::logic_error &) {
    send_rejected = true;
  }
  try {
    (void)receiver.blocking_recv();
  } catch (const std::logic_error &) {
    recv_rejected = true;
  }
  co_return send_rejected && recv_rejected &&
      sender.try_send(2).has_value() &&
      receiver.try_recv().has_value();
}

Task<bool>
marked_pending_send(Sender<int> sender,
                    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  auto result = co_await sender.send(2);
  co_return result.has_value();
}

Task<bool>
install_pending_send(Sender<int> sender,
                     std::shared_ptr<std::atomic<bool>> entered) {
  auto detached =
      cio::task::spawn(marked_pending_send(std::move(sender), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

Task<bool>
marked_pending_reserve_owned(
    Sender<int> sender, std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  auto result = co_await std::move(sender).reserve_owned();
  co_return result.has_value();
}

Task<bool>
install_pending_reserve_owned(
    Sender<int> sender, std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(
      marked_pending_reserve_owned(std::move(sender), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

Task<void>
marked_pending_closed(Sender<int> sender,
                      std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  co_await sender.closed();
}

Task<bool>
install_pending_closed(Sender<int> sender,
                       std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(
      marked_pending_closed(std::move(sender), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

Task<void>
marked_pending_recv(Task<std::optional<int>> receive,
                    std::shared_ptr<std::atomic<bool>> entered) {
  entered->store(true, std::memory_order_release);
  (void)co_await std::move(receive);
}

Task<bool>
install_pending_recv(Task<std::optional<int>> receive,
                     std::shared_ptr<std::atomic<bool>> entered) {
  auto detached = cio::task::spawn(
      marked_pending_recv(std::move(receive), entered));
  while (!entered->load(std::memory_order_acquire)) {
    co_await cio::task::yield_now();
  }
  co_await cio::task::yield_now();
  co_return !detached.is_finished();
}

Task<void>
mark_peer_progress(std::shared_ptr<std::atomic<bool>> progressed) {
  progressed->store(true, std::memory_order_release);
  co_return;
}

Task<void>
peer_try_send_and_drain(
    Sender<int> sender, std::shared_ptr<Receiver<int>> receiver,
    std::shared_ptr<std::atomic<bool>> succeeded, int value) {
  auto sent = sender.try_send(value);
  if (sent.has_value()) {
    auto received = receiver->try_recv();
    succeeded->store(received.has_value() && received.value() == value,
                     std::memory_order_release);
  }
  co_return;
}

Task<bool> send_budget_gate_before_capacity_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    co_await cio::task::consume_budget();
  }

  auto peer_succeeded = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(peer_try_send_and_drain(
      sender, shared_receiver, peer_succeeded, 2));
  auto sent = co_await sender.send(1);
  const auto peer_result = co_await peer;
  auto received = co_await shared_receiver->recv();
  co_return sent.has_value() && peer_result.has_value() &&
      peer_succeeded->load(std::memory_order_acquire) &&
      received == std::optional<int>{1};
}

Task<bool> reserve_budget_gate_before_capacity_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    co_await cio::task::consume_budget();
  }

  auto peer_succeeded = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(peer_try_send_and_drain(
      sender, shared_receiver, peer_succeeded, 3));
  auto reserved = co_await sender.reserve();
  const auto peer_result = co_await peer;
  co_return reserved.has_value() && peer_result.has_value() &&
      peer_succeeded->load(std::memory_order_acquire) &&
      sender.capacity() == 0;
}

Task<bool> reserve_owned_budget_gate_before_capacity_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto observer = sender;
  auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    co_await cio::task::consume_budget();
  }

  auto peer_succeeded = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(peer_try_send_and_drain(
      observer, shared_receiver, peer_succeeded, 4));
  auto reserved = co_await std::move(sender).reserve_owned();
  const auto peer_result = co_await peer;
  if (!reserved.has_value()) {
    co_return false;
  }
  auto returned = std::move(reserved).value().release();
  co_return peer_result.has_value() &&
      peer_succeeded->load(std::memory_order_acquire) &&
      returned.same_channel(observer) && observer.capacity() == 1;
}

Task<bool> owned_permit_close_paths_root() {
  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto observer = sender;
    auto reserved = co_await std::move(sender).reserve_owned();
    if (!reserved.has_value()) {
      co_return false;
    }
    receiver.close();
    auto returned = std::move(reserved).value().send(11);
    auto message = co_await receiver.recv();
    auto terminal = co_await receiver.recv();
    if (message != std::optional<int>{11} || terminal.has_value() ||
        !returned.same_channel(observer) || returned.capacity() != 1) {
      co_return false;
    }
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto reserved = co_await std::move(sender).reserve_owned();
    if (!reserved.has_value()) {
      co_return false;
    }
    auto permit = std::optional<OwnedPermit<int>>{
        std::move(reserved).value()};
    receiver.close();
    auto waiting = cio::task::spawn(receiver.recv());
    co_await cio::task::yield_now();
    if (waiting.is_finished()) {
      co_return false;
    }
    auto returned = std::move(*permit).release();
    permit.reset();
    auto terminal = co_await waiting;
    if (!terminal.has_value() || terminal.value().has_value() ||
        returned.capacity() != 1) {
      co_return false;
    }
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto reserved = co_await std::move(sender).reserve_owned();
    if (!reserved.has_value()) {
      co_return false;
    }
    auto permit = std::optional<OwnedPermit<int>>{
        std::move(reserved).value()};
    receiver.close();
    auto waiting = cio::task::spawn(receiver.recv());
    co_await cio::task::yield_now();
    if (waiting.is_finished()) {
      co_return false;
    }
    permit.reset();
    auto terminal = co_await waiting;
    if (!terminal.has_value() || terminal.value().has_value()) {
      co_return false;
    }
  }

  co_return true;
}

Task<bool> sender_closed_semantics_root() {
  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto first = cio::task::spawn(sender.closed());
    auto second = cio::task::spawn(sender.closed());
    co_await cio::task::yield_now();
    if (first.is_finished() || second.is_finished()) {
      co_return false;
    }
    receiver.close();
    auto first_result = co_await first;
    auto second_result = co_await second;
    if (!first_result.has_value() || !second_result.has_value()) {
      co_return false;
    }
    co_await sender.closed();
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto cancelled = cio::task::spawn(sender.closed());
    co_await cio::task::yield_now();
    cancelled.abort();
    auto cancelled_result = co_await cancelled;
    if (cancelled_result.has_value() ||
        !cancelled_result.error().is_cancelled()) {
      co_return false;
    }
    receiver.close();
    co_await sender.closed();
  }

  {
    auto channel = cio::sync::mpsc::channel<int>(1);
    auto sender = std::move(channel.first);
    auto receiver = std::optional<Receiver<int>>{
        std::move(channel.second)};
    auto waiting = cio::task::spawn(sender.closed());
    co_await cio::task::yield_now();
    receiver.reset();
    auto result = co_await waiting;
    if (!result.has_value()) {
      co_return false;
    }
  }

  co_return true;
}

Task<bool> reserve_owned_cancel_last_sender_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(1).has_value()) {
    co_return false;
  }
  auto pending =
      cio::task::spawn(std::move(sender).reserve_owned());
  co_await cio::task::yield_now();
  if (pending.is_finished()) {
    co_return false;
  }
  pending.abort();
  auto cancelled = co_await pending;
  auto first = co_await receiver.recv();
  auto terminal = co_await receiver.recv();
  co_return !cancelled.has_value() &&
      cancelled.error().is_cancelled() &&
      first == std::optional<int>{1} && !terminal.has_value() &&
      receiver.sender_strong_count() == 0;
}

Task<bool> reserve_owned_closed_consumes_last_sender_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  receiver.close();
  auto rejected = co_await std::move(sender).reserve_owned();
  co_return !rejected.has_value() &&
      receiver.sender_strong_count() == 0 &&
      receiver.is_closed();
}

Task<void>
receive_three(std::shared_ptr<Receiver<int>> receiver,
              std::shared_ptr<std::vector<int>> values) {
  for (int count = 0; count < 3; ++count) {
    auto value = co_await receiver->recv();
    if (!value) {
      throw std::runtime_error{"公平队位测试提前断开"};
    }
    values->push_back(*value);
  }
}

Task<bool> send_budget_gate_before_queue_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  if (!sender.try_send(0).has_value()) {
    co_return false;
  }
  auto shared_receiver =
      std::make_shared<Receiver<int>>(std::move(receiver));
  for (std::size_t unit = 0; unit < 128; ++unit) {
    co_await cio::task::consume_budget();
  }

  auto completion_order = std::make_shared<std::vector<int>>();
  auto received = std::make_shared<std::vector<int>>();
  auto competitor = cio::task::spawn(
      ordered_send(sender, 2, completion_order));
  auto drainer =
      cio::task::spawn(receive_three(shared_receiver, received));
  auto root_sent = co_await sender.send(1);
  completion_order->push_back(1);
  const auto competitor_result = co_await competitor;
  const auto drainer_result = co_await drainer;
  co_return root_sent.has_value() && competitor_result.has_value() &&
      competitor_result.value() && drainer_result.has_value() &&
      *received == std::vector<int>({0, 2, 1}) &&
      *completion_order == std::vector<int>({2, 1});
}

Task<bool> closed_send_budget_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  receiver.close();
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  bool yielded = false;
  for (int value = 0; value < 384; ++value) {
    auto result = co_await sender.send(value);
    if (result.has_value() || result.error().value() != value) {
      co_return false;
    }
    yielded = yielded || progressed->load(std::memory_order_acquire);
  }
  const auto peer_result = co_await peer;
  co_return yielded && peer_result.has_value();
}

Task<bool> closed_reserve_budget_root() {
  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  receiver.close();
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  bool yielded = false;
  for (int attempt = 0; attempt < 384; ++attempt) {
    auto result = co_await sender.reserve();
    if (result.has_value()) {
      co_return false;
    }
    yielded = yielded || progressed->load(std::memory_order_acquire);
  }
  const auto peer_result = co_await peer;
  co_return yielded && peer_result.has_value();
}

Task<bool> cooperative_budget_root() {
  constexpr int iterations = 384;
  auto [sender, receiver] =
      cio::sync::mpsc::channel<int>(static_cast<std::size_t>(iterations));
  auto progressed = std::make_shared<std::atomic<bool>>(false);
  auto peer = cio::task::spawn(mark_peer_progress(progressed));
  bool yielded = false;
  for (int value = 0; value < iterations; ++value) {
    auto sent = co_await sender.send(value);
    if (!sent.has_value()) {
      co_return false;
    }
    if (progressed->load(std::memory_order_acquire)) {
      yielded = true;
    }
  }
  const auto peer_result = co_await peer;

  for (int expected = 0; expected < iterations; ++expected) {
    auto value = co_await receiver.recv();
    if (!value || *value != expected) {
      co_return false;
    }
  }
  co_return yielded && peer_result.has_value();
}

void test_validation_errors_and_traits() {
  bool zero_rejected = false;
  bool oversized_rejected = false;
  try {
    (void)cio::sync::mpsc::channel<int>(0);
  } catch (const std::invalid_argument &) {
    zero_rejected = true;
  }
  try {
    (void)cio::sync::mpsc::channel<int>(
        cio::sync::Semaphore::MAX_PERMITS + 1);
  } catch (const std::invalid_argument &) {
    oversized_rejected = true;
  }
  check(zero_rejected && oversized_rejected,
        "bounded mpsc 非法容量未拒绝");

  auto [sender, receiver] =
      cio::sync::mpsc::channel<int>(cio::sync::Semaphore::MAX_PERMITS);
  check(sender.max_capacity() == cio::sync::Semaphore::MAX_PERMITS &&
            sender.capacity() == cio::sync::Semaphore::MAX_PERMITS &&
            sender.try_send(9).has_value() &&
            receiver.try_recv().has_value(),
        "bounded mpsc 最大容量元数据或惰性分配错误");

  auto full_channel = cio::sync::mpsc::channel<int>(1);
  check(full_channel.first.try_send(1).has_value(),
        "错误格式测试预填充失败");
  auto full = full_channel.first.try_send(2);
  check(!full.has_value() && full.error().is_full() &&
            full.error().value() == 2 &&
            full.error().message() == "no available capacity" &&
            full.error().debug_string() == "\"Full(..)\"",
        "TrySendError::Full 内容或格式错误");

  full_channel.second.close();
  auto closed = full_channel.first.try_send(3);
  check(!closed.has_value() && closed.error().is_closed() &&
            closed.error().value() == 3 &&
            closed.error().message() == "channel closed" &&
            closed.error().debug_string() == "\"Closed(..)\"",
        "TrySendError::Closed 内容或格式错误");

  std::ostringstream send_display;
  send_display << SendError<int>{4};
  std::ostringstream recv_display;
  recv_display << TryRecvError::empty;
  check(send_display.str() == "channel closed" &&
            SendError<int>::debug_string() == "SendError { .. }" &&
            recv_display.str() == "receiving on an empty channel" &&
            cio::sync::mpsc::error::debug_string(
                TryRecvError::disconnected) == "Disconnected",
        "mpsc 错误 Display/Debug 格式错误");
}

void test_basic_fifo_fairness_and_cancellation() {
  Runtime runtime;
  check(runtime.block_on(fifo_backpressure_root()),
        "bounded mpsc FIFO 或背压错误");
  check(runtime.block_on(fifo_fairness_root()),
        "bounded mpsc send FIFO 公平性错误");
  check(runtime.block_on(send_cancellation_root()),
        "bounded mpsc send 取消未归还容量或发送了消息");
  check(runtime.block_on(reserve_cancellation_root()),
        "bounded mpsc reserve 取消未归还容量");
}

void test_permit_close_and_counts() {
  Runtime runtime;
  check(runtime.block_on(permit_capacity_root()),
        "bounded mpsc Permit 容量语义错误");
  check(runtime.block_on(close_drain_outstanding_permit_root()),
        "bounded mpsc close/drain/outstanding permit 语义错误");
  check(runtime.block_on(close_waits_for_dropped_permit_root()),
        "bounded mpsc close 未等待 permit drop");
  check(runtime.block_on(weak_permit_liveness_root()),
        "bounded mpsc permit 未保持逻辑 Sender lease");

  auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
  auto sender_two = sender;
  auto weak_one = sender.downgrade();
  auto weak_two = weak_one;
  check(sender.strong_count() == 2 && sender.weak_count() == 2 &&
            weak_one.strong_count() == 2 && weak_two.weak_count() == 2 &&
            receiver.sender_strong_count() == 2 &&
            receiver.sender_weak_count() == 2,
        "bounded mpsc clone/downgrade 初始计数错误");
  sender_two = Sender<int>{sender};
  check(sender.strong_count() == 2,
        "Sender copy assignment 未保持精确强计数");
  {
    auto upgraded = weak_one.upgrade();
    check(upgraded.has_value() && sender.strong_count() == 3,
          "WeakSender upgrade 未增加强计数");
  }
  check(sender.strong_count() == 2,
        "升级 Sender drop 未减少强计数");
  weak_two = WeakSender<int>{weak_one};
  check(sender.weak_count() == 2,
        "WeakSender copy assignment 未保持精确弱计数");

  std::optional<Sender<int>> first{std::move(sender)};
  std::optional<Sender<int>> second{std::move(sender_two)};
  first.reset();
  check(weak_one.strong_count() == 1 && weak_one.upgrade().has_value(),
        "仍有强 Sender 时 WeakSender upgrade 失败");
  second.reset();
  check(weak_one.strong_count() == 0 && !weak_one.upgrade().has_value() &&
            receiver.is_closed(),
        "强计数归零后 WeakSender 复活或 Receiver 未关闭");
  auto terminal = runtime.block_on(receiver.recv());
  check(!terminal.has_value(), "最后 Sender drop 后 Receiver 未终止");
}

void test_owned_permit_closed_and_observation() {
  Runtime runtime;
  check(runtime.block_on(owned_permit_close_paths_root()),
        "OwnedPermit 在 close 后的 send/release/drop 路径错误");
  check(runtime.block_on(sender_closed_semantics_root()),
        "Sender::closed 多 waiter/永久 ready/cancel/drop 语义错误");
  check(runtime.block_on(reserve_owned_cancel_last_sender_root()),
        "reserve_owned 取消未释放最后一个强 Sender");
  check(runtime.block_on(reserve_owned_closed_consumes_last_sender_root()),
        "reserve_owned Closed 错误未消费最后一个强 Sender");

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(3);
    auto clone = sender;
    auto clone_two = sender;
    auto foreign = cio::sync::mpsc::channel<int>(1);
    check(sender.same_channel(clone) &&
              !sender.same_channel(foreign.first) &&
              receiver.is_empty() && receiver.len() == 0,
          "same_channel 或 Receiver 初始观察错误");

    auto reserved = sender.try_reserve();
    check(reserved.has_value() && sender.capacity() == 2 &&
              receiver.is_empty() && receiver.len() == 0,
          "try_reserve 未预留容量或错误计入 Receiver::len");
    std::move(reserved).value().send(1);
    check(!receiver.is_empty() && receiver.len() == 1,
          "Permit send 后 Receiver::len/is_empty 错误");

    auto owned_result = std::move(clone).try_reserve_owned();
    check(owned_result.has_value() && sender.capacity() == 1 &&
              receiver.sender_strong_count() == 3,
          "try_reserve_owned 成功路径错误");
    auto owned = std::move(owned_result).value();
    auto second_owned_result =
        std::move(clone_two).try_reserve_owned();
    if (!second_owned_result.has_value()) {
      throw std::runtime_error{"第二个 OwnedPermit 预留失败"};
    }
    auto second_owned = std::move(second_owned_result).value();
    check(owned.same_channel_as_sender(sender) &&
              owned.same_channel(second_owned) &&
              !owned.same_channel_as_sender(foreign.first) &&
              receiver.len() == 1 &&
              receiver.sender_strong_count() == 3,
          "OwnedPermit channel identity 或 len 统计错误");
    auto second_returned = std::move(second_owned).release();
    auto returned = std::move(owned).send(2);
    check(returned.same_channel(sender) &&
              second_returned.same_channel(sender) &&
              receiver.len() == 2 &&
              receiver.sender_strong_count() == 3,
          "OwnedPermit::send 未返回原 channel Sender");
    auto first = receiver.try_recv();
    auto second = receiver.try_recv();
    check(first.has_value() && first.value() == 1 &&
              second.has_value() && second.value() == 2 &&
              receiver.is_empty() && receiver.len() == 0 &&
              sender.capacity() == 3,
          "Receiver::len/is_empty 未随接收更新");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(2);
    auto clone = sender;
    auto first_result = std::move(sender).try_reserve_owned();
    auto second_result = std::move(clone).try_reserve_owned();
    if (!first_result.has_value() || !second_result.has_value()) {
      throw std::runtime_error{"OwnedPermit move assignment 预留失败"};
    }
    auto first = std::move(first_result).value();
    auto second = std::move(second_result).value();
    check(receiver.sender_strong_count() == 2 &&
              receiver.capacity() == 0,
          "OwnedPermit move assignment 初始计数错误");
    first = std::move(second);
    check(receiver.sender_strong_count() == 1 &&
              receiver.capacity() == 1,
          "OwnedPermit move assignment 未释放目标旧 permit");
    auto returned = std::move(first).release();
    check(receiver.sender_strong_count() == 1 &&
              receiver.capacity() == 2 &&
              returned.capacity() == 2,
          "OwnedPermit move assignment 源 lease 转移错误");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto held = sender.try_reserve();
    check(held.has_value(), "try_reserve Full 测试预留失败");
    auto full_reserve = sender.try_reserve();
    check(!full_reserve.has_value() && full_reserve.error().is_full(),
          "try_reserve Full 错误内容不匹配");
    std::move(full_reserve.error()).into_inner();
    auto owned_attempt =
        std::move(sender).try_reserve_owned();
    check(!owned_attempt.has_value() &&
              owned_attempt.error().is_full() &&
              owned_attempt.error().message() ==
                  "no available capacity" &&
              owned_attempt.error().debug_string() == "\"Full(..)\"",
          "try_reserve_owned Full 错误内容不匹配");
    auto returned =
        std::move(owned_attempt.error()).into_inner();
    check(returned.capacity() == 0,
          "try_reserve_owned Full 未返还 Sender");
    std::move(held).value().send(3);
    check(receiver.try_recv().has_value() &&
              returned.capacity() == 1,
          "Full 错误返还的 Sender 无法继续使用");

    receiver.close();
    auto closed_attempt =
        std::move(returned).try_reserve_owned();
    check(!closed_attempt.has_value() &&
              closed_attempt.error().is_closed(),
          "try_reserve_owned Closed 分支错误");
    auto closed_sender =
        std::move(closed_attempt.error()).into_inner();
    check(closed_sender.is_closed(),
          "try_reserve_owned Closed 未返还原 Sender");
    auto closed_reserve = closed_sender.try_reserve();
    check(!closed_reserve.has_value() &&
              closed_reserve.error().is_closed(),
          "try_reserve Closed 错误内容不匹配");
    std::move(closed_reserve.error()).into_inner();
  }

  {
    auto throw_now = std::make_shared<std::atomic<bool>>(false);
    auto [sender, receiver] =
        cio::sync::mpsc::channel<ThrowingMove>(1);
    auto acquired = std::move(sender).try_reserve_owned();
    check(acquired.has_value(), "OwnedPermit 异常测试预留失败");
    auto owned = std::move(acquired).value();
    throw_now->store(true, std::memory_order_release);
    bool threw = false;
    try {
      (void)std::move(owned).send(ThrowingMove{throw_now, 9});
    } catch (const std::runtime_error &) {
      threw = true;
    }
    auto returned = std::move(owned).release();
    check(threw && returned.capacity() == 1 &&
              receiver.is_empty(),
          "OwnedPermit 用户移动异常后未保持可 release 状态");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto acquired = std::move(sender).try_reserve_owned();
    check(acquired.has_value(), "OwnedPermit 跨线程预留失败");
    auto owned = std::move(acquired).value();
    auto returned =
        std::make_shared<std::optional<Sender<int>>>();
    std::thread producer{
        [permit = std::move(owned), returned]() mutable {
          returned->emplace(std::move(permit).send(17));
        }};
    producer.join();
    auto value = receiver.try_recv();
    check(value.has_value() && value.value() == 17 &&
              returned->has_value() &&
              returned->value().capacity() == 1,
          "OwnedPermit 跨线程 send/返回 Sender 错误");
  }

  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    auto channel = cio::sync::mpsc::channel<DropProbe>(1);
    auto acquired =
        std::move(channel.first).try_reserve_owned();
    check(acquired.has_value(),
          "Receiver drop 后 OwnedPermit 测试预留失败");
    auto owned = std::move(acquired).value();
    auto receiver = std::optional<Receiver<DropProbe>>{
        std::move(channel.second)};
    receiver.reset();
    auto returned = std::optional<Sender<DropProbe>>{
        std::move(owned).send(DropProbe{drops, 5})};
    check(returned->is_closed() &&
              drops->load(std::memory_order_relaxed) == 0,
          "Receiver drop 后 OwnedPermit::send 未成功持有消息");
    returned.reset();
    check(drops->load(std::memory_order_relaxed) == 1,
          "Receiver drop 后 OwnedPermit 消息泄漏或重复析构");
  }
}

void test_blocking_and_cross_thread() {
  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto received = std::make_shared<std::optional<int>>();
    std::thread waiter{
        [receiver = std::move(receiver), received]() mutable {
          *received = receiver.blocking_recv();
        }};
    std::this_thread::sleep_for(10ms);
    auto sent = sender.blocking_send(41);
    waiter.join();
    check(sent.has_value() && *received == std::optional<int>{41},
          "blocking_send/blocking_recv 跨线程桥接错误");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    check(sender.try_send(1).has_value(),
          "blocking 背压测试预填充失败");
    auto started = std::make_shared<std::atomic<bool>>(false);
    auto completed = std::make_shared<std::atomic<bool>>(false);
    auto succeeded = std::make_shared<std::atomic<bool>>(false);
    std::thread producer{[sender, started, completed, succeeded]() mutable {
      started->store(true, std::memory_order_release);
      auto result = sender.blocking_send(2);
      succeeded->store(result.has_value(), std::memory_order_release);
      completed->store(true, std::memory_order_release);
    }};
    while (!started->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    std::this_thread::sleep_for(10ms);
    check(!completed->load(std::memory_order_acquire),
          "blocking_send 未遵守容量背压");
    auto first = receiver.blocking_recv();
    producer.join();
    auto second = receiver.blocking_recv();
    check(first == std::optional<int>{1} &&
              second == std::optional<int>{2} &&
              succeeded->load(std::memory_order_acquire),
          "blocking_send 公平等待或唤醒错误");
  }

  {
    Runtime runtime;
    check(runtime.block_on(blocking_rejected_root()),
          "mpsc blocking API 未拒绝 runtime worker");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    std::thread producer{[sender]() mutable {
      std::this_thread::sleep_for(10ms);
      if (!sender.try_send(77).has_value()) {
        std::terminate();
      }
    }};
    Runtime runtime;
    auto received = runtime.block_on(receiver.recv());
    producer.join();
    check(received == std::optional<int>{77},
          "外部线程发送未唤醒 runtime Receiver");
  }
}

void test_drop_outside_lock_and_exactly_once() {
  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    auto reentered = std::make_shared<std::atomic<bool>>(false);
    auto channel = cio::sync::mpsc::channel<DropProbe>(1);
    auto weak = channel.first.downgrade();
    auto callback = std::make_shared<std::function<void()>>(
        [weak, reentered] {
          if (weak.weak_count() >= 1) {
            reentered->store(true, std::memory_order_release);
          }
        });
    check(channel.first
              .try_send(DropProbe{drops, 1, std::move(callback)})
              .has_value(),
          "DropProbe 入队失败");
    std::optional<Receiver<DropProbe>> receiver{
        std::move(channel.second)};
    receiver.reset();
    check(drops->load(std::memory_order_relaxed) == 1 &&
              reentered->load(std::memory_order_acquire),
          "Receiver drop 未在锁外恰好析构消息一次");
  }

  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    auto channel = cio::sync::mpsc::channel<DropProbe>(1);
    std::optional<Receiver<DropProbe>> receiver{
        std::move(channel.second)};
    receiver.reset();
    {
      auto rejected =
          channel.first.try_send(DropProbe{drops, 2});
      check(!rejected.has_value() && rejected.error().is_closed() &&
                rejected.error().value().value == 2 &&
                drops->load(std::memory_order_relaxed) == 0,
            "关闭发送未返还 DropProbe 所有权");
    }
    check(drops->load(std::memory_order_relaxed) == 1,
          "关闭发送错误 payload 未恰好析构一次");
  }

  {
    auto drops = std::make_shared<std::atomic<int>>(0);
    Runtime runtime;
    auto channel = cio::sync::mpsc::channel<DropProbe>(1);
    check(channel.first.try_send(DropProbe{drops, 1}).has_value(),
          "取消析构测试预填充失败");
    auto pending = runtime.spawn(
        channel.first.send(DropProbe{drops, 2}));
    runtime.block_on([]() -> Task<void> {
      co_await cio::task::yield_now();
    }());
    pending.abort();
    auto joined = runtime.block_on(
        [&pending]() -> Task<
            cio::Result<cio::Result<void, SendError<DropProbe>>,
                        cio::task::JoinError>> {
          co_return co_await pending;
        }());
    check(!joined.has_value() && joined.error().is_cancelled() &&
              drops->load(std::memory_order_relaxed) == 1,
          "取消 pending send 未恰好析构 payload 一次");
    auto filler = channel.second.try_recv();
    check(filler.has_value() && filler.value().value == 1,
          "取消 pending send 破坏了原缓冲消息");
  }
}

void test_runtime_shutdown_cleanup_and_cooperative_budget() {
  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    check(sender.try_send(1).has_value(), "shutdown send 预填充失败");
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_send(sender, entered)),
            "runtime shutdown 前未建立 pending send");
    }
    auto first = receiver.try_recv();
    check(first.has_value() && first.value() == 1 &&
              sender.capacity() == 1 && sender.try_send(3).has_value(),
          "runtime shutdown 未取消 send 或未归还容量");
    auto third = receiver.try_recv();
    check(third.has_value() && third.value() == 3,
          "runtime shutdown 后 channel 无法继续使用");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_recv(receiver.recv(), entered)),
            "runtime shutdown 前未建立 pending recv");
    }
    check(sender.try_send(4).has_value(),
          "runtime shutdown 后接收 operation 未释放独占状态");
    Runtime continuation;
    auto received = continuation.block_on(receiver.recv());
    check(received == std::optional<int>{4},
          "runtime shutdown 后 Receiver 无法重试");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    check(sender.try_send(5).has_value(),
          "shutdown reserve_owned 预填充失败");
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_reserve_owned(sender, entered)),
            "runtime shutdown 前未建立 pending reserve_owned");
    }
    auto first = receiver.try_recv();
    auto retry = std::move(sender).try_reserve_owned();
    check(first.has_value() && first.value() == 5 &&
              retry.has_value(),
          "runtime shutdown 未释放 reserve_owned operation");
    auto returned = std::move(retry).value().release();
    check(returned.capacity() == 1,
          "runtime shutdown 后 reserve_owned 无法重试");
  }

  {
    auto [sender, receiver] = cio::sync::mpsc::channel<int>(1);
    auto entered = std::make_shared<std::atomic<bool>>(false);
    {
      Runtime shutting_down;
      check(shutting_down.block_on(
                install_pending_closed(sender, entered)),
            "runtime shutdown 前未建立 pending Sender::closed");
    }
    receiver.close();
    Runtime continuation;
    continuation.block_on(sender.closed());
    check(sender.is_closed(),
          "runtime shutdown 后 Sender::closed 无法重试");
  }

  {
    Runtime runtime;
    check(runtime.block_on(cooperative_budget_root()),
          "always-ready mpsc 操作未消耗 cooperative budget");
    check(runtime.block_on(closed_send_budget_root()),
          "always-ready closed send 错误循环饿死 peer");
    check(runtime.block_on(closed_reserve_budget_root()),
          "always-ready closed reserve 错误循环饿死 peer");
    check(runtime.block_on(send_budget_gate_before_capacity_root()),
          "send 在 cooperative gate 前抢占 capacity");
    check(runtime.block_on(reserve_budget_gate_before_capacity_root()),
          "reserve 在 cooperative gate 前抢占 capacity");
    check(runtime.block_on(
              reserve_owned_budget_gate_before_capacity_root()),
          "reserve_owned 在 cooperative gate 前抢占 capacity");
    check(runtime.block_on(send_budget_gate_before_queue_root()),
          "send 在 cooperative gate 前抢占 FIFO 队位");
  }
}

void test_pending_value_timing_and_exception_safety() {
  {
    auto moves = std::make_shared<std::atomic<int>>(0);
    auto [sender, receiver] =
        cio::sync::mpsc::channel<MoveObserved>(1);
    check(sender.try_send(MoveObserved{moves, 1}).has_value(),
          "值时序测试预填充失败");
    auto operation = sender.send(MoveObserved{moves, 2});
    const int moves_before_poll =
        moves->load(std::memory_order_relaxed);
    Runtime runtime;
    auto pending = runtime.spawn(std::move(operation));
    runtime.block_on([]() -> Task<void> {
      co_await cio::task::yield_now();
    }());
    check(!pending.is_finished() &&
              moves->load(std::memory_order_relaxed) ==
                  moves_before_poll,
          "pending send 在 permit 取得前移动了用户值");
    pending.abort();
    auto joined = runtime.block_on(
        [&pending]() -> Task<
            cio::Result<cio::Result<void, SendError<MoveObserved>>,
                        cio::task::JoinError>> {
          co_return co_await pending;
        }());
    check(!joined.has_value() && joined.error().is_cancelled(),
          "pending 值时序测试取消失败");
    check(receiver.try_recv().has_value() && sender.capacity() == 1,
          "pending 值时序取消泄漏容量");
  }

  {
    auto throw_now = std::make_shared<std::atomic<bool>>(false);
    auto [sender, receiver] =
        cio::sync::mpsc::channel<ThrowingMove>(1);
    auto operation = sender.send(ThrowingMove{throw_now, 5});
    throw_now->store(true, std::memory_order_release);
    bool threw = false;
    try {
      Runtime runtime;
      (void)runtime.block_on(std::move(operation));
    } catch (const std::runtime_error &) {
      threw = true;
    }
    check(threw && sender.capacity() == 1 &&
              !receiver.try_recv().has_value(),
          "permit 后用户移动异常未归还容量或发布了半条消息");
  }

  {
    auto throw_now = std::make_shared<std::atomic<bool>>(false);
    auto [sender, receiver] =
        cio::sync::mpsc::channel<ThrowingMove>(1);
    check(sender.try_send(ThrowingMove{throw_now, 7}).has_value(),
          "接收异常测试预填充失败");
    throw_now->store(true, std::memory_order_release);
    bool threw = false;
    try {
      (void)receiver.try_recv();
    } catch (const std::runtime_error &) {
      threw = true;
    }
    throw_now->store(false, std::memory_order_release);
    auto empty = receiver.try_recv();
    check(threw && sender.capacity() == 1 && !empty.has_value() &&
              empty.error() == TryRecvError::empty,
          "接收结果移动异常未归还容量或重复交付消息");

    check(sender.try_send(ThrowingMove{throw_now, 8}).has_value(),
          "接收结果移动异常后 channel 无法继续发送");
    auto recovered = receiver.try_recv();
    check(recovered.has_value() && recovered.value().value == 8 &&
              sender.capacity() == 1,
          "接收结果移动异常后 Receiver 无法继续使用");
  }

  {
    auto entered = std::make_shared<std::atomic<bool>>(false);
    auto proceed = std::make_shared<std::atomic<bool>>(false);
    auto moves = std::make_shared<std::atomic<int>>(0);
    auto threw = std::make_shared<std::atomic<bool>>(false);
    auto [sender, receiver] =
        cio::sync::mpsc::channel<BlockingThrowingMove>(1);

    std::thread producer{
        [sender, entered, proceed, moves, threw]() mutable {
          BlockingThrowingMove value{entered, proceed, moves};
          try {
            (void)sender.try_send(std::move(value));
          } catch (const std::runtime_error &) {
            threw->store(true, std::memory_order_release);
          }
        }};
    while (!entered->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    check(sender.capacity() == 0,
          "try_send 并发异常未先取得容量");
    receiver.close();

    Runtime runtime;
    auto waiting = runtime.spawn(receiver.recv());
    runtime.block_on([]() -> Task<void> {
      co_await cio::task::yield_now();
    }());
    check(!waiting.is_finished(),
          "close 后 outstanding try_send 尚未结清时 recv 提前完成");

    proceed->store(true, std::memory_order_release);
    producer.join();
    auto terminal = runtime.block_on(
        [&waiting]() -> Task<
            cio::Result<std::optional<BlockingThrowingMove>,
                        cio::task::JoinError>> {
          co_return co_await waiting;
        }());
    check(threw->load(std::memory_order_acquire) &&
              terminal.has_value() &&
              !terminal.value().has_value() &&
              sender.capacity() == 1,
          "try_send move 异常释放容量后未唤醒 close/drain Receiver");
  }
}

static_assert(std::copy_constructible<Sender<int>>);
static_assert(std::is_copy_assignable_v<Sender<int>>);
static_assert(!std::copy_constructible<Receiver<int>>);
static_assert(std::move_constructible<Receiver<int>>);
static_assert(!std::copy_constructible<Permit<int>>);
static_assert(std::move_constructible<Permit<int>>);
static_assert(!std::copy_constructible<OwnedPermit<int>>);
static_assert(std::move_constructible<OwnedPermit<int>>);
static_assert(std::copy_constructible<WeakSender<int>>);
static_assert(cio::Send<Sender<SendOnly>>);
static_assert(cio::Sync<Sender<SendOnly>>);
static_assert(cio::Send<Receiver<SendOnly>>);
static_assert(cio::Sync<Receiver<SendOnly>>);
static_assert(cio::Send<Permit<SendOnly>>);
static_assert(cio::Sync<Permit<SendOnly>>);
static_assert(cio::Send<OwnedPermit<SendOnly>>);
static_assert(cio::Sync<OwnedPermit<SendOnly>>);
static_assert(cio::Send<WeakSender<SendOnly>>);
static_assert(cio::Sync<WeakSender<SendOnly>>);
static_assert(!cio::Send<Sender<NotSend>>);
static_assert(!cio::Send<Receiver<NotSend>>);
static_assert(!cio::Send<OwnedPermit<NotSend>>);
static_assert(cio::detail::MpscOwnedValue<int>);
static_assert(!cio::detail::MpscOwnedValue<int *>);
static_assert(
    !cio::detail::MpscOwnedValue<std::reference_wrapper<int>>);

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"mpsc validation errors traits",
       test_validation_errors_and_traits},
      {"mpsc FIFO fairness cancellation",
       test_basic_fifo_fairness_and_cancellation},
      {"mpsc permit close counts", test_permit_close_and_counts},
      {"mpsc owned permit closed observation",
       test_owned_permit_closed_and_observation},
      {"mpsc blocking cross-thread", test_blocking_and_cross_thread},
      {"mpsc drop outside lock", test_drop_outside_lock_and_exactly_once},
      {"mpsc shutdown cooperative",
       test_runtime_shutdown_cleanup_and_cooperative_budget},
      {"mpsc value timing exception",
       test_pending_value_timing_and_exception_safety},
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
  std::cout << "bounded mpsc 全部通过：" << passed << " 项\n";
  return 0;
}
