#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/io/buffer.hpp"
#include "cio/io/operation.hpp"

// 故意把已知 borrowed 标为 Send，证明 OperationOwnedValue 仍会在结构边界拒绝，
// 而不是让测试只依赖未知类型默认非 Send。
namespace cio {

template <typename Element, std::size_t Extent>
struct send_traits<std::span<Element, Extent>>
    : std::true_type {};

template <typename Value>
struct send_traits<std::reference_wrapper<Value>>
    : std::true_type {};

}  // namespace cio

namespace {

using cio::io::OperationKey;
using cio::io::OperationState;
using cio::io::OperationTerminal;

template <typename Lease, typename Result>
concept RegistryFormable = requires {
  typename cio::io::OperationRegistry<Lease, Result>;
};

template <typename Lease, typename Result, typename Wake>
concept RegistryWithWakeFormable = requires {
  typename cio::io::OperationRegistry<
      Lease,
      Result,
      Wake>;
};

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

std::vector<std::byte> bytes(std::string_view text) {
  std::vector<std::byte> result;
  result.reserve(text.size());
  for (const char value : text) {
    result.push_back(
        static_cast<std::byte>(
            static_cast<unsigned char>(value)));
  }
  return result;
}

class TrackingLease final {
 public:
  static constexpr bool cio_send = true;

  TrackingLease(
      cio::io::ConstBufferLease buffer,
      std::shared_ptr<std::atomic<std::size_t>> releases) noexcept
      : buffer_{std::move(buffer)},
        releases_{std::move(releases)} {}

  TrackingLease(const TrackingLease&) = delete;
  TrackingLease& operator=(const TrackingLease&) = delete;

  TrackingLease(TrackingLease&& other) noexcept
      : buffer_{std::move(other.buffer_)},
        releases_{std::move(other.releases_)},
        owns_{std::exchange(other.owns_, false)} {}

  TrackingLease& operator=(TrackingLease&& other) noexcept {
    if (this != &other) {
      release();
      buffer_ = std::move(other.buffer_);
      releases_ = std::move(other.releases_);
      owns_ = std::exchange(other.owns_, false);
    }
    return *this;
  }

  ~TrackingLease() {
    release();
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return buffer_.size();
  }

 private:
  void release() noexcept {
    if (owns_ && releases_) {
      releases_->fetch_add(1, std::memory_order_relaxed);
      owns_ = false;
    }
  }

  cio::io::ConstBufferLease buffer_;
  std::shared_ptr<std::atomic<std::size_t>> releases_;
  bool owns_{true};
};

struct ProbeWakeState;

class ProbeWake final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_operation_wake = true;
  static constexpr bool cio_operation_enqueue_only = true;

  ProbeWake() noexcept = default;

  explicit ProbeWake(
      std::shared_ptr<ProbeWakeState> state) noexcept
      : state_{std::move(state)} {}

  void operator()(OperationKey key) noexcept;

 private:
  std::shared_ptr<ProbeWakeState> state_;
};

using Registry = cio::io::OperationRegistry<
    TrackingLease,
    int,
    ProbeWake>;

struct ProbeWakeState final {
  std::weak_ptr<Registry> registry;
  std::shared_ptr<std::atomic<bool>> woke;
  std::shared_ptr<std::atomic<bool>> observed_delivered;
  std::shared_ptr<std::atomic<std::size_t>> wake_count;
  std::shared_ptr<std::thread::id> enqueue_thread;
  std::shared_ptr<std::atomic<std::size_t>>
      target_continuation_runs;

  void enqueue(OperationKey key) noexcept;
};

void ProbeWakeState::enqueue(OperationKey key) noexcept {
  if (wake_count) {
    wake_count->fetch_add(
        1,
        std::memory_order_relaxed);
  }
  // enqueue target 只记录入队线程；绝不直接调用或 poll 目标 continuation。
  if (enqueue_thread) {
    *enqueue_thread =
        std::this_thread::get_id();
  }
  if (const auto target_registry = registry.lock()) {
    const auto operation_state =
        target_registry->state(key);
    if (observed_delivered) {
      observed_delivered->store(
          operation_state.has_value() &&
              *operation_state ==
                  OperationState::delivered,
          std::memory_order_release);
    }
  }
  if (woke) {
    woke->store(true, std::memory_order_release);
  }
}

void ProbeWake::operator()(OperationKey key) noexcept {
  const auto state = state_;
  if (state) {
    state->enqueue(key);
  }
}

ProbeWake counting_wake(
    std::shared_ptr<std::atomic<std::size_t>> count) {
  auto state = std::make_shared<ProbeWakeState>();
  state->wake_count = std::move(count);
  return ProbeWake{std::move(state)};
}

class ThrowingWake final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_operation_wake = true;
  static constexpr bool cio_operation_enqueue_only = true;

  void operator()(OperationKey) {}
};

class NotEnqueueOnlyWake final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_operation_wake = true;

  void operator()(OperationKey) noexcept {}
};

class UnmarkedWake final {
 public:
  static constexpr bool cio_send = true;

  void operator()(OperationKey) noexcept {}
};

TrackingLease tracking_lease(
    std::string_view text,
    const std::shared_ptr<std::atomic<std::size_t>>& releases) {
  const auto input = bytes(text);
  auto buffer = cio::io::SharedBuffer::copy_from(
      std::span<const std::byte>{input});
  return TrackingLease{buffer.lease(), releases};
}

void state_machine_and_lock_free_dispatch_test() {
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto woke = std::make_shared<std::atomic<bool>>(false);
  const auto observed_delivered =
      std::make_shared<std::atomic<bool>>(false);
  auto registry = std::make_shared<Registry>();
  auto wake_state = std::make_shared<ProbeWakeState>();
  wake_state->registry = registry;
  wake_state->woke = woke;
  wake_state->observed_delivered = observed_delivered;

  OperationKey key;
  key = registry->create(
      tracking_lease("abcd", releases),
      ProbeWake{std::move(wake_state)});

  check(key.valid() && registry->size() == 1,
        "create 必须返回有效 OperationKey 并唯一拥有 record");
  check(registry->state(key) == OperationState::created,
        "新 operation 必须处于 created");
  check(!registry->complete(key, 1).has_value(),
        "created operation 不得跳过 submit 直接完成");
  check(registry->submit(key) && !registry->submit(key),
        "submit 只能使 created 转移一次");

  auto claim = registry->begin_complete(key, 7);
  check(claim.has_value() &&
            registry->state(key) == OperationState::completing,
        "complete 胜者必须先取得 completing 交付权");
  check(!registry->begin_cancel(key, -1).has_value(),
        "completing 后 cancel 不得再取得终态");
  check(releases->load(std::memory_order_acquire) == 0,
        "completing 必须继续持有 owning buffer lease");

  auto dispatch = claim->deliver();
  check(dispatch.has_value() &&
            registry->state(key) == OperationState::delivered &&
            !claim->deliver().has_value(),
        "claim 必须恰好一次发布 delivered");
  check(!woke->load(std::memory_order_acquire) &&
            releases->load(std::memory_order_acquire) == 0,
        "deliver 只能移出 wake，不能在 registry 路径内执行回调或释放 lease");
  check(dispatch->run() && !dispatch->run() &&
            woke->load(std::memory_order_acquire) &&
            observed_delivered->load(std::memory_order_acquire),
        "锁外 dispatch 必须可重入 registry 且最多运行一次");

  auto delivery = registry->consume(key);
  check(delivery.has_value() &&
            delivery->terminal() == OperationTerminal::completed &&
            delivery->result() == 7,
        "consume 必须返回唯一 completed 终态");
  check(releases->load(std::memory_order_acquire) == 1 &&
            registry->size() == 0 &&
            !registry->state(key).has_value(),
        "consume 才能释放 tombstone lease 并使旧 key 失效");
}

void claim_and_dispatch_ownership_test() {
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto wakes =
      std::make_shared<std::atomic<std::size_t>>(0);
  auto registry = std::make_shared<Registry>();

  const auto dropped_complete_claim = registry->create(
      tracking_lease("claim-complete", releases),
      counting_wake(wakes));
  check(registry->submit(dropped_complete_claim),
        "complete claim drop submit 失败");
  {
    auto claim =
        registry->begin_complete(dropped_complete_claim, 10);
    check(claim.has_value() && claim->valid(),
          "complete claim drop 前必须持有真实交付权");
  }
  check(registry->state(dropped_complete_claim) ==
                OperationState::delivered &&
            wakes->load(std::memory_order_acquire) == 1,
        "complete claim 析构必须自动发布并执行唯一 wake");
  const auto completed =
      registry->consume(dropped_complete_claim);
  check(completed.has_value() &&
            completed->terminal() ==
                OperationTerminal::completed &&
            completed->result() == 10 &&
            releases->load(std::memory_order_acquire) == 1,
        "complete claim drop 后终态必须可消费且恰好释放");

  const auto dropped_cancel_claim = registry->create(
      tracking_lease("claim-cancel", releases),
      counting_wake(wakes));
  check(registry->submit(dropped_cancel_claim),
        "cancel claim drop submit 失败");
  {
    auto claim =
        registry->begin_cancel(dropped_cancel_claim, -10);
    check(claim.has_value() && claim->valid(),
          "cancel claim drop 前必须持有真实交付权");
  }
  check(registry->state(dropped_cancel_claim) ==
                OperationState::delivered &&
            wakes->load(std::memory_order_acquire) == 2,
        "cancel claim 析构必须自动发布并执行唯一 wake");
  const auto cancelled =
      registry->consume(dropped_cancel_claim);
  check(cancelled.has_value() &&
            cancelled->terminal() ==
                OperationTerminal::cancelled &&
            releases->load(std::memory_order_acquire) == 1,
        "cancel claim drop consume 必须等待 native terminal 才释放");
  check(!registry->complete(dropped_cancel_claim, 10)
              .has_value() &&
            releases->load(std::memory_order_acquire) == 2 &&
            !registry->state(dropped_cancel_claim).has_value(),
        "cancel claim drop 后 late completion 必须完成 native settle");

  const auto dropped_dispatch = registry->create(
      tracking_lease("dispatch-drop", releases),
      counting_wake(wakes));
  check(registry->submit(dropped_dispatch),
        "dispatch drop submit 失败");
  {
    auto dispatch =
        registry->complete(dropped_dispatch, 20);
    check(dispatch.has_value() &&
              wakes->load(std::memory_order_acquire) == 2,
          "dispatch 析构前不得提前执行 wake");
  }
  check(wakes->load(std::memory_order_acquire) == 3 &&
            registry->consume(dropped_dispatch).has_value() &&
            releases->load(std::memory_order_acquire) == 3,
        "pending dispatch 析构必须结构性补发唯一 wake");

  const auto moved_dispatch = registry->create(
      tracking_lease("dispatch-move", releases),
      counting_wake(wakes));
  check(registry->submit(moved_dispatch),
        "dispatch move submit 失败");
  auto first_dispatch =
      registry->complete(moved_dispatch, 30);
  check(first_dispatch.has_value() &&
            !registry->can_drain(),
        "pending dispatch 必须作为 shutdown in-flight");
  Registry::OperationDispatch second_dispatch{
      std::move(*first_dispatch)};
  Registry::OperationDispatch final_dispatch{
      std::move(second_dispatch)};
  check(!first_dispatch->run() &&
            !second_dispatch.run() &&
            final_dispatch.run() &&
            !final_dispatch.run() &&
            wakes->load(std::memory_order_acquire) == 4 &&
            registry->can_drain(),
        "dispatch move 前后只能由最终 owner 执行一次 wake");
  check(registry->consume(moved_dispatch).has_value() &&
            releases->load(std::memory_order_acquire) == 4,
        "dispatch move operation 回收失败");

  const auto discarded_dispatch = registry->create(
      tracking_lease("dispatch-discard", releases),
      counting_wake(wakes));
  check(registry->submit(discarded_dispatch),
        "dispatch shutdown discard submit 失败");
  {
    auto dispatch =
        registry->complete(discarded_dispatch, 40);
    check(dispatch.has_value() &&
              !registry->can_drain() &&
              dispatch->discard_for_shutdown() &&
              registry->can_drain() &&
              !dispatch->discard_for_shutdown() &&
              !dispatch->run(),
          "discard_for_shutdown 必须只抑制一次 pending wake");
  }
  check(wakes->load(std::memory_order_acquire) == 4 &&
            registry->consume(discarded_dispatch).has_value() &&
            releases->load(std::memory_order_acquire) == 5,
        "显式 shutdown discard 不得在析构时补发 wake");

  const auto drained_claim = registry->create(
      tracking_lease("claim-drain", releases),
      counting_wake(wakes));
  check(registry->submit(drained_claim),
        "claim drain submit 失败");
  auto claim = registry->begin_complete(drained_claim, 50);
  check(claim.has_value() && claim->valid(),
        "drain 前 claim 应真实有效");
  check(registry->drain() == 1 &&
            !claim->valid() &&
            !claim->abandon() &&
            wakes->load(std::memory_order_acquire) == 4 &&
            releases->load(std::memory_order_acquire) == 6,
        "drain 后 nonce 旋转必须使 claim 真实失效且不得执行 wake");
}

void dispatch_drain_handshake_test() {
  auto registry = std::make_shared<Registry>();
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto enqueues =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto enqueue_thread =
      std::make_shared<std::thread::id>();
  const auto target_continuation_runs =
      std::make_shared<std::atomic<std::size_t>>(0);
  auto wake_state = std::make_shared<ProbeWakeState>();
  wake_state->registry = registry;
  wake_state->wake_count = enqueues;
  wake_state->enqueue_thread = enqueue_thread;
  wake_state->target_continuation_runs =
      target_continuation_runs;

  const auto key = registry->create(
      tracking_lease("dispatch-drain-race", releases),
      ProbeWake{std::move(wake_state)});
  check(registry->submit(key),
        "dispatch-drain 竞态 submit 失败");
  auto dispatch = registry->complete(key, 60);
  check(dispatch.has_value() &&
            !registry->can_drain(),
        "pending dispatch ack 前必须硬阻止 drain");

  const auto drained =
      std::make_shared<std::atomic<std::size_t>>(0);
  std::thread drainer{
      [registry, drained] {
        while (!registry->can_drain()) {
          std::this_thread::yield();
        }
        drained->store(
            registry->drain(),
            std::memory_order_release);
      }};
  std::thread driver{
      [owned_dispatch = std::move(dispatch)]() mutable {
        // 模拟 driver 线程丢弃最终 dispatch owner；析构只能 enqueue，不能直接
        // resume/poll 目标 continuation。
        owned_dispatch.reset();
      }};
  const auto driver_id = driver.get_id();
  driver.join();
  drainer.join();

  check(drained->load(std::memory_order_acquire) == 1 &&
            enqueues->load(std::memory_order_acquire) == 1 &&
            *enqueue_thread == driver_id &&
            *enqueue_thread != std::this_thread::get_id() &&
            target_continuation_runs->load(
                std::memory_order_acquire) == 0 &&
            releases->load(std::memory_order_acquire) == 1 &&
            registry->size() == 0 &&
            registry->can_drain(),
        "dispatch 必须先在 driver 线程仅记录 enqueue、析构 owning wake、"
        "ack 一次，随后并发 drain 才能安全回收");
}

void drain_native_safety_test() {
  Registry registry;
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  check(registry.can_drain(),
        "空 registry 必须允许安全 drain");

  (void)registry.create(
      tracking_lease("drain-created", releases));
  check(registry.can_drain() &&
            registry.drain() == 1 &&
            releases->load(std::memory_order_acquire) == 1,
        "created 从未提交给 driver，必须允许安全回收");

  const auto submitted = registry.create(
      tracking_lease("drain-submitted", releases));
  check(registry.submit(submitted) &&
            !registry.can_drain(),
        "submitted native in-flight record 必须阻止 drain");
  auto submitted_dispatch =
      registry.complete(submitted, 1);
  check(submitted_dispatch.has_value() &&
            !registry.can_drain() &&
            submitted_dispatch->run() &&
            registry.can_drain() &&
            registry.consume(submitted).has_value() &&
            releases->load(std::memory_order_acquire) == 2,
        "completed native terminal 必须解除 drain 门槛");

  const auto cancelling = registry.create(
      tracking_lease("drain-cancelling", releases));
  check(registry.submit(cancelling),
        "drain cancelling submit 失败");
  auto cancelling_claim =
      registry.begin_cancel(cancelling, -2);
  check(cancelling_claim.has_value() &&
            !registry.can_drain() &&
            registry.settle_native(cancelling) &&
            registry.can_drain(),
        "cancelling 必须等待显式 native settle 才允许 drain");
  auto cancelling_dispatch =
      cancelling_claim->deliver();
  check(cancelling_dispatch.has_value() &&
            cancelling_dispatch->run() &&
            registry.consume(cancelling).has_value() &&
            releases->load(std::memory_order_acquire) == 3,
        "native-settled cancelling record 回收失败");

  const auto delivered_cancel = registry.create(
      tracking_lease("drain-delivered-cancel", releases));
  check(registry.submit(delivered_cancel),
        "drain delivered cancel submit 失败");
  auto cancel_dispatch =
      registry.cancel(delivered_cancel, -3);
  check(cancel_dispatch.has_value() &&
            cancel_dispatch->run() &&
            !registry.can_drain() &&
            registry.consume(delivered_cancel).has_value() &&
            !registry.can_drain() &&
            registry.settle_native(delivered_cancel) &&
            registry.can_drain() &&
            releases->load(std::memory_order_acquire) == 4,
        "delivered/consumed cancel 都不得绕过 native settle 门槛");

  const auto completing = registry.create(
      tracking_lease("drain-completing", releases));
  check(registry.submit(completing),
        "drain completing submit 失败");
  auto completing_claim =
      registry.begin_complete(completing, 4);
  check(completing_claim.has_value() &&
            completing_claim->valid() &&
            registry.can_drain(),
        "begin_complete 只能在 native terminal 后调用，必须天然 settled");
  check(completing_claim->abandon() &&
            registry.consume(completing).has_value() &&
            releases->load(std::memory_order_acquire) == 5 &&
            registry.can_drain() &&
            registry.drain() == 0,
        "所有 native-safe record 回收后 drain 必须为空");
}

void stale_key_and_aba_test() {
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  Registry registry;

  const auto old_key =
      registry.create(tracking_lease("old", releases));
  check(registry.submit(old_key), "旧 operation submit 失败");
  auto old_dispatch = registry.complete(old_key, 1);
  check(old_dispatch.has_value() && old_dispatch->run(),
        "旧 operation complete/deliver 失败");
  check(registry.consume(old_key).has_value(),
        "旧 operation consume 失败");

  const auto new_key =
      registry.create(tracking_lease("new", releases));
  check(new_key.slot() == old_key.slot() &&
            new_key.generation() != old_key.generation(),
        "槽位复用必须推进 generation");
  check(!registry.submit(old_key) &&
            !registry.cancel(old_key, -1).has_value() &&
            !registry.consume(old_key).has_value(),
        "stale key 不得命中新 generation record");
  check(registry.state(new_key) == OperationState::created,
        "stale key 尝试不得改变新 record");

  Registry other;
  check(!other.submit(new_key) &&
            new_key.registry_nonce() !=
                other.create(
                    tracking_lease("other", releases))
                    .registry_nonce(),
        "OperationKey 不得跨 registry 使用");
  check(registry.drain() == 1,
        "drain 必须释放尚未提交的新 record");
  check(other.drain() == 1,
        "第二 registry drain 数量错误");
  check(releases->load(std::memory_order_acquire) == 3,
        "ABA 测试所有 lease 必须恰好释放一次");
}

void complete_cancel_race_test() {
  constexpr std::size_t rounds = 1000;
  const auto registry = std::make_shared<Registry>();
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto wakes =
      std::make_shared<std::atomic<std::size_t>>(0);

  for (std::size_t round = 0; round < rounds; ++round) {
    const auto key = registry->create(
        tracking_lease("race", releases),
        counting_wake(wakes));
    check(registry->submit(key),
          "竞态 operation submit 失败");

    const auto ready =
        std::make_shared<std::atomic<std::size_t>>(0);
    const auto start =
        std::make_shared<std::atomic<bool>>(false);
    const auto winners =
        std::make_shared<std::atomic<std::size_t>>(0);

    std::thread completer{
        [registry, key, ready, start, winners] {
          ready->fetch_add(1, std::memory_order_release);
          while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          auto dispatch = registry->complete(key, 11);
          if (dispatch) {
            winners->fetch_add(1, std::memory_order_relaxed);
            check(dispatch->run(),
                  "complete 胜者 dispatch 失败");
          }
        }};
    std::thread canceller{
        [registry, key, ready, start, winners] {
          ready->fetch_add(1, std::memory_order_release);
          while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          auto dispatch = registry->cancel(key, -11);
          if (dispatch) {
            winners->fetch_add(1, std::memory_order_relaxed);
            check(dispatch->run(),
                  "cancel 胜者 dispatch 失败");
          }
        }};

    while (ready->load(std::memory_order_acquire) != 2) {
      std::this_thread::yield();
    }
    start->store(true, std::memory_order_release);
    completer.join();
    canceller.join();

    check(winners->load(std::memory_order_acquire) == 1 &&
              registry->state(key) == OperationState::delivered,
          "complete-vs-cancel 必须恰好一个 delivered 胜者");
    const auto delivery = registry->consume(key);
    check(delivery.has_value() &&
              ((delivery->terminal() ==
                    OperationTerminal::completed &&
                delivery->result() == 11) ||
               (delivery->terminal() ==
                    OperationTerminal::cancelled &&
                delivery->result() == -11)),
          "竞态终态类型和值必须与胜者一致");
    check(!registry->state(key).has_value(),
          "竞态 loser completion 已完成 native handshake，consume 后必须回收");
  }

  check(wakes->load(std::memory_order_acquire) == rounds &&
            releases->load(std::memory_order_acquire) == rounds &&
            registry->size() == 0,
        "竞态压力后 wake/lease/record 计数不守恒");
}

void native_terminal_handshake_test() {
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  Registry registry;

  const auto consume_first =
      registry.create(tracking_lease("consume-first", releases));
  check(registry.submit(consume_first),
        "consume-before-late operation submit 失败");
  auto consume_first_cancel =
      registry.cancel(consume_first, -1);
  check(consume_first_cancel.has_value() &&
            consume_first_cancel->run(),
        "consume-before-late cancel 交付失败");
  const auto consume_first_delivery =
      registry.consume(consume_first);
  check(consume_first_delivery.has_value() &&
            consume_first_delivery->terminal() ==
                OperationTerminal::cancelled &&
            releases->load(std::memory_order_acquire) == 0 &&
            registry.size() == 1 &&
            registry.state(consume_first) ==
                OperationState::delivered &&
            !registry.consume(consume_first).has_value(),
        "cancelled consume 不得在 native terminal 前释放或重复交付");

  check(!registry.complete(consume_first, 1).has_value() &&
            releases->load(std::memory_order_acquire) == 1 &&
            registry.size() == 0 &&
            !registry.state(consume_first).has_value() &&
            !registry.settle_native(consume_first),
        "consume 后 late completion 必须只完成 native handshake 并恰好释放一次");
  const auto reused =
      registry.create(tracking_lease("reused", releases));
  check(reused.slot() == consume_first.slot() &&
            reused.generation() != consume_first.generation(),
        "generation 只能在 consume 与 native settled 都完成后推进");
  check(registry.drain() == 1 &&
            releases->load(std::memory_order_acquire) == 2,
        "复用槽位 drain 释放错误");

  const auto late_first =
      registry.create(tracking_lease("late-first", releases));
  check(registry.submit(late_first),
        "late-before-consume operation submit 失败");
  auto late_first_cancel = registry.cancel(late_first, -2);
  check(late_first_cancel.has_value() &&
            late_first_cancel->run() &&
            !registry.complete(late_first, 2).has_value() &&
            releases->load(std::memory_order_acquire) == 2 &&
            registry.size() == 1 &&
            registry.state(late_first) ==
                OperationState::delivered,
        "consume 前 late completion 必须标记 settled 但继续保留 tombstone");
  const auto late_first_delivery = registry.consume(late_first);
  check(late_first_delivery.has_value() &&
            late_first_delivery->terminal() ==
                OperationTerminal::cancelled &&
            releases->load(std::memory_order_acquire) == 3 &&
            registry.size() == 0 &&
            !registry.state(late_first).has_value(),
        "late completion 在先时 consume 必须完成唯一回收");

  const auto readiness =
      registry.create(tracking_lease("readiness", releases));
  check(registry.submit(readiness),
        "readiness 撤销 operation submit 失败");
  auto readiness_cancel = registry.cancel(readiness, -3);
  check(readiness_cancel.has_value() &&
            readiness_cancel->run() &&
            registry.consume(readiness).has_value() &&
            releases->load(std::memory_order_acquire) == 3,
        "readiness cancel consume 不得提前释放 lease");
  check(registry.settle_native(readiness) &&
            !registry.settle_native(readiness) &&
            releases->load(std::memory_order_acquire) == 4 &&
            registry.size() == 0,
        "readiness 同步撤销必须显式确认且只释放一次");

  const auto settled_first =
      registry.create(tracking_lease("settled-first", releases));
  check(registry.submit(settled_first),
        "settled-before-consume operation submit 失败");
  auto settled_first_cancel =
      registry.cancel(settled_first, -4);
  check(settled_first_cancel.has_value() &&
            settled_first_cancel->run() &&
            registry.settle_native(settled_first) &&
            releases->load(std::memory_order_acquire) == 4 &&
            registry.size() == 1,
        "native settled 在先时必须等待用户 consume");
  check(registry.consume(settled_first).has_value() &&
            releases->load(std::memory_order_acquire) == 5 &&
            registry.size() == 0,
        "native settled 在先时 consume 必须完成唯一回收");

  constexpr std::size_t handshake_rounds = 500;
  const auto race_registry = std::make_shared<Registry>();
  const auto race_releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  for (std::size_t round = 0; round < handshake_rounds; ++round) {
    const auto key = race_registry->create(
        tracking_lease("handshake-race", race_releases));
    check(race_registry->submit(key),
          "handshake 竞态 operation submit 失败");
    auto dispatch = race_registry->cancel(key, -6);
    check(dispatch.has_value() && dispatch->run(),
          "handshake 竞态 cancel 交付失败");

    const auto ready =
        std::make_shared<std::atomic<std::size_t>>(0);
    const auto start =
        std::make_shared<std::atomic<bool>>(false);
    const auto consumed =
        std::make_shared<std::atomic<bool>>(false);
    const auto settled =
        std::make_shared<std::atomic<bool>>(false);
    std::thread consumer{
        [race_registry, key, ready, start, consumed] {
          ready->fetch_add(1, std::memory_order_release);
          while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          const auto delivery = race_registry->consume(key);
          consumed->store(
              delivery.has_value() &&
                  delivery->terminal() ==
                      OperationTerminal::cancelled,
              std::memory_order_release);
        }};
    std::thread native_settler{
        [race_registry, key, ready, start, settled] {
          ready->fetch_add(1, std::memory_order_release);
          while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          settled->store(
              race_registry->settle_native(key),
              std::memory_order_release);
        }};
    while (ready->load(std::memory_order_acquire) != 2) {
      std::this_thread::yield();
    }
    start->store(true, std::memory_order_release);
    consumer.join();
    native_settler.join();
    check(consumed->load(std::memory_order_acquire) &&
              settled->load(std::memory_order_acquire) &&
              !race_registry->state(key).has_value(),
          "consume-vs-native-settled 竞态必须两边各成功一次并回收");
  }
  check(race_releases->load(std::memory_order_acquire) ==
                handshake_rounds &&
            race_registry->size() == 0,
        "handshake 竞态 lease 必须恰好释放一次");
}

void registry_destruction_and_claim_invalidation_test() {
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  std::optional<Registry::DeliveryClaim> claim;

  {
    auto registry = std::make_unique<Registry>();
    const auto key =
        registry->create(tracking_lease("destroy", releases));
    check(registry->submit(key), "析构测试 submit 失败");
    auto acquired = registry->begin_complete(key, 9);
    check(acquired.has_value() &&
              registry->state(key) == OperationState::completing &&
              registry->can_drain(),
          "completed claim 只有 native-settled 后才允许析构");
    claim.emplace(std::move(*acquired));
    registry.reset();
  }

  check(releases->load(std::memory_order_acquire) == 1,
        "registry 析构必须在锁外释放未消费 record lease");
  check(claim.has_value() && !claim->deliver().has_value(),
        "registry 析构后旧 claim 必须失效");

  {
    Registry registry;
    (void)registry.create(tracking_lease("created", releases));
  }
  check(releases->load(std::memory_order_acquire) == 2,
        "registry 析构必须释放 created record");
}

void retirement_capacity_invariant_test() {
  constexpr std::size_t slot_count = 2048;
  Registry registry;
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  std::vector<OperationKey> first_generation;
  first_generation.reserve(slot_count);

  // 同时保留全部 record，强制 create 连续扩展 slots；每个新增 slot 发布前都必须
  // 为其未来 retire 预留 free-list 容量。
  for (std::size_t index = 0; index < slot_count; ++index) {
    const auto key =
        registry.create(tracking_lease("capacity", releases));
    check(registry.submit(key),
          "容量不变量首轮 submit 失败");
    first_generation.push_back(key);
  }
  for (std::size_t index = 0; index < slot_count; ++index) {
    auto dispatch = registry.complete(
        first_generation[index],
        static_cast<int>(index));
    check(dispatch.has_value() && dispatch->run() &&
              registry.consume(first_generation[index]).has_value(),
          "容量不变量首轮 retire 失败");
  }
  check(registry.size() == 0 &&
            releases->load(std::memory_order_acquire) ==
                slot_count,
        "容量不变量首轮未恰好回收全部 record");

  // 清空 free-list 以复用全部 slot，再以两种取消握手顺序回收。retire_locked 内部
  // 的容量守卫会在任意 push_back 需要扩容时终止测试。
  std::vector<OperationKey> reused;
  reused.reserve(slot_count);
  for (std::size_t index = 0; index < slot_count; ++index) {
    const auto key =
        registry.create(tracking_lease("reuse", releases));
    check(key.slot() ==
                  static_cast<std::uint32_t>(
                      slot_count - index - 1) &&
              key.generation() !=
                  first_generation[key.slot()].generation() &&
              registry.submit(key),
          "容量不变量复用 slot/generation 或 submit 错误");
    auto dispatch = registry.cancel(
        key,
        -static_cast<int>(index));
    check(dispatch.has_value() && dispatch->run(),
          "容量不变量复用 cancel 失败");
    reused.push_back(key);
  }
  for (std::size_t index = 0; index < slot_count; ++index) {
    if ((index & 1U) == 0) {
      check(registry.consume(reused[index]).has_value() &&
                registry.settle_native(reused[index]),
            "容量不变量 consume-first 回收失败");
    } else {
      check(registry.settle_native(reused[index]) &&
                registry.consume(reused[index]).has_value(),
            "容量不变量 settle-first 回收失败");
    }
  }
  check(registry.size() == 0 &&
            releases->load(std::memory_order_acquire) ==
                slot_count * 2,
        "容量不变量复用轮未恰好回收全部 record");
}

void parallel_registry_pressure_test() {
  constexpr std::size_t operation_count = 4096;
  constexpr std::size_t thread_count = 8;
  const auto registry = std::make_shared<Registry>();
  const auto releases =
      std::make_shared<std::atomic<std::size_t>>(0);
  const auto wakes =
      std::make_shared<std::atomic<std::size_t>>(0);
  auto keys = std::make_shared<std::vector<OperationKey>>();
  keys->reserve(operation_count);

  for (std::size_t index = 0; index < operation_count; ++index) {
    const auto key = registry->create(
        tracking_lease("load", releases),
        counting_wake(wakes));
    check(registry->submit(key),
          "并行压力 operation submit 失败");
    keys->push_back(key);
  }

  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  for (std::size_t worker = 0; worker < thread_count; ++worker) {
    workers.emplace_back(
        [registry, keys, worker] {
          for (std::size_t index = worker;
               index < keys->size();
               index += thread_count) {
            if ((index & 1U) == 0) {
              auto dispatch = registry->complete(
                  keys->at(index),
                  static_cast<int>(index));
              check(dispatch.has_value() && dispatch->run(),
                    "并行压力 complete 交付失败");
            } else {
              auto dispatch = registry->cancel(
                  keys->at(index),
                  -static_cast<int>(index));
              check(dispatch.has_value() && dispatch->run(),
                    "并行压力 cancel 交付失败");
            }
          }
        });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  for (std::size_t index = 0; index < operation_count; ++index) {
    const auto delivery = registry->consume(keys->at(index));
    const auto expected_terminal =
        (index & 1U) == 0
        ? OperationTerminal::completed
        : OperationTerminal::cancelled;
    const auto expected_result =
        (index & 1U) == 0
        ? static_cast<int>(index)
        : -static_cast<int>(index);
    check(delivery.has_value() &&
              delivery->terminal() == expected_terminal &&
              delivery->result() == expected_result,
          "并行压力 consume 结果错误");
    if (expected_terminal == OperationTerminal::cancelled) {
      check(registry->settle_native(keys->at(index)),
            "并行压力 cancelled record 未完成 native handshake");
    }
  }

  check(wakes->load(std::memory_order_acquire) ==
                operation_count &&
            releases->load(std::memory_order_acquire) ==
                operation_count &&
            registry->size() == 0,
        "并行压力后 wake、lease 或 registry 数量不守恒");
}

static_assert(
    std::is_nothrow_move_constructible_v<TrackingLease>);
static_assert(!std::is_copy_constructible_v<TrackingLease>);
static_assert(cio::io::OperationOwnedValue<TrackingLease>);
static_assert(cio::io::OperationWake<
              cio::io::NoopOperationWake>);
static_assert(cio::io::OperationWake<ProbeWake>);
static_assert(RegistryFormable<TrackingLease, int>);
static_assert(RegistryWithWakeFormable<
              TrackingLease,
              int,
              ProbeWake>);
static_assert(!RegistryFormable<int&, int>);
static_assert(!RegistryFormable<int*, int>);
static_assert(!RegistryFormable<
              std::span<const std::byte>,
              int>);
static_assert(!RegistryFormable<
              std::reference_wrapper<int>,
              int>);
static_assert(!RegistryFormable<TrackingLease, int&>);
static_assert(!RegistryFormable<TrackingLease, int*>);
static_assert(!RegistryFormable<
              TrackingLease,
              std::span<const std::byte>>);
static_assert(!RegistryFormable<
              TrackingLease,
              std::reference_wrapper<int>>);
static_assert(!cio::io::OperationWake<ThrowingWake>);
static_assert(!cio::io::OperationWake<NotEnqueueOnlyWake>);
static_assert(!cio::io::OperationWake<UnmarkedWake>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              ThrowingWake>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              NotEnqueueOnlyWake>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              UnmarkedWake>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              ProbeWake&>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              ProbeWake*>);
static_assert(!RegistryWithWakeFormable<
              TrackingLease,
              int,
              std::reference_wrapper<ProbeWake>>);
static_assert(!std::is_copy_constructible_v<Registry>);
static_assert(!std::is_copy_constructible_v<
              Registry::DeliveryClaim>);
static_assert(!std::is_copy_constructible_v<
              Registry::OperationDispatch>);
static_assert(std::is_nothrow_destructible_v<
              Registry::OperationDispatch>);
static_assert(noexcept(
    std::declval<Registry::OperationDispatch&>().run()));
static_assert(noexcept(
    std::declval<Registry::OperationDispatch&>()
        .discard_for_shutdown()));

}  // namespace

int main() {
  try {
    state_machine_and_lock_free_dispatch_test();
    claim_and_dispatch_ownership_test();
    dispatch_drain_handshake_test();
    drain_native_safety_test();
    stale_key_and_aba_test();
    complete_cancel_race_test();
    native_terminal_handshake_test();
    registry_destruction_and_claim_invalidation_test();
    retirement_capacity_invariant_test();
    parallel_registry_pressure_test();
  } catch (const std::exception& error) {
    std::cerr << "[FAIL] io operation: "
              << error.what() << '\n';
    return 1;
  }

  std::cout << "io operation tests passed: 10/10\n";
  return 0;
}
