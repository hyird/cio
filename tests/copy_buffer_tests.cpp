#include <atomic>
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cio/io/detail/copy_buffer.hpp"
#include "cio/io/operation.hpp"
#include "cio/send.hpp"

namespace {

using cio::io::detail::CopyBuffer;
using cio::io::detail::CopyNativeCompletionToken;
using cio::io::detail::CopyNativeTailSubmission;
using cio::io::detail::CopyNativeWritableTailPin;
using cio::io::detail::CopyReadablePrefixLease;
using cio::io::detail::CopyWritableTailLease;

[[nodiscard]] std::byte byte(unsigned int value) {
  return static_cast<std::byte>(value);
}

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

void fill(
    CopyWritableTailLease& tail,
    std::initializer_list<unsigned int> values) {
  std::size_t index = 0;
  for (const auto value : values) {
    tail.write_at(index, byte(value));
    ++index;
  }
}

void fill_native(
    CopyNativeCompletionToken& token,
    std::initializer_list<unsigned int> values) {
  std::size_t index = 0;
  for (const auto value : values) {
    token.write_at(index, byte(value));
    ++index;
  }
}

using NativeRegistry =
    cio::io::OperationRegistry<
        CopyNativeWritableTailPin,
        std::size_t>;

[[noreturn]] void simulate_registry_create_failure(
    CopyNativeWritableTailPin) {
  throw std::runtime_error{"simulated create failure"};
}

void prefix_and_tail_coexist_without_overlap() {
  CopyBuffer buffer;
  auto initial_tail = buffer.lease_writable_tail();
  fill(initial_tail, {1, 2, 3});
  check(initial_tail.commit(3) == 1,
        "首次 commit 必须发布 revision 1");

  auto readable = buffer.lease_readable_prefix();
  auto tail = buffer.lease_writable_tail();
  check(readable.offset() == 0 && readable.size() == 3,
        "readable prefix 范围错误");
  check(tail.offset() == 3 &&
            tail.size() == CopyBuffer::capacity - 3,
        "writable tail 范围错误");
  check(readable.offset() + readable.size() <= tail.offset(),
        "readable prefix 与 writable tail 不得重叠");

  fill(tail, {4, 5});
  check(readable.snapshot() ==
            std::vector<std::byte>{byte(1), byte(2), byte(3)},
        "tail 未提交写入不得改变旧 readable lease");
  check(tail.commit(2) == 2,
        "top-up commit 必须推进 revision");
}

void overlap_and_reuse_are_rejected() {
  CopyBuffer buffer;
  auto tail = buffer.lease_writable_tail();

  bool second_tail_rejected = false;
  try {
    (void)buffer.lease_writable_tail();
  } catch (const std::logic_error&) {
    second_tail_rejected = true;
  }
  check(second_tail_rejected,
        "active tail 期间必须拒绝第二个 writable lease");

  bool prefix_write_rejected = false;
  try {
    tail.write_at(tail.size(), byte(9));
  } catch (const std::out_of_range&) {
    prefix_write_rejected = true;
  }
  check(prefix_write_rejected,
        "tail lease 必须拒绝范围外写入");

  fill(tail, {7, 8});
  (void)tail.commit(2);
  auto completion_lease = buffer.lease_readable_prefix();
  buffer.consume(2);
  check(!buffer.try_recycle_empty(),
        "旧 completion lease 存活时不得复用物理 storage");
  completion_lease = {};
  check(buffer.try_recycle_empty(),
        "全部旧 readable lease 释放后应允许回收");
  check(buffer.epoch() == 2 &&
            buffer.readable_size() == 0 &&
            buffer.writable_size() == CopyBuffer::capacity,
        "回收后 epoch 或区间状态错误");

  auto mixed = buffer.lease_writable_tail();
  mixed.write_at(0, byte(1));
  bool mixed_submit_rejected = false;
  try {
    (void)mixed.submit_native();
  } catch (const std::logic_error&) {
    mixed_submit_rejected = true;
  }
  check(mixed_submit_rejected,
        "manual write 后必须拒绝切换 native submitted");
  mixed.cancel_now();
}

void top_up_revision_keeps_old_completion_alive() {
  CopyBuffer buffer;
  auto first_tail = buffer.lease_writable_tail();
  fill(first_tail, {10, 11, 12});
  (void)first_tail.commit(3);

  auto old_completion = buffer.lease_readable_prefix();
  auto retained_completion = old_completion;
  auto top_up = buffer.lease_writable_tail();
  fill(top_up, {13, 14});
  (void)top_up.commit(2);
  auto expanded = buffer.lease_readable_prefix();

  check(expanded.is_monotonic_extension_of(old_completion),
        "top-up 后的新 writer revision 必须严格扩展旧 revision");
  check(expanded.revision() == 2 && expanded.size() == 5,
        "扩展 lease 的 revision/length 错误");
  check(old_completion.revision() == 1 &&
            old_completion.size() == 3,
        "旧 completion lease 的 revision/length 不得被就地修改");
  check(retained_completion.snapshot() ==
            std::vector<std::byte>{
                byte(10), byte(11), byte(12)},
        "旧 completion lease 必须继续拥有并读取原提交范围");
  check(expanded.snapshot() ==
            std::vector<std::byte>{
                byte(10), byte(11), byte(12),
                byte(13), byte(14)},
        "扩展 writer lease 必须看到完整 top-up prefix");
}

void cancellation_and_destruction_do_not_publish() {
  CopyBuffer buffer;
  {
    auto cancelled = buffer.lease_writable_tail();
    fill(cancelled, {20, 21});
    cancelled.cancel_now();
  }
  check(buffer.revision() == 0 &&
            buffer.readable_size() == 0,
        "显式取消不得发布未提交 tail");

  {
    auto abandoned = buffer.lease_writable_tail();
    fill(abandoned, {22, 23});
  }
  check(buffer.revision() == 0 &&
            buffer.readable_size() == 0,
        "tail 析构不得发布未提交字节");

  auto replacement = buffer.lease_writable_tail();
  fill(replacement, {24, 25});
  (void)replacement.commit(2);
  auto survivor = buffer.lease_readable_prefix();
  {
    CopyBuffer moved{std::move(buffer)};
    check(moved.readable_size() == 2,
          "CopyBuffer move 必须转移 owner");
  }
  check(survivor.snapshot() ==
            std::vector<std::byte>{byte(24), byte(25)},
        "CopyBuffer owner 析构后 readable lease 必须继续保活 storage");
}

void cancelled_native_pin_blocks_reuse_until_release() {
  CopyBuffer buffer;
  auto first = buffer.lease_writable_tail();
  fill(first, {40, 41});
  (void)first.commit(2);
  auto old_prefix = buffer.lease_readable_prefix();

  auto submitted_tail = buffer.lease_writable_tail();
  auto submission = submitted_tail.submit_native();
  auto native_pin = submission.take_registry_pin();
  auto completion = submission.take_completion_token();
  auto submit_guard = submission.begin_submit();
  check(submit_guard.accept(),
        "OS 接受后 submit guard 必须进入 submitted");

  bool manual_write_rejected = false;
  try {
    submitted_tail.write_at(0, byte(42));
  } catch (const std::logic_error&) {
    manual_write_rejected = true;
  }
  check(manual_write_rejected,
        "native submitted 后必须拒绝 manual write");

  bool manual_commit_rejected = false;
  try {
    (void)submitted_tail.commit(0);
  } catch (const std::logic_error&) {
    manual_commit_rejected = true;
  }
  check(manual_commit_rejected,
        "native submitted 后必须拒绝 manual commit");

  bool premature_native_commit_rejected = false;
  try {
    (void)submitted_tail.commit_native(0);
  } catch (const std::logic_error&) {
    premature_native_commit_rejected = true;
  }
  check(premature_native_commit_rejected,
        "pin settle 前必须拒绝 commit_native");

  submitted_tail.cancel_now();
  buffer.consume(2);

  bool active_pin_rejected_tail = false;
  try {
    (void)buffer.lease_writable_tail();
  } catch (const std::logic_error&) {
    active_pin_rejected_tail = true;
  }
  check(active_pin_rejected_tail,
        "逻辑 cancel 后 active native pin 必须阻止新 tail");
  check(!buffer.try_recycle_empty(),
        "active native pin 必须阻止 storage recycle");

  // 模拟 cancel 后才到达的原生完成写；固定 tail 仍由 pin 保活且不覆盖旧 prefix。
  fill_native(completion, {42, 43, 44});
  check(old_prefix.snapshot() ==
            std::vector<std::byte>{byte(40), byte(41)},
        "late native write 不得覆盖旧 readable prefix");
  check(completion.settle_native(3),
        "late completion 必须发布 bytes_transferred");

  bool settled_write_rejected = false;
  try {
    completion.write_at(0, byte(99));
  } catch (const std::logic_error&) {
    settled_write_rejected = true;
  }
  check(settled_write_rejected,
        "settled native pin 必须拒绝后续写入");

  bool settled_pin_rejected_tail = false;
  try {
    (void)buffer.lease_writable_tail();
  } catch (const std::logic_error&) {
    settled_pin_rejected_tail = true;
  }
  check(settled_pin_rejected_tail,
        "settled 但未释放的 native pin 仍必须阻止新 tail");

  native_pin = CopyNativeWritableTailPin{};
  auto next_tail = buffer.lease_writable_tail();
  check(next_tail.offset() == 2,
        "native pin 释放后才应允许发放下一 tail");
  next_tail.cancel_now();
}

void cross_thread_native_commit_is_visible() {
  CopyBuffer buffer;
  auto first = buffer.lease_writable_tail();
  fill(first, {30, 31});
  (void)first.commit(2);
  auto old_completion = buffer.lease_readable_prefix();

  auto tail = buffer.lease_writable_tail();
  auto submission = tail.submit_native();
  auto native_pin = submission.take_registry_pin();
  auto completion = submission.take_completion_token();
  auto submit_guard = submission.begin_submit();
  check(submit_guard.accept(),
        "native submit guard 必须确认 accepted");
  auto published =
      std::make_shared<std::atomic<bool>>(false);
  std::jthread producer{
      [owned_completion = std::move(completion),
       published]() mutable {
        fill_native(owned_completion, {32, 33, 34});
        (void)owned_completion.settle_native(3);
        published->store(true, std::memory_order_release);
      }};

  while (!published->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  check(tail.commit_native(3) == 2,
        "native settle 后 commit_native 必须推进 revision");
  native_pin = CopyNativeWritableTailPin{};
  auto expanded = buffer.lease_readable_prefix();
  producer.join();

  check(expanded.is_monotonic_extension_of(old_completion),
        "跨线程 top-up 必须发布单调扩展 revision");
  check(expanded.snapshot() ==
            std::vector<std::byte>{
                byte(30), byte(31), byte(32),
                byte(33), byte(34)},
        "commit 的 release/acquire 可见性错误");
  check(old_completion.snapshot() ==
            std::vector<std::byte>{byte(30), byte(31)},
        "跨线程 top-up 后旧 completion lease 仍必须稳定");
}

void registry_reject_rollback_and_immediate_completion() {
  CopyBuffer buffer;
  auto tail = buffer.lease_writable_tail();

  // create 后、进入 OS 前的准备失败必须能锁外回滚 record/pin，并让同一逻辑
  // tail 回到可重试状态。
  {
    auto rejected = tail.submit_native();
    auto completion = rejected.take_completion_token();
    NativeRegistry registry;
    const auto key =
        registry.create(rejected.take_registry_pin());
    check(registry.rollback_created(key) &&
              registry.size() == 0 &&
              tail.abort_before_submit(),
          "created rollback 必须释放 pin 且允许同一 tail 重试");
  }

  auto retried = tail.submit_native();
  auto completion = retried.take_completion_token();
  NativeRegistry registry;
  const auto key = registry.create(
      retried.take_registry_pin());
  auto registry_submit = registry.begin_submit(key);
  check(registry_submit.has_value(),
        "registry 必须在进入 OS 前转为 submitting");
  auto buffer_submit = retried.begin_submit();

  // 模拟 IOCP/内联后端在 submit API 返回前就交付 completion。
  fill_native(completion, {70, 71, 72, 73});
  check(completion.settle_native(4),
        "即时 completion 必须先发布 bytes_transferred");
  auto dispatch = registry.complete(key, 4);
  check(dispatch.has_value() && dispatch->run(),
        "submitting record 必须接受即时 completion");
  check(buffer_submit.accept() &&
            registry_submit->accept(),
        "submit 返回 accepted 后必须收口两侧 handshake");

  const auto delivery = registry.consume(key);
  check(delivery.has_value() &&
            delivery->terminal() ==
                cio::io::OperationTerminal::completed &&
            delivery->result() == 4 &&
            registry.size() == 0,
        "即时完成必须保留 bytes_transferred 并释放 registry pin");
  check(tail.commit_native(delivery->result()) == 1,
        "即时完成 bytes_transferred 必须驱动 tail commit");
  check(buffer.lease_readable_prefix().snapshot() ==
            std::vector<std::byte>{
                byte(70), byte(71), byte(72), byte(73)},
        "即时完成写入必须在 commit 后可见");
}

void cancel_late_completion_and_submit_reject() {
  {
    CopyBuffer buffer;
    auto tail = buffer.lease_writable_tail();
    auto submission = tail.submit_native();
    auto completion = submission.take_completion_token();
    NativeRegistry registry;
    const auto key =
        registry.create(submission.take_registry_pin());
    auto registry_submit = registry.begin_submit(key);
    auto buffer_submit = submission.begin_submit();
    check(registry_submit.has_value() &&
              buffer_submit.accept() &&
              registry_submit->accept(),
          "accepted cancel 测试提交失败");

    auto cancelled = registry.cancel(key, 0);
    check(cancelled.has_value() && cancelled->run(),
          "cancel 必须先发布唯一上层终态");
    const auto delivery = registry.consume(key);
    check(delivery.has_value() &&
              delivery->terminal() ==
                  cio::io::OperationTerminal::cancelled &&
              registry.size() == 1,
          "late completion 前 cancelled tombstone 必须继续持有 pin");
    tail.cancel_now();

    fill_native(completion, {80, 81, 82});
    check(completion.settle_native(3) &&
              !registry.complete(key, 3).has_value() &&
              registry.size() == 0,
          "cancel 后迟到 completion 必须只 settle native pin");
    auto next = buffer.lease_writable_tail();
    check(next.offset() == 0,
          "late completion 回收后才允许重新发放 tail");
    next.cancel_now();
  }

  {
    // cancel 在 submitting 阶段先赢，而 OS 明确同步拒绝：两侧 reject 必须把
    // cancelled tombstone 视为 native-settled，不留下永久 pin。
    CopyBuffer buffer;
    auto tail = buffer.lease_writable_tail();
    auto submission = tail.submit_native();
    auto completion = submission.take_completion_token();
    NativeRegistry registry;
    const auto key =
        registry.create(submission.take_registry_pin());
    auto registry_submit = registry.begin_submit(key);
    auto buffer_submit = submission.begin_submit();
    check(registry_submit.has_value(),
          "cancel-during-submitting begin 失败");
    auto cancelled = registry.cancel(key, 0);
    check(cancelled.has_value() && cancelled->run() &&
              registry.consume(key).has_value(),
          "submitting cancel 必须可交付");
    check(buffer_submit.reject() &&
              registry_submit->reject() &&
              registry.size() == 0 &&
              tail.abort_before_submit(),
          "同步 reject 必须收口 cancel tombstone 并允许 tail 重试");
  }
}

static_assert(CopyBuffer::capacity == 8192);
static_assert(cio::Send<CopyBuffer>);
static_assert(cio::Send<CopyReadablePrefixLease>);
static_assert(cio::Sync<CopyReadablePrefixLease>);
static_assert(!cio::Send<CopyWritableTailLease>);
static_assert(!cio::Sync<CopyWritableTailLease>);
static_assert(cio::Send<CopyNativeWritableTailPin>);
static_assert(!cio::Sync<CopyNativeWritableTailPin>);
static_assert(!std::copy_constructible<CopyBuffer>);
static_assert(!std::copy_constructible<CopyWritableTailLease>);
static_assert(!std::copy_constructible<CopyNativeWritableTailPin>);
static_assert(!std::is_move_assignable_v<
              CopyNativeCompletionToken>);
static_assert(std::copy_constructible<CopyReadablePrefixLease>);

}  // namespace

int main() {
  try {
    prefix_and_tail_coexist_without_overlap();
    overlap_and_reuse_are_rejected();
    top_up_revision_keeps_old_completion_alive();
    cancellation_and_destruction_do_not_publish();
    cancelled_native_pin_blocks_reuse_until_release();
    cross_thread_native_commit_is_visible();
    registry_reject_rollback_and_immediate_completion();
    cancel_late_completion_and_submit_reject();
  } catch (const std::exception& error) {
    std::cerr << "[FAIL] copy buffer: "
              << error.what() << '\n';
    return 1;
  }

  std::cout << "copy buffer tests passed: 8/8\n";
  return 0;
}
