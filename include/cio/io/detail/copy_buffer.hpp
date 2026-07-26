#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cio::io::detail {

inline constexpr std::size_t copy_buffer_capacity{8192};

namespace copy_buffer_detail {

template <typename Integer>
Integer advance_nonzero(std::atomic<Integer>& value) noexcept {
  auto current = value.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<Integer>::max()) {
      std::terminate();
    }
    if (value.compare_exchange_weak(
            current,
            static_cast<Integer>(current + 1),
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return static_cast<Integer>(current + 1);
    }
  }
}

inline std::atomic<std::uint64_t> next_owner_id{0};

/**
 * CopyBuffer 的共享 storage 与最小跨线程 gate。
 *
 * epoch/revision/position/capacity_end/tail generation 只由拥有 copy Future 的
 * parent task 访问；task 跨 worker 迁移依赖 scheduler 的 release/acquire 边。
 * completion 线程只访问互不重叠的 bytes 区间以及两个 atomic pin 计数。
 */
struct CopyBufferState final {
  CopyBufferState()
      : owner_id{advance_nonzero(next_owner_id)} {}

  std::array<std::byte, copy_buffer_capacity> bytes{};
  const std::uint64_t owner_id{0};

  // 以下字段只属于串行 parent poll，不允许外部线程并发访问。
  std::uint64_t epoch{1};
  std::uint64_t revision{0};
  std::uint64_t next_tail_generation{0};
  std::uint64_t active_tail_generation{0};
  std::size_t position{0};
  std::size_t capacity_end{0};
  bool tail_active{false};

  // lease/pin 最后一个拥有者可能在任意 completion worker 上释放。
  std::atomic<std::size_t> readable_pins{0};
  std::atomic<std::size_t> native_writable_pins{0};
};

inline void acquire_pin(
    std::atomic<std::size_t>& pins) noexcept {
  auto current = pins.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<std::size_t>::max()) {
      std::terminate();
    }
    if (pins.compare_exchange_weak(
            current,
            current + 1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
}

inline void release_pin(
    std::atomic<std::size_t>& pins) noexcept {
  const auto previous =
      pins.fetch_sub(1, std::memory_order_acq_rel);
  if (previous == 0) {
    std::terminate();
  }
}

class ReadableLeaseRegistration final {
 public:
  explicit ReadableLeaseRegistration(
      std::shared_ptr<CopyBufferState> owner) noexcept
      : owner_{std::move(owner)} {
    acquire_pin(owner_->readable_pins);
  }

  ReadableLeaseRegistration(
      const ReadableLeaseRegistration&) = delete;
  ReadableLeaseRegistration& operator=(
      const ReadableLeaseRegistration&) = delete;

  ~ReadableLeaseRegistration() {
    release_pin(owner_->readable_pins);
  }

  [[nodiscard]] const std::shared_ptr<CopyBufferState>&
  owner() const noexcept {
    return owner_;
  }

 private:
  std::shared_ptr<CopyBufferState> owner_;
};

enum class NativePinPhase : std::uint8_t {
  prepared,
  submitting,
  submitted,
  completing_before_accept,
  completing,
  settled_before_accept,
  settled,
  rolled_back,
};

struct NativeTailControl final {
  NativeTailControl(
      std::shared_ptr<CopyBufferState> buffer_owner,
      std::uint64_t buffer_epoch,
      std::uint64_t tail_generation,
      std::size_t tail_offset,
      std::size_t tail_length) noexcept
      : owner{std::move(buffer_owner)},
        epoch{buffer_epoch},
        generation{tail_generation},
        offset{tail_offset},
        length{tail_length} {}

  std::shared_ptr<CopyBufferState> owner;
  const std::uint64_t epoch{0};
  const std::uint64_t generation{0};
  const std::size_t offset{0};
  const std::size_t length{0};
  std::atomic<NativePinPhase> phase{NativePinPhase::prepared};
  std::atomic<std::size_t> initialized_prefix{0};
};

}  // namespace copy_buffer_detail

class CopyBuffer;
class CopyWritableTailLease;
class CopyNativeTailSubmission;

/**
 * writer 或 completion record 持有的只读前缀租约。
 *
 * lease 以 owner id、epoch、revision、offset 和 length 表达，不保存裸地址或
 * span。复制 lease 共享 registration；最后一个副本以 release 发布完成，
 * parent 回收 storage 前以 acquire 检查 readable pin 已归零。
 */
class CopyReadablePrefixLease final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;
  static constexpr bool cio_poll_native_owned_value = true;

  CopyReadablePrefixLease() noexcept = default;
  CopyReadablePrefixLease(
      const CopyReadablePrefixLease&) noexcept = default;
  CopyReadablePrefixLease& operator=(
      const CopyReadablePrefixLease&) noexcept = default;
  CopyReadablePrefixLease(
      CopyReadablePrefixLease&&) noexcept = default;
  CopyReadablePrefixLease& operator=(
      CopyReadablePrefixLease&&) noexcept = default;
  ~CopyReadablePrefixLease() = default;

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(registration_);
  }

  [[nodiscard]] std::uint64_t owner_id() const noexcept {
    return owner_id_;
  }

  [[nodiscard]] std::uint64_t epoch() const noexcept {
    return epoch_;
  }

  [[nodiscard]] std::uint64_t revision() const noexcept {
    return revision_;
  }

  [[nodiscard]] std::size_t offset() const noexcept {
    return offset_;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return length_;
  }

  [[nodiscard]] bool empty() const noexcept {
    return length_ == 0;
  }

  [[nodiscard]] std::byte at(std::size_t index) const {
    const auto owner = require_owner();
    if (index >= length_) {
      throw std::out_of_range{
          "CopyReadablePrefixLease 下标越界"};
    }
    // 本 lease 固定在已发布 prefix。active readable pin 阻止该物理区间在
    // 最后一个副本析构前被回收覆盖；native writer 只能写 length 之后的 tail。
    return owner->bytes.at(offset_ + index);
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const auto owner = require_owner();
    std::vector<std::byte> result;
    result.reserve(length_);
    for (std::size_t index = 0; index < length_; ++index) {
      result.push_back(owner->bytes.at(offset_ + index));
    }
    return result;
  }

  [[nodiscard]] bool is_monotonic_extension_of(
      const CopyReadablePrefixLease& previous) const noexcept {
    return valid() && previous.valid() &&
        owner_id_ == previous.owner_id_ &&
        epoch_ == previous.epoch_ &&
        offset_ == previous.offset_ &&
        revision_ > previous.revision_ &&
        length_ >= previous.length_;
  }

 private:
  CopyReadablePrefixLease(
      std::shared_ptr<
          copy_buffer_detail::ReadableLeaseRegistration>
          registration,
      std::uint64_t owner_id,
      std::uint64_t epoch,
      std::uint64_t revision,
      std::size_t offset,
      std::size_t length) noexcept
      : registration_{std::move(registration)},
        owner_id_{owner_id},
        epoch_{epoch},
        revision_{revision},
        offset_{offset},
        length_{length} {}

  [[nodiscard]] std::shared_ptr<
      copy_buffer_detail::CopyBufferState>
  require_owner() const {
    if (!registration_) {
      throw std::logic_error{
          "CopyReadablePrefixLease 已移动或释放"};
    }
    return registration_->owner();
  }

  std::shared_ptr<
      copy_buffer_detail::ReadableLeaseRegistration>
      registration_;
  std::uint64_t owner_id_{0};
  std::uint64_t epoch_{0};
  std::uint64_t revision_{0};
  std::size_t offset_{0};
  std::size_t length_{0};

  friend class CopyBuffer;
};

/**
 * OperationRegistry/tombstone 独占的 native writable storage pin。
 *
 * pin 本身不完成 native operation；driver 持有配对的弱 completion token。
 * prepared pin 可在分配失败或同步 submit 失败时安全 rollback；只有进入
 * submitted 后才必须等待 token 发布 terminal settled，避免异常路径临时量
 * 析构被误判成丢失在途内核访问。
 */
class CopyNativeWritableTailPin final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_poll_native_owned_value = true;

  CopyNativeWritableTailPin() noexcept = default;
  CopyNativeWritableTailPin(
      const CopyNativeWritableTailPin&) = delete;
  CopyNativeWritableTailPin& operator=(
      const CopyNativeWritableTailPin&) = delete;

  CopyNativeWritableTailPin(
      CopyNativeWritableTailPin&& other) noexcept
      : control_{std::move(other.control_)} {}

  CopyNativeWritableTailPin& operator=(
      CopyNativeWritableTailPin&& other) noexcept {
    if (this != &other) {
      release();
      control_ = std::move(other.control_);
    }
    return *this;
  }

  ~CopyNativeWritableTailPin() {
    release();
  }

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(control_);
  }

  [[nodiscard]] bool settled() const noexcept {
    return control_ &&
        control_->phase.load(std::memory_order_acquire) ==
            copy_buffer_detail::NativePinPhase::settled;
  }

  [[nodiscard]] std::uint64_t owner_id() const noexcept {
    return control_ ? control_->owner->owner_id : 0;
  }

  [[nodiscard]] std::uint64_t epoch() const noexcept {
    return control_ ? control_->epoch : 0;
  }

  [[nodiscard]] std::uint64_t generation() const noexcept {
    return control_ ? control_->generation : 0;
  }

  [[nodiscard]] std::size_t offset() const noexcept {
    return control_ ? control_->offset : 0;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return control_ ? control_->length : 0;
  }

 private:
  explicit CopyNativeWritableTailPin(
      std::shared_ptr<copy_buffer_detail::NativeTailControl>
          control) noexcept
      : control_{std::move(control)} {}

  void release() noexcept {
    if (!control_) {
      return;
    }
    auto phase =
        control_->phase.load(std::memory_order_acquire);
    if (phase == copy_buffer_detail::NativePinPhase::prepared) {
      (void)control_->phase.compare_exchange_strong(
          phase,
          copy_buffer_detail::NativePinPhase::rolled_back,
          std::memory_order_acq_rel,
          std::memory_order_acquire);
      phase = control_->phase.load(std::memory_order_acquire);
    }
    if (phase != copy_buffer_detail::NativePinPhase::settled &&
        phase != copy_buffer_detail::NativePinPhase::rolled_back) {
      std::terminate();
    }
    copy_buffer_detail::release_pin(
        control_->owner->native_writable_pins);
    control_.reset();
  }

  std::shared_ptr<copy_buffer_detail::NativeTailControl>
      control_;

  friend class CopyWritableTailLease;
  friend class CopyNativeTailSubmission;
};

/**
 * 进入 OS submit 调用前取得的唯一 buffer 提交事务。
 *
 * begin 已把 prepared 推进为 submitting，因而 pin/token 此后不得静默回滚。
 * OS 同步返回后必须显式 accept 或 reject；即时 completion 可以先发布
 * settled_before_accept，再由 accept 收口。completion 已开始却报告同步拒绝
 * 属于 driver 契约矛盾并 fail-fast。
 */
class CopyNativeSubmitGuard final {
 public:
  CopyNativeSubmitGuard(const CopyNativeSubmitGuard&) = delete;
  CopyNativeSubmitGuard& operator=(const CopyNativeSubmitGuard&) = delete;

  CopyNativeSubmitGuard(
      CopyNativeSubmitGuard&& other) noexcept
      : control_{std::move(other.control_)},
        active_{std::exchange(other.active_, false)} {}

  CopyNativeSubmitGuard& operator=(CopyNativeSubmitGuard&&) = delete;

  ~CopyNativeSubmitGuard() {
    if (active_) {
      std::terminate();
    }
  }

  [[nodiscard]] bool accept() noexcept {
    return finish(true);
  }

  [[nodiscard]] bool reject() noexcept {
    return finish(false);
  }

 private:
  explicit CopyNativeSubmitGuard(
      std::shared_ptr<copy_buffer_detail::NativeTailControl>
          control) noexcept
      : control_{std::move(control)}, active_{true} {}

  [[nodiscard]] bool finish(bool accepted) noexcept {
    if (!active_ || !control_) {
      return false;
    }
    active_ = false;
    auto phase =
        control_->phase.load(std::memory_order_acquire);
    while (true) {
      auto next = phase;
      if (accepted) {
        if (phase ==
            copy_buffer_detail::NativePinPhase::submitting) {
          next =
              copy_buffer_detail::NativePinPhase::submitted;
        } else if (
            phase ==
            copy_buffer_detail::NativePinPhase::
                completing_before_accept) {
          next =
              copy_buffer_detail::NativePinPhase::completing;
        } else if (
            phase ==
            copy_buffer_detail::NativePinPhase::
                settled_before_accept) {
          next = copy_buffer_detail::NativePinPhase::settled;
        } else {
          return false;
        }
      } else {
        if (phase ==
            copy_buffer_detail::NativePinPhase::submitting) {
          next =
              copy_buffer_detail::NativePinPhase::rolled_back;
        } else if (
            phase ==
                copy_buffer_detail::NativePinPhase::
                    completing_before_accept ||
            phase ==
                copy_buffer_detail::NativePinPhase::
                    settled_before_accept) {
          std::terminate();
        } else {
          return false;
        }
      }
      if (control_->phase.compare_exchange_weak(
              phase,
              next,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        control_.reset();
        return true;
      }
    }
  }

  std::shared_ptr<copy_buffer_detail::NativeTailControl>
      control_;
  bool active_{false};

  friend class CopyNativeTailSubmission;
};

/**
 * driver 持有的非 owning native completion token。
 *
 * token 只保存 weak control，不延长 buffer/pin 生命周期。OperationRegistry 必须
 * 持有配对 pin；driver 在进入 OS 前先取得 CopyNativeSubmitGuard，真实 terminal
 * completion 调用 settle_native(bytes_transferred)，submit 返回后再由 guard
 * accept/reject 收口。测试用 write_at 模拟内核写入，但生产连续内存 ABI
 * accessor 仍只允许位于 `src/platform/`。
 */
class CopyNativeCompletionToken final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_poll_native_owned_value = true;

  CopyNativeCompletionToken() noexcept = default;
  CopyNativeCompletionToken(
      const CopyNativeCompletionToken&) = delete;
  CopyNativeCompletionToken& operator=(
      const CopyNativeCompletionToken&) = delete;
  CopyNativeCompletionToken(
      CopyNativeCompletionToken&& other) noexcept
      : control_{std::move(other.control_)},
        owner_id_{std::exchange(other.owner_id_, 0)},
        epoch_{std::exchange(other.epoch_, 0)},
        generation_{std::exchange(other.generation_, 0)},
        offset_{std::exchange(other.offset_, 0)},
        length_{std::exchange(other.length_, 0)},
        test_initialized_prefix_{
            std::exchange(other.test_initialized_prefix_, 0)},
        used_test_writer_{
            std::exchange(other.used_test_writer_, false)},
        active_{std::exchange(other.active_, false)} {}
  CopyNativeCompletionToken& operator=(
      CopyNativeCompletionToken&&) = delete;
  ~CopyNativeCompletionToken() {
    release();
  }

  [[nodiscard]] bool valid() const noexcept {
    return active_ && !control_.expired();
  }

  [[nodiscard]] std::uint64_t owner_id() const noexcept {
    return owner_id_;
  }

  [[nodiscard]] std::uint64_t epoch() const noexcept {
    return epoch_;
  }

  [[nodiscard]] std::uint64_t generation() const noexcept {
    return generation_;
  }

  [[nodiscard]] std::size_t offset() const noexcept {
    return offset_;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return length_;
  }

  /**
   * 测试专用的受控 native byte write。
   *
   * 临时 lock weak control 只覆盖本次同步调用，不把生命周期权保存在 token 中。
   * 只能顺序初始化 submitted tail；settled 后立即拒绝。
   */
  void write_at(std::size_t index, std::byte value) {
    const auto control = require_completion_claim();
    if (index >= control->length) {
      throw std::out_of_range{
          "CopyNativeCompletionToken 下标越界"};
    }
    if (index > test_initialized_prefix_) {
      throw std::logic_error{
          "CopyNativeCompletionToken 不允许跳过未初始化字节"};
    }
    control->owner->bytes.at(control->offset + index) = value;
    if (index == test_initialized_prefix_) {
      ++test_initialized_prefix_;
    }
    used_test_writer_ = true;
  }

  /**
   * 发布真实 native terminal completion 与 bytes_transferred。
   *
   * bytes_transferred 只要求不超过固定 tail；测试若通过 write_at 模拟内核，
   * 还会校验不超过已初始化前缀。release 与 parent commit_native 的 acquire
   * 配对，保证真实 OS 写入在扩大 readable prefix 前可见。
   */
  [[nodiscard]] bool settle_native(
      std::size_t bytes_transferred) {
    const auto control = require_completion_claim();
    if (bytes_transferred > control->length) {
      throw std::out_of_range{
          "native bytes_transferred 超过 tail length"};
    }
    if (used_test_writer_ &&
        bytes_transferred > test_initialized_prefix_) {
      throw std::out_of_range{
          "native bytes_transferred 超过测试 initialized prefix"};
    }
    control->initialized_prefix.store(
        bytes_transferred,
        std::memory_order_relaxed);
    auto phase =
        control->phase.load(std::memory_order_acquire);
    while (true) {
      auto next = phase;
      if (phase ==
          copy_buffer_detail::NativePinPhase::
              completing_before_accept) {
        next =
            copy_buffer_detail::NativePinPhase::
                settled_before_accept;
      } else if (
          phase ==
          copy_buffer_detail::NativePinPhase::completing) {
        next = copy_buffer_detail::NativePinPhase::settled;
      } else {
        return false;
      }
      if (control->phase.compare_exchange_weak(
              phase,
              next,
              std::memory_order_release,
              std::memory_order_acquire)) {
        active_ = false;
        control_.reset();
        return true;
      }
    }
  }

 private:
  explicit CopyNativeCompletionToken(
      const std::shared_ptr<
          copy_buffer_detail::NativeTailControl>&
          control) noexcept
      : control_{control},
        owner_id_{control->owner->owner_id},
        epoch_{control->epoch},
        generation_{control->generation},
        offset_{control->offset},
        length_{control->length},
        active_{true} {}

  [[nodiscard]] std::shared_ptr<
      copy_buffer_detail::NativeTailControl>
  require_completion_claim() {
    if (!active_) {
      throw std::logic_error{
          "CopyNativeCompletionToken 已结算、回滚或移动"};
    }
    const auto control = control_.lock();
    if (!control) {
      throw std::logic_error{
          "CopyNativeCompletionToken 对应 pin 已释放"};
    }
    auto phase =
        control->phase.load(std::memory_order_acquire);
    while (true) {
      auto next = phase;
      if (phase ==
          copy_buffer_detail::NativePinPhase::submitting) {
        next =
            copy_buffer_detail::NativePinPhase::
                completing_before_accept;
      } else if (
          phase ==
          copy_buffer_detail::NativePinPhase::submitted) {
        next =
            copy_buffer_detail::NativePinPhase::completing;
      } else if (
          phase ==
              copy_buffer_detail::NativePinPhase::
                  completing_before_accept ||
          phase ==
              copy_buffer_detail::NativePinPhase::completing) {
        return control;
      } else {
        throw std::logic_error{
            "CopyNativeCompletionToken 未进入 submitting 或已终结"};
      }
      if (control->phase.compare_exchange_weak(
              phase,
              next,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return control;
      }
    }
  }

  void release() noexcept {
    if (!active_) {
      return;
    }
    const auto control = control_.lock();
    if (!control) {
      active_ = false;
      return;
    }
    const auto phase =
        control->phase.load(std::memory_order_acquire);
    if (phase != copy_buffer_detail::NativePinPhase::prepared &&
        phase !=
            copy_buffer_detail::NativePinPhase::rolled_back) {
      std::terminate();
    }
    active_ = false;
    control_.reset();
  }

  std::weak_ptr<copy_buffer_detail::NativeTailControl>
      control_;
  std::uint64_t owner_id_{0};
  std::uint64_t epoch_{0};
  std::uint64_t generation_{0};
  std::size_t offset_{0};
  std::size_t length_{0};
  std::size_t test_initialized_prefix_{0};
  bool used_test_writer_{false};
  bool active_{false};

  friend class CopyNativeTailSubmission;
  friend class CopyWritableTailLease;
};

/**
 * prepared native submission 的唯一拆分包。
 *
 * `take_registry_pin` 交给 OperationRegistry，`take_completion_token` 交给
 * driver。bundle 或 prepared pin 因异常析构时会自动 rollback，不会把尚未被
 * OS 接受的请求误判为必须等待 completion。
 */
class CopyNativeTailSubmission final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_poll_native_owned_value = true;

  CopyNativeTailSubmission(
      const CopyNativeTailSubmission&) = delete;
  CopyNativeTailSubmission& operator=(
      const CopyNativeTailSubmission&) = delete;

  CopyNativeTailSubmission(
      CopyNativeTailSubmission&& other) noexcept
      : registry_pin_{std::move(other.registry_pin_)},
        completion_token_{
            std::move(other.completion_token_)},
        control_{std::move(other.control_)},
        submit_guard_taken_{
            std::exchange(other.submit_guard_taken_, true)} {
    other.registry_pin_.reset();
    other.completion_token_.reset();
  }

  CopyNativeTailSubmission& operator=(
      CopyNativeTailSubmission&&) = delete;
  ~CopyNativeTailSubmission() = default;

  [[nodiscard]] CopyNativeWritableTailPin
  take_registry_pin() {
    if (!registry_pin_) {
      throw std::logic_error{
          "native submission registry pin 已取走"};
    }
    auto pin = std::move(*registry_pin_);
    registry_pin_.reset();
    return pin;
  }

  [[nodiscard]] CopyNativeCompletionToken
  take_completion_token() {
    if (!completion_token_) {
      throw std::logic_error{
          "native submission completion token 已取走"};
    }
    auto token = std::move(*completion_token_);
    completion_token_.reset();
    return token;
  }

  /**
   * 在调用 OS submit 前推进为 submitting，并取得唯一收口 guard。
   */
  [[nodiscard]] CopyNativeSubmitGuard begin_submit() {
    if (submit_guard_taken_) {
      throw std::logic_error{
          "native submission guard 已取走"};
    }
    const auto control = control_.lock();
    if (!control) {
      throw std::logic_error{
          "native submission control 已释放"};
    }
    auto expected =
        copy_buffer_detail::NativePinPhase::prepared;
    if (!control->phase.compare_exchange_strong(
            expected,
            copy_buffer_detail::NativePinPhase::submitting,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      throw std::logic_error{
          "native submission 已开始或终结"};
    }
    submit_guard_taken_ = true;
    return CopyNativeSubmitGuard{control};
  }

 private:
  explicit CopyNativeTailSubmission(
      const std::shared_ptr<
          copy_buffer_detail::NativeTailControl>&
          control)
      : registry_pin_{
            std::in_place,
            CopyNativeWritableTailPin{control}},
        completion_token_{
            std::in_place,
            CopyNativeCompletionToken{control}},
        control_{control} {}

  std::optional<CopyNativeWritableTailPin>
      registry_pin_;
  std::optional<CopyNativeCompletionToken>
      completion_token_;
  std::weak_ptr<copy_buffer_detail::NativeTailControl>
      control_;
  bool submit_guard_taken_{false};

  friend class CopyWritableTailLease;
};

/**
 * reader operation 独占的逻辑 writable tail lease。
 *
 * manual write/commit 与 native submitted/commit_native 是互斥状态机。提交
 * native 后只有 pin 能写 storage；tail cancel 只释放逻辑 active，不会伪装成
 * 内核已停止访问。
 */
class CopyWritableTailLease final {
 public:
  // 逻辑 tail 直接修改 parent-only metadata，只能作为完整 CopyBuffer 状态机的
  // 一部分随串行 parent task 迁移，禁止拆出后交给 completion/Registry 并发持有。
  static constexpr bool cio_send = false;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_poll_native_owned_value = false;

  CopyWritableTailLease() noexcept = default;
  CopyWritableTailLease(const CopyWritableTailLease&) = delete;
  CopyWritableTailLease& operator=(
      const CopyWritableTailLease&) = delete;

  CopyWritableTailLease(
      CopyWritableTailLease&& other) noexcept
      : owner_{std::move(other.owner_)},
        native_control_{std::move(other.native_control_)},
        owner_id_{std::exchange(other.owner_id_, 0)},
        epoch_{std::exchange(other.epoch_, 0)},
        generation_{std::exchange(other.generation_, 0)},
        offset_{std::exchange(other.offset_, 0)},
        length_{std::exchange(other.length_, 0)},
        initialized_prefix_{
            std::exchange(other.initialized_prefix_, 0)},
        submitted_{std::exchange(other.submitted_, false)} {}

  CopyWritableTailLease& operator=(
      CopyWritableTailLease&& other) noexcept {
    if (this != &other) {
      cancel_now();
      owner_ = std::move(other.owner_);
      native_control_ = std::move(other.native_control_);
      owner_id_ = std::exchange(other.owner_id_, 0);
      epoch_ = std::exchange(other.epoch_, 0);
      generation_ = std::exchange(other.generation_, 0);
      offset_ = std::exchange(other.offset_, 0);
      length_ = std::exchange(other.length_, 0);
      initialized_prefix_ =
          std::exchange(other.initialized_prefix_, 0);
      submitted_ =
          std::exchange(other.submitted_, false);
    }
    return *this;
  }

  ~CopyWritableTailLease() {
    cancel_now();
  }

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(owner_) &&
        epoch_ != 0 &&
        generation_ != 0;
  }

  [[nodiscard]] bool submitted() const noexcept {
    return valid() && submitted_;
  }

  [[nodiscard]] std::uint64_t owner_id() const noexcept {
    return owner_id_;
  }

  [[nodiscard]] std::uint64_t epoch() const noexcept {
    return epoch_;
  }

  [[nodiscard]] std::uint64_t generation() const noexcept {
    return generation_;
  }

  [[nodiscard]] std::size_t offset() const noexcept {
    return offset_;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return length_;
  }

  [[nodiscard]] std::size_t initialized_size() const noexcept {
    return initialized_prefix_;
  }

  void write_at(std::size_t index, std::byte value) {
    const auto owner = require_owner();
    require_current(*owner);
    if (submitted_) {
      throw std::logic_error{
          "submitted tail 禁止 manual write"};
    }
    if (index >= length_) {
      throw std::out_of_range{
          "CopyWritableTailLease 下标越界"};
    }
    if (index > initialized_prefix_) {
      throw std::logic_error{
          "CopyWritableTailLease 不允许跳过未初始化字节"};
    }
    owner->bytes.at(offset_ + index) = value;
    if (index == initialized_prefix_) {
      ++initialized_prefix_;
    }
  }

  [[nodiscard]] std::uint64_t commit(std::size_t amount) {
    const auto owner = require_owner();
    require_current(*owner);
    if (submitted_) {
      throw std::logic_error{
          "submitted tail 必须使用 commit_native"};
    }
    if (amount > initialized_prefix_) {
      throw std::out_of_range{
          "CopyWritableTailLease commit 超过 initialized prefix"};
    }
    return finish_commit(*owner, amount);
  }

  /**
   * 把固定 tail 物理区间提交给 native operation。
   *
   * manual 初始化后不得切换到 native，避免两套提交协议混合。返回的 prepared
   * bundle 必须拆成 Registry pin 与 driver completion token，并在进入 OS 前
   * 取得 submit guard 推进到 submitting；OS 同步返回后由 guard 收口。
   */
  [[nodiscard]] CopyNativeTailSubmission submit_native() {
    const auto owner = require_owner();
    require_current(*owner);
    if (submitted_) {
      throw std::logic_error{
          "CopyWritableTailLease 已提交 native"};
    }
    if (initialized_prefix_ != 0) {
      throw std::logic_error{
          "manual write 后不得切换到 native submitted"};
    }

    auto control =
        std::make_shared<copy_buffer_detail::NativeTailControl>(
            owner,
            epoch_,
            generation_,
            offset_,
            length_);
    copy_buffer_detail::acquire_pin(
        owner->native_writable_pins);
    native_control_ = control;
    submitted_ = true;
    return CopyNativeTailSubmission{control};
  }

  /**
   * prepared 请求尚未被 OS 接受时回到 manual tail。
   *
   * 可由同步 submit 失败路径显式调用；prepared pin 的异常析构也会先把 phase
   * rollback。submitted/settled 后拒绝回滚，避免把真实在途 I/O 当成未提交。
   */
  [[nodiscard]] bool abort_before_submit() noexcept {
    if (!valid() || !submitted_ || !native_control_) {
      return false;
    }
    auto phase =
        native_control_->phase.load(std::memory_order_acquire);
    if (phase == copy_buffer_detail::NativePinPhase::prepared) {
      (void)native_control_->phase.compare_exchange_strong(
          phase,
          copy_buffer_detail::NativePinPhase::rolled_back,
          std::memory_order_acq_rel,
          std::memory_order_acquire);
      phase =
          native_control_->phase.load(std::memory_order_acquire);
    }
    if (phase !=
        copy_buffer_detail::NativePinPhase::rolled_back) {
      return false;
    }
    native_control_.reset();
    submitted_ = false;
    return true;
  }

  /**
   * 在 native pin 已 settle 后发布 amount 个字节。
   *
   * acquire 读取 settle，保证 native 写入先于 cap/revision 扩展。pin 即使已经
   * settle 仍必须由 tombstone 持有到其自身释放；在此之前新 tail/recycle 仍
   * 会被 native_writable_pins 拒绝。
   */
  [[nodiscard]] std::uint64_t commit_native(
      std::size_t amount) {
    const auto owner = require_owner();
    require_current(*owner);
    if (!submitted_ || !native_control_) {
      throw std::logic_error{
          "manual tail 禁止 commit_native"};
    }
    if (native_control_->phase.load(
            std::memory_order_acquire) !=
        copy_buffer_detail::NativePinPhase::settled) {
      throw std::logic_error{
          "native pin 未 settle"};
    }
    const auto initialized =
        native_control_->initialized_prefix.load(
            std::memory_order_relaxed);
    if (amount > initialized) {
      throw std::out_of_range{
          "commit_native 超过 native initialized prefix"};
    }
    return finish_commit(*owner, amount);
  }

  /**
   * 只取消 parent 的逻辑 tail。
   *
   * 若已经 submitted，native pin 及其 storage 权限不受影响；late native write
   * 继续落在旧 tail，直到 pin settle 并释放前都禁止新 tail 和 storage 回收。
   */
  void cancel_now() noexcept {
    if (!valid()) {
      return;
    }
    if (owner_->tail_active &&
        owner_->epoch == epoch_ &&
        owner_->active_tail_generation == generation_) {
      owner_->tail_active = false;
      owner_->active_tail_generation = 0;
    }
    release_local();
  }

 private:
  CopyWritableTailLease(
      std::shared_ptr<copy_buffer_detail::CopyBufferState> owner,
      std::uint64_t epoch,
      std::uint64_t generation,
      std::size_t offset,
      std::size_t length) noexcept
      : owner_{std::move(owner)},
        owner_id_{owner_->owner_id},
        epoch_{epoch},
        generation_{generation},
        offset_{offset},
        length_{length} {}

  [[nodiscard]] std::shared_ptr<
      copy_buffer_detail::CopyBufferState>
  require_owner() const {
    if (!valid()) {
      throw std::logic_error{
          "CopyWritableTailLease 已移动、提交或取消"};
    }
    return owner_;
  }

  void require_current(
      const copy_buffer_detail::CopyBufferState& owner) const {
    if (!owner.tail_active ||
        owner.epoch != epoch_ ||
        owner.active_tail_generation != generation_ ||
        owner.capacity_end != offset_) {
      throw std::logic_error{
          "CopyWritableTailLease generation 已失效"};
    }
  }

  [[nodiscard]] std::uint64_t finish_commit(
      copy_buffer_detail::CopyBufferState& owner,
      std::size_t amount) {
    owner.capacity_end = offset_ + amount;
    owner.tail_active = false;
    owner.active_tail_generation = 0;
    if (amount != 0) {
      if (owner.revision ==
          std::numeric_limits<std::uint64_t>::max()) {
        std::terminate();
      }
      ++owner.revision;
    }
    const auto revision = owner.revision;
    release_local();
    return revision;
  }

  void release_local() noexcept {
    owner_.reset();
    native_control_.reset();
    owner_id_ = 0;
    epoch_ = 0;
    generation_ = 0;
    offset_ = 0;
    length_ = 0;
    initialized_prefix_ = 0;
    submitted_ = false;
  }

  std::shared_ptr<copy_buffer_detail::CopyBufferState> owner_;
  std::shared_ptr<copy_buffer_detail::NativeTailControl>
      native_control_;
  std::uint64_t owner_id_{0};
  std::uint64_t epoch_{0};
  std::uint64_t generation_{0};
  std::size_t offset_{0};
  std::size_t length_{0};
  std::size_t initialized_prefix_{0};
  bool submitted_{false};

  friend class CopyBuffer;
};

/**
 * Tokio copy 专用的 8192-byte owned buffer。
 *
 * 所有可变 metadata 都由单个 parent task 串行拥有，worker poll 热路径不使用
 * OS mutex。readable prefix 为 [position, capacity_end)，writable tail 为
 * [capacity_end, 8192)。旧 readable pin 或 native writable pin 未释放时，
 * parent 不得回收 storage；native pin 存活时也不得发放下一 writable tail。
 */
class CopyBuffer final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_poll_native_owned_value = true;
  static constexpr std::size_t capacity{
      copy_buffer_capacity};

  CopyBuffer()
      : state_{
            std::make_shared<
                copy_buffer_detail::CopyBufferState>()} {}

  CopyBuffer(const CopyBuffer&) = delete;
  CopyBuffer& operator=(const CopyBuffer&) = delete;
  CopyBuffer(CopyBuffer&&) noexcept = default;
  CopyBuffer& operator=(CopyBuffer&&) noexcept = default;
  ~CopyBuffer() = default;

  [[nodiscard]] std::size_t readable_size() const {
    const auto state = require_state();
    return state->capacity_end - state->position;
  }

  [[nodiscard]] std::size_t writable_size() const {
    const auto state = require_state();
    return capacity - state->capacity_end;
  }

  [[nodiscard]] bool empty() const {
    return readable_size() == 0;
  }

  [[nodiscard]] std::uint64_t epoch() const {
    return require_state()->epoch;
  }

  [[nodiscard]] std::uint64_t revision() const {
    return require_state()->revision;
  }

  [[nodiscard]] CopyReadablePrefixLease
  lease_readable_prefix() const {
    const auto state = require_state();
    auto registration = std::make_shared<
        copy_buffer_detail::ReadableLeaseRegistration>(
        state);
    return CopyReadablePrefixLease{
        std::move(registration),
        state->owner_id,
        state->epoch,
        state->revision,
        state->position,
        state->capacity_end - state->position};
  }

  [[nodiscard]] CopyWritableTailLease
  lease_writable_tail() {
    const auto state = require_state();
    if (state->tail_active) {
      throw std::logic_error{
          "CopyBuffer 已有 active writable tail lease"};
    }
    if (state->native_writable_pins.load(
            std::memory_order_acquire) != 0) {
      throw std::logic_error{
          "CopyBuffer native writable pin 尚未释放"};
    }
    if (state->next_tail_generation ==
        std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    ++state->next_tail_generation;
    state->tail_active = true;
    state->active_tail_generation =
        state->next_tail_generation;
    return CopyWritableTailLease{
        state,
        state->epoch,
        state->active_tail_generation,
        state->capacity_end,
        capacity - state->capacity_end};
  }

  void consume(std::size_t amount) {
    const auto state = require_state();
    const auto available =
        state->capacity_end - state->position;
    if (amount > available) {
      throw std::out_of_range{
          "CopyBuffer consume 超过 readable prefix"};
    }
    state->position += amount;
  }

  /**
   * 尝试回收已经消费完的物理 storage。
   *
   * atomic acquire 与最后一个 readable/native pin 的 release 配对。并发 release
   * 可能让本次保守地返回 false，parent 可在下一 fresh poll 重试。
   */
  [[nodiscard]] bool try_recycle_empty() {
    const auto state = require_state();
    if (state->position != state->capacity_end ||
        state->tail_active ||
        state->readable_pins.load(
            std::memory_order_acquire) != 0 ||
        state->native_writable_pins.load(
            std::memory_order_acquire) != 0) {
      return false;
    }
    if (state->epoch ==
        std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    ++state->epoch;
    state->revision = 0;
    state->position = 0;
    state->capacity_end = 0;
    return true;
  }

 private:
  [[nodiscard]] std::shared_ptr<
      copy_buffer_detail::CopyBufferState>
  require_state() const {
    if (!state_) {
      throw std::logic_error{"CopyBuffer 已移动"};
    }
    return state_;
  }

  std::shared_ptr<copy_buffer_detail::CopyBufferState> state_;
};

}  // namespace cio::io::detail
