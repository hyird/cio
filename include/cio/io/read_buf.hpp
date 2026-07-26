#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cio::io {

namespace detail {

enum class ReadBufferAccess : std::uint8_t {
  idle,
  owner,
  lease,
};

struct ReadBufferState final {
  explicit ReadBufferState(std::size_t capacity)
      : bytes(capacity, std::byte{0}),
        initialized(capacity) {}

  std::vector<std::byte> bytes;
  std::size_t filled{0};
  std::size_t initialized{0};
  std::atomic<ReadBufferAccess> access{ReadBufferAccess::idle};
  std::atomic<std::uint64_t> generation{0};
};

class ReadBufferOwnerGuard final {
 public:
  ReadBufferOwnerGuard() noexcept = default;
  ReadBufferOwnerGuard(const ReadBufferOwnerGuard&) = delete;
  ReadBufferOwnerGuard& operator=(const ReadBufferOwnerGuard&) = delete;

  ReadBufferOwnerGuard(ReadBufferOwnerGuard&& other) noexcept
      : state_{std::move(other.state_)} {}

  ReadBufferOwnerGuard& operator=(
      ReadBufferOwnerGuard&& other) noexcept {
    if (this != &other) {
      release();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~ReadBufferOwnerGuard() {
    release();
  }

  [[nodiscard]] static ReadBufferOwnerGuard acquire(
      const std::shared_ptr<ReadBufferState>& state) {
    ReadBufferAccess expected = ReadBufferAccess::idle;
    if (!state->access.compare_exchange_strong(
            expected,
            ReadBufferAccess::owner,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      if (expected == ReadBufferAccess::lease) {
        throw std::logic_error{
            "ReadBuf 存在活动可变 operation，owner 暂时不能访问"};
      }
      throw std::logic_error{"ReadBuf 状态正被同步访问"};
    }
    return ReadBufferOwnerGuard{state};
  }

 private:
  explicit ReadBufferOwnerGuard(
      std::shared_ptr<ReadBufferState> state) noexcept
      : state_{std::move(state)} {}

  void release() noexcept {
    if (!state_) {
      return;
    }
    state_->access.store(
        ReadBufferAccess::idle,
        std::memory_order_release);
    state_.reset();
  }

  std::shared_ptr<ReadBufferState> state_;
};

inline void require_filled_bound(
    const ReadBufferState& state,
    std::size_t filled) {
  if (filled > state.initialized) {
    throw std::out_of_range{"ReadBuf filled 不能超过 initialized"};
  }
}

inline void put_bytes(
    ReadBufferState& state,
    std::span<const std::byte> bytes) {
  if (bytes.size() > state.bytes.size() - state.filled) {
    throw std::out_of_range{"输入字节超过 ReadBuf remaining"};
  }
  const auto begin = state.filled;
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    state.bytes[begin + index] = bytes[index];
  }
  state.filled += bytes.size();
}

inline std::vector<std::byte> prefix_snapshot(
    const ReadBufferState& state,
    std::size_t length) {
  std::vector<std::byte> result;
  result.reserve(length);
  for (std::size_t index = 0; index < length; ++index) {
    result.push_back(state.bytes[index]);
  }
  return result;
}

}  // namespace detail

class ReadBuf;

/**
 * 单个异步 read operation 持有的独占可变租约。
 *
 * 租约移动专属并携带 generation。析构只释放与当前 generation 匹配的活动
 * 租约，过期 lease 不能错误解锁新 operation。
 */
class MutableBufferLease final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;

  MutableBufferLease() noexcept = default;
  MutableBufferLease(const MutableBufferLease&) = delete;
  MutableBufferLease& operator=(const MutableBufferLease&) = delete;

  MutableBufferLease(MutableBufferLease&& other) noexcept
      : state_{std::move(other.state_)},
        generation_{std::exchange(other.generation_, 0)} {}

  MutableBufferLease& operator=(MutableBufferLease&& other) noexcept {
    if (this != &other) {
      release();
      state_ = std::move(other.state_);
      generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
  }

  ~MutableBufferLease() {
    release();
  }

  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(state_) && generation_ != 0;
  }

  [[nodiscard]] std::size_t capacity() const {
    const auto state = require_state();
    require_current(*state);
    return state->bytes.size();
  }

  [[nodiscard]] std::size_t filled_size() const {
    const auto state = require_state();
    require_current(*state);
    return state->filled;
  }

  [[nodiscard]] std::size_t initialized_size() const {
    const auto state = require_state();
    require_current(*state);
    return state->initialized;
  }

  [[nodiscard]] std::size_t remaining() const {
    const auto state = require_state();
    require_current(*state);
    return state->bytes.size() - state->filled;
  }

  void clear() {
    const auto state = require_state();
    require_current(*state);
    state->filled = 0;
  }

  void advance(std::size_t amount) {
    const auto state = require_state();
    require_current(*state);
    if (amount > state->initialized - state->filled) {
      throw std::out_of_range{"ReadBuf advance 超过 initialized"};
    }
    state->filled += amount;
  }

  void set_filled(std::size_t filled) {
    const auto state = require_state();
    require_current(*state);
    detail::require_filled_bound(*state, filled);
    state->filled = filled;
  }

  void put_slice(std::span<const std::byte> bytes) {
    const auto state = require_state();
    require_current(*state);
    detail::put_bytes(*state, bytes);
  }

  [[nodiscard]] std::vector<std::byte> filled_snapshot() const {
    const auto state = require_state();
    require_current(*state);
    return detail::prefix_snapshot(*state, state->filled);
  }

  [[nodiscard]] std::vector<std::byte> initialized_snapshot() const {
    const auto state = require_state();
    require_current(*state);
    return detail::prefix_snapshot(*state, state->initialized);
  }

 private:
  MutableBufferLease(
      std::shared_ptr<detail::ReadBufferState> state,
      std::uint64_t generation) noexcept
      : state_{std::move(state)},
        generation_{generation} {}

  [[nodiscard]] std::shared_ptr<detail::ReadBufferState> require_state() const {
    if (!valid()) {
      throw std::logic_error{"MutableBufferLease 已移动或释放"};
    }
    return state_;
  }

  void require_current(const detail::ReadBufferState& state) const {
    if (state.access.load(std::memory_order_acquire) !=
            detail::ReadBufferAccess::lease ||
        state.generation.load(std::memory_order_relaxed) != generation_) {
      throw std::logic_error{"MutableBufferLease generation 已失效"};
    }
  }

  void release() noexcept {
    if (!state_ || generation_ == 0) {
      return;
    }
    if (state_->access.load(std::memory_order_acquire) ==
            detail::ReadBufferAccess::lease &&
        state_->generation.load(std::memory_order_relaxed) == generation_) {
      // release 发布本租约对 bytes/filled 的全部修改；后续 owner 的 acquire
      // gate 成功后才允许读取。析构路径不等待任何 OS mutex。
      state_->access.store(
          detail::ReadBufferAccess::idle,
          std::memory_order_release);
    }
    state_.reset();
    generation_ = 0;
  }

  std::shared_ptr<detail::ReadBufferState> state_;
  std::uint64_t generation_{0};

  friend class ReadBuf;
};

/**
 * Tokio ReadBuf 的完全初始化 C++20 安全首片。
 *
 * ReadBuf 移动专属。物理存储创建时全部初始化，因此本切片始终保持
 * initialized == capacity；filled 仍可增长、缩小和 clear。unsafe uninitialized
 * 能力待后续以安全 writable-region API 映射。
 */
class ReadBuf final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;

  ReadBuf() = delete;

  [[nodiscard]] static ReadBuf with_capacity(std::size_t capacity) {
    return ReadBuf{
        std::make_shared<detail::ReadBufferState>(capacity)};
  }

  ReadBuf(const ReadBuf&) = delete;
  ReadBuf& operator=(const ReadBuf&) = delete;
  ReadBuf(ReadBuf&&) noexcept = default;
  ReadBuf& operator=(ReadBuf&&) noexcept = default;
  ~ReadBuf() = default;

  [[nodiscard]] std::size_t capacity() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return state->bytes.size();
  }

  [[nodiscard]] std::size_t filled_size() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return state->filled;
  }

  [[nodiscard]] std::size_t initialized_size() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return state->initialized;
  }

  [[nodiscard]] std::size_t remaining() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return state->bytes.size() - state->filled;
  }

  void clear() {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    state->filled = 0;
  }

  void advance(std::size_t amount) {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    if (amount > state->initialized - state->filled) {
      throw std::out_of_range{"ReadBuf advance 超过 initialized"};
    }
    state->filled += amount;
  }

  void set_filled(std::size_t filled) {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    detail::require_filled_bound(*state, filled);
    state->filled = filled;
  }

  void put_slice(std::span<const std::byte> bytes) {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    detail::put_bytes(*state, bytes);
  }

  [[nodiscard]] std::vector<std::byte> filled_snapshot() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return detail::prefix_snapshot(*state, state->filled);
  }

  [[nodiscard]] std::vector<std::byte> initialized_snapshot() const {
    const auto state = require_state();
    auto guard = detail::ReadBufferOwnerGuard::acquire(state);
    return detail::prefix_snapshot(*state, state->initialized);
  }

  /**
   * 同步取得供一个异步 read operation 持有的独占租约。
   *
   * 该调用发生在 lazy Task 返回前；因此未 poll Task 也已经占用 Rust `&mut`
   * 对应的独占窗口，Task 析构会释放窗口。
   */
  [[nodiscard]] MutableBufferLease lease_mut() {
    const auto state = require_state();
    detail::ReadBufferAccess expected =
        detail::ReadBufferAccess::idle;
    if (!state->access.compare_exchange_strong(
            expected,
            detail::ReadBufferAccess::lease,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      if (expected == detail::ReadBufferAccess::lease) {
        throw std::logic_error{"ReadBuf 已有活动可变 operation"};
      }
      throw std::logic_error{"ReadBuf 状态正被同步访问"};
    }
    auto generation =
        state->generation.fetch_add(
            1,
            std::memory_order_relaxed) +
        1;
    if (generation == 0) {
      generation =
          state->generation.fetch_add(
              1,
              std::memory_order_relaxed) +
          1;
    }
    return MutableBufferLease{state, generation};
  }

 private:
  explicit ReadBuf(
      std::shared_ptr<detail::ReadBufferState> state) noexcept
      : state_{std::move(state)} {}

  [[nodiscard]] std::shared_ptr<detail::ReadBufferState> require_state() const {
    if (!state_) {
      throw std::logic_error{"ReadBuf 已移动"};
    }
    return state_;
  }

  std::shared_ptr<detail::ReadBufferState> state_;
};

}  // namespace cio::io
