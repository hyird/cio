#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "cio/io/async_read.hpp"
#include "cio/io/async_write.hpp"

namespace cio::io {

namespace detail {

template <typename State>
class EndpointOperationGuard final {
 public:
  EndpointOperationGuard() noexcept = default;
  EndpointOperationGuard(const EndpointOperationGuard&) = delete;
  EndpointOperationGuard& operator=(const EndpointOperationGuard&) = delete;

  EndpointOperationGuard(EndpointOperationGuard&& other) noexcept
      : state_{std::move(other.state_)},
        generation_{std::exchange(other.generation_, 0)} {}

  EndpointOperationGuard& operator=(
      EndpointOperationGuard&& other) noexcept {
    if (this != &other) {
      release();
      state_ = std::move(other.state_);
      generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
  }

  ~EndpointOperationGuard() {
    release();
  }

  [[nodiscard]] static EndpointOperationGuard acquire(
      const std::shared_ptr<State>& state) {
    if (!state) {
      throw std::logic_error{"内存 I/O 端点已移动"};
    }
    std::unique_lock state_lock{
        state->mutex,
        std::try_to_lock};
    if (!state_lock.owns_lock()) {
      throw std::logic_error{"内存 I/O 端点状态正被同步观察"};
    }
    bool expected = false;
    if (!state->operation_active.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      throw std::logic_error{"内存 I/O 端点已有活动 operation"};
    }
    auto generation =
        state->operation_generation.fetch_add(
            1,
            std::memory_order_relaxed) +
        1;
    if (generation == 0) {
      generation =
          state->operation_generation.fetch_add(
              1,
              std::memory_order_relaxed) +
          1;
    }
    return EndpointOperationGuard{
        state,
        generation};
  }

  void require_current(const State& state) const {
    if (!state_ || generation_ == 0 ||
        !state.operation_active.load(std::memory_order_acquire) ||
        state.operation_generation.load(std::memory_order_relaxed) !=
            generation_) {
      throw std::logic_error{"内存 I/O operation generation 已失效"};
    }
  }

 private:
  EndpointOperationGuard(
      std::shared_ptr<State> state,
      std::uint64_t generation) noexcept
      : state_{std::move(state)},
        generation_{generation} {}

  void release() noexcept {
    if (!state_ || generation_ == 0) {
      return;
    }
    if (state_->operation_active.load(std::memory_order_acquire) &&
        state_->operation_generation.load(std::memory_order_relaxed) ==
            generation_) {
      state_->operation_active.store(
          false,
          std::memory_order_release);
    }
    state_.reset();
    generation_ = 0;
  }

  std::shared_ptr<State> state_;
  std::uint64_t generation_{0};
};

template <typename State>
struct EndpointSessionState final {
  EndpointSessionState(
      std::shared_ptr<State> endpoint_state,
      EndpointOperationGuard<State> endpoint_operation) noexcept
      : endpoint{std::move(endpoint_state)},
        endpoint_guard{std::move(endpoint_operation)} {}

  std::shared_ptr<State> endpoint;
  EndpointOperationGuard<State> endpoint_guard;
  std::atomic<bool> primitive_active{false};
  std::atomic<std::uint64_t> primitive_generation{0};
};

/**
 * 一个 Session 内单个 primitive 的 generation guard。
 *
 * guard 只串行化 primitive；端点级独占由 EndpointSessionState 中的
 * endpoint_guard 保持，不能随一次 partial primitive 完成而释放。
 */
template <typename State>
class SessionPrimitiveGuard final {
 public:
  using SessionState = EndpointSessionState<State>;

  SessionPrimitiveGuard() noexcept = default;
  SessionPrimitiveGuard(const SessionPrimitiveGuard&) = delete;
  SessionPrimitiveGuard& operator=(const SessionPrimitiveGuard&) = delete;

  SessionPrimitiveGuard(SessionPrimitiveGuard&& other) noexcept
      : session_{std::move(other.session_)},
        generation_{std::exchange(other.generation_, 0)} {}

  SessionPrimitiveGuard& operator=(
      SessionPrimitiveGuard&& other) noexcept {
    if (this != &other) {
      release();
      session_ = std::move(other.session_);
      generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
  }

  ~SessionPrimitiveGuard() {
    release();
  }

  [[nodiscard]] static SessionPrimitiveGuard acquire(
      const std::shared_ptr<SessionState>& session) {
    if (!session) {
      throw std::logic_error{"内存 I/O Session 已移动"};
    }
    bool expected = false;
    if (!session->primitive_active.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      throw std::logic_error{"内存 I/O Session 已有活动 primitive"};
    }
    auto generation =
        session->primitive_generation.fetch_add(
            1,
            std::memory_order_relaxed) +
        1;
    if (generation == 0) {
      generation =
          session->primitive_generation.fetch_add(
              1,
              std::memory_order_relaxed) +
          1;
    }
    return SessionPrimitiveGuard{
        session,
        generation};
  }

  void require_current(const SessionState& session) const {
    if (!session_ || generation_ == 0 ||
        !session.primitive_active.load(std::memory_order_acquire) ||
        session.primitive_generation.load(std::memory_order_relaxed) !=
            generation_) {
      throw std::logic_error{
          "内存 I/O primitive generation 已失效"};
    }
  }

 private:
  SessionPrimitiveGuard(
      std::shared_ptr<SessionState> session,
      std::uint64_t generation) noexcept
      : session_{std::move(session)},
        generation_{generation} {}

  void release() noexcept {
    if (!session_ || generation_ == 0) {
      return;
    }
    if (session_->primitive_active.load(std::memory_order_acquire) &&
        session_->primitive_generation.load(std::memory_order_relaxed) ==
            generation_) {
      session_->primitive_active.store(
          false,
          std::memory_order_release);
    }
    session_.reset();
    generation_ = 0;
  }

  std::shared_ptr<SessionState> session_;
  std::uint64_t generation_{0};
};

struct MemoryReaderState final {
  MemoryReaderState(SharedBuffer bytes, std::size_t maximum_chunk)
      : source{std::move(bytes)},
        max_chunk{maximum_chunk} {}

  mutable std::mutex mutex;
  SharedBuffer source;
  std::size_t position{0};
  std::size_t max_chunk{std::numeric_limits<std::size_t>::max()};
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
};

struct MemoryWriterState final {
  MemoryWriterState(std::size_t maximum_chunk, bool return_zero)
      : bytes{std::make_shared<const std::vector<std::byte>>()},
        max_chunk{maximum_chunk},
        zero_write{return_zero} {}

  mutable std::mutex mutex;
  std::shared_ptr<const std::vector<std::byte>> bytes;
  std::size_t max_chunk{std::numeric_limits<std::size_t>::max()};
  bool zero_write{false};
  bool shutdown{false};
  std::size_t flush_count{0};
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
};

}  // namespace detail

class MemoryReader;

/**
 * MemoryReader 的移动专属端点级 read 会话。
 *
 * 会话从创建到最后一个 primitive 终态持续持有 endpoint generation。一次
 * partial read 完成只释放 primitive guard，不释放整个 endpoint session。
 */
class MemoryReaderSession final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_async_read_session = true;

  MemoryReaderSession(const MemoryReaderSession&) = delete;
  MemoryReaderSession& operator=(const MemoryReaderSession&) = delete;
  MemoryReaderSession(MemoryReaderSession&&) noexcept = default;
  MemoryReaderSession& operator=(MemoryReaderSession&&) noexcept = default;
  ~MemoryReaderSession() = default;

  /**
   * 单次读取最多 max_chunk 个字节。
   *
   * initiating call 同步取得 Session 内 primitive guard。Task 未 poll 析构
   * 不推进 cursor；Pending primitive 取消后端点仍由 Session 独占。
   */
  [[nodiscard]] Task<IoResult<MutableBufferLease>> read(
      MutableBufferLease buffer) {
    auto guard =
        detail::SessionPrimitiveGuard<
            detail::MemoryReaderState>::acquire(session_);
    return read_impl(session_, std::move(guard), std::move(buffer));
  }

 private:
  using SessionState =
      detail::EndpointSessionState<detail::MemoryReaderState>;

  explicit MemoryReaderSession(
      std::shared_ptr<SessionState> session) noexcept
      : session_{std::move(session)} {}

  [[nodiscard]] static MemoryReaderSession acquire(
      const std::shared_ptr<detail::MemoryReaderState>& state) {
    auto endpoint_guard =
        detail::EndpointOperationGuard<
            detail::MemoryReaderState>::acquire(state);
    return MemoryReaderSession{
        std::make_shared<SessionState>(
            state,
            std::move(endpoint_guard))};
  }

  [[nodiscard]] static Task<IoResult<MutableBufferLease>> read_impl(
      std::shared_ptr<SessionState> session,
      detail::SessionPrimitiveGuard<
          detail::MemoryReaderState> primitive_guard,
      MutableBufferLease buffer) {
    const auto state = session->endpoint;
    std::size_t position = 0;
    std::size_t source_remaining = 0;
    std::size_t max_chunk = 0;
    primitive_guard.require_current(*session);
    {
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      position = state->position;
      source_remaining = state->source.size() - state->position;
      max_chunk = state->max_chunk;
    }

    const auto amount = std::min(
        {source_remaining, buffer.remaining(), max_chunk});
    std::vector<std::byte> chunk;
    chunk.reserve(amount);
    for (std::size_t index = 0; index < amount; ++index) {
      chunk.push_back(state->source.at(position + index));
    }

    if (!chunk.empty()) {
      buffer.put_slice(std::span<const std::byte>{chunk});
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      state->position += amount;
    }

    co_return IoResult<MutableBufferLease>::success(std::move(buffer));
  }

  std::shared_ptr<SessionState> session_;

  friend class MemoryReader;
};

/**
 * 已拥有输入的确定性内存 AsyncRead 端点。
 *
 * MemoryReader 用 max_chunk 稳定制造部分 read。open_read_session 在 initiating
 * call 同步取得覆盖完整组合 Future 的端点独占权。
 */
class MemoryReader final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;
  static constexpr bool cio_async_read_endpoint = true;
  using ReadSession = MemoryReaderSession;

  [[nodiscard]] static MemoryReader from(
      SharedBuffer source,
      std::size_t max_chunk = std::numeric_limits<std::size_t>::max()) {
    if (max_chunk == 0) {
      throw std::invalid_argument{
          "MemoryReader max_chunk 必须大于零"};
    }
    return MemoryReader{
        std::make_shared<detail::MemoryReaderState>(
            std::move(source),
            max_chunk)};
  }

  MemoryReader(const MemoryReader&) noexcept = default;
  MemoryReader& operator=(const MemoryReader&) noexcept = default;
  MemoryReader(MemoryReader&&) noexcept = default;
  MemoryReader& operator=(MemoryReader&&) noexcept = default;
  ~MemoryReader() = default;

  /**
   * 同步取得移动专属 read Session。
   *
   * 所有权：Session 强拥有 endpoint state。生命周期/取消：从本调用成功到
   * Session 及全部 child primitive 析构，所有别名 initiating call 都被拒绝；
   * 未 poll Session 本身不推进 cursor。线程迁移：Session 为 Send/non-Sync。
   * 阻塞：只执行有界同步状态转换，不执行 I/O。
   */
  [[nodiscard]] ReadSession open_read_session() const {
    return ReadSession::acquire(require_state());
  }

  /**
   * 兼容单次 primitive 的便利入口。
   *
   * 内部取得一次性 Session，Task 持有 Session 与 buffer lease 到终态；取消
   * Pending primitive 不产生晚到 buffer 写入。本内存端点不执行阻塞 I/O。
   */
  [[nodiscard]] Task<IoResult<MutableBufferLease>> read(
      MutableBufferLease buffer) const {
    auto session = open_read_session();
    return session.read(std::move(buffer));
  }

  [[nodiscard]] std::size_t position() const {
    const auto state = require_state();
    require_idle(*state);
    std::unique_lock lock{state->mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      throw std::logic_error{"MemoryReader 状态正被同步观察"};
    }
    require_idle(*state);
    return state->position;
  }

  [[nodiscard]] std::size_t remaining() const {
    const auto state = require_state();
    require_idle(*state);
    std::unique_lock lock{state->mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      throw std::logic_error{"MemoryReader 状态正被同步观察"};
    }
    require_idle(*state);
    return state->source.size() - state->position;
  }

 private:
  explicit MemoryReader(
      std::shared_ptr<detail::MemoryReaderState> state) noexcept
      : state_{std::move(state)} {}

  [[nodiscard]] std::shared_ptr<detail::MemoryReaderState>
  require_state() const {
    if (!state_) {
      throw std::logic_error{"MemoryReader 已移动"};
    }
    return state_;
  }

  static void require_idle(const detail::MemoryReaderState& state) {
    if (state.operation_active.load(std::memory_order_acquire)) {
      throw std::logic_error{
          "MemoryReader Session 活动期间不能观察状态"};
    }
  }

  std::shared_ptr<detail::MemoryReaderState> state_;
};

class MemoryWriter;

/**
 * MemoryWriter 的移动专属端点级 write 会话。
 *
 * write/flush/shutdown primitive 在会话内串行化；一次 primitive 完成不会释放
 * endpoint generation，因而组合 operation 的 partial 边界不存在别名插入窗。
 */
class MemoryWriterSession final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = false;
  static constexpr bool cio_async_write_session = true;

  MemoryWriterSession(const MemoryWriterSession&) = delete;
  MemoryWriterSession& operator=(const MemoryWriterSession&) = delete;
  MemoryWriterSession(MemoryWriterSession&&) noexcept = default;
  MemoryWriterSession& operator=(MemoryWriterSession&&) noexcept = default;
  ~MemoryWriterSession() = default;

  /**
   * 单次写入最多 max_chunk 个字节。
   *
   * 输入 lease、Session 和 primitive generation 都由 Task 持有；未 poll
   * 析构不提交字节，也不会提前释放外层 Session。
   */
  [[nodiscard]] Task<IoResult<std::size_t>> write(
      ConstBufferLease buffer) {
    auto guard =
        detail::SessionPrimitiveGuard<
            detail::MemoryWriterState>::acquire(session_);
    return write_impl(session_, std::move(guard), std::move(buffer));
  }

  /**
   * 在当前 Session 内记录一次 flush。
   *
   * 所有权/生命周期：primitive Task 强拥有 Session control；取消或未 poll
   * 不增加计数，且不会释放仍存活的外层 Session。线程迁移为 Send/non-Sync，
   * 本内存端点不执行阻塞 I/O。
   */
  [[nodiscard]] Task<IoResult<void>> flush() {
    auto guard =
        detail::SessionPrimitiveGuard<
            detail::MemoryWriterState>::acquire(session_);
    return flush_impl(session_, std::move(guard));
  }

  /**
   * 在当前 Session 内推进 writer 终态。
   *
   * 首次成功隐含一次 flush，重复调用幂等；未 poll/取消不提交终态，也不会
   * 提前释放 Session。线程迁移为 Send/non-Sync，不执行阻塞 I/O。
   */
  [[nodiscard]] Task<IoResult<void>> shutdown() {
    auto guard =
        detail::SessionPrimitiveGuard<
            detail::MemoryWriterState>::acquire(session_);
    return shutdown_impl(session_, std::move(guard));
  }

 private:
  using SessionState =
      detail::EndpointSessionState<detail::MemoryWriterState>;

  explicit MemoryWriterSession(
      std::shared_ptr<SessionState> session) noexcept
      : session_{std::move(session)} {}

  [[nodiscard]] static MemoryWriterSession acquire(
      const std::shared_ptr<detail::MemoryWriterState>& state) {
    auto endpoint_guard =
        detail::EndpointOperationGuard<
            detail::MemoryWriterState>::acquire(state);
    return MemoryWriterSession{
        std::make_shared<SessionState>(
            state,
            std::move(endpoint_guard))};
  }

  [[nodiscard]] static Task<IoResult<std::size_t>> write_impl(
      std::shared_ptr<SessionState> session,
      detail::SessionPrimitiveGuard<
          detail::MemoryWriterState> primitive_guard,
      ConstBufferLease buffer) {
    const auto state = session->endpoint;
    std::size_t amount = 0;
    std::size_t current_size = 0;
    bool shutdown = false;
    primitive_guard.require_current(*session);
    {
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      shutdown = state->shutdown;
      current_size = state->bytes->size();
      if (!shutdown && !buffer.empty() && !state->zero_write) {
        amount = std::min(buffer.size(), state->max_chunk);
      }
    }

    if (shutdown) {
      co_return IoResult<std::size_t>::failure(
          Error::broken_pipe());
    }
    if (amount >
        std::numeric_limits<std::size_t>::max() - current_size) {
      co_return IoResult<std::size_t>::failure(
          Error::other("MemoryWriter 长度溢出"));
    }

    if (amount != 0) {
      auto input = buffer.sublease(0, amount).snapshot();
      std::shared_ptr<const std::vector<std::byte>> current;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        current = state->bytes;
      }
      auto next = std::make_shared<std::vector<std::byte>>();
      next->reserve(current_size + amount);
      next->insert(next->end(), current->begin(), current->end());
      for (const auto value : input) {
        next->push_back(value);
      }
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->bytes = std::move(next);
      }
    }
    co_return IoResult<std::size_t>::success(amount);
  }

  [[nodiscard]] static Task<IoResult<void>> flush_impl(
      std::shared_ptr<SessionState> session,
      detail::SessionPrimitiveGuard<
          detail::MemoryWriterState> primitive_guard) {
    const auto state = session->endpoint;
    primitive_guard.require_current(*session);
    {
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      if (state->flush_count <
          std::numeric_limits<std::size_t>::max()) {
        ++state->flush_count;
      }
    }
    co_return IoResult<void>::success();
  }

  [[nodiscard]] static Task<IoResult<void>> shutdown_impl(
      std::shared_ptr<SessionState> session,
      detail::SessionPrimitiveGuard<
          detail::MemoryWriterState> primitive_guard) {
    const auto state = session->endpoint;
    primitive_guard.require_current(*session);
    {
      const std::lock_guard lock{state->mutex};
      session->endpoint_guard.require_current(*state);
      if (!state->shutdown) {
        if (state->flush_count <
            std::numeric_limits<std::size_t>::max()) {
          ++state->flush_count;
        }
        state->shutdown = true;
      }
    }
    co_return IoResult<void>::success();
  }

  std::shared_ptr<SessionState> session_;

  friend class MemoryWriter;
};

/**
 * 收集写入字节的确定性内存 AsyncWrite 端点。
 *
 * max_chunk 用于部分 write；zero_writer 对非空输入成功返回零，供后续
 * write_all 的 WriteZero 语义测试。shutdown 首次成功时隐含一次 flush。
 */
class MemoryWriter final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;
  static constexpr bool cio_async_write_endpoint = true;
  using WriteSession = MemoryWriterSession;

  [[nodiscard]] static MemoryWriter with_max_chunk(
      std::size_t max_chunk = std::numeric_limits<std::size_t>::max()) {
    if (max_chunk == 0) {
      throw std::invalid_argument{
          "MemoryWriter max_chunk 必须大于零；write-zero 请使用 zero_writer"};
    }
    return MemoryWriter{
        std::make_shared<detail::MemoryWriterState>(
            max_chunk,
            false)};
  }

  [[nodiscard]] static MemoryWriter zero_writer() {
    return MemoryWriter{
        std::make_shared<detail::MemoryWriterState>(
            std::numeric_limits<std::size_t>::max(),
            true)};
  }

  MemoryWriter(const MemoryWriter&) noexcept = default;
  MemoryWriter& operator=(const MemoryWriter&) noexcept = default;
  MemoryWriter(MemoryWriter&&) noexcept = default;
  MemoryWriter& operator=(MemoryWriter&&) noexcept = default;
  ~MemoryWriter() = default;

  /**
   * 同步取得移动专属 write Session。
   *
   * Session 从本调用成功到自身及 child primitive 全部析构持续排斥 endpoint
   * 别名，覆盖组合操作所有 partial 边界。Session 为 Send/non-Sync；这里只
   * 执行有界同步状态转换，不执行 I/O。
   */
  [[nodiscard]] WriteSession open_write_session() const {
    return WriteSession::acquire(require_state());
  }

  /**
   * 兼容单次 write primitive 的便利入口。
   *
   * Task 持有一次性 Session 和输入 lease；未 poll/取消不提交字节，线程迁移
   * 为 Send/non-Sync，本内存端点不阻塞。
   */
  [[nodiscard]] Task<IoResult<std::size_t>> write(
      ConstBufferLease buffer) const {
    auto session = open_write_session();
    return session.write(std::move(buffer));
  }

  /**
   * 兼容单次 flush primitive 的便利入口。
   *
   * 取得一次性 Session；未 poll/取消不增加 flush 计数，不阻塞 worker。
   */
  [[nodiscard]] Task<IoResult<void>> flush() const {
    auto session = open_write_session();
    return session.flush();
  }

  /**
   * 兼容单次 shutdown primitive 的便利入口。
   *
   * 成功隐含 flush 并进入终态；未 poll/取消不提交终态，不阻塞 worker。
   */
  [[nodiscard]] Task<IoResult<void>> shutdown() const {
    auto session = open_write_session();
    return session.shutdown();
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const auto state = require_state();
    std::shared_ptr<const std::vector<std::byte>> bytes;
    {
      require_idle(*state);
      std::unique_lock lock{state->mutex, std::try_to_lock};
      if (!lock.owns_lock()) {
        throw std::logic_error{"MemoryWriter 状态正被同步观察"};
      }
      require_idle(*state);
      bytes = state->bytes;
    }
    // 只在短临界区取得不可变版本；大块复制不占用端点状态锁。
    return *bytes;
  }

  [[nodiscard]] std::size_t flush_count() const {
    const auto state = require_state();
    require_idle(*state);
    std::unique_lock lock{state->mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      throw std::logic_error{"MemoryWriter 状态正被同步观察"};
    }
    require_idle(*state);
    return state->flush_count;
  }

  [[nodiscard]] bool is_shutdown() const {
    const auto state = require_state();
    require_idle(*state);
    std::unique_lock lock{state->mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      throw std::logic_error{"MemoryWriter 状态正被同步观察"};
    }
    require_idle(*state);
    return state->shutdown;
  }

 private:
  explicit MemoryWriter(
      std::shared_ptr<detail::MemoryWriterState> state) noexcept
      : state_{std::move(state)} {}

  [[nodiscard]] std::shared_ptr<detail::MemoryWriterState>
  require_state() const {
    if (!state_) {
      throw std::logic_error{"MemoryWriter 已移动"};
    }
    return state_;
  }

  static void require_idle(const detail::MemoryWriterState& state) {
    if (state.operation_active.load(std::memory_order_acquire)) {
      throw std::logic_error{
          "MemoryWriter Session 活动期间不能观察状态"};
    }
  }

  std::shared_ptr<detail::MemoryWriterState> state_;
};

static_assert(AsyncRead<MemoryReader>);
static_assert(AsyncWrite<MemoryWriter>);

}  // namespace cio::io
