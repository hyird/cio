#include <algorithm>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/cio.hpp"

namespace {

using cio::io::ConstBufferLease;
using cio::io::ConstBufferSequence;
using cio::io::ErrorKind;
using cio::io::MemoryReader;
using cio::io::MemoryWriter;
using cio::io::OwnedBuffer;
using cio::io::ReadBuf;
using cio::io::SharedBuffer;
using cio::runtime::Runtime;

class GatedReader final {
 public:
  static constexpr bool cio_async_read_endpoint = true;

  explicit GatedReader(SharedBuffer source)
      : source_{std::move(source)},
        operation_state_{std::make_shared<OperationState>()},
        live_instances_{std::make_shared<std::atomic<std::size_t>>(1)} {}

  GatedReader(const GatedReader& other)
      : gate_{other.gate_},
        source_{other.source_},
        operation_state_{other.operation_state_},
        live_instances_{other.live_instances_} {
    live_instances_->fetch_add(1, std::memory_order_relaxed);
  }

  GatedReader& operator=(const GatedReader&) = delete;

  GatedReader(GatedReader&& other) noexcept
      : gate_{std::move(other.gate_)},
        source_{std::move(other.source_)},
        operation_state_{std::move(other.operation_state_)},
        live_instances_{std::move(other.live_instances_)},
        owns_instance_{std::exchange(other.owns_instance_, false)} {}

  GatedReader& operator=(GatedReader&&) = delete;

  ~GatedReader() {
    if (owns_instance_) {
      live_instances_->fetch_sub(1, std::memory_order_relaxed);
    }
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<cio::io::MutableBufferLease>>
  read(cio::io::MutableBufferLease buffer) const {
    auto guard = OperationGuard::acquire(operation_state_);
    return read_pending(std::move(buffer), std::move(guard));
  }

  void open_one() const {
    gate_.notify_one();
  }

  [[nodiscard]] std::size_t live_instances() const noexcept {
    return live_instances_->load(std::memory_order_relaxed);
  }

 private:
  struct OperationState final {
    std::mutex mutex;
    bool active{false};
  };

  class OperationGuard final {
   public:
    OperationGuard(const OperationGuard&) = delete;
    OperationGuard& operator=(const OperationGuard&) = delete;

    OperationGuard(OperationGuard&& other) noexcept
        : state_{std::move(other.state_)} {}

    OperationGuard& operator=(OperationGuard&&) = delete;

    ~OperationGuard() {
      if (state_) {
        const std::lock_guard lock{state_->mutex};
        state_->active = false;
      }
    }

    [[nodiscard]] static OperationGuard acquire(
        const std::shared_ptr<OperationState>& state) {
      const std::lock_guard lock{state->mutex};
      if (state->active) {
        throw std::logic_error{"GatedReader 已有活动 operation"};
      }
      state->active = true;
      return OperationGuard{state};
    }

   private:
    explicit OperationGuard(
        std::shared_ptr<OperationState> state) noexcept
        : state_{std::move(state)} {}

    std::shared_ptr<OperationState> state_;
  };

  [[nodiscard]] cio::Task<cio::io::IoResult<cio::io::MutableBufferLease>>
  read_pending(
      cio::io::MutableBufferLease buffer,
      OperationGuard guard) const {
    co_await gate_.notified();
    (void)guard;
    const auto value = source_.snapshot();
    const auto amount = std::min(value.size(), buffer.remaining());
    buffer.put_slice(
        std::span<const std::byte>{value}.first(amount));
    co_return cio::io::IoResult<cio::io::MutableBufferLease>::success(
        std::move(buffer));
  }

  cio::sync::Notify gate_;
  SharedBuffer source_;
  std::shared_ptr<OperationState> operation_state_;
  std::shared_ptr<std::atomic<std::size_t>> live_instances_;
  bool owns_instance_{true};
};

class GatedWriter final {
 public:
  static constexpr bool cio_async_write_endpoint = true;

  GatedWriter()
      : state_{std::make_shared<State>()},
        live_instances_{std::make_shared<std::atomic<std::size_t>>(1)} {}

  GatedWriter(const GatedWriter& other)
      : gate_{other.gate_},
        state_{other.state_},
        live_instances_{other.live_instances_} {
    live_instances_->fetch_add(1, std::memory_order_relaxed);
  }

  GatedWriter& operator=(const GatedWriter&) = delete;

  GatedWriter(GatedWriter&& other) noexcept
      : gate_{std::move(other.gate_)},
        state_{std::move(other.state_)},
        live_instances_{std::move(other.live_instances_)},
        owns_instance_{std::exchange(other.owns_instance_, false)} {}

  GatedWriter& operator=(GatedWriter&&) = delete;

  ~GatedWriter() {
    if (owns_instance_) {
      live_instances_->fetch_sub(1, std::memory_order_relaxed);
    }
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>> write(
      ConstBufferLease buffer) const {
    auto guard = OperationGuard::acquire(state_);
    return write_pending(std::move(buffer), std::move(guard));
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush() const {
    auto guard = OperationGuard::acquire(state_);
    return flush_ready(std::move(guard));
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown() const {
    auto guard = OperationGuard::acquire(state_);
    return shutdown_ready(std::move(guard));
  }

  void open_one() const {
    gate_.notify_one();
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const std::lock_guard lock{state_->mutex};
    require_idle(*state_);
    return state_->bytes;
  }

  [[nodiscard]] bool is_shutdown() const {
    const std::lock_guard lock{state_->mutex};
    require_idle(*state_);
    return state_->shutdown;
  }

  [[nodiscard]] std::size_t live_instances() const noexcept {
    return live_instances_->load(std::memory_order_relaxed);
  }

 private:
  struct State final {
    std::mutex mutex;
    std::vector<std::byte> bytes;
    bool shutdown{false};
    std::size_t flush_count{0};
    bool active{false};
  };

  class OperationGuard final {
   public:
    OperationGuard(const OperationGuard&) = delete;
    OperationGuard& operator=(const OperationGuard&) = delete;

    OperationGuard(OperationGuard&& other) noexcept
        : state_{std::move(other.state_)} {}

    OperationGuard& operator=(OperationGuard&&) = delete;

    ~OperationGuard() {
      if (state_) {
        const std::lock_guard lock{state_->mutex};
        state_->active = false;
      }
    }

    [[nodiscard]] static OperationGuard acquire(
        const std::shared_ptr<State>& state) {
      const std::lock_guard lock{state->mutex};
      if (state->active) {
        throw std::logic_error{"GatedWriter 已有活动 operation"};
      }
      state->active = true;
      return OperationGuard{state};
    }

   private:
    explicit OperationGuard(std::shared_ptr<State> state) noexcept
        : state_{std::move(state)} {}

    std::shared_ptr<State> state_;
  };

  [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>> write_pending(
      ConstBufferLease buffer,
      OperationGuard guard) const {
    co_await gate_.notified();
    (void)guard;
    auto value = buffer.snapshot();
    {
      const std::lock_guard lock{state_->mutex};
      if (state_->shutdown) {
        co_return cio::io::IoResult<std::size_t>::failure(
            cio::io::Error::broken_pipe());
      }
      state_->bytes.insert(
          state_->bytes.end(),
          value.begin(),
          value.end());
    }
    co_return cio::io::IoResult<std::size_t>::success(value.size());
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush_ready(
      OperationGuard guard) const {
    (void)guard;
    {
      const std::lock_guard lock{state_->mutex};
      ++state_->flush_count;
    }
    co_return cio::io::IoResult<void>::success();
  }

  [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown_ready(
      OperationGuard guard) const {
    (void)guard;
    {
      const std::lock_guard lock{state_->mutex};
      if (!state_->shutdown) {
        ++state_->flush_count;
        state_->shutdown = true;
      }
    }
    co_return cio::io::IoResult<void>::success();
  }

  static void require_idle(const State& state) {
    if (state.active) {
      throw std::logic_error{"GatedWriter operation 活动期间不能观察状态"};
    }
  }

  cio::sync::Notify gate_;
  std::shared_ptr<State> state_;
  std::shared_ptr<std::atomic<std::size_t>> live_instances_;
  bool owns_instance_{true};
};

class SessionGatedReader final {
 private:
  struct State final {
    explicit State(SharedBuffer input)
        : source{std::move(input)} {}

    mutable std::mutex mutex;
    std::atomic<bool> operation_active{false};
    std::atomic<std::uint64_t> operation_generation{0};
    cio::sync::Notify gate;
    SharedBuffer source;
    std::atomic<std::size_t> active_sessions{0};
  };

  using EndpointSession =
      cio::io::detail::EndpointSessionState<State>;

  struct SessionOwner final {
    SessionOwner(
        std::shared_ptr<State> endpoint_state,
        std::shared_ptr<EndpointSession> endpoint_session)
        : endpoint{std::move(endpoint_state)},
          session{std::move(endpoint_session)} {
      endpoint->active_sessions.fetch_add(
          1,
          std::memory_order_relaxed);
    }

    ~SessionOwner() {
      endpoint->active_sessions.fetch_sub(
          1,
          std::memory_order_relaxed);
    }

    std::shared_ptr<State> endpoint;
    std::shared_ptr<EndpointSession> session;
  };

 public:
  class ReadSession final {
   public:
    static constexpr bool cio_async_read_session = true;

    ReadSession(const ReadSession&) = delete;
    ReadSession& operator=(const ReadSession&) = delete;
    ReadSession(ReadSession&&) noexcept = default;
    ReadSession& operator=(ReadSession&&) noexcept = default;
    ~ReadSession() = default;

    [[nodiscard]] cio::Task<
        cio::io::IoResult<cio::io::MutableBufferLease>>
    read(cio::io::MutableBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<State>::acquire(
              owner_->session);
      return read_pending(
          owner_,
          std::move(primitive),
          std::move(buffer));
    }

   private:
    explicit ReadSession(
        std::shared_ptr<SessionOwner> owner) noexcept
        : owner_{std::move(owner)} {}

    [[nodiscard]] static cio::Task<
        cio::io::IoResult<cio::io::MutableBufferLease>>
    read_pending(
        std::shared_ptr<SessionOwner> owner,
        cio::io::detail::SessionPrimitiveGuard<State> primitive,
        cio::io::MutableBufferLease buffer) {
      co_await owner->endpoint->gate.notified();
      {
        const std::lock_guard lock{owner->endpoint->mutex};
        owner->session->endpoint_guard.require_current(
            *owner->endpoint);
      }
      (void)primitive;
      const auto value = owner->endpoint->source.snapshot();
      const auto amount =
          std::min(value.size(), buffer.remaining());
      buffer.put_slice(
          std::span<const std::byte>{value}.first(amount));
      co_return cio::io::IoResult<
          cio::io::MutableBufferLease>::success(
          std::move(buffer));
    }

    std::shared_ptr<SessionOwner> owner_;

    friend class SessionGatedReader;
  };

  static constexpr bool cio_async_read_endpoint = true;

  explicit SessionGatedReader(SharedBuffer source)
      : state_{std::make_shared<State>(std::move(source))} {}

  [[nodiscard]] ReadSession open_read_session() const {
    auto endpoint_guard =
        cio::io::detail::EndpointOperationGuard<State>::acquire(
            state_);
    auto session = std::make_shared<EndpointSession>(
        state_,
        std::move(endpoint_guard));
    return ReadSession{
        std::make_shared<SessionOwner>(
            state_,
            std::move(session))};
  }

  void open_one() const {
    state_->gate.notify_one();
  }

  [[nodiscard]] std::size_t live_instances() const noexcept {
    return 1 + state_->active_sessions.load(
        std::memory_order_relaxed);
  }

 private:
  std::shared_ptr<State> state_;
};

class SessionGatedWriter final {
 private:
  struct State final {
    mutable std::mutex mutex;
    std::atomic<bool> operation_active{false};
    std::atomic<std::uint64_t> operation_generation{0};
    cio::sync::Notify gate;
    std::vector<std::byte> bytes;
    std::size_t flush_count{0};
    bool shutdown{false};
    std::atomic<std::size_t> active_sessions{0};
  };

  using EndpointSession =
      cio::io::detail::EndpointSessionState<State>;

  struct SessionOwner final {
    SessionOwner(
        std::shared_ptr<State> endpoint_state,
        std::shared_ptr<EndpointSession> endpoint_session)
        : endpoint{std::move(endpoint_state)},
          session{std::move(endpoint_session)} {
      endpoint->active_sessions.fetch_add(
          1,
          std::memory_order_relaxed);
    }

    ~SessionOwner() {
      endpoint->active_sessions.fetch_sub(
          1,
          std::memory_order_relaxed);
    }

    std::shared_ptr<State> endpoint;
    std::shared_ptr<EndpointSession> session;
  };

 public:
  class WriteSession final {
   public:
    static constexpr bool cio_async_write_session = true;

    WriteSession(const WriteSession&) = delete;
    WriteSession& operator=(const WriteSession&) = delete;
    WriteSession(WriteSession&&) noexcept = default;
    WriteSession& operator=(WriteSession&&) noexcept = default;
    ~WriteSession() = default;

    [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>> write(
        ConstBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<State>::acquire(
              owner_->session);
      return write_pending(
          owner_,
          std::move(primitive),
          std::move(buffer));
    }

    [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<State>::acquire(
              owner_->session);
      return flush_ready(owner_, std::move(primitive));
    }

    [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<State>::acquire(
              owner_->session);
      return shutdown_ready(owner_, std::move(primitive));
    }

   private:
    explicit WriteSession(
        std::shared_ptr<SessionOwner> owner) noexcept
        : owner_{std::move(owner)} {}

    [[nodiscard]] static cio::Task<
        cio::io::IoResult<std::size_t>>
    write_pending(
        std::shared_ptr<SessionOwner> owner,
        cio::io::detail::SessionPrimitiveGuard<State> primitive,
        ConstBufferLease buffer) {
      co_await owner->endpoint->gate.notified();
      const auto input = buffer.snapshot();
      {
        const std::lock_guard lock{owner->endpoint->mutex};
        owner->session->endpoint_guard.require_current(
            *owner->endpoint);
        if (owner->endpoint->shutdown) {
          co_return cio::io::IoResult<std::size_t>::failure(
              cio::io::Error::broken_pipe());
        }
        owner->endpoint->bytes.insert(
            owner->endpoint->bytes.end(),
            input.begin(),
            input.end());
      }
      (void)primitive;
      co_return cio::io::IoResult<std::size_t>::success(
          input.size());
    }

    [[nodiscard]] static cio::Task<cio::io::IoResult<void>>
    flush_ready(
        std::shared_ptr<SessionOwner> owner,
        cio::io::detail::SessionPrimitiveGuard<State> primitive) {
      {
        const std::lock_guard lock{owner->endpoint->mutex};
        owner->session->endpoint_guard.require_current(
            *owner->endpoint);
        ++owner->endpoint->flush_count;
      }
      (void)primitive;
      co_return cio::io::IoResult<void>::success();
    }

    [[nodiscard]] static cio::Task<cio::io::IoResult<void>>
    shutdown_ready(
        std::shared_ptr<SessionOwner> owner,
        cio::io::detail::SessionPrimitiveGuard<State> primitive) {
      {
        const std::lock_guard lock{owner->endpoint->mutex};
        owner->session->endpoint_guard.require_current(
            *owner->endpoint);
        if (!owner->endpoint->shutdown) {
          ++owner->endpoint->flush_count;
          owner->endpoint->shutdown = true;
        }
      }
      (void)primitive;
      co_return cio::io::IoResult<void>::success();
    }

    std::shared_ptr<SessionOwner> owner_;

    friend class SessionGatedWriter;
  };

  static constexpr bool cio_async_write_endpoint = true;

  SessionGatedWriter()
      : state_{std::make_shared<State>()} {}

  [[nodiscard]] WriteSession open_write_session() const {
    auto endpoint_guard =
        cio::io::detail::EndpointOperationGuard<State>::acquire(
            state_);
    auto session = std::make_shared<EndpointSession>(
        state_,
        std::move(endpoint_guard));
    return WriteSession{
        std::make_shared<SessionOwner>(
            state_,
            std::move(session))};
  }

  void open_one() const {
    state_->gate.notify_one();
  }

  [[nodiscard]] std::size_t live_instances() const noexcept {
    return 1 + state_->active_sessions.load(
        std::memory_order_relaxed);
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const std::lock_guard lock{state_->mutex};
    require_idle(*state_);
    return state_->bytes;
  }

  [[nodiscard]] bool is_shutdown() const {
    const std::lock_guard lock{state_->mutex};
    require_idle(*state_);
    return state_->shutdown;
  }

 private:
  static void require_idle(const State& state) {
    if (state.operation_active.load(std::memory_order_acquire)) {
      throw std::logic_error{
          "SessionGatedWriter Session 活动期间不能观察状态"};
    }
  }

  std::shared_ptr<State> state_;
};

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

template <typename Function>
void check_throws(Function&& function, std::string_view message) {
  bool thrown = false;
  try {
    std::forward<Function>(function)();
  } catch (const std::exception&) {
    thrown = true;
  }
  check(thrown, message);
}

std::vector<std::byte> bytes(std::initializer_list<unsigned int> values) {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

ConstBufferLease lease(std::initializer_list<unsigned int> values) {
  const auto value = bytes(values);
  return SharedBuffer::copy_from(
             std::span<const std::byte>{value})
      .lease();
}

void owned_and_shared_buffer_test() {
  const auto input = bytes({1, 2, 3, 4});
  auto owned = OwnedBuffer::copy_from(
      std::span<const std::byte>{input});
  check(owned.size() == 4 && owned.snapshot() == input,
        "OwnedBuffer 必须同步复制输入");

  auto shared = std::move(owned).share();
  check(shared.size() == 4 && shared.snapshot() == input,
        "SharedBuffer 必须拥有 move 后的字节");
  const auto middle = shared.subbuffer(1, 2);
  check(middle.snapshot() == bytes({2, 3}),
        "SharedBuffer 子区间错误");

  const auto lease = shared.lease();
  const auto second_lease = lease;
  check(second_lease.at(3) == std::byte{4},
        "ConstBufferLease 复制后必须保持存储生命期");

  auto moved_shared = std::move(shared);
  check(moved_shared.snapshot() == input && shared.snapshot() == input,
        "SharedBuffer move 后源句柄必须仍安全可观察");
  auto movable_lease = moved_shared.lease();
  auto moved_lease = std::move(movable_lease);
  check(moved_lease.snapshot() == input &&
            movable_lease.snapshot() == input,
        "ConstBufferLease move 后源租约必须仍安全拥有存储");
  check_throws(
      [&moved_shared] {
        (void)moved_shared.subbuffer(3, 2);
      },
      "SharedBuffer 越界子区间必须失败");
}

void buffer_sequence_test() {
  ConstBufferSequence sequence;
  sequence.push(SharedBuffer{}.lease());
  sequence.push(
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({1, 2})})
          .lease());
  sequence.push(
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({3})})
          .lease());
  check(sequence.segment_count() == 3 && sequence.total_size() == 3,
        "ConstBufferSequence 计数或总长度错误");
  check(sequence.segments()[0].empty() &&
            sequence.segments()[1].snapshot() == bytes({1, 2}),
        "ConstBufferSequence 必须保留空段和顺序");
}

void error_model_native_round_trip_test() {
  const std::error_code native{123, std::system_category()};
  const cio::io::Error error{
      ErrorKind::other,
      "native round trip",
      native};
  check(error.kind() == ErrorKind::other &&
            error.native_code().value() == native.value() &&
            error.native_code().category() == native.category() &&
            error.message() == "native round trip",
        "I/O Error 必须保留 kind、native category/value 和 message");
  const auto broken = cio::io::Error::broken_pipe();
  check(broken.kind() == ErrorKind::broken_pipe &&
            !broken.message().empty(),
        "I/O broken_pipe 分类或消息错误");
}

void read_buf_regions_and_bounds_test() {
  auto buffer = ReadBuf::with_capacity(4);
  check(buffer.capacity() == 4 && buffer.filled_size() == 0 &&
            buffer.initialized_size() == 4 && buffer.remaining() == 4,
        "ReadBuf 初始区域错误");

  const auto first = bytes({7, 8});
  buffer.put_slice(std::span<const std::byte>{first});
  check(buffer.filled_size() == 2 && buffer.remaining() == 2 &&
            buffer.filled_snapshot() == first,
        "ReadBuf put_slice 没有提交 filled");

  buffer.set_filled(1);
  buffer.advance(2);
  check(buffer.filled_size() == 3,
        "ReadBuf set_filled/advance 错误");
  const auto before = buffer.filled_snapshot();
  check_throws(
      [&buffer] {
        buffer.advance(2);
      },
      "ReadBuf 越界 advance 必须失败");
  check(buffer.filled_size() == 3 && buffer.filled_snapshot() == before,
        "ReadBuf 越界 advance 必须保持强保证");

  buffer.clear();
  check(buffer.filled_size() == 0 && buffer.initialized_size() == 4 &&
            buffer.initialized_snapshot() == bytes({7, 8, 0, 0}),
        "ReadBuf clear 不得修改 initialized 或内容");

  const auto overflow = bytes({1, 2, 3, 4, 5});
  check_throws(
      [&buffer, &overflow] {
        buffer.put_slice(std::span<const std::byte>{overflow});
      },
      "ReadBuf 越界 put_slice 必须失败");
  check(buffer.filled_size() == 0,
        "ReadBuf 越界 put_slice 不得提交部分进度");
}

void mutable_lease_exclusion_and_lifetime_test() {
  auto buffer = ReadBuf::with_capacity(3);
  {
    auto lease = buffer.lease_mut();
    check(lease.valid() && lease.capacity() == 3,
          "MutableBufferLease 必须有效");
    check_throws(
        [&buffer] {
          (void)buffer.remaining();
        },
        "活动 lease 期间 owner 访问必须失败");
    check_throws(
        [&buffer] {
          (void)buffer.lease_mut();
        },
        "活动 lease 期间第二个 operation 必须失败");

    const auto value = bytes({5, 6});
    lease.put_slice(std::span<const std::byte>{value});
    auto moved = std::move(lease);
    check(!lease.valid() && moved.filled_snapshot() == value,
          "MutableBufferLease move 必须保留唯一 generation");
    check_throws(
        [&lease] {
          (void)lease.remaining();
        },
        "move 后 MutableBufferLease 必须拒绝访问");
  }

  check(buffer.filled_snapshot() == bytes({5, 6}),
        "lease 析构后 owner 必须重新可见");

  auto orphaned = ReadBuf::with_capacity(2).lease_mut();
  const auto value = bytes({9});
  orphaned.put_slice(std::span<const std::byte>{value});
  check(orphaned.filled_snapshot() == value,
        "owner 先析构时 lease 必须继续拥有 storage");
}

void read_buf_cross_thread_lease_stress_test() {
  constexpr std::size_t rounds = 512;
  auto buffer = ReadBuf::with_capacity(1);
  std::barrier start{2};
  std::barrier checked{2};
  std::barrier released{2};
  std::barrier observed{2};
  std::atomic<std::size_t> rejected{0};
  std::atomic<std::size_t> visible{0};
  std::atomic<bool> unexpected{false};
  const auto value = bytes({7});

  std::jthread contender{[&] {
    for (std::size_t round = 0; round < rounds; ++round) {
      start.arrive_and_wait();
      try {
        (void)buffer.remaining();
      } catch (const std::logic_error&) {
        rejected.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
      }
      try {
        (void)buffer.lease_mut();
      } catch (const std::logic_error&) {
        rejected.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
      }
      checked.arrive_and_wait();
      released.arrive_and_wait();
      try {
        if (buffer.filled_snapshot() == value) {
          visible.fetch_add(1, std::memory_order_relaxed);
        }
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
      }
      observed.arrive_and_wait();
    }
  }};

  bool exact = true;
  for (std::size_t round = 0; round < rounds; ++round) {
    {
      auto lease = buffer.lease_mut();
      lease.put_slice(std::span<const std::byte>{value});
      start.arrive_and_wait();
      checked.arrive_and_wait();
      exact = exact &&
          rejected.load(std::memory_order_relaxed) ==
              (round + 1) * 2;
    }
    released.arrive_and_wait();
    observed.arrive_and_wait();
    exact = exact &&
        visible.load(std::memory_order_relaxed) == round + 1;
    buffer.clear();
  }

  check(exact && !unexpected.load(std::memory_order_relaxed),
        "ReadBuf 活动租约必须跨线程拒绝 owner/第二租约，"
        "并向另一线程发布释放前进度");
}

cio::Task<bool> partial_read_eof_and_zero_capacity_root() {
  const auto source_bytes = bytes({1, 2, 3, 4, 5});
  auto reader = MemoryReader::from(
      SharedBuffer::copy_from(
          std::span<const std::byte>{source_bytes}),
      2);
  auto buffer = ReadBuf::with_capacity(5);

  const auto first = co_await cio::io::read(reader, buffer);
  if (!first.has_value() || buffer.filled_snapshot() != bytes({1, 2}) ||
      reader.position() != 2) {
    co_return false;
  }
  const auto second = co_await cio::io::read(reader, buffer);
  const auto third = co_await cio::io::read(reader, buffer);
  if (!second.has_value() || !third.has_value() ||
      buffer.filled_snapshot() != source_bytes ||
      reader.position() != source_bytes.size()) {
    co_return false;
  }

  auto eof = ReadBuf::with_capacity(1);
  const auto eof_result = co_await cio::io::read(reader, eof);
  if (!eof_result.has_value() || eof.filled_size() != 0 ||
      reader.position() != source_bytes.size()) {
    co_return false;
  }

  auto zero = ReadBuf::with_capacity(0);
  auto fresh = MemoryReader::from(
      SharedBuffer::copy_from(
          std::span<const std::byte>{source_bytes}));
  const auto zero_result = co_await cio::io::read(fresh, zero);
  co_return zero_result.has_value() && zero.filled_size() == 0 &&
      fresh.position() == 0;
}

void unpolled_read_exclusion_and_release_test() {
  auto reader = MemoryReader::from(
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({1, 2})}));
  auto buffer = ReadBuf::with_capacity(2);
  auto other = ReadBuf::with_capacity(1);
  {
    auto operation = cio::io::read(reader, buffer);
    check_throws(
        [&buffer] {
          (void)buffer.filled_size();
        },
        "未 poll read 必须已经占用 buffer");
    check_throws(
        [&reader] {
          (void)reader.position();
        },
        "未 poll read 必须已经占用 endpoint");
    check_throws(
        [&reader, &other] {
          (void)cio::io::read(reader, other);
        },
        "未 poll read 期间第二个 endpoint operation 必须失败");
    check(other.filled_size() == 0,
          "endpoint 获取失败必须释放新 buffer lease");
    (void)operation;
  }
  check(buffer.filled_size() == 0 && reader.position() == 0,
        "未 poll read 析构必须释放两个租约且不提交进度");
}

void unpolled_session_control_owner_lifetime_test() {
  SessionGatedReader reader{
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({1})})};
  auto buffer = ReadBuf::with_capacity(1);
  {
    auto operation = cio::io::read(reader, buffer);
    check(reader.live_instances() == 2,
          "未 poll read 必须保留 stable session control");
    check_throws(
        [&buffer] {
          (void)buffer.filled_size();
        },
        "未 poll read 必须持有 buffer lease");
    (void)operation;
  }
  check(reader.live_instances() == 1 && buffer.filled_size() == 0,
        "未 poll read 必须先销毁 primitive 再销毁 Session");

  {
    auto retry = cio::io::read(reader, buffer);
    (void)retry;
  }
  check(reader.live_instances() == 1,
        "未 poll read 必须释放 endpoint Session");
}

cio::Task<bool> partial_write_vectored_and_zero_root() {
  auto writer = MemoryWriter::with_max_chunk(2);
  const auto first = co_await cio::io::write(writer, lease({1, 2, 3}));
  if (!first.has_value() || first.value() != 2 ||
      writer.snapshot() != bytes({1, 2}) ||
      cio::io::is_write_vectored(writer)) {
    co_return false;
  }

  ConstBufferSequence sequence;
  sequence.push(lease({}));
  sequence.push(lease({4, 5, 6}));
  sequence.push(lease({7}));
  const auto vectored =
      co_await cio::io::write_vectored(writer, std::move(sequence));
  if (!vectored.has_value() || vectored.value() != 2 ||
      writer.snapshot() != bytes({1, 2, 4, 5})) {
    co_return false;
  }

  ConstBufferSequence empty_sequence;
  const auto empty =
      co_await cio::io::write_vectored(writer, std::move(empty_sequence));
  if (!empty.has_value() || empty.value() != 0) {
    co_return false;
  }

  auto zero = MemoryWriter::zero_writer();
  const auto zero_result =
      co_await cio::io::write(zero, lease({9}));
  co_return zero_result.has_value() && zero_result.value() == 0 &&
      zero.snapshot().empty();
}

cio::Task<bool> flush_shutdown_and_owner_lifetime_root() {
  auto writer = MemoryWriter::with_max_chunk();
  const auto flushed = co_await cio::io::flush(writer);
  if (!flushed.has_value() || writer.flush_count() != 1) {
    co_return false;
  }
  const auto closed = co_await cio::io::shutdown(writer);
  const auto closed_again = co_await cio::io::shutdown(writer);
  if (!closed.has_value() || !closed_again.has_value() ||
      !writer.is_shutdown() || writer.flush_count() != 2) {
    co_return false;
  }
  const auto after_close =
      co_await cio::io::write(writer, lease({1}));
  if (after_close.has_value() ||
      after_close.error().kind() != ErrorKind::broken_pipe) {
    co_return false;
  }

  std::optional<MemoryWriter> owner{
      MemoryWriter::with_max_chunk()};
  auto operation = cio::io::write(*owner, lease({8, 9}));
  owner.reset();
  const auto completed = co_await std::move(operation);
  co_return completed.has_value() && completed.value() == 2;
}

cio::Task<bool> pending_read_cancel_and_late_wake_root() {
  const auto source = bytes({3, 4});
  SessionGatedReader reader{
      SharedBuffer::copy_from(
          std::span<const std::byte>{source})};
  auto buffer = ReadBuf::with_capacity(2);
  auto waiting = cio::task::spawn(cio::io::read(reader, buffer));
  if (reader.live_instances() != 2) {
    co_return false;
  }
  co_await cio::task::yield_now();
  if (waiting.is_finished() || reader.live_instances() != 2) {
    co_return false;
  }
  auto conflicting = ReadBuf::with_capacity(1);
  bool endpoint_rejected = false;
  try {
    (void)cio::io::read(reader, conflicting);
  } catch (const std::logic_error&) {
    endpoint_rejected = true;
  }
  if (!endpoint_rejected || conflicting.filled_size() != 0) {
    co_return false;
  }

  waiting.abort();
  const auto joined = co_await waiting;
  if (joined.has_value() || buffer.filled_size() != 0 ||
      reader.live_instances() != 1) {
    co_return false;
  }

  reader.open_one();
  co_await cio::task::yield_now();
  if (buffer.filled_size() != 0) {
    co_return false;
  }

  auto wake_before = ReadBuf::with_capacity(2);
  const auto completed =
      co_await cio::io::read(reader, wake_before);
  co_return completed.has_value() &&
      wake_before.filled_snapshot() == source;
}

void unpolled_write_exclusion_and_release_test() {
  auto writer = MemoryWriter::with_max_chunk();
  {
    auto operation = cio::io::write(writer, lease({1}));
    check_throws(
        [&writer] {
          (void)writer.snapshot();
        },
        "未 poll write 必须已经占用 endpoint");
    check_throws(
        [&writer] {
          (void)cio::io::flush(writer);
        },
        "未 poll write 期间 flush 必须拒绝别名 operation");
    (void)operation;
  }
  check(writer.snapshot().empty() && writer.flush_count() == 0,
        "未 poll write 析构必须释放 endpoint 且不提交字节");
}

void unpolled_writer_session_control_lifetime_test() {
  SessionGatedWriter writer;
  {
    auto operation = cio::io::shutdown(writer);
    check(writer.live_instances() == 2,
          "未 poll shutdown 必须保留 stable session control");
    check_throws(
        [&writer] {
          (void)writer.is_shutdown();
        },
        "未 poll shutdown 必须占用 writer endpoint");
    (void)operation;
  }
  check(writer.live_instances() == 1 && !writer.is_shutdown(),
        "未 poll shutdown 析构不得提交终态");

  {
    auto operation = cio::io::write(writer, lease({1}));
    check_throws(
        [&writer] {
          (void)cio::io::shutdown(writer);
        },
        "未 poll write 期间 shutdown 必须拒绝别名 operation");
    (void)operation;
  }
  check(writer.live_instances() == 1 && writer.snapshot().empty() &&
            !writer.is_shutdown(),
        "未 poll write 必须先释放 inner operation 再释放 owner");
}

cio::Task<bool> read_session_partial_exclusion_root(
    MemoryReader reader) {
  auto session = reader.open_read_session();
  auto first = ReadBuf::with_capacity(1);
  auto first_result =
      co_await session.read(first.lease_mut());
  if (!first_result.has_value() ||
      first_result.value().filled_snapshot() != bytes({1})) {
    co_return false;
  }

  bool alias_rejected = false;
  try {
    auto conflicting = ReadBuf::with_capacity(1);
    (void)cio::io::read(reader, conflicting);
  } catch (const std::logic_error&) {
    alias_rejected = true;
  }
  if (!alias_rejected) {
    co_return false;
  }

  auto second = ReadBuf::with_capacity(1);
  auto second_result =
      co_await session.read(second.lease_mut());
  co_return second_result.has_value() &&
      second_result.value().filled_snapshot() == bytes({2});
}

cio::Task<bool> write_session_partial_exclusion_root(
    MemoryWriter writer) {
  auto session = writer.open_write_session();
  const auto first =
      co_await session.write(lease({1}));
  if (!first.has_value() || first.value() != 1) {
    co_return false;
  }

  bool alias_rejected = false;
  try {
    (void)cio::io::shutdown(writer);
  } catch (const std::logic_error&) {
    alias_rejected = true;
  }
  if (!alias_rejected) {
    co_return false;
  }

  const auto second =
      co_await session.write(lease({2}));
  co_return second.has_value() && second.value() == 1;
}

cio::Task<bool> pending_child_cancel_keeps_session_root() {
  SessionGatedReader reader{
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({7})})};
  auto buffer = ReadBuf::with_capacity(1);
  bool retained_after_cancel = false;
  {
    auto session = reader.open_read_session();
    auto waiting =
        cio::task::spawn(session.read(buffer.lease_mut()));
    co_await cio::task::yield_now();
    if (waiting.is_finished() || reader.live_instances() != 2) {
      co_return false;
    }
    waiting.abort();
    const auto joined = co_await waiting;
    bool alias_rejected = false;
    try {
      (void)reader.open_read_session();
    } catch (const std::logic_error&) {
      alias_rejected = true;
    }
    retained_after_cancel =
        !joined.has_value() && alias_rejected &&
        reader.live_instances() == 2 &&
        buffer.filled_size() == 0;
  }

  reader.open_one();
  co_await cio::task::yield_now();
  co_return retained_after_cancel &&
      reader.live_instances() == 1 &&
      buffer.filled_size() == 0;
}

cio::Task<bool> pending_writer_child_cancel_keeps_session_root() {
  SessionGatedWriter writer;
  bool retained_after_cancel = false;
  bool reused = false;
  {
    auto session = writer.open_write_session();
    auto waiting =
        cio::task::spawn(session.write(lease({3, 4})));
    co_await cio::task::yield_now();
    if (waiting.is_finished() || writer.live_instances() != 2) {
      co_return false;
    }
    waiting.abort();
    const auto joined = co_await waiting;
    bool alias_rejected = false;
    try {
      (void)writer.open_write_session();
    } catch (const std::logic_error&) {
      alias_rejected = true;
    }
    retained_after_cancel =
        !joined.has_value() && alias_rejected &&
        writer.live_instances() == 2;

    writer.open_one();
    const auto completed =
        co_await session.write(lease({5, 6}));
    reused = completed.has_value() &&
        completed.value() == 2;
  }

  writer.open_one();
  co_await cio::task::yield_now();
  co_return retained_after_cancel && reused &&
      writer.live_instances() == 1 &&
      writer.snapshot() == bytes({5, 6});
}

void endpoint_session_lifetime_and_continuity_test() {
  auto reader = MemoryReader::from(
      SharedBuffer::copy_from(
          std::span<const std::byte>{bytes({1, 2, 3})}),
      1);
  {
    auto session = reader.open_read_session();
    check_throws(
        [&reader] {
          (void)reader.open_read_session();
        },
        "未 poll read Session 必须立即排斥 endpoint 别名");
    (void)session;
  }
  check(reader.position() == 0,
        "未 poll read Session 析构不得推进 cursor");

  auto writer = MemoryWriter::with_max_chunk(1);
  {
    auto session = writer.open_write_session();
    check_throws(
        [&writer] {
          (void)writer.open_write_session();
        },
        "未 poll write Session 必须立即排斥 endpoint 别名");
    (void)session;
  }
  check(writer.snapshot().empty(),
        "未 poll write Session 析构不得提交字节");

  Runtime runtime;
  check(runtime.block_on(
            read_session_partial_exclusion_root(reader)),
        "read Session 必须跨 partial primitive 保持端点独占");
  check(reader.position() == 2,
        "read Session 两次 partial 后 cursor 错误");
  check(runtime.block_on(
            write_session_partial_exclusion_root(writer)),
        "write Session 必须跨 partial primitive 保持端点独占");
  check(writer.snapshot() == bytes({1, 2}),
        "write Session 两次 partial 后输出错误");
  check(runtime.block_on(
            pending_child_cancel_keeps_session_root()),
        "Pending child 取消不能提前释放外层 Session");
  check(runtime.block_on(
            pending_writer_child_cancel_keeps_session_root()),
        "Pending writer child 取消后 Session 必须保持独占且可复用");

  auto move_writer = MemoryWriter::with_max_chunk();
  {
    auto session = move_writer.open_write_session();
    {
      auto primitive = session.write(lease({9}));
      auto moved = std::move(session);
      check_throws(
          [&session] {
            (void)session.flush();
          },
          "move 后源 Session 必须失效");
      check_throws(
          [&moved] {
            (void)moved.flush();
          },
          "活动 primitive 期间移动后的 Session 仍须拒绝第二 primitive");
      (void)primitive;
      session = std::move(moved);
    }
    const auto completed =
        runtime.block_on(session.write(lease({9})));
    check(completed.has_value() && completed.value() == 1,
          "primitive 析构后移动过的 Session 必须可继续使用");
  }
  check(move_writer.snapshot() == bytes({9}),
        "Session move/reuse 后输出错误");
}

void long_session_cross_thread_alias_stress_test() {
  constexpr std::size_t rounds = 512;
  auto writer = MemoryWriter::with_max_chunk(1);
  auto session = writer.open_write_session();
  Runtime runtime;
  std::barrier start{2};
  std::barrier done{2};
  std::atomic<std::size_t> rejected{0};
  std::atomic<bool> unexpected{false};

  std::jthread contender{[&] {
    for (std::size_t round = 0; round < rounds; ++round) {
      start.arrive_and_wait();
      try {
        (void)writer.open_write_session();
      } catch (const std::logic_error&) {
        rejected.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
      }
      done.arrive_and_wait();
    }
  }};

  bool exact = true;
  for (std::size_t round = 0; round < rounds; ++round) {
    const auto written =
        runtime.block_on(session.write(lease({1})));
    exact = exact && written.has_value() &&
        written.value() == 1;
    start.arrive_and_wait();
    done.arrive_and_wait();
    exact = exact &&
        rejected.load(std::memory_order_relaxed) == round + 1;
  }

  check(exact && !unexpected.load(std::memory_order_relaxed),
        "长期 Session 的 partial 边界必须跨线程持续排斥 endpoint 别名");
}

void concurrent_write_shutdown_initiation_stress_test() {
  constexpr std::size_t rounds = 512;
  std::barrier start{3};
  std::barrier acquired{3};
  std::barrier release{3};
  std::barrier done{3};
  std::atomic<std::size_t> accepted{0};
  std::atomic<std::size_t> rejected{0};
  std::atomic<bool> unexpected{false};
  auto writer = MemoryWriter::with_max_chunk();

  std::jthread write_thread{[&] {
    for (std::size_t round = 0; round < rounds; ++round) {
      start.arrive_and_wait();
      try {
        auto operation = cio::io::write(writer, lease({1}));
        accepted.fetch_add(1, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
        (void)operation;
      } catch (const std::logic_error&) {
        rejected.fetch_add(1, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
      }
      done.arrive_and_wait();
    }
  }};
  std::jthread shutdown_thread{[&] {
    for (std::size_t round = 0; round < rounds; ++round) {
      start.arrive_and_wait();
      try {
        auto operation = cio::io::shutdown(writer);
        accepted.fetch_add(1, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
        (void)operation;
      } catch (const std::logic_error&) {
        rejected.fetch_add(1, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
      } catch (...) {
        unexpected.store(true, std::memory_order_relaxed);
        acquired.arrive_and_wait();
        release.arrive_and_wait();
      }
      done.arrive_and_wait();
    }
  }};

  bool counts_are_exact = true;
  bool state_is_unchanged = true;
  for (std::size_t round = 0; round < rounds; ++round) {
    writer = MemoryWriter::with_max_chunk();
    start.arrive_and_wait();
    acquired.arrive_and_wait();
    counts_are_exact =
        counts_are_exact &&
        accepted.load(std::memory_order_relaxed) == round + 1 &&
        rejected.load(std::memory_order_relaxed) == round + 1;
    release.arrive_and_wait();
    done.arrive_and_wait();
    state_is_unchanged =
        state_is_unchanged && writer.snapshot().empty() &&
        !writer.is_shutdown() && writer.flush_count() == 0;
  }

  check(!unexpected.load(std::memory_order_relaxed) &&
            counts_are_exact && state_is_unchanged,
        "并发 write/shutdown initiating 必须恰好接受一个并拒绝一个，"
        "未 poll operation 不得提交状态");
}

cio::Task<bool> pending_member_writer_cancel_root() {
  SessionGatedWriter writer;
  auto waiting =
      cio::task::spawn(cio::io::write(writer, lease({6, 7})));
  if (writer.live_instances() != 2) {
    co_return false;
  }
  co_await cio::task::yield_now();
  if (waiting.is_finished() || writer.live_instances() != 2) {
    co_return false;
  }

  waiting.abort();
  const auto joined = co_await waiting;
  if (joined.has_value() || writer.live_instances() != 1 ||
      !writer.snapshot().empty()) {
    co_return false;
  }

  writer.open_one();
  auto completed =
      co_await cio::io::write(writer, lease({6, 7}));
  co_return completed.has_value() && completed.value() == 2 &&
      writer.snapshot() == bytes({6, 7});
}

void memory_endpoint_async_test() {
  Runtime runtime;
  check(runtime.block_on(partial_read_eof_and_zero_capacity_root()),
        "MemoryReader partial/EOF/zero-capacity 语义错误");
  check(runtime.block_on(partial_write_vectored_and_zero_root()),
        "MemoryWriter partial/vectored/write-zero 语义错误");
  check(runtime.block_on(flush_shutdown_and_owner_lifetime_root()),
        "MemoryWriter flush/shutdown/owner lifetime 语义错误");
  check(runtime.block_on(pending_read_cancel_and_late_wake_root()),
        "AsyncRead Pending cancel/late wake 语义错误");
  check(runtime.block_on(pending_member_writer_cancel_root()),
        "AsyncWrite member coroutine Pending cancel 语义错误");
}

struct UnmarkedReader final {
  [[nodiscard]] cio::Task<
      cio::io::IoResult<cio::io::MutableBufferLease>>
  read(cio::io::MutableBufferLease) const;
};

struct UnmarkedWriter final {
  [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>>
  write(ConstBufferLease) const;
  [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush() const;
  [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown() const;
};

struct ShapeOnlyReadSession final {
  ShapeOnlyReadSession(const ShapeOnlyReadSession&) = delete;
  ShapeOnlyReadSession& operator=(const ShapeOnlyReadSession&) = delete;
  ShapeOnlyReadSession(ShapeOnlyReadSession&&) noexcept = default;
  ShapeOnlyReadSession& operator=(ShapeOnlyReadSession&&) noexcept = default;

  [[nodiscard]] cio::Task<
      cio::io::IoResult<cio::io::MutableBufferLease>>
  read(cio::io::MutableBufferLease);
};

struct EndpointWithUnmarkedReadSession final {
  static constexpr bool cio_async_read_endpoint = true;
  using ReadSession = ShapeOnlyReadSession;

  [[nodiscard]] ReadSession open_read_session() const;
};

struct ShapeOnlyWriteSession final {
  ShapeOnlyWriteSession(const ShapeOnlyWriteSession&) = delete;
  ShapeOnlyWriteSession& operator=(const ShapeOnlyWriteSession&) = delete;
  ShapeOnlyWriteSession(ShapeOnlyWriteSession&&) noexcept = default;
  ShapeOnlyWriteSession& operator=(ShapeOnlyWriteSession&&) noexcept = default;

  [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>>
  write(ConstBufferLease);
  [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush();
  [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown();
};

struct EndpointWithUnmarkedWriteSession final {
  static constexpr bool cio_async_write_endpoint = true;
  using WriteSession = ShapeOnlyWriteSession;

  [[nodiscard]] WriteSession open_write_session() const;
};

static_assert(cio::io::AsyncRead<MemoryReader>);
static_assert(cio::io::AsyncWrite<MemoryWriter>);
static_assert(cio::io::AsyncReadSession<MemoryReader::ReadSession>);
static_assert(cio::io::AsyncWriteSession<MemoryWriter::WriteSession>);
static_assert(!cio::io::AsyncRead<UnmarkedReader>);
static_assert(!cio::io::AsyncWrite<UnmarkedWriter>);
static_assert(
    !cio::io::AsyncRead<EndpointWithUnmarkedReadSession>);
static_assert(
    !cio::io::AsyncWrite<EndpointWithUnmarkedWriteSession>);
static_assert(cio::Send<cio::io::Error>);
static_assert(cio::Sync<cio::io::Error>);
static_assert(cio::Send<OwnedBuffer>);
static_assert(cio::Send<SharedBuffer>);
static_assert(cio::Sync<SharedBuffer>);
static_assert(cio::Send<ConstBufferLease>);
static_assert(cio::Sync<ConstBufferLease>);
static_assert(cio::Send<ConstBufferSequence>);
static_assert(cio::Sync<ConstBufferSequence>);
static_assert(cio::Send<ReadBuf>);
static_assert(!cio::Sync<ReadBuf>);
static_assert(cio::Send<cio::io::MutableBufferLease>);
static_assert(!cio::Sync<cio::io::MutableBufferLease>);
static_assert(cio::Send<MemoryReader>);
static_assert(cio::Sync<MemoryReader>);
static_assert(cio::Send<MemoryReader::ReadSession>);
static_assert(!cio::Sync<MemoryReader::ReadSession>);
static_assert(cio::Send<MemoryWriter>);
static_assert(cio::Sync<MemoryWriter>);
static_assert(cio::Send<MemoryWriter::WriteSession>);
static_assert(!cio::Sync<MemoryWriter::WriteSession>);
static_assert(cio::Send<cio::io::IoResult<void>>);
static_assert(cio::Send<cio::io::IoResult<cio::io::MutableBufferLease>>);
static_assert(!std::is_copy_constructible_v<ReadBuf>);
static_assert(!std::is_copy_constructible_v<cio::io::MutableBufferLease>);

template <typename Writer>
concept HasBorrowedSpanWrite =
    requires(Writer writer, std::span<const std::byte> view) {
      cio::io::write(writer, view);
    };

static_assert(!HasBorrowedSpanWrite<MemoryWriter>);

}  // namespace

int main() {
  struct Case final {
    std::string_view name;
    void (*run)();
  };

  const std::vector<Case> cases{
      {"owned/shared buffer", owned_and_shared_buffer_test},
      {"buffer sequence", buffer_sequence_test},
      {"error model native round trip",
       error_model_native_round_trip_test},
      {"ReadBuf regions/bounds", read_buf_regions_and_bounds_test},
      {"mutable lease exclusion/lifetime",
       mutable_lease_exclusion_and_lifetime_test},
      {"ReadBuf cross-thread lease stress",
       read_buf_cross_thread_lease_stress_test},
      {"unpolled read exclusion/release",
       unpolled_read_exclusion_and_release_test},
      {"unpolled session control owner lifetime",
       unpolled_session_control_owner_lifetime_test},
      {"unpolled write exclusion/release",
       unpolled_write_exclusion_and_release_test},
      {"unpolled writer session control lifetime/shutdown",
       unpolled_writer_session_control_lifetime_test},
      {"endpoint session lifetime/continuity",
       endpoint_session_lifetime_and_continuity_test},
      {"long session cross-thread alias stress",
       long_session_cross_thread_alias_stress_test},
      {"concurrent write/shutdown initiation stress",
       concurrent_write_shutdown_initiation_stress_test},
      {"memory endpoint async", memory_endpoint_async_test},
  };

  std::size_t passed = 0;
  for (const auto& test : cases) {
    try {
      test.run();
      ++passed;
    } catch (const std::exception& error) {
      std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
      return 1;
    }
  }

  std::cout << "io tests passed: " << passed << '/' << cases.size() << '\n';
  return 0;
}
