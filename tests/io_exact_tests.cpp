#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "cio/io/memory.hpp"
#include "cio/io/read_exact.hpp"
#include "cio/io/write_all.hpp"
#include "cio/runtime/runtime.hpp"
#include "cio/sync/notify.hpp"
#include "cio/task/spawn.hpp"
#include "cio/task/yield_now.hpp"

namespace {

using cio::io::ConstBufferLease;
using cio::io::ErrorKind;
using cio::io::MemoryReader;
using cio::io::MemoryWriter;
using cio::io::MutableBufferLease;
using cio::io::ReadBuf;
using cio::io::SharedBuffer;
using cio::runtime::Runtime;

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

template <typename Function>
void check_throws(Function &&function, std::string_view message) {
  bool thrown = false;
  try {
    std::forward<Function>(function)();
  } catch (const std::exception &) {
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
  const auto input = bytes(values);
  return SharedBuffer::copy_from(std::span<const std::byte>{input}).lease();
}

template <typename Predicate>
bool wait_until(Predicate &&predicate,
                std::chrono::milliseconds timeout = std::chrono::seconds{3}) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::forward<Predicate>(predicate)()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  return std::forward<Predicate>(predicate)();
}

struct StepReaderState final {
  StepReaderState(SharedBuffer input, std::size_t failure_call)
      : source{std::move(input)}, fail_on_call{failure_call} {}

  mutable std::mutex mutex;
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
  SharedBuffer source;
  std::size_t position{0};
  std::size_t initiations{0};
  std::size_t calls{0};
  std::size_t fail_on_call{0};
  std::vector<std::thread::id> call_threads;
  std::vector<std::thread::id> resumed_threads;
  cio::sync::Notify gate;
};

class StepReader final {
private:
  using EndpointSession =
      cio::io::detail::EndpointSessionState<StepReaderState>;

public:
  class ReadSession final {
  public:
    static constexpr bool cio_async_read_session = true;

    ReadSession(const ReadSession &) = delete;
    ReadSession &operator=(const ReadSession &) = delete;
    ReadSession(ReadSession &&) noexcept = default;
    ReadSession &operator=(ReadSession &&) noexcept = default;
    ~ReadSession() = default;

    [[nodiscard]] cio::Task<cio::io::IoResult<MutableBufferLease>>
    read(MutableBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<StepReaderState>::acquire(
              session_);
      {
        const std::lock_guard lock{session_->endpoint->mutex};
        ++session_->endpoint->initiations;
      }
      return read_impl(session_, std::move(primitive), std::move(buffer));
    }

  private:
    explicit ReadSession(std::shared_ptr<EndpointSession> session) noexcept
        : session_{std::move(session)} {}

    [[nodiscard]] static cio::Task<cio::io::IoResult<MutableBufferLease>>
    read_impl(std::shared_ptr<EndpointSession> session,
              cio::io::detail::SessionPrimitiveGuard<StepReaderState> primitive,
              MutableBufferLease buffer) {
      const auto state = session->endpoint;
      std::size_t call = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        call = ++state->calls;
        state->call_threads.push_back(std::this_thread::get_id());
      }
      if (call == state->fail_on_call) {
        co_return cio::io::IoResult<MutableBufferLease>::failure(
            cio::io::Error::other("注入 read 错误",
                                  std::make_error_code(std::errc::io_error)));
      }
      if (call > 1) {
        co_await state->gate.notified();
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->resumed_threads.push_back(std::this_thread::get_id());
      }

      std::size_t position = 0;
      std::size_t amount = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        position = state->position;
        amount = std::min({state->source.size() - position, buffer.remaining(),
                           std::size_t{2}});
      }
      const auto input = state->source.subbuffer(position, amount).snapshot();
      if (!input.empty()) {
        buffer.put_slice(std::span<const std::byte>{input});
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->position += amount;
      }
      (void)primitive;
      co_return cio::io::IoResult<MutableBufferLease>::success(
          std::move(buffer));
    }

    std::shared_ptr<EndpointSession> session_;

    friend class StepReader;
  };

  static constexpr bool cio_async_read_endpoint = true;

  explicit StepReader(SharedBuffer source, std::size_t fail_on_call = 0)
      : state_{std::make_shared<StepReaderState>(std::move(source),
                                                 fail_on_call)} {}

  [[nodiscard]] ReadSession open_read_session() const {
    auto guard =
        cio::io::detail::EndpointOperationGuard<StepReaderState>::acquire(
            state_);
    return ReadSession{
        std::make_shared<EndpointSession>(state_, std::move(guard))};
  }

  void open_one() const { state_->gate.notify_one(); }

  [[nodiscard]] std::size_t calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->calls;
  }

  [[nodiscard]] std::size_t initiations() const {
    const std::lock_guard lock{state_->mutex};
    return state_->initiations;
  }

  [[nodiscard]] bool active() const {
    const std::lock_guard lock{state_->mutex};
    return state_->operation_active.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::thread::id call_thread(std::size_t index) const {
    const std::lock_guard lock{state_->mutex};
    if (index >= state_->call_threads.size()) {
      throw std::out_of_range{"reader call thread 下标越界"};
    }
    return state_->call_threads[index];
  }

  [[nodiscard]] std::thread::id resumed_thread(std::size_t index) const {
    const std::lock_guard lock{state_->mutex};
    if (index >= state_->resumed_threads.size()) {
      throw std::out_of_range{"reader resumed thread 下标越界"};
    }
    return state_->resumed_threads[index];
  }

private:
  std::shared_ptr<StepReaderState> state_;
};

struct StepWriterState final {
  mutable std::mutex mutex;
  std::atomic<bool> operation_active{false};
  std::atomic<std::uint64_t> operation_generation{0};
  std::vector<std::byte> bytes;
  std::size_t initiations{0};
  std::size_t calls{0};
  std::size_t fail_on_call{0};
  std::size_t flush_calls{0};
  std::size_t shutdown_calls{0};
  std::vector<std::thread::id> call_threads;
  std::vector<std::thread::id> resumed_threads;
  cio::sync::Notify gate;
};

class StepWriter final {
private:
  using EndpointSession =
      cio::io::detail::EndpointSessionState<StepWriterState>;

public:
  class WriteSession final {
  public:
    static constexpr bool cio_async_write_session = true;

    WriteSession(const WriteSession &) = delete;
    WriteSession &operator=(const WriteSession &) = delete;
    WriteSession(WriteSession &&) noexcept = default;
    WriteSession &operator=(WriteSession &&) noexcept = default;
    ~WriteSession() = default;

    [[nodiscard]] cio::Task<cio::io::IoResult<std::size_t>>
    write(ConstBufferLease buffer) {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<StepWriterState>::acquire(
              session_);
      {
        const std::lock_guard lock{session_->endpoint->mutex};
        ++session_->endpoint->initiations;
      }
      return write_impl(session_, std::move(primitive), std::move(buffer));
    }

    [[nodiscard]] cio::Task<cio::io::IoResult<void>> flush() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<StepWriterState>::acquire(
              session_);
      return flush_impl(session_, std::move(primitive));
    }

    [[nodiscard]] cio::Task<cio::io::IoResult<void>> shutdown() {
      auto primitive =
          cio::io::detail::SessionPrimitiveGuard<StepWriterState>::acquire(
              session_);
      return shutdown_impl(session_, std::move(primitive));
    }

  private:
    explicit WriteSession(std::shared_ptr<EndpointSession> session) noexcept
        : session_{std::move(session)} {}

    [[nodiscard]] static cio::Task<cio::io::IoResult<std::size_t>> write_impl(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<StepWriterState> primitive,
        ConstBufferLease buffer) {
      const auto state = session->endpoint;
      std::size_t call = 0;
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        call = ++state->calls;
        state->call_threads.push_back(std::this_thread::get_id());
      }
      if (call == state->fail_on_call) {
        co_return cio::io::IoResult<std::size_t>::failure(cio::io::Error::other(
            "注入 write 错误", std::make_error_code(std::errc::io_error)));
      }
      if (call > 1) {
        co_await state->gate.notified();
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->resumed_threads.push_back(std::this_thread::get_id());
      }

      const auto amount = std::min(buffer.size(), std::size_t{2});
      const auto input = buffer.sublease(0, amount).snapshot();
      {
        const std::lock_guard lock{state->mutex};
        session->endpoint_guard.require_current(*state);
        state->bytes.insert(state->bytes.end(), input.begin(), input.end());
      }
      (void)primitive;
      co_return cio::io::IoResult<std::size_t>::success(amount);
    }

    [[nodiscard]] static cio::Task<cio::io::IoResult<void>> flush_impl(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<StepWriterState> primitive) {
      {
        const std::lock_guard lock{session->endpoint->mutex};
        session->endpoint_guard.require_current(*session->endpoint);
        ++session->endpoint->flush_calls;
      }
      (void)primitive;
      co_return cio::io::IoResult<void>::success();
    }

    [[nodiscard]] static cio::Task<cio::io::IoResult<void>> shutdown_impl(
        std::shared_ptr<EndpointSession> session,
        cio::io::detail::SessionPrimitiveGuard<StepWriterState> primitive) {
      {
        const std::lock_guard lock{session->endpoint->mutex};
        session->endpoint_guard.require_current(*session->endpoint);
        ++session->endpoint->shutdown_calls;
      }
      (void)primitive;
      co_return cio::io::IoResult<void>::success();
    }

    std::shared_ptr<EndpointSession> session_;

    friend class StepWriter;
  };

  static constexpr bool cio_async_write_endpoint = true;

  explicit StepWriter(std::size_t fail_on_call = 0)
      : state_{std::make_shared<StepWriterState>()} {
    state_->fail_on_call = fail_on_call;
  }

  [[nodiscard]] WriteSession open_write_session() const {
    auto guard =
        cio::io::detail::EndpointOperationGuard<StepWriterState>::acquire(
            state_);
    return WriteSession{
        std::make_shared<EndpointSession>(state_, std::move(guard))};
  }

  void open_one() const { state_->gate.notify_one(); }

  [[nodiscard]] std::size_t calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->calls;
  }

  [[nodiscard]] std::size_t initiations() const {
    const std::lock_guard lock{state_->mutex};
    return state_->initiations;
  }

  [[nodiscard]] std::size_t flush_calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->flush_calls;
  }

  [[nodiscard]] std::size_t shutdown_calls() const {
    const std::lock_guard lock{state_->mutex};
    return state_->shutdown_calls;
  }

  [[nodiscard]] bool active() const {
    const std::lock_guard lock{state_->mutex};
    return state_->operation_active.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    const std::lock_guard lock{state_->mutex};
    return state_->bytes;
  }

  [[nodiscard]] std::thread::id call_thread(std::size_t index) const {
    const std::lock_guard lock{state_->mutex};
    if (index >= state_->call_threads.size()) {
      throw std::out_of_range{"writer call thread 下标越界"};
    }
    return state_->call_threads[index];
  }

  [[nodiscard]] std::thread::id resumed_thread(std::size_t index) const {
    const std::lock_guard lock{state_->mutex};
    if (index >= state_->resumed_threads.size()) {
      throw std::out_of_range{"writer resumed thread 下标越界"};
    }
    return state_->resumed_threads[index];
  }

private:
  std::shared_ptr<StepWriterState> state_;
};

cio::Task<void>
hold_selected_worker(std::thread::id selected,
                     std::shared_ptr<std::atomic<bool>> started,
                     std::shared_ptr<std::atomic<bool>> release) {
  if (std::this_thread::get_id() != selected) {
    for (std::size_t attempt = 0; attempt < 8; ++attempt) {
      co_await cio::task::yield_now();
    }
    co_return;
  }

  started->store(true, std::memory_order_release);
  while (!release->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  co_return;
}

class WorkerRelease final {
public:
  explicit WorkerRelease(std::shared_ptr<std::atomic<bool>> release) noexcept
      : release_{std::move(release)} {}

  WorkerRelease(const WorkerRelease &) = delete;
  WorkerRelease &operator=(const WorkerRelease &) = delete;
  WorkerRelease(WorkerRelease &&) = delete;
  WorkerRelease &operator=(WorkerRelease &&) = delete;

  ~WorkerRelease() { release_->store(true, std::memory_order_release); }

private:
  std::shared_ptr<std::atomic<bool>> release_;
};

std::vector<cio::task::JoinHandle<void>>
occupy_worker(Runtime &runtime, std::thread::id selected,
              const std::shared_ptr<std::atomic<bool>> &started,
              const std::shared_ptr<std::atomic<bool>> &release) {
  std::vector<cio::task::JoinHandle<void>> blockers;
  blockers.reserve(64);
  for (std::size_t index = 0; index < 64; ++index) {
    blockers.push_back(runtime.spawn(cio::task::assume_portable(
        hold_selected_worker(selected, started, release))));
  }
  check(wait_until(
            [&started] { return started->load(std::memory_order_acquire); }),
        "未能占用 exact 首次 Pending 所在 worker，不能伪称发生迁移");
  return blockers;
}

void release_worker_and_wait(
    const std::shared_ptr<std::atomic<bool>> &release,
    const std::vector<cio::task::JoinHandle<void>> &blockers) {
  release->store(true, std::memory_order_release);
  check(wait_until([&blockers] {
          return std::all_of(
              blockers.begin(), blockers.end(),
              [](const auto &handle) { return handle.is_finished(); });
        }),
        "worker 占用任务释放后没有全部完成");
}

template <typename Value>
cio::Task<bool>
await_native_error(cio::task::JoinHandle<cio::io::IoResult<Value>> handle) {
  const auto joined = co_await handle;
  if (!joined.has_value() || joined.value().has_value()) {
    co_return false;
  }
  co_return joined.value().error().kind() == ErrorKind::other &&
      joined.value().error().native_code() ==
          std::make_error_code(std::errc::io_error);
}

template <typename Value>
cio::Task<bool>
await_cancelled(cio::task::JoinHandle<cio::io::IoResult<Value>> handle) {
  const auto joined = co_await handle;
  co_return !joined.has_value() && joined.error().is_cancelled();
}

cio::Task<bool> exact_success_and_errors_root() {
  auto reader =
      MemoryReader::from(SharedBuffer::copy_from(std::span<const std::byte>{
                             bytes({1, 2, 3, 4, 5})}),
                         2);
  auto target = ReadBuf::with_capacity(5);
  const auto read_result = co_await cio::io::read_exact(reader, target);
  if (!read_result.has_value() || read_result.value() != 5 ||
      target.filled_snapshot() != bytes({1, 2, 3, 4, 5})) {
    co_return false;
  }

  auto short_reader = MemoryReader::from(
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({6, 7, 8})}), 2);
  auto short_target = ReadBuf::with_capacity(5);
  const auto short_result =
      co_await cio::io::read_exact(short_reader, short_target);
  if (short_result.has_value() ||
      short_result.error().kind() != ErrorKind::unexpected_eof ||
      short_target.filled_snapshot() != bytes({6, 7, 8})) {
    co_return false;
  }

  auto writer = MemoryWriter::with_max_chunk(2);
  const auto write_result =
      co_await cio::io::write_all(writer, lease({1, 2, 3, 4, 5}));
  if (!write_result.has_value() ||
      writer.snapshot() != bytes({1, 2, 3, 4, 5}) ||
      writer.flush_count() != 0) {
    co_return false;
  }

  auto zero = MemoryWriter::zero_writer();
  const auto zero_result = co_await cio::io::write_all(zero, lease({9}));
  if (zero_result.has_value() ||
      zero_result.error().kind() != ErrorKind::write_zero ||
      !zero.snapshot().empty()) {
    co_return false;
  }

  auto closed = MemoryWriter::with_max_chunk();
  const auto shutdown = co_await cio::io::shutdown(closed);
  const auto broken = co_await cio::io::write_all(closed, lease({7}));
  co_return shutdown.has_value() && !broken.has_value() &&
      broken.error().kind() == ErrorKind::broken_pipe;
}

cio::Task<bool> exact_native_error_progress_root() {
  const auto native = std::make_error_code(std::errc::io_error);
  StepReader reader{
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({1, 2, 3, 4})}),
      2};
  auto target = ReadBuf::with_capacity(4);
  const auto read_result = co_await cio::io::read_exact(reader, target);
  if (read_result.has_value() ||
      read_result.error().kind() != ErrorKind::other ||
      read_result.error().native_code() != native ||
      target.filled_snapshot() != bytes({1, 2}) || reader.calls() != 2 ||
      reader.active()) {
    co_return false;
  }

  StepWriter writer{2};
  const auto write_result =
      co_await cio::io::write_all(writer, lease({5, 6, 7, 8}));
  co_return !write_result.has_value() &&
      write_result.error().kind() == ErrorKind::other &&
      write_result.error().native_code() == native &&
      writer.snapshot() == bytes({5, 6}) && writer.calls() == 2 &&
      !writer.active() && writer.flush_calls() == 0 &&
      writer.shutdown_calls() == 0;
}

void initiating_and_empty_test() {
  StepReader reader{
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({1, 2})})};
  auto target = ReadBuf::with_capacity(2);
  {
    auto operation = cio::io::read_exact(reader, target);
    check(reader.active() && reader.calls() == 0 && reader.initiations() == 1,
          "非空未 poll read_exact 必须取得 Session 并创建一个 primitive");
    check_throws([&reader] { (void)reader.open_read_session(); },
                 "read_exact 全生命周期必须排斥 reader 别名");
    check_throws([&target] { (void)target.filled_size(); },
                 "未 poll read_exact 必须占用 ReadBuf");
    (void)operation;
  }
  check(!reader.active() && target.filled_size() == 0,
        "未 poll read_exact 析构不得提交进度");

  auto empty_target = ReadBuf::with_capacity(0);
  auto empty_read = cio::io::read_exact(reader, empty_target);
  check(reader.active(), "空 read_exact 必须保持 Session 独占");
  Runtime runtime;
  const auto empty_read_result = runtime.block_on(std::move(empty_read));
  check(empty_read_result.has_value() && empty_read_result.value() == 0 &&
            reader.calls() == 0 && reader.initiations() == 1 &&
            !reader.active(),
        "空 read_exact 不得创建或 poll reader primitive");

  auto prefilled = ReadBuf::with_capacity(1);
  prefilled.put_slice(std::span<const std::byte>{bytes({9})});
  check_throws(
      [&reader, &prefilled] { (void)cio::io::read_exact(reader, prefilled); },
      "read_exact 必须同步拒绝非空 filled");
  check(!reader.active() && prefilled.filled_size() == 1,
        "拒绝 prefilled 不得取得 Session 或修改 buffer");

  StepWriter writer;
  {
    auto operation = cio::io::write_all(writer, lease({1, 2}));
    check(writer.active() && writer.calls() == 0 && writer.initiations() == 1,
          "非空未 poll write_all 必须取得 Session 并创建一个 primitive");
    check_throws([&writer] { (void)writer.open_write_session(); },
                 "write_all 全生命周期必须排斥 writer 别名");
    (void)operation;
  }
  check(!writer.active() && writer.snapshot().empty(),
        "未 poll write_all 析构不得提交字节");

  auto empty_write = cio::io::write_all(writer, lease({}));
  check(writer.active(), "空 write_all 必须保持 Session 独占");
  const auto empty_write_result = runtime.block_on(std::move(empty_write));
  check(empty_write_result.has_value() && writer.calls() == 0 &&
            writer.initiations() == 1 && !writer.active() &&
            writer.snapshot().empty(),
        "空 write_all 不得创建或 poll writer primitive");
}

cio::Task<bool> read_cancel_root() {
  StepReader reader{
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({1, 2, 3, 4})})};
  auto target = ReadBuf::with_capacity(4);
  auto waiting = cio::task::spawn(cio::io::read_exact(reader, target));
  co_await cio::task::yield_now();
  if (waiting.is_finished() || reader.calls() != 2 || !reader.active()) {
    co_return false;
  }
  try {
    (void)reader.open_read_session();
    co_return false;
  } catch (const std::logic_error &) {
  }

  waiting.abort();
  const auto joined = co_await waiting;
  if (joined.has_value() || reader.active() ||
      target.filled_snapshot() != bytes({1, 2})) {
    co_return false;
  }
  reader.open_one();
  co_await cio::task::yield_now();
  co_return target.filled_snapshot() == bytes({1, 2});
}

cio::Task<bool> write_cancel_root() {
  StepWriter writer;
  auto waiting =
      cio::task::spawn(cio::io::write_all(writer, lease({1, 2, 3, 4})));
  co_await cio::task::yield_now();
  if (waiting.is_finished() || writer.calls() != 2 || !writer.active() ||
      writer.snapshot() != bytes({1, 2})) {
    co_return false;
  }
  try {
    (void)writer.open_write_session();
    co_return false;
  } catch (const std::logic_error &) {
  }

  waiting.abort();
  const auto joined = co_await waiting;
  if (joined.has_value() || writer.active() ||
      writer.snapshot() != bytes({1, 2})) {
    co_return false;
  }
  writer.open_one();
  co_await cio::task::yield_now();
  co_return writer.snapshot() == bytes({1, 2});
}

void async_semantics_test() {
  Runtime runtime;
  check(runtime.block_on(exact_success_and_errors_root()),
        "exact partial/EOF/write-zero/error 语义错误");
  check(runtime.block_on(exact_native_error_progress_root()),
        "exact native error/部分进度/不隐式 flush 语义错误");
  check(runtime.block_on(read_cancel_root()),
        "read_exact Pending 取消或 late wake 语义错误");
  check(runtime.block_on(write_cancel_root()),
        "write_all Pending 取消或 late wake 语义错误");
}

void multi_thread_migration_and_partial_error_test() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();

  StepReader reader{SharedBuffer::copy_from(
                        std::span<const std::byte>{bytes({1, 2, 3, 4, 5, 6})}),
                    3};
  auto target = ReadBuf::with_capacity(6);
  auto read_handle = runtime.spawn(
      cio::task::assume_portable(cio::io::read_exact(reader, target)));
  check(
      wait_until([&reader] { return reader.calls() == 2 && reader.active(); }),
      "multi-thread read_exact 没有停在部分读取后的第二次 read");

  const auto read_origin = reader.call_thread(1);
  auto read_blocked = std::make_shared<std::atomic<bool>>(false);
  auto read_release = std::make_shared<std::atomic<bool>>(false);
  WorkerRelease read_release_guard{read_release};
  auto read_blockers =
      occupy_worker(runtime, read_origin, read_blocked, read_release);

  std::thread read_notifier{[reader] { reader.open_one(); }};
  const auto read_notifier_id = read_notifier.get_id();
  read_notifier.join();
  check(runtime.block_on(cio::task::assume_portable(
            await_native_error(std::move(read_handle)))),
        "跨线程 wake 后 read_exact 没有保留 native error");
  check(reader.calls() == 3 &&
            target.filled_snapshot() == bytes({1, 2, 3, 4}) && !reader.active(),
        "跨 worker read_exact 没有保留错误前的部分进度或释放 Session/ReadBuf");
  check(
      reader.resumed_thread(0) != read_origin &&
          reader.resumed_thread(0) != read_notifier_id,
      "read_exact 没有在与原 worker、外部 notifier 不同的 runtime worker 恢复");
  release_worker_and_wait(read_release, read_blockers);

  StepWriter writer{3};
  auto write_handle = runtime.spawn(cio::task::assume_portable(
      cio::io::write_all(writer, lease({7, 8, 9, 10, 11, 12}))));
  check(
      wait_until([&writer] { return writer.calls() == 2 && writer.active(); }),
      "multi-thread write_all 没有停在部分写入后的第二次 write");

  const auto write_origin = writer.call_thread(1);
  auto write_blocked = std::make_shared<std::atomic<bool>>(false);
  auto write_release = std::make_shared<std::atomic<bool>>(false);
  WorkerRelease write_release_guard{write_release};
  auto write_blockers =
      occupy_worker(runtime, write_origin, write_blocked, write_release);

  std::thread write_notifier{[writer] { writer.open_one(); }};
  const auto write_notifier_id = write_notifier.get_id();
  write_notifier.join();
  check(runtime.block_on(cio::task::assume_portable(
            await_native_error(std::move(write_handle)))),
        "跨线程 wake 后 write_all 没有保留 native error");
  check(writer.calls() == 3 && writer.snapshot() == bytes({7, 8, 9, 10}) &&
            !writer.active() && writer.flush_calls() == 0 &&
            writer.shutdown_calls() == 0,
        "跨 worker write_all 没有保留错误前的部分进度或释放 Session");
  check(
      writer.resumed_thread(0) != write_origin &&
          writer.resumed_thread(0) != write_notifier_id,
      "write_all 没有在与原 worker、外部 notifier 不同的 runtime worker 恢复");
  release_worker_and_wait(write_release, write_blockers);
}

void multi_thread_external_abort_release_test() {
  auto builder = cio::runtime::Builder::new_multi_thread();
  auto runtime = builder.worker_threads(4).build();

  StepReader reader{
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({1, 2, 3, 4})})};
  auto target = ReadBuf::with_capacity(4);
  auto read_handle = runtime.spawn(
      cio::task::assume_portable(cio::io::read_exact(reader, target)));
  check(
      wait_until([&reader] { return reader.calls() == 2 && reader.active(); }),
      "multi-thread read_exact 取消前没有形成可观察的部分进度");
  const auto read_worker = reader.call_thread(1);
  const auto read_abort = read_handle.abort_handle();
  std::thread read_aborter{[read_abort] { read_abort.abort(); }};
  const auto read_aborter_id = read_aborter.get_id();
  read_aborter.join();
  check(read_aborter_id != read_worker,
        "read_exact abort 没有来自 runtime worker 外部线程");
  check(runtime.block_on(cio::task::assume_portable(
            await_cancelled(std::move(read_handle)))),
        "外部线程 abort 后 read_exact join 没有返回 cancelled");
  check(!reader.active() && target.filled_snapshot() == bytes({1, 2}),
        "read_exact cancelled join 发布前没有释放 Session/ReadBuf "
        "或保留部分进度");
  {
    auto reused = reader.open_read_session();
    (void)reused;
  }
  reader.open_one();

  StepWriter writer;
  const auto write_source =
      SharedBuffer::copy_from(std::span<const std::byte>{bytes({5, 6, 7, 8})});
  auto write_handle = runtime.spawn(cio::task::assume_portable(
      cio::io::write_all(writer, write_source.lease())));
  check(
      wait_until([&writer] { return writer.calls() == 2 && writer.active(); }),
      "multi-thread write_all 取消前没有形成可观察的部分进度");
  const auto write_worker = writer.call_thread(1);
  const auto write_abort = write_handle.abort_handle();
  std::thread write_aborter{[write_abort] { write_abort.abort(); }};
  const auto write_aborter_id = write_aborter.get_id();
  write_aborter.join();
  check(write_aborter_id != write_worker,
        "write_all abort 没有来自 runtime worker 外部线程");
  check(runtime.block_on(cio::task::assume_portable(
            await_cancelled(std::move(write_handle)))),
        "外部线程 abort 后 write_all join 没有返回 cancelled");
  check(
      !writer.active() && writer.snapshot() == bytes({5, 6}) &&
          write_source.snapshot() == bytes({5, 6, 7, 8}),
      "write_all cancelled join 发布前没有释放 Session、保留部分进度或保持输入 owner");
  {
    auto reused = writer.open_write_session();
    (void)reused;
  }
  writer.open_one();
}

static_assert(cio::io::AsyncRead<StepReader>);
static_assert(cio::io::AsyncWrite<StepWriter>);

} // namespace

int main() {
  struct Case final {
    std::string_view name;
    void (*run)();
  };

  const std::vector<Case> cases{
      {"initiating/empty", initiating_and_empty_test},
      {"exact async semantics", async_semantics_test},
      {"multi-thread migration/partial error",
       multi_thread_migration_and_partial_error_test},
      {"multi-thread external abort/release",
       multi_thread_external_abort_release_test},
  };

  std::size_t passed = 0;
  for (const auto &test : cases) {
    try {
      test.run();
      ++passed;
    } catch (const std::exception &error) {
      std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
      return 1;
    }
  }

  std::cout << "io exact tests passed: " << passed << '/' << cases.size()
            << '\n';
  return 0;
}
