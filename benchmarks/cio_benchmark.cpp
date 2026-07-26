#include <algorithm>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "cio/cio.hpp"
#include "cio/sync.hpp"
#include "cio/sync/oneshot.hpp"

namespace {

using Nanoseconds = std::chrono::nanoseconds;
using SteadyClock = std::chrono::steady_clock;

#define CIO_BENCH_STRINGIFY_DETAIL(value) #value
#define CIO_BENCH_STRINGIFY(value) CIO_BENCH_STRINGIFY_DETAIL(value)

#if defined(_MSC_VER)
constexpr std::string_view compiler_version{
    "msvc-" CIO_BENCH_STRINGIFY(_MSC_FULL_VER)};
#elif defined(__clang__)
constexpr std::string_view compiler_version{
    "clang-" CIO_BENCH_STRINGIFY(__clang_major__) "." CIO_BENCH_STRINGIFY(
        __clang_minor__) "." CIO_BENCH_STRINGIFY(__clang_patchlevel__)};
#elif defined(__GNUC__)
constexpr std::string_view compiler_version{
    "gcc-" CIO_BENCH_STRINGIFY(__GNUC__) "." CIO_BENCH_STRINGIFY(
        __GNUC_MINOR__) "." CIO_BENCH_STRINGIFY(__GNUC_PATCHLEVEL__)};
#else
constexpr std::string_view compiler_version{"unknown"};
#endif

#if defined(NDEBUG)
constexpr std::string_view build_mode{"release"};
#else
constexpr std::string_view build_mode{"debug"};
#endif

constexpr std::size_t bounded_mpsc_capacity{64};
constexpr std::size_t bounded_mpsc_consumers{1};
constexpr std::size_t broadcast_capacity{64};
constexpr std::size_t io_memory_payload_bytes{64};
constexpr std::uint64_t io_memory_payload_checksum{2016};

struct Config final {
  std::string workload;
  std::size_t workers{0};
  std::size_t operations{0};
  std::size_t warmups{0};
  std::size_t samples{0};
};

Config read_config() {
  Config config;
  if (!(std::cin >> config.workload >> config.workers >> config.operations >>
        config.warmups >> config.samples)) {
    throw std::invalid_argument{
        "需要从标准输入读取：workload workers operations warmups samples"};
  }
  if (config.workers == 0 || config.operations == 0 || config.samples == 0) {
    throw std::invalid_argument{"workers、operations 和 samples 必须大于零"};
  }
  if (config.workload != "schedule" && config.workload != "yield" &&
      config.workload != "mutex" && config.workload != "rwlock_read" &&
      config.workload != "rwlock_write" && config.workload != "rwlock_mixed" &&
      config.workload != "once_cell_ready" &&
      config.workload != "once_cell_init" &&
      config.workload != "set_once_fanout" &&
      config.workload != "oneshot_wake" &&
      config.workload != "mpsc_bounded" &&
      config.workload != "watch_fanout" &&
      config.workload != "broadcast_fanout" &&
      config.workload != "io_memory_ready") {
    throw std::invalid_argument{"未知 benchmark workload"};
  }
  return config;
}

[[nodiscard]] std::size_t partitioned_task_count(std::size_t operations,
                                                 std::size_t workers) {
  return std::min(operations, std::max<std::size_t>(2, workers * 4));
}

[[nodiscard]] std::size_t bounded_mpsc_producer_count(std::size_t operations,
                                                      std::size_t workers) {
  return partitioned_task_count(operations, workers);
}

[[nodiscard]] std::size_t watch_subscriber_count(std::size_t operations,
                                                 std::size_t workers) {
  return partitioned_task_count(operations, workers);
}

[[nodiscard]] std::size_t broadcast_subscriber_count(
    std::size_t operations, std::size_t workers) {
  return partitioned_task_count(operations, workers);
}

struct ScheduleChild final {
  cio::Task<void>
  operator()(std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
    co_return;
  }
};

struct ScheduleRoot final {
  cio::Task<void> operator()(std::size_t operations) const {
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < operations; ++index) {
      auto handle = cio::task::spawn(
          cio::task::owned(ScheduleChild{}, completed, operations, notify));
      (void)handle;
    }

    co_await finished;
    if (completed->load(std::memory_order_acquire) != operations) {
      throw std::runtime_error{"schedule 完成计数错误"};
    }
  }
};

struct YieldRoot final {
  cio::Task<void> operator()(std::size_t operations) const {
    for (std::size_t index = 0; index < operations; ++index) {
      co_await cio::task::yield_now();
    }
  }
};

struct IoMemoryReadyChild final {
  cio::Task<void> operator()(
      std::size_t operations,
      std::shared_ptr<std::atomic<std::size_t>> total_calls,
      std::shared_ptr<std::atomic<std::uint64_t>> total_bytes,
      std::shared_ptr<std::atomic<std::uint64_t>> total_checksum,
      std::shared_ptr<std::atomic<std::size_t>> failures,
      std::shared_ptr<std::atomic<std::size_t>> completed,
      std::size_t target, cio::sync::Notify notify) const {
    std::vector<std::byte> source;
    source.reserve(operations * io_memory_payload_bytes);
    for (std::size_t operation = 0; operation < operations; ++operation) {
      for (std::size_t value = 0; value < io_memory_payload_bytes; ++value) {
        source.push_back(static_cast<std::byte>(value));
      }
    }
    auto reader = cio::io::MemoryReader::from(
        cio::io::SharedBuffer::copy_from(
            std::span<const std::byte>{source}),
        io_memory_payload_bytes);
    auto buffer =
        cio::io::ReadBuf::with_capacity(io_memory_payload_bytes);
    std::size_t local_calls = 0;
    std::uint64_t local_bytes = 0;
    std::uint64_t local_checksum = 0;

    for (std::size_t operation = 0; operation < operations; ++operation) {
      const auto result = co_await cio::io::read(reader, buffer);
      if (!result.has_value() ||
          buffer.filled_size() != io_memory_payload_bytes) {
        failures->fetch_add(1, std::memory_order_relaxed);
        break;
      }
      const auto snapshot = buffer.filled_snapshot();
      if (snapshot.size() != io_memory_payload_bytes) {
        failures->fetch_add(1, std::memory_order_relaxed);
        break;
      }
      for (const auto value : snapshot) {
        local_checksum += std::to_integer<unsigned int>(value);
      }
      ++local_calls;
      local_bytes += snapshot.size();
      buffer.clear();
    }

    total_calls->fetch_add(local_calls, std::memory_order_relaxed);
    total_bytes->fetch_add(local_bytes, std::memory_order_relaxed);
    total_checksum->fetch_add(local_checksum, std::memory_order_relaxed);
    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
    co_return;
  }
};

struct IoMemoryReadyRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    if (operations >
        std::numeric_limits<std::uint64_t>::max() /
            io_memory_payload_bytes ||
        operations >
            std::numeric_limits<std::uint64_t>::max() /
                io_memory_payload_checksum) {
      throw std::invalid_argument{"io_memory_ready 计数溢出"};
    }
    const auto task_count = partitioned_task_count(operations, workers);
    const auto base_operations = operations / task_count;
    const auto remainder = operations % task_count;
    auto total_calls = std::make_shared<std::atomic<std::size_t>>(0);
    auto total_bytes = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto total_checksum =
        std::make_shared<std::atomic<std::uint64_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < task_count; ++index) {
      const auto child_operations =
          base_operations + (index < remainder ? 1 : 0);
      auto child = cio::task::spawn(cio::task::owned(
          IoMemoryReadyChild{}, child_operations, total_calls,
          total_bytes, total_checksum, failures, completed,
          task_count, notify));
      (void)child;
    }

    co_await finished;
    const auto expected_bytes =
        static_cast<std::uint64_t>(operations) *
        io_memory_payload_bytes;
    const auto expected_checksum =
        static_cast<std::uint64_t>(operations) *
        io_memory_payload_checksum;
    if (completed->load(std::memory_order_acquire) != task_count ||
        failures->load(std::memory_order_relaxed) != 0 ||
        total_calls->load(std::memory_order_relaxed) != operations ||
        total_bytes->load(std::memory_order_relaxed) != expected_bytes ||
        total_checksum->load(std::memory_order_relaxed) !=
            expected_checksum) {
      throw std::runtime_error{"io_memory_ready 最终状态错误"};
    }
  }
};

struct OnceCellReadyChild final {
  cio::Task<void>
  operator()(std::shared_ptr<cio::sync::OnceCell<std::uint64_t>> cell,
             std::size_t operations,
             std::shared_ptr<std::atomic<std::uint64_t>> checksum,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      const auto value = cell->get();
      if (!value) {
        throw std::runtime_error{"once_cell_ready 观察到空 cell"};
      }
      checksum->fetch_add(*value, std::memory_order_relaxed);
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
    co_return;
  }
};

struct OnceCellReadyRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    const auto task_count = partitioned_task_count(operations, workers);
    const auto base_operations = operations / task_count;
    const auto remainder = operations % task_count;

    auto cell = std::make_shared<cio::sync::OnceCell<std::uint64_t>>(
        cio::sync::OnceCell<std::uint64_t>::new_with(1));
    auto checksum = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < task_count; ++index) {
      const auto child_operations =
          base_operations + (index < remainder ? 1 : 0);
      auto handle = cio::task::spawn(
          cio::task::owned(OnceCellReadyChild{}, cell, child_operations,
                           checksum, completed, task_count, notify));
      (void)handle;
    }

    co_await finished;
    if (completed->load(std::memory_order_acquire) != task_count ||
        checksum->load(std::memory_order_relaxed) != operations) {
      throw std::runtime_error{"once_cell_ready 最终状态错误"};
    }
  }
};

cio::Task<std::uint64_t>
once_cell_initializer(std::shared_ptr<std::atomic<std::size_t>> factory_calls) {
  factory_calls->fetch_add(1, std::memory_order_relaxed);
  co_await cio::task::yield_now();
  co_return 1;
}

struct OnceCellInitFactory final {
  std::shared_ptr<std::atomic<std::size_t>> factory_calls;
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  cio::Task<std::uint64_t> operator()() const {
    return once_cell_initializer(factory_calls);
  }
};

struct OnceCellInitChild final {
  cio::Task<void>
  operator()(std::shared_ptr<cio::sync::OnceCell<std::uint64_t>> cell,
             std::shared_ptr<std::atomic<std::size_t>> factory_calls,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    const auto value =
        co_await cell->get_or_init(OnceCellInitFactory{factory_calls});
    if (!value || *value != 1) {
      failures->fetch_add(1, std::memory_order_relaxed);
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct OnceCellInitRoot final {
  cio::Task<void> operator()(std::size_t operations) const {
    auto cell = std::make_shared<cio::sync::OnceCell<std::uint64_t>>();
    auto factory_calls = std::make_shared<std::atomic<std::size_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < operations; ++index) {
      auto handle = cio::task::spawn(
          cio::task::owned(OnceCellInitChild{}, cell, factory_calls, failures,
                           completed, operations, notify));
      (void)handle;
    }

    co_await finished;
    const auto value = cell->get();
    if (!value || *value != 1 ||
        factory_calls->load(std::memory_order_relaxed) != 1 ||
        failures->load(std::memory_order_relaxed) != 0 ||
        completed->load(std::memory_order_acquire) != operations) {
      throw std::runtime_error{"once_cell_init 最终状态错误"};
    }
  }
};

class SetOnceFanoutWait final {
public:
  using Wait = cio::sync::SetOnce<std::uint64_t>::Wait;

  class Awaiter final {
  public:
    Awaiter(Wait::Awaiter awaiter,
            std::shared_ptr<std::atomic<std::size_t>> entered) noexcept
        : awaiter_{std::move(awaiter)}, entered_{std::move(entered)} {}

    [[nodiscard]] bool await_ready() { return awaiter_.await_ready(); }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> coroutine) {
      const auto suspended = awaiter_.await_suspend(coroutine);
      entered_->fetch_add(1, std::memory_order_release);
      return suspended;
    }

    [[nodiscard]] std::shared_ptr<const std::uint64_t> await_resume() {
      return awaiter_.await_resume();
    }

  private:
    Wait::Awaiter awaiter_;
    std::shared_ptr<std::atomic<std::size_t>> entered_;
  };

  SetOnceFanoutWait(std::shared_ptr<cio::sync::SetOnce<std::uint64_t>> set_once,
                    std::shared_ptr<std::atomic<std::size_t>> entered) noexcept
      : set_once_{std::move(set_once)}, entered_{std::move(entered)} {}

  [[nodiscard]] Awaiter operator co_await() && {
    auto wait = set_once_->wait();
    return Awaiter{std::move(wait).operator co_await(), std::move(entered_)};
  }

private:
  std::shared_ptr<cio::sync::SetOnce<std::uint64_t>> set_once_;
  std::shared_ptr<std::atomic<std::size_t>> entered_;
};

struct SetOnceFanoutChild final {
  cio::Task<void>
  operator()(std::shared_ptr<cio::sync::SetOnce<std::uint64_t>> set_once,
             std::shared_ptr<std::atomic<std::size_t>> entered,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    const auto value =
        co_await SetOnceFanoutWait{std::move(set_once), std::move(entered)};
    if (!value || *value != 1) {
      failures->fetch_add(1, std::memory_order_relaxed);
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct SetOnceFanoutRoot final {
  cio::Task<void> operator()(std::size_t operations) const {
    auto set_once = std::make_shared<cio::sync::SetOnce<std::uint64_t>>();
    auto entered = std::make_shared<std::atomic<std::size_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < operations; ++index) {
      auto handle = cio::task::spawn(
          cio::task::owned(SetOnceFanoutChild{}, set_once, entered, failures,
                           completed, operations, notify));
      (void)handle;
    }

    while (entered->load(std::memory_order_acquire) != operations) {
      co_await cio::task::yield_now();
    }
    const auto result = set_once->set(1);
    if (!result.has_value()) {
      throw std::runtime_error{"set_once_fanout 首次 set 失败"};
    }

    co_await finished;
    if (failures->load(std::memory_order_relaxed) != 0 ||
        completed->load(std::memory_order_acquire) != operations) {
      throw std::runtime_error{"set_once_fanout 未恢复全部 waiter"};
    }
  }
};

class OneshotWakeReceive final {
public:
  using Receive = cio::sync::oneshot::Receiver<std::uint64_t>::Receive;

  class Awaiter final {
  public:
    Awaiter(Receive::Awaiter awaiter,
            std::shared_ptr<std::atomic<std::size_t>> entered) noexcept
        : awaiter_{std::move(awaiter)}, entered_{std::move(entered)} {}

    [[nodiscard]] bool await_ready() { return awaiter_.await_ready(); }

    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> coroutine) {
      const auto suspended = awaiter_.await_suspend(coroutine);
      if (suspended) {
        entered_->fetch_add(1, std::memory_order_release);
      }
      return suspended;
    }

    [[nodiscard]]
    cio::Result<std::uint64_t, cio::sync::oneshot::error::RecvError>
    await_resume() {
      return awaiter_.await_resume();
    }

  private:
    Receive::Awaiter awaiter_;
    std::shared_ptr<std::atomic<std::size_t>> entered_;
  };

  OneshotWakeReceive(Receive receive,
                     std::shared_ptr<std::atomic<std::size_t>> entered) noexcept
      : receive_{std::move(receive)}, entered_{std::move(entered)} {}

  [[nodiscard]] Awaiter operator co_await() && {
    return Awaiter{std::move(receive_).operator co_await(),
                   std::move(entered_)};
  }

private:
  Receive receive_;
  std::shared_ptr<std::atomic<std::size_t>> entered_;
};

struct OneshotWakeChild final {
  cio::Task<void>
  operator()(cio::sync::oneshot::Receiver<std::uint64_t> receiver,
             std::uint64_t expected,
             std::shared_ptr<std::atomic<std::size_t>> entered,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::uint64_t>> checksum,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    const auto received =
        co_await OneshotWakeReceive{receiver.receive(), std::move(entered)};
    if (!received.has_value() || received.value() != expected) {
      failures->fetch_add(1, std::memory_order_relaxed);
    } else {
      checksum->fetch_add(received.value(), std::memory_order_relaxed);
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct OneshotWakeRoot final {
  cio::Task<void> operator()(std::size_t operations) const {
    using Sender = cio::sync::oneshot::Sender<std::uint64_t>;

    auto entered = std::make_shared<std::atomic<std::size_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto checksum = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    std::vector<Sender> senders;
    senders.reserve(operations);
    for (std::size_t index = 0; index < operations; ++index) {
      auto [sender, receiver] = cio::sync::oneshot::channel<std::uint64_t>();
      senders.push_back(std::move(sender));
      auto handle = cio::task::spawn(
          cio::task::owned(OneshotWakeChild{}, std::move(receiver),
                           static_cast<std::uint64_t>(index + 1), entered,
                           failures, checksum, completed, operations, notify));
      (void)handle;
    }

    while (entered->load(std::memory_order_acquire) != operations) {
      co_await cio::task::yield_now();
    }

    for (std::size_t index = 0; index < operations; ++index) {
      const auto sent =
          std::move(senders[index]).send(static_cast<std::uint64_t>(index + 1));
      if (!sent.has_value()) {
        throw std::runtime_error{"oneshot_wake 同步 send 失败"};
      }
    }

    co_await finished;
    const auto count = static_cast<std::uint64_t>(operations);
    const auto expected_checksum =
        count % 2 == 0 ? (count / 2) * (count + 1) : count * ((count + 1) / 2);
    if (entered->load(std::memory_order_acquire) != operations ||
        completed->load(std::memory_order_acquire) != operations ||
        failures->load(std::memory_order_relaxed) != 0 ||
        checksum->load(std::memory_order_relaxed) != expected_checksum) {
      throw std::runtime_error{"oneshot_wake 值守恒或完成计数错误"};
    }
  }
};

struct BoundedMpscProducer final {
  cio::Task<void>
  operator()(cio::sync::mpsc::Sender<std::uint64_t> sender,
             std::uint64_t first_value, std::size_t operations,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      const auto sent =
          co_await sender.send(first_value + static_cast<std::uint64_t>(index));
      if (!sent.has_value()) {
        failures->fetch_add(1, std::memory_order_relaxed);
        break;
      }
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct BoundedMpscConsumer final {
  cio::Task<void>
  operator()(cio::sync::mpsc::Receiver<std::uint64_t> receiver,
             std::shared_ptr<std::atomic<std::size_t>> received_count,
             std::shared_ptr<std::atomic<std::uint64_t>> checksum,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    std::size_t local_count = 0;
    std::uint64_t local_checksum = 0;
    while (auto received = co_await receiver.recv()) {
      ++local_count;
      local_checksum += received.value();
    }
    received_count->store(local_count, std::memory_order_relaxed);
    checksum->store(local_checksum, std::memory_order_relaxed);

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct BoundedMpscRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    using Sender = cio::sync::mpsc::Sender<std::uint64_t>;

    const auto producer_count =
        bounded_mpsc_producer_count(operations, workers);
    const auto base_operations = operations / producer_count;
    const auto remainder = operations % producer_count;
    const auto task_count = producer_count + bounded_mpsc_consumers;

    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto received_count = std::make_shared<std::atomic<std::size_t>>(0);
    auto checksum = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    {
      auto [sender, receiver] =
          cio::sync::mpsc::channel<std::uint64_t>(bounded_mpsc_capacity);
      auto consumer = cio::task::spawn(cio::task::owned(
          BoundedMpscConsumer{}, std::move(receiver), received_count, checksum,
          completed, task_count, notify));
      (void)consumer;

      std::size_t first_operation = 0;
      for (std::size_t index = 0; index < producer_count; ++index) {
        const auto child_operations =
            base_operations + static_cast<std::size_t>(index < remainder);
        auto producer = cio::task::spawn(cio::task::owned(
            BoundedMpscProducer{}, Sender{sender},
            static_cast<std::uint64_t>(first_operation + 1), child_operations,
            failures, completed, task_count, notify));
        (void)producer;
        first_operation += child_operations;
      }
    }

    co_await finished;
    const auto count = static_cast<std::uint64_t>(operations);
    const auto expected_checksum =
        count % 2 == 0 ? (count / 2) * (count + 1) : count * ((count + 1) / 2);
    if (failures->load(std::memory_order_relaxed) != 0 ||
        received_count->load(std::memory_order_relaxed) != operations ||
        checksum->load(std::memory_order_relaxed) != expected_checksum ||
        completed->load(std::memory_order_acquire) != task_count) {
      throw std::runtime_error{"mpsc_bounded 消息守恒或完成计数错误"};
    }
  }
};

struct WatchSubscriber final {
  cio::Task<void>
  operator()(cio::sync::watch::Receiver<std::uint64_t> receiver,
             std::size_t operations,
             std::shared_ptr<std::atomic<std::size_t>> acknowledgements,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify acknowledgement_notify,
             cio::sync::Notify completion_notify) const {
    for (std::size_t index = 1; index <= operations; ++index) {
      const auto changed = co_await receiver.changed();
      if (!changed.has_value() ||
          receiver.borrow().value() != static_cast<std::uint64_t>(index)) {
        failures->fetch_add(1, std::memory_order_relaxed);
        acknowledgement_notify.notify_one();
        break;
      }
      const auto observed =
          acknowledgements->fetch_add(1, std::memory_order_acq_rel) + 1;
      if (observed % target == 0) {
        acknowledgement_notify.notify_one();
      }
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      completion_notify.notify_one();
    }
  }
};

struct WatchFanoutRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    using Receiver = cio::sync::watch::Receiver<std::uint64_t>;

    const auto subscriber_count =
        watch_subscriber_count(operations, workers);
    if (operations >
        std::numeric_limits<std::size_t>::max() / subscriber_count) {
      throw std::invalid_argument{"watch_fanout delivery 数量溢出"};
    }
    const auto expected_deliveries = operations * subscriber_count;
    auto acknowledgements =
        std::make_shared<std::atomic<std::size_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify acknowledgement_notify;
    cio::sync::Notify completion_notify;
    auto finished = completion_notify.notified();
    (void)finished.enable();

    auto [sender, receiver] =
        cio::sync::watch::channel<std::uint64_t>(0);
    std::vector<Receiver> receivers;
    receivers.reserve(subscriber_count);
    receivers.push_back(std::move(receiver));
    for (std::size_t index = 1; index < subscriber_count; ++index) {
      receivers.push_back(sender.subscribe());
    }
    for (auto &child_receiver : receivers) {
      auto child = cio::task::spawn(cio::task::owned(
          WatchSubscriber{}, std::move(child_receiver), operations,
          acknowledgements, failures, completed, subscriber_count,
          acknowledgement_notify, completion_notify));
      (void)child;
    }

    for (std::size_t index = 1; index <= operations; ++index) {
      const auto sent = sender.send(static_cast<std::uint64_t>(index));
      if (!sent.has_value()) {
        throw std::runtime_error{"watch_fanout 发布时 Receiver 意外关闭"};
      }
      const auto target = index * subscriber_count;
      for (;;) {
        auto notified = acknowledgement_notify.notified();
        (void)notified.enable();
        if (failures->load(std::memory_order_acquire) != 0) {
          throw std::runtime_error{"watch_fanout subscriber 观察版本失败"};
        }
        if (acknowledgements->load(std::memory_order_acquire) == target) {
          break;
        }
        co_await std::move(notified);
      }
    }

    co_await finished;
    if (failures->load(std::memory_order_relaxed) != 0 ||
        acknowledgements->load(std::memory_order_acquire) !=
            expected_deliveries ||
        completed->load(std::memory_order_acquire) != subscriber_count) {
      throw std::runtime_error{"watch_fanout 版本或 delivery 守恒错误"};
    }
  }
};

struct BroadcastSubscriber final {
  cio::Task<void>
  operator()(cio::sync::broadcast::Receiver<std::uint64_t> receiver,
             std::size_t operations,
             std::shared_ptr<std::atomic<std::size_t>> acknowledgements,
             std::shared_ptr<std::atomic<std::size_t>> failures,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify acknowledgement_notify,
             cio::sync::Notify completion_notify) const {
    for (std::size_t index = 1; index <= operations; ++index) {
      const auto received = co_await receiver.recv();
      if (!received.has_value() ||
          received.value() != static_cast<std::uint64_t>(index)) {
        failures->fetch_add(1, std::memory_order_release);
        acknowledgement_notify.notify_one();
        break;
      }
      const auto observed =
          acknowledgements->fetch_add(1, std::memory_order_acq_rel) + 1;
      if (observed % target == 0) {
        acknowledgement_notify.notify_one();
      }
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      completion_notify.notify_one();
    }
  }
};

struct BroadcastFanoutRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    using Receiver =
        cio::sync::broadcast::Receiver<std::uint64_t>;

    const auto subscriber_count =
        broadcast_subscriber_count(operations, workers);
    if (operations >
        std::numeric_limits<std::size_t>::max() / subscriber_count) {
      throw std::invalid_argument{"broadcast_fanout delivery 数量溢出"};
    }
    const auto expected_deliveries = operations * subscriber_count;
    auto acknowledgements =
        std::make_shared<std::atomic<std::size_t>>(0);
    auto failures = std::make_shared<std::atomic<std::size_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify acknowledgement_notify;
    cio::sync::Notify completion_notify;
    auto finished = completion_notify.notified();
    (void)finished.enable();

    auto [sender, receiver] =
        cio::sync::broadcast::channel<std::uint64_t>(
            broadcast_capacity);
    std::vector<Receiver> receivers;
    receivers.reserve(subscriber_count);
    receivers.push_back(std::move(receiver));
    for (std::size_t index = 1; index < subscriber_count; ++index) {
      receivers.push_back(sender.subscribe());
    }
    for (auto &child_receiver : receivers) {
      auto child = cio::task::spawn(cio::task::owned(
          BroadcastSubscriber{}, std::move(child_receiver), operations,
          acknowledgements, failures, completed, subscriber_count,
          acknowledgement_notify, completion_notify));
      (void)child;
    }

    for (std::size_t index = 1; index <= operations; ++index) {
      const auto sent =
          sender.send(static_cast<std::uint64_t>(index));
      if (!sent.has_value() || sent.value() != subscriber_count) {
        throw std::runtime_error{
            "broadcast_fanout 发布时 Receiver 数错误"};
      }
      const auto target = index * subscriber_count;
      for (;;) {
        auto notified = acknowledgement_notify.notified();
        (void)notified.enable();
        if (failures->load(std::memory_order_acquire) != 0) {
          throw std::runtime_error{
              "broadcast_fanout subscriber 观察值失败"};
        }
        if (acknowledgements->load(std::memory_order_acquire) == target) {
          break;
        }
        co_await std::move(notified);
      }
    }

    co_await finished;
    if (failures->load(std::memory_order_acquire) != 0 ||
        acknowledgements->load(std::memory_order_acquire) !=
            expected_deliveries ||
        completed->load(std::memory_order_acquire) != subscriber_count) {
      throw std::runtime_error{
          "broadcast_fanout delivery 或完成计数错误"};
    }
  }
};

struct MutexChild final {
  cio::Task<void>
  operator()(cio::sync::Mutex<std::uint64_t> mutex, std::size_t operations,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      {
        auto guard = co_await mutex.lock_owned();
        co_await cio::task::yield_now();
        ++guard.get();
      }
      co_await cio::task::yield_now();
    }

    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct MutexRoot final {
  cio::Task<void> operator()(std::size_t operations,
                             std::size_t workers) const {
    const auto task_count = partitioned_task_count(operations, workers);
    const auto base_operations = operations / task_count;
    const auto remainder = operations % task_count;

    cio::sync::Mutex<std::uint64_t> mutex{0};
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();

    for (std::size_t index = 0; index < task_count; ++index) {
      const auto child_operations =
          base_operations + (index < remainder ? 1 : 0);
      auto handle = cio::task::spawn(
          cio::task::owned(MutexChild{}, mutex, child_operations, completed,
                           task_count, notify));
      (void)handle;
    }

    co_await finished;
    auto guard = co_await mutex.lock_owned();
    if (guard.get() != operations) {
      throw std::runtime_error{"mutex 最终计数错误"};
    }
  }
};

struct RwLockReadChild final {
  cio::Task<void>
  operator()(cio::sync::RwLock<std::uint64_t> rwlock, std::size_t operations,
             std::shared_ptr<std::atomic<std::uint64_t>> checksum,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      {
        auto guard = co_await rwlock.read_owned();
        co_await cio::task::yield_now();
        checksum->fetch_add(guard.get(), std::memory_order_relaxed);
      }
      co_await cio::task::yield_now();
    }
    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct RwLockWriteChild final {
  cio::Task<void>
  operator()(cio::sync::RwLock<std::uint64_t> rwlock, std::size_t operations,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      {
        auto guard = co_await rwlock.write_owned();
        co_await cio::task::yield_now();
        ++guard.get();
      }
      co_await cio::task::yield_now();
    }
    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct RwLockMixedChild final {
  cio::Task<void>
  operator()(cio::sync::RwLock<std::uint64_t> rwlock,
             std::size_t first_operation, std::size_t operations,
             std::shared_ptr<std::atomic<std::uint64_t>> checksum,
             std::shared_ptr<std::atomic<std::size_t>> completed,
             std::size_t target, cio::sync::Notify notify) const {
    for (std::size_t index = 0; index < operations; ++index) {
      const auto global_index = first_operation + index;
      if (global_index % 5 == 0) {
        auto guard = co_await rwlock.write_owned();
        co_await cio::task::yield_now();
        ++guard.get();
      } else {
        auto guard = co_await rwlock.read_owned();
        co_await cio::task::yield_now();
        checksum->fetch_add(guard.get(), std::memory_order_relaxed);
      }
      co_await cio::task::yield_now();
    }
    const auto observed =
        completed->fetch_add(1, std::memory_order_acq_rel) + 1;
    if (observed == target) {
      notify.notify_one();
    }
  }
};

struct RwLockRoot final {
  cio::Task<void> operator()(std::string workload, std::size_t operations,
                             std::size_t workers) const {
    const auto task_count = partitioned_task_count(operations, workers);
    const auto base_operations = operations / task_count;
    const auto remainder = operations % task_count;

    cio::sync::RwLock<std::uint64_t> rwlock{workload == "rwlock_read" ? 1U
                                                                      : 0U};
    auto checksum = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto completed = std::make_shared<std::atomic<std::size_t>>(0);
    cio::sync::Notify notify;
    auto finished = notify.notified();
    (void)finished.enable();
    std::size_t first_operation = 0;

    for (std::size_t index = 0; index < task_count; ++index) {
      const auto child_operations =
          base_operations + (index < remainder ? 1 : 0);
      cio::task::JoinHandle<void> handle;
      if (workload == "rwlock_read") {
        handle = cio::task::spawn(
            cio::task::owned(RwLockReadChild{}, rwlock, child_operations,
                             checksum, completed, task_count, notify));
      } else if (workload == "rwlock_write") {
        handle = cio::task::spawn(cio::task::owned(RwLockWriteChild{}, rwlock,
                                                   child_operations, completed,
                                                   task_count, notify));
      } else {
        handle = cio::task::spawn(cio::task::owned(
            RwLockMixedChild{}, rwlock, first_operation, child_operations,
            checksum, completed, task_count, notify));
      }
      (void)handle;
      first_operation += child_operations;
    }

    co_await finished;
    auto guard = co_await rwlock.read_owned();
    const auto expected_writes =
        workload == "rwlock_write"
            ? operations
            : (workload == "rwlock_mixed" ? (operations + 4) / 5 : 0);
    if (guard.get() != (workload == "rwlock_read" ? 1U : expected_writes) ||
        (workload == "rwlock_read" &&
         checksum->load(std::memory_order_relaxed) != operations)) {
      throw std::runtime_error{"rwlock 最终状态错误"};
    }
  }
};

void run_once(cio::runtime::Runtime &runtime, const Config &config) {
  if (config.workload == "schedule") {
    runtime.block_on(cio::task::owned(ScheduleRoot{}, config.operations));
    return;
  }
  if (config.workload == "yield") {
    runtime.block_on(cio::task::owned(YieldRoot{}, config.operations));
    return;
  }
  if (config.workload == "io_memory_ready") {
    runtime.block_on(cio::task::owned(
        IoMemoryReadyRoot{}, config.operations, config.workers));
    return;
  }
  if (config.workload == "once_cell_ready") {
    runtime.block_on(cio::task::owned(OnceCellReadyRoot{}, config.operations,
                                      config.workers));
    return;
  }
  if (config.workload == "once_cell_init") {
    runtime.block_on(cio::task::owned(OnceCellInitRoot{}, config.operations));
    return;
  }
  if (config.workload == "set_once_fanout") {
    runtime.block_on(cio::task::owned(SetOnceFanoutRoot{}, config.operations));
    return;
  }
  if (config.workload == "oneshot_wake") {
    runtime.block_on(cio::task::owned(OneshotWakeRoot{}, config.operations));
    return;
  }
  if (config.workload == "mpsc_bounded") {
    runtime.block_on(
        cio::task::owned(BoundedMpscRoot{}, config.operations, config.workers));
    return;
  }
  if (config.workload == "watch_fanout") {
    runtime.block_on(
        cio::task::owned(WatchFanoutRoot{}, config.operations, config.workers));
    return;
  }
  if (config.workload == "broadcast_fanout") {
    runtime.block_on(cio::task::owned(
        BroadcastFanoutRoot{}, config.operations, config.workers));
    return;
  }
  if (config.workload == "mutex") {
    runtime.block_on(
        cio::task::owned(MutexRoot{}, config.operations, config.workers));
    return;
  }
  runtime.block_on(cio::task::owned(RwLockRoot{}, config.workload,
                                    config.operations, config.workers));
}

std::vector<std::uint64_t> measure(const Config &config) {
  auto builder = config.workers == 1
                     ? cio::runtime::Builder::new_current_thread()
                     : cio::runtime::Builder::new_multi_thread();
  if (config.workers > 1) {
    builder.worker_threads(config.workers);
  }
  auto runtime = builder.build();

  std::vector<std::uint64_t> samples;
  samples.reserve(config.samples);
  const auto total_runs = config.warmups + config.samples;
  for (std::size_t index = 0; index < total_runs; ++index) {
    const auto started = SteadyClock::now();
    run_once(runtime, config);
    const auto elapsed =
        std::chrono::duration_cast<Nanoseconds>(SteadyClock::now() - started);
    if (index >= config.warmups) {
      samples.push_back(static_cast<std::uint64_t>(elapsed.count()));
    }
  }
  return samples;
}

void write_result(const Config &config,
                  const std::vector<std::uint64_t> &samples) {
  const auto task_count =
      config.workload == "mpsc_bounded"
          ? bounded_mpsc_producer_count(config.operations, config.workers) +
                bounded_mpsc_consumers
      : config.workload == "watch_fanout"
          ? watch_subscriber_count(config.operations, config.workers)
      : config.workload == "broadcast_fanout"
          ? broadcast_subscriber_count(config.operations, config.workers)
      : config.workload == "schedule" || config.workload == "once_cell_init" ||
              config.workload == "set_once_fanout" ||
              config.workload == "oneshot_wake"
          ? config.operations
          : (config.workload == "yield"
                 ? 1
                 : partitioned_task_count(config.operations, config.workers));
  std::cout << "{\"runtime\":\"cio\","
            << "\"runtime_version\":\"0.1.0\","
            << "\"compiler\":\"" << compiler_version << "\","
            << "\"build_mode\":\"" << build_mode << "\","
            << "\"runtime_type\":\""
            << (config.workers == 1 ? "current_thread" : "multi_thread")
            << "\","
            << "\"workload\":\"" << config.workload << "\","
            << "\"workers\":" << config.workers << ','
            << "\"tasks\":" << task_count << ','
            << "\"operations\":" << config.operations << ','
            << "\"warmups\":" << config.warmups;
  if (config.workload == "mpsc_bounded") {
    std::cout << ",\"channel_capacity\":" << bounded_mpsc_capacity
              << ",\"producers\":"
              << bounded_mpsc_producer_count(config.operations, config.workers)
              << ",\"consumers\":" << bounded_mpsc_consumers;
  }
  if (config.workload == "watch_fanout") {
    const auto subscribers =
        watch_subscriber_count(config.operations, config.workers);
    std::cout << ",\"subscribers\":" << subscribers
              << ",\"deliveries\":" << config.operations * subscribers;
  }
  if (config.workload == "broadcast_fanout") {
    const auto subscribers =
        broadcast_subscriber_count(config.operations, config.workers);
    std::cout << ",\"channel_capacity\":" << broadcast_capacity
              << ",\"subscribers\":" << subscribers
              << ",\"deliveries\":" << config.operations * subscribers;
  }
  if (config.workload == "io_memory_ready") {
    std::cout << ",\"bytes\":"
              << config.operations * io_memory_payload_bytes
              << ",\"payload_bytes\":" << io_memory_payload_bytes;
  }
  std::cout << ",\"samples_ns\":[";
  for (std::size_t index = 0; index < samples.size(); ++index) {
    if (index != 0) {
      std::cout << ',';
    }
    std::cout << samples[index];
  }
  std::cout << "]}\n";
}

#undef CIO_BENCH_STRINGIFY
#undef CIO_BENCH_STRINGIFY_DETAIL

} // namespace

int main() {
  try {
    const auto config = read_config();
    write_result(config, measure(config));
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "CIO benchmark 失败：" << error.what() << '\n';
    return 1;
  }
}
