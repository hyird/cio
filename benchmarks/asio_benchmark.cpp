#include <asio/any_io_executor.hpp>
#include <asio/post.hpp>
#include <asio/thread_pool.hpp>
#include <asio/version.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
  if (config.workload != "schedule" && config.workload != "yield") {
    throw std::invalid_argument{"Asio 对照只支持 schedule 与 yield"};
  }
  return config;
}

class Completion final {
public:
  explicit Completion(std::size_t remaining) : remaining_{remaining} {}

  std::future<void> future() { return finished_.get_future(); }

  void complete_one() {
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      finished_.set_value();
    }
  }

private:
  std::atomic<std::size_t> remaining_;
  std::promise<void> finished_;
};

class YieldChain final : public std::enable_shared_from_this<YieldChain> {
public:
  YieldChain(asio::any_io_executor executor, std::size_t remaining)
      : executor_{std::move(executor)}, remaining_{remaining} {}

  std::future<void> future() { return finished_.get_future(); }

  void start() { schedule_next(); }

private:
  void schedule_next() {
    auto self = shared_from_this();
    asio::post(executor_, [self = std::move(self)] { self->resume(); });
  }

  void resume() {
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      finished_.set_value();
      return;
    }
    schedule_next();
  }

  asio::any_io_executor executor_;
  std::atomic<std::size_t> remaining_;
  std::promise<void> finished_;
};

void run_schedule(asio::thread_pool &pool, std::size_t operations) {
  auto completion = std::make_shared<Completion>(operations);
  auto finished = completion->future();
  auto executor = pool.get_executor();
  asio::post(executor, [executor, completion, operations] {
    for (std::size_t index = 0; index < operations; ++index) {
      asio::post(executor, [completion] { completion->complete_one(); });
    }
  });
  finished.get();
}

void run_yield(asio::thread_pool &pool, std::size_t operations) {
  auto chain = std::make_shared<YieldChain>(pool.get_executor(), operations);
  auto finished = chain->future();
  chain->start();
  finished.get();
}

std::vector<std::uint64_t> measure(const Config &config) {
  asio::thread_pool pool{config.workers};
  std::vector<std::uint64_t> samples;
  samples.reserve(config.samples);

  const auto total_runs = config.warmups + config.samples;
  for (std::size_t index = 0; index < total_runs; ++index) {
    const auto started = SteadyClock::now();
    if (config.workload == "schedule") {
      run_schedule(pool, config.operations);
    } else {
      run_yield(pool, config.operations);
    }
    const auto elapsed =
        std::chrono::duration_cast<Nanoseconds>(SteadyClock::now() - started);
    if (index >= config.warmups) {
      samples.push_back(static_cast<std::uint64_t>(elapsed.count()));
    }
  }

  pool.join();
  return samples;
}

void write_result(const Config &config,
                  const std::vector<std::uint64_t> &samples) {
  const auto task_count = config.workload == "schedule" ? config.operations : 1;
  std::cout << "{\"runtime\":\"asio\","
            << "\"runtime_version\":\"" << ASIO_VERSION / 100000 << '.'
            << ASIO_VERSION / 100 % 1000 << '.' << ASIO_VERSION % 100 << "\","
            << "\"compiler\":\"" << compiler_version << "\","
            << "\"build_mode\":\"" << build_mode << "\","
            << "\"runtime_type\":\"thread_pool\","
            << "\"workload\":\"" << config.workload << "\","
            << "\"workers\":" << config.workers << ','
            << "\"tasks\":" << task_count << ','
            << "\"operations\":" << config.operations << ','
            << "\"warmups\":" << config.warmups << ',' << "\"samples_ns\":[";
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
    std::cerr << "Asio benchmark 失败：" << error.what() << '\n';
    return 1;
  }
}
