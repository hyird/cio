#include <atomic>
#include <concepts>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

#include "cio/send.hpp"
#include "cio/sync/mutex.hpp"

struct ProjectionRecord final {
  int first{11};
  int second{97};
};

struct UnknownMutexMobility final {};

namespace cio {

template <> struct send_traits<ProjectionRecord> : std::true_type {};
template <> struct sync_traits<ProjectionRecord> : std::true_type {};

} // namespace cio

namespace {

using cio::sync::MappedMutexGuard;
using cio::sync::Mutex;
using cio::sync::MutexGuard;

static_assert(std::same_as<decltype(std::declval<Mutex<int> &>().get_mut()),
                           MutexGuard<int>>);
static_assert(cio::Send<MutexGuard<int>>);
static_assert(!cio::Sync<MutexGuard<int>>);
static_assert(cio::Send<MappedMutexGuard<int, int>>);
static_assert(!cio::Sync<MappedMutexGuard<int, int>>);
static_assert(!cio::Sync<std::shared_ptr<MutexGuard<int>>>);
static_assert(!cio::Sync<std::shared_ptr<MappedMutexGuard<int, int>>>);
static_assert(cio::Send<MappedMutexGuard<ProjectionRecord, int>>);
static_assert(!cio::Sync<MappedMutexGuard<ProjectionRecord, int>>);
static_assert(cio::Send<Mutex<int>::Lock>);
static_assert(!cio::Sync<Mutex<int>::Lock>);
static_assert(cio::Send<Mutex<int>::Lock::Awaiter>);
static_assert(!cio::Sync<Mutex<int>::Lock::Awaiter>);
static_assert(!cio::Send<Mutex<UnknownMutexMobility>::Lock>);

void check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

void test_get_mut_guard_closes_shared_handle_window() {
  Mutex<int> mutex{1};

  {
    auto exclusive = mutex.get_mut();
    auto copy = mutex;
    check(!copy.try_lock().has_value(),
          "get_mut guard 存活时复制句柄不应取得锁");
    bool into_inner_rejected = false;
    try {
      (void)std::move(copy).into_inner();
    } catch (const std::logic_error &) {
      into_inner_rejected = true;
    }
    check(into_inner_rejected,
          "get_mut guard 存活时 into_inner 必须拒绝共享状态");
    *exclusive = 7;
  }

  {
    auto copy = mutex;
    auto acquired = copy.try_lock();
    check(acquired.has_value() && *acquired.value() == 7,
          "get_mut guard 析构后应归还许可并保留写入");
  }

  bool rejected = false;
  {
    auto copy = mutex;
    try {
      (void)mutex.get_mut();
    } catch (const std::logic_error &) {
      rejected = true;
    }
  }
  check(rejected, "存在共享句柄时 get_mut 必须拒绝");
  check(std::move(mutex).into_inner() == 7,
        "释放共享句柄后 into_inner 应移出最终值");
}

void test_projection_runs_once_and_survives_worker_move() {
  Mutex<ProjectionRecord> mutex{ProjectionRecord{}};
  auto acquired = mutex.try_lock();
  check(acquired.has_value(), "测试前置锁获取失败");

  auto projection_calls = std::make_shared<std::atomic<int>>(0);
  auto projection_lifetime = std::make_shared<int>(1);
  std::weak_ptr<int> projection_lifetime_observer{projection_lifetime};
  auto mapped = MutexGuard<ProjectionRecord>::map(
      std::move(acquired).value(),
      [projection_calls,
       projection_lifetime](ProjectionRecord &value) -> int & {
        const auto call =
            projection_calls->fetch_add(1, std::memory_order_relaxed);
        return call == 0 ? value.first : value.second;
      });
  projection_lifetime.reset();

  check(projection_calls->load(std::memory_order_relaxed) == 1,
        "根映射 projection 必须只执行一次");
  check(projection_lifetime_observer.expired(),
        "mapped guard 不得跨暂停或 worker 保存 projection callable");
  check(*mapped == 11 && mapped.get() == 11,
        "mapped guard 重复访问应使用稳定别名视图");

  auto nested_calls = std::make_shared<std::atomic<int>>(0);
  auto nested = MappedMutexGuard<ProjectionRecord, int>::map(
      std::move(mapped), [nested_calls](int &value) -> int & {
        nested_calls->fetch_add(1, std::memory_order_relaxed);
        return value;
      });

  check(nested_calls->load(std::memory_order_relaxed) == 1,
        "嵌套映射 projection 必须只执行一次");

  std::jthread worker{
      [guard = std::move(nested), projection_calls, nested_calls]() mutable {
        check(*guard == 11, "跨 worker 后别名视图读取错误");
        *guard = 23;
        check(guard.get() == 23, "跨 worker 后别名视图写入错误");
        check(projection_calls->load(std::memory_order_relaxed) == 1 &&
                  nested_calls->load(std::memory_order_relaxed) == 1,
              "跨 worker 访问不得重新执行 projection");
      }};
  worker.join();

  auto final = mutex.try_lock();
  check(final.has_value() && (*final.value()).first == 23 &&
            (*final.value()).second == 97,
        "mapped guard 跨 worker 析构后应归还许可");
}

void test_try_map_failure_keeps_original_guard() {
  Mutex<int> mutex{5};
  auto acquired = mutex.try_lock();
  check(acquired.has_value(), "try_map 测试前置锁获取失败");

  auto projection_calls = std::make_shared<std::atomic<int>>(0);
  auto result = MutexGuard<int>::try_map(
      std::move(acquired).value(), [](int &) { return false; },
      [projection_calls](int &value) -> int & {
        projection_calls->fetch_add(1, std::memory_order_relaxed);
        return value;
      });

  check(!result.has_value(), "失败 predicate 不应生成 mapped guard");
  check(projection_calls->load(std::memory_order_relaxed) == 0,
        "失败 predicate 后不得执行 projection");
  auto original = std::move(result).error();
  *original = 9;
}

} // namespace

int main() {
  try {
    test_get_mut_guard_closes_shared_handle_window();
    test_projection_runs_once_and_survives_worker_move();
    test_try_map_failure_keeps_original_guard();
    std::cout << "Mutex 安全回归全部通过：3 项\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "Mutex 安全回归失败：" << error.what() << '\n';
    return 1;
  }
}
