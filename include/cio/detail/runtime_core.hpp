#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/detail/blocking_pool.hpp"
#include "cio/detail/execution_context.hpp"
#include "cio/detail/join_state.hpp"
#include "cio/detail/task_key.hpp"
#include "cio/detail/timer_driver.hpp"
#include "cio/detail/timer_state.hpp"
#include "cio/result.hpp"
#include "cio/task/id.hpp"
#include "cio/task/join_error.hpp"
#include "cio/task/join_handle.hpp"
#include "cio/task/portable.hpp"
#include "cio/task/task.hpp"

namespace cio::detail {

class RuntimeState;

struct WorkerBinding final {
  std::uint64_t runtime_nonce{0};
  std::size_t executor_core{0};
};

inline thread_local std::optional<WorkerBinding> active_worker_binding;

class ScopedWorkerBinding final {
 public:
  explicit ScopedWorkerBinding(WorkerBinding binding) noexcept
      : previous_{std::exchange(
            active_worker_binding,
            std::optional<WorkerBinding>{binding})} {}

  ScopedWorkerBinding(const ScopedWorkerBinding&) = delete;
  ScopedWorkerBinding& operator=(const ScopedWorkerBinding&) = delete;

  ~ScopedWorkerBinding() {
    active_worker_binding = std::move(previous_);
  }

 private:
  std::optional<WorkerBinding> previous_;
};

inline std::atomic<std::uint64_t> next_runtime_nonce{1};

inline std::uint64_t allocate_runtime_nonce() noexcept {
  const auto nonce =
      next_runtime_nonce.fetch_add(1, std::memory_order_relaxed);
  if (nonce == 0) {
    std::terminate();
  }
  return nonce;
}

/**
 * slot-map 所拥有的 task 状态机基类。
 *
 * ready queue 与外部 wake 只携带 TaskKey。协程恢复位置只保存在派生 Task 的
 * CoroutineOwner 中，任何 key 解析都同时检查 runtime nonce 与 generation。
 */
class TaskControlBase {
 public:
  TaskControlBase(
      std::weak_ptr<RuntimeState> runtime,
      TaskKey key,
      task::Id id,
      std::shared_ptr<AbortRegistration> registration) noexcept
      : runtime_{std::move(runtime)},
        key_{key},
        id_{id},
        registration_{std::move(registration)} {}

  virtual ~TaskControlBase() = default;

  TaskControlBase(const TaskControlBase&) = delete;
  TaskControlBase& operator=(const TaskControlBase&) = delete;

  void initialize();
  void request_abort() noexcept;
  void cancel_for_shutdown() noexcept;
  void run_scheduled(
      std::optional<std::size_t> executor_core = std::nullopt) noexcept;

  [[nodiscard]] bool try_mark_scheduled() noexcept {
    if (!active_.load(std::memory_order_acquire)) {
      return false;
    }
    auto state = schedule_state_.load(std::memory_order_acquire);
    while (true) {
      if (!active_.load(std::memory_order_acquire)) {
        return false;
      }
      if ((state & running_bit) != 0) {
        const auto desired =
            static_cast<std::uint8_t>(state | notified_bit);
        if (schedule_state_.compare_exchange_weak(
                state,
                desired,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          return false;
        }
        continue;
      }
      if ((state & scheduled_bit) != 0) {
        return false;
      }
      const auto desired =
          static_cast<std::uint8_t>(state | scheduled_bit);
      if (schedule_state_.compare_exchange_weak(
              state,
              desired,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return true;
      }
    }
  }

  void park(CoroutineRef coroutine) noexcept {
    if (active_.load(std::memory_order_acquire)) {
      set_resumable(coroutine);
    }
  }

 protected:
  [[nodiscard]] task::Id id() const noexcept {
    return id_;
  }

  void mark_inactive() noexcept {
    active_.store(false, std::memory_order_release);
    schedule_state_.store(0, std::memory_order_release);
  }

  void remove_from_runtime();

 private:
  virtual void bind_root_context(
      const std::shared_ptr<ExecutionContext>& context) noexcept = 0;
  virtual void set_resumable(CoroutineRef coroutine) noexcept = 0;
  virtual void resume_current() = 0;
  [[nodiscard]] virtual bool root_done() const noexcept = 0;
  virtual void finish_normally() noexcept = 0;
  virtual void cancel_now() noexcept = 0;

  [[nodiscard]] bool begin_poll() noexcept;
  void finish_pending_poll(
      std::optional<std::size_t> executor_core) noexcept;

  static constexpr std::uint8_t scheduled_bit = 1U << 0U;
  static constexpr std::uint8_t running_bit = 1U << 1U;
  static constexpr std::uint8_t notified_bit = 1U << 2U;

  std::weak_ptr<RuntimeState> runtime_;
  TaskKey key_;
  task::Id id_;
  std::shared_ptr<AbortRegistration> registration_;
  std::shared_ptr<ExecutionContext> execution_context_;
  std::atomic<bool> abort_requested_{false};
  std::atomic<std::uint8_t> schedule_state_{0};
  std::atomic<bool> active_{true};
};

class RuntimeState final : public std::enable_shared_from_this<RuntimeState> {
 private:
  struct ExecutorCore;

 public:
  explicit RuntimeState(
      bool enable_time,
      bool start_paused = false,
      std::size_t worker_count = 0,
      std::size_t max_blocking_threads = 512)
      : runtime_nonce_{allocate_runtime_nonce()},
        time_enabled_{enable_time},
        multi_thread_{worker_count != 0},
        worker_count_{worker_count},
        blocking_pool_{
            std::make_shared<BlockingPool>(max_blocking_threads)},
        timer_driver_{runtime_nonce_},
        clock_base_{std::chrono::steady_clock::now()},
        time_origin_{clock_base_} {
    if (!start_paused) {
      unfrozen_since_.emplace(std::chrono::steady_clock::now());
    }
  }
  RuntimeState(const RuntimeState&) = delete;
  RuntimeState& operator=(const RuntimeState&) = delete;

  void start_workers() {
    if (!multi_thread_) {
      return;
    }
    executor_cores_.reserve(worker_count_);
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
      executor_cores_.push_back(std::make_shared<ExecutorCore>());
    }
    const auto weak_runtime = weak_from_this();
    for (std::size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back([weak_runtime, index] {
        if (const auto runtime = weak_runtime.lock()) {
          runtime->worker_loop(index);
        }
      });
    }
  }

  [[nodiscard]] bool is_multi_thread() const noexcept {
    return multi_thread_;
  }

  [[nodiscard]] std::size_t worker_count() const noexcept {
    return worker_count_;
  }

  template <typename T>
  task::JoinHandle<T> spawn(Task<T> task, bool portable = false);

  template <typename Factory, typename... Args>
  auto spawn_blocking(
      Factory factory,
      std::tuple<Args...> arguments)
      -> task::JoinHandle<
          std::invoke_result_t<Factory, Args...>>;

  [[nodiscard]] std::size_t max_blocking_threads() const noexcept {
    return blocking_pool_->thread_cap();
  }

  [[nodiscard]] std::size_t blocking_thread_count() const noexcept {
    return blocking_pool_->thread_count();
  }

  [[nodiscard]] std::size_t blocking_queue_depth() const noexcept {
    return blocking_pool_->queue_depth();
  }

  void schedule(TaskKey key) noexcept {
    const auto control = resolve_task(key);
    if (!control || !control->try_mark_scheduled()) {
      return;
    }
    enqueue_marked(key, current_executor_core());
  }

  void enqueue_marked(
      TaskKey key,
      std::optional<std::size_t> executor_core) noexcept {
    if (!accepting_.load(std::memory_order_acquire)) {
      return;
    }
    if (multi_thread_ && executor_core &&
        *executor_core < executor_cores_.size()) {
      enqueue_local(*executor_core, key, true);
      return;
    }
    {
      std::lock_guard lock{queue_mutex_};
      if (!accepting_.load(std::memory_order_acquire)) {
        return;
      }
      ready_queue_.push_back(key);
    }
    publish_work();
  }

  void park(TaskKey key, CoroutineRef coroutine) noexcept {
    if (const auto control = resolve_task(key)) {
      control->park(coroutine);
    }
  }

  void request_abort(TaskKey key) noexcept {
    if (const auto control = resolve_task(key)) {
      control->request_abort();
    }
  }

  void cancel_task_now(TaskKey key) noexcept {
    if (const auto control = resolve_task(key)) {
      if (multi_thread_) {
        // 其他 worker 可能正在 poll 目标 task；多线程下只能发布合作式取消。
        control->request_abort();
      } else {
        control->cancel_for_shutdown();
      }
    }
  }

  [[nodiscard]] bool time_enabled() const noexcept {
    return time_enabled_;
  }

  [[nodiscard]] std::chrono::steady_clock::time_point clock_now() const {
    std::lock_guard lock{time_mutex_};
    return clock_now_locked();
  }

  void pause_clock() {
    if (multi_thread_) {
      throw std::logic_error{
          "time::pause 只支持 current-thread runtime"};
    }
    {
      std::lock_guard lock{time_mutex_};
      if (!unfrozen_since_) {
        throw std::logic_error{"CIO 时钟已经冻结"};
      }
      clock_base_ = clock_now_locked();
      unfrozen_since_.reset();
    }
    publish_work();
  }

  void resume_clock() {
    {
      std::lock_guard lock{time_mutex_};
      if (unfrozen_since_) {
        throw std::logic_error{"CIO 时钟没有冻结"};
      }
      unfrozen_since_.emplace(std::chrono::steady_clock::now());
    }
    publish_work();
  }

  void advance_clock(std::chrono::steady_clock::duration duration) {
    {
      std::lock_guard lock{time_mutex_};
      if (unfrozen_since_) {
        throw std::logic_error{"time::advance 只能推进已冻结的 CIO 时钟"};
      }
      if (duration < std::chrono::steady_clock::duration::zero() ||
          std::chrono::steady_clock::time_point::max() - clock_base_ <
              duration) {
        throw std::overflow_error{"CIO 时钟推进发生溢出"};
      }
      clock_base_ += duration;
    }
    publish_work();
  }

  void inhibit_auto_advance() noexcept {
    {
      std::lock_guard lock{time_mutex_};
      ++auto_advance_inhibit_count_;
    }
    publish_work();
  }

  void allow_auto_advance() noexcept {
    {
      std::lock_guard lock{time_mutex_};
      if (auto_advance_inhibit_count_ == 0) {
        std::terminate();
      }
      --auto_advance_inhibit_count_;
    }
    publish_work();
  }

  TimerKey register_timer(
      std::chrono::steady_clock::time_point deadline,
      std::uint64_t epoch,
      std::weak_ptr<TimerWaitState> state) {
    TimerKey key;
    {
      std::lock_guard lock{time_mutex_};
      ensure_time_enabled();
      key = timer_driver_.insert(
          deadline_to_tick_locked(deadline),
          epoch,
          std::move(state));
    }
    publish_work();
    return key;
  }

  TimerKey replace_timer(
      TimerKey old_key,
      std::chrono::steady_clock::time_point deadline,
      std::uint64_t epoch,
      std::weak_ptr<TimerWaitState> state) {
    TimerKey key;
    {
      std::lock_guard lock{time_mutex_};
      ensure_time_enabled();
      key = timer_driver_.replace(
          old_key,
          deadline_to_tick_locked(deadline),
          epoch,
          std::move(state));
    }
    publish_work();
    return key;
  }

  void cancel_timer(TimerKey key) noexcept {
    {
      std::lock_guard lock{time_mutex_};
      timer_driver_.cancel(key);
    }
    publish_work();
  }

  [[nodiscard]] std::size_t active_timer_count() const noexcept {
    std::lock_guard lock{time_mutex_};
    return timer_driver_.active_timer_count();
  }

  [[nodiscard]] bool timer_deadline_elapsed(
      std::chrono::steady_clock::time_point deadline) const {
    std::lock_guard lock{time_mutex_};
    ensure_time_enabled();
    return deadline_to_tick_locked(deadline) <=
           timer_driver_.elapsed_tick();
  }

  void run_until(const std::function<bool()>& stop_condition) {
    std::unique_lock run_lock{run_mutex_};

    if (multi_thread_) {
      const auto completed = stop_condition;
      std::unique_lock queue_lock{queue_mutex_};
      while (!completed()) {
        const auto observed =
            work_sequence_.load(std::memory_order_acquire);
        queue_changed_.wait_for(
            queue_lock,
            std::chrono::milliseconds{10},
            [this, observed, completed] {
              return completed() ||
                     workers_stop_.load(std::memory_order_acquire) ||
                     work_sequence_.load(std::memory_order_acquire) !=
                         observed;
            });
        if (workers_stop_.load(std::memory_order_acquire) &&
            !completed()) {
          throw std::runtime_error{"CIO multi-thread runtime 已关闭"};
        }
      }
      return;
    }

    while (!stop_condition()) {
      process_due_timers();
      if (stop_condition()) {
        break;
      }

      TaskKey key;
      {
        std::unique_lock queue_lock{queue_mutex_};
        if (ready_queue_.empty() &&
            accepting_.load(std::memory_order_acquire)) {
          if (try_auto_advance_to_next_timer()) {
            queue_lock.unlock();
            continue;
          }

          const auto wait_duration = next_timer_wait_duration();
          if (wait_duration) {
            if (*wait_duration <=
                std::chrono::steady_clock::duration::zero()) {
              queue_lock.unlock();
              continue;
            }
            queue_changed_.wait_for(queue_lock, *wait_duration);
          } else {
            queue_changed_.wait(queue_lock);
          }
        }
        if (ready_queue_.empty() &&
            accepting_.load(std::memory_order_acquire)) {
          queue_lock.unlock();
          continue;
        }
        if (ready_queue_.empty() &&
            !accepting_.load(std::memory_order_acquire)) {
          throw std::runtime_error{"CIO runtime 已关闭"};
        }
        key = ready_queue_.front();
        ready_queue_.pop_front();
      }

      if (const auto control = resolve_task(key)) {
        control->run_scheduled();
      }
    }
  }

  void remove_task(TaskKey key) {
    {
      std::lock_guard lock{tasks_mutex_};
      if (!matches_runtime(key)) {
        return;
      }

      const auto slot_index =
          static_cast<std::size_t>(TaskKeyFactory::slot(key));
      if (slot_index >= task_slots_.size()) {
        return;
      }

      auto& slot = task_slots_[slot_index];
      if (slot.generation != TaskKeyFactory::generation(key) ||
          !slot.control) {
        return;
      }

      slot.control.reset();
      ++slot.generation;
      if (slot.generation == 0) {
        std::terminate();
      }
      free_slots_.push_back(
          static_cast<std::uint32_t>(slot_index));
    }
    tasks_changed_.notify_all();
    publish_work();
  }

  void shutdown() noexcept {
    if (multi_thread_) {
      shutdown_multi_thread();
      return;
    }
    std::unique_lock run_lock{run_mutex_};
    std::vector<std::shared_ptr<TaskControlBase>> controls;
    {
      std::lock_guard tasks_lock{tasks_mutex_};
      if (shutdown_started_) {
        return;
      }
      shutdown_started_ = true;
      controls.reserve(task_slots_.size());
      for (const auto& slot : task_slots_) {
        if (slot.control) {
          controls.push_back(slot.control);
        }
      }
    }

    accepting_.store(false, std::memory_order_release);
    queue_changed_.notify_all();

    for (const auto& control : controls) {
      control->cancel_for_shutdown();
    }

    {
      std::lock_guard queue_lock{queue_mutex_};
      ready_queue_.clear();
    }
    {
      std::lock_guard time_lock{time_mutex_};
      timer_driver_.shutdown();
    }
    blocking_pool_->shutdown();
  }

 private:
  struct ExecutorCore final {
    mutable std::mutex mutex;
    std::deque<TaskKey> local_ready;
    std::optional<TaskKey> runnext;
  };

  struct TaskSlot final {
    std::uint64_t generation{1};
    std::shared_ptr<TaskControlBase> control;
  };

  [[nodiscard]] bool matches_runtime(TaskKey key) const noexcept {
    return key.valid() &&
           TaskKeyFactory::runtime_nonce(key) == runtime_nonce_;
  }

  [[nodiscard]] std::shared_ptr<TaskControlBase> resolve_task(
      TaskKey key) const noexcept {
    std::lock_guard lock{tasks_mutex_};
    if (!matches_runtime(key)) {
      return {};
    }

    const auto slot_index =
        static_cast<std::size_t>(TaskKeyFactory::slot(key));
    if (slot_index >= task_slots_.size()) {
      return {};
    }

    const auto& slot = task_slots_[slot_index];
    if (slot.generation != TaskKeyFactory::generation(key)) {
      return {};
    }
    return slot.control;
  }

  [[nodiscard]] std::optional<std::size_t>
  current_executor_core() const noexcept {
    if (!multi_thread_ || !active_worker_binding ||
        active_worker_binding->runtime_nonce != runtime_nonce_) {
      return std::nullopt;
    }
    return active_worker_binding->executor_core;
  }

  void publish_work() noexcept {
    work_sequence_.fetch_add(1, std::memory_order_release);
    queue_changed_.notify_one();
  }

  void enqueue_local(
      std::size_t core_index,
      TaskKey key,
      bool prefer_runnext) noexcept {
    constexpr std::size_t local_capacity = 256;
    constexpr std::size_t spill_count = local_capacity / 2;
    std::vector<TaskKey> spill;
    const auto core = executor_cores_[core_index];
    {
      std::lock_guard lock{core->mutex};
      if (prefer_runnext && !core->runnext) {
        core->runnext = key;
      } else {
        if (core->local_ready.size() >= local_capacity) {
          spill.reserve(spill_count);
          for (std::size_t count = 0;
               count < spill_count && !core->local_ready.empty();
               ++count) {
            spill.push_back(core->local_ready.front());
            core->local_ready.pop_front();
          }
        }
        core->local_ready.push_back(key);
      }
    }

    if (!spill.empty()) {
      std::lock_guard lock{queue_mutex_};
      for (const auto spilled_key : spill) {
        ready_queue_.push_back(spilled_key);
      }
    }
    publish_work();
  }

  [[nodiscard]] std::optional<TaskKey> take_local(
      std::size_t core_index,
      std::size_t& runnext_streak) noexcept {
    constexpr std::size_t runnext_limit = 3;
    const auto core = executor_cores_[core_index];
    std::lock_guard lock{core->mutex};

    if (core->runnext && runnext_streak < runnext_limit) {
      const auto key = *core->runnext;
      core->runnext.reset();
      ++runnext_streak;
      return key;
    }
    if (!core->local_ready.empty()) {
      const auto key = core->local_ready.back();
      core->local_ready.pop_back();
      runnext_streak = 0;
      return key;
    }
    if (core->runnext) {
      const auto key = *core->runnext;
      core->runnext.reset();
      runnext_streak = 1;
      return key;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::optional<TaskKey> take_global() noexcept {
    std::lock_guard lock{queue_mutex_};
    if (ready_queue_.empty()) {
      return std::nullopt;
    }
    const auto key = ready_queue_.front();
    ready_queue_.pop_front();
    return key;
  }

  [[nodiscard]] std::optional<TaskKey> steal_work(
      std::size_t thief_index,
      std::uint64_t& random_state) noexcept {
    if (worker_count_ < 2) {
      return std::nullopt;
    }
    random_state ^= random_state << 13U;
    random_state ^= random_state >> 7U;
    random_state ^= random_state << 17U;
    const auto start = static_cast<std::size_t>(
        random_state % static_cast<std::uint64_t>(worker_count_));

    for (std::size_t attempt = 0; attempt < worker_count_; ++attempt) {
      const auto victim_index = (start + attempt) % worker_count_;
      if (victim_index == thief_index) {
        continue;
      }

      std::vector<TaskKey> stolen;
      const auto victim = executor_cores_[victim_index];
      {
        std::lock_guard lock{victim->mutex};
        const auto count = (victim->local_ready.size() + 1) / 2;
        stolen.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
          stolen.push_back(victim->local_ready.front());
          victim->local_ready.pop_front();
        }
      }
      if (stolen.empty()) {
        continue;
      }

      const auto selected = stolen.front();
      if (stolen.size() > 1) {
        const auto thief = executor_cores_[thief_index];
        std::lock_guard lock{thief->mutex};
        for (std::size_t index = 1; index < stolen.size(); ++index) {
          thief->local_ready.push_back(stolen[index]);
        }
      }
      return selected;
    }
    return std::nullopt;
  }

  void worker_loop(std::size_t core_index) noexcept {
    ScopedWorkerBinding binding{
        WorkerBinding{runtime_nonce_, core_index}};
    std::size_t polls_since_global = 0;
    std::size_t polls_since_event = 0;
    std::size_t runnext_streak = 0;
    auto last_global_check = std::chrono::steady_clock::now();
    auto last_event_check = last_global_check;
    std::uint64_t random_state =
        runtime_nonce_ ^
        (static_cast<std::uint64_t>(core_index) +
         std::uint64_t{0x9E3779B97F4A7C15});

    while (!workers_stop_.load(std::memory_order_acquire)) {
      std::optional<TaskKey> key;
      const auto loop_now = std::chrono::steady_clock::now();
      if (polls_since_global >= 61 ||
          loop_now - last_global_check >=
              std::chrono::milliseconds{10}) {
        key = take_global();
        polls_since_global = 0;
        last_global_check = loop_now;
      }
      if (!key) {
        key = take_local(core_index, runnext_streak);
      }
      if (!key) {
        key = take_global();
        polls_since_global = 0;
      }

      if (!key) {
        const auto search_limit =
            std::max<std::size_t>(1, worker_count_ / 2);
        const auto previous =
            searching_workers_.fetch_add(1, std::memory_order_acq_rel);
        if (previous < search_limit) {
          key = steal_work(core_index, random_state);
        }
        searching_workers_.fetch_sub(1, std::memory_order_acq_rel);
      }

      if (key) {
        if (const auto control = resolve_task(*key)) {
          control->run_scheduled(core_index);
        }
        ++polls_since_global;
        ++polls_since_event;
        const auto after_poll = std::chrono::steady_clock::now();
        if (polls_since_event >= 61 ||
            after_poll - last_event_check >=
                std::chrono::microseconds{100}) {
          process_due_timers();
          polls_since_event = 0;
          last_event_check = after_poll;
        }
        continue;
      }

      process_due_timers();
      if (const auto after_timer = take_local(core_index, runnext_streak)) {
        if (const auto control = resolve_task(*after_timer)) {
          control->run_scheduled(core_index);
        }
        continue;
      }
      if (const auto after_global = take_global()) {
        if (const auto control = resolve_task(*after_global)) {
          control->run_scheduled(core_index);
        }
        continue;
      }

      const auto observed =
          work_sequence_.load(std::memory_order_acquire);
      std::unique_lock queue_lock{queue_mutex_};
      const auto timer_wait = next_timer_wait_duration();
      const auto wait_for = timer_wait.value_or(
          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
              std::chrono::milliseconds{10}));
      queue_changed_.wait_for(
          queue_lock,
          wait_for,
          [this, observed] {
            return workers_stop_.load(std::memory_order_acquire) ||
                   work_sequence_.load(std::memory_order_acquire) !=
                       observed ||
                   !ready_queue_.empty();
          });
    }
  }

  [[nodiscard]] bool tasks_empty_locked() const noexcept {
    for (const auto& slot : task_slots_) {
      if (slot.control) {
        return false;
      }
    }
    return true;
  }

  void shutdown_multi_thread() noexcept {
    std::unique_lock run_lock{run_mutex_};
    std::vector<std::shared_ptr<TaskControlBase>> controls;
    {
      std::lock_guard tasks_lock{tasks_mutex_};
      if (shutdown_started_) {
        return;
      }
      shutdown_started_ = true;
      controls.reserve(task_slots_.size());
      for (const auto& slot : task_slots_) {
        if (slot.control) {
          controls.push_back(slot.control);
        }
      }
    }

    for (const auto& control : controls) {
      control->request_abort();
    }

    {
      std::unique_lock tasks_lock{tasks_mutex_};
      tasks_changed_.wait(
          tasks_lock,
          [this] { return tasks_empty_locked(); });
    }

    accepting_.store(false, std::memory_order_release);
    workers_stop_.store(true, std::memory_order_release);
    work_sequence_.fetch_add(1, std::memory_order_release);
    queue_changed_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();

    {
      std::lock_guard queue_lock{queue_mutex_};
      ready_queue_.clear();
    }
    for (const auto& core : executor_cores_) {
      std::lock_guard core_lock{core->mutex};
      core->local_ready.clear();
      core->runnext.reset();
    }
    {
      std::lock_guard time_lock{time_mutex_};
      timer_driver_.shutdown();
    }
    blocking_pool_->shutdown();
  }

  void ensure_time_enabled() const {
    if (!time_enabled_) {
      throw std::logic_error{
          "CIO runtime 未启用时间驱动；请调用 Builder::enable_time"};
    }
  }

  [[nodiscard]] std::chrono::steady_clock::time_point
  clock_now_locked() const noexcept {
    auto now = clock_base_;
    if (unfrozen_since_) {
      now += std::chrono::steady_clock::now() - *unfrozen_since_;
    }
    return now;
  }

  [[nodiscard]] std::uint64_t deadline_to_tick_locked(
      std::chrono::steady_clock::time_point deadline) const noexcept {
    if (deadline <= time_origin_) {
      return 0;
    }

    const auto elapsed = deadline - time_origin_;
    const auto whole_milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    auto tick = static_cast<std::uint64_t>(whole_milliseconds.count());
    if (whole_milliseconds < elapsed &&
        tick != std::numeric_limits<std::uint64_t>::max()) {
      ++tick;
    }
    return tick;
  }

  [[nodiscard]] std::uint64_t now_tick_locked() const noexcept {
    const auto now = clock_now_locked();
    if (now <= time_origin_) {
      return 0;
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - time_origin_);
    return static_cast<std::uint64_t>(elapsed.count());
  }

  void process_due_timers() {
    std::vector<TimerFire> fires;
    {
      std::lock_guard lock{time_mutex_};
      if (!time_enabled_) {
        return;
      }
      fires = timer_driver_.process(now_tick_locked());
    }

    for (const auto& fire : fires) {
      fire.state->fire(fire.epoch);
    }
  }

  [[nodiscard]] bool try_auto_advance_to_next_timer() {
    std::lock_guard lock{time_mutex_};
    if (!time_enabled_ || unfrozen_since_ ||
        auto_advance_inhibit_count_ != 0) {
      return false;
    }
    const auto deadline = timer_driver_.next_deadline();
    if (!deadline) {
      return false;
    }

    const auto now_tick = now_tick_locked();
    if (*deadline > now_tick) {
      const auto delta = *deadline - now_tick;
      clock_base_ += std::chrono::milliseconds{delta};
    }
    return true;
  }

  [[nodiscard]] std::optional<std::chrono::steady_clock::duration>
  next_timer_wait_duration() const {
    std::lock_guard lock{time_mutex_};
    if (!time_enabled_ || !unfrozen_since_) {
      return std::nullopt;
    }
    const auto deadline = timer_driver_.next_deadline();
    if (!deadline) {
      return std::nullopt;
    }
    const auto now_tick = now_tick_locked();
    if (*deadline <= now_tick) {
      return std::chrono::steady_clock::duration::zero();
    }
    return std::chrono::milliseconds{*deadline - now_tick};
  }

  template <typename T>
  std::pair<TaskKey, std::shared_ptr<TaskControl<T>>> create_task_control(
      task::Id id,
      Task<T> task,
      const std::shared_ptr<JoinState<T>>& join_state,
      const std::shared_ptr<AbortRegistration>& registration);

  task::Id next_id() noexcept {
    return task::IdFactory::from_runtime(
        next_task_id_.fetch_add(1, std::memory_order_relaxed));
  }

  mutable std::mutex queue_mutex_;
  std::condition_variable queue_changed_;
  std::deque<TaskKey> ready_queue_;
  std::atomic<bool> accepting_{true};

  mutable std::mutex tasks_mutex_;
  std::condition_variable tasks_changed_;
  std::vector<TaskSlot> task_slots_;
  std::vector<std::uint32_t> free_slots_;
  bool shutdown_started_{false};

  std::mutex run_mutex_;
  const std::uint64_t runtime_nonce_;
  const bool time_enabled_;
  const bool multi_thread_;
  const std::size_t worker_count_;
  std::shared_ptr<BlockingPool> blocking_pool_;
  std::vector<std::shared_ptr<ExecutorCore>> executor_cores_;
  std::vector<std::thread> workers_;
  std::atomic<bool> workers_stop_{false};
  std::atomic<std::uint64_t> work_sequence_{0};
  std::atomic<std::size_t> searching_workers_{0};
  mutable std::mutex time_mutex_;
  TimerDriver timer_driver_;
  std::chrono::steady_clock::time_point clock_base_;
  std::optional<std::chrono::steady_clock::time_point> unfrozen_since_;
  std::size_t auto_advance_inhibit_count_{0};
  const std::chrono::steady_clock::time_point time_origin_;
  std::atomic<std::uint64_t> next_task_id_{1};
};

inline void TaskControlBase::initialize() {
  const auto weak_runtime = runtime_;
  const auto task_key = key_;
  registration_->abort = [weak_runtime, task_key] {
    if (const auto runtime = weak_runtime.lock()) {
      runtime->request_abort(task_key);
    }
  };
  registration_->abort_now = [weak_runtime, task_key] {
    if (const auto runtime = weak_runtime.lock()) {
      runtime->cancel_task_now(task_key);
    }
  };

  execution_context_ = std::make_shared<ExecutionContext>(
      runtime_,
      key_,
      [weak_runtime, task_key](CoroutineRef coroutine) {
        if (const auto runtime = weak_runtime.lock()) {
          runtime->park(task_key, coroutine);
        }
      },
      [weak_runtime, task_key] {
        if (const auto runtime = weak_runtime.lock()) {
          runtime->schedule(task_key);
        }
      });
  bind_root_context(execution_context_);
}

inline void TaskControlBase::request_abort() noexcept {
  if (abort_requested_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  if (const auto runtime = runtime_.lock()) {
    runtime->schedule(key_);
  }
}

inline void TaskControlBase::cancel_for_shutdown() noexcept {
  abort_requested_.store(true, std::memory_order_release);
  if (active_.load(std::memory_order_acquire)) {
    cancel_now();
  }
}

inline bool TaskControlBase::begin_poll() noexcept {
  auto state = schedule_state_.load(std::memory_order_acquire);
  while (true) {
    if (!active_.load(std::memory_order_acquire) ||
        (state & scheduled_bit) == 0 ||
        (state & running_bit) != 0) {
      return false;
    }
    const auto desired = static_cast<std::uint8_t>(
        (state & ~scheduled_bit) | running_bit);
    if (schedule_state_.compare_exchange_weak(
            state,
            desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
}

inline void TaskControlBase::finish_pending_poll(
    std::optional<std::size_t> executor_core) noexcept {
  auto state = schedule_state_.load(std::memory_order_acquire);
  bool enqueue = false;
  while (true) {
    enqueue = (state & notified_bit) != 0;
    auto desired = static_cast<std::uint8_t>(
        state & ~(running_bit | notified_bit));
    if (enqueue) {
      desired = static_cast<std::uint8_t>(desired | scheduled_bit);
    }
    if (schedule_state_.compare_exchange_weak(
            state,
            desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
  if (enqueue) {
    if (const auto runtime = runtime_.lock()) {
      runtime->enqueue_marked(key_, executor_core);
    }
  }
}

inline void TaskControlBase::run_scheduled(
    std::optional<std::size_t> executor_core) noexcept {
  if (!begin_poll()) {
    return;
  }
  if (abort_requested_.load(std::memory_order_acquire)) {
    cancel_now();
    return;
  }

  {
    ScopedExecutionContext scope{execution_context_};
    execution_context_->reset_cooperative_budget();
    resume_current();
  }

  if (root_done()) {
    finish_normally();
    return;
  }
  if (abort_requested_.load(std::memory_order_acquire)) {
    cancel_now();
    return;
  }
  finish_pending_poll(executor_core);
}

inline void TaskControlBase::remove_from_runtime() {
  if (const auto runtime = runtime_.lock()) {
    runtime->remove_task(key_);
  }
}

template <typename T>
class TaskControl final : public TaskControlBase {
 public:
  using JoinResult = Result<T, task::JoinError>;

  TaskControl(
      std::weak_ptr<RuntimeState> runtime,
      TaskKey key,
      task::Id id,
      Task<T> task,
      std::shared_ptr<JoinState<T>> join_state,
      std::shared_ptr<AbortRegistration> registration) noexcept
      : TaskControlBase{std::move(runtime), key, id, registration},
        task_{std::move(task)},
        join_state_{std::move(join_state)} {}

 private:
  void bind_root_context(
      const std::shared_ptr<ExecutionContext>& context) noexcept override {
    task_.bind_root_context(context);
  }

  void set_resumable(CoroutineRef coroutine) noexcept override {
    task_.set_resumable(coroutine);
  }

  void resume_current() override {
    task_.resume_current();
  }

  [[nodiscard]] bool root_done() const noexcept override {
    return task_.done();
  }

  void finish_normally() noexcept override {
    JoinResult result = make_join_result();
    mark_inactive();
    task_.reset();
    join_state_->complete(std::move(result));
    remove_from_runtime();
  }

  void cancel_now() noexcept override {
    mark_inactive();
    task_.reset();
    join_state_->complete(
        JoinResult::failure(task::JoinError::cancelled(id())));
    remove_from_runtime();
  }

  JoinResult make_join_result() noexcept {
    try {
      if constexpr (std::is_void_v<T>) {
        task_.take_result();
        return JoinResult::success();
      } else {
        return JoinResult::success(task_.take_result());
      }
    } catch (...) {
      return JoinResult::failure(
          task::JoinError::panic(id(), std::current_exception()));
    }
  }

  Task<T> task_;
  std::shared_ptr<JoinState<T>> join_state_;
};

template <typename T>
std::pair<TaskKey, std::shared_ptr<TaskControl<T>>>
RuntimeState::create_task_control(
    task::Id id,
    Task<T> task,
    const std::shared_ptr<JoinState<T>>& join_state,
    const std::shared_ptr<AbortRegistration>& registration) {
  std::lock_guard lock{tasks_mutex_};
  if (shutdown_started_) {
    throw std::runtime_error{"不能在已关闭的 CIO runtime 上 spawn"};
  }

  std::uint32_t slot_index = 0;
  if (free_slots_.empty()) {
    if (task_slots_.size() >=
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
      throw std::overflow_error{"CIO runtime task slot 已耗尽"};
    }
    slot_index = static_cast<std::uint32_t>(task_slots_.size());
    task_slots_.emplace_back();
  } else {
    slot_index = free_slots_.back();
    free_slots_.pop_back();
  }

  auto& slot = task_slots_[slot_index];
  const auto key = TaskKeyFactory::make(
      slot_index,
      slot.generation,
      runtime_nonce_);
  auto control = std::make_shared<TaskControl<T>>(
      weak_from_this(),
      key,
      id,
      std::move(task),
      join_state,
      registration);
  slot.control = control;
  return {key, std::move(control)};
}

template <typename T>
task::JoinHandle<T> RuntimeState::spawn(Task<T> task, bool portable) {
  if (!task.valid()) {
    throw std::invalid_argument{"不能 spawn 空 Task"};
  }
  if (multi_thread_ && !portable) {
    throw std::invalid_argument{
        "multi-thread runtime 只接受 PortableTask；请使用 owned 或"
        " assume_portable 完成显式安全审计"};
  }

  const auto id = next_id();
  auto join_state = std::make_shared<JoinState<T>>();
  auto registration = std::make_shared<AbortRegistration>();
  registration->id = id;
  registration->is_finished = [weak_state = std::weak_ptr{join_state}] {
    const auto state = weak_state.lock();
    return !state || state->is_ready();
  };

  auto [key, control] = create_task_control(
      id,
      std::move(task),
      join_state,
      registration);

  try {
    control->initialize();
  } catch (...) {
    control->cancel_for_shutdown();
    throw;
  }
  schedule(key);

  return JoinHandleAccess::make<T>(
      std::move(join_state),
      std::move(registration));
}

template <typename Factory, typename... Args>
auto RuntimeState::spawn_blocking(
    Factory factory,
    std::tuple<Args...> arguments)
    -> task::JoinHandle<
        std::invoke_result_t<Factory, Args...>> {
  using Output = std::invoke_result_t<Factory, Args...>;
  static_assert(
      !std::is_reference_v<Output>,
      "spawn_blocking 不能返回引用");

  {
    std::lock_guard lock{tasks_mutex_};
    if (shutdown_started_) {
      throw std::runtime_error{
          "不能在已关闭的 CIO runtime 上 spawn_blocking"};
    }
  }

  const auto id = next_id();
  auto join_state = std::make_shared<JoinState<Output>>();
  auto registration = std::make_shared<AbortRegistration>();
  registration->id = id;
  registration->is_finished =
      [weak_state = std::weak_ptr{join_state}] {
        const auto state = weak_state.lock();
        return !state || state->is_ready();
      };

  const auto weak_runtime = weak_from_this();
  auto job = std::make_shared<
      BlockingJob<Output, Factory, Args...>>(
      id,
      std::move(factory),
      std::move(arguments),
      join_state,
      weak_runtime,
      [weak_runtime] {
        if (const auto runtime = weak_runtime.lock()) {
          runtime->allow_auto_advance();
        }
      });
  const auto weak_job =
      std::weak_ptr<BlockingJobBase>{job};
  registration->abort = [weak_job] {
    if (const auto locked = weak_job.lock()) {
      locked->cancel_if_queued();
    }
  };
  registration->abort_now = registration->abort;

  inhibit_auto_advance();
  try {
    if (!blocking_pool_->submit(job)) {
      throw std::runtime_error{
          "CIO blocking pool 已关闭"};
    }
  } catch (...) {
    job->cancel_if_queued();
    throw;
  }

  return JoinHandleAccess::make<Output>(
      std::move(join_state),
      std::move(registration));
}

}  // namespace cio::detail
