#pragma once

#include <atomic>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "cio/send.hpp"

namespace cio::io {

namespace operation_detail {

template <typename T>
concept ReferenceWrapperLike = requires(T value) {
  typename T::type;
  value.get();
  requires std::is_lvalue_reference_v<decltype(value.get())>;
  requires std::is_same_v<
      std::remove_reference_t<decltype(value.get())>,
      typename T::type>;
};

template <typename T>
struct BorrowedView : std::false_type {};

template <typename Element, std::size_t Extent>
struct BorrowedView<std::span<Element, Extent>>
    : std::true_type {};

template <typename Character, typename Traits>
struct BorrowedView<
    std::basic_string_view<Character, Traits>>
    : std::true_type {};

}  // namespace operation_detail

/**
 * OperationRegistry 可以跨 driver/worker 转移的 owning 值边界。
 *
 * 引用、裸指针和未通过 CIO Send 显式审计的未知类型一律拒绝；Send 默认拒绝
 * 未知类型，且已知 span、string_view、reference_wrapper borrowed view 即使被
 * 错误标为 Send 仍会被拒绝。值还必须 noexcept 移动与析构，保证终态发布和
 * 锁外回收不会因用户类型留下半提交状态。
 */
template <typename T>
concept OperationOwnedValue =
    std::is_object_v<T> &&
    (!std::is_reference_v<T>) &&
    (!std::is_pointer_v<std::remove_cv_t<T>>) &&
    (!operation_detail::ReferenceWrapperLike<
        std::remove_cv_t<T>>) &&
    (!operation_detail::BorrowedView<
        std::remove_cv_t<T>>::value) &&
    cio::Send<T> &&
    std::is_nothrow_move_constructible_v<T> &&
    std::is_nothrow_destructible_v<T>;

class OperationKey;
class NoopOperationWake;

/**
 * 可进入 operation wake 热路径的已审计 owning callable。
 *
 * 类型必须同时满足 OperationOwnedValue、显式声明 cio_operation_wake=true，
 * cio_operation_enqueue_only=true，并能以 `void(OperationKey) noexcept`
 * 调用。它只能借助 weak runtime 与 TaskKey/LaneKey 把工作入队，绝不能直接
 * resume/poll continuation；这样 dispatch 析构即使发生在任意 driver 线程也
 * 不会把 task 恢复到错误线程。
 *
 * C++20 无法检查 callable 内部是否真的只入队，该边界仍依赖显式审计 opt-in、
 * code review 与线程探针测试。wake 失败不能通过异常逃逸到 scheduler；需要
 * 失败信息时应写入自身拥有的共享状态或 runtime 诊断通道。
 */
template <typename T>
concept OperationWake =
    OperationOwnedValue<T> &&
    requires(T& wake, OperationKey key) {
      typename std::bool_constant<
          static_cast<bool>(
              std::remove_cv_t<T>::cio_operation_wake)>;
      requires static_cast<bool>(
          std::remove_cv_t<T>::cio_operation_wake);
      typename std::bool_constant<
          static_cast<bool>(
              std::remove_cv_t<T>::
                  cio_operation_enqueue_only)>;
      requires static_cast<bool>(
          std::remove_cv_t<T>::
              cio_operation_enqueue_only);
      wake(key);
      requires std::is_same_v<decltype(wake(key)), void>;
      requires noexcept(wake(key));
    };

template <
    OperationOwnedValue Lease,
    OperationOwnedValue Result,
    OperationWake Wake = NoopOperationWake>
class OperationRegistry;

/**
 * 平台中立 I/O operation 的代际键。
 *
 * slot 定位 registry 槽位，generation 拒绝槽位复用后的迟到完成，
 * registry_nonce 拒绝跨 registry 误投。key 不拥有 operation，也不保存地址。
 */
class OperationKey final {
 public:
  constexpr OperationKey() noexcept = default;

  [[nodiscard]] constexpr bool valid() const noexcept {
    return generation_ != 0 && registry_nonce_ != 0;
  }

  [[nodiscard]] constexpr std::uint32_t slot() const noexcept {
    return slot_;
  }

  [[nodiscard]] constexpr std::uint64_t generation() const noexcept {
    return generation_;
  }

  [[nodiscard]] constexpr std::uint64_t registry_nonce() const noexcept {
    return registry_nonce_;
  }

  auto operator<=>(const OperationKey&) const noexcept = default;

 private:
  constexpr OperationKey(
      std::uint32_t slot,
      std::uint64_t generation,
      std::uint64_t registry_nonce) noexcept
      : slot_{slot},
        generation_{generation},
        registry_nonce_{registry_nonce} {}

  std::uint32_t slot_{0};
  std::uint64_t generation_{0};
  std::uint64_t registry_nonce_{0};

  template <
      OperationOwnedValue,
      OperationOwnedValue,
      OperationWake>
  friend class OperationRegistry;
};

/** 不需要唤醒上层时使用的默认 owning wake 值。 */
class NoopOperationWake final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_operation_wake = true;
  static constexpr bool cio_operation_enqueue_only = true;

  constexpr void operator()(OperationKey) const noexcept {}
};

/**
 * operation 的平台中立生命周期状态。
 *
 * 正常路径为 created -> submitting -> submitted -> completing -> delivered；
 * 即时完成允许从 submitting 直接进入 completing。取消竞争同样可以从
 * submitting/submitted 进入 cancelling。completed record 在 consume 后回收；
 * cancelled record 必须同时完成上层 consume 与 native settled，才释放 buffer
 * lease 并推进 generation。
 */
enum class OperationState : std::uint8_t {
  created,
  submitting,
  submitted,
  completing,
  cancelling,
  delivered,
};

enum class OperationTerminal : std::uint8_t {
  completed,
  cancelled,
};

/**
 * consume 后交给上层的唯一终态值。
 *
 * 本值不携带 buffer lease。completed consume 会在锁外释放 owning record；
 * cancelled consume 可以先返回，本地 tombstone 仍持有 lease，直至 native
 * terminal settled。
 */
template <typename Result>
class OperationDelivery final {
 public:
  OperationDelivery(
      OperationTerminal terminal,
      Result result) noexcept(
      std::is_nothrow_move_constructible_v<Result>)
      : terminal_{terminal},
        result_{std::move(result)} {}

  OperationDelivery(const OperationDelivery&) = delete;
  OperationDelivery& operator=(const OperationDelivery&) = delete;
  OperationDelivery(OperationDelivery&&) noexcept = default;
  OperationDelivery& operator=(OperationDelivery&&) noexcept = default;
  ~OperationDelivery() = default;

  [[nodiscard]] OperationTerminal terminal() const noexcept {
    return terminal_;
  }

  [[nodiscard]] const Result& result() const& noexcept {
    return result_;
  }

  [[nodiscard]] Result&& take_result() && noexcept {
    return std::move(result_);
  }

 private:
  OperationTerminal terminal_;
  Result result_;
};

namespace operation_detail {

inline std::uint64_t next_registry_nonce() noexcept {
  static std::atomic<std::uint64_t> nonce{0};
  auto current = nonce.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    if (nonce.compare_exchange_weak(
            current,
            current + 1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return current + 1;
    }
  }
}

}  // namespace operation_detail

/**
 * 平台中立 I/O operation slot-map 原型。
 *
 * registry 的槽位是 OperationRecord 的唯一 owner。Lease 必须是 noexcept
 * move-only 或安全共享的 owning lease；Result 也必须可 noexcept 移动。终态先由
 * begin_complete/begin_cancel 唯一认领，再由 DeliveryClaim::deliver 发布。
 *
 * wake 只作为 OperationDispatch 值移出 registry，因此 registry mutex 从不
 * 覆盖 wake/取消回调。调用方应显式 run；若 dispatch 被意外丢弃，其唯一 owner
 * 会在析构中执行 wake，只有 runtime shutdown 可显式 discard_for_shutdown。
 * complete/cancel 的任意线程调用都只竞争 record 状态，不直接运行 continuation。
 * runtime 析构本 registry 前必须先停止 driver，并 drain 所有 native completion。
 *
 * 组合阻塞：当前 Delivery 只转移 Result，不转移 Lease。completed consume 会在
 * 返回前析构 record/Lease，因此本原型还不能直接承载要求完成后由上层调用
 * commit 的 CopyWritableTailLease read completion；接入前必须新增 completed
 * lease 转移，或定义由 driver 在发布 Result 前完成 commit 的独立已审计契约。
 */
template <
    OperationOwnedValue Lease,
    OperationOwnedValue Result,
    OperationWake Wake>
class OperationRegistry final {
 public:
  using Delivery = OperationDelivery<Result>;

 private:
  struct Record final {
    Record(Lease owned_lease, Wake owned_wake)
        : lease{std::move(owned_lease)},
          wake{std::make_unique<Wake>(
              std::move(owned_wake))} {}

    Record(const Record&) = delete;
    Record& operator=(const Record&) = delete;
    Record(Record&&) noexcept = default;
    Record& operator=(Record&&) noexcept = default;
    ~Record() = default;

    OperationState state{OperationState::created};
    Lease lease;
    std::unique_ptr<Wake> wake;
    std::optional<Delivery> delivery;
    std::optional<OperationTerminal> terminal;
    bool consumed{false};
    bool native_settled{false};
    bool submission_pending{false};
  };

  struct Slot final {
    Slot() = default;
    Slot(const Slot&) = delete;
    Slot& operator=(const Slot&) = delete;
    Slot(Slot&&) noexcept = default;
    Slot& operator=(Slot&&) noexcept = default;
    ~Slot() = default;

    std::uint64_t generation{1};
    std::unique_ptr<Record> record;
  };

  struct RegistryState final {
    mutable std::mutex mutex;
    std::vector<Slot> slots;
    std::vector<std::uint32_t> free_slots;
    std::uint64_t nonce{operation_detail::next_registry_nonce()};
    std::size_t active_records{0};
    std::atomic<std::size_t> outstanding_submissions{0};
    std::atomic<std::size_t> outstanding_dispatches{0};
  };

 public:
  /**
   * native submit 调用的唯一事务 guard。
   *
   * guard 在进入 OS 前取得，使 record 先进入 submitting。同步接受后调用
   * accept；同步拒绝后调用 reject。即时 completion/cancel 可以先认领终态，
   * guard 再负责收口 submission_pending。active guard 不允许静默析构。
   */
  class SubmissionGuard final {
   public:
    SubmissionGuard(const SubmissionGuard&) = delete;
    SubmissionGuard& operator=(const SubmissionGuard&) = delete;

    SubmissionGuard(SubmissionGuard&& other) noexcept
        : state_{std::move(other.state_)},
          key_{other.key_},
          active_{std::exchange(other.active_, false)} {}

    SubmissionGuard& operator=(SubmissionGuard&&) = delete;

    ~SubmissionGuard() {
      if (active_) {
        std::terminate();
      }
    }

    [[nodiscard]] OperationKey key() const noexcept {
      return key_;
    }

    [[nodiscard]] bool accept() noexcept {
      return finish(true);
    }

    [[nodiscard]] bool reject() noexcept {
      return finish(false);
    }

   private:
    SubmissionGuard(
        std::weak_ptr<RegistryState> state,
        OperationKey key) noexcept
        : state_{std::move(state)}, key_{key} {}

    [[nodiscard]] bool finish(bool accepted) noexcept {
      if (!active_) {
        return false;
      }
      active_ = false;
      auto state = state_.lock();
      if (!state) {
        return false;
      }

      std::unique_ptr<Record> retired;
      bool matched = false;
      {
        const std::lock_guard lock{state->mutex};
        if (matches_locked(*state, key_)) {
          auto& record = *state->slots[key_.slot_].record;
          if (record.submission_pending) {
            matched = true;
            record.submission_pending = false;
            if (accepted) {
              if (record.state == OperationState::submitting) {
                record.state = OperationState::submitted;
              }
              if (record.consumed && record.native_settled) {
                retire_locked(*state, key_.slot_, retired);
              }
            } else if (
                record.terminal &&
                *record.terminal ==
                    OperationTerminal::completed) {
              std::terminate();
            } else if (
                record.terminal &&
                *record.terminal ==
                    OperationTerminal::cancelled) {
              record.native_settled = true;
              if (record.consumed) {
                retire_locked(*state, key_.slot_, retired);
              }
            } else if (
                record.state == OperationState::submitting) {
              retire_locked(*state, key_.slot_, retired);
            } else {
              std::terminate();
            }
          }
        }
      }
      retired.reset();
      // owning lease/pin 析构完成后才允许 shutdown drain 观察到零。
      acknowledge_submission(*state);
      return matched;
    }

    std::weak_ptr<RegistryState> state_;
    OperationKey key_;
    bool active_{true};

    friend class OperationRegistry;
  };

  /**
   * deliver 后得到的锁外 wake 调度值。
   *
   * run 最多调用一次保存的、显式审计且 noexcept 的 owning wake。回调可安全
   * 重入 registry，异常不能逃逸到 scheduler。
   */
  class OperationDispatch final {
   public:
    OperationDispatch(const OperationDispatch&) = delete;
    OperationDispatch& operator=(const OperationDispatch&) = delete;

    OperationDispatch(OperationDispatch&& other) noexcept
        : key_{other.key_},
          wake_{std::move(other.wake_)},
          registry_state_{
              std::move(other.registry_state_)},
          state_{std::exchange(
              other.state_,
              DispatchState::moved)} {}

    OperationDispatch& operator=(OperationDispatch&&) = delete;
    ~OperationDispatch() {
      if (state_ == DispatchState::pending) {
        // 防止调用方丢弃唯一 wake；OperationWake 契约保证析构热路径不抛异常。
        (void)run();
      }
    }

    [[nodiscard]] OperationKey key() const noexcept {
      return key_;
    }

    /**
     * 在 registry 锁外运行一次关联 wake。
     *
     * 重复调用返回 false。回调可重入 registry；OperationWake 契约保证本函数
     * 不向 scheduler 抛异常，终态在调用前已经保持 delivered。
     */
    [[nodiscard]] bool run() noexcept {
      return finalize(true);
    }

    /**
     * runtime shutdown 时显式放弃尚未运行的 wake。
     *
     * 只有调用方已经保证关联 continuation 不会再被 poll 时才能调用。首次从
     * pending 转换返回 true；run、重复 discard 或 moved-from 对象返回 false。
     * wake owning state 在本函数内释放，析构不再自动执行。
     */
    [[nodiscard]] bool discard_for_shutdown() noexcept {
      return finalize(false);
    }

   private:
    enum class DispatchState : std::uint8_t {
      pending,
      ran,
      discarded,
      moved,
    };

    OperationDispatch(
        OperationKey key,
        std::unique_ptr<Wake> wake,
        std::shared_ptr<RegistryState> registry_state) noexcept
        : key_{key},
          wake_{std::move(wake)},
          registry_state_{std::move(registry_state)} {}

    [[nodiscard]] bool finalize(bool enqueue) noexcept {
      if (state_ != DispatchState::pending) {
        return false;
      }
      state_ = enqueue
          ? DispatchState::ran
          : DispatchState::discarded;
      if (!wake_ || !registry_state_) {
        std::terminate();
      }

      // unique owner 的 move 会把 source 清空。只有 wake 调用与 owning Wake
      // 析构都完成后，才允许 outstanding ack 变为零。
      auto final_wake = std::move(wake_);
      if (enqueue) {
        (*final_wake)(key_);
      }
      final_wake.reset();
      acknowledge_dispatch();
      return true;
    }

    void acknowledge_dispatch() noexcept {
      auto registry_state =
          std::move(registry_state_);
      if (!registry_state) {
        std::terminate();
      }
      auto outstanding =
          registry_state->outstanding_dispatches.load(
              std::memory_order_acquire);
      while (true) {
        if (outstanding == 0) {
          std::terminate();
        }
        if (registry_state->outstanding_dispatches
                .compare_exchange_weak(
                    outstanding,
                    outstanding - 1,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
          return;
        }
      }
    }

    OperationKey key_;
    std::unique_ptr<Wake> wake_;
    std::shared_ptr<RegistryState> registry_state_;
    DispatchState state_{DispatchState::pending};

    friend class DeliveryClaim;
  };

  /**
   * complete/cancel 竞争胜者持有的唯一交付权。
   *
   * claim 不拥有 record；registry drain/析构会使其失效。调用 deliver 只在锁内
   * 发布 delivered 并移出 wake，不运行用户代码。live claim 不允许静默丢弃：
   * 析构等价于 abandon，发布已认领终态并在锁外执行 audited wake。
   */
  class DeliveryClaim final {
   public:
    DeliveryClaim(const DeliveryClaim&) = delete;
    DeliveryClaim& operator=(const DeliveryClaim&) = delete;

    DeliveryClaim(DeliveryClaim&& other) noexcept
        : state_{std::move(other.state_)},
          key_{other.key_},
          expected_{other.expected_},
          active_{std::exchange(other.active_, false)} {}

    DeliveryClaim& operator=(DeliveryClaim&&) = delete;

    ~DeliveryClaim() {
      if (active_) {
        (void)abandon();
      }
    }

    /**
     * 在当前 registry 中验证本 claim 仍持有 expected 状态的唯一交付权。
     *
     * 本检查锁内核对 registry nonce、slot generation、record 和 expected 状态；
     * drain 旋转 nonce 后立即返回 false，不把仅有 weak state/key 的浅存活误报为
     * 有效。
     */
    [[nodiscard]] bool valid() const {
      if (!active_ || !key_.valid()) {
        return false;
      }
      auto state = state_.lock();
      if (!state) {
        return false;
      }
      const std::lock_guard lock{state->mutex};
      if (!matches_locked(*state, key_)) {
        return false;
      }
      const auto& record =
          *state->slots[key_.slot_].record;
      return record.state == expected_ &&
          record.delivery.has_value() &&
          record.terminal.has_value() &&
          static_cast<bool>(record.wake);
    }

    [[nodiscard]] OperationKey key() const noexcept {
      return key_;
    }

    /**
     * 把 completing/cancelling 原子发布为 delivered。
     *
     * 成功返回只含锁外 wake 的 dispatch；registry 被 drain/析构、key 过期或本
     * claim 已使用时返回空。该调用不运行回调、不阻塞等待其他 operation。
     */
    [[nodiscard]] std::optional<OperationDispatch> deliver() {
      if (!active_) {
        return std::nullopt;
      }
      auto state = state_.lock();
      if (!state) {
        active_ = false;
        return std::nullopt;
      }

      std::unique_ptr<Wake> wake;
      {
        const std::lock_guard lock{state->mutex};
        if (!matches_locked(*state, key_)) {
          active_ = false;
          return std::nullopt;
        }
        auto& record =
            *state->slots[key_.slot_].record;
        if (record.state != expected_ || !record.delivery ||
            !record.terminal || !record.wake) {
          // 同一 live claim 的 expected record 不可能被其他路径改写；若发生，
          // 继续运行会静默遗失唯一终态或 wake，只能 fail-fast。
          std::terminate();
        }
        acquire_dispatch_locked(*state);
        record.state = OperationState::delivered;
        wake = std::move(record.wake);
        active_ = false;
      }
      return OperationDispatch{
          key_,
          std::move(wake),
          std::move(state)};
    }

    /**
     * 放弃由调用方选择 executor 的手动 deliver，但不放弃已认领的终态。
     *
     * live claim 会发布 delivered 并立即在 registry 锁外运行 audited wake；
     * drain/析构已使 key 失效时返回 false。析构函数自动执行同一策略，防止
     * completing/cancelling record 因 claim 被丢弃而永久卡住。
     */
    [[nodiscard]] bool abandon() {
      auto dispatch = deliver();
      return dispatch.has_value() && dispatch->run();
    }

   private:
    DeliveryClaim(
        std::weak_ptr<RegistryState> state,
        OperationKey key,
        OperationState expected) noexcept
        : state_{std::move(state)},
          key_{key},
          expected_{expected} {}

    std::weak_ptr<RegistryState> state_;
    OperationKey key_;
    OperationState expected_{OperationState::created};
    bool active_{true};

    friend class OperationRegistry;
  };

  OperationRegistry()
      : state_{std::make_shared<RegistryState>()} {}

  OperationRegistry(const OperationRegistry&) = delete;
  OperationRegistry& operator=(const OperationRegistry&) = delete;

  OperationRegistry(OperationRegistry&&) noexcept = default;

  OperationRegistry& operator=(OperationRegistry&& other) noexcept {
    if (this != &other) {
      drain();
      state_ = std::move(other.state_);
    }
    return *this;
  }

  ~OperationRegistry() {
    drain();
  }

  /**
   * 注册 created record，并同步转移 buffer lease 所有权。
   *
   * 新增 slot 前先为 free-list 建立 `capacity >= slots.size() + 1` 不变量。
   * reserve、slot 扩容或 record 分配失败时，record 尚未发布，registry 的可观察
   * 状态不变；发布后该 slot 的 retire 路径不再分配且不抛异常。
   *
   * 未 submit 的 record 仍由 registry 拥有；registry 析构或 drain 会释放它。
   */
  [[nodiscard]] OperationKey create(
      Lease lease,
      Wake wake = {}) {
    ensure_valid();
    auto record = std::make_unique<Record>(
        std::move(lease),
        std::move(wake));
    const std::lock_guard lock{state_->mutex};

    std::uint32_t slot_index = 0;
    if (!state_->free_slots.empty()) {
      slot_index = state_->free_slots.back();
      state_->free_slots.pop_back();
    } else {
      if (state_->slots.size() >
              static_cast<std::size_t>(
                  std::numeric_limits<std::uint32_t>::max()) ||
          state_->slots.size() == state_->slots.max_size() ||
          state_->slots.size() >=
              state_->free_slots.max_size()) {
        throw std::length_error{"OperationRegistry slot 已耗尽"};
      }
      const auto required_capacity =
          state_->slots.size() + 1;
      if (state_->free_slots.capacity() <
          required_capacity) {
        const auto current_capacity =
            state_->free_slots.capacity();
        const auto maximum_capacity =
            state_->free_slots.max_size();
        auto target_capacity =
            current_capacity == 0
            ? (maximum_capacity < 8 ? maximum_capacity : 8)
            : (current_capacity > maximum_capacity / 2
                   ? maximum_capacity
                   : current_capacity * 2);
        if (target_capacity < required_capacity) {
          target_capacity = required_capacity;
        }
        // reserve 是 retire 前唯一允许的 free-list 分配点；此时 record 尚未发布。
        state_->free_slots.reserve(target_capacity);
      }
      slot_index =
          static_cast<std::uint32_t>(state_->slots.size());
      state_->slots.emplace_back();
    }

    if (static_cast<std::size_t>(slot_index) >=
        state_->slots.size() ||
        state_->slots[slot_index].record) {
      std::terminate();
    }
    auto& slot = state_->slots[slot_index];
    slot.record = std::move(record);
    ++state_->active_records;
    return OperationKey{
        slot_index,
        slot.generation,
        state_->nonce};
  }

  /**
   * 在进入 OS submit 调用前启动唯一提交事务。
   *
   * 成功把 created 推进为 submitting，使同步 completion/cancel 可以安全竞争，
   * 并返回必须显式 accept/reject 的 guard。
   */
  [[nodiscard]] std::optional<SubmissionGuard> begin_submit(
      OperationKey key) {
    ensure_valid();
    const std::lock_guard lock{state_->mutex};
    if (!matches_locked(*state_, key)) {
      return std::nullopt;
    }
    auto& record = *state_->slots[key.slot_].record;
    if (record.state != OperationState::created) {
      return std::nullopt;
    }
    acquire_submission_locked(*state_);
    record.state = OperationState::submitting;
    record.submission_pending = true;
    return SubmissionGuard{state_, key};
  }

  /**
   * 兼容无需真实 OS 调用的同步提交路径。
   */
  [[nodiscard]] bool submit(OperationKey key) {
    auto guard = begin_submit(key);
    return guard.has_value() && guard->accept();
  }

  /**
   * 回滚尚未进入提交事务的 created record。
   *
   * lease 在 registry mutex 外释放，不移出或运行 wake。用于 create 后的平台
   * 准备失败；一旦 begin_submit 成功必须改由 SubmissionGuard::reject 收口。
   */
  [[nodiscard]] bool rollback_created(OperationKey key) noexcept {
    if (!state_) {
      return false;
    }
    std::unique_ptr<Record> retired;
    {
      const std::lock_guard lock{state_->mutex};
      if (!matches_locked(*state_, key)) {
        return false;
      }
      const auto& record = *state_->slots[key.slot_].record;
      if (record.state != OperationState::created ||
          record.submission_pending || record.terminal) {
        return false;
      }
      retire_locked(*state_, key.slot_, retired);
    }
    retired.reset();
    return true;
  }

  /**
   * 竞争完成终态的唯一交付权。
   *
   * 仅 submitted 可成功；成功后 record 进入 completing 并继续持有 lease。
   * 任意线程可调用，不运行 wake。
   */
  [[nodiscard]] std::optional<DeliveryClaim> begin_complete(
      OperationKey key,
      Result result) {
    auto claim = begin_terminal(
        key,
        OperationState::completing,
        OperationTerminal::completed,
        std::move(result));
    if (!claim) {
      // completion backend 的迟到完成仍是 native-terminal handshake。若 cancel
      // 已胜出，本次结果不能覆盖上层终态，但证明内核不会再访问 buffer。
      (void)settle_native(key);
    }
    return claim;
  }

  /**
   * 竞争取消终态的唯一交付权。
   *
   * 仅 submitted 可成功；胜者进入 cancelling，失败方不能覆盖已保存的终态。
   * 该调用不运行 wake，late OS completion 将被当前状态拒绝。
   */
  [[nodiscard]] std::optional<DeliveryClaim> begin_cancel(
      OperationKey key,
      Result result) {
    return begin_terminal(
        key,
        OperationState::cancelling,
        OperationTerminal::cancelled,
        std::move(result));
  }

  /**
   * 原子认领并发布 completed 终态。
   *
   * 返回的 dispatch 必须由调用方在合适 executor 上运行；失败表示 key 过期或
   * 另一终态已胜出。buffer lease 仍由 delivered tombstone 持有。
   */
  [[nodiscard]] std::optional<OperationDispatch> complete(
      OperationKey key,
      Result result) {
    auto claim = begin_complete(key, std::move(result));
    if (!claim) {
      return std::nullopt;
    }
    return claim->deliver();
  }

  /**
   * 原子认领并发布 cancelled 终态。
   *
   * 取消只竞争 operation record，不强制中断 OS 调用；平台撤销结果仍须通过同一
   * key 回报。返回的 dispatch 在 registry 锁外运行。
   */
  [[nodiscard]] std::optional<OperationDispatch> cancel(
      OperationKey key,
      Result result) {
    auto claim = begin_cancel(key, std::move(result));
    if (!claim) {
      return std::nullopt;
    }
    return claim->deliver();
  }

  /**
   * 明确确认平台后端已不再访问 operation 的 native storage/buffer。
   *
   * IOCP late completion 会由 begin_complete/complete 失败路径自动执行该握手；
   * epoll/kqueue readiness backend 在同步撤销注册并确认没有在途系统调用后调用
   * 本 API。仅 cancelling/delivered cancelled record 可首次成功。
   *
   * 用户已 consume 时，本调用在锁外释放 tombstone lease 并推进 generation；
   * 用户尚未 consume 时只记录 native settled，继续保留可交付结果。
   */
  [[nodiscard]] bool settle_native(OperationKey key) {
    ensure_valid();
    std::unique_ptr<Record> retired;
    bool settled = false;
    {
      const std::lock_guard lock{state_->mutex};
      if (!matches_locked(*state_, key)) {
        return false;
      }
      auto& record = *state_->slots[key.slot_].record;
      if (!record.terminal ||
          *record.terminal != OperationTerminal::cancelled ||
          (record.state != OperationState::cancelling &&
           record.state != OperationState::delivered) ||
          record.native_settled) {
        return false;
      }
      record.native_settled = true;
      settled = true;
      if (record.consumed &&
          !record.submission_pending) {
        retire_locked(*state_, key.slot_, retired);
      }
    }
    retired.reset();
    return settled;
  }

  /**
   * 消费 delivered tombstone。
   *
   * completed 终态天然 native-settled，consume 会立即回收。cancelled 终态若
   * native 尚未 settled，consume 只转移上层结果，record/tombstone 与 Lease
   * 继续保留；直到 settle_native 或迟到 complete 确认内核终态后才推进
   * generation。所有实际 lease release 都发生在 registry mutex 外。
   */
  [[nodiscard]] std::optional<Delivery> consume(OperationKey key) {
    ensure_valid();
    std::optional<Delivery> delivery;
    std::unique_ptr<Record> retired;
    {
      const std::lock_guard lock{state_->mutex};
      if (!matches_locked(*state_, key)) {
        return std::nullopt;
      }
      auto& record = *state_->slots[key.slot_].record;
      if (record.state != OperationState::delivered ||
          !record.delivery || record.consumed) {
        return std::nullopt;
      }

      delivery.emplace(std::move(*record.delivery));
      record.consumed = true;
      if (record.native_settled &&
          !record.submission_pending) {
        retire_locked(*state_, key.slot_, retired);
      }
    }
    retired.reset();
    return delivery;
  }

  /**
   * 查询仍由 registry 拥有的 record 状态。
   *
   * stale、跨 registry 或已 consume/drain 的 key 返回空。
   */
  [[nodiscard]] std::optional<OperationState> state(
      OperationKey key) const {
    ensure_valid();
    const std::lock_guard lock{state_->mutex};
    if (!matches_locked(*state_, key)) {
      return std::nullopt;
    }
    return state_->slots[key.slot_].record->state;
  }

  /** 返回 registry 当前唯一拥有的 record/tombstone 数量。 */
  [[nodiscard]] std::size_t size() const {
    ensure_valid();
    const std::lock_guard lock{state_->mutex};
    return state_->active_records;
  }

  /**
   * 判断当前是否满足 drain/析构的 native 生命周期硬前置条件。
   *
   * created 从未提交给 driver，可以直接释放；其他 record 只有明确记录
   * native_settled 才安全。submitted、未 settled 的 cancelling 或 delivered
   * cancelled 均返回 false。已经移出 record 的 pending OperationDispatch 也
   * 计为 in-flight，必须先 run 或 discard_for_shutdown 并完成 ack。调用方必须
   * 先停止/撤销 driver、消费迟到 completion，再完成全部 dispatch。
   */
  [[nodiscard]] bool can_drain() const {
    if (!state_) {
      return true;
    }
    const std::lock_guard lock{state_->mutex};
    return can_drain_locked(*state_);
  }

  /**
   * 显式释放所有 active record 与 delivered tombstone。
   *
   * 本调用是 runtime shutdown 的最终全局握手：调用方必须已经停止/撤销平台
   * driver，并确认不存在任何 native in-flight 或迟到 completion。尤其不能在
   * IOCP completion 尚未 drain 时提前调用。普通单 operation 撤销应使用
   * settle_native，不得用 drain 绕过 native-terminal handshake。该前置条件
   * 由 can_drain_locked 执行验证；违反时立即 terminate，绝不提前释放可能仍被
   * 内核访问的 Lease。析构与 move-assignment 也复用本硬门槛。
   *
   * drain 旋转 registry nonce，因此 drain 前的全部 key/claim 永久失效。槽位
   * vector 整体移出 mutex 后才析构，保证 owning lease 在锁外释放。
   */
  std::size_t drain() noexcept {
    if (!state_) {
      return 0;
    }

    std::vector<Slot> retired;
    std::vector<std::uint32_t> retired_free_slots;
    std::size_t count = 0;
    {
      const std::lock_guard lock{state_->mutex};
      if (!can_drain_locked(*state_)) {
        std::terminate();
      }
      count = state_->active_records;
      retired = std::move(state_->slots);
      retired_free_slots = std::move(state_->free_slots);
      state_->active_records = 0;
      state_->nonce = operation_detail::next_registry_nonce();
    }
    return count;
  }

 private:
  [[nodiscard]] static bool matches_locked(
      const RegistryState& state,
      OperationKey key) noexcept {
    if (!key.valid() || key.registry_nonce_ != state.nonce ||
        static_cast<std::size_t>(key.slot_) >=
            state.slots.size()) {
      return false;
    }
    const auto& slot = state.slots[key.slot_];
    return slot.generation == key.generation_ &&
        static_cast<bool>(slot.record);
  }

  [[nodiscard]] static bool can_drain_locked(
      const RegistryState& state) noexcept {
    if (state.outstanding_dispatches.load(
            std::memory_order_acquire) != 0) {
      return false;
    }
    if (state.outstanding_submissions.load(
            std::memory_order_acquire) != 0) {
      return false;
    }
    for (const auto& slot : state.slots) {
      if (slot.record &&
          (slot.record->submission_pending ||
           (slot.record->state != OperationState::created &&
            !slot.record->native_settled))) {
        return false;
      }
    }
    return true;
  }

  static void acquire_dispatch_locked(
      RegistryState& state) noexcept {
    auto outstanding =
        state.outstanding_dispatches.load(
            std::memory_order_acquire);
    while (true) {
      if (outstanding ==
          std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      if (state.outstanding_dispatches.compare_exchange_weak(
              outstanding,
              outstanding + 1,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  static void acquire_submission_locked(
      RegistryState& state) noexcept {
    auto outstanding =
        state.outstanding_submissions.load(
            std::memory_order_acquire);
    while (true) {
      if (outstanding ==
          std::numeric_limits<std::size_t>::max()) {
        std::terminate();
      }
      if (state.outstanding_submissions.compare_exchange_weak(
              outstanding,
              outstanding + 1,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  static void acknowledge_submission(
      RegistryState& state) noexcept {
    auto outstanding =
        state.outstanding_submissions.load(
            std::memory_order_acquire);
    while (true) {
      if (outstanding == 0) {
        std::terminate();
      }
      if (state.outstanding_submissions.compare_exchange_weak(
              outstanding,
              outstanding - 1,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  [[nodiscard]] std::optional<DeliveryClaim> begin_terminal(
      OperationKey key,
      OperationState transition,
      OperationTerminal terminal,
      Result result) {
    ensure_valid();
    Delivery delivery{terminal, std::move(result)};
    {
      const std::lock_guard lock{state_->mutex};
      if (!matches_locked(*state_, key)) {
        return std::nullopt;
      }
      auto& record = *state_->slots[key.slot_].record;
      if (record.state != OperationState::submitted &&
          record.state != OperationState::submitting) {
        return std::nullopt;
      }
      record.delivery.emplace(std::move(delivery));
      record.terminal = terminal;
      record.native_settled =
          terminal == OperationTerminal::completed;
      record.state = transition;
    }
    return DeliveryClaim{state_, key, transition};
  }

  static void advance_generation(Slot& slot) noexcept {
    if (slot.generation ==
        std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    ++slot.generation;
  }

  static void retire_locked(
      RegistryState& state,
      std::uint32_t slot_index,
      std::unique_ptr<Record>& retired) noexcept {
    if (static_cast<std::size_t>(slot_index) >=
            state.slots.size() ||
        retired) {
      std::terminate();
    }
    auto& slot = state.slots[slot_index];
    // create 在发布每个新 slot 前预留了回收容量。该守卫既记录不变量，也让扩容
    // /复用压力测试可直接发现破坏；满足条件时 uint32_t push_back 不分配、不抛。
    if (state.free_slots.capacity() < state.slots.size() ||
        state.free_slots.size() >= state.slots.size() ||
        state.active_records == 0 ||
        !slot.record) {
      std::terminate();
    }
    state.free_slots.push_back(slot_index);
    retired = std::move(slot.record);
    advance_generation(slot);
    --state.active_records;
  }

  void ensure_valid() const {
    if (!state_) {
      throw std::logic_error{"OperationRegistry 已移动"};
    }
  }

  std::shared_ptr<RegistryState> state_;
};

}  // namespace cio::io
