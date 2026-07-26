#include "cio/detail/timer_driver.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "cio/detail/timer_state.hpp"

namespace cio::detail {

TimerKey TimerDriver::insert(
    std::uint64_t deadline_tick,
    std::uint64_t epoch,
    std::weak_ptr<TimerWaitState> state) {
  const auto key =
      allocate_slot(deadline_tick, epoch, std::move(state));
  place(key, deadline_tick);
  return key;
}

TimerKey TimerDriver::replace(
    TimerKey old_key,
    std::uint64_t deadline_tick,
    std::uint64_t epoch,
    std::weak_ptr<TimerWaitState> state) {
  cancel(old_key);
  return insert(deadline_tick, epoch, std::move(state));
}

void TimerDriver::cancel(TimerKey key) noexcept {
  if (slot_matches(key)) {
    release_slot(key);
  }
}

std::vector<TimerFire> TimerDriver::process(std::uint64_t now_tick) {
  if (now_tick > elapsed_tick_) {
    elapsed_tick_ = now_tick;
  }

  std::vector<TimerKey> queued;
  for (auto& level : levels_) {
    for (auto& bucket : level) {
      while (!bucket.empty()) {
        queued.push_back(bucket.front());
        bucket.pop_front();
      }
    }
  }
  for (const auto& [deadline, key] : overflow_) {
    (void)deadline;
    queued.push_back(key);
  }
  overflow_.clear();

  std::vector<TimerFire> fires;
  fires.reserve(queued.size());
  for (const auto key : queued) {
    if (!slot_matches(key)) {
      continue;
    }

    const auto slot_index =
        static_cast<std::size_t>(TimerKeyFactory::slot(key));
    auto& slot = slots_[slot_index];
    auto state = slot.state.lock();
    if (!state) {
      release_slot(key);
      continue;
    }
    if (slot.deadline_tick <= elapsed_tick_) {
      fires.push_back(TimerFire{
          .state = std::move(state),
          .epoch = slot.epoch,
      });
      release_slot(key);
      continue;
    }

    place(key, slot.deadline_tick);
  }
  return fires;
}

std::optional<std::uint64_t> TimerDriver::next_deadline() const noexcept {
  std::optional<std::uint64_t> next;
  for (const auto& slot : slots_) {
    if (!slot.active || slot.state.expired()) {
      continue;
    }
    if (!next || slot.deadline_tick < *next) {
      next = slot.deadline_tick;
    }
  }
  return next;
}

std::size_t TimerDriver::active_timer_count() const noexcept {
  return static_cast<std::size_t>(std::count_if(
      slots_.begin(),
      slots_.end(),
      [](const TimerSlot& slot) {
        return slot.active && !slot.state.expired();
      }));
}

void TimerDriver::shutdown() noexcept {
  for (auto& level : levels_) {
    for (auto& bucket : level) {
      bucket.clear();
    }
  }
  overflow_.clear();
  slots_.clear();
  free_slots_.clear();
}

bool TimerDriver::matches_runtime(TimerKey key) const noexcept {
  return key.valid() &&
         TimerKeyFactory::runtime_nonce(key) == runtime_nonce_;
}

bool TimerDriver::slot_matches(TimerKey key) const noexcept {
  if (!matches_runtime(key)) {
    return false;
  }
  const auto slot_index =
      static_cast<std::size_t>(TimerKeyFactory::slot(key));
  if (slot_index >= slots_.size()) {
    return false;
  }
  const auto& slot = slots_[slot_index];
  return slot.active &&
         slot.generation == TimerKeyFactory::generation(key);
}

TimerKey TimerDriver::allocate_slot(
    std::uint64_t deadline_tick,
    std::uint64_t epoch,
    std::weak_ptr<TimerWaitState> state) {
  std::uint32_t slot_index = 0;
  if (free_slots_.empty()) {
    if (slots_.size() >=
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
      throw std::overflow_error{"CIO runtime timer slot 已耗尽"};
    }
    slot_index = static_cast<std::uint32_t>(slots_.size());
    slots_.emplace_back();
  } else {
    slot_index = free_slots_.back();
    free_slots_.pop_back();
  }

  auto& slot = slots_[slot_index];
  slot.deadline_tick = deadline_tick;
  slot.epoch = epoch;
  slot.state = std::move(state);
  slot.active = true;
  return TimerKeyFactory::make(
      slot_index,
      slot.generation,
      runtime_nonce_);
}

void TimerDriver::release_slot(TimerKey key) noexcept {
  if (!slot_matches(key)) {
    return;
  }
  const auto slot_index =
      static_cast<std::size_t>(TimerKeyFactory::slot(key));
  auto& slot = slots_[slot_index];
  slot.active = false;
  slot.deadline_tick = 0;
  slot.epoch = 0;
  slot.state.reset();
  ++slot.generation;
  if (slot.generation == 0) {
    std::terminate();
  }
  free_slots_.push_back(static_cast<std::uint32_t>(slot_index));
}

void TimerDriver::place(TimerKey key, std::uint64_t deadline_tick) {
  const auto delta =
      deadline_tick > elapsed_tick_ ? deadline_tick - elapsed_tick_ : 0;
  if (delta >= wheel_horizon) {
    overflow_.emplace(deadline_tick, key);
    return;
  }

  const auto level = choose_level(deadline_tick);
  const auto shift = static_cast<unsigned int>(level * 6);
  const auto bucket_index = static_cast<std::size_t>(
      (deadline_tick >> shift) &
      static_cast<std::uint64_t>(slots_per_level - 1));
  levels_[level][bucket_index].push_back(key);
}

std::size_t TimerDriver::choose_level(
    std::uint64_t deadline_tick) const noexcept {
  const auto delta =
      deadline_tick > elapsed_tick_ ? deadline_tick - elapsed_tick_ : 0;
  std::size_t level = 0;
  std::uint64_t span = slots_per_level;
  while (level + 1 < level_count && delta >= span) {
    ++level;
    span <<= 6;
  }
  return level;
}

}  // namespace cio::detail
