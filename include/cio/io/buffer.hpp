#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cio::io {

class SharedBuffer;
class ConstBufferLease;

/**
 * 独占拥有的连续字节存储。
 *
 * span 只允许在 copy_from 的同步调用期间作为输入；OwnedBuffer 自身不保存该
 * span。异步 write 必须先转换成拥有式 ConstBufferLease。
 */
class OwnedBuffer final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  OwnedBuffer() = default;

  explicit OwnedBuffer(std::size_t size)
      : bytes_(size, std::byte{0}) {}

  [[nodiscard]] static OwnedBuffer copy_from(
      std::span<const std::byte> bytes) {
    OwnedBuffer result;
    result.bytes_.assign(bytes.begin(), bytes.end());
    return result;
  }

  OwnedBuffer(const OwnedBuffer&) = delete;
  OwnedBuffer& operator=(const OwnedBuffer&) = delete;
  OwnedBuffer(OwnedBuffer&&) noexcept = default;
  OwnedBuffer& operator=(OwnedBuffer&&) noexcept = default;
  ~OwnedBuffer() = default;

  [[nodiscard]] std::size_t size() const noexcept {
    return bytes_.size();
  }

  [[nodiscard]] bool empty() const noexcept {
    return bytes_.empty();
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    return bytes_;
  }

  [[nodiscard]] SharedBuffer share() &&;

 private:
  std::vector<std::byte> bytes_;

  friend class SharedBuffer;
};

/**
 * 只读共享字节存储。
 *
 * 子区间以 owner + offset + length 表达，不保存裸地址。复制 SharedBuffer 或
 * ConstBufferLease 会延长底层存储生命周期。
 */
class SharedBuffer final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  SharedBuffer()
      : storage_{std::make_shared<const std::vector<std::byte>>()} {}

  SharedBuffer(const SharedBuffer&) noexcept = default;
  SharedBuffer& operator=(const SharedBuffer&) noexcept = default;

  // move 采用共享复制语义，使源对象仍是可观察且拥有存储的有效只读句柄。
  SharedBuffer(SharedBuffer&& other) noexcept
      : storage_{other.storage_},
        offset_{other.offset_},
        length_{other.length_} {}

  SharedBuffer& operator=(SharedBuffer&& other) noexcept {
    if (this != &other) {
      storage_ = other.storage_;
      offset_ = other.offset_;
      length_ = other.length_;
    }
    return *this;
  }

  [[nodiscard]] static SharedBuffer copy_from(
      std::span<const std::byte> bytes) {
    auto storage = std::make_shared<const std::vector<std::byte>>(
        bytes.begin(),
        bytes.end());
    return SharedBuffer{std::move(storage), 0, bytes.size()};
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return length_;
  }

  [[nodiscard]] bool empty() const noexcept {
    return length_ == 0;
  }

  [[nodiscard]] std::byte at(std::size_t index) const {
    if (index >= length_) {
      throw std::out_of_range{"SharedBuffer 下标越界"};
    }
    return storage_->at(offset_ + index);
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    std::vector<std::byte> result;
    result.reserve(length_);
    for (std::size_t index = 0; index < length_; ++index) {
      result.push_back(storage_->at(offset_ + index));
    }
    return result;
  }

  [[nodiscard]] SharedBuffer subbuffer(
      std::size_t offset,
      std::size_t length) const {
    if (offset > length_ || length > length_ - offset) {
      throw std::out_of_range{"SharedBuffer 子区间越界"};
    }
    return SharedBuffer{storage_, offset_ + offset, length};
  }

  [[nodiscard]] ConstBufferLease lease() const;

 private:
  SharedBuffer(
      std::shared_ptr<const std::vector<std::byte>> storage,
      std::size_t offset,
      std::size_t length) noexcept
      : storage_{std::move(storage)},
        offset_{offset},
        length_{length} {}

  std::shared_ptr<const std::vector<std::byte>> storage_;
  std::size_t offset_{0};
  std::size_t length_{0};

  friend class OwnedBuffer;
  friend class ConstBufferLease;
};

/**
 * 异步写操作持有的只读 buffer 租约。
 *
 * 租约可以复制，所有副本都拥有底层 storage；平台后端完成或取消终态交付前
 * 必须保留至少一个副本。
 */
class ConstBufferLease final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  ConstBufferLease() = default;

  explicit ConstBufferLease(SharedBuffer buffer) noexcept
      : buffer_{std::move(buffer)} {}

  [[nodiscard]] std::size_t size() const noexcept {
    return buffer_.size();
  }

  [[nodiscard]] bool empty() const noexcept {
    return buffer_.empty();
  }

  [[nodiscard]] std::byte at(std::size_t index) const {
    return buffer_.at(index);
  }

  [[nodiscard]] std::vector<std::byte> snapshot() const {
    return buffer_.snapshot();
  }

  [[nodiscard]] ConstBufferLease sublease(
      std::size_t offset,
      std::size_t length) const {
    return ConstBufferLease{buffer_.subbuffer(offset, length)};
  }

 private:
  SharedBuffer buffer_;
};

inline SharedBuffer OwnedBuffer::share() && {
  auto storage = std::make_shared<const std::vector<std::byte>>(
      std::move(bytes_));
  const auto size = storage->size();
  return SharedBuffer{std::move(storage), 0, size};
}

inline ConstBufferLease SharedBuffer::lease() const {
  return ConstBufferLease{*this};
}

/**
 * 拥有式只读 scatter/gather 序列。
 *
 * 每个 segment 都是 ConstBufferLease，因此序列跨暂停存活时不存在临时 span
 * 悬空。默认 vectored write 只消费首个非空 segment。
 */
class ConstBufferSequence final {
 public:
  static constexpr bool cio_send = true;
  static constexpr bool cio_sync = true;

  ConstBufferSequence() = default;

  explicit ConstBufferSequence(std::vector<ConstBufferLease> segments)
      : segments_{std::move(segments)} {}

  void push(ConstBufferLease segment) {
    segments_.push_back(std::move(segment));
  }

  [[nodiscard]] std::size_t segment_count() const noexcept {
    return segments_.size();
  }

  [[nodiscard]] bool empty() const noexcept {
    return segments_.empty();
  }

  [[nodiscard]] std::size_t total_size() const {
    std::size_t total = 0;
    for (const auto& segment : segments_) {
      if (segment.size() > std::numeric_limits<std::size_t>::max() - total) {
        throw std::length_error{"ConstBufferSequence 总长度溢出"};
      }
      total += segment.size();
    }
    return total;
  }

  [[nodiscard]] const std::vector<ConstBufferLease>& segments() const& noexcept {
    return segments_;
  }

  const std::vector<ConstBufferLease>& segments() const&& = delete;

 private:
  std::vector<ConstBufferLease> segments_;
};

}  // namespace cio::io
