#pragma once

#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "cio/io/buffer.hpp"
#include "cio/io/error.hpp"
#include "cio/task/task.hpp"

namespace cio::io {

namespace detail {

template <typename Session, typename T>
struct OwnedWriteOperation final {
  // 成员逆序析构：先销毁 primitive，再销毁覆盖完整 Future 的 endpoint session。
  std::unique_ptr<Session> session;
  Task<IoResult<T>> operation;
};

template <typename Session, typename T>
Task<IoResult<T>> keep_writer_alive(
    OwnedWriteOperation<Session, T> owned) {
  auto result = co_await std::move(owned.operation);
  (void)owned.session;
  co_return std::move(result);
}

}  // namespace detail

/**
 * Tokio AsyncWrite 的 coroutine-native C++20 安全映射。
 *
 * write、flush、shutdown 都必须由实现提供；输入是 owning lease，不能把临时
 * span 跨越暂停点。shutdown 成功必须已经包含成功 flush。
 */
template <typename Writer>
concept AuditedAsyncWriteEndpoint =
    requires {
      {
        std::remove_cvref_t<Writer>::cio_async_write_endpoint
      } -> std::convertible_to<bool>;
    } &&
    static_cast<bool>(
        std::remove_cvref_t<Writer>::cio_async_write_endpoint);

/**
 * 一个端点级 write 独占会话。
 *
 * Session 从创建到组合 Future 终态持续持有 Tokio `&mut self` 等价独占权。
 * write/flush/shutdown primitive 只能串行发起；取消一个 primitive 不能提前释放
 * 整个 Session。opt-in marker 还承诺 primitive Task 自拥有 control/lease，
 * 不借用可移动 Session 的地址。
 */
template <typename Session>
concept AuditedAsyncWriteSession =
    requires {
      {
        std::remove_cvref_t<Session>::cio_async_write_session
      } -> std::convertible_to<bool>;
    } &&
    static_cast<bool>(
        std::remove_cvref_t<Session>::cio_async_write_session);

template <typename Session>
concept AsyncWriteSession =
    AuditedAsyncWriteSession<Session> &&
    std::movable<Session> &&
    (!std::copy_constructible<Session>) &&
    (!std::is_copy_assignable_v<Session>) &&
    requires(Session session, ConstBufferLease lease) {
      {
        session.write(std::move(lease))
      } -> std::same_as<Task<IoResult<std::size_t>>>;
      {
        session.flush()
      } -> std::same_as<Task<IoResult<void>>>;
      {
        session.shutdown()
      } -> std::same_as<Task<IoResult<void>>>;
    };

/**
 * 满足形状且显式承诺 Tokio `&mut self` 等价独占语义的 writer handle。
 *
 * open_write_session 必须在 initiating call 同步取得端点独占权；复制 handle
 * 必须保持同一语义身份，或调用方必须移交 move-only endpoint。Session 必须
 * 覆盖完整组合 operation，形状匹配本身不等于通过所有权审计。
 */
template <typename Writer>
concept AsyncWrite =
    AuditedAsyncWriteEndpoint<Writer> &&
    requires(Writer writer) {
      typename std::remove_cvref_t<Writer>::WriteSession;
      requires AsyncWriteSession<
          typename std::remove_cvref_t<Writer>::WriteSession>;
      {
        writer.open_write_session()
      } -> std::same_as<
          typename std::remove_cvref_t<Writer>::WriteSession>;
    };

template <AsyncWrite Writer>
/**
 * 执行一次 write。
 *
 * 所有权：writer 与 ConstBufferLease 按值进入 operation。生命周期：输入存储由
 * lease 持有到终态。取消安全：实现返回 Pending 时不得已经提交字节；Ready
 * 允许部分写。线程迁移取决于 Writer 的 Send opt-in；本函数不阻塞 worker。
 */
Task<IoResult<std::size_t>> write(
    Writer writer,
    ConstBufferLease buffer) {
  using Session = typename std::remove_cvref_t<Writer>::WriteSession;
  auto session =
      std::make_unique<Session>(writer.open_write_session());
  auto operation = session->write(std::move(buffer));
  return detail::keep_writer_alive(detail::OwnedWriteOperation<
      Session,
      std::size_t>{
      std::move(session),
      std::move(operation)});
}

/**
 * 默认 vectored write 与 Tokio 一致：只向 write 提交首个非空 segment。
 *
 * 空序列或全空序列提交一个拥有式空 buffer。首片不声称具备原生 scatter/gather
 * 原子提交能力。
 */
template <AsyncWrite Writer>
/**
 * 执行一次默认 vectored write。
 *
 * 序列及每个 segment 都拥有底层存储；调用只提交首个非空 segment。取消、线程
 * 迁移与阻塞规则和 write 相同。
 */
Task<IoResult<std::size_t>> write_vectored(
    Writer writer,
    ConstBufferSequence buffers) {
  for (const auto& segment : buffers.segments()) {
    if (!segment.empty()) {
      return write(std::move(writer), segment);
    }
  }
  return write(
      std::move(writer),
      SharedBuffer{}.lease());
}

template <AsyncWrite Writer>
/**
 * 返回当前安全首片是否支持单次原生 scatter/gather。
 *
 * 默认固定为 false；后续平台实现只有在一次提交遵守 Tokio partial/Pending/error
 * 契约时才能返回 true。
 */
[[nodiscard]] bool is_write_vectored(const Writer&) noexcept {
  return false;
}

template <AsyncWrite Writer>
/**
 * 完成 writer 已缓冲数据的异步 flush。
 *
 * writer 按值拥有。取消前底层可能已经推进部分 flush，但重试不得重放已经提交
 * 的字节；线程迁移由 Writer 决定，不得阻塞 runtime worker。
 */
Task<IoResult<void>> flush(Writer writer) {
  using Session = typename std::remove_cvref_t<Writer>::WriteSession;
  auto session =
      std::make_unique<Session>(writer.open_write_session());
  auto operation = session->flush();
  return detail::keep_writer_alive(detail::OwnedWriteOperation<
      Session,
      void>{
      std::move(session),
      std::move(operation)});
}

template <AsyncWrite Writer>
/**
 * 异步关闭 writer。
 *
 * 成功必须隐含成功 flush，并把端点推进到终态。Tokio 未承诺 shutdown 完全
 * cancel-safe，CIO 同样不扩大承诺；线程迁移由 Writer 决定，不阻塞 worker。
 */
Task<IoResult<void>> shutdown(Writer writer) {
  using Session = typename std::remove_cvref_t<Writer>::WriteSession;
  auto session =
      std::make_unique<Session>(writer.open_write_session());
  auto operation = session->shutdown();
  return detail::keep_writer_alive(detail::OwnedWriteOperation<
      Session,
      void>{
      std::move(session),
      std::move(operation)});
}

}  // namespace cio::io
