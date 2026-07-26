#pragma once

#include <concepts>
#include <memory>
#include <type_traits>
#include <utility>

#include "cio/io/error.hpp"
#include "cio/io/read_buf.hpp"
#include "cio/task/task.hpp"

namespace cio::io {

/**
 * Tokio AsyncRead 的 coroutine-native C++20 安全映射。
 *
 * primitive 消费并在成功时返还 MutableBufferLease。这样组合操作能够在多次
 * read 间继续持有同一独占租约，而实现不得把 ReadBuf 引用保存进协程帧。
 */
template <typename Reader>
concept AuditedAsyncReadEndpoint =
    requires {
      {
        std::remove_cvref_t<Reader>::cio_async_read_endpoint
      } -> std::convertible_to<bool>;
    } &&
    static_cast<bool>(
        std::remove_cvref_t<Reader>::cio_async_read_endpoint);

/**
 * 一个端点级 read 独占会话。
 *
 * Session 必须移动专属，并从创建到最后一个 primitive 终态持续占有同一端点。
 * primitive 可以先后执行，但不能重叠；销毁 Pending primitive 后，Session 仍
 * 保留端点独占，直至组合 operation 终态或取消。opt-in marker 还承诺每个
 * primitive Task 自拥有 Session control/lease，不借用可移动 Session 的地址。
 */
template <typename Session>
concept AuditedAsyncReadSession =
    requires {
      {
        std::remove_cvref_t<Session>::cio_async_read_session
      } -> std::convertible_to<bool>;
    } &&
    static_cast<bool>(
        std::remove_cvref_t<Session>::cio_async_read_session);

template <typename Session>
concept AsyncReadSession =
    AuditedAsyncReadSession<Session> &&
    std::movable<Session> &&
    (!std::copy_constructible<Session>) &&
    (!std::is_copy_assignable_v<Session>) &&
    requires(Session session, MutableBufferLease lease) {
      {
        session.read(std::move(lease))
      } -> std::same_as<Task<IoResult<MutableBufferLease>>>;
    };

/**
 * 满足形状且显式承诺 Tokio `&mut self` 等价独占语义的 reader handle。
 *
 * open_read_session 必须在 initiating call 同步取得端点独占权；复制 handle
 * 必须保持同一语义身份，或调用方必须把 move-only endpoint 移交给 operation。
 * Session 的独占窗口覆盖整个组合 Future，不能在 partial primitive 之间释放。
 */
template <typename Reader>
concept AsyncRead =
    AuditedAsyncReadEndpoint<Reader> &&
    requires(Reader reader) {
      typename std::remove_cvref_t<Reader>::ReadSession;
      requires AsyncReadSession<
          typename std::remove_cvref_t<Reader>::ReadSession>;
      {
        reader.open_read_session()
      } -> std::same_as<
          typename std::remove_cvref_t<Reader>::ReadSession>;
    };

namespace detail {

template <AsyncReadSession Session>
struct OwnedReadOperation final {
  // 成员逆序析构：先销毁 primitive，再销毁覆盖完整 Future 的 endpoint session。
  std::unique_ptr<Session> session;
  Task<IoResult<MutableBufferLease>> operation;
};

template <AsyncReadSession Session>
Task<IoResult<void>> read_once(
    OwnedReadOperation<Session> owned) {
  auto result = co_await std::move(owned.operation);
  (void)owned.session;
  if (!result.has_value()) {
    co_return IoResult<void>::failure(std::move(result).error());
  }
  auto completed_lease = std::move(result).value();
  (void)completed_lease;
  co_return IoResult<void>::success();
}

}  // namespace detail

/**
 * 对 reader 执行一次异步读取尝试。
 *
 * 所有权：reader 按值进入 Task；调用阶段同步取得 buffer 独占租约。生命周期：
 * Task 只持 owning lease，不捕获 buffer 引用。取消安全：未交付完成前不得改变
 * buffer；内存端点首片满足此保证。线程迁移：取决于 Reader 的 Send opt-in；
 * 本函数不执行阻塞调用。
 */
template <AsyncRead Reader>
Task<IoResult<void>> read(Reader reader, ReadBuf& buffer) {
  auto lease = buffer.lease_mut();
  auto session = std::make_unique<
      typename std::remove_cvref_t<Reader>::ReadSession>(
      reader.open_read_session());
  auto operation = session->read(std::move(lease));
  return detail::read_once(detail::OwnedReadOperation<
      typename std::remove_cvref_t<Reader>::ReadSession>{
      std::move(session),
      std::move(operation)});
}

}  // namespace cio::io
