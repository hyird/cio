#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "cio/io/async_read.hpp"

namespace cio::io {

namespace detail {

template <AsyncReadSession Session>
struct ExactReadOperation final {
  // 成员逆序析构：先取消当前 primitive，再释放覆盖完整组合 Future 的 Session。
  std::unique_ptr<Session> session;
  Task<IoResult<MutableBufferLease>> operation;
};

template <AsyncReadSession Session>
struct EmptyExactReadOperation final {
  // 空目标不创建 primitive；先释放 buffer lease，再释放 endpoint Session。
  std::unique_ptr<Session> session;
  MutableBufferLease buffer;
};

template <AsyncReadSession Session>
Task<IoResult<std::size_t>> read_exact_empty(
    EmptyExactReadOperation<Session> owned,
    std::size_t target_size) {
  // 空目标仍持有 endpoint/ReadBuf 独占窗口，但不创建也不 poll primitive。
  (void)owned;
  co_return IoResult<std::size_t>::success(target_size);
}

template <AsyncReadSession Session>
Task<IoResult<std::size_t>> read_exact_loop(
    ExactReadOperation<Session> owned,
    std::size_t remaining_before,
    std::size_t target_size) {
  while (true) {
    auto result = co_await std::move(owned.operation);
    if (!result.has_value()) {
      co_return IoResult<std::size_t>::failure(
          std::move(result).error());
    }

    auto buffer = std::move(result).value();
    const auto remaining_after = buffer.remaining();
    if (remaining_after == 0) {
      co_return IoResult<std::size_t>::success(target_size);
    }
    if (remaining_after == remaining_before) {
      co_return IoResult<std::size_t>::failure(
          Error::unexpected_eof());
    }
    if (remaining_after > remaining_before) {
      co_return IoResult<std::size_t>::failure(Error{
          ErrorKind::invalid_data,
          "AsyncRead 违反 ReadBuf 只追加契约"});
    }

    remaining_before = remaining_after;
    // primitive guard 已释放，但 endpoint 独占仍由 owned.session 连续持有。
    owned.operation = owned.session->read(std::move(buffer));
  }
}

}  // namespace detail

/**
 * 持续读取，直到填满一个初始为空的 ReadBuf。
 *
 * Tokio 1.53.1 对齐：成功返回调用时的目标长度；零长度目标不 poll reader；
 * 目标未满而一次成功 primitive 没有增加 filled 时返回 unexpected_eof；底层
 * 错误原样传播。为安全映射 Rust `&mut [u8]` 的固定目标，本首片要求 ReadBuf
 * 调用时 `filled_size() == 0`，否则在 initiating call 抛出 invalid_argument。
 *
 * 所有权：调用阶段同步取得 ReadBuf lease 与端点级 ReadSession，Task 不保存
 * ReadBuf 引用。生命周期：Session 覆盖全部 partial primitives，外部别名不能
 * 插入；销毁顺序保证当前 primitive 先于 Session。取消安全：与 Tokio
 * `read_exact` 一致，不保证 cancel-safe；取消前完成的分段读取保留在 ReadBuf，
 * 调用方必须检查部分数据后决定是否重试。线程迁移由 Session 的 Send 审计决定；
 * 本组合操作不阻塞 worker。
 */
template <AsyncRead Reader>
Task<IoResult<std::size_t>> read_exact(
    Reader reader,
    ReadBuf& buffer) {
  if (buffer.filled_size() != 0) {
    throw std::invalid_argument{
        "read_exact 要求初始 ReadBuf filled 为空"};
  }

  auto lease = buffer.lease_mut();
  const auto target_size = lease.remaining();
  using Session =
      typename std::remove_cvref_t<Reader>::ReadSession;
  auto session =
      std::make_unique<Session>(reader.open_read_session());
  if (target_size == 0) {
    return detail::read_exact_empty(
        detail::EmptyExactReadOperation<Session>{
            std::move(session),
            std::move(lease)},
        target_size);
  }
  auto operation = session->read(std::move(lease));
  detail::ExactReadOperation<Session> owned{
      std::move(session),
      std::move(operation)};
  return detail::read_exact_loop(
      std::move(owned),
      target_size,
      target_size);
}

}  // namespace cio::io
