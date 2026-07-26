#pragma once

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "cio/io/async_write.hpp"

namespace cio::io {

namespace detail {

template <AsyncWriteSession Session>
struct WriteAllOperation final {
  // 逆序析构：primitive -> 输入 lease -> 覆盖完整组合 Future 的 Session。
  std::unique_ptr<Session> session;
  ConstBufferLease buffer;
  Task<IoResult<std::size_t>> operation;
};

template <AsyncWriteSession Session>
struct EmptyWriteAllOperation final {
  // 空输入不创建 primitive；输入 lease 先于 endpoint Session 析构。
  std::unique_ptr<Session> session;
  ConstBufferLease buffer;
};

template <AsyncWriteSession Session>
Task<IoResult<void>> write_all_empty(
    EmptyWriteAllOperation<Session> owned) {
  // 空输入保持 endpoint 独占窗口，但不创建/poll writer，也不会隐式 flush。
  (void)owned;
  co_return IoResult<void>::success();
}

template <AsyncWriteSession Session>
Task<IoResult<void>> write_all_loop(
    WriteAllOperation<Session> owned) {
  std::size_t offset = 0;
  while (true) {
    auto result = co_await std::move(owned.operation);
    if (!result.has_value()) {
      co_return IoResult<void>::failure(
          std::move(result).error());
    }

    const auto written = result.value();
    const auto remaining = owned.buffer.size() - offset;
    if (written == 0) {
      co_return IoResult<void>::failure(Error::write_zero());
    }
    if (written > remaining) {
      co_return IoResult<void>::failure(Error{
          ErrorKind::invalid_data,
          "AsyncWrite 返回的字节数超过提交长度"});
    }

    offset += written;
    if (offset == owned.buffer.size()) {
      co_return IoResult<void>::success();
    }

    // primitive guard 已释放，endpoint 独占仍由 owned.session 连续持有。
    owned.operation = owned.session->write(
        owned.buffer.sublease(
            offset,
            owned.buffer.size() - offset));
  }
}

}  // namespace detail

/**
 * 持续写入整个 owning buffer。
 *
 * Tokio 1.53.1 对齐：空输入不 poll writer；partial write 后只提交尚未写出的
 * 后缀；底层错误原样传播；非空剩余输入成功写入零字节时返回 write_zero。合法
 * AsyncWrite 不得报告超过提交长度的计数，CIO 将该契约违规映射为 invalid_data。
 *
 * 所有权：调用阶段同步取得 WriteSession；Session、完整输入 lease 和当前
 * primitive 全部进入 Task。生命周期：Session 覆盖全部 partial primitives，
 * 销毁顺序保证 primitive 与输入早于 Session。取消安全：与 Tokio `write_all`
 * 一致，不保证 cancel-safe；取消前已写出的前缀不回滚，使用完整输入重试可能
 * 重复写入。线程迁移由 Session 的 Send 审计决定；本操作不 flush 且不阻塞
 * worker。
 */
template <AsyncWrite Writer>
Task<IoResult<void>> write_all(
    Writer writer,
    ConstBufferLease buffer) {
  const auto empty = buffer.empty();
  using Session =
      typename std::remove_cvref_t<Writer>::WriteSession;
  auto session =
      std::make_unique<Session>(writer.open_write_session());
  if (empty) {
    return detail::write_all_empty(
        detail::EmptyWriteAllOperation<Session>{
            std::move(session),
            std::move(buffer)});
  }
  auto operation = session->write(buffer);
  detail::WriteAllOperation<Session> owned{
      std::move(session),
      std::move(buffer),
      std::move(operation)};
  return detail::write_all_loop(std::move(owned));
}

}  // namespace cio::io
