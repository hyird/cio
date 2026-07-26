# bounded mpsc 设计

## 对齐范围

本垂直切片对齐 Tokio 1.53.1 的有界 `tokio::sync::mpsc` 核心能力：

- `channel<T>(capacity)`，容量必须位于 `1..=Semaphore::MAX_PERMITS`；
- 可复制 `Sender<T>` 的 `send`、`try_send`、`reserve`、`try_reserve`、
  `reserve_owned`、`try_reserve_owned`、`closed`、`same_channel`、
  `blocking_send`、`is_closed`、`capacity`、`max_capacity`、`downgrade`
  和强弱计数；
- move-only `Receiver<T>` 的 `recv`、`try_recv`、`blocking_recv`、`close`、
  `is_closed`、`len`、`is_empty`、容量和发送端计数；
- move-only `Permit<T>` 的预留、发送与析构归还；
- move-only `OwnedPermit<T>` 的拥有式预留、发送/释放后返还 Sender、
  channel identity 和析构归还；
- `WeakSender<T>` 的复制、计数、`upgrade` 和不可复活语义；
- `SendError`、`TrySendError`、`TryRecvError` 的值返还、分类和格式。

这是 bounded mpsc 的前两个阶段，不代表整个 `tokio::sync::mpsc` 已完成。
批量 reserve/permit iterator、轮询式接收、批量接收及更多诊断 API 仍需后续
垂直切片；unbounded channel 已由独立文档记录。

## 状态与容量

channel 使用一个拥有式共享状态：

- `Semaphore` 保存剩余容量，并提供 send/reserve 共用的 FIFO 等待队列；
- `std::deque<std::shared_ptr<T>>` 保存已经发布的消息；
- `Notify` 唤醒唯一接收 operation；
- 短 mutex 临界区保护消息队列、Receiver 状态和逻辑 strong/weak sender 计数。

容量从初始上限开始。send 或 reserve 取得一个 semaphore permit 后容量减一；
未使用 Permit 析构时立即归还；消息发布会 forget 该 permit，直到 Receiver 取走
消息才归还容量。因此缓冲消息和 outstanding Permit 都占用容量。

`try_send` 尊重 semaphore 的既有 FIFO 等待者：即使当前瞬间看似有容量，也不能
越过已经排队的 send/reserve。队首取消会失去位置，已经部分分配的容量由
Semaphore 转交或归还。

## 关闭、排空与析构

`Receiver::close` 线性化关闭容量 semaphore，之后的新 send/reserve 返回关闭
错误，但关闭前已经取得的 Permit 仍可成功发送。Receiver 必须继续排空：

1. 已缓冲消息；
2. 关闭前 outstanding Permit 后续发送的消息；
3. outstanding Permit 全部发送或析构后，才观察最终断开。

因此 close 后缓冲区暂时为空但仍有 Permit 时，`try_recv` 返回 `Empty`，
而不是 `Disconnected`。

最后一个逻辑 Sender 析构会关闭发送侧。WeakSender 不维持 channel 开放；
strong count 一旦归零，`upgrade` 永久失败。Permit 共享原 Sender 的逻辑 lease，
不额外增加 strong count，但会像 Rust 借用一样维持该 Sender 直到 Permit 被消费
或析构。

`reserve_owned` 在普通包装函数中立即消费 Sender，再创建不捕获 `this` 的
coroutine operation。pending operation、成功的 OwnedPermit，以及
`send`/`release` 返回的 Sender 始终转移同一个逻辑 strong lease，不会隐式
clone。`try_reserve_owned` 的 Full/Closed 错误拥有原 Sender，调用方可通过
`into_inner` 恢复；异步 `reserve_owned` 的关闭错误与 Tokio 一样只返回
`SendError<void>`，被消费的 Sender 不返还。

Receiver 真正析构时关闭发送侧，并把当时已经缓冲的消息移到锁外析构。与固定
Tokio 实现一致，Receiver 析构后已有 Permit/OwnedPermit 仍可调用 send；若
WeakSender 或返回的 Sender 继续持有 channel 控制块，该晚到值可以保留到控制块
最终析构。此行为不能误写成“Receiver drop 后立即析构所有未来 Permit 值”。

## 合作式调度与取消

Tokio 1.53.1 的 bounded mpsc 在 poll capacity semaphore 前先执行
`coop::poll_proceed`。CIO 的 send/reserve 同样先消耗合作式预算，再取得容量或
进入 FIFO 队列；`reserve_owned` 复用同一顺序。预算耗尽时，当前 task 必须先
重新排队：

- 不得提前减少 channel capacity；
- 不得提前占据 semaphore FIFO 位置；
- 即使结果是同步 `Closed`，也必须计为一次 ready progress；
- 自调度后的新 poll 必须从重置的 128 单位预算中扣除该次完成。

recv 只有在消息或终态 ready 时才消耗预算；真正 Pending 的等待不消耗。取消
pending recv 会注销 Notify 等待且不取走消息，同一 Receiver 同时只允许一个
接收 operation。

Tokio 1.53.1 的 `Sender::closed` 只通过 `Notified` 等待 Receiver close/drop，
不经过 cooperative budget。CIO 保持该行为，并在检查 closed 前先创建通知
operation，避免检查与关闭之间丢失 wake；Receiver 关闭会唤醒全部 waiter，
取消只注销本次等待，重新等待仍可观察永久关闭状态。

send/reserve operation 拥有 Sender lease，不捕获 `this` 或调用方引用。recv
operation 拥有 Receiver lease；外层 C++ Receiver 先析构不会形成悬垂引用，
等价于把 Rust `&mut Receiver` 的独占生命期安全映射到 operation 所有权。

## 所有权、异常与线程迁移

公开 API 不接收或暴露裸指针。`MpscOwnedValue` 在编译期拒绝裸指针、引用和
标准或结构等价的非拥有引用包装。消息、lease、waiter 和 coroutine frame 都由
值语义或拥有式智能句柄管理。

用户 `T` 的移动和析构不在 channel 状态锁内执行：

- pending send 在取得容量前保留 coroutine 参数，不提前执行用户 move；
- 发布时先在锁外构造 owning candidate，再把智能句柄放入队列；
- recv 先从队列取出 owning handle、归还容量，再在锁外构造结果；
- Receiver drop 先交换队列，再在锁外析构所有消息。

如果消息候选构造或队列节点分配抛异常，已取得的 Permit 通过 RAII 归还，消息
不会发布。若最终接收结果的 `T` 移动抛异常，该消息已经从队列移除并提供基本
异常保证：不会重复交付，Receiver 仍可继续使用。task 内异常由 JoinHandle
观察，不得逃逸 worker 事件循环。

Sender、Receiver、WeakSender、Permit 和 OwnedPermit 在 `T` 满足 CIO Send
时均可跨 worker 迁移；异步 operation 保守标记为非 Sync。运行时 worker 只
短暂取得内部 mutex，容量与消息等待会挂起 task，不执行无界阻塞。

`blocking_send` 和 `blocking_recv` 只允许普通同步线程调用，并以独立
current-thread runtime 作为同步桥接；在 CIO 异步执行上下文调用会抛出
`std::logic_error`。

## 性能链路

benchmark 框架已加入 `mpsc_bounded`：固定容量 64，使用
`min(N, max(2, workers * 4))` 个 producer 和一个 consumer；producer 合计发送
且仅发送 `1..N`，consumer 在全部 Sender 释放后排空至关闭，并校验消息数、
失败数和 64 位校验和。CIO 与 Tokio 使用相同 operations、worker、task 和容量
元数据；Asio 没有等价的 Tokio 风格 bounded mpsc channel，因此明确 skip。

Windows 上另以 N=100000、容量 64、2 次预热、5 个样本、CPU 亲和性 0-3
运行 dirty smoke：单 worker CIO/Tokio 中位耗时为 43.125/3.285 ms（约
13.128 倍，CIO RSD 0.60%）；四 worker 为 122.814/10.727 ms（约 11.449 倍，
CIO RSD 7.45%）。原始 JSON 与报告位于
`build-bench/mpsc-second-slice-smoke/20260725T174431Z-dirty-smoke.*`。
工作树仍为 dirty，样本数未达到正式门槛且没有 profiler，因此这些数字只能
作为后续调查调度、分配与 wake 热点的信号，不能写成性能结论。

## 验证边界

状态机测试覆盖 FIFO/背压、send/reserve/reserve_owned 取消、两类 Permit 容量、
close 后排空、closed 多 waiter/取消重试、strong/weak 精确计数、channel
identity、len/is_empty、blocking bridge、跨线程唤醒、runtime shutdown、用户
移动异常、锁外析构和值守恒。固定 Tokio 1.53.1 的差分覆盖：

- `mpsc_fifo_backpressure`；
- `mpsc_send_reserve_fairness`；
- `mpsc_cancel_send`；
- `mpsc_cancel_reserve`；
- `mpsc_permit_capacity`；
- `mpsc_close_drain_permit`；
- `mpsc_try_errors`；
- `mpsc_receiver_drop`；
- `mpsc_last_sender_weak`；
- `mpsc_sender_counts`；
- `mpsc_error_format`；
- `mpsc_closed_wakes`；
- `mpsc_closed_cancel_safe`；
- `mpsc_same_channel`；
- `mpsc_receiver_len_empty`；
- `mpsc_try_reserve_errors`；
- `mpsc_owned_permit_send_release`；
- `mpsc_owned_permit_same_channel`；
- `mpsc_owned_permit_lifetime`；
- `mpsc_reserve_owned_closed_consumes_sender`；
- `mpsc_try_reserve_owned_errors`；
- `mpsc_reserve_owned_cancel_safe`。

当前 Windows/MSVC Debug、Release 和 ASan 全套测试均通过；bounded mpsc
Release 回归连续执行 300/300，ASan 回归连续执行 100/100。固定 Tokio 1.53.1
的全套差分累计 84 项并通过 84/84 与 20/20 重复验证；核心源码契约检查覆盖
48 个文件并通过。

Windows ASan 不等同于 LeakSanitizer。Linux/macOS、Clang/GCC、UBSan/TSan/LSan、
模型检查、完整 API 表面和正式性能结论仍是必需缺口。
