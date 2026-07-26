# unbounded mpsc 设计

## 对齐范围

本垂直切片对齐 Tokio 1.53.1 的无界 `tokio::sync::mpsc` 核心能力：

- `unbounded_channel<T>()`；
- 可复制 `UnboundedSender<T>` 的同步 `send`、`closed`、`is_closed`、
  `same_channel`、`downgrade` 和强弱计数；
- `WeakUnboundedSender<T>` 的复制、移动、`upgrade` 和强弱计数；
- move-only `UnboundedReceiver<T>` 的 `recv`、`try_recv`、
  `blocking_recv`、`close`、`is_closed`、`len`、`is_empty` 和发送端计数；
- 与 bounded channel 共用的 `SendError<T>` 和 `TryRecvError` 语义。

`recv_many`、`blocking_recv_many`、`poll_recv` 和 `poll_recv_many` 暂留下一
垂直切片。unbounded 家族本来就没有 try_send、reserve、Permit、capacity、
blocking_send 或 send_timeout；同步 `send` 永远不等待容量。

## 状态、队列与内存上限

unbounded channel 使用独立拥有式共享状态：

- `std::deque<std::shared_ptr<T>>` 保存已发布消息；
- 一个短 mutex 保护队列、Receiver 状态和逻辑 strong/weak sender 计数；
- 两个 `Notify` 分别唤醒 Receiver 和等待 Receiver 关闭的 Sender；
- Sender、Weak 和 Receiver lease 分离公开逻辑计数与共享控制块寿命。

Weak 持有共享状态，但只增加逻辑 weak count。最后一个 strong Sender 析构后，
strong count 永久归零，Weak 不能复活 channel；Receiver 已 close/drop 但仍有
strong Sender 时，Weak 仍可升级，只是得到的 Sender 已关闭且 send 返回错误。

该 channel 没有背压。系统内存是隐式上限，Receiver 落后时队列可以持续增长。
“send 不阻塞”只表示不等待容量，不表示无分配、wait-free 或不会发生 OOM。
正式性能评估必须同时记录分配、峰值 RSS、尾延迟和 shutdown drain 时间。

## send、close 与排空

消息先在状态锁外构造成 owning candidate。`send` 与 Receiver close/drop 在同一
mutex 内决定线性化顺序：

- send 先取得锁并成功入队，则返回成功，之后 close 仍允许 Receiver drain；
- close/drop 先取得锁，则 send 返回拥有原值的 `SendError<T>`；
- 队列节点分配抛异常时不会半提交，锁释放后 candidate 在锁外析构。

本实现没有跨同步临界区的“已获发布资格但尚未入队”状态，因此无需 Tokio 内部
计数器式 in-flight publication；锁内 enqueue 本身就是完整发布线性化点。

Receiver `close()` 立即使 Sender 观察到关闭并唤醒全部 `closed()` waiter，但
不会丢弃已缓冲消息。`recv`/`try_recv` 先 drain，之后才返回空值/
`Disconnected`。Receiver 真正析构时交换出整个队列，并在状态锁外析构用户值，
允许析构回调安全重入 channel 查询。

## cooperative budget 与取消

固定 Tokio 1.53.1 中：

- 同步 `send` 不消耗 cooperative budget；
- `Sender::closed` 不消耗 budget；
- ready `recv` 和关闭终态每次消耗一个单位；
- Pending recv 没有取得进展，不永久扣除预算。

CIO 保持上述行为。`closed()` 先创建 generation-aware 通知 operation，再检查
关闭状态，避免检查与 close/drop 之间丢 wake；取消只注销本次 waiter，关闭状态
永久可见。`recv()` 在取走消息前取消不消费消息，operation 拥有 Receiver lease，
不捕获 `this`、裸引用或 `reference_wrapper`。Notify 真正唤醒后的 fresh poll
也精确扣除一次预算，连续 ready 接收保持 128 次进展上限。

## 所有权、异常与线程迁移

`MpscOwnedValue` 在编译期拒绝裸指针、引用和非拥有引用包装。消息、状态、
lease、waiter 和 coroutine frame 都由值语义或拥有式智能句柄管理。

用户 `T` 的移动和析构不在 channel 状态锁内执行。消息候选构造抛异常时队列
不变；最终接收移动抛异常时该消息已从队列移除，保证不会重复交付，Receiver
仍可继续使用。Receiver drop、关闭 send 错误和异常路径均验证值恰好析构一次。

Sender/Weak 可并发跨线程发送和升级；Receiver 可移动到其他 worker，但始终只有
一个逻辑消费端。`blocking_recv` 只允许普通同步线程调用，在 CIO 异步执行上下文
内会抛出 `std::logic_error`，不会阻塞 runtime worker。

## 验证边界

本地状态机与压力测试覆盖：

- wake-before-wait、wait-before-wake、FIFO 和四生产者值守恒；
- close/drain、close/send 线性化压力、Receiver drop 和最后 Sender；
- 多个 `closed()` waiter、取消重试、pending recv 取消和 runtime shutdown；
- Weak 在 open/close/drop 状态下的升级、永久不可复活和精确计数；
- `len/is_empty/is_closed`、错误 payload、Display/Debug；
- blocking bridge、跨线程 wake、ready/fresh-poll budget；
- 用户移动异常、接收异常、析构重入和恰好一次析构。

固定 Tokio 1.53.1 增加 14 项差分，覆盖 FIFO/多 Sender、错误、close/drain、
Receiver drop、closed 多 waiter/取消、last Sender + Weak、identity/计数、
len/is_empty、ready recv budget、值析构、close/drop 后 Weak upgrade、recv
取消，以及 send/closed 不消耗 budget。

当前 Windows/MSVC Debug、Release 和 ASan 全套测试通过；unbounded mpsc
Release 回归连续执行 300/300，ASan 回归连续执行 100/100。固定 Tokio 1.53.1
的全套差分累计 98 项并通过 98/98，Release 全套差分重复通过 20/20；核心源码
契约检查覆盖 48 个文件并通过。

Windows ASan 不等同于 LeakSanitizer。Linux/macOS、Clang/GCC、UBSan/TSan/LSan、
模型检查、batch/poll 接收 API 和正式性能证据仍是必需缺口。
