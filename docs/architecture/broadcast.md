# broadcast 设计

## 对齐范围

本垂直切片以 Tokio 1.53.1 的 `tokio::sync::broadcast` 为唯一公开语义
基线，目标覆盖：

- `channel<T>(capacity)` 与只有 Sender 的构造能力；
- `Sender<T>` 的发送、订阅、弱化、关闭等待、长度、计数和 channel identity；
- move-only `Receiver<T>` 的独立游标、`recv`、`try_recv`、阻塞桥接、
  `resubscribe`、长度、关闭和 Sender 计数；
- `WeakSender<T>` 的升级和强弱计数；
- `SendError<T>`、`RecvError` 与 `TryRecvError`；
- 环形覆盖、精确 lag、最后 Sender/Receiver 析构、取消、异常、跨线程唤醒和
  runtime shutdown。

Tokio 的 `Sender::new` 在 C++ 中与关键字 `new` 冲突，CIO 使用
`Sender::new_sender` 表达同一能力。API 名称是 C++20 安全映射，能力和可观察
语义仍以固定 Tokio 版本为准。

## 容量、序号与独立游标

请求容量必须大于零且不得超过 `size_t` 最大值的一半。与 Tokio 相同，非 2 的
幂容量向上取整到下一个 2 的幂；例如请求容量 3 的实际环容量是 4。

每次成功发送在 channel 状态锁下形成一个全序：

1. 读取发送线性化点的 Receiver 数；
2. 以 64 位 wrapping 序号选择环形槽；
3. 记录该消息仍需被多少个当时存活的 Receiver 消费；
4. 发布消息并推进 tail；
5. 解锁后唤醒等待者并回收被覆盖值。

没有 Receiver 时发送失败、返还原值且不得推进 tail。新的 Receiver 从订阅时的
tail 开始，因此不会看到订阅前的历史消息。`resubscribe` 同样创建当前 tail
游标，不复制原 Receiver 的 backlog；Receiver 本身不可复制，避免把 Rust 的
显式重订阅误映射为隐式复制。

Receiver 的 `len` 是 tail 与自身游标的 wrapping 距离，因此可以大于实际环
容量。距离恰好等于容量时仍能读取最老保留消息；只有距离大于容量时才返回
`Lagged(distance - capacity)`，并把游标移动到当前最老保留序号。报告 lag
本身不领取该最老值，下一次接收才复制它。

Sender 的 `len` 语义不同：只统计环内仍未被发送时全部 Receiver 消费的消息，
被覆盖值不计入，结果不会大于实际环容量。Receiver 领取或析构 backlog 时都会
恰好一次递减消息的剩余接收者计数。

## 消息所有权与锁外用户代码

环形槽不保存非拥有地址。每个槽保存序号、checked 剩余接收者计数和拥有式消息
节点；Sender、WeakSender、Receiver、异步 operation 与 waiter 都只使用值
语义或拥有式智能句柄。

公开 channel 值在编译期拒绝裸指针、`reference_wrapper`、`std::span` 和
`std::string_view`。用户自定义类型仍需通过 CIO 的 owned/Send trait 审核其
深层状态；C++20 无法自动证明任意用户对象没有隐藏非拥有地址。

发送候选在 channel 状态锁外构造。锁内只交换 owning 句柄和更新元数据；被
覆盖消息的最后 owner 析构、错误值返还、通知和 task 恢复都发生在锁外。

接收在状态锁内完成 lag 判断、游标推进、剩余计数递减和消息节点强拥有句柄的
提取，随后解锁并复制 `T`。因此：

- 用户复制构造不会重入 channel 状态锁；
- 复制抛异常时游标已经推进、剩余计数已经恰好递减一次；
- 同一 Receiver 下一次接收下一条消息，对齐 Tokio 的 clone panic 语义；
- 消息覆盖、最后消费和 Receiver 析构都不会在 channel 锁内运行用户析构。

多个 Receiver 可能同时复制同一个广播值。实现必须用消息节点自身的受控同步
串行化这些复制，或者把更严格的 `Sync` 要求明确记录为安全替代；不得无同步地
并发读取未知用户类型，也不得为了规避该问题而在 channel 状态锁内执行复制。

首片用 `std::list<Entry>` 保存最多实际容量条消息的逻辑环窗口。Receiver 析构
在状态锁内只递减元数据，并用 `list::splice` 无分配地把 consumed prefix 转移
到锁外析构，避免 noexcept 清理因临时容器扩容而终止。该结构保证当前生命周期
正确性，但每次 send 仍分配 list/message 节点，sender `len` 和按序号查找为
O(capacity)，没有达到 Tokio 固定槽环的分配与复杂度；必须以 profiler 数据驱动
后续固定环、slab 和 ResourceHandle 优化。

## 接收、取消与 cooperative budget

异步 `recv` 使用 owning Receiver lease，不保存调用方 `this`、裸引用或协程
地址。同一 Receiver 同时最多有一个会推进游标的 operation；外层 Receiver
移动或析构不会让正在等待的 operation 悬空，也不会提前减少逻辑 Receiver
计数。

每轮等待按“先取得 Notify generation，再检查消息或关闭状态”的顺序执行：

- 已有消息、lag 或关闭时立即形成终态；
- 空 channel 时注册 waiter 并挂起；
- 发送和最后 Sender 析构都唤醒全部当前接收 waiter；
- wake-before-wait、wait-before-wake 和取消-vs-wake 不会丢失通知。

pending `recv` 在真正提交游标前取消，不领取消息。成功、`Lagged` 和 `Closed`
三类 ready 结果都消耗一次 Tokio cooperative budget；`try_recv`、同步发送和
`Sender::closed` 不消耗预算。一次真实挂起后的 fresh poll 必须重新扣费，而
同一根 poll 中仅因 Notify 已就绪而重试不能重复扣费。

CIO 当前的直接 `Task` await 路径可在真实挂起后重置预算；统一
`select`/`join`/timeout 组合轮询的 Pending 预算退款仍需由通用 cooperative
wrapper 收口。在该 wrapper 完成前，broadcast 只能标记为部分实现。

`blocking_recv` 只允许普通线程使用。它复用同一 owning 异步 operation 与关闭
规则；在 CIO 异步执行上下文内调用必须抛出 `std::logic_error`，不能阻塞
runtime worker。

## 关闭、重订阅与弱 Sender

最后一个强 Sender 析构后，channel 的发送方向永久关闭并唤醒全部 Receiver。
Receiver 的 `is_closed` 此时立即为真，但仍需先 drain 环中对自己可见的保留
消息，只有游标追上 tail 后才返回 `Closed`。

最后一个 Receiver 析构时：

- 先从逻辑 Receiver 数中移除自己；
- 只清理析构线性化点之前属于自己的未读 backlog；
- 恰好一次递减相应消息的剩余接收者计数；
- 标记当前接收侧无订阅者并唤醒全部 `Sender::closed` waiter。

只要仍有强 Sender，后续 `subscribe` 可以重新打开接收侧。若最后 Receiver
析构已经唤醒 `closed`，但 Sender 在 future 重新 poll 前又订阅了 Receiver，
`closed` 必须重新观察状态并继续等待，不能把一次通知误当成永久终态。

`WeakSender` 保持共享控制状态存活但不维持发送方向。升级与最后强 Sender
析构在同一个受控状态下线性化；强 Sender 数一旦到零便不能复活。强、弱和
Receiver 逻辑计数均使用 checked increment，拒绝回绕成伪造的零。

Tokio 的 async `closed(&self)` 由 Rust 借用保证原 Sender 存活。CIO 需要用
隐藏拥有借用覆盖 operation 生命周期，同时不能把该内部借用伪报成用户可见的
额外 Sender。该安全映射必须在计数与最后析构测试中明确验证。

## 错误与异常

`SendError<T>` 拥有失败发送的值。lvalue 错误对象可以短暂借用值；从临时错误
对象禁止返回引用，调用方通过 `into_inner() &&` 按值取回所有权。

`RecvError` 区分 `Closed` 与精确的 `Lagged(u64)`；
`TryRecvError` 还包含 `Empty`。错误比较、文本和输出格式应与固定 Tokio 版本
的含义逐项对应。

C++ 用户类型的构造、移动、复制和析构都可能抛异常或重入。候选构造失败不得
修改 channel；接收复制失败按 Tokio clone panic 语义保留已提交游标；清理、
取消、wake 和析构路径必须 `noexcept`，无法安全恢复的内部不变量破坏才允许
终止进程。

## 验收边界

首个完整切片至少需要以下证据后才能标记为“部分实现”而非“未实现”：

- 容量取整、无 Receiver 发送、独立 Receiver、精确 lag 和 resubscribe；
- 先 drain 后关闭、try 错误、Sender 长度与 Receiver 长度；
- Receiver drop 清理、最后端点关闭、重新订阅和 Weak 不可复活；
- pending 取消、lost-wake、跨线程总序、runtime shutdown；
- copy 异常后游标前进，以及覆盖、领取、析构时的用户重入；
- cooperative ready 边界、fresh-poll 扣费和非 cooperative 操作；
- Windows Debug/Release/ASan、反复压力、核心裸指针契约扫描；
- 固定 Tokio 1.53.1 差分探针。

Linux、macOS、TSan、LSan、模型检查、正式 profiler 数据和可复现三平台性能
对比未完成前，不得宣称 broadcast 或 CIO 已完整对齐 Tokio。

当前 Windows 实测证据：

- Release、Debug、MSVC ASan 专项均为 12/12；
- 当前全套 CTest 为 Release/Debug 13/13、ASan 14/14；
- Release 重复压力 100/100，MSVC ASan 重复压力 50/50；
- broadcast 新增 14 项 Tokio 1.53.1 差分，当前全套 Release/Debug
  141/141；
- 核心契约扫描当前覆盖 57 个文件；
- `broadcast_fanout` dirty smoke 在 CPU 0–3、2 次预热/5 样本下，1 worker
  CIO/Tokio p50 为 3.213/0.555 ms（5.792x），4 worker 为
  25.710/3.565 ms（7.212x）；4-worker CIO RSD 17.26%，数据仅验证链路并
  标识 profiler 优先级。

当前仍记录为安全映射或未收口的边界：

- Receiver checked 上限使用 `size_t` 最大值，而 Tokio 内部保留锁位后的
  `MAX_RECEIVERS` 更小；
- hidden Sender borrow 活跃而公开 Sender 已析构时，发送方向仍存活但
  `strong_count()` 可见为 0；这是避免虚报内部借用的 C++ owning 安全映射；
- per-message recursive mutex 串行复制与 list/逐消息分配尚未优化；
- 通用 `select`/`join`/timeout Pending budget 退款和三平台动态证据尚未完成。
