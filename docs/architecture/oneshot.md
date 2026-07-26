# oneshot 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `tokio::sync::oneshot`：

- `channel<T>()`；
- move-only `Sender<T>` 的 `send`、`closed`、`is_closed` 和
  `poll_closed` 能力映射；
- move-only `Receiver<T>` 的 `close`、`is_empty`、`is_terminated`、
  `try_recv`、异步接收和 `blocking_recv`；
- `RecvError`、`TryRecvError::Empty/Closed` 的分类与 Display/Debug 文本；
- 两端析构、显式关闭、取消、跨线程发布和 runtime shutdown。

条目仍保持“部分实现”。`poll_closed(Context)` 被映射为拥有式 C++20
awaitable，不能源码级复刻 Rust 的 `Context/Waker`；通用 Debug、tracing、
loom/TSan、Linux/macOS 动态证据和正式性能证据仍待闭环。

## 单值状态机与线性化

`Sender` 与 `Receiver` 共享一个拥有式状态。短同步临界区只保护以下状态：

- Sender 是否已经发送或无值关闭；
- Receiver 是否关闭或已经观察终态；
- 可空的唯一 owning 值；
- receive 与 closed operation 的独占登记。

`send` 先在状态锁外构造 owning 候选值，再在锁内与 `Receiver::close` 竞争：

- send 先线性化：值发布成功；随后 close 仍保留该值，Receiver 可以取出；
- close 先线性化：send 失败并完整返还原值；
- send 成功只说明线性化时 Receiver 尚未关闭，不保证 Receiver 最终消费。

Sender 未发送即析构会发布无值关闭；Receiver 尚未观察错误，因此
`is_empty()` 为 true、`is_terminated()` 为 false。Receiver 通过异步接收或
`try_recv` 观察 `RecvError/Closed` 后才进入 terminated。Receiver 析构会关闭
发送侧并在锁外销毁未接收值。

状态分别使用两个 Notify 唤醒接收等待者和 `Sender::closed` 等待者。同步
`blocking_recv` 使用 condition variable，但只允许在普通同步线程调用。状态
锁释放后才执行 Notify/condition 唤醒，恢复 task 不会在内部锁下重入 channel。

## 接收、关闭与析构

`Receiver::close()` 禁止之后线性化的 send，但不会丢弃已经发布的值。单独调用
close 也不会立即把 Receiver 标为 terminated；调用方仍须执行
`try_recv`/异步接收来取得旧值或观察关闭错误。

Rust 用 `&mut Receiver` 静态排除并发接收。C++20 无法证明独占借用，因此 CIO
使用拥有式 `Receive` 和运行期登记：

- 同一 Receiver 同时只能存在一个 receive operation；
- active receive 存续时，`try_recv`、`close`、`blocking_recv` 或第二次
  `receive` 以 `logic_error` 拒绝；
- Wait 与 Awaiter 都是 move-only、single-use 和 non-Sync，重复 poll/resume
  明确拒绝；
- Receiver 本体在 active operation 等待期间析构时，operation 仍拥有共享
  状态，并稳定完成为 `RecvError`，不会悬垂或变成内部取消异常。

最后一项是 Rust 借用规则下无法写出的 C++ 生命周期边界。它不放宽正常并发访问，
只保证 owning operation 与句柄析构竞态具有安全、确定的关闭结果。

## 取消与无丢失唤醒

异步接收与 `Sender::closed` 都采用“先创建 Notify generation snapshot，再登记
独占 operation，poll 时检查状态、启用通知并在 suspend 前复查”的顺序。它覆盖：

- 事件发生在 operation 创建前；
- 创建后、首次 poll 前；
- 首次状态检查和 waiter 注册之间；
- 注册完成后、真正 suspend 前；
- task 已经挂起之后。

接收值只在 `await_resume` 中移出。receive 在未 poll、pending 或已通知未恢复
处被取消时，只注销 waiter 和独占登记，不消费值、不终止 Receiver；之后可以
重新接收。`Sender::closed` 取消同样只注销等待状态，Sender 随后仍可重新等待或
发送。

runtime shutdown 销毁挂起 task 后，operation 的 owning lease 注销 Notify
登记。只要 channel 句柄仍存活，可以在新 runtime 中重新等待、发送或接收。

## 所有权、异常与线程迁移

公开 API 不接收或暴露裸指针，不用引用或 `std::reference_wrapper` 保存异步
关系。值、共享状态、通知 operation 和协程状态均由值语义或智能 owning handle
管理；原生地址不跨越调用边界。`OneshotOwnedValue` 还会在编译期拒绝标准及
结构等价的非拥有引用包装，避免局部对象离开作用域后仍能从 channel 取出悬垂
引用。

所有可能执行 `T` 用户代码的移动、复制和析构都在 channel 状态锁外：

- send 的候选值先在锁外构造；
- receive/try_recv 先把 owning 值移出状态，再构造结果；
- Receiver drop 先把 owning 值移出状态，再在锁外析构；
- send 失败的值返还也发生在锁外。

若候选值在 send 线性化前构造失败，channel 保持未发送；若最终结果值的移动在
状态已经线性化后抛异常，channel 保持终态并提供基本异常保证，不会重复交付。
异常由普通同步调用方观察，或由 task 边界保存为 JoinError，不能逃逸 worker
事件循环。

Tokio 1.53.1 的边界是 Sender/Receiver 在 `T: Send` 时同时为 Send/Sync；
CIO 的 mobility trait 保持相同条件。Receive/Closed 及其 Awaiter 在 `T: Send`
时为 Send、始终 non-Sync，允许 portable task 在暂停后迁移 worker，但禁止
并发 poll 同一 operation。

## 阻塞行为

`send`、`try_recv`、`close` 和状态查询只取得不跨 `co_await` 的短同步锁。
异步 receive/closed 通过 Notify 挂起 task，不阻塞 runtime worker。

与 Tokio 的 `coop::poll_proceed` 一致，Receive 和 Closed 每次产生 ready 进展
都会消耗一个 cooperative budget 单位；128 个单位耗尽后，awaiter 先将当前
task 重新排队，并在 fresh poll 恢复时从重置后的 128 单位中扣除这次完成，再
交付结果。真正 pending 的 poll 不消耗预算，但通知唤醒后的 ready fresh poll
会扣除一个单位。这样不会形成每轮 129 次的硬上限偏差，
send-before-receive 或 already-closed 的热循环不能永久饿死同一 worker 上的
其他 task。

`blocking_recv() &&` 消费 Receiver 的接收能力并阻塞普通线程；在 CIO
current-thread 或 multi-thread 执行上下文调用会抛 `logic_error`。它可被
跨线程 send、Sender drop 或 Receiver 关闭唤醒，不允许与 active receive 并发。

## 验证证据与剩余门槛

本地测试覆盖：

- send-before-receive、wait-before-send、Sender drop、Receiver close/drop；
- close 保留旧值、close-before-send 返还值、try_recv Empty/Closed；
- `Sender::closed`、Receiver drop 唤醒及全部 operation 独占冲突；
- receive/closed 在未 poll、pending、已通知未恢复处取消并重试；
- Receiver drop 与 active receive 的 owning 生命周期；
- current-thread、4 worker runtime、外部同步线程和 `blocking_recv` 可见性；
- 500 轮 send/receive 无丢失唤醒、300 轮 send/close 线性化竞态；
- always-ready receive/closed 热循环在 cooperative budget 上限内让出 worker；
- 预算自调度和真实 Notify 唤醒后的 fresh poll 精确扣除首个 ready progress；
- 非拥有引用包装的编译期拒绝；
- runtime shutdown 后在新 runtime 复用，以及 20 次 runtime 反复启停；
- send 成功、失败返还和 Receiver drop 的值恰好析构一次；
- send 候选移动异常后的可重试，以及 try_recv/receive 结果移动异常后的终态、
  禁止重复交付和构造/析构计数守恒；
- 值析构时重入同一 Receiver 状态查询，验证用户析构确实发生在锁外；
- move-only 句柄、错误文本、诊断文本与 Send/Sync 静态边界。

固定 Tokio 1.53.1 的 Rust/C++ 差分新增十一项：

- `oneshot_send_receive`；
- `sender_drop_recv_error`；
- `receiver_drop_returns_value`；
- `close_preserves_sent`；
- `close_rejects_late_send`；
- `try_recv_empty_closed`；
- `receive_cancel_safe`；
- `sender_closed_wakes`；
- `empty_terminated_transitions`；
- `value_drop_once`；
- `oneshot_ready_budget_yields`。

当前 Windows/MSVC Debug、Release 与 ASan 目标通过，修复 Receiver drop 与
active receive、非拥有类型和协作预算边界后，Release 回归连续执行 300/300
无失败，异常/重入扩展后的 ASan 回归曾连续执行 100/100；fresh-poll 预算修复后
又通过 ASan 50/50。全套固定 Tokio 差分累计 73 项并通过 73/73 和 20/20 重复
验证。Windows
ASan 不等同于 LeakSanitizer，这些结果不能替代 Linux/macOS、Clang/GCC、
UBSan/TSan/LSan、模型检查和三平台高竞争验证。

性能框架已加入 `oneshot_wake`：为每个 operation 建立独立 channel，并确认
Receiver 实际进入 Pending 后再统一发送、等待和校验值守恒。固定 CPU 2–5、
5000 operation、3 次预热/10 样本的 dirty smoke 显示 CIO 中位耗时暂约为
Tokio 的 9 倍，只能作为 profiler 与分配优化的优先级信号。仍需增加
send/close 竞争和跨线程 `blocking_recv` 负载，并在 clean revision 上记录
CPU、分配、park/unpark 与平台 profiler 证据；在此之前不形成正式性能结论。
