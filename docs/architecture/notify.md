# Notify 异步通知状态机设计

## 范围与 Tokio 对应

本切片对应 Tokio `1.53.1` 的：

- `tokio::sync::Notify::new`；
- `notified`、`notified_owned` 与 `Notified::enable`；
- `notify_one`、`notify_last`、`notify_waiters`；
- 单 permit、FIFO/LIFO、broadcast 创建快照和取消转交语义。

CIO `Notify` 是可复制共享值句柄，复制等价于复制 Rust `Arc<Notify>`；
`Notify::Notified` 始终拥有内部状态，因此同时覆盖 Rust 借用 `Notified<'a>` 和
`OwnedNotified` 的安全能力。`const_new`、tracing instrumentation 和 loom 模型
测试尚未实现，所以兼容矩阵保持“部分实现”。

## 所有权

`Notify` 持有 `shared_ptr<NotifyState>`。每次 `notified()` 创建独立的
`NotifyWaitOperation`：

- operation 强拥有 NotifyState，确保 Notify 句柄析构后等待仍安全；
- NotifyState 的 FIFO 队列只保存 operation 的 `weak_ptr` 和单调 waiter ID；
- operation 持有等待 task 的 `ExecutionContext`，不保存协程地址；
- waiter ID 用于取消时从队列移除，不用对象地址比较；
- task 取消或协程帧析构时，operation 析构同步撤销登记。

公开 API 与核心队列不接收、保存或暴露裸指针，也不捕获跨暂停点裸引用。

## 单 permit 与登记

NotifyState 在同一互斥下维护：

```text
permit: bool
notify_waiters_generation: uint64
waiters: list<{ waiter_id, weak operation, empty retained slot }>
```

`notify_one` 在没有已登记 waiter 时把 `permit` 设为 true；重复调用仍只有一个
permit。新 Notified 首次 `enable` 或 poll 时：

1. 若已收到 waiter 专属通知，进入 fused 完成态；
2. 若 broadcast generation 已变化，完成但不消费 permit；
3. 若 permit 存在，消费 permit 并完成；
4. 否则进入 FIFO 队列。

`enable()` 与首次 poll 使用同一个状态转换。它允许调用方在检查外部队列之前先
占据 Notify 等待位置，避免多个生产事件折叠成单 permit 后丢失一个消费者 wake。

## await 与无丢失唤醒

`await_ready` 先执行 enable。若仍未完成，`await_suspend` 在 NotifyState 锁内：

1. 再次检查通知、generation 和 permit；
2. 确保 operation 已登记；
3. 通过 ExecutionContext 把当前暂停位置写回 task 的 owning wrapper；
4. 安装 context 后才允许 notifier 获得同一锁。

因此 notify-before-wait、wait-before-notify，以及 notify 落在
`await_ready`/`await_suspend` 之间都不会丢失。notify 从队列取出 waiter 后先
写入通知种类、移走 context，再在锁外 wake；用户 task 不会在状态锁内被 poll。

## FIFO、LIFO 与 broadcast

- `notify_one` 从队首选择最早登记的 waiter；
- `notify_last` 从队尾选择最近登记的 waiter；
- `notify_waiters` 递增 generation，并标记调用时队列中的全部 waiter；
- Notified 在创建时记录 generation，因此即使尚未 enable/poll，只要
  `notify_waiters` 发生在创建之后，它仍会完成；
- `notify_waiters` 不创建 permit；
- 若调用前已有一个 `notify_one` permit，broadcast 不消费该 permit。

notify_waiters 先在锁内原子标记整个旧队列并移出，再逐个在锁外 wake。新 waiter
只能进入新队列，不会被错误纳入已经开始的 broadcast。

队列使用 `weak_ptr` 时必须额外处理最后 owner 竞态：notifier 在状态锁内执行
`weak_ptr::lock()`，得到的强引用可能恰好成为 operation 的最后所有者；若该
临时量在锁内析构，operation 析构会调用 `cancel()` 并递归获取同一 mutex。
CIO 使用已分配的 `std::list` waiter 节点和 `splice`：

- 扫描前把节点无分配地 splice 到锁外声明的 `retired` 列表；
- 成功提升的强引用存入该 retired 节点；
- FIFO/LIFO 单播、broadcast 和取消转交都保持同一规则；
- retired 只在状态锁释放后析构，所有 wake 也发生在锁外。

该设计不要求 noexcept 取消路径临时分配 keep-alive 数组，也不会在 bad_alloc
时把最后 operation owner 留在锁内。

## 取消安全与通知转交

取消尚未获通知的 Notified 会失去 FIFO/LIFO 队列位置，不产生 permit。

若 `notify_one` 已把专属通知交给某 waiter，但该 waiter 在消费前被取消，析构
会按原策略转交通知：

- FIFO 通知转给当前最早 waiter；
- LIFO 通知转给当前最近 waiter；
- 若没有 waiter，则恢复成单个 permit。

`notify_waiters` 的 broadcast 不转交，因为所有调用时已存在的 Notified 都通过
generation 或直接标记独立观察该次 broadcast。

## 线程迁移与阻塞

NotifyState 的所有共享转换都由短互斥临界区保护；竞争时只短暂阻塞调用
`notify_*` 或 poll 的线程，不会等待用户条件，也不会把 runtime worker 长期
阻塞。等待 task 通过 ExecutionContext 挂起，可在 multi-thread runtime 的其他
worker 恢复。CIO 不把该实现声称为 lock-free。

## 已执行验证

Windows/MSVC 单元、竞态和压力测试覆盖：

- 多次 notify_one 只保存一个 permit；
- enable 前后通知；
- FIFO `notify_one` 与 LIFO `notify_last`；
- notify_waiters 的未 poll 创建快照、全部唤醒和不存 permit；
- broadcast 保留旧 permit；
- 取消未通知 waiter；
- 取消已获单播通知时转交下一 waiter；
- current-thread 与 multi-thread 各 100 轮跨线程 wake；
- current-thread 与 multi-thread 各 100 轮 notify/abort 竞态；
- FIFO、LIFO 与 broadcast 的 retired waiter/最后 owner 同时取消竞态；
- 100 waiter broadcast fanout。

Tokio 1.53.1 差分新增 `notify_permit_coalesces`、`notify_fifo_lifo`、
`notify_waiters_snapshot` 和 `notify_cancel_transfers`。Linux、macOS、TSan、
LSan 和 loom 等价状态空间探索仍需实际证据。
