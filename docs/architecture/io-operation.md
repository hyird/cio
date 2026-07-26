# I/O operation 代际表与 native 终态握手

## 1. 范围

本切片建立平台中立的 `OperationKey`、operation 生命周期和拥有式 lease
tombstone。它用于约束后续 epoll、IOCP 与 kqueue 后端，但当前尚未连接真实
系统调用，也不代表平台 driver 已完成。

固定 Tokio 1.53.1 的公开语义仍由上层 coroutine API 决定；本层只负责保证：

- 调度队列和完成包只携带 slot、generation 与 registry nonce，不携带对象地址；
- complete 与 cancel 竞争只能产生一个上层终态；
- wake 在 registry 锁外执行且不抛异常；
- IOCP 等 completion backend 在内核真正终结前始终保有提交时的 buffer lease；
- stale、跨 registry 与槽位复用后的迟到完成不能命中新 operation。

## 2. 拥有值边界

`OperationRegistry<Lease, Result, Wake>` 的三个异步值都经过编译期审计：

- `Lease` 与 `Result` 必须是非引用、非裸指针、非已知 borrowed view 的
  `cio::Send` owning value，并且移动和析构不抛异常；
- `Wake` 还必须显式声明 `cio_operation_wake=true` 与
  `cio_operation_enqueue_only=true`，并提供 `void(OperationKey) noexcept`；
- `span`、`string_view`、`reference_wrapper` 即使被错误标成 `Send` 仍在本
  边界拒绝；
- 默认 `NoopOperationWake` 只用于不需要恢复上层 task 的内部记录。

registry 不保存任意 `std::function`，因此 wake 热路径不会引入未经审计的引用
捕获或异常。Wake 只能通过 weak runtime 与代际 task/lane key 把工作入队，不得
直接 resume 或 poll continuation。`OperationDispatch::run` 只在 registry 锁外
调用一次 owning wake。

C++20 无法从类型结构证明 callable 内部真的只执行 enqueue；上述 marker 是显式
审计承诺，仍需 code review、线程探针与真实 runtime 测试共同约束。默认
`NoopOperationWake` 和当前测试 `ProbeWake` 都满足该承诺，Probe 只调用测试
enqueue target，不执行目标 continuation。

## 3. key 与状态机

`OperationKey` 由三部分组成：

- `slot`：定位 registry 槽位；
- `generation`：拒绝槽位复用后的 ABA；
- `registry_nonce`：拒绝跨 registry 误投，并在 shutdown drain 后使全部旧 key
  失效。

状态转移固定为：

```text
created -> submitting -> submitted -> completing -> delivered
               |            \------> cancelling -> delivered
               \--------------------> completing/cancelling
```

`begin_submit` 必须在进入可能即时完成的 OS API 前调用。它把 record 推进为
`submitting` 并返回唯一 `SubmissionGuard`；OS 同步接受后 `accept`，同步拒绝且
保证不会再产生 completion 时 `reject`。即时 completion/cancel 可以先从
`submitting` 认领终态，guard 随后只收口 submission gate，不覆盖终态。
`rollback_created` 只用于尚未进入提交事务的平台准备失败，并在 registry 锁外
释放 lease。

`begin_complete` 与 `begin_cancel` 先在锁内竞争唯一 `DeliveryClaim`；胜者再用
`deliver` 发布终态并把 owning wake 移出锁区。claim、dispatch 和 delivery 都是
move-only，不能复制交付权。

live claim 被丢弃时不会停在 `completing`/`cancelling`：析构会执行
`abandon`，发布已认领终态并在锁外 enqueue wake。`valid` 会在锁内核对 nonce、
generation、record 与 expected state；drain 后的旧 claim 不会只因 weak state
仍存在而误报有效。显式 `deliver` 若在 mutex 获取阶段遇到异常，交付权保持
active，可由调用方重试，不会先清权再留下半提交 record。

## 4. cancelled operation 的双栅栏

上层观察到取消，不等于平台已停止访问原生 operation storage。尤其在 Windows
上，`CancelIoEx` 成功或上层 future 已取消，都不能证明 IOCP completion 已经
出队。

因此 cancelled record 只有同时满足以下两项才允许回收：

1. `consumed`：上层已经消费唯一 cancelled delivery；
2. `native_settled`：driver 已确认内核不会再访问 storage/buffer。

两者可以任意顺序完成：

- consume 在先：结果先返回，record/tombstone 与 lease 继续留在 registry；
- native settled 在先：记录确认，但仍等待上层消费结果；
- IOCP 迟到 completion 调用 `complete` 时，不能覆盖 cancelled delivery，只
  完成 native-terminal handshake；
- epoll/kqueue readiness backend 在同步撤销注册并确认没有在途 syscall 后，
  显式调用 `settle_native`。

只有双栅栏都完成后，槽位才推进 generation 并把 record 移到锁外析构。
completed operation 天然已经 native-settled，consume 后可以直接回收。

## 5. 回收与 shutdown

新增 slot 发布前，registry 先为 free-list 预留至少与 slot 数相同的容量。因此
`retire_locked` 不分配且不抛异常；consume/settle 已经提交状态后不会因
free-list 扩容失败留下无法再次消费的半终态。复用与回收入口还会检查 slot
范围、record 占用、active 数量和重复 retired owner，不变量损坏时 fail-fast。

`deliver` 在发布 `delivered` 前增加 `outstanding_dispatches` 原子计数，并把
`RegistryState` 强所有权和唯一 ack 权一起交给 `OperationDispatch`。dispatch
move 只转移该权利；最终 owner 的 `run`、`discard_for_shutdown` 或 pending
析构会依次：

1. enqueue wake，或在 shutdown 明确丢弃；
2. 析构 dispatch 持有的全部 owning Wake 状态；
3. 用原子 RMW 恰好一次 ack。

`can_drain` 与 `drain` 同时要求 native record 安全、outstanding submission
为零且 outstanding dispatch 为零。SubmissionGuard 在 reject 时先锁外析构
退役 record/lease，再 ack；dispatch ack 则位于 wake owning 析构之后。两类
ack 都使用 release/acquire 链，deliver/begin_submit 与 drain 仍由同一 registry
mutex 串行，避免“检查为零后又发布提交权或 dispatch”的窗口。

`drain` 是 shutdown 的最终全局握手，不是普通取消捷径。调用前必须已经：

1. 停止新 operation 提交；
2. 请求取消并停止平台 driver；
3. drain 所有 native completion；
4. 让所有 claim 完成交付或在 shutdown drain 中失效；
5. run 或显式 discard 全部 outstanding dispatch；
6. 确认不存在内核仍可能访问的 buffer。

只有 `created` 或明确 `native_settled` 的 record 才可回收；submitted、未
settled 的 cancellation 或 outstanding dispatch 会使 `can_drain` 返回 false，
直接调用 `drain`、析构或 move-assignment 则 fail-fast。满足门槛后才能旋转
registry nonce，并在锁外析构全部 record/lease。

## 6. 当前证据与限制

当前 Windows/MSVC OperationRegistry 专项测试共 **10 组**，覆盖提交与终态
状态机、锁外 enqueue wake、
generation/ABA、跨 registry 拒绝、complete-vs-cancel、claim/dispatch 丢弃与
move 唯一权、consume/native-settled 两种顺序及竞态、IOCP 式迟到 completion、
readiness 显式 settle、native/dispatch 双重 drain 门槛、driver 线程析构
dispatch 与并发 drainer、enqueue 线程 ID 且目标 continuation 未直接执行、
2048 槽容量扩展复用，以及 4096 operation/8 线程压力。另有 CopyBuffer 集成
测试覆盖 created rollback 后重试、submitting 阶段即时 completion、同步拒绝、
cancel 后迟到 completion 和 cancel-during-submitting。

这仍是平台中立所有权原型，存在明确的后续工作：

- 当前 slot-map 原型使用 `std::mutex`，并为 record 与 owning Wake 使用独立
  `unique_ptr` 分配；这是为了先证明锁外析构和无歧义所有权转移，不是热路径
  性能方案。真实 runtime worker 不得在竞争中的 OS mutex 上阻塞。平台接入前
  必须采用 driver 线程串行命令、分 shard 所有权或等价的非阻塞投递边界，并用
  profiler/benchmark 选择；
- record 分配应接入 `ExecutorCoreResource`/专用 size-class pool；
- 真实 IOCP `OVERLAPPED`、epoll/kqueue readiness 重试和 shutdown 顺序仍需
  平台测试；
- Linux、Windows、macOS、TSan、LSan 和真实取消/关闭竞态证据尚未完成。

因此本切片不能表述为平台 I/O driver 完成，也不能形成性能结论。
