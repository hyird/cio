# I/O 拥有缓冲、端点 session 与组合精确读写

## 1. 范围

本切片先建立 Tokio 1.53.1 `AsyncRead`、`AsyncWrite` 与 `ReadBuf` 的安全
C++20 表达，不把尚未实现的 epoll、IOCP、kqueue 或 TCP 能力伪装成已完成。

首片覆盖：

- 统一 I/O 错误类型，并保留原生 `std::error_code`；
- 拥有式字节缓冲、只读共享租约和只读 buffer sequence；
- 只使用已初始化内存的 `ReadBuf` 与独占可变租约；
- coroutine-native 的单次 `read`、`write`、`write_vectored`、`flush` 和
  `shutdown`；
- 覆盖整个组合 future 的 `AsyncReadSession`/`AsyncWriteSession`；
- Tokio 语义的 `read_exact` 与 `write_all`；
- 可稳定制造部分完成、EOF 与 write-zero 的内存 reader/writer；
- Tokio 固定版本的差分语义与 Windows sanitizer 回归。

`copy`、buffered I/O、split、duplex、平台 driver 和网络对象属于后续切片。
它们不会以占位实现提前进入公开 API。

## 2. Tokio 契约与 C++20 安全映射

Tokio 的低层 trait 在一次同步 poll 中借用 reader/writer、task context 和
`ReadBuf`。CIO 的公开层以 coroutine 为主，不公开内部 coroutine address 或
waker：

- initiating function 同步取得 buffer 与端点的独占租约；
- 返回的 lazy `Task` 只保存拥有状态和 generation，不保存调用者引用；
- 未 poll 就析构、等待中取消或原端点句柄先析构，都会通过拥有租约安全清理；
- operation 完成前，调用者对同一 `ReadBuf` 的别名访问和第二个可变 operation
  会被拒绝；
- span 只用于不会暂停的同步复制或观察，绝不作为异步 operation 的唯一生命期
  依据。

`AsyncRead`/`AsyncWrite` concept 除函数形状外，还要求实现类型显式声明
`cio_async_read_endpoint`/`cio_async_write_endpoint` 审计标记。该 opt-in
表示 initiating primitive 会在返回 Task 前取得端点独占权，而且复制句柄必须
保持相同语义 identity；move-only 端点则必须整体转移。C++20 concept 无法机械
证明这些运行期语义，所以未标记的同形类型会被拒绝，第三方端点仍须通过生命周期、
并发 initiating 和取消测试后才能 opt-in。

`ReadBuf` 本身使用原子 `idle -> owner -> idle` 与
`idle -> lease -> idle` 三态 gate。同步 owner 访问和异步 lease 取得都用
strong CAS 线性化，竞争时明确拒绝；lease 数据路径与析构释放不等待
`std::mutex`，也不依赖可能无竞争伪失败的 `try_lock`。lease 的 release store
向下一次 owner acquire 发布 `bytes`/`filled` 修改，跨线程测试由另一线程实际
读取已发布字节。

组合操作不会在每个 partial primitive 完成后释放端点排他权。端点必须先创建
move-only `AsyncReadSession`/`AsyncWriteSession`，session 的 audited marker
承诺每个 primitive Task 强拥有共享 control 与 buffer lease，绝不借用可移动
session wrapper 的地址。`read_exact`/`write_all` 把 session 放在稳定的
`unique_ptr` owner 中，并保证 child Task 先于 session 析构。C++20 concept
不能证明第三方没有虚假声明 marker，因此该能力仍须靠中文契约、静态检查和
第三方端点生命周期测试共同审核。

这保留了 Tokio `&mut` 的独占能力与可观察行为，但不是 Rust 源码级接口复制。

## 3. ReadBuf

首片的物理存储在创建时全部初始化为零，因此逻辑上
`initialized == capacity`。它仍维护：

```text
0 <= filled <= initialized <= capacity
```

`clear` 只把 `filled` 归零，不修改已初始化长度和原字节。`advance`、
`set_filled` 与 `put_slice` 在越界时必须保持原状态不变。公开观察返回拥有式
快照，避免把锁外 span 或引用泄漏给异步调用者。

Tokio `ReadBuf::uninit`、`unsafe inner_mut/unfilled_mut/assume_init` 暂未实现。
后续只能以安全的 writable region 与显式 commit 能力替代，不能把 Rust 的
`unsafe` 裸内存接口原样移植到 CIO。

## 4. 单次 I/O 语义

### 4.1 read

- 一次调用最多执行一次底层读取尝试；
- 成功读取量由 `filled` 的增量确定；
- 增量为零表示 EOF 或目标剩余容量为零，不能只凭零区分两者；
- error 或 Pending 在交付前不得改变目标 buffer；
- 取消未交付的 operation 后，buffer 租约必须释放且不得出现晚到写入。

### 4.2 write

- 成功量满足 `0 <= n <= 输入长度`，允许部分写；
- 非空输入成功返回零由单次 `write` 原样表达，后续 `write_all` 才负责映射
  `WriteZero`；
- error 或 Pending 在交付前不得提交字节；
- 默认 vectored write 只选择首个非空 segment；声称原生 vectored 支持前必须
  实现单次原子 scatter/gather 提交。

### 4.3 flush 与 shutdown

`flush` 成功表示内部缓冲已经到达目的地。`shutdown` 成功必须隐含一次成功
flush，成功后 writer 进入终态；后续 write 返回保留原生信息的
`broken_pipe` 类错误。Tokio 没有承诺 shutdown 完全 cancel-safe，因此 CIO
不得扩大承诺。

### 4.4 read_exact 与 write_all

`read_exact` 在 initiating call 同步取得目标 buffer lease 和 read session。
成功时返回目标容量；底层若在目标尚未填满时不再增加 `filled`，映射为
`UnexpectedEof`。`write_all` 保持同一个 write session，对剩余 suffix 循环
写入；非空 suffix 返回零映射为 `WriteZero`。两者均保留已提交的部分进度，
均不额外消耗 cooperative budget，也不宣称 cancel-safe。

空输入仍取得组合 operation 的 session 与 buffer lease，以保持创建后独占契约，
但不会创建或 poll 底层 primitive。`write_all` 不隐式 flush 或 shutdown。
底层原生错误保留 `std::error_code`，不能被 `UnexpectedEof`/`WriteZero`
覆盖。

## 5. 内存端点的测试角色

内存端点不是平台 driver 的替代物。它们用于在不引入 syscall 差异时确定性验证：

- EOF、零容量、部分读写和 write-zero；
- initiating-call 独占、未 poll 析构和 move 后状态；
- endpoint 原句柄先析构时 operation 仍安全；
- 默认 vectored 选择、flush/shutdown 顺序和终态；
- buffer/endpoint 锁外的复制、析构与错误构造；
- 两线程反复竞争 write/shutdown initiating 时恰好一个取得 operation，未
  poll 析构后不提交字节、flush 或 shutdown。
- 长期 session 跨多个 partial primitive 时，另一个线程的别名 operation 始终
  被拒绝；
- read/write child Pending 后取消只释放 primitive guard，不提前释放外层
  session，随后同 session 仍可继续；
- session wrapper 移动、原端点 handle 先析构、空组合输入和 late wake 均保持
  control 与 lease 生命周期。

内存端点通过 `max_chunk` 固定每次最大进度，测试和 benchmark 必须校验总字节、
校验和、调用次数与 flush 次数，不能让不同 runtime 偷减工作量。
MemoryWriter 的 observer 只在短临界区取得不可变字节版本，大块 snapshot 复制
在锁外完成；观察点已发现活动 operation 时会按端点独占契约拒绝，先取得版本的
observer 则线性化在 operation 之前，并依靠不可变版本安全完成复制。

当前 Windows/MSVC 实测证据：

- 内存端点专项为 14/14，精确读写专项为 4/4，Release、Debug、MSVC ASan
  均通过；
- 当前全套 CTest 为 Release/Debug 18/18、ASan 19/19；
- I/O 共 15 项 Tokio 1.53.1 差分，当前全套 Release/Debug 148/148；
- 内存端点通过 Release 300/300、MSVC ASan 100/100；精确读写通过
  Release 100/100、Debug/MSVC ASan 20/20；
- 精确读写另验证 4-worker partial 后外部线程 wake 的真实 worker 迁移、
  native error 进度，以及外部 abort 后 Session/ReadBuf/input lease 释放；
- 核心禁止裸指针契约扫描当前覆盖 64 个文件。

这些证据覆盖拥有缓冲和内存端点，不得外推成平台 I/O driver、TCP 或三平台已经
完成。

## 6. 后续平台边界

平台中立 `OperationKey`/Registry 已完成原型，状态机为
`created -> submitting -> submitted -> completing/cancelling -> delivered`。
进入 OS API 前由唯一 submission guard 封住 rollback，支持即时 completion、
同步拒绝回滚和 cancel-during-submitting；8 KiB CopyBuffer 使用逻辑 parent
tail、Registry native pin 与非 owning completion authority 分离 buffer 生命周期。
真实平台接入仍须完成：

- epoll/kqueue readiness 后重试 syscall，`EAGAIN` 回到 Pending；
- IOCP 的 `OVERLAPPED` 与 buffer lease 必须活到 late completion 被消费，
  `CancelIoEx` 返回不代表可以提前释放；
- driver 只保存 generation key 和弱拥有状态，不保存 coroutine address；
- runtime shutdown 必须先停止提交、取消并 drain operation，再销毁 driver、
  executor 与资源。

在上述真实后端、Linux/Windows/macOS 测试和 sanitizer 门槛完成前，兼容矩阵
保持“部分实现”。

## 7. 性能证据计划

后续性能对照分开记录：

- `io_memory_ready`：reader/writer 立即完成，比较不同 buffer 与 max-chunk；
- `io_memory_pending`：每个 chunk 恰好一次 Pending/wake，比较调度与组合成本；
- vectored segment 数为 1、4、16、64，并包含空 segment；
- 记录 p50/p95/p99/p999、吞吐、CPU、分配、RSS、context switch、read/write/
  flush/wake 次数。

Asio 只有在 buffer state、associated executor/allocator/cancellation 均使用
拥有式实现且工作量等价时才进入表格，否则明确 skip。任何 dirty smoke 只验证
链路，不作为正式性能结论。

当前 `io_memory_ready` dirty smoke 固定每次 read 为 64 B，在 CPU 0–3、
2 次预热/5 样本下：

- 1 worker：CIO/Tokio p50 为 61.090/4.538 ms，耗时比 13.463x；
- 4 worker：CIO/Tokio p50 为 45.094/1.899 ms，耗时比 23.747x；
- 每行均校验 100000 次 read、6400000 bytes 和相同逐字节 checksum；
- 1/4-worker CIO RSD 为 0.28%/1.86%，Asio 按上述边界明确 skip。

这些数字来自 dirty revision，只能说明当前拥有 endpoint、operation、buffer
快照与调度组合路径需要 profiler；不能仅凭架构推断把差距归因于某一次分配或
复制，更不能外推到真实 socket 后端。
