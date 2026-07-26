# Semaphore 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `Semaphore`、`SemaphorePermit`、
`OwnedSemaphorePermit`、`AcquireError` 和 `TryAcquireError`。可观察契约如下：

- 许可按首次轮询的先后顺序进入 FIFO 队列；
- 队首 `acquire_many` 可以部分接收许可，并阻塞队尾较小请求，避免大请求饥饿；
- 取消等待会失去队列位置，已经分配给该等待的部分或全部许可必须转交下一等待者，
  没有等待者时才回到可用计数；
- `close` 幂等地阻止所有后续获取，并唤醒全部等待者返回 `AcquireError`；
- `add_permits`、`forget_permits`、`available_permits` 和 `is_closed` 在同一状态锁下
  建立全序，释放许可与后续成功获取之间形成同步关系；
- permit 析构归还一次且仅一次，移动、异常、取消、`forget`、`split` 和 `merge`
  都不得泄漏或重复归还许可。

## C++20 所有权映射

Tokio 的借用 permit 把 `&Semaphore` 保存在 future 和 guard 中。CIO 禁止让异步
工作保存裸引用，因此 `Semaphore` 是可复制的共享值句柄，复制等价于
`Arc<Semaphore>`；`acquire` 和 `acquire_owned` 都返回拥有共享状态的
`SemaphorePermit`。`OwnedSemaphorePermit` 是该安全 guard 的别名，两组 API
能力和可观察行为相同，但不暴露 Rust 生命周期参数。

同理，`OwnedSemaphorePermit::semaphore` 在 CIO 中返回一个 `Semaphore` 值句柄，
而不是可能悬空的引用。等待操作、队列节点和执行上下文分别由
`shared_ptr`/`weak_ptr` 管理；队列使用单调 waiter ID 删除取消项，不以对象地址
表达身份。

Tokio 的 `const_new` 能在 Rust 静态存储中原位构造含内部同步状态的对象。
C++20 的 `std::mutex` 与共享所有权控制块无法在这里提供等价的常量求值构造，
CIO 保留同名工厂以便迁移，但它执行普通运行期构造；兼容矩阵必须一直明确这一
语言差异。

## 状态机

每个获取操作保存 `requested`、`remaining`、队列状态、完成状态和可选执行上下文。
共享状态保存 `available`、`closed` 与 FIFO waiter 队列。

1. 首次轮询先检查关闭。零许可请求在未关闭时立即成功。
2. 队列为空且许可足够时直接扣减并完成。
3. 许可不足时先把当前全部可用许可分给本请求，再把剩余需求排到队尾。
4. 释放许可只处理队首：需求满足后移出并唤醒；不足时停在队首。
5. 完成但尚未恢复的请求仍拥有已分配许可。此时取消必须把许可重新分配。
6. `close` 只标记错误并唤醒，不凭空丢弃等待者已部分获得的许可；等待 future
   恢复或销毁时再归还，与 Tokio 的 drop 行为一致。

所有 task 唤醒均在释放状态锁后执行，避免把调度器重入同步状态机。状态锁只保护
短小的内存操作，不执行用户代码、系统 I/O 或无界阻塞工作。

队列只保存 operation 的 `weak_ptr`，因此在持状态锁扫描队列时，
`weak_ptr::lock()` 得到的临时强引用可能恰好成为最后一个 owner。若它在锁内
析构，operation 析构会调用 `cancel()` 并递归获取同一非递归 mutex。CIO 对每次
扫描采用锁外声明的 operation keep-alive 批次：在提升前一次性预留容量，所有
成功提升结果立即转入批次，直到状态锁释放后才允许析构。该规则覆盖许可分发、
溢出预检、close、失败结果消费和取消转交，不能只在某个 RwLock 调用点绕开
竞态。

## 错误与容量

`MAX_PERMITS` 为 `size_t::max() >> 3`。构造、批量获取和显式增加超过上限时，
CIO 使用 C++ 异常表达 Tokio panic 边界。permit 析构是 `noexcept`；若用户先
显式增加许可，随后 guard 析构导致超过硬上限，无法安全传播异常，CIO 按内部
不可恢复契约调用 `terminate`。

## 验证要求

- 立即获取、零许可、try 错误、关闭与关闭后增加；
- FIFO、批量请求头阻塞、部分分配；
- 排队取消、部分分配取消、已满足但尚未恢复时取消；
- permit 的移动、析构、`forget`、`split`、同源/异源 `merge`；
- current-thread 与 multi-thread 跨线程释放、关闭和高竞争；
- release/abort 同时发生时，最后 operation owner 不得在状态锁内析构或递归
  cancel；
- 固定 Tokio 1.53.1 的 FIFO/头阻塞、取消转交、关闭和 permit 操作差分。

本地曾在 Windows/MSVC Release 的 RwLock 压力测试中捕获
`0xc0000409`。WER/minidump 证明它是上述递归锁导致的
`resource deadlock would occur`，不是栈破坏。修复后原 CTest 复现管道在
Release 连续通过 500/500，MSVC ASan 同样通过 500/500；两种配置的全套测试
也均通过。此证据仍需 Linux TSan/ASan/UBSan 和 macOS 动态验证补齐。
