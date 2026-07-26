# OnceCell 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `tokio::sync::OnceCell<T>` 与
`SetError<T>`。当前覆盖：

- 空构造、带初值构造、`from`、`initialized`、`get`、`set`；
- `get_or_init` 与 `get_or_try_init` 的单初始化者、排队和失败重试；
- 初始化 task 取消、异常、runtime shutdown 后的许可转交；
- `get_mut`、`take`、`into_inner` 的 C++20 安全所有权映射；
- `Clone` 的值快照语义、相等比较和可读调试输出；
- `SetError` 的 already-initialized/initializing 分类和失败值返还。

本条目仍是“部分实现”。C++20 运行期工厂无法提供 Rust `const fn` 的静态初始化
能力；返回值采用 owning snapshot，而不是 Rust 借用引用；Tokio 的完整格式、
tracing、loom、三平台 sanitizer 和性能证据仍未闭环。

## 单初始化状态机

共享状态包含一个始终开放、许可数为 1 的 FIFO Semaphore、可空的 owning 值和
一个只保护短状态变更的同步互斥区：

1. 快路径先读取已发布值；存在时直接返回同一不可变 snapshot。
2. 空 cell 的异步调用竞争唯一 Semaphore 许可。只有获得许可的 task 才调用
   factory，其他 task 按 Semaphore FIFO 规则等待。
3. 获得许可后再次读取值；若前一位已发布，直接返回该值并转交许可，不再调用
   自己的 factory。
4. 唯一初始化者完整构造 owning 值，在状态锁内发布，然后归还许可。后续排队者
   依次获得许可、观察同一发布值并继续转交。
5. factory 返回错误、抛异常或 task 在发布前被销毁时，许可 guard 自动归还，
   队首等待者取得初始化权，cell 继续保持为空。

Semaphore 从不因初始化成功而关闭，也不在 `take` 后替换 generation。发布只
包含“构造完成 → 状态锁内存入 owning 值”，因此没有“值已可见但 close
失败”或 `take` 与旧 generation 的并发读写窗口。排队者拿到许可后必须复查值，
所以不会执行第二次 factory；私有 Semaphore 若意外关闭属于内部不变量错误。

`set` 使用同一许可状态机的非阻塞路径：

- 已有值时返回 `AlreadyInitializedError`；
- 其他 task 正持有初始化许可时返回 `InitializingError`；
- 成功取得许可后再次复查值；仍为空时同步发布，随后归还许可。

两类错误都保留调用方传入值的所有权，可经 `SetError::value` 或
`into_value` 取回。

## 取消、异常和关闭

`get_or_init` 与 `get_or_try_init` 在完成发布前是 cancel-safe：

- future 尚未首次 poll 就销毁时，factory 不执行，cell 不改变；
- 初始化者在 factory 的任一暂停点被 abort，许可随协程帧析构而归还；
- 排队等待者被取消时，从 Semaphore 队列注销，并按许可转交规则唤醒下一位；
- factory 抛异常时，异常进入 task 的 JoinError 路径，许可仍只归还一次；
- `get_or_try_init` 返回错误时不发布值，后续调用可以重新初始化；
- runtime shutdown 销毁 initializer 和 waiter 后，只要 OnceCell 句柄仍存活，
  可在新的 runtime 中重试。

一旦值已发布，在执行 `take` 前所有等待者取得该值。取消已经取得返回 snapshot
的调用不会撤销初始化。factory 递归初始化同一个 cell 会与 Tokio 一样形成逻辑
死锁，API 文档明确要求调用方避免这种自依赖。

## C++20 所有权映射

Rust `OnceCell::get` 和异步初始化 API 返回受借用规则约束的 `&T`。C++20 无法
证明引用不会逃逸到 cell 生命周期之外，因此 CIO 返回
`std::shared_ptr<const T>`：

- snapshot 拥有值的生命周期，不依赖 OnceCell 对象继续存活；
- snapshot 可安全跨协程暂停点和 worker 迁移；
- 只读类型阻止通过该句柄修改已发布值；
- 公开 API 不暴露裸指针或用裸引用表达异步所有权。

这是安全替代，不是源码级等价。它允许旧 snapshot 在 `take` 后继续观察旧版本。
因此：

- 对可复制 `T`，`take` 在仍有 snapshot 时复制返回值，不移动旧 snapshot 正在
  观察的对象；
- 对不可复制 `T`，仍有 snapshot 时 `take` 以 `logic_error` 拒绝；
- `get_mut` 返回不可复制、non-Send/non-Sync 的 `OnceCellMutGuard<T>`，而非
  可逃逸的 `T&`；
- guard 只允许同步 `update`、`replace` 和受约束的值复制，析构时重新发布值；
- `update` 编译期拒绝直接返回引用或指针，guard 不得跨越 `co_await`；C++20
  仍无法阻止 callback 通过副作用保存地址、span 或 view，调用方必须遵守不
  逃逸契约。

`get_mut`、`take` 和 `into_inner` 对应 Rust 的 `&mut self`/按值消费边界。C++
不能静态证明唯一借用，所以 CIO 在运行期要求内部状态唯一；存在异步操作、共享
cell 状态或不安全 snapshot 条件时明确拒绝，不放宽成潜在悬垂访问。
`into_inner() &&` 成功或在值构造中抛异常后都会令源对象进入明确的 moved-from
不可用状态；`take()` 则保留可再次初始化的 cell。

调用 `into_inner()` 时，调用方必须通过外部同步独占该具体 C++ 对象；不能一个
线程移动其 `state_`，另一个线程同时在同一个对象上调用 `get`、`set` 或异步
API。`use_count()` 只验证共享状态所有者，不会把并发 move/use 变成安全操作。

值的复制、移动和旧值析构全部在状态锁外执行，避免 `T` 的用户代码重入 cell 或
形成锁顺序反转。由于 C++ move/copy 可以抛异常，`take`/`into_inner` 在值构造
失败时提供基本异常保证：cell 已为空或源已消费，但内部状态保持有效且不持锁
传播用户异常。

## Clone、移动与线程安全

Tokio `OnceCell<T>::clone` 克隆当前值快照，而不是让两个 cell 共享同一初始化
过程。CIO 的复制构造、复制赋值和 `clone()` 保持这一语义：

- 空 cell 克隆为空 cell；
- 已初始化 cell 在 `T` 可复制时复制值；
- 克隆后的任一 cell 执行 `take`、`set` 或修改，不影响另一个 cell；
- 不可复制 `T` 的 OnceCell 禁止复制，但仍可移动。

移动后的源对象处于明确不可用状态，继续调用会抛出 `logic_error`。共享状态和
factory 对象均由异步操作按值拥有，CIO 不额外捕获调用方引用；但 factory 类型
内部仍可能自行捕获引用或 view，C++20 无法自动证明其 portable 安全性，必须
由 mobility trait 与人工审计共同约束。`OnceCell<T>` 的 CIO mobility 边界为：

- `Send<T>` 时 OnceCell 是 `Send`；
- `Send<T> && Sync<T>` 时 OnceCell 是 `Sync`；
- `OnceCellMutGuard<T>` 始终 non-Send/non-Sync。

portable 初始化 factory 的协程允许暂停后迁移 worker；API 不保证首次 poll 与
完成发生在同一线程。

## 阻塞行为

异步初始化竞争通过 Semaphore 挂起 task，不会在等待 factory 或初始化权时阻塞
runtime worker。值快照、发布和独占状态切换只在不执行用户代码、不包含
`co_await` 的短同步临界区内完成；用户 factory 始终在锁外运行。

OnceCell 不提供同步等待接口。同步代码可使用 `set`，但如果需要等待其他 task
完成初始化，必须由 runtime 驱动 `get_or_init`/`get_or_try_init`，不能忙等或
把无界阻塞工作放进 factory。factory 内的文件 I/O、DNS 或长时间 CPU 工作必须
显式桥接 `spawn_blocking`。

## 验证证据与剩余门槛

本地测试覆盖：

- 所有构造、快速读取、重复 `set` 错误分类和值返还；
- 64 个 multi-thread 竞争者只调用一次 factory；
- 2,000 轮同步 `set`/`take` 同起竞争的安全线性化和值守恒；
- 未 poll、活跃 initializer、排队 waiter 三类取消边界；
- factory 返回错误、同步抛出、异步抛出后的重试；
- runtime shutdown 后在新 runtime 重新初始化；
- owning snapshot、move-only 值、`get_mut`、`take`、`into_inner`；
- 20 次 runtime 创建/关闭并发初始化循环；
- Clone 独立性、相等与调试输出、Send/Sync 静态边界。

调试输出使用 C++ `operator<<` 构造可读映射。整数等基础类型已与当前 Rust
格式差分一致，但任意用户类型、字符串引号和转义不承诺逐字等同 Rust `Debug`；
兼容判定应聚焦结构和状态，不把通用文本格式误标为完全一致。

固定 Tokio 1.53.1 的 Rust/C++ 差分包含六项：

- `once_cell_single_initializer`；
- `once_cell_cancel_retry`；
- `once_cell_try_error_retry`；
- `once_cell_clone_independent`；
- `once_cell_debug_format`；
- `once_cell_set_error_format`。

当前 Windows/MSVC 主机的 Debug、Release 与 ASan 测试均已通过，OnceCell
Release 回归测试连续执行 100/100 无失败；Semaphore/Notify 生命周期修复后的
Release 与 ASan 全套均通过，当前源码的固定 Tokio 差分累计 51 项并重复通过
20/20。Windows ASan 不等同于 LeakSanitizer，这些结果也不能替代 Linux/macOS、
Clang/GCC、UBSan/TSan/LSan、模型检查和正式性能测试。

性能框架已加入 `once_cell_ready` 与 `once_cell_init`，分别覆盖已初始化 get
快路径和多个 task 竞争唯一异步 factory，并完成 Windows Release dirty smoke。
仍需补失败重试与取消风暴，固定 worker、竞争者、factory 成本和 CPU 亲和性，
记录吞吐、p50/p95/p99/p999、CPU、分配与调度唤醒。在正式 clean revision
测量前，不从 smoke 或功能测试推导性能结论。
