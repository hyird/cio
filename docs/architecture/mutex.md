# Mutex 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `Mutex<T>`、`MutexGuard<T>`、
`OwnedMutexGuard<T>`、`MappedMutexGuard<T>`、`OwnedMappedMutexGuard<T, U>`
和 `TryLockError`。核心可观察契约如下：

- `lock` 与 `lock_owned` 按首次轮询顺序严格 FIFO；
- 取消尚未取得锁的 future 会失去队列位置，锁许可必须转交下一等待者；
- 已经取得锁但尚未恢复的 future 被取消时，同样必须释放并转交锁；
- guard 可安全跨协程暂停点和 worker 迁移，析构、异常和 task abort 均只解锁一次；
- `try_lock` 在锁已持有或已有公平队列时返回 `TryLockError`；
- 持锁 task 抛出异常不会 poison，清理后后续 task 仍可获取；
- `blocking_lock` 在异步执行上下文中拒绝调用，在普通或 blocking worker 线程上
  按同一 FIFO 状态机等待；
- mapped guard 仍独占整把锁，销毁任意层映射 guard 都只解锁一次。

## 所有权映射

Tokio 的 `MutexGuard` 借用 `&Mutex<T>`，而 `OwnedMutexGuard` 保存
`Arc<Mutex<T>>`。CIO 禁止异步状态保存裸引用，因此 `Mutex<T>` 是可复制的共享值
句柄，复制等价于复制 `Arc<Mutex<T>>`；`lock` 与 `lock_owned` 都返回拥有共享状态
的同一种 `MutexGuard<T>`，`OwnedMutexGuard<T>` 是其别名。

guard 的 `mutex()` 返回新的 `Mutex<T>` 值句柄，不返回可能悬空的引用。
`get_mut()` 和 `into_inner()` 都先要求共享状态唯一，否则抛出
`logic_error`。`into_inner()` 消费锁并移出值。`get_mut()` 不直接返回
`T&`：C++ 无法像 Rust `&mut self` 一样维持返回引用的独占期，因此 CIO
立即取得唯一许可并返回 `MutexGuard<T>`。guard 存活时即使随后复制锁句柄，
其他访问也不能取得许可。这是 C++20 的安全替代。

`const_new` 受 C++20 `std::mutex` 和共享控制块不能常量求值的限制，只保留为
普通运行期兼容工厂；兼容矩阵必须明确该语言差异。

## FIFO 与取消

Mutex 的锁许可直接复用已经验证的 `Semaphore(1)` 公平状态机：

1. `Lock` awaitable 同时拥有 Mutex 共享状态与 semaphore 获取 future；
2. 获取 future 首次轮询时进入 FIFO 队列；
3. 成功恢复后，semaphore permit 被移动到 Mutex guard；
4. 等待 future 取消时由 semaphore 状态机删除队列节点；
5. 已满足但尚未恢复时取消，permit 自动转交下一等待者；
6. guard 析构归还唯一 permit，后续等待者按 FIFO 唤醒。

因此 Mutex 不重复实现另一套 waiter/waker 状态机，Semaphore 的取消、跨线程
wake、无丢失唤醒和内存同步证据可以组合复用。数据只允许由持有 guard 的代码
访问；permit 释放和下一次获取在同一个状态锁上建立 happens-before。

## 稳定别名 mapped guard

Tokio 的 mapped guard 在内部保存指向子对象的裸指针。CIO 不声明、接收、返回
或以裸指针表达所有权；`MappedMutexGuard<T, U>` 保存：

- 根 Mutex 共享状态；
- 唯一 semaphore permit；
- 一个与根状态共用控制块的 `std::shared_ptr<U>` 别名视图。

projection 只在映射调用中同步执行一次，随后由标准 `shared_ptr` aliasing
constructor 把所选子对象绑定到根状态控制块。guard 不保存用户 callable，
后续访问和跨 worker 移动都不会重复 projection。projection 必须返回由当前受
保护对象拥有、且至少存活到根对象析构的子对象左值；C++20 无法像 Rust 一样
静态证明返回引用的来源，因此这是公开 API 的显式前置条件。

Tokio 的 `try_map` 用 `Option<&mut U>` 同时表达条件和子对象地址；该表示在 CIO
公开 API 中会引入 `reference_wrapper` 或裸指针。CIO 改为两个同步 callable：

1. predicate 对当前对象同步判断是否可映射；
2. projection 在成功时返回稳定的子对象引用。

失败分支返回原 guard，能力、锁所有权和解锁语义保持一致。aliasing constructor
所需子对象地址只作为标准库构造调用的同步临时实参，不声明裸指针变量、不进入
公开 API，也不表达所有权；所有权始终由根状态控制块维持。

为遵守公开 API 禁止裸指针，guard 不提供 C++ `operator->`；使用 `operator*` 或
`get()` 访问。引用不得在 guard 释放后保存或捕获进异步工作。

`MutexGuard` 和 `MappedMutexGuard` 可以在 `T` 满足约束时移动到其他 worker，
但明确不是 `Sync`。Rust 的 `Arc` 只能通过共享引用只读访问 guard；C++ 的
`shared_ptr<Guard>` 即使自身为 const 仍可得到可变 `Guard&`。若把写 guard
标为 `Sync`，两个 task 可同时调用可变 `get()`，因此 CIO 必须保守拒绝共享。

## blocking lock

`blocking_lock` 先拒绝 CIO 异步执行上下文，然后在调用线程上用临时
current-thread runtime 驱动同一个 owned `lock` future。其他线程释放 guard 时
通过保存的执行上下文唤醒该临时 runtime，因此没有第二套不同公平性的条件变量
队列。该 API 会阻塞调用线程，必须放在普通同步线程或 `spawn_blocking` 中。

## 验证要求

- 立即锁定、try 失败、guard 读写、移动和析构；
- 三个以上 waiter 的严格 FIFO 与重新排队；
- 排队取消、已满足未恢复取消、异常不 poison；
- guard 跨 `co_await`、跨 worker 迁移和高竞争计数；
- blocking lock 的真实跨线程等待及异步上下文拒绝；
- map、嵌套 map、try_map 成功/失败和失败后原 guard 保持；
- unique `get_mut/into_inner` 与共享句柄拒绝；
- 固定 Tokio 1.53.1 的 FIFO、取消、非 poison、owned guard 与映射差分；
- Debug、Release、ASan、TSan、重复压力和三平台动态证据。
