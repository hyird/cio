# RwLock 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `RwLock<T>`、读写 guard、owned guard、mapped
guard 和 `TryLockError`。覆盖的构造与操作包括：

- 映射 `new` 的值构造函数、`with_max_readers`、`const_new`、
  `const_with_max_readers`；
- `read`、`read_owned`、`try_read`、`try_read_owned`、`blocking_read`；
- `write`、`write_owned`、`try_write`、`try_write_owned`、`blocking_write`；
- `get_mut`、`into_inner`；
- 读 guard 的 `map`、`try_map`；
- 写 guard 的 `map`、`try_map`、`into_mapped`、`downgrade`、
  `downgrade_map` 和 `try_downgrade_map`。

核心可观察契约如下：

- 同一时刻允许不超过 `max_readers` 个读者，或者恰好一个写者；
- 等待者按首次轮询顺序进入 FIFO 队列，队首写者阻止后续读者插队，避免写者
  饥饿；
- 取消等待会失去队列位置，已经为该等待预留的部分或全部许可必须转交；
- guard 析构、异常展开、task abort 和移动赋值均只释放一次访问权；
- 持锁 task 抛出异常不会 poison，后续读写者仍能正常取得锁；
- 写 guard 降级为读 guard 的过程是原子的，排队写者不能在中间取得排他访问；
- blocking 操作复用同一公平队列，并在 CIO 异步执行上下文中拒绝调用；
- mapped guard 继续持有整把读锁或写锁，任意层映射都不保存子对象裸地址。

## Tokio batch semaphore 状态模型

Tokio 1.53.1 的 `RwLock` 使用公平的 batch semaphore。CIO 采用相同能力模型：

- 共享状态持有 `Semaphore(max_readers)`、`max_readers` 和受保护的 `T`；
- 每个读请求获取一个许可；
- 每个写请求以一次 `acquire_many(max_readers)` 获取全部许可；
- 默认 `max_readers` 与 Tokio 一致，为 `uint32_t::max() >> 3`；
- `with_max_readers` 拒绝零值和超过该上限的值。

Semaphore 按首次轮询顺序把请求放进同一 FIFO 队列。写请求到达队首时，即使仍有
活跃读者，它也可以逐步预留已经释放的许可；只要尚未集齐全部许可，后续读请求
就不能绕过该批量请求。这同时给出 FIFO 和写者优先语义，而不是另加一套可能与
Semaphore 公平性分叉的读写等待队列。

RwLock 的内部 Semaphore 不向用户暴露 `close`，因此正常读写等待不存在
`AcquireError`。实现仍在不可能的关闭分支保留内部不变量检查，不能把异常状态
伪装成成功。

所有唤醒都在释放同步状态锁之后提交给执行上下文。状态锁只保护有界的队列和
计数更新，不执行用户代码、不做系统 I/O，也不在 runtime worker 上等待竞争中的
操作系统锁。

## C++20 所有权与生命周期映射

Tokio 的 `read`/`write` guard 借用 `&RwLock<T>`，owned 版本保存
`Arc<RwLock<T>>`。CIO 禁止可能跨暂停点的异步状态保存裸引用，因此
`RwLock<T>` 是可复制的共享值句柄，复制等价于复制 `Arc<RwLock<T>>`：

- `read` 与 `read_owned` 返回同一种拥有状态的读 guard；
- `write` 与 `write_owned` 返回同一种拥有状态的写 guard；
- `OwnedRwLockReadGuard`、`OwnedRwLockWriteGuard` 和
  `OwnedRwLockMappedWriteGuard` 是对应安全 guard 的别名；
- guard 的 `rwlock()` 返回新的 `RwLock<T>` 值句柄，而不是可能悬空的引用；
- 等待 future 自身也拥有锁状态，调用方句柄提前析构不会造成悬空。

`get_mut()` 和右值限定的 `into_inner()` 都先要求共享状态唯一；存在复制句柄、
guard 或等待操作时会抛出 `logic_error`。`into_inner()` 随后消费锁并移出值。
`get_mut()` 不直接返回 `T&`：C++ 无法像 Rust `&mut self` 一样禁止调用者在
返回引用后复制锁并发访问，因此 CIO 立即取得全部 Semaphore 许可并返回
`RwLockWriteGuard<T>`。该 guard 的整个生命周期都维持独占性，即使随后复制锁
句柄，其他读写也必须等待。这是能力相同但适合 C++20 所有权模型的安全替代。

`const_new` 与 `const_with_max_readers` 受 C++20 `std::mutex` 和共享控制块无法
常量求值的限制，只是运行期兼容工厂。无效的 `max_readers` 使用
`invalid_argument` 表达 Tokio 的 panic 边界。兼容矩阵必须持续标明这些语言
映射，不能把同名误写成常量求值等价。

公开 guard 不提供会暴露裸指针的 `operator->`，调用方使用 `operator*` 或
`get()` 同步访问。取得的引用受 guard 生命周期约束，不能在 guard 释放后保存，
也不能捕获进可能超过当前同步作用域的异步工作。

## 公平性、取消与非 poison

读写 awaitable 直接组合 Semaphore 的已验证取消状态机：

1. 首次轮询时，读请求以一个许可、写请求以 `max_readers` 个许可加入 FIFO；
2. 队首批量写请求可以部分接收许可，并阻止后续读请求插队；
3. 等待中的请求被取消时，从队列删除并把已预留许可继续分配给队首；
4. 已满足但尚未恢复的请求被取消时，同样归还其全部许可；
5. 读 guard 持有一个许可，写 guard 持有全部许可；
6. guard 析构归还许可，并与后续成功获取在同一状态机上建立同步关系。

因此取消是安全的，但与 Tokio 一样会丢失原有队列位置。取消不会丢许可，也不会
使后续读者越过更早排队的写者。`try_read` 和 `try_write` 只在无需绕过已有公平
队列且许可立即充足时成功。

写者优先还保留 Tokio 的一个重要可观察边界：若同一 task 已持有读 guard，另一
写请求在其后排队，该 task 再次等待读锁时，新读请求不能越过写者；在释放原读
guard 前会形成自等待。CIO 不用“可重入读锁”绕过该 FIFO 契约。

RwLock 不实现 poison。用户代码抛出异常时，异常存入 task 的 join 结果，guard
沿协程帧清理路径析构；锁状态只恢复为可获取，不记录 poison 标志。

## 稳定别名 read/write mapped guard

Tokio 的 mapped guard 在内部保存子对象地址。CIO 不声明、接收、返回或用裸指针
表达该关系；读写映射统一保存：

- 根 `RwLock<T>` 的共享状态；
- 一个读许可或写者持有的全部许可；
- 一个与根状态共用控制块的 `std::shared_ptr<U>` 别名视图。

读映射使用 `const U&(const T&)`，写映射使用 `U&(T&)`。projection 只在
`map`/`try_map`/`downgrade_map` 的同步调用中执行一次；实现随即用标准
`shared_ptr` aliasing constructor 把选中的子对象绑定到根状态控制块。guard
不保存用户 callable，后续访问不会重复求值，因此依赖全局状态或
`thread_local` 的 callable 也不会在 worker 迁移后改选另一个子对象。

projection 必须返回由当前受保护对象拥有、且至少存活到根对象析构的子对象
左值；C++20 无法像 Rust 生命周期系统一样静态证明返回引用的来源，所以这是
公开 API 的显式前置条件。Tokio 的 `try_map`/`try_downgrade_map` 用
`Option<&U>` 或 `Option<&mut U>` 同时表达条件和地址，CIO 为避免在公开 API
使用裸指针与 `reference_wrapper`，改成两个同步 callable：

1. predicate 判断当前对象是否可以映射；
2. projection 在成功分支返回受保护对象拥有的子对象左值。

失败分支返回原 guard，原锁访问权保持不变。写映射继续独占根对象；读映射仍允许
其他读者，但禁止写者进入。aliasing constructor 所需的子对象地址只作为标准库
构造调用的同步临时实参，不声明裸指针变量、不进入公开 API，也不表达所有权；
实际所有权始终由根状态的 `shared_ptr` 控制块维持。

## 原子降级

写 guard 持有 `max_readers` 个许可。`downgrade` 和 `downgrade_map` 先从该 guard
中拆出一个许可并构造读 guard，再释放其余 `max_readers - 1` 个许可。整个转换
期间始终至少保留一个许可，所以：

- 其他写者不可能在写访问和新读访问之间插入；
- 已排队写者仍位于后续读者之前；
- 被释放的许可可以按 FIFO 分配，但写者必须等新读 guard 释放最后一个许可；
- `try_downgrade_map` 的 predicate 失败时返回原写 guard，不发生降级。

`into_mapped` 只把根写 guard 转换成根对象的 mapped write 表达，不释放任何
许可，也不改变排他性。

## blocking bridge

`blocking_read` 与 `blocking_write` 先检查当前线程是否位于 CIO 异步执行上下文。
若是则抛出 `logic_error`；若不是，则在调用线程上以临时 current-thread runtime
驱动同一个 owned `read`/`write` awaitable。

因此 blocking 操作与异步操作共享完全相同的 FIFO、批量许可和取消状态机，没有
第二套条件变量队列。其他线程释放 guard 后会通过等待操作保存的执行上下文唤醒
临时 runtime。该 API 会阻塞调用线程，只能用于普通同步线程或
`spawn_blocking`；不得在 runtime worker 内直接调用。

## `Send`/`Sync` 边界

CIO 的 guard 全部拥有共享状态，因此按 Tokio owned guard 的安全边界保守映射：

- `RwLock<T>` 在 `T` 满足 `Send` 时满足 `Send`，在 `T` 同时满足 `Send` 和
  `Sync` 时满足 `Sync`；
- 根读 guard 和根写 guard 要求 `T` 同时满足 `Send`、`Sync` 才能跨 worker；
- mapped read guard 还要求视图 `U` 满足相应的 `Sync`，共享 guard 本身时要求
  `U` 同时满足 `Send`、`Sync`；
- mapped write guard 要求根对象和映射对象都满足 `Send`、`Sync` 才能移动到
  另一个 worker；
- 写 guard 与 mapped write guard 明确为 `Send` 但非 `Sync`。Rust 可通过
  `&Guard` 与 `&mut Guard` 区分只读和可变解引用；C++ 的
  `shared_ptr<Guard>` 即使自身为 const 仍可取得可变 `Guard&`，所以将写 guard
  标为 `Sync` 会允许两个 task 同时调用可变 `get()`，必须保守拒绝。

满足 `Send` 的 owned guard 可以跨 `co_await` 和 worker 迁移；只有读 guard
可以在满足 `Sync` 时被并发共享。portable spawn 边界拒绝不满足要求的类型，
调用方必须留在 local task。C++20 无法像 Rust 一样检查完整协程帧，这些 trait、
owned factory、静态检查和 debug affinity 检查共同构成安全边界，不能声称等同
于 Rust 的完整编译期证明。

## 验证要求与当前边界

该垂直切片必须验证：

- 构造上限、默认构造、共享读、排他写和所有 try 操作；
- `max_readers` 并发边界、严格 FIFO、写者优先以及后续读者不插队；
- 排队取消、部分取得许可的写者取消、已满足未恢复取消和许可转交；
- guard 移动、析构、异常非 poison、跨暂停点和跨 worker 迁移；
- read/write map、嵌套 map、try_map 成功/失败和失败后原 guard 保持；
- `downgrade`、`downgrade_map`、`try_downgrade_map` 的原子性；
- blocking read/write 的真实跨线程等待、异步上下文拒绝和
  `spawn_blocking` 桥接；
- unique `get_mut/into_inner` 与共享状态拒绝；
- 固定 Tokio 1.53.1 的共享读/最大读者、写者优先 FIFO、部分写者取消、
  非 poison、owned mapping 和原子 downgrade 六项差分；
- Debug、Release、ASan、UBSan、TSan、重复压力以及 Linux、Windows、macOS
  动态证据。

性能验收还必须在固定硬件、编译器、worker 数、亲和性和样本数下，对比 CIO、
Tokio 与 Asio 的读密集、写密集、读写混合、队首写者阻塞、跨线程释放和高竞争
负载，同时记录吞吐、p50/p95/p99/p999、CPU、分配、上下文切换和公平性；单次
smoke 结果不得写成性能结论。

当前 Windows/MSVC 主机已实际通过固定 Tokio 1.53.1 的累计 41 项差分，其中上述
RwLock 六项均通过；RwLock 回归测试另以 `repeat-until-fail` 连续执行 20/20
无失败。该证据不替代 Linux/macOS、TSan/LSan、足量性能样本和三平台动态验证。
