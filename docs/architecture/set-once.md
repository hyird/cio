# SetOnce 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `tokio::sync::SetOnce<T>` 与
`SetOnceError<T>`。SetOnce 是只能成功写入一次、并允许异步等待该值的事件型
同步原语；它与执行异步 factory 的 OnceCell 不同，值必须由显式 `set` 提供。

当前覆盖：

- 空构造、可选初值构造、`from`、`initialized`、`get`；
- 恰好一次成功的并发 `set` 和失败值完整返还；
- `wait` 的立即完成、wait-before-set、wake-before-wait 和取消安全；
- `into_inner` 的 C++20 owning snapshot 安全映射；
- `Default`、深复制 Clone、相等、Debug 与 SetOnceError Display。

本条目保持“部分实现”。C++20 的互斥状态和共享控制块不能提供 Rust `const fn`
静态初始化；`get`/`wait` 返回 owning snapshot 而非借用引用；tracing、loom、
Linux/macOS 动态验证和正式性能证据仍待补齐。

## 单次发布状态

内部状态包含可空 owning 值、只保护短同步状态变更的 mutex，以及一个 Notify：

1. `set` 先检查已发布快路径；确定失败时直接返还错误值，不分配候选控制块。
2. 空状态调用在锁外完整构造候选 owning 值，避免在内部锁内执行 `T` 的用户
   代码。
3. 在锁内再次检查发布状态；第一个调用发布候选值，并发 loser 失败。
4. 释放状态锁后执行 `notify_waiters()`，所有在发布前创建的等待操作都被唤醒。
5. 被唤醒 task 再取得 owning snapshot，必须观察到完整发布值。

锁内的唯一成功点保证多个线程并发调用 `set` 时恰有一个 winner。每个 loser
收到 `SetOnceError<T>` 并取回自己未发布的值；winner 与全部 loser 的值都只
析构一次。CIO 不用 CAS 结果虚构“无锁”性质，也不把这个短同步临界区描述成
异步 Mutex。

发布值发生在通知之前，mutex 解锁构成同步边界。等待者只能在发布后的通知路径
恢复，因此对 `T` 的完整初始化可见。Notify 唤醒发生在状态锁外，避免恢复 task
时重入 SetOnce 状态锁。

## wait 与无丢失唤醒

`SetOnce::wait()` 返回拥有式 `Wait`，创建时同时创建 Notify 的 generation
snapshot。awaiter 依次执行：

1. 注册前检查当前值；已有值则立即完成。
2. 若仍为空，启用预先创建的 Notified 操作。
3. suspend 前再次检查当前值；若 `set` 已发布则不挂起。
4. 收到通知后读取 owning snapshot；通知已发生却没有值属于内部不变量错误。

这个顺序覆盖所有竞态：

- `set` 发生在 `wait()` 前：首次值检查立即完成；
- `set` 发生在 Wait 创建后、首次 poll 前：Notify snapshot 收到广播，或值检查
  直接完成；
- `set` 发生在注册检查与 suspend 之间：第二次检查或已登记通知阻止丢 wake；
- waiter 已挂起：`notify_waiters` 唤醒它。

同一个 Wait 只允许等待一次。`operator co_await` 会把共享状态和 Notified
operation 一并移入 move-only Awaiter，使原 Wait 立即进入 moved-from 状态；
重复等待以 `logic_error` 拒绝。取消 task 时 Awaiter 成为通知操作的唯一 owner，
会立即注销 waiter，不会因为调用方仍保留原 Wait 而滞留登记或 execution
context。Wait 与 Awaiter 都是 Send/non-Sync，禁止并发 poll 同一操作。

## 取消、析构和 runtime shutdown

Tokio 1.53.1 明确规定 `SetOnce::wait` cancel-safe，CIO 保持这一语义：

- 未首次 poll 的 Wait 析构没有可观察副作用；
- pending waiter 被 abort 时只注销自己的 Notify 等待状态；
- 已收到通知但尚未恢复的 waiter 被取消，不会撤销已发布值，也不影响其他
  waiter；
- 取消不消费 SetOnce 事件，随后可重新 `wait`；
- runtime shutdown 销毁等待 task 后，SetOnce 本身仍可由同步线程 `set`，也可
  在新 runtime 中重新等待。

`set` 是同步且线性化的操作，不存在“部分 set 被取消”。成功返回表示值已发布且
广播已发出；失败返回表示原值未改变且失败值仍由错误对象拥有。

## C++20 所有权与线程迁移

Rust `get` 和 `wait` 返回受借用规则约束的 `&T`。C++20 无法证明引用不会逃逸，
CIO 返回 `std::shared_ptr<const T>` owning snapshot：

- SetOnce 句柄移动或析构后，snapshot 仍保持值生命周期；
- snapshot 可跨 `co_await` 和 worker 迁移，不保存裸引用；
- `const T` 防止经公开句柄修改单次发布值；
- SetOnce 的公开 API 与核心状态不暴露裸指针。

owning snapshot 比 Rust 借用更宽松，因此 `into_inner` 采用明确边界：

- 仍有 Wait 拥有 SetOnce 共享状态时拒绝消费；
- 可复制 `T` 即使仍有 snapshot，也可复制出返回值，让旧 snapshot 继续观察
  旧对象；
- 不可复制 `T` 仍有 snapshot 时拒绝移动，避免修改其正在观察的对象。

`into_inner() &&` 会真正移走内部 state，之后源 SetOnce 处于明确不可用状态，
不能再次 `set`，因此不会把 Tokio 的消费 API 意外扩展成 reset。值的复制、移动
与旧值析构均在状态锁外执行，避免用户类型的构造/析构重入 SetOnce 或产生锁
顺序反转；若 C++ 用户类型在构造返回值时抛异常，源仍保持已消费，提供基本异常
保证。

与 Rust 按值 `self` 不同，C++ 调用方仍能错误地让一个线程对同一个具体对象执行
`into_inner()`，同时让另一线程访问该对象的 `state_` 成员。这属于对象本身的
并发 move/use 数据竞争，不由 `use_count()` 解决。调用 `into_inner()` 前必须
由外部同步保证对该 SetOnce 对象的独占访问；共享事件的其他线程应先停止使用其
句柄。OnceCell 的 `into_inner()` 也遵守同一规则。

Clone 对齐 Tokio 的值快照语义，不共享发布事件：空 SetOnce 克隆后，原件和
副本可以分别成功设置不同值；已发布对象在 `T` 可复制时复制当前值。业务需要
共享同一个事件时，应像 Rust `Arc<SetOnce<T>>` 一样共享 CIO SetOnce owning
handle，而不是调用 `clone()`。

`SetOnce<T>` 在 `T` 为 Send 时可移动，在 `T` 同时为 Send/Sync 时可共享；
Wait 与 Awaiter 仅在 `T` 同时满足 Send/Sync 时可进入 portable task。portable
waiter 暂停后允许恢复到其他 worker，不承诺线程亲和性。

## 阻塞行为

`wait` 通过 Notify 挂起 task，不阻塞 runtime worker。`get`、`initialized`
和 `set` 只竞争一个不跨 `co_await`、不执行用户回调、不进行系统 I/O 的短状态
锁。SetOnce 不提供 blocking_wait；同步调用方可执行 `set` 或 `get`，但等待值
必须由 runtime 驱动异步 Wait。

## 验证证据与剩余门槛

本地测试覆盖：

- 所有构造、首次/重复 set、已发布快路径不构造候选值、错误 Display/Debug 和
  失败值返还；
- 64 个同步线程并发 set 的唯一 winner 与整数值守恒；
- 32 个 move-only drop probe 的 winner/loser 恰好一次析构；
- 96 个 multi-thread waiter 的广播唤醒与发布可见性；
- 300 轮不同 yield 时序的 set/wait lost-wake 竞态；
- 未 poll、pending、已通知未恢复三类取消边界；
- runtime shutdown 后 set、重新 wait 和 20 次反复启停；
- Wait/Awaiter 消费式 single-use、into_inner 消费源、Clone 独立、owning
  snapshot 与 mobility 静态边界。

Debug 能力复用 C++ `operator<<`。当前整数用例与 Rust 结构文本一致，但字符串
引号/转义和任意用户类型不承诺逐字复刻 Rust `Debug`，因此这是可读的 C++ 映射，
不是所有 `T` 的文本完全对齐。

固定 Tokio 1.53.1 的 Rust/C++ 差分新增四项：

- `set_once_wait_unblocks`；
- `set_once_single_winner_values`；
- `set_once_cancel_safe`；
- `set_once_clone_independent`。

当前 Windows/MSVC 主机的 Debug、Release 与 ASan 测试均通过，SetOnce Release
回归测试连续执行 100/100 无失败；Notify 最后-owner 修复后，SetOnce 又在
Release 与 ASan 下各连续通过 500/500；全套固定 Tokio 差分累计 51 项并重复
通过 20/20。Windows ASan 不等同于 LeakSanitizer，这些结果也不替代
Linux/macOS、Clang/GCC、UBSan/TSan/LSan、模型检查和三平台高竞争验证。

性能框架已加入 `set_once_fanout`：确认全部 waiter 实际进入 Pending 后统一
发布并等待恢复，且已完成 Windows Release dirty smoke。仍需补已发布快路径、
并发 set 与取消风暴，并固定 worker、waiter/setter 数量、发布值大小、CPU
亲和性和样本数，记录吞吐、p50/p95/p99/p999、CPU、分配和调度唤醒。在 clean
revision 上形成正式报告前，不得把 smoke 或架构推断写成性能结论。
