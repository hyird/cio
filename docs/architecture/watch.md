# watch 设计

## 对齐范围与当前状态

本垂直切片对齐 Tokio 1.53.1 的 `tokio::sync::watch` 核心能力：

- `channel<T>(initial)`；
- 可复制、可移动的 `Sender<T>` 与 `Receiver<T>`；
- `Sender::send`、`send_replace`、`borrow`、`subscribe`、`closed`、
  `is_closed`、`receiver_count`、`sender_count` 和 `same_channel`；
- `Receiver::borrow`、`borrow_and_update`、`has_changed`、`changed`、
  `wait_for`、`mark_changed`、`mark_unchanged` 和 `same_channel`；
- `SendError<T>`、`RecvError` 的值返还和文本；
- 每个 Receiver 独立的已读版本、最后 Sender/Receiver 析构、重新订阅、
  取消、跨线程唤醒和 runtime shutdown。

Tokio 的 `Sender::new` 在 C++ 中与关键字 `new` 冲突，CIO 使用
`Sender::new_sender` 表达同一能力；`Sender<T>` 的受约束默认构造对应 Tokio
的 `Default`。两者都只创建 Sender，初始 Receiver 数为零。

本模块仍是“部分实现/安全替代”，不能标记为完整对齐：

- 尚未实现 `send_modify` 和 `send_if_modified`；
- CIO `Snapshot<T>` 是对 Tokio 锁持有型 `Ref<'_, T>` 的拥有式安全替代，
  可观察行为并不完全相同；
- `wait_for` 的 Predicate 捕获与 portable `Send` 边界尚不能由 C++20 完整证明；
- 通用 `select`/组合轮询尚不能在 Future 返回 Pending 后退还本 poll 的
  cooperative budget；
- Linux、macOS、TSan、LSan 和正式性能证据仍未完成。

## 状态、版本与发布

Sender、Receiver 和异步 operation 共享一个拥有式 `WatchState<T>`。状态锁只
保护以下短小元数据：

- 当前不可变值的 owning 智能句柄；
- 64 位 wrapping version；
- 所有 Sender 已析构的关闭位；
- 公开 Sender/Receiver 逻辑计数；
- `Sender::closed` operation 使用的隐藏 Sender 借用计数。

每个 Receiver lease 单独保存最后已读 version。Receiver copy 继承源 Receiver
的游标，因此源端未读的版本对副本也未读；`Sender::subscribe` 则读取当前
version，新 Receiver 把订阅前的最新值视为已读。连续发布只保留最新值，中间
版本允许被跳过，watch 不是消息队列。

`send` 先读取 Receiver 计数提示。观察到零时返回拥有原值的 `SendError<T>`，
且不修改未来订阅者可见的值；观察到非零后，最后 Receiver 仍可能并发析构，
此时发送可以成功，这与 Tokio 的提示式计数语义一致。

`send_replace` 即使没有 Receiver 也发布新值、增加 version、通知全部等待者并
返回旧值。候选值先在锁外构造，状态锁内只交换 owning 句柄和增加 version，
通知、旧值复制/移动及析构都在锁外执行。

所有逻辑计数使用 checked increment。达到 `size_t` 上限时抛出
`std::length_error` 且不改变原计数，防止回绕成零后伪造最后端点析构。version
使用无符号 wrapping 运算；`mark_changed` 对当前 Receiver 的游标执行 wrapping
减一，`mark_unchanged` 则同步到当前 version。

## Snapshot：对 Tokio Ref 的安全替代

Tokio `Ref<'_, T>` 借用 Receiver 或 Sender，并在存续期持有读锁：

- Ref 不能安全跨越任意 await；
- Ref 存续时写入方可能阻塞；
- Ref 不是 `Send`。

CIO 不把此类借用或锁 guard 暴露到可迁移协程。`Snapshot<T>` 共享拥有一个
不可变版本：

- Snapshot 本身覆盖值的完整生命周期，不依赖 Receiver 栈对象；
- 读取不持 channel 锁，Snapshot 可跨暂停点保存；
- 只有在 `T` 同时满足 CIO `Send`/`Sync` 时，Snapshot 才声明为
  `Send`/`Sync`；
- `value() const &` 返回的引用只在 Snapshot owner 存续期间有效；
- 临时 Snapshot 只有在 `T` 可复制时按值返回，move-only 值禁止从临时对象
  取得可能悬垂的引用。

该设计是安全替代，不是 Tokio Ref 的可观察等价实现：

- 长期 Snapshot 不会阻塞 `send`；
- Snapshot 可以延长旧版本的析构时间；
- copyable `T` 的 `send_replace` 在存在 Snapshot 时复制旧值，因此用户复制
  代码可能运行或抛异常；
- 若旧值复制在发布后抛异常，新版本已经发布、version 已增加且等待者已经收到
  通知，CIO 提供基本异常保证；
- move-only `T` 存在活动 Snapshot 时无法既返回独占旧值又保持 Snapshot
  不变，因此 `send_replace` 在发布前抛出 `logic_error`，channel 保持不变。

这些边界必须保留在兼容矩阵中，不得把整数等可复制类型的差分通过外推为任意
`T` 的 Ref 完整等价。

## changed、关闭与隐藏借用

`Receiver::changed` 遵循“先创建 Notify generation snapshot，再检查
version/closed”的顺序：

1. version 不同优先返回成功并把最新版本标为已读；
2. version 相同且全部 Sender 已析构时返回 `RecvError`；
3. 其余情况挂起，唤醒后循环并容忍伪唤醒。

因此最后 Sender 在发布后析构时，未读 Receiver 的第一次 `changed` 仍成功，
下一次才返回关闭错误。`has_changed` 与之不同：只要全部 Sender 已析构就直接
返回 `RecvError`，即使还有未读版本。

Tokio 的 async method 借用 Sender/Receiver，Rust 编译器保证句柄在 future
存续期内不能被销毁。CIO operation 不保存调用方引用：

- Receiver operation 共享拥有原 Receiver lease；调用方移动或析构外层句柄
  不会让 Receiver 计数提前归零；
- `Sender::closed` 增加隐藏借用计数，但 `sender_count()` 仍只报告公开 Sender
  数量；
- 最后公开 Sender 在 pending `closed` operation 存续时析构，隐藏借用继续
  保持接收方向打开；operation 完成或取消后再按最后借用规则关闭；
- Notify waiter 由拥有 operation、waiter ID 和 generation 管理，取消只注销
  本次等待，不消费版本或丢失永久关闭状态。

`Sender::closed` 等待 Receiver 计数变为零。最后 Receiver 析构后若很快
`subscribe`，等待既可能已经观察到短暂的零并完成，也可能醒来后看到重新订阅
而继续循环，这与 Tokio 明示的并发边界一致。

## wait_for、取消与用户 Predicate

`wait_for` 在首次 poll 时总会检查当前最新值，即使该版本已读。之后仅在新版本
到达时再次调用 Predicate；发送过快时允许跳过中间值。每次调用前先把该版本
标为已读：

- Predicate 返回 true：返回同一版本的 owning Snapshot；
- Predicate 返回 false：该版本保持已读，然后等待下一次变化；
- 所有 Sender 已析构：最后值仍会被 Predicate 检查；true 时成功，false 时
  才返回 `RecvError`；
- Predicate 抛异常：异常传播，但 channel、Receiver cursor 和 waiter 不会
  poison。

Predicate 由 frame-owned `WatchWaitOperation` 拥有。成员析构顺序保证正常
完成、异常和取消时，先释放 Receiver 的独占 operation，再执行用户 Predicate
析构；析构回调可以安全重入 Receiver。Predicate 返回 false 后，临时 Snapshot
在真正 await Notify 前释放，不会把旧值无意保留到下一次唤醒或取消。

取消 `changed` 不会提交未读版本。取消 `wait_for` 可以保留已经由 Predicate
返回 false 的已读进展，但不会把 Predicate 返回 true 的值留在“取消后已读”
状态，这对应 Tokio 的不同取消保证。

Predicate 本身按值进入 coroutine frame，不保存调用方引用；但 C++20 无法检查
任意 lambda 的全部捕获。当前返回的普通 `Task` 也不能完整表达“Predicate 含
引用捕获时只能 local、满足 CIO `Send` 时才可 portable spawn”的类型性质。
在 owned task/`PortableTask` 边界收口前，调用方不得把带裸引用、
`reference_wrapper` 或未审计线程亲和状态的 Predicate 送入 portable task。

## cooperative budget 边界

固定 Tokio 1.53.1 的 `changed`、`wait_for` 和 `Sender::closed` 都受
cooperative budget 约束。CIO 当前保证：

- ready 成功和错误路径在提交 cursor 或执行 Predicate 前通过 cooperative
  gate；
- 128 个 ready 进展后会让出 worker；
- pending `changed`/`closed` 不提交状态；
- `wait_for` 在同一 runtime poll 内收到立即通知并循环时只扣一次预算；
- 真正 Notify 暂停后，新 poll 会重新进入 gate。

仍有一个必须明确的缺口：Tokio 的 `cooperative(inner)` 在 inner 返回 Pending
时会用 `RestoreOnPending` 退还本 poll 的预算。CIO 直接 `co_await Task` 时，
任务挂起后下一次调度会重置预算，因此现有路径表现一致；但未来
`select`、`join`、timeout 或手动 poll 若在同一根 poll 中继续轮询其他分支，
当前 watch gate 没有通用的“提交/回滚预算”协议，可能在 127/128 边界产生差异。
在统一 Cooperative wrapper 完成前，这一项保持“部分实现”。

## 所有权、异常、阻塞与线程迁移

公开 channel 值禁止裸指针、引用和 `reference_wrapper` 风格非拥有包装。
共享状态、Snapshot、Receiver operation、Sender 隐藏借用、Notify waiter 和
Predicate frame 均使用值语义或拥有式智能句柄；异步 operation 不捕获
`this` 或调用方裸引用。

所有可能执行用户代码的路径都在 channel mutex 外：

- 初值和发送候选值的构造、移动；
- 被替换值的复制、移动和析构；
- Snapshot 的最后 owner 析构；
- Predicate 调用及析构；
- 错误值返还。

短 mutex 只保护 owning 句柄、version、关闭位和计数，绝不跨越 await，也不在
锁内通知或恢复 task。它不是算法意义上的 lock-free；高竞争下仍可能短暂等待
同步锁。后续必须用 profiler 和三平台 benchmark 判断是否需要分片 Notify、
无锁版本发布或专用内存资源，不能只凭架构推断宣称性能。

Sender、Receiver 和 Snapshot 仅在 `T` 同时满足 CIO `Send`/`Sync` 时声明可跨
worker。这个 trait 是 C++ 用户和库的显式安全承诺，不等同于 Rust 编译器对整个
Future frame 的证明。

## 验证证据与剩余门槛

本地状态机、异常和压力测试覆盖：

- 初值已读、相同值仍增加 version、borrow 不标已读、borrow_and_update 标已读；
- mark changed/unchanged、Receiver copy 游标继承和 subscribe 当前版本已读；
- 最后 Sender 关闭但保留最后值、最后 Receiver 唤醒 closed、重新订阅；
- send 无 Receiver 返还值、send_replace 无 Receiver 仍更新；
- changed、wait_for、closed 的未 poll/pending 取消和句柄复用；
- 多 Receiver、跨线程发布、runtime shutdown 和反复创建；
- Predicate 锁外重入、异常、析构重入和 operation 析构顺序；
- false Snapshot 在真正等待前释放；
- 用户值移动/复制异常、发布后异常状态和析构锁外重入；
- count overflow 拒绝、裸指针/引用包装拒绝和 Send/Sync 静态边界；
- ready changed/closed/wait_for 的 cooperative fairness。

固定 Tokio 1.53.1 的 Rust/C++ 差分新增二十一项：

- `watch_initial_borrow`；
- `watch_send_changed_borrow_update`；
- `watch_marks_and_has_changed`；
- `watch_independent_receivers_subscribe`；
- `watch_last_sender_close_retains_value`；
- `watch_last_receiver_closes_sender`；
- `watch_changed_cancel_safe`；
- `watch_same_channel_counts`；
- `watch_send_replace`；
- `watch_wait_for`；
- `watch_value_drop_and_clone`；
- `watch_error_format`；
- `watch_cooperative_ready_paths`；
- `watch_coop_changed_success_boundary`；
- `watch_coop_changed_error_boundary`；
- `watch_coop_closed_boundary`；
- `watch_coop_wait_for_success_boundary`；
- `watch_coop_wait_for_error_boundary`；
- `watch_coop_changed_fresh_wake_budget`；
- `watch_coop_wait_for_fresh_wake_budget`；
- `watch_coop_closed_fresh_wake_budget`。

新增八条精确 cooperative 差分覆盖 `changed` 成功/错误、`closed`、
`wait_for` 成功/错误的 128/129 边界，以及
`changed`/`wait_for`/`closed` 真实通知后 fresh poll 的预算扣费。这里的直接
await 路径已获得精确证据，但不改变未来 `select` 等组合轮询的 Pending 预算
退款缺口。

当前 Windows/MSVC Debug、Release 和 ASan 测试通过；watch 独立目标包含
12 组测试，覆盖 1000 轮 send/drop/subscribe 三方竞态、精确 cooperative
budget 与真实通知后的 fresh poll。最新的临时 Snapshot 生命周期、Predicate
析构顺序和计数溢出修复后，Release 回归连续执行 300/300、ASan 回归连续执行
100/100 无失败。固定 Tokio 全套差分累计 119 项，Windows Release/Debug 均
通过 119/119。核心源码契约检查通过 49 个文件。

Windows ASan 不提供 LeakSanitizer。Linux/macOS、Clang/GCC、UBSan/TSan/LSan、
模型检查、通用 Pending budget 回滚、Predicate portable trait、
`send_modify`/`send_if_modified`、三平台高竞争和正式性能比较仍是必需缺口。

性能框架已加入等工作量 `watch_fanout`：发布者每发送一个版本，都等待全部
subscriber 确认后才发布下一版本，禁止通过 latest-value coalescing 偷减工作；
吞吐按 delivery 数计算。Windows dirty smoke 固定 CPU 0–3、2 次预热/5 个样本：

- 1 worker、4 subscriber、1000 发布/4000 delivery：CIO p50 为
  3.189 ms，Tokio 为 0.702 ms，耗时比 4.545x；
- 4 worker、16 subscriber、1000 发布/16000 delivery：CIO p50 为
  27.302 ms，Tokio 为 4.129 ms，耗时比 6.612x；
- Asio 没有语义等价的 Tokio 风格 watch API，明确 skip。

报告位于
`build-bench/watch-fanout-smoke/20260726T021512Z-dirty-smoke.md`。该工作树
为 dirty revision，样本只有 5 个，且报告未采集 CPU、分配、RSS、上下文切换
或 profiler 数据；这些数值只证明负载和比较链路可运行，并提示后续调查
多 worker 扩展与分配开销，不能写成正式性能结论或外推到其他平台。
