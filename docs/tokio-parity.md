# Tokio 1.53.1 兼容矩阵

本文档是 CIO 的唯一权威 Tokio 兼容矩阵。基线范围由
[`TOKIO_BASELINE.md`](../TOKIO_BASELINE.md) 定义。

## 状态定义

| 状态 | 含义 |
| --- | --- |
| 未实现 | 尚无可用 CIO 对应能力 |
| 部分实现 | 有真实实现，但 API、语义、平台或测试证据仍不完整 |
| 已实现 | API、可观察语义、目标平台和规定测试均已闭环 |
| 安全替代 | C++20 无法等价表达，已有论证、最接近的安全替代和边界测试 |

在差分测试、跨平台 CI 与规定的 sanitizer 证据补齐前，本阶段条目不会标记为
“已实现”。

## 第一批逐项矩阵

| Tokio 1.53.1 项 | CIO 对应项 | 状态 | 当前证据与缺口 |
| --- | --- | --- | --- |
| `tokio::runtime::Builder::new_current_thread` | `cio::runtime::Builder::new_current_thread` | 部分实现 | 已构建 current-thread runtime，并实现 `enable_time`、`enable_all`、`start_paused`；待其余 Builder 配置面 |
| `tokio::runtime::Builder::new_multi_thread` | `cio::runtime::Builder::new_multi_thread` | 部分实现 | 已实现准确 `worker_threads`、多 P worker 和启动/关闭；默认并行度仍只读取 `hardware_concurrency()`，待其余 Builder 配置与 quota-aware 探测 |
| `Builder::max_blocking_threads` | `Builder::max_blocking_threads` | 部分实现 | 默认 512、零值拒绝和准确并发上限已测；待 `thread_keep_alive`、线程 hooks/命名/栈大小 |
| `tokio::runtime::Runtime` | `cio::runtime::Runtime` | 部分实现 | 已有 current-thread、multi-thread、时间驱动、blocking pool 和析构关闭；待 I/O 驱动、Handle 与完整关闭选项 |
| `Runtime::block_on` | `Runtime::block_on` | 部分实现 | 已验证输出、嵌套 task、异常传播、嵌套调用拒绝和 multi-thread spawn/join 差分；multi-thread 根任务目前也要求 `PortableTask`，比 Tokio 更严格 |
| `tokio::task::spawn` | `Runtime::spawn`、`cio::task::spawn` | 部分实现 | 不同步 poll 与 multi-thread spawn/join 差分通过；multi-thread 只接受 `PortableTask`，已有 owned factory，待 `spawn_local`/`LocalSet` |
| `tokio::task::JoinHandle<T>` | `cio::task::JoinHandle<T>` | 部分实现 | detach 与清理先于 join 可见的差分通过；已测 stale waiter wake，待 select 差分 |
| `JoinHandle::abort` | `JoinHandle::abort` | 部分实现 | 启动前取消和析构顺序差分通过；另有暂停点、跨线程、幂等测试，待三平台/TSan |
| `tokio::task::AbortHandle` | `cio::task::AbortHandle` | 部分实现 | 可复制远程取消句柄；待 task ID 与跨线程压力覆盖 |
| `tokio::task::JoinError` | `cio::task::JoinError` | 部分实现 | 区分 cancelled/panic 并保留 `exception_ptr`；待错误格式和 ID 对齐 |
| `tokio::task::Id` | `cio::task::Id` | 部分实现 | 公开 ID 单调；内部 ready/waker 已用 slot/generation/runtime nonce，待 `id/try_id` |
| `tokio::task::yield_now` | `cio::task::yield_now` | 部分实现 | 回到 ready queue 尾部且无需额外 wake；已做跨线程 wake 竞态，待非保证项差分 |
| Rust `Future`/Tokio task 基础 | `cio::Task<T>`、`cio::Task<void>` | 部分实现 | lazy/组合 poll/异常六项差分通过，另有 2 万层 symmetric transfer；待 allocator 与更完整的可移植性静态检查 |
| Tokio `Send + 'static` spawn 边界 | `cio::Send`、`cio::Sync`、`PortableTask<T>`、`owned`、`assume_portable` | 部分实现 | 未知类型默认拒绝，owned factory 延迟到 worker 创建用户帧；C++20 无法证明完整帧，显式 trait/人工审计仍是安全承诺 |
| `tokio::task::coop::consume_budget` | `cio::task::consume_budget` | 部分实现 | 每次 poll 128 单位并通过 Tokio 差分；已修复预算耗尽自调度后 fresh poll 漏扣首个 ready progress 的 129 上限偏差，oneshot 与 bounded mpsc 有精确回归；尚未接入未来所有 I/O、channel 与同步原语 |
| `tokio::task::spawn_blocking`、`Runtime::spawn_blocking` | CIO 同名能力 | 部分实现 | owned factory/Send 边界、默认 512、按需 pool、排队取消、running abort 无效、异常、嵌套提交、冻结时间抑制和析构等待已测；待 keep-alive、mandatory、shutdown timeout/background、block_in_place 与三平台证据 |
| `tokio::sync::Notify`、`Notified`、`OwnedNotified` | `cio::sync::Notify`、`Notify::Notified` | 部分实现 | 单 permit、enable、FIFO notify_one、LIFO notify_last、notify_waiters 创建快照、取消转交、跨线程和 4 项差分已测；weak waiter 提升采用无分配 list/splice retired 批次，保证最后 owner 只在状态锁外析构，覆盖单播、broadcast 与取消转交；CIO Notified 统一使用 owned 状态，待 const_new、tracing、loom/TSan 与三平台证据 |
| `tokio::sync::Semaphore`、两类 permit 与获取错误 | `cio::sync::Semaphore`、`SemaphorePermit`、`OwnedSemaphorePermit`、`AcquireError`、`TryAcquireError` | 部分实现 | 全部获取/try/close/计数 API、严格 FIFO、批量头阻塞、部分与已满足取消转交、permit move/forget/split/merge、跨线程和 4 项差分已测；weak waiter 提升使用锁外 keep-alive 批次，已修复 release/abort 最后 owner 锁内析构递归 cancel，并由原复现管道 Release/ASan 各 500/500 验证；借用 permit 安全映射为 owned guard，`const_new` 仅运行期兼容工厂，待 tracing、loom/TSan 与三平台证据 |
| `tokio::sync::Mutex`、四类 guard 与 `TryLockError` | `cio::sync::Mutex<T>`、`MutexGuard<T>`、`OwnedMutexGuard<T>`、`MappedMutexGuard<T,U>`、`OwnedMappedMutexGuard<T,U>`、`TryLockError` | 部分实现 | lock/owned/try/blocking/get_mut/into_inner、严格 FIFO、排队与已满足取消转交、持锁暂停、异常非 poison、跨线程高竞争、map/嵌套 map/try_map 和 5 项差分已测；借用 guard 安全映射为 owned，projection 只执行一次并由 aliasing `shared_ptr` 稳定持有子对象，try_map 使用 predicate+projection；`get_mut` 返回持许可 guard，写 guard 为 Send/non-Sync，`const_new` 仅运行期工厂，均是 C++20 安全映射；待格式 trait、tracing、loom/TSan 与三平台证据 |
| `tokio::sync::RwLock`、读写/owned/mapped guard 与 `TryLockError` | `cio::sync::RwLock<T>`、`RwLockReadGuard<T,U>`、`OwnedRwLockReadGuard<T,U>`、`RwLockWriteGuard<T>`、`OwnedRwLockWriteGuard<T>`、`RwLockMappedWriteGuard<T,U>`、`OwnedRwLockMappedWriteGuard<T,U>`、`TryLockError` | 部分实现 | 值构造/with_max_readers/read/write/owned/try/blocking/get_mut/into_inner、batch semaphore 最大读者、写者优先 FIFO、部分与已满足取消转交、异常非 poison、read/write map/try_map、into_mapped、原子 downgrade/downgrade_map/try_downgrade_map、跨线程高竞争均有实现与本地测试；共享读/最大读者、写者优先 FIFO、部分写者取消、非 poison、owned mapping、原子 downgrade 六项 Tokio 差分已通过，RwLock 回归 repeat 20/20；借用 guard 统一安全映射为 owned，projection 只执行一次并由 aliasing `shared_ptr` 稳定持有子对象，条件映射使用 predicate+projection；`get_mut` 返回全许可写 guard，写 guard 为 Send/non-Sync，`const_*` 仅运行期工厂，均是 C++20 安全映射；待格式 trait、tracing、loom/TSan、完整性能对比与三平台证据 |
| `tokio::sync::Barrier`、`BarrierWaitResult` | `cio::sync::Barrier`、`Barrier::Wait`、`BarrierWaitResult` | 部分实现 | 已对齐 `n == 0` 按 1 处理、首次 poll 才计入到达、可复用 generation、每代唯一 leader、owned 共享值句柄以及“已 poll 到达不回滚”的非 cancel-safe 语义；current-thread、多线程高竞争、runtime shutdown、Windows Release/ASan、100/100 重复回归和 4 项 Tokio 差分已通过；待格式 trait、tracing、loom/TSan、Linux/macOS 与完整性能证据，详见 [`architecture/barrier.md`](architecture/barrier.md) |
| `tokio::sync::OnceCell<T>`、`SetError<T>` | `cio::sync::OnceCell<T>`、`OnceCellMutGuard<T>`、`SetError<T>` | 部分实现 | 构造/get/set/get_or_init/get_or_try_init/get_mut/take/into_inner/Clone/错误分类均有真实实现；owning `shared_ptr<const T>` 和 non-Send/non-Sync 修改 guard 是不暴露裸引用的 C++20 安全映射；状态机已移除成功后 close/generation 重建窗口，take/into_inner 的用户 move/copy/destruct 在锁外；单初始化者、初始化中取消重试、try-error 重试、Clone 独立、Some/None Debug、SetError Debug/Display 6 项 Tokio 差分通过；Windows Debug/Release/ASan、100/100 回归与 51 项全差分 20/20 均通过；`const_*` 只有运行期工厂，待通用 awaitable、tracing、loom/TSan、Linux/macOS 与性能证据，详见 [`architecture/once-cell.md`](architecture/once-cell.md) |
| `tokio::sync::SetOnce<T>`、`SetOnceError<T>` | `cio::sync::SetOnce<T>`、`SetOnce<T>::Wait`、`SetOnceError<T>` | 部分实现 | 构造/get/set/wait/into_inner/Clone/错误值返还均有真实实现；owning `shared_ptr<const T>` 是不暴露裸引用的 C++20 安全映射，Wait 是拥有式、single-use 且 cancel-safe；广播可见性、64 路并发 set、96 waiter、取消、关闭重启和 100/100 回归均通过，wait 唤醒、唯一 winner/值守恒、取消安全、Clone 独立 4 项 Tokio 差分通过；`const_*` 只有运行期工厂，待 tracing、loom/TSan、Linux/macOS 与性能证据，详见 [`architecture/set-once.md`](architecture/set-once.md) |
| `tokio::sync::oneshot`、`Sender<T>`、`Receiver<T>`、`RecvError`、`TryRecvError` | `cio::sync::oneshot::channel<T>`、`Sender<T>`、`Receiver<T>`、`RecvError`、`TryRecvError` | 部分实现 | channel/send/closed/is_closed/poll_closed 能力映射/close/is_empty/is_terminated/try_recv/异步 receive/blocking_recv 均有真实实现；move-only owning operation 和运行期独占检查安全映射 Rust `&mut`，Receiver drop 与 active receive 稳定完成为 RecvError，非拥有引用包装在编译期拒绝，ready path 对齐 cooperative budget；close 保留旧值、send/close 线性化、全部取消边界、跨线程、shutdown、用户 move 异常/析构重入和值守恒、Windows Debug/Release/ASan、Release 300/300、ASan 100/100 回归和 11 项 Tokio 差分均通过；待通用 Debug、tracing、loom/TSan、Linux/macOS 与正式性能证据，详见 [`architecture/oneshot.md`](architecture/oneshot.md) |
| `tokio::sync::mpsc` bounded channel、`Sender<T>`、`Receiver<T>`、`Permit<T>`、`OwnedPermit<T>`、`WeakSender<T>` 与错误 | `cio::sync::mpsc::channel<T>`、`Sender<T>`、`Receiver<T>`、`Permit<T>`、`OwnedPermit<T>`、`WeakSender<T>` 与 `error::*` | 部分实现 | 前两个 bounded 垂直切片已实现 send/try_send/reserve/try_reserve/reserve_owned/try_reserve_owned、closed/same_channel、recv/try_recv、len/is_empty、close/drain、FIFO 背压、两类 Permit、strong/weak 精确计数、blocking bridge、跨线程唤醒与 runtime shutdown；owned lease 安全映射 Rust 借用/拥有式 permit，编译期拒绝裸指针和非拥有引用包装，用户值只在状态锁外移动/析构；已修复 cooperative gate 抢 capacity/FIFO、Closed 热循环饥饿、pending send 提前移动、try_send 异常时 close/drain 丢 idle wake 和 TrySend Debug 格式偏差，覆盖关闭/取消/异常与 Receiver drop 后 owned send；Windows Debug/Release/ASan、Release 300/300、ASan 100/100 与 22 项 Tokio 差分通过；等工作量 `mpsc_bounded` 性能负载已完成 dirty smoke，Asio 明确 skip；待 batch reserve/permit iterator、poll/batch receive、tracing、loom/TSan、Linux/macOS 和正式性能证据，详见 [`architecture/mpsc.md`](architecture/mpsc.md) |
| `tokio::sync::mpsc` unbounded channel、`UnboundedSender<T>`、`UnboundedReceiver<T>`、`WeakUnboundedSender<T>` 与错误 | `cio::sync::mpsc::unbounded_channel<T>`、`UnboundedSender<T>`、`UnboundedReceiver<T>`、`WeakUnboundedSender<T>` 与共用 `error::*` | 部分实现 | 首个 unbounded 垂直切片已实现同步 send、closed/is_closed/same_channel、downgrade/upgrade/精确 strong+weak count、recv/try_recv/blocking_recv、close/drain、len/is_empty 和 sender count；send/close 同锁线性化且用户值移动/析构在锁外，覆盖四生产者、close/send 压力、取消、Receiver drop、Weak 不可复活、异常/析构重入、runtime shutdown、跨线程和精确 cooperative budget；Windows Debug/Release/ASan、Release 300/300、ASan 100/100 与 14 项 Tokio 差分通过；待 recv_many/blocking_recv_many/poll_recv/poll_recv_many、性能负载、tracing、loom/TSan、Linux/macOS，详见 [`architecture/mpsc-unbounded.md`](architecture/mpsc-unbounded.md) |
| `tokio::sync::broadcast`、`Sender<T>`、`WeakSender<T>`、`Receiver<T>` 与错误 | `cio::sync::broadcast::channel<T>`、`Sender<T>`、`WeakSender<T>`、`Receiver<T>`、`SendError<T>`、`RecvError`、`TryRecvError` | 部分实现 / 安全映射 | 首片已实现 channel/new_sender、同步 send、subscribe/resubscribe、move-only Receiver 的 recv/try_recv/blocking_recv、精确 Lagged、先 drain 后 Closed、closed、len/is_empty、channel identity、强弱/Receiver 计数和 Weak upgrade；容量按 Tokio 向上取整为 2 的幂，消息 clone 由 per-message mutex 串行且复制/析构在 channel 锁外，recv copy 异常前已提交游标；owning receive 与 hidden Sender borrow 安全映射 Rust 借用，裸指针、reference_wrapper、span 和 string_view channel 值被拒绝；Windows Release/Debug/ASan 12 组测试、Release 100/100、ASan 50/50、14 项 broadcast 差分及当前全套 148/148 Release/Debug 已通过；当前全套 CTest 为 Release/Debug 18/18、ASan 19/19，核心契约检查覆盖 64 文件；`broadcast_fanout` dirty smoke 的 w1/w4 CIO p50 暂为 Tokio 5.792x/7.212x，4-worker CIO RSD 17.26%，只作 profiler 优先级信号；Receiver 极限上限、hidden borrow 活跃且 public strong_count 为零的安全映射、list/逐消息分配、线性槽查找、clone mutex、通用 Pending budget 退款、TSan/LSan/Linux/macOS 和正式性能证据仍未收口，详见 [`architecture/broadcast.md`](architecture/broadcast.md) |
| `tokio::sync::watch`、`Sender<T>`、`Receiver<T>`、`Ref<T>` 与错误 | `cio::sync::watch::channel<T>`、`Sender<T>`、`Receiver<T>`、`Snapshot<T>`、`SendError<T>`、`RecvError` | 部分实现 / 安全替代 | 已实现 channel、Sender default/new_sender/copy/move/send/send_replace/borrow/subscribe/closed/is_closed/count/same_channel，以及 Receiver copy/move/borrow/borrow_and_update/has_changed/changed/wait_for/mark_changed/mark_unchanged/same_channel；每个 Receiver 独立 version cursor，owning operation/隐藏 Sender 借用覆盖析构与取消，checked count 防回绕，用户 Predicate 和 T 构造/移动/复制/析构均在状态锁外；Windows Debug/Release/ASan、12 组测试、1000 轮三方竞态、Release 300/300、ASan 100/100、21 项 watch 差分和当前全套 148/148 Release/Debug 已通过；新增八条精确 cooperative 差分覆盖 changed 成功/错误、closed、wait_for 成功/错误的 128/129 边界，以及 changed/wait_for/closed 真实通知后 fresh poll 的预算扣费；核心契约检查当前覆盖 64 文件；`Snapshot<T>` 是不持读锁、可跨暂停点的安全替代，不等价于 Tokio `Ref`，copyable 值的 replace 可能复制，move-only 值存在活动 Snapshot 时 replace 在发布前失败；等工作量 `watch_fanout` 已完成 dirty smoke，w1/w4 CIO p50 暂为 Tokio 的 4.545x/6.612x，只作链路与 profiler 优先级信号；缺 `send_modify`/`send_if_modified`、Predicate portable `Send` 证明、通用 select Pending budget 退款、TSan/LSan/Linux/macOS 和正式性能证据，详见 [`architecture/watch.md`](architecture/watch.md) |
| `tokio::io::ReadBuf`、`AsyncRead`、`AsyncWrite` 与基础 ext | `cio::io::ReadBuf`、拥有式 buffer/lease、`AsyncRead`/`AsyncWrite` concepts、session、`read`/`write`/`write_vectored`/`flush`/`shutdown`/`read_exact`/`write_all` | 部分实现 / 安全映射 | 使用 fully-initialized storage 维护 filled/initialized/capacity，不暴露跨暂停裸 span；ReadBuf 采用无伪失败的原子 idle/owner/lease gate，端点/session 双层 audited marker 拒绝未承诺共享 identity、独占和 self-owned primitive control 的同形类型；session 覆盖整个组合 future，未 poll 析构、Pending 子 primitive 取消、partial primitive 间别名竞争和 late wake 均不提前释放端点或误提交进度；MemoryReader/MemoryWriter 对齐单次 partial、EOF/零容量、write-zero、默认首个非空 vectored 和 shutdown，`read_exact`/`write_all` 对齐 partial、UnexpectedEof、WriteZero、原生错误、空输入 no-poll、不隐式 flush/shutdown，并验证 4-worker 真迁移与外部 abort。Windows Release/Debug/ASan 内存端点 14 组、精确读写 4 组测试；内存端点 Release 300/300、ASan 100/100，精确读写 Release 100/100、Debug/ASan 20/20；15 项 I/O 差分和当前全套 148/148 Release/Debug 各重复 20/20。多 lane composed poll 基础有 13 组测试，poll-native operation 5 组，OperationRegistry 10 组，CopyBuffer/native submission 8 组；全套 CTest 为 Release/Debug 18/18、ASan 19/19，核心契约检查覆盖 64 文件；`io_memory_ready` dirty smoke 的 w1/w4 CIO p50 暂为 Tokio 13.463x/23.747x，CIO RSD 为 0.28%/1.86%，只作 profiler 优先级信号，Asio 明确 skip。Tokio uninitialized/unsafe ReadBuf 能力只记录安全替代缺口；真实平台 driver、关联 executor/resource、严格 `copy`、buffered/split/duplex、io-std、epoll/IOCP/kqueue、三平台/TSan/LSan 和正式性能证据仍未实现，详见 [`architecture/io-memory.md`](architecture/io-memory.md) 与 [`architecture/io-composed-poll.md`](architecture/io-composed-poll.md) |
| `tokio::time::Instant` | `cio::time::Instant` | 部分实现 | 已感知冻结时钟并提供 duration/checked/saturating 运算；待完整运算符、格式与三平台证据 |
| `sleep`、`sleep_until`、`Sleep` | `cio::time::sleep`、`sleep_until`、`Sleep` | 部分实现 | 1 ms 向上舍入、runtime 检查、析构取消、deadline/is_elapsed 已实现；差分和 reset 竞态测试通过 |
| `Sleep::reset` | `Sleep::reset` | 部分实现 | 等待前、等待中、完成后 reset；TimerKey generation 与 epoch 双重拒绝旧 fire；待 TSan/三平台 |
| `interval`、`interval_at`、`Interval` | CIO 同名能力 | 部分实现 | 首 tick、固定计划、全部 reset API 已实现；C++ `tick()` 返回 owned Task，待 `poll_tick` 能力论证 |
| `MissedTickBehavior` | `cio::time::MissedTickBehavior` | 部分实现 | `burst`、`delay`、`skip` 和 5 ms 判断已通过 Tokio 差分；待更多溢出边界 |
| `timeout`、`timeout_at`、`Timeout`、`Elapsed` | CIO 同名能力 | 部分实现 | 先执行 value、同 deadline 优先、current-thread 失败分支先析构和 `into_inner` 已测；当前只接受 `Task<T>`，multi-thread loser 清理屏障和任意 awaitable 待实现 |
| `time::pause`、`resume`、`advance` | CIO 同名能力 | 部分实现 | current-thread 冻结、空闲自动推进、advance 后 yield 和 blocking job inhibit 已测；multi-thread 的 `start_paused`/`pause` 已拒绝 |

## 模块级覆盖审计

下表防止尚未逐项展开的模块被误认为不存在。每一行最终都必须展开成稳定公开项
级别的记录。

| 基线模块/能力 | 当前状态 | 下一项权威工作 |
| --- | --- | --- |
| `runtime` current-thread | 部分实现 | Handle/enter、I/O driver、shutdown 选项、metrics |
| `runtime` multi-thread | 部分实现 | 已有 G/M/P、本地/注入队列、runnext、随机半数窃取、searcher、合作预算、blocking pool 和关闭；待 LocalSet、remote inbox、idle-P 位图、分片 driver、metrics 与三平台动态证据 |
| `task` | 部分实现 | spawn_local、JoinSet、LocalSet、task-local、block_in_place |
| `time`、`test-util` | 部分实现 | 泛化 Timeout、完整 Instant API、multi-thread 分片 wheel、三平台与 sanitizer 证据 |
| `sync` | 部分实现 | Notify、Semaphore、Mutex、RwLock、Barrier、OnceCell、SetOnce、oneshot、bounded mpsc 前两个切片、unbounded mpsc、watch 和 broadcast 首片已有真实实现；仍需补全 channel 家族与跨平台证据 |
| `sync::mpsc` | 部分实现 | bounded 核心 send/recv、try/owned reserve、两类 Permit、WeakSender、关闭观察、FIFO 背压、close/drain 和 blocking bridge，以及 unbounded 同步 send/recv/Weak/关闭线性化已覆盖；待 batch/poll receive、三平台与正式性能证据 |
| `sync::oneshot` | 部分实现 | 单值、关闭、析构、取消和 blocking bridge 已覆盖；待 tracing、三平台和正式性能证据 |
| `sync::broadcast` | 部分实现 / 安全映射 | channel/send/subscribe/Weak、独立游标、精确 lag、resubscribe、drain/close、blocking bridge、计数与 cooperative 边界已覆盖；待极限计数、分配/复杂度优化、通用 Pending budget 退款、三平台/TSan/LSan 和正式性能证据 |
| `sync::watch` | 部分实现 / 安全替代 | 最新值、独立版本游标、changed/wait_for、关闭、重订阅和 owning Snapshot 已覆盖；待 send_modify/send_if_modified、Ref 差异收口、Predicate portable 边界、通用 Pending budget 退款、三平台/TSan/LSan 和性能证据 |
| `io`、`io-util`、`io-std` | 部分实现 / 安全映射 | 已有拥有 buffer/lease、安全 ReadBuf、AsyncRead/AsyncWrite concepts、端点 session、单次 read/write/vectored/flush/shutdown、read_exact/write_all、内存端点及 composed poll 基础；待严格 copy、OperationKey、完整组合/缓冲/stdio、平台 driver 与三平台证据 |
| `net` | 未实现 | TCP、UDP、Unix socket、Windows named pipe |
| `fs` | 未实现 | blocking pool 桥接与文件语义 |
| `process` | 未实现 | command、stdio、wait、kill、kill-on-drop |
| `signal` | 未实现 | Unix signal 与 Ctrl-C |
| `macros` | 未实现 | C++20 builder/函数/受控宏的能力映射 |
| `tracing` | 未实现 | task/runtime 诊断事件与关联 ID |
| `select`、`join`、`try_join` | 未实现 | 轮询顺序、公平性、取消和错误传播 |

## 当前结论

CIO 尚未完整对齐 Tokio 1.53.1。当前工作覆盖协程、current-thread runtime、
时间驱动、首个 multi-thread G/M/P 调度器、blocking pool、Notify、Semaphore、
Mutex、RwLock、Barrier、OnceCell、SetOnce、oneshot、bounded mpsc 前两个
垂直切片、unbounded mpsc、watch、broadcast 以及 I/O 拥有缓冲/内存端点与精确读写切片，且所有
“部分实现/安全替代”条目仍需按缺口补证。

仓库的 `cio.differential.tokio-1.53.1` 锁定精确 Tokio 版本，已在
Windows/MSVC 主机实际通过 148 项稳定契约：六项 task/runtime 契约，以及
1 ms 暂停时钟舍入、零 timeout 的立即 value 优先、同 deadline value 优先、
timeout 失败分支析构、完成后 Sleep reset、Interval 固定周期和 missed-tick
策略，再加上合作式预算让出、multi-thread spawn/join、blocking running abort
无效、blocking queued abort、冻结时钟 inhibit，以及 Notify permit 折叠、
FIFO/LIFO、broadcast 创建快照和取消转交，以及 Semaphore FIFO/批量头阻塞、
部分取消转交、关闭与 permit 操作，以及 Mutex FIFO、取消转交、非 poison、
owned map 与 blocking bridge，以及 RwLock 共享读/最大读者、写者优先 FIFO、
部分写者取消、非 poison、owned mapping 和原子 downgrade，以及 Barrier 零值/
单参与者、lazy 未 poll、可复用代/唯一 leader 和取消到达保留，以及 OnceCell
单初始化者、初始化取消后重试、try-error 后重试、Clone 独立、Some/None Debug
和 SetError Debug/Display，以及 SetOnce
等待唤醒、并发唯一 winner/值守恒、取消安全和 Clone 独立，以及 oneshot 的
发送接收、两端 drop、close 前后线性化、try_recv 状态、接收取消安全、
closed 唤醒、empty/terminated 转换、值析构守恒和 ready budget 让出，以及
bounded mpsc 的 FIFO/背压、send/reserve 公平与取消、Permit 容量、close/drain、
try 错误、Receiver drop、WeakSender、计数和错误格式，以及 closed 唤醒/取消、
channel identity、len/is_empty、try_reserve 错误、OwnedPermit send/release/
identity/生命周期、reserve_owned 关闭消费/取消和 try_reserve_owned 错误恢复。
unbounded mpsc 另覆盖 FIFO/多 Sender、send/try_recv 错误、close/drain、
Receiver drop、closed 多 waiter/取消、last Sender + Weak、identity/计数、
len/is_empty、ready recv budget、值析构、close/drop 后 Weak upgrade、recv
取消和 send/closed 不消耗 budget。watch 另覆盖初值 borrow、send/changed/
borrow_and_update、mark/has_changed、Receiver 独立游标与 subscribe、最后
Sender/Receiver 关闭、changed 取消安全、channel identity/计数、send_replace、
wait_for、值析构/Clone、错误格式和 ready cooperative 路径。broadcast 另覆盖
容量取整/精确 lag、失败发送后订阅、独立 Receiver、resubscribe、drain/close、
try 错误、发送线性化 Receiver 数、强弱计数、Weak upgrade、复制异常游标提交及
recv/closed cooperative 边界。I/O 另覆盖 ReadBuf regions/clear、partial
read/EOF/zero-capacity、partial/zero write、默认 vectored 首非空、flush/
shutdown 顺序、shutdown 终态、ready ext 不主动扣 cooperative budget，以及
read_exact partial/early EOF、write_all partial/WriteZero 和空输入 no-poll。
Barrier、OnceCell、SetOnce、oneshot、两类 mpsc、watch、broadcast 与 I/O 内存
当前切片的 Windows Debug/Release/ASan 测试均通过，核心源码契约检查覆盖 64 个文件；前三个切片的回归测试分别以
`repeat-until-fail` 连续执行 100/100 无失败，oneshot 修复 owning receive
生命周期后连续执行 300/300 无失败，bounded mpsc 也通过 Release 300/300 和
ASan 100/100，unbounded mpsc 也通过 Release 300/300 和 ASan 100/100。最新
watch 目标含 12 组测试、1000 轮 send/drop/subscribe 三方竞态和精确
budget/fresh-notification 边界；八条新增 cooperative 差分覆盖 changed
成功/错误、closed、wait_for 成功/错误的 128/129 边界，以及
changed/wait_for/closed 真实通知后 fresh poll 的预算扣费。修复 Snapshot
生命周期、Predicate 析构顺序
与计数溢出后通过 Release 300/300 和 ASan 100/100。45、51、62、84 与 98
项阶段的全套差分都曾重复通过
20/20；当前 148 项全套差分在 Windows Release/Debug 均通过 148/148，并各自
重复 20/20 无失败。Semaphore/Notify 最后-owner
锁内析构修复后，Notify、SetOnce、RwLock 又分别在 Windows Release 与 ASan
下连续通过 500/500。上述证据只覆盖对应差分项，不能外推到尚未实现的 API、
Linux/macOS 行为或完整 Tokio 对齐。watch 尤其仍缺 `send_modify`/
`send_if_modified`，且 owning Snapshot、move-only replace、Predicate portable
边界和 select Pending budget 退款均有已记录差异，不能把 21 项差分通过描述为
`Ref<T>` 的完整等价。`watch_fanout` dirty smoke 在固定 CPU 0–3、2 次预热/
5 样本下，w1 CIO/Tokio p50 为 3.189/0.702 ms（4.545x），w4 为
27.302/4.129 ms（6.612x），Asio 明确 skip；该数据只验证负载链路，不构成
clean revision、跨平台或正式性能结论。
`broadcast_fanout` dirty smoke 使用相同亲和性与样本设置，w1 CIO/Tokio p50
为 3.213/0.555 ms（5.792x），w4 为 25.710/3.565 ms（7.212x），Asio 明确
skip；w4 CIO RSD 17.26%，只说明当前 list/分配/线性查找/clone mutex 路径是
profiler 优先级，不能视为正式性能结论。
