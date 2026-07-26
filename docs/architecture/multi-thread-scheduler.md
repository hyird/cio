# multi-thread G/M/P 调度器设计

## 范围与当前结论

本切片在 CIO 自有任务模型上实现首个可运行的 multi-thread scheduler：

- `Builder::new_multi_thread()` 与准确的 `worker_threads`；
- 每个 worker 一个执行资源 P 和一个 M 线程；
- 每 P 本地队列、全局注入队列、`runnext` 与随机工作窃取；
- `scheduled/running/notified` 原子调度状态机；
- 合作式预算、全局队列和 timer 的硬公平性检查；
- 跨线程 wake、abort 和有序 runtime 关闭；
- 显式 `PortableTask`、`Send`/`Sync` 与 owned factory 安全边界。

这是真实的端到端垂直切片，不是把多线程调用 `io_context::run()` 当作 Tokio
调度器。专用 blocking pool 已在后续切片补齐，但仍缺少 `LocalSet`、
`block_in_place`、remote inbox、idle-P 位图、quota-aware 默认并行度、分片
I/O/timer driver、完整指标和三平台动态验证，因此兼容矩阵保持“部分实现”。

## portable task 安全边界

C++20 不能像 Rust 编译器一样证明整个协程帧满足 `Send + 'static`。CIO 采用
默认拒绝的显式边界：

- 未知用户类型的 `send_traits` 和 `sync_traits` 默认为 false；
- `PortableTask<T>` 表示调用方已审计整个协程帧可跨 worker 恢复；
- `task::owned(factory, args...)` 要求无捕获 factory、全部 owned 参数满足
  `Send`，并在 worker 第一次 poll 时创建用户协程帧；
- `task::assume_portable` 是人工审计入口，不是自动推断；
- multi-thread `spawn` 和 `block_on` 拒绝普通 `Task<T>`。

trait 特化和 `assume_portable` 都是安全承诺：跨暂停点的局部值、嵌套 Task 和
可达状态仍必须由调用方审计。后续需要静态检查辅助和负面编译测试。

Tokio 的 multi-thread `block_on` 允许调用线程驱动一个非 `Send` 根 future，
同时 spawned task 仍要求 `Send`。当前 CIO 为避免未证明的线程迁移，根任务也
要求 `PortableTask`，比 Tokio 更严格，属于待收口的 API/能力缺口。

## G/M/P 与队列

`RuntimeState` 持有多个 `ExecutorCore`，每个 core 是一个 P；当前每个 P 固定
绑定一个 `std::thread` M。task 是 G，调度队列只保存
`TaskKey { slot, generation, runtime_nonce }`，不保存协程地址。

本地调度规则：

- worker 内部产生的 ready task 优先进入本 P 的 `runnext`；
- `runnext` 连续执行上限为 3，之后必须给普通本地队列机会；
- owner 从本地队列尾部取任务；
- 本地队列容量暂定 256；满时把最旧的 128 个 token 溢出到全局队列；
- 外部线程产生的 ready task 进入全局注入队列；
- thief 以 xorshift 随机起点选择 victim，从 victim 头部窃取约一半任务，
  其中一个立即执行，其余进入 thief 本地队列。

队列实现当前使用短临界区互斥，不宣称 lock-free。异步 worker 不会为了用户
同步原语阻塞；后续热路径优化必须基于基准和内存序证明。

## 无并发 poll 的调度状态机

每个 task control 使用三个原子位：

```text
scheduled -> running
running + wake -> running | notified
running | notified + Pending -> scheduled，并且只入队一次
running + Pending -> idle
running + Ready/Cancelled -> terminal
```

因此同一 task 不会被两个 worker 并发 poll。poll 期间出现的一个或多个 wake
合并成 `notified`，返回 Pending 后补发唯一 ready token，避免丢 wake 和重复
调度。旧 token 仍通过 slot/generation/runtime nonce 校验安全失效。

## 公平性与合作式预算

当前硬边界均写入调度循环，而不是只作为设计目标：

- 连续 61 次 poll 或 10 ms 后检查全局注入队列；
- 连续 61 次 poll 或 100 μs 后驱动到期 timer；
- `runnext` 最多连续执行 3 次；
- 每次独立 task poll 重置 128 单位合作式预算；
- `task::consume_budget()` 在预算耗尽后挂起并重新排队。

`consume_budget` 已与 Tokio 1.53.1 做差分，但 CIO 尚未把预算接入所有未来
channel、I/O 和同步原语，因此不能宣称完整的 cooperative scheduling 对齐。

## 搜索、停车与无丢失唤醒

空闲 worker 只有在 searcher 数量低于 `max(1, worker_count / 2)` 时才尝试
窃取，避免所有 worker 同时扫描。若仍无工作，worker 读取单调递增的
`work_sequence`，再使用带谓词的 condition variable 停车。

调度、timer reset、task 移除和关闭都会先以 release 递增序列再通知；停车方以
acquire 比较观察值。即使通知发生在进入等待之前，序列变化也使谓词成立，避免
sleep/wake 窗口丢失工作。后续 remote inbox 和 idle-P 位图会在不改变此可观察
语义的前提下减少锁竞争。

## timer、取消与关闭

multi-thread worker 已能共同驱动实时 timer。`start_paused` 和运行中的
`time::pause()` 都明确拒绝 multi-thread runtime，与 Tokio test-util 限制一致。
当前 timer wheel 仍由 runtime 共享，尚未分片。

`abort` 只发布幂等取消请求。目标 task 可能正在其他 worker poll，因此
multi-thread runtime 不会越线程立即销毁其协程帧；目标在 poll 边界观察请求，
完成局部析构后才发布 cancelled join 结果。runtime 关闭按以下顺序执行：

1. 停止接受新 spawn；
2. 向所有未完成 task 发布 abort；
3. worker 继续运行，直至 task slot-map 为空；
4. 发布停止序列并唤醒所有 worker；
5. join 所有 M，清理队列和 timer driver。

当前 `Timeout` 的同步失败分支析构屏障只在 current-thread runtime 闭环。
multi-thread 下 loser 可能正在其他 worker 执行，只能先发布取消；专用的异步
取消完成屏障尚未实现，因此 multi-thread Timeout 不具备完整 Tokio 语义证据。

## 已执行验证

Windows/MSVC 测试覆盖：

- multi-thread 拒绝未审计普通 `Task`；
- 显式 worker 数量；
- 600 个 owned task 的本地队列溢出、全局注入和跨 worker 执行；
- `runnext`/本地队列公平性；
- 单 worker 下 128 单位合作式预算；
- 实时时钟 timer；
- `start_paused` 与运行中 `time::pause` 拒绝；
- abort 析构先于 join 结果；
- runtime 析构取消永久暂停 task；
- 100 轮外部线程 wake 竞态。

锁定 Tokio `1.53.1` 的差分测试新增 `consume_budget_yields` 和
`multi_spawn_join`。Linux、macOS、TSan、LSan 和长时间高竞争 soak 仍需实际
执行；默认 worker 数目前只使用 `hardware_concurrency()`，也尚未验证 cgroup、
CPU affinity 和 Windows Job Object 配额。
