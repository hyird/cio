# blocking pool 与 spawn_blocking 设计

## 范围与基线

本切片对应 Tokio `1.53.1` 的 blocking pool 和 `spawn_blocking` 核心契约：

- current-thread 与 multi-thread runtime 共用同一种专用 blocking pool；
- `Builder::max_blocking_threads`，默认上限 512；
- `Runtime::spawn_blocking` 与 `cio::task::spawn_blocking`；
- 按需创建 blocking worker，FIFO 队列与并发上限；
- 排队取消、开始后不可强制 abort、异常 join 和析构关闭；
- current-thread 冻结时钟的 auto-advance inhibit；
- blocking worker 内嵌套 `spawn_blocking`。

当前空闲 worker 不会按 `thread_keep_alive` 回收，尚无
`spawn_mandatory_blocking`、`block_in_place`、`shutdown_timeout`、
`shutdown_background`、线程命名/栈大小/hooks 和完整 metrics，因此矩阵保持
“部分实现”。

## 所有权与 Send 边界

C++20 不能检查捕获闭包是否暗含引用。CIO 的安全公开形式为：

```text
spawn_blocking(无捕获 factory, owned_arg_1, owned_arg_2, ...)
```

- factory 必须是空类型；
- 参数全部按值存入 job，并满足 CIO `Send`；
- 输出不能是引用，并且 `void` 之外必须满足 `Send`；
- 未知用户类型默认不是 `Send`；
- 共享状态必须通过审核过的 `shared_ptr<T>` 且 `T: Sync`；
- API 不接受裸指针，也不会把引用或 `reference_wrapper` 保存到队列。

job 的调用包在提交线程完整构造后由 `shared_ptr` 稳定拥有。worker 只移动智能
句柄，不在 `noexcept` 执行路径再次移动可能抛异常的用户 factory/参数。

## pool 队列与扩容

pool 使用互斥保护 FIFO job 队列、live/idle worker 计数和 shutdown 状态：

1. submit 把 job 放入队尾；
2. 若 idle worker 足以覆盖当前队列，只通知一个 worker；
3. 否则在 live worker 低于上限时按需创建新线程；
4. 达到上限后 job 留在队列，由已存在 worker 依次消费；
5. worker 完成一个 job 后重新检查队列，不在异步 runtime worker 上阻塞。

默认上限 512 与 Tokio 1.53.1 一致。CIO 的异步 G/M/P worker 由独立线程集合
管理，不像 Tokio 当前实现那样借 blocking pool 启动 scheduler worker，因此
`max_blocking_threads` 只限制用户 blocking job 的并发数。

当前实现没有 idle keep-alive 回收，长寿命 runtime 最多保留配置上限数量的空闲
blocking 线程。这是明确的资源行为缺口，不把它描述成性能优化完成。

## job 状态机与 abort

每个 job 使用独立互斥保护：

```text
queued --worker 领取--> running --返回/异常--> finished
   |
   +--abort/shutdown--> cancelled
```

- `queued` 状态下，`JoinHandle::abort` 可以原子地阻止函数开始；调用包先析构，
  再发布 `JoinError::cancelled`；
- `running` 状态下 abort 是无操作，函数继续执行到返回或抛异常；
- 正常输出发布为 join 成功；
- 未处理异常捕获为 `JoinError::panic`，异常不会逃出 blocking worker；
- `JoinHandle` 析构仍只 detach，不取消 job。

Tokio 文档把排队取消描述为“可能阻止开始”，因为领取与 abort 竞态由先获得状态
转换者决定；CIO 保持相同的可观察竞态边界。

## runtime 上下文与嵌套提交

blocking worker 没有协程 `ExecutionContext`，但 Tokio 会在 blocking 线程进入
runtime handle。CIO 使用独立的弱 `ScopedRuntimeContext`：

- 不拥有 runtime，不阻止 shutdown；
- 不包含协程暂停位置和 task key；
- 允许 blocking 函数再次调用 `cio::task::spawn_blocking`；
- 不把 blocking 线程误标为异步 G/M/P worker。

当前尚未实现公开 `Handle::enter`，也尚未允许从 blocking 函数直接调用普通
`task::spawn`；这些能力在 runtime Handle 垂直切片收口。

## 冻结时间

Tokio 1.53.1 在 current-thread runtime 创建 blocking schedule 时增加
auto-advance inhibit 计数，并在 job 释放时递减。CIO 同样从成功提交前开始抑制，
在以下任一终态只释放一次：

- 排队 abort；
- shutdown 取消队列；
- 正常返回；
- 函数抛异常。

因此只要 blocking job 尚未终结，冻结时钟不会为了待处理 timer 自动跳跃；
blocking 完成后发布 work sequence，runtime 重新评估 ready task 和 timer。

## shutdown

普通 Runtime 析构执行：

1. 停止并取消异步 task；
2. 清理异步调度队列和 timer driver；
3. 标记 blocking pool shutdown；
4. 取消所有仍在队列中的非 mandatory job；
5. 唤醒 worker；
6. 等待已经开始的 blocking 函数返回并 join worker。

这与 Tokio 默认 Drop“可能无限等待已开始 blocking task”一致。当前没有超时或
后台 shutdown；调用方必须确保 blocking 函数可终止。若不能保证，应自行使用
可观察的停止标志或更高层专用服务线程。

## 已执行验证

Windows/MSVC 单元与竞态测试覆盖：

- current-thread 和 multi-thread 专用线程执行；
- `max_blocking_threads(0)` 拒绝、准确上限和三 job/两 worker 并发；
- 排队 abort 不执行函数；
- running abort 无效且返回正常值；
- 异常转换并保留 task ID；
- 单 worker 内嵌套提交；
- 冻结时钟在 blocking job 期间不自动推进；
- shutdown 等待 running job 并取消 queued job。

Tokio 1.53.1 差分新增：

- `blocking_running_abort_noop`；
- `blocking_queued_abort`；
- `blocking_paused_inhibits_time`。

Linux、macOS、TSan、LSan、线程创建失败注入和长时间资源回收测试仍需实际证据。
