# current-thread 时间驱动设计

## 范围与基线

本切片对应 Tokio `1.53.1` 的 `time` 与 `test-util` 核心能力，已提供：

- `Instant`、`Duration`；
- `Sleep`、`sleep`、`sleep_until`、`deadline`、`is_elapsed`、`reset`；
- `Interval`、`interval`、`interval_at`、`tick`、四种 reset API，以及
  `Burst`、`Delay`、`Skip` missed-tick 策略；
- `Timeout`、`timeout`、`timeout_at`、`Elapsed`、`into_inner`；
- `pause`、`resume`、`advance` 与 `Builder::start_paused`。

该实现已接入 current-thread runtime，也能由 multi-thread worker 驱动实时时钟
timer；冻结时钟与 test-util 推进只允许 current-thread runtime。multi-thread
分片时间轮、时间指标、任意 awaitable 的 `Timeout` 泛化和三平台 CI 证据仍未
完成，因此兼容矩阵保持“部分实现”。

## 时钟与 1 ms 语义

每个 runtime 拥有独立单调时钟：

- 正常模式由 `std::chrono::steady_clock` 推进；
- 冻结模式保存 CIO `Instant`，不读取墙上时间；
- `advance` 一次性跳跃时钟，然后执行一次 `yield_now`，不等待跨过的 timer
  对应 task 全部完成；
- 冻结时如果 ready queue 为空，runtime 自动推进到最近 timer deadline。

timer source 以 runtime 创建时刻为原点。普通 `now` 向下换算为毫秒 tick，
deadline 向上换算到毫秒末端；因此 1 ns sleep 在冻结时钟下推进 1 ms。
`Builder::new_current_thread()` 默认不启用 timer，必须调用 `enable_time` 或
`enable_all`。在 runtime 外创建 `Sleep`，或在未启用 timer 的 runtime 中创建
`Sleep`，均抛出逻辑错误。

## TimerKey、slot-map 与时间轮

driver 不保存协程地址。每个 timer 使用：

```text
TimerKey { slot, generation, runtime_nonce }
```

- `slot` 定位 runtime 所有的 timer record；
- `generation` 使取消、完成和 reset 后的旧 wheel 条目失效；
- `runtime_nonce` 拒绝其他 runtime 的 key；
- timer record 只持有 `TimerWaitState` 的 `weak_ptr`；
- 到期后通过 `ExecutionContext` 发布 owning task 的 `TaskKey`。

时间轮由六层组成，每层 64 槽，最低层粒度为 1 ms。超出 `2^36` ms
覆盖范围的 deadline 进入 overflow，接近后重新归入时间轮。本正确性切片每次
driver 周期会整理所有非空槽，复杂度仍是 O(已登记 timer 数)；位图跳跃、
分片 wheel 和热路径基准属于后续性能切片，当前文档不把架构预期表述为性能结论。

取消只释放 slot 并递增 generation，不必从每个 bucket 中搜索旧 key。旧条目在
后续整理时因 generation 不匹配而丢弃。这同时覆盖 reset、析构、跨线程 reset
和到期处理之间的竞态。

## Sleep 所有权和取消

`Sleep` 移动专属并拥有 `TimerWaitState`；driver 仅弱引用该状态。首次等待才登记
timer，析构会取消登记。`reset` 可以在首次等待前、等待中或完成后调用：

1. 递增状态 epoch；
2. 使旧 `TimerKey` 失效；
3. 安装新 deadline；
4. 旧 fire 即使已经从 wheel 取出，也会因 epoch 不匹配成为无操作。

deadline 已经落后于 driver elapsed tick 时，首次等待或 reset 会同步进入
elapsed 状态；否则由 driver 唤醒。公开异步路径不捕获裸引用，暂停位置只写回
拥有协程帧的 task record。

## Interval 计划与 missed tick

`Interval::tick()` 返回拥有共享 `IntervalState` 的 `Task<Instant>`，避免成员
协程把 `this` 或引用跨越暂停点。第一次 tick 使用创建时刻，之后返回的是计划
时间点而不是实际唤醒时间。

完成 tick 后只更新内部 Sleep deadline，不提前登记下一次 timer，防止无人等待
Interval 时冻结时钟被意外自动推进。missed tick 超过 5 ms 才应用策略：

- `burst`：继续使用上一个计划时间加 period，快速追赶；
- `delay`：从当前时间重新计算一个完整 period；
- `skip`：跳到相对原计划的下一个 period 整数倍。

取消未完成的 tick 不推进计划 deadline，下一次 tick 仍等待原时间点。同一
Interval 的并发 tick 在 C++20 中无法由借用检查器静态拒绝，因此 CIO 运行时
检查并拒绝第二个并发等待者。

## Timeout 轮询与失败分支析构

Tokio 规定每次先 poll 被包装 future，再检查 timeout；立即完成的 future 必须
优先于零时长 timeout。当前 CIO 组合实现按以下顺序工作：

1. 先把 value task 放入 ready queue；
2. 再放入 deadline task；
3. 同 deadline 的 timer 也按 value、deadline 的登记顺序发布；
4. 胜者通过共享状态只发布一次结果；
5. current-thread 下，失败分支使用仅限 runtime 内部的立即取消屏障，先销毁
   协程帧和 timer，再唤醒 Timeout 等待者。

公开 `AbortHandle::abort` 仍然只是幂等取消请求；立即取消入口没有暴露给用户。
multi-thread 下 loser 可能正在另一个 worker poll，目前只能发布合作式取消，
尚无等待其清理完成的异步屏障；因此失败分支析构顺序证据只覆盖 current-thread。
当前 `Timeout` 只接受 CIO `Task<T>`，尚未泛化到任意 C++ awaitable，整体保持
部分实现状态。

## blocking job 与自动推进

current-thread 冻结时钟已与专用 blocking pool 联动。每个已提交且尚未终结的
blocking job 都持有一个 auto-advance inhibit；排队取消、正常完成、异常或
shutdown 清理时只释放一次。因而 runtime 可以等待真实阻塞 I/O，而不会先把
虚拟时间跳到未来 timer。对应设计与差分证据见
[`blocking-pool.md`](blocking-pool.md)。

## 已执行验证

Windows/MSVC 本地测试覆盖：

- 1 ns 舍入、正常到期、过去 deadline、实时等待和冻结时钟自动推进；
- reset 前后、完成后 reset、旧 generation、跨线程 reset；
- 析构取消、父 task 取消、runtime 关闭和 timer slot 清理；
- 六层时间轮范围外三年 deadline；
- `advance` 不等待 timer task 完成；
- Interval 首 tick、固定周期、三种 missed-tick 策略和 tick 取消安全；
- timeout 立即成功、到期、同 deadline、异常和失败分支析构顺序。

锁定 Tokio `1.53.1` 的 Rust/C++ 差分现有 22 项，其中 7 项覆盖纯 time 行为，
另有 1 项覆盖 blocking job 对冻结时钟自动推进的抑制。
Linux、macOS、TSan 与 LSan 仍只有 CI 配置，没有本地成功证据。
