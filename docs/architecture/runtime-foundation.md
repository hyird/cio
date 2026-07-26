# 协程与 current-thread runtime 基础设计

## 设计范围

本切片实现 `Task<T>`、current-thread `Runtime`、`block_on`、`spawn`、
`JoinHandle`、`AbortHandle`、`JoinError`、task ID、竞态安全唤醒槽和
`yield_now`。current-thread 时间驱动和首个 multi-thread 调度切片已经实现，
分别详见 [`time-driver.md`](time-driver.md) 与
[`multi-thread-scheduler.md`](multi-thread-scheduler.md)。专用 blocking pool
也已作为后续切片实现，见 [`blocking-pool.md`](blocking-pool.md)；I/O 驱动
仍未实现，不能由上述切片的通过状态推导为已完成。

## 所有权

- `Task<T>` 独占协程帧，禁止复制；移动会转移唯一所有权。
- 标准协程句柄只在编译器协程 ABI 边界出现，立即包装为 CIO 的
  `CoroutineRef` 或 `CoroutineOwner` 值类型。
- `CoroutineRef` 只在审核过的包装层传递当前暂停位置，随即写回根
  `CoroutineOwner`；ready queue、waker 和 join waiter 不保存协程地址。
- runtime 的 generation slot-map 拥有 task control。ready queue 只保存
  `TaskKey { slot, generation, runtime_nonce }`，`JoinHandle` 析构只放弃结果，
  不取消 task。
- slot 复用前 generation 递增。旧 wake 或 ready token 解析失败后直接丢弃，
  不会访问新 task；跨 runtime 的 key 还会被 runtime nonce 拒绝。

## 调度与组合

`Task` 是 lazy coroutine。`spawn` 只把初始 poll 放入 ready queue，绝不在调用
栈内同步 poll。普通子 `Task` 的 await 则像 Rust Future 组合一样，在父 task 的
当前 poll 内使用 C++20 symmetric transfer 继续；子协程 final suspend 对称转移
回父协程。该机制既不把子 future 错误地变成独立调度 task，也不会让深层组合形成
普通函数递归栈。

current-thread runtime 只在 `block_on` 期间驱动 ready queue。背景 task 在
`block_on` 返回后保留但暂停，直到下一次 `block_on`；runtime 析构会取消全部
未完成 task。

`yield_now` 总是挂起当前 task。关联 `ExecutionContext` 先通过 `TaskKey` 把当前
暂停位置写回 owning coroutine wrapper，再只把 key 排到 ready queue 尾部。
与 Tokio 一样，调用方不得依赖它必然先运行某个特定 task，也不得依赖每次 yield
都驱动 I/O。

## 唤醒状态机

`WakerSlot` 用互斥保护以下状态：

1. 空闲：没有通知，也没有 waiter；
2. 提前通知：wake 先发生，下一次 register 消费通知且不挂起；
3. 已注册：只持有包含 runtime 弱句柄与 `TaskKey` 的执行上下文；
4. 已关闭：拒绝新 waiter，旧 waiter 不再被恢复。

注册 waiter 时，暂停位置先通过 key 写入 task record 的 owning wrapper；
`WakerSlot` 自身不保存 `CoroutineRef`。`wake` 与 `register_waiter` 在同一锁下转移状态，因此覆盖
wake-before-wait、wait-before-wake 和并发 close，不会丢失通知。真正调度在
锁外执行，且只发布 `TaskKey`，避免把用户 task poll 放进状态锁临界区。

## 取消、异常与析构

`abort` 只发布幂等取消请求。task 在下一次开始 poll 或从一次 poll 返回到暂停点
时处理请求：

- 如果 task 已完成，正常结果获胜；
- 如果 task 尚未开始或已暂停，runtime 销毁根协程帧；
- 销毁根帧会递归销毁当前持有的子 `Task` 和局部对象；
- 所有析构完成后，才把 `JoinError::cancelled` 发布给 join waiter。

协程内未处理异常由 promise 捕获为 `exception_ptr`。spawn task 的异常转成
`JoinError::panic`，不会逃逸到事件循环；`block_on` 根 task 的异常则在同步调用
边界重新抛出。

`JoinHandle` await 注册使用调用 task 的执行上下文。等待方被取消时，即使完成方
随后触发旧 wake，执行上下文也只携带旧 `TaskKey`；generation 解析失败后不会
访问已销毁帧或复用 slot 中的新 task。

## 线程与阻塞行为

本 runtime 的 task 只在调用 `block_on` 的线程执行，当前切片不发生 worker 间
迁移。`spawn`、`abort` 和 wake 入队使用互斥与原子状态，可从其他线程发布；
但 `Runtime::block_on` 不允许嵌套或并发调用。

runtime worker 不执行专门标记的阻塞工作。后续切片已提供专用
`spawn_blocking` pool；普通 task 内仍不得执行无界阻塞操作，详细所有权、取消
和关闭语义见 [`blocking-pool.md`](blocking-pool.md)。

## Asio 设计吸收

- async task 通过 `ExecutionContext` 显式关联执行器；
- 高层 Task await 由统一的“排队完成”规则组合，不把 handler-first 接口暴露给
  用户；
- 完成回调不在持锁区运行，也不产生递归 completion 链；
- timer 已复用相同的执行上下文，并使用独立的 generation `TimerKey`；
  后续 I/O 操作将沿用该契约。

## 与总体蓝图的当前边界

本基础切片建立了 slot/generation/runtime nonce、重复 ready 合并和 stale key
失效。后续 multi-thread 切片已把它扩展为 `scheduled/running/notified`
状态机、G/M/P、多 P 队列、runnext、searcher、工作窃取、portable task 边界和
合作式预算；其准确边界与剩余缺口以
[`multi-thread-scheduler.md`](multi-thread-scheduler.md) 和兼容矩阵为准。
