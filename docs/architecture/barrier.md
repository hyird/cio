# Barrier 设计

## 对齐范围

本切片对齐 Tokio 1.53.1 的 `tokio::sync::Barrier` 与
`BarrierWaitResult`，并以适合 C++20 所有权模型的 `Barrier::Wait` 表达
`Barrier::wait` 返回的惰性 future。当前覆盖：

- `Barrier(n)` 构造；`n == 0` 与 Tokio 一样按 `n == 1` 处理；
- `wait()` 的 lazy 首次 poll、异步 rendezvous 和每代唯一 leader；
- 屏障完成后的 generation 重用；
- 已首次 poll 到达后取消不回滚的非 cancel-safe 语义；
- 可复制的 owned 共享值句柄、跨 worker 恢复和独立拥有的等待结果。

核心契约是：前 `n - 1` 个已首次 poll 的 `wait` 挂起，第 `n` 个到达者完成
当前一代、成为该代唯一 leader，并唤醒同代其余等待者。全部后续 `wait` 自动
进入下一代；`Barrier` 不需要重建或手动 reset。

本条目仍是“部分实现”。当前没有补齐 Tokio 的格式 trait、tracing 资源事件、
loom/TSan 和 Linux/macOS 动态证据，也没有足量跨平台性能数据。

## 惰性到达与代状态机

调用 `Barrier::wait()` 只创建一个拥有式 `Barrier::Wait`，不会修改屏障的
到达计数。只有 runtime 首次 poll 该 Wait、进入 `await_ready()` 时，当前
task 才提交本次到达。因此：

- 创建后未 poll 就析构的 Wait 不占参与者名额；
- Wait 的创建顺序不等于到达顺序，首次 poll 顺序才具有可观察意义；
- 同一个 Wait 只能被消费一次，重复等待会以 `logic_error` 拒绝。

共享状态维护参与者数、当前代到达数、generation 和该代专属 `Notify`。每次
到达都在短临界区内执行：

1. 若当前 task 不是第 `n` 个到达者，先创建该代的 owned notification，再增加
   到达数；先注册通知状态可避免“到达已提交但尚无等待对象”造成丢失唤醒。
2. 若当前 task 是第 `n` 个到达者，把旧代 `Notify` 移入完成状态，把共享状态
   切换到全新的 `Notify`，清零到达数并递增 generation。
3. 离开状态锁后才对旧代执行 `notify_waiters()`，因此不会在内部锁保护区恢复
   用户 task，也不会让旧代通知串入下一代。

`n == 0` 在构造时规范化为 1。因此 `Barrier(0)` 与 `Barrier(1)` 的每次
`wait` 都立即完成，且每次结果都是其所在代的唯一 leader。这一边界来自固定
Tokio 1.53.1，而不是 CIO 自行定义的扩展。

## 唯一 leader 与内存可见性

只有触发代切换的第 `n` 个到达者取得
`BarrierWaitResult{true}`；同代其余等待者在收到旧代通知后取得
`BarrierWaitResult{false}`。leader 只是标识本代唯一完成者，不赋予额外所有权、
固定线程或调度优先级。

到达状态在互斥区内提交，代完成通过 `Notify` 的发布/唤醒路径向等待者可见。
当前压力测试在每代 wait 前发布到达计数，并在恢复后验证所有参与者的到达都已
可见；每一代还单独统计 leader，必须恰好为 1。

`BarrierWaitResult` 是独立的可复制值，不借用 Barrier 或 task。原 Barrier
句柄和 Wait 析构后，结果仍可安全读取并跨线程传递。

## 取消语义

Tokio 1.53.1 明确规定 `Barrier::wait` **不是 cancel-safe**。CIO 保持这一
可观察语义：

- 尚未首次 poll 的 Wait 被取消或析构，不会计入到达数；
- 首次 poll 已提交到达后，task 被 abort、Wait 被析构或 runtime shutdown，
  已增加的到达数不会回滚；
- 取消的 task 不会取得 `BarrierWaitResult`，但它提交的到达仍可能帮助后续
  task 凑齐本代；
- 本代完成后照常清零计数并进入下一代，不把取消者迁移为下一代到达。

例如 `Barrier(3)` 中，两个 task 首次 poll 后其中一个被取消，再有一个 task
到达即可完成该代。仍存活的两个 task 中恰有一个观察到 leader。将取消者的
到达回滚、要求再增加一个参与者，反而会偏离 Tokio 契约。

runtime shutdown 也遵守同一规则：销毁已经挂起的 task 会注销其通知等待状态，
但不会撤销已提交到 Barrier 的到达。只要 Barrier 的共享句柄仍存活，后续
runtime 中的到达仍可完成该代。调用方必须据此设计参与者取消策略；如果业务
需要“取消后减少参与者”，应使用明确支持成员变更的其他协调协议，不能假定
Barrier 会自动补偿。

## C++20 所有权、生命周期与线程迁移

Tokio 通常通过 `Arc<Barrier>` 把屏障分享给多个 task。CIO 的 `Barrier` 本身是
可复制的 shared-value handle，复制等价于复制 `Arc<Barrier>`：

- 每个 Barrier 句柄拥有同一个内部状态的强生命周期；
- `Barrier::Wait` 继续拥有状态，不要求创建它的 Barrier 句柄存活；
- awaiter 也拥有等待操作，协程暂停期间不保存裸引用；
- `BarrierWaitResult` 不引用共享状态。

这些拥有式边界使 `Barrier`、`Barrier::Wait` 和结果可以进入已审计的 portable
task。等待 task 恢复时允许迁移到任意 runtime worker，不承诺线程亲和性。
共享状态的短临界区和 `Notify` 状态机承担并发同步，调用方不得依赖恢复线程与
首次 poll 线程相同。

公开 API 和核心状态不以裸指针或裸引用表达所有权。标准协程 handle 只作为
`await_suspend` 的受限值参数交给 CIO 的等待状态机，不保存为 Barrier 对象
所有权。

## 阻塞行为

`Barrier::wait` 是异步等待：未凑齐参与者时挂起当前 task，不让 runtime worker
在屏障条件上同步等待。其内部只使用同步互斥保护一次有界的计数、generation
和 Notify 交换；临界区不包含 `co_await`、用户代码、系统 I/O 或通知恢复，
但多个 worker 同时到达时仍可能短暂竞争这把内部互斥锁。

完成者在解锁后广播旧代通知，避免持锁恢复 task 和递归重入。Barrier 没有
`blocking_wait`；同步线程需要参与时，应在 runtime 中驱动 `wait`，不能把
普通阻塞调用伪装成 Tokio 的异步契约。

## 与 Tokio 内部实现及其他参考的边界

Tokio 1.53.1 内部使用同步状态锁、`watch` channel 和 generation 完成屏障。
CIO 使用同步状态锁、每代独立的 `Notify` 和 owned Wait 实现相同可观察语义。
内部数据结构不同不构成兼容差异；判定依据是 lazy 到达、代重用、唯一 leader、
取消不回滚、可见性和跨线程行为。

Barrier 复用 CIO 已验证的 Notify 广播快照与拥有式等待设计。这属于 Asio
“组合小操作并保持统一完成规则”的内部经验，但公开接口仍是 Tokio 风格
coroutine API。Barrier 不引入独立线程、I/O 后端或专用 scheduler，也不把短
同步状态锁解释成异步 Mutex。

## 验证证据与剩余门槛

本地状态机与压力测试覆盖：

- `Barrier(0)`、`Barrier(1)` 的立即完成和唯一 leader；
- 三参与者 rendezvous，以及未 poll Wait 不计入到达；
- current-thread 下 8 个参与者连续复用 32 代，每代唯一 leader 与可见性；
- 四 worker 下 48 个参与者连续复用 100 代的高竞争路径；
- 已 poll 等待者 abort 后到达数保留；
- runtime shutdown 销毁等待 task 后，其他 runtime 继续完成该代；
- `Barrier`、Wait 和结果的 `Send`/`Sync` 静态边界。

固定 Tokio 1.53.1 的 Rust/C++ 差分包含四项：

- `barrier_zero_single_leader`；
- `barrier_lazy_unpolled`；
- `barrier_reusable_unique_leader`；
- `barrier_cancelled_arrival_retained`。

当前 Windows/MSVC 主机的 Release 与 ASan 测试均已通过，Barrier 回归测试以
`repeat-until-fail` 连续执行 100/100 无失败；全套固定 Tokio 差分累计 41 项
通过。Windows ASan 不提供 LeakSanitizer，这些结果也不替代 Linux/macOS、
Clang/GCC、UBSan/TSan/LSan、模型测试和三平台高竞争验证。

性能工作仍需在固定 Tokio 版本、编译器、worker 数、CPU 亲和性、参与者规模与
generation 数下，对比 rendezvous 吞吐、p50/p95/p99/p999、CPU、分配、
park/unpark 和关闭延迟。当前没有足量 Barrier 跨平台 benchmark，因此不得从
现有功能测试推导性能结论。
