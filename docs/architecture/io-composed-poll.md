# I/O 组合轮询与多 lane 唤醒基础

## 1. 范围

本切片只建立严格实现 Tokio 1.53.1 `io::copy` 所需的组合轮询基础，不提前公开
`copy`。顺序执行 `co_await read`、`co_await write` 无法表达 Tokio 的以下行为：

- writer Pending 时继续尝试把内部 buffer 读满；
- reader Pending 且已有写入时尝试 flush，避免读写互相等待；
- read、write、flush 可以分别 Pending，并由任一 lane 唤醒父组合 operation；
- 取消未胜出 lane 后必须先注销 waiter，再释放 buffer/session。

因此 CIO 需要一个父 task 内部的多 lane poll 模型，而不是把 child operation
spawn 成可分离 task。

## 2. 所有权模型

`TaskPollLane<T>` 唯一拥有一个 `Task<T>` coroutine frame。lane 不 spawn、
不 detach，也不允许并发 poll。第一次 `poll_once` 绑定独立
`ExecutionContext`，后续只恢复该 lane 自己保存的 resumable coroutine。

每个 lane 具有：

- `LaneKey`：只包含 slot 与 generation，不表达对象所有权；
- `LaneRegistration`：唯一拥有注册代，取消时先使 active 失效；
- `LaneWakeToken`：弱拥有 registration/gate，可跨线程复制并拒绝旧 generation；
- 独立 resumable 槽：read/write/flush 的 Pending 状态互不覆盖。

`TaskPollLane` 析构与 `cancel_now` 都先使 token 失效，再同步销毁 owning frame，
最后清空 resumable。child frame 中的 waiter、buffer lease 和 session control
因此在取消返回前完成逻辑注销。

## 3. 防丢唤醒

`ComposedWakeGate` 维护单调递增的原子 sequence。父组合 operation 在一轮开始时
记录 sequence，poll 所有可运行 lane；全部 Pending 后调用 `wait_after`。

`wait_after` 的握手顺序固定为：

1. 把父 coroutine 发布到父 `ExecutionContext::park`；
2. 以 acquire 语义重读 sequence；
3. sequence 未变化才真正保持挂起。

lane wake 先校验 active、slot 与 generation，再原子推进 sequence，并通过已绑定
的父 `ExecutionContext` 请求调度。这覆盖 wake-before-park、park-before-wake、
两个 lane 同时 wake 和旧 token 晚到 wake。该原型自己的 wake 热路径不取得
`std::mutex` 或 `Notify`；它仍依赖 runtime `ExecutionContext::wake` 的任务状态
合并与过期 `TaskKey` 拒绝能力，不能据此声称整个 runtime 已无锁。

## 4. 当前证据

Windows/MSVC 当前验证：

- C++20、`CXX_EXTENSIONS OFF`、`/W4 /WX` 编译通过；
- 13 组确定性测试覆盖 ready/void、Pending 恢复、wake-before-park、旧
  generation、双 Pending、双线程同时 wake、lane move、同步取消、late wake
  和异常清理、共享 cooperative root budget、ticket 完整快照退款与唯一 move
  权，以及 4-worker runtime 的外部线程 lane wake；
- Release 重复 300/300、Debug/MSVC ASan 重复 100/100；
- 核心禁止裸指针契约扫描覆盖 64 个文件。

这些证据只证明组合轮询基础的当前状态机，不证明 `copy`、平台 I/O late
completion、TSan 或三平台行为已经完成。

## 5. 严格实现 copy 的下一步

后续 `copy` 必须继续对齐固定 Tokio 源码，而不是只保证最终字节相同：

- 内部 buffer 固定为 8 KiB，并持有 read/write 两个 endpoint session；
- writer Pending 时允许 read lane top-up；
- 空 buffer 上 reader Pending 且 `need_flush` 为真时 poll flush lane；
- EOF 后必须 flush，但不得 shutdown writer；
- 非空 write 返回零映射 `WriteZero`，计数使用无符号 64 位并检查溢出；
- 每个 fresh parent poll 只取得一张 cooperative progress ticket；Pending 且
  没有进度时退款，真实 read/write/flush/error 才提交进度；
- reader/writer/flush 的错误、父取消和 winner 切换都必须同步 cancel
  outstanding lane；
- IOCP 等 completion 后端必须由 generation 化 `OperationKey` tombstone
  保持 native operation 与 buffer lease，不能因 coroutine lane 销毁就提前
  释放 ABI 资源。

上述语义和差分测试通过前，兼容矩阵中的 `copy` 保持未实现。

当前另有 5 组 poll-native operation、10 组平台中立 OperationRegistry 与 8 组
CopyBuffer/native submission 测试。它们已经覆盖 move-only owning 输入输出、
代际 completion、取消后的 native 双栅栏、OS 调用前的 submitting 事务、同步
拒绝回滚、即时 completion 和 late completion buffer pin；但尚未把具体
Read/Write/Flush lane、writer Pending top-up、EOF flush 与真实平台 driver
组合成公开 `copy`，因此不改变上述状态。
