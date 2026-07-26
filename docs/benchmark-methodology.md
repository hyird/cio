# CIO 性能对比方法

## 目的与边界

性能对比用于发现调度器、同步原语和 I/O 后端的实际成本，不能代替语义测试。
任何性能结论必须来自同一台机器、相同 CPU 亲和性、相同 Release/LTO 策略和
相同负载参数下的原始样本。架构推断、单次运行和调试构建都不能写成 benchmark
结论。

当前固定对照版本如下：

- Tokio `1.53.1`，feature 与 `TOKIO_BASELINE.md` 一致；
- standalone Asio `1.32.0`，由仓库 `vcpkg.json` 的 baseline 固定；
- CIO 使用被测 Git revision，报告必须记录 dirty 状态。

## 首批统一负载

| workload | 定义 | 可比 runtime |
|---|---|---|
| `schedule` | runtime 内的控制 task 批量提交 N 个空异步单元并等待全部完成 | CIO、Tokio、Asio |
| `yield` | runtime 内的 spawned task 连续重新排队 N 次 | CIO、Tokio、Asio |
| `mutex` | 多 task 争用锁，持锁期间 yield，再释放并 yield | CIO、Tokio |
| `rwlock_read` | 多 task 共享读；guard 内外各 yield 一次，观测并发读和跨 worker 恢复 | CIO、Tokio |
| `rwlock_write` | 多 task 排他写；guard 内外各 yield 一次，观测写竞争和 FIFO 调度 | CIO、Tokio |
| `rwlock_mixed` | 全局操作序列 80% 读、20% 写，覆盖队首写者阻塞后续读者 | CIO、Tokio |
| `once_cell_ready` | 已初始化 OnceCell 的 get 快路径；N 次操作均匀分摊到 `min(N, max(2, workers * 4))` 个 task | CIO、Tokio |
| `once_cell_init` | N 个 task 竞争初始化同一个空 OnceCell；唯一 factory 恰好执行一次并 yield | CIO、Tokio |
| `set_once_fanout` | 先建立 N 个 SetOnce waiter，再统一 set 并等待全部 waiter 恢复 | CIO、Tokio |
| `oneshot_wake` | 每个 operation 创建独立 channel 与 receiver task；确认全部 receiver 已实际 poll 到 Pending 后，同步发送全部值并等待全部恢复 | CIO、Tokio |
| `mpsc_bounded` | 固定容量 64；`min(N, max(2, workers * 4))` 个 producer 合计异步发送 `1..N`，单 consumer drain 至关闭 | CIO、Tokio |
| `watch_fanout` | `min(N, max(2, workers * 4))` 个 subscriber；每个版本须被全体确认后才发布下一版，禁止 latest-value coalescing 偷减工作量 | CIO、Tokio |
| `broadcast_fanout` | 固定容量 64；`min(N, max(2, workers * 4))` 个独立 Receiver，每条消息须被全体复制并确认后才发送下一条 | CIO、Tokio |
| `io_memory_ready` | N 次单次 64 B ready read 均匀分摊到 `min(N, max(2, workers * 4))` 个独立 reader task；每个 task 复用自己的 buffer | CIO、Tokio |

Asio 的 `schedule` 和 `yield` 是 executor 能力对照，不代表它具备 Tokio task
生命周期。Asio 没有 Tokio 风格异步 Mutex/RwLock，因此这些同步负载不生成
Asio 数值。Asio 同样没有语义等价的 Tokio 风格
OnceCell/SetOnce/oneshot/bounded mpsc/watch/broadcast channel；`once_cell_ready`、
`once_cell_init`、`set_once_fanout`、`oneshot_wake`、`mpsc_bounded` 和
`watch_fanout`、`broadcast_fanout`
在原始 JSON 与 Markdown 报告中明确记为 skip，不使用锁、future 或 executor
post 拼出虚假对照。

`io_memory_ready` 同样明确跳过 Asio。Asio 提供 buffer 和异步组合机制，但没有
与本负载等价的拥有式 `ReadBuf` 和内存 `AsyncRead` ready 端点；手写一个
`async_read_some` 内存适配器会把自定义适配器实现计入 Asio 数值，不能作为
同一公开能力的公平对照。

报告中的 `tasks` 只统计被测子 task，不包含每次样本的控制 root：

- `schedule`、`once_cell_init`、`set_once_fanout`、`oneshot_wake` 为
  `operations`；
- `yield` 为 1；
- `mutex`、三类 RwLock 和 `once_cell_ready` 为
  `min(operations, max(2, workers * 4))`。
- `mpsc_bounded` 为
  `min(operations, max(2, workers * 4)) + 1`，即 producer 数加一个 consumer。
- `watch_fanout` 为 `min(operations, max(2, workers * 4))`，即 subscriber
  数；控制 publisher 不计入。
- `broadcast_fanout` 使用同一 subscriber 公式；控制 publisher 不计入。
- `io_memory_ready` 为 `min(operations, max(2, workers * 4))`，每个 task
  拥有独立 reader 并复用一个 64 B buffer。

默认正式参数兼顾单次操作量与重复样本成本：`once_cell_ready=100000`，
`once_cell_init=10000`，`set_once_fanout=10000`，`oneshot_wake=10000`，
`mpsc_bounded=100000`，`watch_fanout=10000`，`broadcast_fanout=10000`，
`io_memory_ready=100000`。
`once_cell_ready` 是同步快路径；`once_cell_init`、
`set_once_fanout` 和 `oneshot_wake` 每个 operation 都会创建 task；
`oneshot_wake` 还会为每个 operation 创建独立 channel，不能仅凭相同 N 与
快路径横向排名。

`oneshot_wake` 的计时区间包含 channel 创建、receiver task 创建与调度、Pending
确认、同步 send、wake、恢复和回收。CIO 与 Tokio 都必须在发送前确认每个
receiver 的首次 poll 返回 Pending；最终必须验证接收值总和、失败数和完成数，
避免把漏唤醒或丢值的运行误记为快速样本。

`mpsc_bounded` 的 `operations` 精确定义为成功传过 bounded channel 的消息数，
不是 send/recv 调用数之和。CIO 与 Tokio 都固定容量 64、一个 consumer，并使用
相同公式决定 producer 数；各 producer 获得互不重叠的连续消息编号区间。计时
包含 channel 创建、sender clone、producer/consumer task 创建与调度、容量背压、
全部 send/recv、最后一个 sender 释放、consumer drain 至关闭和 task 回收。每个
样本必须验证发送失败数为零、接收数为 N、接收值 64 位校验和为
`N * (N + 1) / 2`，报告必须记录 `channel_capacity`、`producers`、
`consumers`、`workers` 和 `tasks`。

`watch_fanout` 的 `operations` 是发布版本数，`deliveries` 是
`operations * subscribers`。每个 subscriber 必须按顺序观察 `1..operations`；
publisher 在全部 subscriber 确认当前版本前不得发送下一版，因此 CIO 与 Tokio
不能通过合并中间版本减少工作。计时包含 channel/Receiver 创建、subscriber task
调度、send、广播唤醒、版本检查、确认通知和 task 回收；报告吞吐按 delivery
计算，并记录 `subscribers`、`deliveries`、`workers` 和 `tasks`。

`broadcast_fanout` 的 `operations` 是发送消息数，`deliveries` 同样是
`operations * subscribers`。每个独立 Receiver 必须按顺序复制并观察
`1..operations`；publisher 等待全体确认后才发送下一条，因此两端不会通过
lag、覆盖或丢 Receiver 偷减工作。计时包含 channel/Receiver 创建、subscriber
task 调度、每消息节点分配、广播唤醒、每 Receiver 复制、确认和回收。报告必须
记录固定 `channel_capacity=64`、`subscribers`、`deliveries`、`workers` 和
`tasks`，吞吐按 delivery 计算。Asio 明确 skip。

`io_memory_ready` 的一个 operation 精确定义为一次且仅一次 64 B read。CIO
使用 `MemoryReader(max_chunk=64)`，Tokio 使用独立 `Cursor` 与
`AsyncReadExt::read`；每个 reader 的源长度恰好覆盖分配给该 task 的 operation，
因此两端每次调用都返回 64 B，不使用 `read_exact` 或循环补齐来隐藏不同进度。
每个 task 复用自己的 64 B `ReadBuf`/数组；为了对齐 CIO 安全快照接口，两端都
为每次结果生成同样大小的拥有式快照并逐字节计算 checksum。样本结束必须验证
总 read 调用数为 N、总 bytes 为 `N * 64`、checksum 为 `N * 2016`，以及全部
task 完成且没有失败。报告吞吐仍按 operation/s 计算，并额外记录 `bytes` 和
`payload_bytes=64`；不能把 bytes/s 与其他 operation 负载混排。

Tokio 的负载必须先 `spawn` 再由 `block_on` 等待，避免 multi-thread runtime
直接在调用线程轮询根 future，而 CIO 根 task 已进入 worker 所造成的不公平路径。

## 构建

Windows 示例：

```powershell
$env:RUSTUP_HOME = 'F:\Dev\Rust\Rustup'
$env:CARGO_HOME = 'F:\Dev\Rust\Cargo'
$env:PATH = 'F:\Dev\Rust\Cargo\bin;' + $env:PATH

cmake -S . -B build-bench-vs -G 'Visual Studio 17 2022' -A x64 `
  -DCIO_BUILD_TESTS=OFF `
  -DCIO_BUILD_BENCHMARKS=ON `
  -DCMAKE_TOOLCHAIN_FILE=F:/Dev/Vcpkg/scripts/buildsystems/vcpkg.cmake `
  -DVCPKG_MANIFEST_FEATURES=benchmarks
cmake --build build-bench-vs --config Release --parallel
```

Linux 和 macOS 使用同一 manifest；只需把 toolchain 路径改为本机 vcpkg 路径。
Tokio 对照由 runner 使用 `Cargo.lock` 和 `--locked --release` 构建。

## Smoke 与正式测量

Smoke 只验证构建、负载和报告链路：

```powershell
python benchmarks/run_comparison.py `
  --cio build-bench-vs/Release/cio_benchmark.exe `
  --asio build-bench-vs/Release/cio_asio_benchmark.exe `
  --cargo F:/Dev/Rust/Cargo/bin/cargo.exe `
  --mode smoke --warmups 1 --samples 2 `
  --operations schedule=1000,yield=1000,mutex=200,`
rwlock_read=200,rwlock_write=200,rwlock_mixed=200,`
once_cell_ready=1000,once_cell_init=200,set_once_fanout=200,`
oneshot_wake=200,mpsc_bounded=1000,watch_fanout=200,`
broadcast_fanout=200,io_memory_ready=1000
```

正式测量必须使用 clean Git 工作树、至少 3 次 warmup、至少 10 个样本，并显式
固定 CPU 亲和性。例如：

```powershell
python benchmarks/run_comparison.py `
  --cio build-bench-vs/Release/cio_benchmark.exe `
  --asio build-bench-vs/Release/cio_asio_benchmark.exe `
  --cargo F:/Dev/Rust/Cargo/bin/cargo.exe `
  --mode measurement --affinity 2-5
```

runner 先设置自身亲和性，CIO、Tokio 和 Asio 子进程继承同一设置。macOS 没有
由本 runner 设置亲和性的稳定公开接口，正式报告必须记录未绑定限制，并使用
固定机器、低噪声环境和更多样本。

dirty 工作树只允许 `--mode smoke`。此时 JSON 的 `metadata.evidence_label` 和
Markdown 标题必须明确写为 `dirty_smoke`；该产物只证明构建、负载校验与报告
链路贯通，任何耗时、吞吐和相对 Tokio 数值都不得引用为正式性能结论。

## 必须记录的环境

JSON 原始报告至少包含：

- OS、CPU、逻辑核数和实际 CPU 亲和性；
- Git revision、dirty 状态；
- CIO/Asio 编译器和构建模式、Rust 工具链；
- runtime 版本与类型、worker 数、task 数、操作数、warmup 和全部纳秒样本；
- `io_memory_ready` 还必须记录总 `bytes` 与固定 `payload_bytes=64`；
- p50、p95、p99、p999、吞吐和相对标准差。

网络 benchmark 以后还必须记录连接数、payload、TLS、keep-alive、客户端工具、
CPU 亲和性和机器配置。不能把不同语义或不同完成条件的负载放在同一排名中。

## CPU、内存和分配证据

当前 runner 只生成 wall-clock 吞吐和延迟样本。正式性能验收还必须为相同命令
保存平台 profiler 证据：

- Linux：`perf stat` 记录 cycles、instructions、context-switches，
  `heaptrack` 或等价工具记录峰值内存和分配；
- Windows：WPR/WPA 或 Visual Studio Profiler 记录 CPU、working set 和
  allocation；
- macOS：Instruments 的 Time Profiler 与 Allocations。

profiler 原始文件、工具版本和对应 JSON 报告必须互相关联。为了遵守 CIO 禁止
裸指针的核心契约，不在 benchmark 源码中覆盖全局 allocator 来伪造分配计数。

## 结果解释

- smoke 报告一律不得用于性能结论；
- RSD 明显偏高时必须扩大样本或排除环境噪声后重测；
- 只报告测得工作负载，不外推未实现的 I/O 或平台后端；
- 优化前后必须保留相同配置的原始 JSON，不能只保存汇总表；
- benchmark 回归不能降低取消、公平性、关闭或跨线程语义。
