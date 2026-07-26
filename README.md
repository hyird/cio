# CIO

CIO 是面向 C++20 的生产级跨平台异步运行时，目标是在功能、公开 API 能力和
可观察语义上完整对齐固定版本的 Tokio。内部执行资源借鉴 Go runtime 的 G/M/P，
调度优化借鉴 lalinsky/zio，异步操作内核借鉴 Asio；这些来源均不能改变 Tokio
兼容契约。

项目当前处于基础垂直切片阶段。尚未完成的兼容项会在
[`docs/tokio-parity.md`](docs/tokio-parity.md) 中明确标记；在全部兼容矩阵、
跨平台 CI、sanitizer、压力测试和差分测试闭环前，不宣称完整对齐 Tokio。

## 当前基线

- 语言：C++20，禁用编译器扩展；
- Tokio：`1.53.1`，启用 `full`、`test-util`、`tracing`；
- 平台：Linux/epoll、Windows/IOCP、macOS/BSD/kqueue；
- 核心约束：公开 API 与核心所有权路径禁止裸指针和显式 `new/delete`。

准确的版本、feature 和范围定义见
[`TOKIO_BASELINE.md`](TOKIO_BASELINE.md)。

## 构建与测试

```shell
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

Windows 可直接使用 Visual Studio 生成器；Linux 和 macOS 可使用 Ninja 或
Unix Makefiles。构建目标强制声明 `cxx_std_20` 和 `CXX_EXTENSIONS OFF`。

可选验证：

```shell
# Clang/GCC：ASan + UBSan
cmake -S . -B build-sanitize -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCIO_ENABLE_ASAN=ON -DCIO_ENABLE_UBSAN=ON

# 固定 Tokio 1.53.1 差分；需要 cargo 与 Python 3
cmake -S . -B build-diff -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCIO_ENABLE_TOKIO_DIFFERENTIAL_TESTS=ON
```

`CIO_ENABLE_TSAN=ON` 必须使用单独构建目录，不能和 ASan/UBSan 混用。
Windows MSVC ASan 运行时目录需要位于 `PATH`；Windows ASan 不提供
LeakSanitizer，泄漏证据由 Linux ASan/LSan CI 补齐。

## 文档

- [`GOAL.md`](GOAL.md)：一句话项目目标；
- [`TOKIO_BASELINE.md`](TOKIO_BASELINE.md)：固定 Tokio 基线；
- [`docs/tokio-parity.md`](docs/tokio-parity.md)：唯一权威兼容矩阵；
- [`docs/architecture/runtime-blueprint.md`](docs/architecture/runtime-blueprint.md)：
  综合 Tokio、Go runtime、lalinsky/zio 与 Asio 的总体架构；
- [`docs/architecture/runtime-foundation.md`](docs/architecture/runtime-foundation.md)：
  协程与 current-thread runtime 的所有权、调度和取消设计；
- [`docs/architecture/time-driver.md`](docs/architecture/time-driver.md)：
  generation timer、时间轮、暂停时钟、Sleep、Interval 与 Timeout 设计；
- [`docs/architecture/multi-thread-scheduler.md`](docs/architecture/multi-thread-scheduler.md)：
  G/M/P、portable task、队列、工作窃取、合作式预算与关闭设计；
- [`docs/architecture/blocking-pool.md`](docs/architecture/blocking-pool.md)：
  专用 blocking worker、spawn_blocking、取消、冻结时间与关闭设计；
- [`docs/architecture/notify.md`](docs/architecture/notify.md)：
  单 permit、FIFO/LIFO、broadcast 快照和取消转交设计；
- [`docs/architecture/semaphore.md`](docs/architecture/semaphore.md)：
  FIFO、批量头阻塞、部分许可取消转交和 owned permit 设计；
- [`docs/architecture/mutex.md`](docs/architecture/mutex.md)：
  FIFO、owned guard、非 poison、blocking bridge 和稳定别名 mapped guard 设计；
- [`docs/architecture/rwlock.md`](docs/architecture/rwlock.md)：
  batch semaphore、写者优先 FIFO、owned mapped guard 和原子降级设计；
- [`docs/architecture/barrier.md`](docs/architecture/barrier.md)：
  lazy 首次 poll、可复用 generation、唯一 leader 和非 cancel-safe 到达语义；
- [`docs/architecture/once-cell.md`](docs/architecture/once-cell.md)：
  单初始化许可、失败/取消重试、owning snapshot 和安全独占修改设计；
- [`docs/architecture/set-once.md`](docs/architecture/set-once.md)：
  单次显式发布、广播等待、失败值返还和 cancel-safe 等待设计；
- [`docs/architecture/oneshot.md`](docs/architecture/oneshot.md)：
  单值传递、关闭线性化、owning receive、取消和 blocking bridge 设计；
- [`docs/architecture/mpsc.md`](docs/architecture/mpsc.md)：
  bounded mpsc 的 FIFO 背压、两类 Permit、owned reserve、关闭观察/排空、
  计数和 blocking bridge 设计；
- [`docs/architecture/mpsc-unbounded.md`](docs/architecture/mpsc-unbounded.md)：
  unbounded mpsc 的同步发送、逻辑 Weak、关闭线性化、无界内存与取消设计；
- [`docs/architecture/watch.md`](docs/architecture/watch.md)：
  watch 的独立版本游标、owning Snapshot、关闭/重订阅、取消和安全替代边界；
- [`docs/architecture/broadcast.md`](docs/architecture/broadcast.md)：
  broadcast 的独立游标、精确 lag、弱 Sender、锁外复制/析构和关闭语义；
- [`docs/architecture/io-memory.md`](docs/architecture/io-memory.md)：
  I/O 拥有缓冲、ReadBuf 独占租约、端点 session、内存端点、组合精确读写与关闭语义；
- [`docs/architecture/io-composed-poll.md`](docs/architecture/io-composed-poll.md)：
  多 lane 组合轮询、generation wake、同步取消与严格 `copy` 的前置契约；
- [`docs/architecture/io-operation.md`](docs/architecture/io-operation.md)：
  OperationKey 代际表、native-terminal 双栅栏、owning wake 与平台接入限制；
- [`docs/benchmark-methodology.md`](docs/benchmark-methodology.md)：
  Tokio/Asio/CIO 的统一负载、亲和性、原始样本和 profiler 证据规范。

## 当前验证边界

当前工作树已建立跨平台构建、sanitizer 和 Tokio 差分 CI 配置，但只有实际执行且
成功的作业才能成为兼容证据。本地已完成 Windows/MSVC Debug、Release、ASan
以及锁定 Tokio 1.53.1 的 148 项 Rust/C++ 差分；其中 RwLock 与 OnceCell
各六项，Barrier 与 SetOnce 各四项、oneshot 十一项、bounded mpsc 二十二项均已
通过，unbounded mpsc 另有十四项，watch 二十一项，broadcast 十四项，I/O
拥有缓冲、内存端点与组合精确读写新增十五项。
watch 新增的八条精确
cooperative 差分覆盖 `changed` 成功/错误、`closed`、`wait_for` 成功/错误的
128/129 边界，以及 `changed`/`wait_for`/`closed` 真实通知后 fresh poll 的预算
扣费。Barrier、OnceCell、
SetOnce、oneshot、两类 mpsc、watch、broadcast 和 I/O 当前切片的 Windows
Debug、Release、ASan 测试均通过；全套 CTest 为 Release/Debug 18/18、ASan
19/19，核心源码契约检查覆盖 64 个文件并通过。前三个切片的回归测试分别
连续执行 100/100 无失败，
oneshot、bounded mpsc 与 unbounded mpsc 的 Release 回归均连续执行 300/300
无失败，两类 mpsc 的 ASan 回归均连续执行 100/100 无失败。最新 watch
目标包含 12 组测试及 1000 轮三方竞态，修复 Snapshot 生命周期、Predicate
析构顺序和计数溢出后通过 Release 300/300、ASan 100/100。51、62、84、98
项阶段的全套差分曾重复通过 20/20；
当前 148 项全套差分在 Windows Release/Debug 均通过 148/148，并各自重复
20/20 无失败。Semaphore 与 Notify 的
最后-owner 锁内析构竞态修复后，Notify、SetOnce、RwLock 又分别在 Release 和
ASan 下通过 500/500。性能框架已加入严格等工作量的 `mpsc_bounded` CIO/Tokio
对照并完成 dirty smoke；该结果只验证链路，不是正式性能结论。Linux、macOS、
TSan 与 LSan 仍需 CI 实际运行，因此矩阵状态保持“部分实现”。
Tokio/Asio/CIO 性能对比框架已加入 RwLock 读、写、80/20 混合，以及 OnceCell
已初始化快路径、初始化竞争和 SetOnce 广播负载并完成 Release smoke 验证；
Asio 无语义等价项时明确 skip。正式性能结论仍要求 clean revision、固定亲和性、
足量样本与平台 profiler 证据。

broadcast 首片已实现 channel/new_sender、同步 send、独立 move-only Receiver、
recv/try_recv/blocking_recv、精确 lag、resubscribe、Sender/WeakSender 计数与
升级、closed、len/is_empty、关闭 drain 和 channel identity。12 组专项测试覆盖
多 Sender 总序、Weak/last-drop 竞态、hidden Sender borrow、未 poll operation
独占、复制异常、三类析构重入和 cooperative 精确边界；Release 重复
100/100、MSVC ASan 重复 50/50 无失败。它仍因极限 Receiver 上限、安全映射的
hidden-borrow count、`std::list`/逐消息分配、per-message clone mutex、通用
Pending budget 退款和三平台证据保持“部分实现”。

I/O 首片已实现 `OwnedBuffer`、`SharedBuffer`、`ConstBufferLease`、
`ConstBufferSequence`、完全初始化的安全 `ReadBuf`、`AsyncRead`/
`AsyncWrite` concepts、单次 read/write、默认 vectored write、flush、
shutdown、`read_exact`、`write_all` 与确定性内存端点。concept 还要求端点和
session 分别显式 opt-in 审计标记，不能让只有同形函数、却不满足共享 identity、
initiating-call 独占和 primitive 自拥有 control 语义的类型误通过。端点 session
覆盖整个组合 future，两个 partial primitive 之间也不释放排他权。
内存端点 14 组、精确读写 4 组专项测试覆盖未 poll 双租约、owner 先析构、并发 write/shutdown
initiating 压力、原生错误往返、部分读写、EOF/零容量、write-zero、shutdown
隐含 flush、组合操作期间的别名排斥、Pending 子 primitive 取消、空输入不启动
底层 primitive、跨 worker 恢复、外部线程 abort 和晚到 wake；内存端点通过
Release 300/300、ASan 100/100，精确读写通过 Release 100/100、Debug/ASan
20/20，并有十五项 Tokio 差分。多 lane 组合轮询基础另有 13 组状态机测试，
通过 Release 300/300、Debug/ASan 100/100。平台中立 OperationRegistry 有
10 组测试，poll-native operation 有 5 组，8 KiB CopyBuffer 与 native
submission 事务有 8 组；新增事务覆盖 OS 调用前封住 rollback、即时完成、
同步拒绝、取消后迟到完成和 shutdown gate。真实 epoll/IOCP/kqueue driver、
uninitialized 安全替代、严格 `copy`、TCP 与三平台证据仍未实现，因此只标记为
“部分实现”。
等工作量 `io_memory_ready` dirty smoke 固定每次 64 B read、CPU 0–3、2 次
预热/5 样本：1 worker 的 CIO/Tokio p50 为 61.090/4.538 ms（13.463x），
4 worker 为 45.094/1.899 ms（23.747x）；CIO 的 1/4-worker RSD 分别为
0.28%/1.86%。
两端都校验 100000 次调用、6400000 bytes 和逐字节 checksum；Asio 因没有
等价拥有式 ReadBuf/ready 内存端点明确 skip。该 dirty 数据只验证链路并把
endpoint/session stable-owner 分配、快照复制等路径列为 profiler 候选，不能直接认定瓶颈或形成
正式性能结论。

watch 的 `Snapshot<T>` 是 Tokio 锁持有型 `Ref<'_, T>` 的拥有式安全替代，
不是可观察等价实现；`send_modify`/`send_if_modified`、Predicate 的 portable
`Send` 边界和 future Pending 后 cooperative budget 退款仍未完成。watch 也
已加入等工作量 `watch_fanout` dirty smoke：固定 CPU 0–3、2 次预热/5 样本下，
1 worker 的 CIO/Tokio p50 为 3.189/0.702 ms（4.545x），4 worker 为
27.302/4.129 ms（6.612x），Asio 因无语义等价 API 明确 skip。该结果只验证
负载和比较链路，不能从 dirty revision 的小样本推导正式性能结论。

等工作量 `broadcast_fanout` dirty smoke 同样固定 CPU 0–3、2 次预热/5 样本：
1 worker 的 CIO/Tokio p50 为 3.213/0.555 ms（5.792x），4 worker 为
25.710/3.565 ms（7.212x）；4 worker CIO RSD 为 17.26%，噪声仍较高。Asio 因
无语义等价 API 明确 skip；这些数据只验证负载与报告链路，并提示后续 profiler
应优先检查逐消息分配、线性槽查找和 clone mutex，不能作为正式性能结论。
