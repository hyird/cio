# CIO 仓库开发约束

## 项目使命

CIO 是 Tokio 的 C++20 对等实现。

项目不是简单模仿 Tokio 的命名，也不是对 Asio 的浅层封装。CIO 必须使用
C++20 协程实现独立的异步运行时，并针对固定版本的 Tokio，在功能、公开 API
能力和可观察语义上建立逐项对应关系。

CIO 必须综合借鉴 Tokio、Go runtime、lalinsky/zio 与 Asio：

- Tokio 是公开 API、能力和可观察语义的唯一兼容基线；
- Go runtime 提供 G/M/P、分布式调度、工作窃取、netpoll、停放/唤醒和诊断经验；
- lalinsky/zio 提供单搜索者、自适应 I/O 轮询、负载回迁、结构化任务组和
  跨平台事件循环经验；
- Asio 提供执行器、组合异步操作、关联上下文、缓冲区、串行执行器和平台后端
  隔离经验。

这些参考只决定内部实现质量，不能改变 Tokio 兼容契约。CIO 必须是独立运行时，
不得成为上述项目的包装层或移植层。

开始架构设计或实现前，必须先阅读 `GOAL.md` 和
`docs/architecture/runtime-blueprint.md`。

## 设计来源与优先级

发生设计冲突时按以下优先级裁决：

1. 内存安全、生命周期正确、无数据竞争、无唤醒丢失；
2. 固定 Tokio 版本的公开能力和可观察语义；
3. Linux、Windows、macOS 一致的公开契约；
4. 可诊断、可确定性测试和可维护性；
5. 有复现证据的吞吐、尾延迟、分配和能耗优化。

不得为了复制某个参考实现的内部细节而降低更高优先级目标。Go 的可增长栈和
异步抢占、zio 的裸指针侵入式结构与栈式 fiber、Tokio 依赖 Rust 编译器证明的
`Send`/`Sync`、Asio 的 handler-first 公开接口均不得直接照搬。

## 不可妥协的要求

### 语言和构建基线

- 只使用 C++20，不依赖 C++23 标准库功能。
- 使用 CMake，并声明 `cxx_std_20`。
- 必须设置 `CXX_EXTENSIONS OFF`。
- 支持 Clang、GCC 和 MSVC。
- 支持 Linux、Windows、macOS；BSD 可作为扩展平台。
- 平台相关代码必须封装在边界清晰的内部适配层。
- Boost.Asio、standalone Asio、libuv、Folly、Seastar 等不得成为 CIO
  运行时核心；允许将其用于设计参考、测试对照和基准比较。

### 禁止裸指针

- CIO 的公开 API 不得接收或暴露裸指针。
- CIO 核心代码不得声明 `T*`、`void*` 等裸指针类型。
- 禁止显式使用 `new`、`delete`、`new[]`、`delete[]`。
- 任何情况下都不得使用裸指针表达所有权。
- 可能超过当前同步作用域的异步工作不得捕获裸引用或
  `std::reference_wrapper`。
- 连续内存使用 `std::span`、容器或 CIO 的强类型缓冲区视图表达。
- 独占所有权使用值语义或 `std::unique_ptr`。
- 共享所有权使用 CIO 审核通过的侵入式智能句柄，或
  `std::shared_ptr`/`std::weak_ptr`。
- 可空非拥有关系使用经过校验的句柄、ID 或生命周期受限的视图类型。
- 标准协程句柄和操作系统原生句柄必须包装成 CIO 值类型，不能表示对象所有权。
- 操作系统 ABI 强制要求指针参数时，只允许在 `src/platform/` 内从持有生命周期
  的对象或 `std::span` 临时取得并立即调用；不得保存、返回或跨越该调用。
- 标准 PMR/allocator ABI 强制使用 `void*`、`T*` 时，只允许在
  `src/memory/abi/` 内出现，并必须立即转换成 `OwnedBlock`、`ResourceHandle`
  或等价的 CIO 拥有类型；裸地址不得进入 scheduler、task、operation、waiter、
  timer 或公开 API。
- `src/memory/abi/` 之外不得调用 `memory_resource::resource()`、直接传递
  `memory_resource*`，也不得保存 PMR 返回的裸地址。
- 外部 ABI 造成的例外必须在调用点用中文说明，并通过生命周期、重复释放、
  跨线程释放和 shutdown 测试验证；该例外绝不允许扩展成通用裸指针所有权。

### Tokio 完整对齐

- 在 `TOKIO_BASELINE.md` 固定准确的 Tokio 版本和启用的 feature。
- `docs/tokio-parity.md` 是唯一权威的 Tokio 兼容矩阵。
- 固定版本 Tokio 的每个稳定公开项都必须具备以下之一：
  - 已实现并通过语义测试的 CIO 对应项；
  - 能力完全相同但符合 C++20 表达方式的 API；
  - 经论证属于语言层面无法等价实现，并给出最接近且安全的替代方案。
- 编译通过不代表完成。相关功能必须验证取消、析构、唤醒、公平性、错误、
  关闭、背压和跨线程行为。
- 只要兼容矩阵仍有必需项缺失、部分实现、未经测试或语义不同，就不得宣称
  CIO 已完全对齐 Tokio。
- “API 对齐”指能力和行为逐项对应，不要求 Rust 与 C++ 源码级兼容。

## 必须借鉴的 Asio 设计

CIO 应吸收下列 Asio 设计的成熟经验，同时保持自身 Tokio 风格 API：

- 执行器与执行上下文分离，异步对象能够明确关联执行器。
- 关联执行器、关联分配器和关联取消上下文的思想。
- 异步操作由小操作组合成高层组合操作，并保持统一的完成规则。
- 使用 buffer sequence 或 `std::span` 风格视图表达分散/聚集 I/O。
- 使用 strand 所代表的“串行执行而非线程绑定”思想，提供 CIO 的串行执行器。
- I/O 对象与平台 reactor/proactor 后端解耦。
- 保证异步完成不会造成意外的无限递归或不可控的栈增长。
- 允许小对象优化、定制内存资源和减少热路径分配。
- 原生句柄采用 RAII，移动后状态和关闭行为必须明确。
- 组合操作必须明确中间状态所有权、部分读写、取消和最终完成语义。

不得直接照搬 Asio 的 handler-first 公开接口来替代 Tokio 风格协程 API。
不得把 `io_context.run()` 多线程执行误认为已经实现 Tokio 的任务调度器。
CIO 必须自行实现任务队列、唤醒器、协作预算、工作窃取、阻塞池和任务生命周期。

## 跨平台架构

必须设计统一的前端语义和可替换的平台驱动：

- Linux：首先实现 epoll；io_uring 作为可选高性能后端，不能改变公开语义。
- Windows：实现 IOCP，并正确处理完成式 I/O、取消和句柄关闭竞态。
- macOS/BSD：实现 kqueue。
- 文件系统、DNS、进程和其他可能阻塞的系统能力，在没有合适原生异步接口时，
  必须通过专用阻塞池实现。
- 平台差异必须在内部吸收；确实无法统一的能力才可标记为平台特有。
- 同一 API 在各平台必须保持一致的取消、超时、部分完成、EOF、错误和关闭语义。
- 原生错误必须转换为统一且保留平台信息的 CIO 错误模型。
- 字节序、套接字选项、信号、管道和进程行为必须有跨平台测试。

最低 CI 矩阵：

- Linux：Clang、GCC，Debug/Release。
- Windows：MSVC，Debug/Release。
- macOS：Apple Clang，Debug/Release。
- 每个平台都必须运行单线程 runtime、多线程 runtime、网络、定时器、同步原语、
  channel、取消和反复启停测试。

## 运行时必备架构

CIO 必须自行实现：

- C++20 协程 Task 和 promise 模型。
- 无唤醒丢失且竞态安全的 waker 与注册状态机。
- current-thread 运行时。
- multi-thread 运行时。
- 每个 worker 的本地队列和全局注入队列。
- 工作窃取、负载均衡及明确的公平性和饥饿策略。
- 协作式调度预算，防止持续 ready 的任务长期独占 worker。
- epoll、IOCP、kqueue 平台后端和统一驱动接口。
- sleep、interval、timeout、reset 和 missed-tick 语义完整的时间驱动。
- 专用且可管理的 blocking pool。
- runtime handle、enter guard、运行时上下文和优雅关闭。
- 暂停时间、手动推进时间等确定性测试能力。

异步 worker 不得执行无界阻塞操作。阻塞文件 I/O、阻塞 DNS、第三方阻塞 API
和长时间 CPU 计算必须使用 `cio::task::spawn_blocking` 或明确的专用执行器。

## 必须实现的 API 家族

- `cio::runtime`
  - `Runtime`、`Builder`、`Handle`、current-thread、multi-thread、
    `block_on`、上下文进入、指标和关闭控制。
- `cio::task`
  - `Task<T>`、`spawn`、`spawn_local`、`spawn_blocking`、
    `JoinHandle<T>`、`AbortHandle`、`JoinError`、`JoinSet`、`LocalSet`、
    task ID、task-local 和协作式让出。
- `cio::time`
  - `Instant`、`Sleep`、`sleep`、`sleep_until`、`Interval`、`interval`、
    `interval_at`、missed-tick、`Timeout` 和确定性时间。
- `cio::sync`
  - 异步 `Mutex`、`RwLock`、`Semaphore`、owned permit、`Notify`、
    `Barrier`、`OnceCell`、`SetOnce` 和适用的 owned guard。
- `cio::sync::mpsc`
  - 有界与无界 channel、预留/permit、背压、关闭、发送端计数和 Tokio
    对应的阻塞桥接能力。
- `cio::sync::oneshot`
  - 单值传递、发送端析构、接收端取消和关闭状态。
- `cio::sync::broadcast`
  - 独立接收游标、lag 检测、重新订阅、关闭和容量语义。
- `cio::sync::watch`
  - 最新值、版本跟踪、changed/wait、安全借用替代、计数和关闭语义。
- `cio::io`
  - `AsyncRead`、`AsyncWrite`、缓冲 I/O、split/owned split、copy、
    duplex、工具 source/sink 和扩展操作。
- `cio::net`
  - TCP listener/stream/socket、UDP、地址解析、受支持平台的 Unix
    domain socket，以及 Tokio 基线存在的 Windows named pipe 对应能力。
- `cio::fs`
  - 与 Tokio 可观察语义对齐的异步文件系统 API。
- `cio::process`
  - 命令、子进程、stdio、wait、kill 和析构时终止语义。
- `cio::signal`
  - 平台信号和 Ctrl-C。
- `cio::select`、`cio::join`、`cio::try_join`
  - 用 C++20 函数、builder 或宏实现，并对齐分支轮询、公平性、取消和错误语义。

## 任务、所有权和线程安全

- CIO task 不是操作系统线程。
- `Task<T>` 是惰性、唯一拥有协程帧的计算值；是否允许迁移由 spawn 边界决定，
  不能仅由返回类型名称推断。
- `spawn` 接受 CIO 的 owned task factory 或经过审计的 portable task，入口参数
  必须满足 CIO 定义的 `Send` concept/trait，task 暂停后允许在其他 worker 恢复。
- `spawn_local` 只在 `LocalSet`/local executor 上运行，保证整个生命周期留在
  同一线程，允许使用不满足 `Send` 的状态。
- `spawn` 与 `spawn_local` 必须在类型、API 和运行时断言上严格区分；禁止通过
  隐式转换绕过。
- 跨线程共享状态必须满足 `Sync` concept/trait，并使用显式同步机制。
- C++ 无法像 Rust 一样完整证明 `Send`/`Sync`，因此无法确认安全的类型默认拒绝。
- `Send`/`Sync` 的标准库白名单、用户显式 opt-in、owned factory、clang-tidy
  规则和 debug affinity 检查必须共同构成安全边界；不得声称它等同于 Rust 的
  完整编译期证明。
- CIO 提供的 thread-affine 类型必须自动把当前 task 标记为 local/pinned，或者
  在 portable task 中拒绝使用；portable task 禁止依赖普通 `thread_local`，
  上下文数据必须使用 task-local。
- 不得因为代码运行在 CIO 上就推断其线程安全。
- 优先使用单 task 所有权和 channel，减少共享可变状态。
- 必须区分“通过所有权避免互斥锁”和算法意义上的 lock-free。
- 没有算法证明和对应平台原子保证，不得声称实现无锁。

## Go 风格执行资源模型

CIO 的多线程 runtime 使用经过 C++20 约束改造的 G/M/P 模型：

- G 对应 `TaskRecord`：协程帧、状态机、取消、join、task-local 和预算；
- P 对应 `ExecutorCore`：本地 ready 队列、runnext/LIFO 槽、分配器缓存、
  timer/driver shard、随机源和局部指标；
- M 对应 `WorkerThread`：真正执行 task poll 的操作系统线程。

默认 `ExecutorCore` 数量根据 CPU 亲和性、容器/作业对象配额和显式配置确定，
不能只读取物理核心数。正常 worker 必须先取得一个 `ExecutorCore` 才能 poll
portable task。`block_in_place` 期间必须把 `ExecutorCore` 交给替代 worker；
无界阻塞工作仍进入独立且有背压的 blocking pool。

调度器必须满足：

- 每个 `ExecutorCore` 有固定容量本地环形队列、一个有连续命中上限的
  runnext/LIFO 槽，以及 runtime 级全局注入队列；
- task wake 使用 `scheduled/notified/running/completed` 状态合并重复通知，
  保证同一 task 不会并发 poll；
- worker 周期性检查全局队列、I/O、timer 和协作预算，不允许永久只消费本地队列；
- 空闲 worker 随机选择 victim 并窃取约一半 portable task；local task 和
  thread-affine task 永不被窃取；
- 外部线程提交、队列溢出和无法安全投递到本地的完成进入全局注入队列；
- spinning/searching worker 数量有严格上限，最后一个搜索者停放前重新检查
  本地、全局、I/O 和 timer；
- 发布工作与清除搜索/空闲状态之间必须有经模型测试验证的内存序，禁止发生
  “有工作但所有 worker 已睡眠”的丢失唤醒；
- 所有公平性目标必须有硬上限，不能只依赖统计平均值。

Go 的信号式异步抢占不能安全移植到任意 C++ 用户代码。CIO 使用 Tokio 风格的
协作预算、CIO awaitable 安全点和显式 `yield_now`；CPU 密集型循环必须主动检查
预算或进入 CPU/blocking executor。

## 自适应调度与 zio 经验

CIO 可吸收 lalinsky/zio 的下列机制：

- 以 task poll 耗时的 EWMA 动态计算下一次 I/O poll 预算；
- 只允许有限数量的 worker 充当 searcher，降低同时扫描队列和内核的竞争；
- 当 driver shard 负载明显失衡时，有限度地把 portable task wake 投入全局队列，
  让工作窃取完成回迁；
- task group 取消必须等待子 task 清理完成，且关键清理区允许 cancellation shield；
- blocking pool 的完成仍会唤醒 runtime，因此 shutdown 顺序必须先停止并回收
  blocking work，再销毁 I/O driver 和 executor。

不得复制 zio 的栈式 fiber、虚拟内存可增长栈、裸指针侵入式链表或特定 Zig ABI。
所有自适应参数必须有最小值、最大值和固定策略回退；确定性测试模式必须关闭
自适应和随机性。

## 任务与唤醒状态机

- 调度队列只保存带 generation 的 `TaskKey`，不保存裸指针或裸引用。
- runtime 的 slot-map/分段表拥有 `TaskRecord`；过期 `TaskKey` 必须因 generation
  不匹配而失效，防止 ABA 和 use-after-free。
- waker 保存 runtime 弱拥有句柄、`TaskKey` 和调度提示，不直接保存协程地址。
- 协程帧只由一个 owning wrapper 销毁且恰好一次；标准 coroutine handle 只能
  出现在审核过的包装层。
- wake 与 park 必须用双方握手状态机覆盖 wake-before-park、park-before-wake、
  wake-during-poll、abort-during-poll 和 completion-vs-cancel。
- task poll 结束前若观察到 `notified`，必须重新入队；否则才能提交 waiting 状态。
- join 完成只有在结果已发布、协程局部对象已析构且清理可见后才能被观察。

## 内存分配与 PMR

CIO 采用 **PMR-aware，而不是 PMR-everywhere** 的设计。优化优先级固定为：

1. 避免分配：值语义、固定容量环形队列、small-object optimization、内联状态；
2. 每个 `ExecutorCore` 的专用 slab/size-class pool；
3. 适合成组生命周期的 local monotonic arena；
4. 通过 CIO `ResourceHandle` 关联的 PMR/custom resource；
5. `new_delete_resource` 或等价系统分配作为兜底。

不得因为使用 PMR 就声称性能更高。`memory_resource` 是资源选择与传播协议，
性能由具体 resource、对象尺寸、线程竞争、释放位置和生命周期决定。

资源分层必须明确：

- `RuntimeSharedResource`：线程安全，供 JoinState、跨线程 channel、共享 waker 和
  runtime 级控制块使用；
- `ExecutorCoreResource`：由单个 P/Core 独占，供 TaskRecord、OperationRecord、
  WaiterRecord、TimerRecord 等热路径固定尺寸对象使用；
- `LocalResource`：供 `LocalSet`、单 task 或结构化 scope 的成组临时对象使用，
  分配结果不得越过其生命周期边界；
- `FallbackResource`：处理超大块、未知尺寸或专用池无法满足的请求。

跨线程规则：

- `synchronized_pool_resource` 或经证明的 CIO concurrent resource 才允许并发
  allocate/deallocate；
- `unsynchronized_pool_resource` 与 `monotonic_buffer_resource` 默认属于单个
  `ExecutorCore`/`LocalSet`，不得被多个线程同时调用；
- portable task 可以迁移，但 allocation 的 owner resource 不随 task 迁移；
- 非 owner 线程释放 core-local allocation 时，只能把 `AllocationKey` 投入
  owner core 的 remote-free queue，由 owner 批量回收；
- resource 必须活到其全部 allocation 和 remote-free 都回收之后；
- runtime shutdown 必须先停止生产、drain remote-free，再销毁 resource。

PMR 的公开与核心边界：

- 公开 API 只接受 `ResourceHandle`、`ResourceId`、`OwnedBlock` 或 allocator
  policy 值类型，不接受 `std::pmr::memory_resource*`；
- `std::pmr::polymorphic_allocator` 只是非拥有资源关联，不得被当成生命周期句柄；
- coroutine frame 的资源选择必须在 frame 创建前完成；owned task factory 应在
  目标 runtime 内创建 frame，使其使用正确的 core/runtime resource；
- `monotonic_buffer_resource` 只用于有明确整体释放点的 request/scope，不得作为
  无界长生命周期 task 的默认资源；
- task、channel、timer、TCP 和跨线程 wake benchmark 必须分别比较零分配、
  专用池、PMR pool 和系统分配，按真实数据选择默认值。

## I/O、timer 与组合操作总体规则

- 公开层以 Tokio 风格 coroutine/awaitable API 为主；内部异步操作采用 Asio
  经验，关联 executor、allocator/memory resource、cancellation context 和
  tracing context。
- 每个 I/O 操作都使用带 generation 的 `OperationKey` 与拥有缓冲区租约，
  状态统一为 created、submitted、completing/cancelling、delivered。
- readiness 后端必须重试系统调用，completion 后端必须消费完成结果，但最终都
  只能向上层交付一次终态。
- 组合操作必须传播关联上下文、维持中间状态所有权，并定义部分完成与取消保证。
- buffer view 可以使用 `std::span`，但异步操作必须同时持有 `BufferLease` 或
  等价拥有句柄；单独的非拥有 span 不得跨越暂停点。
- timer 采用每个 driver shard 的分层时间轮与远期溢出结构，统一支持 reset、
  missed tick、timeout、暂停时间和手动推进时间。
- `SerialExecutor`/strand 只保证任务不并发执行，不保证固定线程；
  `LocalSet` 才保证固定线程，两者不得混淆。

## 结构化并发

- 为 Tokio 兼容保留 `spawn`、`JoinHandle` 丢弃后 task detach、显式 abort 等语义。
- 同时提供 `task::scope`/`TaskGroup` 作为 CIO 推荐的安全默认：父操作返回前必须
  cancel-and-join 全部子 task，异常和取消按文档化策略传播。
- C++ 析构函数不能异步等待，因此结构化 scope 必须由 awaitable 组合器负责收口，
  不得伪装成仅靠 RAII 析构就完成异步 join。
- `select` 未胜出分支必须调用每个操作声明的取消契约；不能通过直接销毁未知状态
  假设取消总是安全。
- blocking task 一旦在操作系统线程开始执行，默认只能请求合作停止，不能声称
  已强制 abort。

## 锁与同步要求

- 同步锁 guard 绝不能跨越协程暂停点。
- runtime worker 不得阻塞等待竞争中的操作系统互斥锁。
- CIO 异步锁发生竞争时必须挂起 task，不能阻塞 worker。
- 异步锁必须定义排队、公平性和取消等待者的行为。
- guard 在正常返回、异常和协程销毁时都必须且只能释放一次。
- owned async guard 只有在所有权和跨线程语义明确且经过测试时才允许跨
  `co_await`。
- 取消不能丢失 permit、通知、消息或 wake-up。
- 所有同步原语必须测试：
  - wake-before-wait 与 wait-before-wake；
  - 每个暂停边界上的取消；
  - owner、sender、receiver 析构；
  - runtime 关闭；
  - 并发 close；
  - 规定的公平性和饥饿行为；
  - 跨线程唤醒；
  - 容量、溢出和 lag 边界。

## 取消、析构和关闭语义

- 协程在任意暂停点被销毁都必须安全。
- 销毁 JoinHandle 的行为必须与固定 Tokio 版本一致。
- 显式 abort 必须幂等，并能从 join 结果观察。
- `select` 未胜出分支必须按相应操作契约安全取消或分离。
- 每个异步操作都必须用中文说明 cancellation safety。
- runtime 关闭时必须明确处理普通 task、blocking task、timer、I/O 注册和
  已排队 wake-up。
- 完成、取消或关闭后，不得泄漏 task、waiter、timer、I/O 注册或 channel 内存。

## 错误与异常

- 必须统一规定异常跨越协程边界的处理策略。
- task 异常存入 JoinHandle 结果，不能逃逸出 worker 事件循环。
- 预期的运行错误使用强类型错误值或基于 `std::error_code` 的结果表达。
- 不依赖 C++23 `std::expected`；需要时实现 C++20 兼容的 CIO Result。
- 析构、取消清理、wake 路径和调度器内部必须不抛异常。

## 测试与验收

每项功能必须具备：

- 状态机单元测试。
- 可实现时的确定性竞态测试。
- 多线程压力测试。
- 每个 await 边界的取消和析构测试。
- 对细微行为编写固定 Tokio 版本的 Rust 对照程序，执行差分语义测试。
- Linux、Windows、macOS 对应的平台测试。
- 泄漏检查和 sanitizer 覆盖。

质量门槛：

- 受支持的 Clang/GCC 配置通过 ASan 和 UBSan。
- 支持的平台通过多线程 runtime 与同步模块的 TSan。
- Windows 根据需要加入 MSVC runtime checks 和 Application Verifier。
- 必须重复执行 runtime 创建/关闭和高竞争测试。
- benchmark 必须记录工作负载、连接数、负载大小、runtime 类型、worker 数量、
  CPU 亲和性、TLS 状态和机器配置。
- 性能结论必须来自真实测量，架构推断不能写成 benchmark 结论。

## 持续优化闭环

CIO 必须持续吸收 Tokio、Go runtime、lalinsky/zio 与 Asio 的新优化，但任何变更
都按以下闭环执行：

1. 先固定对照版本、平台、硬件、编译器、配置和工作负载；
2. 用 tracing、采样 profiler 和 runtime metrics 找到真实瓶颈；
3. 为单一假设编写 microbenchmark、混合负载和回归测试；
4. 先通过语义、竞态、取消、sanitizer 与确定性测试，再比较性能；
5. 同时记录 p50/p95/p99/p999、吞吐、CPU、分配、上下文切换、队列长度、
   steal 成功率、park/unpark、I/O 延迟和 blocking pool 饱和度；
6. 至少在 Linux、Windows、macOS 的相关后端验证，不以单平台结果推广全部平台；
7. 只有统计显著且没有重要回退的优化才保留，并把数据、脚本和结论写成中文；
8. 保留固定调度策略和可回退开关，防止自适应算法导致不可诊断的线上退化。

建议 benchmark 必须覆盖 spawn/join、yield、waker 风暴、channel、锁竞争、timer、
TCP/UDP 小包与大包、连接建立、文件 I/O、阻塞池、CPU/I/O 混合、短任务与长连接，
并与固定 Tokio、Go、zio、Asio 的等价工作负载进行可复现对照。不得只优化平均
吞吐而牺牲尾延迟、公平性、内存或关闭时间。

## 实现工作流

1. 固定 Tokio 基线并建立完整兼容矩阵。
2. 为一个垂直功能切片先定义可观察语义和测试。
3. 实现最小但完整的垂直切片。
4. 运行对应的单元、压力、取消、sanitizer 和平台测试。
5. 在兼容矩阵中记录实现与测试证据。
6. 所有门槛通过后才可标记完成。

优先完成端到端垂直切片，不要一次铺开大量空壳 API。禁止提交始终返回成功、
忽略取消或仅为通过编译而存在的占位实现。

## 初始交付顺序

1. 协程基础：`Task<T>`、waker、executor 契约、Result/Error。
2. current-thread runtime：`block_on`、`spawn`、JoinHandle、abort、shutdown。
3. 时间驱动：sleep、timeout、interval、暂停时间。
4. multi-thread scheduler：注入队列、本地队列、工作窃取、协作预算、跨线程唤醒。
5. blocking pool 和 `spawn_blocking`。
6. 核心同步：Notify、Semaphore、Mutex、RwLock。
7. channel：oneshot、bounded/unbounded mpsc、watch、broadcast。
8. I/O trait、组合异步操作和平台 reactor/proactor 后端。
9. TCP、UDP、本地 IPC、signal、process、filesystem。
10. 完整工具 API、诊断、指标、中文文档和兼容矩阵收口。

## 中文文档规范

- 所有项目自有文档必须使用简体中文。
- `README`、`GOAL`、架构文档、兼容矩阵、贡献指南、变更记录、示例说明、
  benchmark 报告和故障说明都必须使用中文。
- 所有公开 API 的 Doxygen/参考文档必须使用中文。
- 源码中解释并发、生命周期、内存序、取消和平台差异的设计注释必须使用中文。
- 标识符、标准术语、API 名称、代码和不可翻译的协议名称可以保留英文。
- 引用 Tokio、Asio 或操作系统原文时，必须同时给出中文解释。
- 每个公开异步 API 必须说明所有权、生命周期、取消安全、线程迁移和阻塞行为。
- 每个 Tokio 对应 API 都必须关联兼容矩阵条目。
- 示例必须覆盖 echo、chat、优雅关闭、timeout/select、有界 worker pool、
  broadcast、取消树和阻塞桥接。
- 不支持、部分支持、实验性和平台特有限制必须明确标注。

## 完成定义

只有同时满足以下条件，才能宣称 CIO 达成目标：

- 固定 Tokio 版本的公开 API 兼容矩阵不存在缺失或部分完成的必需项；
- 每个对应项都通过语义测试和差分测试；
- Linux、Windows、macOS CI、压力测试和 sanitizer 门槛通过；
- 公开 API 与核心所有权路径不存在裸指针；
- Asio 优秀设计已被有选择地吸收，同时 CIO 仍是独立 Tokio 风格运行时；
- 所有项目文档和公开 API 文档均为中文；
- 取消、析构、唤醒、公平性、背压和关闭行为已用中文记录并验证；
- benchmark 可复现，且没有把未测量的推断表述为事实。
