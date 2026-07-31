# 仓库开发规范

本规范适用于整个仓库。

## 项目

cio 是仅支持 Linux 的 C++20 协程运行时。公开模型刻意保持小：任务、任务派生、
channel、`select`、结构化任务组、取消、挂起任务的同步原语、定时器、非阻塞
socket 与阻塞线程池。实现使用 worker 本地调度与 reactor 所有权，公开 API 不暴
露 executor 或 completion token。

除非任务明确要求 API 变更，否则保持公开名字、签名与可观察语义不变。
`cio::detail` 内的内部类型可以改动，但公开头文件被下游直接编译，属于兼容性
表面。

## 仓库地图

- `include/cio/`：公开的、以模板为主的 API
- `include/cio/detail/`：私有的调度器/reactor/定时器/队列契约
- `src/`：非模板运行时实现；`src/tls.cpp` 仅在 `-DCIO_TLS=ON` 时构建，是唯一
  有外部依赖的文件
- `tests/`：由 CMake 自动发现的测试可执行文件
- `examples/`：公开 API 的编译与用法示例
- `bench/`：C++ 微基准与相互隔离的 Go/HTTP/echo 对比
- `cmake/`：`find_package(cio)` 的包配置模板与 pkg-config 的 `cio.pc` 模板
- `fuzz/`：消费不可信字节的解析器的 libFuzzer harness 与语料
- `.github/workflows/ci.yml`：下述验证门，每次推送都会运行

本文件是唯一的设计文档。`README.md` 面向用户介绍这个库；改动运行时必须遵守
的一切都在这里。

不要提交生成的构建树、基准可执行文件、sanitizer 输出、Python 缓存或临时原始
结果。相关模式在 `.gitignore` 中。

## 构建与验证

使用树外构建：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

sanitizer 配置：

```sh
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=asan
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug -DCIO_SANITIZE=tsan
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

CI 在 GCC 与 Clang 上运行 Release 加两套 sanitizer、shared 与 metrics 两个构
建变体、一个 arm64 构建、解析器的短时 fuzz、一次非 sanitizer 浸泡、安装并
`find_package` 的往返、一个 `add_subdirectory` 消费方以及空白检查。GitHub
Actions 按提交 SHA 钉定，由 dependabot 推进。本地验证与改动的规模成比例：

- 文档或脚本：语法/帮助/冒烟检查，加 `git diff --check`。
- 公开头文件：Release 构建、完整 CTest 与 `test_api_surface`。
- 调度器、队列、同步、定时器或生命周期代码：完整的 Release、ASan/UBSan 与
  TSan 三套。
- 网络/reactor 代码：三套全跑，另加相关竞争测试的重复运行和一次非 sanitizer
  浸泡。
- 性能改动：先过正确性套件，再跑冻结二进制的交替 A/B 基准。

`test_soak` 在 CTest 里默认短跑。更长的浸泡放在优化过的非 sanitizer 构建里：

```sh
./build/test_soak 90
```

ASan 与 TSan 可能使对称协程转移不再是原生尾调用。不要拿长时间的 sanitizer
浸泡当作发布运行时的栈测试。

Go 对比是独立模块：

```sh
cd bench/go-core
go test ./...
```

## 设计约束

以下已成定论。重开任何一条需要新的证据，而不是新的论证。

- **仅 Linux 与 epoll。** 不做 io_uring、kqueue 或 IOCP 后端。常规文件与系统
  解析器调用改走有界阻塞执行器。
- **公开头文件不出现 executor、亲和、分片、completion token 或迁移控制。**
  公开 API 里没有 processor、shard 或线程对象；`RuntimeOptions` 不添加亲和或
  队列旋钮。内部调优常量放在 `detail`，由基准而非应用选择。
- **不做 Go 式 G/M/P processor 交接。** 实测的差距是共享调度状态上的缓存停
  顿，G/M/P 消除不了它。
- **无栈协程。** 不是有栈 fiber 运行时；对称转移依赖尾调用。
- **公开 API 跟随 Go。** Go 有对应物的地方，用 Go 的名字与形状：
  `net.Conn`/`PacketConn`/`Listener` 以 concept 表达，`io.Reader`/`Writer`/
  `ReadFull`/`Copy`（目标在前），`net.Error` 的错误分类器，
  `sync.WaitGroup`/`Mutex`，`os.File`，`tls.Config`/`tls.Conn`，
  `Resolver.PreferGo`。Go 没有对应物的地方——`Task`、`Runtime`、`Result`——
  不要发明虚假的对应。
- **每个公开类型只暴露其 Go 对应物的方法集。** `net::Socket` 是实现基类：成
  员全部 protected，由各具体类型逐个重导出自己那份——监听器因此没有
  `remote_addr()`。Go 没有导出基类，公开继承一个「什么都有」的基类正是方法集
  漂移的来源。配置字段直接挂在对象上（`Resolver::prefer_builtin`、
  `Dialer::timeout`），不做单独的 options 结构去包一层。
- **用 concept，不用虚接口。** Go 用得起运行时接口；socket 快速路径背不起虚
  表。泛型在编译期完成。
- **截止时间与取消活在连接上，不在调用上。** 这是 `read`、`write` 与
  `accept` 不带取消参数的原因：取消绑定到描述符，在系统调用准入处与截止时间
  并排检查。
- **取消必须关闭，不能只是放弃。** 被取消的操作必须关闭它的描述符，每个派生
  出的尝试都必须在父任务返回前被 join。detach 会让任务 park 在 socket 上直到
  内核放弃重试，而运行时若先关停则直接泄漏协程帧——关停不会展开 park 在
  channel 或 socket 上的任务。
- **`io::Writer` 遵循 Go 的 `io.Writer` 契约。** `write()` 无错误即写满；无
  错误的短写是坏 writer，`copy` 对它报 `EIO`。因此库内不存在 `write_all`——
  每个调用方自带重试循环的世界正是这条契约要消灭的。
- **TLS 的 OpenSSL 上下文属于 `Config`，不属于连接。** 首次使用时编译一次，
  由该 `Config` 的所有副本共享——共享搭在 `Config` 的拷贝语义上，所以公开签名
  不需要出现任何指针或句柄类型。证书文件因此只读一次而不是每条连接一次，服务
  端也不必显式共享 ticket 密钥就能复用会话。不要退回每条连接各建上下文：那会
  把文件 I/O 放回 accept 路径，并让进程内会话复用静默失效。
- **客户端会话缓存存的必须是会话的副本。** OpenSSL 在 TLS 1.3 下把新 ticket
  会话同时设为该连接自己的会话，连接未经 close_notify 拆除时会把它标记为不可
  复用，直接毒化缓存条目——而且没有任何错误信号，只表现为握手成本莫名偏高。
- **不做 context value。** 取消作用域携带完成信号、错误与截止时间。以不透明
  类型为键的映射是依赖注入机制，不是取消机制。
- **内置解析器不做 DNS 缓存、DNSSEC 与 search 列表**，除 stub 解析外不实现用
  户态协议。它只读 `/etc/hosts` 与 `/etc/resolv.conf`；NSS 模块是系统后端的
  职责。

## 运行时不变量

以下是承重契约，不是实现建议。

### 可运行任务的所有权

- `runnext` 仅归所有者，提供直接交接的快速路径。
- 本地可运行 FIFO 只有一个生产者（其所有者），可由所有者或窃取者消费。远程
  生产者绝不能向它推入。
- `RemoteInbox` 是有界 MPSC、由所有者消费。只用于携带明确所有权目标的硬定向
  内部工作。
- 普通外来提交、软亲和完成与收件箱溢出走共享回退队列。不要把偏好变成对某个
  任意繁忙 worker 的硬亲和。
- 公开的 `cio::Chan<T>` 是互斥锁保护的 MPMC，与调度器的 MPSC 收件箱无关。

### 发布与休眠

- 可窃取状态的发布与清除使用纪元握手。位图中的一个位是通告，不是队列非空的
  精确状态。
- worker 必须先发布空闲，再做最终的可运行任务、定时器、reactor、回退队列与
  受害者复查。
- 搜索者信用在 `Scheduler::park()` 返回后的第一次调度循环里恰好消费一次。普
  通的协程间恢复不得给这条路径增加原子读。
- 窃取成功后，若原受害者的尾部仍处于已发布状态，要保留对它的搜索责任。

### 等待者与协程帧生命周期

唤醒一个协程可能让另一个线程立即恢复并销毁其帧。唤醒者必须：

1. 在持有保护该等待者的锁或握手时决定所有权；
2. 释放保护之后不再触碰等待者，除非它赢得了所有权；
3. 把调度该帧作为自己的最后一个动作。

不要往 channel 唤醒路径里加等待者/帧的引用计数来绕开这条所有权规则。

定时器的 disarm 是无条件的。即使节点看起来已不处于 armed 状态，`disarm()` 也必
须等出正在触发的回调，节点才能复用。

### reactor 与关停生命周期

- 每个描述符有稳定的 home reactor 分片。
- 描述符代号、生命周期钉与系统调用租约保护 close、截止时间、取消与陈旧
  epoll 事件之间的竞争。
- 同运行时的完成走缓存端点的快速路径。
- 外部或跨运行时的完成获取短暂的计数端点租约。关停先对新租约关门，等待存量
  租约结束，再清空调度器指针。
- 完成端点身份从不回收。它们的小墓碑记录刻意保留到进程结束，以避免 ABA 与静
  态析构期的 use-after-free。
- `Runtime::shutdown()` 是外部阻塞操作。从运行时自己的 worker 上调用必须在
  stop 或 join 开始前抛出。

### 任务完成

- 正常的 `spawn()` 任务从 final suspend 直接完成其 `JoinState`；不要在热路径
  上重新引入只为分配而存在的包装协程。
- 无效或已完成的任务保留冷路径包装，以维持既有语义并避免恢复一个已在 final
  suspend 的协程。
- `go()` 与 `go_on()` 必须在 detach 前清空共享的续体/完成槽，此后分离中止完
  成仅用于未捕获的失败。
- 保持异常捕获、结果移动失败捕获、detach 与跨运行时 join 的行为不变。

## 基准测试规范

性能声明需要可复现的证据：

- 从冻结的源码状态构建 Release 二进制。
- 每个二进制对应一个明确的干净源码版本；逐字节可复现的中间态混合体仍然只是
  诊断证据。
- 运行前后记录并校验服务端/运行时二进制与负载生成器的 SHA-256，并随哈希记录
  编译与链接命令行。没有构建命令的哈希无法复现。
- 服务端与客户端钉在互不相交的 CPU 集合上。
- 测量窗口之前先预热。
- 配对运行并交替 AB/BA 次序；不要比较两次独立扫描。
- 报告原始样本、配对几何增量、延迟、服务端 CPU 与错误。
- 任一侧出现 socket 错误、非 2xx 响应、服务端提前退出或输入哈希变化，则整对
  作废。
- 客户端饱和告警意味着这是容量筛查而非可发布证据；增加 `wrk` 线程或客户端
  CPU 后重跑完整矩阵。
- 依赖次序的筛查结果按噪声处理，直到更长的确认复现它。

HTTP 对比使用第三方 `wrk`：

```sh
python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline path/to/candidate \
  --cells 1:1,8:4,64:16,256:16,1024:16 \
  --pairs 10 --warmup 5 --duration 15
```

在 24 核基准主机上：服务端钉 CPU 0-7，`wrk` 钉 8-21，harness 自身钉 23。
`matrix_wrk.py` 会拒绝重叠的 CPU 集合、未钉核的 harness、socket/HTTP/服务端
失败、输入哈希漂移、客户端饱和与不完整的配对；它在 manifest 里写入
`publication_ready`，验证的是*测量*，不是候选实现。`mixed_wrk.py` 用第二个独
立钉核的 `wrk` 在流水线批量负载旁边打小规模延迟探针，测出单次饱和运行显示不
了的公平性权衡。`wrk_tail.lua` 补充 p99.9/p99.99/p99.999/Max；这些属于诊断数
据，直到它们在两个次序里都复现。

调度器工作的验收是联合门，不是吞吐头条：低负载吞吐保持中性，饱和吞吐与
p50-p99 不实质回退，深分位单独检查。在 AB 与 BA 次序间反号的稀有尾部移动是噪
声。「没有改进」是有效结果——该改动被移除，先前的实现保留。

echo A/B 只有在完全相同的冻结客户端二进制在独立钉核的进程里跑两侧时，才允许
使用 cio 自制负载生成器。两侧之间绝不重新构建客户端。echo 服务器会记录
`accept()` 把每条连接放到了哪个 reactor 分片并在 SIGINT 时打印表格，这是倾斜
负载结果成为落位证据而非假设的前提。

`bench/*/results/` 下的原始运行目录是本地的、不入库。仓库内的任何内容不得依
赖读取它们：声明所依赖的每个数字都写进正文。

对基于时间的微基准，B/A ns/op 为正表示 B 更慢。对吞吐基准，B/A req/s 为正表
示 B 更快。方向必须写明。

## 代码与变更卫生

- 遵循现有 C++ 风格：四空格缩进、大括号同行、RAII 所有权、不抛异常的运行时
  路径标 `noexcept`、共享原子量写明内存序。
- 优先选择满足实际所有权的最弱队列与同步契约。没有实测需求，不要把热路径上
  的 SPSC/SPMC/MPSC 结构泛化成 MPMC。
- 公开头文件保持自包含。改动头文件可见的代码时，向
  `tests/test_api_surface.cpp` 添加有代表性的下游用法。
- `include/cio/version.hpp` 与 `project(VERSION)` 必须一致；两者漂移时 CMake
  会在 configure 阶段报错。两处一起升。
- `.clang-format` 是风格本身，不是建议。新增或移动的代码应与它的输出一致。
- 为被修复的确切竞争或生命周期故障添加回归测试，而不是只加一个宽泛的压力测
  试。解析不可信字节的新代码要在 `fuzz/` 加一个 harness；fuzz 发现的崩溃样本
  放进 `fuzz/corpus/`，从此永久回归。
- 不变量或约束变化时更新本文件，用户可见行为变化时更新 `README.md`。两者都
  不写测量叙事：记录结论与理由，不记录运行日志。
- 一个机制被测量并移除时，在提交信息里写明它失败的原因。提交历史就是记录；
  后续提案应当先搜索它，再重建已被否决的东西。
- 保护脏工作树中的无关工作。不要 reset、覆盖或删除用户的改动。
- 除非用户明确要求，不要 commit 或 push。
- 交接前运行 `git diff --check`，并如实报告实际执行过的测试，包括任何 flake
  或跳过的环境。
