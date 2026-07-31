# cio

面向 C++20 无栈协程的 goroutine 风格并发库。

cio 提供惰性任务、分离式与可 join 的任务派生、channel、`select`、结构化任务
组、取消、同步原语、定时器、非阻塞 socket 与阻塞线程池。应用只使用协程 API，
运行时负责 M:N 调度、worker 本地 epoll 分片、定时器分片与条件化工作窃取。

要求 Linux 与 C++20。核心库没有任何外部依赖。

## 快速上手

```cpp
#include <cio/cio.hpp>

cio::Task<> worker(cio::Chan<int> jobs, cio::Chan<int> out,
                   cio::CancelToken quit) {
    for (;;) {
        auto selected =
            cio::select(cio::recv(jobs), cio::recv(quit.done()));
        if (co_await selected == 1) co_return;

        auto job = selected.get<0>();
        if (!job) co_return;
        if (!(co_await out.send(*job * 2))) co_return;
    }
}

CIO_MAIN {
    auto jobs = cio::make_chan<int>(64);
    auto out = cio::make_chan<int>(64);
    cio::CancelSource stop;

    cio::TaskGroup workers;
    for (int i = 0; i < 4; ++i) {
        workers.spawn(worker(jobs, out, stop.token()));
    }

    for (int value = 1; value <= 100; ++value) {
        co_await jobs.send(value);
    }
    jobs.close();

    int total = 0;
    for (int i = 0; i < 100; ++i) {
        total += *co_await out.recv();
    }
    co_await workers.join();
    co_return total == 10'100 ? 0 : 1;
}
```

`CIO_MAIN` 让用户编写的主体保持异步，同时生成 C++ 标准要求的真正的非协程
`main`。需要显式配置 worker 数量或运行时所有权时，直接使用 `cio::Runtime`。

## 安装与使用

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cmake --install build --prefix /usr/local
```

```cmake
find_package(cio 0.2.0 REQUIRED)
target_link_libraries(app PRIVATE cio::cio)
# 以 -DCIO_TLS=ON 构建时再加 cio::cio_tls
```

不用 CMake 的项目可经 pkg-config 发现（TLS 模块是 CMake 专属）：

```sh
pkg-config --cflags --libs cio
```

`add_subdirectory()` 同样可用，且不会继承本项目的测试、示例和安装规则。
`cio/version.hpp` 提供 `CIO_VERSION_MAJOR`、`CIO_VERSION_MINOR`、
`CIO_VERSION_PATCH` 以及可比较的 `CIO_VERSION`：

```cpp
#if CIO_VERSION >= CIO_VERSION_NUMBER(0, 1, 0)
#endif
```

## 构建与测试

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

项目在 Linux 6.12 上以 GCC 13.3 与 Clang 19 测试。要求 CMake 3.20 及以上。

sanitizer 配置是一等公民的 CMake 构建：

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

十九个测试可执行文件覆盖公开 API、调度器、worker 位图、定向 MPSC 收件箱、
channel、`select`、网络、Unix socket、DNS、文件、进程、信号、缓冲 I/O、定时
器与池、作用域截止时间与描述符收养、同步原语以及浸泡（soak）行为。可选的
TLS 模块贡献第二十个：

```sh
cmake -S . -B build-tls -DCMAKE_BUILD_TYPE=Release -DCIO_TLS=ON
cmake --build build-tls -j
ctest --test-dir build-tls --output-on-failure
```

更长的非 sanitizer 网络浸泡：

```sh
./build/test_soak 90
```

长协程浸泡不应在 ASan 或 TSan 下运行；见[已知限制](#已知限制)。

## 公开 API

| API | 用途 |
|---|---|
| `cio::Task<T>` | 惰性、单消费者协程 |
| `cio::go(task)` | 发射后不管的分离任务 |
| `cio::spawn(task)` | 可 join 的任务，返回 `JoinHandle<T>` |
| `cio::yield()` | 让出当前 worker |
| `cio::Chan<T>` / `make_chan<T>(n)` | 互斥锁保护的 MPMC channel；`n == 0` 为会合语义 |
| `cio::select(...)` | 接收、发送、超时与 default 分支 |
| `cio::TaskGroup` | 结构化子任务作用域 |
| `CancelSource` / `CancelToken` | 协作式取消 |
| `cio::with_cancel` / `with_timeout` / `with_deadline` | 可嵌套、可过期的取消作用域（`context`） |
| `WaitGroup` / `Mutex` / `RWMutex` / `Once` / `Cond` | 挂起任务的同步原语（`sync`） |
| `cio::sleep(duration)` | 运行时定时器 |
| `cio::Timer` / `Ticker` / `after_func` | `time.Timer` / `time.Ticker` / `time.AfterFunc` |
| `cio::buffer_pool()` / `Pool<T>` | 可复用的 I/O 缓冲区与对象 |
| `cio::blocking(fn)` | 把阻塞工作放到调度器 worker 之外执行 |
| `cio::net::TcpListener` / `TcpConn` / `UdpConn` | 带截止时间的非阻塞 socket |
| `cio::net::UnixListener` / `UnixConn` / `UnixAddr` | Unix 域 socket，文件系统或抽象命名空间 |
| `cio::bufio::Reader` / `Writer` | 缓冲 I/O、按行与分帧（`bufio`） |
| `cio::net::Resolver` / `resolve()` | 名字解析统一入口；字段即配置，如 Go 的 `net.Resolver` |
| `cio::dns::Resolver` | 内置 DNS 后端；经 `prefer_builtin` 选择 |
| `cio::Timeout` | 作用域化、可嵌套、退出时还原外层的截止时间 |
| `cio::PollableFd` | 收养外来 fd（eventfd、timerfd、inotify、C 库） |
| `cio::net::Dialer` / `dial_tcp()` | 解析加地址竞速与超时 |
| `cio::fs::File` / `open` / `read_file` / `read_dir` | 阻塞池上的文件与目录操作（`os`） |
| `cio::process::Command` / `start` / `run` / `output` | 经 `pidfd` 等待的子进程（`os/exec`） |
| `cio::signal::SignalSet` | 基于 `signalfd` 的信号投递 |
| `cio::tls::Conn` / `Config` / `Certificate` | 可选 TLS，含 ALPN 与 SNI 多证书（`-DCIO_TLS=ON`，链接 OpenSSL） |
| `cio::io::read_full` / `copy` / `read_all` | `io.ReadFull` / `io.Copy` / `io.ReadAll` |
| `cio::io::LimitReader` / `TeeReader` | `io.LimitReader` / `io.TeeReader` |
| `net::Conn` / `PacketConn` / `Listener` | Go net 的三个接口，以 concept 表达 |
| `cio::io::Reader` / `Writer` | `io.Reader` / `io.Writer`，以 concept 表达 |
| `net::split_host_port` / `join_host_port` | `net.SplitHostPort` / `net.JoinHostPort` |
| `cio::Runtime` / `cio::run(task)` / `CIO_MAIN` | 运行时所有权与入口点 |

`write()` 遵循 Go 的 `io.Writer` 契约：除非返回错误，否则写满整个 span——短写
必须伴随错误。因此不存在 `write_all()`：`write()` 本身就是。`copy` 遇到无错误
的短写目标时报告 `EIO`，对应 Go 的 `io.ErrShortWrite`。

`net::Conn`、`net::PacketConn` 与 `net::Listener` 是 Go net 的三个接口，以
concept 而非虚基类表达：协议库可以只针对「行为像连接的任何东西」写一次，而具
体 socket 保持无虚表的快速路径。`tls::Conn` 满足 `net::Conn`，正如 Go 的
`tls.Conn` 实现 `net.Conn`，所以泛型辅助函数在明文与 TLS 上通用无改动。
`cio::io::copy` 目标参数在前，与 `io.Copy(dst, src)` 一致。`Error` 提供
`is_timeout()`、`is_cancelled()`、`is_temporary()`、`is_closed()` 与
`is_not_found()`，对应 `net.Error`，调用方按类别判断而不是比对错误码。

从已关闭且取空的 channel 接收返回 `std::nullopt`。向已关闭的 channel 发送返回
`false`。`select` 返回胜出分支的下标，分支值仍可经 `selected.get<I>()` 取得。

socket 截止时间按方向设置、直到重置前持续生效；无后缀的 `set_deadline()`、
`set_timeout()` 与 `clear_deadline()` 同时作用于两个方向。`cio::Timeout` 在一
个作用域内施加截止时间、退出时还原外层的值，因此超时可以嵌套而无需手工保存
恢复；内层作用域只能收紧外层，不能放宽。

`with_cancel`、`with_timeout` 与 `with_deadline` 按 `context.WithCancel` 与
`context.WithTimeout` 的方式嵌套作用域：取消父级会到达每个后代，子作用域的截
止时间会被钳制到父级之内，子操作无法给自己批比整个请求更多的时间。截止时间
到期报告 `Errc::timed_out`，显式取消报告 `Errc::cancelled`——两者是不同的结
果，调用方的决策也不同。不提供 context value：以不透明类型为键的映射是依赖注
入机制，不是取消机制。

取消绑定在 socket 上而不是调用上：`set_cancel(token)` 之后，一旦令牌触发，两
个方向上的所有操作都以 `Errc::cancelled` 失败——包括已经 park 的操作，它会被
唤醒。这就是 `read()`、`write()` 与 `accept()` 不带取消参数的原因——与截止时
间一样，取消活在连接上。`TcpConn::dial()` 以及解析器和拨号器的入口另外直接
接受令牌。

`net::Resolver` 是名字解析的唯一入口，用 `LookupOptions::prefer_builtin` 选择
后端，对应 Go 的 `Resolver.PreferGo`；`Dialer` 经
`DialOptions::prefer_builtin_resolver` 做同样的选择。

默认后端是内置 DNS 解析器，与 Go 在 Unix 上的默认一致，理由也相同：阻塞的
DNS 查询只占一个任务，阻塞的 C 调用占一个操作系统线程。它在运行时自己的
socket 上收发 DNS 并读取 `/etc/hosts`：查询可在途中取消、不占用池线程；但它
不查询 NSS，LDAP、NIS 与 mDNS 对它不可见。系统后端在阻塞池上调用
`getaddrinfo()`：尊重所有 NSS 模块，但占用一个池线程且一旦开始无法中断，被取
消的查询会立即恢复调用方、让调用在后台跑完。依赖 NSS 解析名字、或要求结果与
`getent hosts` 一致的机器，应把 `prefer_builtin` 设为 false。

`cio::blocking(fn)` 使用惰性增长的线程池。`RuntimeOptions` 限定其线程数上限
（`max_blocking_threads`，默认 512）、FIFO 等待队列（`max_blocking_queue`，
默认 1024），以及内置操作类别（`max_file_operations` 默认 32、
`max_resolver_operations` 默认 8）。类别限额约束的是并发准入的操作数而非线程
数：等待准入的任务只是 park，不占池线程；每个类别有独立等待队列，文件操作的
洪峰不会排在名字解析前面。向已满队列提交会抛出携带 `Errc::overloaded` 的
`cio::SystemError`。池中没有服务线程且操作系统拒绝创建第一个时返回同样的错
误；被拒绝的可调用对象绝不会被执行。

## 运行时架构

每个运行时 worker 拥有：

- 一个单槽 `runnext` 直接交接位；
- 一条仅由所有者生产的本地 FIFO，其已发布尾部可被窃取；
- 一个有界 256 项的 MPSC `RemoteInbox`，只承接硬定向的内部工作；
- 一个边沿触发的 epoll 实例与 eventfd；
- 一个 4 叉定时器堆。

MPSC 收件箱刻意不做通用可运行队列。只有携带明确内部所有权目标的提交才会进
入，且只有目标 worker 消费。普通外来提交、软亲和完成与收件箱溢出走共享的互
斥锁保护回退队列，任意繁忙 worker 都无法把它们困住。公开的 `cio::Chan<T>`
仍是 MPMC，与调度器收件箱无关。

worker 在可扩展位图中发布空闲与可窃取状态。窃取者只检查已通告的受害者，一个
纪元（epoch）握手闭合并发的发布/清除竞争。粘性搜索者信用保证新唤醒的 worker
即使被 I/O 或其他可运行任务截胡，负载再分配也不会丢失。

被 accept 的 socket 获得稳定的 home reactor 分片。描述符代号、生命周期钉与系
统调用租约让陈旧 epoll 事件在 close、截止时间与取消的竞争中保持安全。延迟的
跨运行时完成使用进程生命周期内唯一的端点身份，配计数的外部租约。

承重的不变量在 [AGENTS.md](AGENTS.md)。

## 仓库布局

| 路径 | 内容 |
|---|---|
| `include/cio/` | 公开头文件 |
| `include/cio/detail/` | 内部调度器、reactor、定时器与队列契约 |
| `src/` | 运行时实现 |
| `tests/` | 单元、并发、API 表面与浸泡测试 |
| `examples/` | 可构建的小示例 |
| `bench/` | 核心、I/O、echo、Go 与 HTTP/`wrk` 基准 |
| `include/cio/tls.hpp` | 可选 TLS 模块；仅在 `-DCIO_TLS=ON` 时构建 |
| `AGENTS.md` | 仓库的开发与验证规范 |

构建目录、sanitizer 产物、基准二进制、Python 缓存与本地原始结果目录都是本地
生成物，必须保持不入库。

## 基准测试

用常规 Release 配置构建 C++ 微基准：

```sh
./build/bench_core 24
./build/bench_io
```

对应的 Go 核心负载隔离在自己的模块里：

```sh
cd bench/go-core
go test ./...
go build -o go_core .
taskset -c 0-23 ./go_core -gomaxprocs=24 -warmup=1 -repeat=5
```

HTTP 前后对比两侧使用同一个第三方 `wrk` 可执行文件、互不相交的服务端/客户端
CPU 集合、预热、交替的 AB/BA 配对、输入 SHA-256 校验、错误拒绝与保留的原始
日志：

```sh
taskset -c 23 python3 bench/http-comparison/matrix_wrk.py \
  path/to/baseline-server path/to/candidate-server \
  --cells 1:1,8:4,64:14,256:14,1024:14 \
  --pairs 10 --warmup 5 --duration 15 \
  --server-cores 0-7 --client-cores 8-21 \
  --tail-script bench/http-comparison/wrk_tail.lua \
  --expected-a-sha256 <sha256> \
  --expected-b-sha256 <sha256> \
  --expected-wrk-sha256 <sha256> \
  --expected-tail-script-sha256 <sha256>
```

冻结的本地二进制是生成物，不提交入库。每份结果都要记录它们与 `wrk` 的哈希。

### 实测吞吐

极简 HTTP/1.1 服务器，八个 worker 钉在 CPU 0-7，由钉在 CPU 8-23 的第三方
`wrk` 驱动。五个格子来自针对已发布 v0.0.1 运行时的同一次十对矩阵，因此可以
直接互相比较：

| 连接数 | req/s | p50 中位数 | p99 中位数 |
|---:|---:|---:|---:|
| 1 | 14,339 | 68 us | 110 us |
| 8 | 125,598 | 52 us | 644 us |
| 64 | 789,141 | 70 us | 501 us |
| 256 | 771,416 | 306 us | 2765 us |
| 1024 | 781,518 | 1125 us | 4635 us |

后来一轮 monitor-balance 测得 c1024 为 859,820 req/s、p99 中位数 2060 us。那
是针对另一个基线的独立确认，与上表不构成配对；不要把两者当作一次扫描合读。

绝对数字与主机相关，主要用于看形状：吞吐在 64 连接附近饱和，尾延迟才是移动
的轴。

与 Boost.Asio 和 Go 的对比在 `bench/` 下：`http-comparison` 用同一个第三方
`wrk` 驱动三个运行时，`echo-comparison` 增加倾斜负载扫描，`go-core` 是
`bench_core` 的 Go 对应物、独立成模块。它们必须遵守的规则在
[AGENTS.md](AGENTS.md#基准测试规范)。

可选的运行时计数器以 `-DCIO_METRICS=ON` 启用。`cio::runtime_metrics()` 始终
可链接，未启用插桩时返回全零计数。

## 从 0.0.1 迁移

公开 API 已改为 Go 的命名，熟悉 `net`、`os` 与 `crypto/tls` 的读者可以直接推
断 cio 的拼写。所有 0.0.1 程序需要如下修改：

| 0.0.1 | 现在 | Go |
|---|---|---|
| `net::TcpStream` | `net::TcpConn` | `net.TCPConn` |
| `net::UdpSocket` | `net::UdpConn` | `net.UDPConn` |
| `TcpListener::bind()` | `TcpListener::listen()` | `net.ListenTCP` |
| `UdpSocket::bind()` | `UdpConn::listen()` | `net.ListenUDP` |
| `TcpStream::connect()` | `TcpConn::dial()` | `net.DialTCP` |
| `peer_addr()` | `remote_addr()` | `Conn.RemoteAddr` |
| `recv_from()` / `send_to()` | `read_from()` / `write_to()` | `PacketConn.ReadFrom` / `WriteTo` |
| `shutdown_write()` | `close_write()` | `TCPConn.CloseWrite` |
| `fs::FileInfo::is_directory()` | `is_dir()` | `FileInfo.IsDir` |
| `cio::AsyncReader` / `AsyncWriter` | `cio::io::Reader` / `Writer` | `io.Reader` / `io.Writer` |
| `cio::read_exact()` | `cio::io::read_full()` | `io.ReadFull` |
| `cio::write_all()` / `copy()` | `write()` 成员（写满）/ `cio::io::copy()` | `io.Writer` 契约 / `io.Copy` |
| `tls::ClientConfig` + `ServerConfig` | 合并为一个 `tls::Config` | `tls.Config` |
| `tls::TlsStream` | `tls::Conn` | `tls.Conn` |

有两处变更不是改名，会静默编译通过，需要特别注意：

- **`copy()` 的目标参数在前**，与 `io.Copy(dst, src)` 一致。旧顺序是
  `(src, dst)`。两侧类型相同的调用无论怎么写都能编译。
- **名字解析默认改为内置 DNS 解析器**，不再是 `getaddrinfo()`，与 Go 在 Unix
  上的默认一致。依赖内置解析器看不见的 NSS 模块（LDAP、NIS、mDNS）解析名字
  的机器，或要求结果与 `getent hosts` 一致的场合，把
  `LookupOptions::prefer_builtin` 或 `DialOptions::prefer_builtin_resolver`
  设为 false。

### 0.1.0 → 0.2.0（未发布）

这一轮把名字之外的两个维度也对齐了 Go：参数/契约与方法归属。

| 0.1.0 | 0.2.0 | Go |
|---|---|---|
| 各类型的 `write_all()` 与 `cio::io::write_all()` | 删除；`write()` 即写满 | `io.Writer` 契约 |
| `bufio::Reader::read_until()` | `read_string()` | `bufio.Reader.ReadString` |
| `bufio::Reader::peek()`（仅看缓冲） | `peek(n)`（按需填充） | `bufio.Reader.Peek` |
| `bufio::Reader::consume(n)` | `discard(n)`（可跨缓冲跳过） | `bufio.Reader.Discard` |
| `bufio::Reader::read_full()` 成员 | 删除；用自由函数 `io::read_full` 组合 | Go 的组合方式 |
| `bufio::Writer::write_all()` | `write()`（满足 `io::Writer`） | `bufio.Writer.Write` |
| `BufferPool::take` / `Pool<T>::take` | `get` | `sync.Pool.Get` |
| `Cond::notify_one` / `notify_all` | `signal()` / `broadcast()` | `sync.Cond.Signal` / `Broadcast` |
| `RWMutex::lock_read` 等 | `rlock` / `runlock` / `try_rlock` | `RLock` / `RUnlock` / `TryRLock` |
| `process::spawn(cmd)` / `process::run(cmd)` | `cmd.start()` / `cmd.run()` / `cmd.output()` | `Cmd.Start` / `Run` / `Output` |
| `Command::working_dir` | `dir` | `Cmd.Dir` |
| `Child::in/out/err()`、`close_in()` | `stdin_pipe()` 等、`close_stdin()` | `Cmd.StdinPipe` 等 |
| `File::read_at` 允许短读 | 填满或到 EOF | `io.ReaderAt` 契约 |
| `net::LookupOptions` / `DialOptions` | 删除；字段直接挂在 `Resolver` / `Dialer` 上 | `net.Resolver{PreferGo: …}` 的形状 |
| 监听器的 `local_addr()` / `remote_addr()` | 只有 `addr()`；监听器没有对端 | `Listener.Addr` |
| `net::Socket` 作为公开基类 | 变为实现基类；每个类型只重导出其 Go 对应物的方法集 | Go 无导出基类 |

语义变更（会静默编译通过）：所有 `write()`——socket、文件、`PollableFd`、
bufio——现在写满才返回，短写必然伴随错误；`File::read_at` 短返回只意味着
EOF。保留的刻意偏差：`Once::call`（`do` 是 C++ 关键字）、`Timer::chan()`
（Go 的 `C` 字段没有可用的 C++ 名字）、`stdin_pipe()` 而非 `stdin()`
（`stdin` 是 `<cstdio>` 宏）、`select` 的 `otherwise()`（`default` 是关键
字）。

## 已知限制

运行时：

- 仅 Linux/epoll；不声称支持 kqueue、IOCP 或 io_uring 后端。
- 调度是协作式的。从不挂起的任务会占住它的 worker。
- 取消是协作式的，只在任务检查它的地方被观察到。
- 已开始执行的阻塞可调用无法被抢占。运行时关停会等待已开始与已排队的阻塞工
  作完成。
- 运行时关停不会展开仍 park 在 channel 或 socket 上的任务。从运行时自己的
  worker 上调用 `Runtime::shutdown()` 会抛出 `std::logic_error`。
- socket 对象必须比使用它的任务活得久。每个 socket 同方向至多一个等待者。
- 对称协程转移依赖尾调用。CMake 传递 GCC 需要的
  `-foptimize-sibling-calls`；sanitizer 插桩仍可能把长的不挂起协程链变成深原
  生栈。
- `cio::blocking()` 本身没有准入上限；只有内置的文件与解析器类别受限。用户
  阻塞工作的洪峰只受全局队列上限与线程数上限约束。
- accept 按连接轮询分配，而权重是连接之后承载的流量的属性，因此重连接会随机
  聚集，倾斜负载在 reactor 分片间落得不均。

文件：

- 无取消、无截止时间，这是设计选择：被取消的 read 会让池线程继续写入调用方
  已销毁的 span。
- `read()` 与 `write()` 共享文件偏移量，同一 `File` 上不可并发；
  `read_at()`/`write_at()` 在缓冲区不同时可以并发。
- `close()` 是同步的。在 close 本身可能无界阻塞的文件系统上，请放进
  `cio::blocking()` 调用。

名字解析：

- 系统后端使用 `getaddrinfo()`。被取消的查询立即恢复调用方，但
  `getaddrinfo()` 无法中断、会跑到结束；迟到的结果被丢弃。
- 内置后端读 `/etc/hosts` 但不查 NSS，LDAP、NIS 与 mDNS 对它不可见。它也没
  有缓存、没有 DNSSEC 校验、没有 TCP 回退——截断且无可用记录的应答如实报
  错，而不是改走 TCP 重试——也不实现 `resolv.conf` 的 search 列表和
  `ndots`，名字按原样查询。

信号：

- `cio::signal::block()` 必须在运行时创建任何线程之前调用。对未被屏蔽的信
  号，`subscribe()` 报告 `Errc::broken`，而不是返回一个永远不会触发的集合。
- 到达速度快于消费速度的同种信号可能被内核合并。请用于生命周期事件，不要当
  计数器。

TLS（可选）：

- 需要 `-DCIO_TLS=ON` 并链接 OpenSSL；核心库保持零依赖。
- 下限为 TLS 1.2。无会话复用、无客户端证书（mTLS）。
- ALPN 经 `Config::next_protos` 协商（对应 Go 的 `Config.NextProtos`），
  `Conn::negotiated_protocol()` 读取结果——HTTP/2 over TLS 只能在这里协商，
  两端都要有 `"h2"`，否则必须回落 HTTP/1.1。双方无交集时握手照常成功、协商
  结果为空，而不是失败。
- 服务端多证书经 `Config::certificates` 按 SNI 选择，匹配的是证书自身的
  subjectAltName（对应 Go 拿 ClientHello 的 ServerName 比对每张证书），无匹
  配时用第一张作默认。`Conn::server_name()` 读取客户端发来的 SNI 名。

设计约束与运行时不变量在 [AGENTS.md](AGENTS.md)。

## 开发

改动运行时所有权、等待者生命周期、关停或基准方法之前，先读
[AGENTS.md](AGENTS.md)。除非任务明确要求 API 变更并写入文档，公开 API 与可观
察语义应保持稳定。

提出新的调度器机制之前先搜提交历史：一长串机制已经被实现、对冻结基线测量并
移除，提交信息记录了各自的否决理由。

以 [MIT 许可证](LICENSE)发布。
