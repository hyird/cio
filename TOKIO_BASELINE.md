# Tokio 固定基线

## 版本

CIO 固定对齐 **Tokio 1.53.1**，发布日期为 2026-07-20。

- crates.io 包：`tokio 1.53.1`
- crates.io SHA-256：
  `202caea871b69668250d242070849eb495be178ed697a3e98aebce5bc81a0bed`
- Rust edition：2021
- Tokio 声明的最低 Rust 版本：1.71
- 固定 API 文档：<https://docs.rs/tokio/1.53.1/tokio/>
- 固定源码：<https://docs.rs/crate/tokio/1.53.1/source/>

版本号和校验值取自 crates.io 的不可变版本元数据。兼容性分析、Rust 对照程序和
差分测试必须锁定精确版本 `=1.53.1`，不得使用会自动升级的宽松版本范围。

## 启用的 feature

语义基线启用：

```toml
tokio = { version = "=1.53.1", features = ["full", "test-util", "tracing"] }
```

其中 `full` 在该版本展开为：

- `fs`
- `io-std`
- `io-util`
- `macros`
- `net`
- `parking_lot`
- `process`
- `rt`
- `rt-multi-thread`
- `signal`
- `sync`
- `time`

另外启用：

- `test-util`：将暂停时间、手动推进时间等确定性测试能力纳入对齐范围；
- `tracing`：将稳定的运行时 tracing 集成能力纳入对齐范围。

不启用 `io-uring` 和 `taskdump`。它们在该 Tokio 版本中仍依赖实验性配置或
实验性契约，不属于本轮稳定公开 API 基线。CIO 可在不改变公开语义的前提下把
io_uring 作为独立实验后端，但不得用它替代 Linux epoll 基线。

## 纳入和排除规则

纳入范围：

1. 上述 feature 在 Tokio 1.53.1 中暴露的全部稳定公开项；
2. Linux、Windows、macOS 上可用的稳定平台 API；
3. 文档明确规定的取消安全、公平性、关闭、析构、错误、EOF、背压和线程行为；
4. C++20 无法源码级照搬但能够等价表达的能力。

排除范围：

1. 必须通过 `tokio_unstable` 才能使用的项目；
2. Tokio 私有模块和未公开实现细节；
3. CIO 目标平台之外、且三个目标平台没有同等能力的专有 API。

排除不等于静默忽略。任何语言或平台层面无法等价表达的稳定项，都必须在
`docs/tokio-parity.md` 中逐项说明原因、替代 API 和经过测试的行为边界。

## 兼容判定

“已实现”必须同时具备：

- 可使用的 CIO API，而不是占位符；
- 对应的正常、错误、取消、析构和关闭语义测试；
- 适用时的跨线程、压力和平台测试；
- 固定 Tokio 1.53.1 Rust 对照程序及差分证据；
- 中文 API 契约和兼容矩阵证据链接。

仅编译成功或仅有名称相似的 API，状态必须保持“未实现”或“部分实现”。
