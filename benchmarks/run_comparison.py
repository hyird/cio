#!/usr/bin/env python3
"""在同一进程亲和性和统一负载下运行 CIO、Tokio 与 Asio 对照。"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "benchmarks" / "tokio-reference" / "Cargo.toml"
DEFAULT_TARGET_DIR = ROOT / "build-bench" / "tokio-target"
DEFAULT_OUTPUT_DIR = ROOT / "benchmarks" / "results"
BOUNDED_MPSC_CAPACITY = 64
BOUNDED_MPSC_CONSUMERS = 1
BROADCAST_CAPACITY = 64
IO_MEMORY_PAYLOAD_BYTES = 64
ALL_WORKLOADS = {
    "schedule",
    "yield",
    "mutex",
    "rwlock_read",
    "rwlock_write",
    "rwlock_mixed",
    "once_cell_ready",
    "once_cell_init",
    "set_once_fanout",
    "oneshot_wake",
    "mpsc_bounded",
    "watch_fanout",
    "broadcast_fanout",
    "io_memory_ready",
}
ASIO_SKIPPED_WORKLOADS = {
    "mutex",
    "rwlock_read",
    "rwlock_write",
    "rwlock_mixed",
    "once_cell_ready",
    "once_cell_init",
    "set_once_fanout",
    "oneshot_wake",
    "mpsc_bounded",
    "watch_fanout",
    "broadcast_fanout",
    "io_memory_ready",
}
ASIO_SKIP_REASONS = {
    "io_memory_ready": (
        "Asio 没有与该负载等价的拥有式 ReadBuf 和内存 AsyncRead ready "
        "端点；手写适配器会测量自定义实现而非 Asio 对应 API"
    ),
}


def parse_cpu_list(value: str) -> list[int]:
    cpus: set[int] = set()
    for part in value.split(","):
        field = part.strip()
        if not field:
            continue
        if "-" in field:
            first_text, last_text = field.split("-", 1)
            first = int(first_text)
            last = int(last_text)
            if first > last:
                raise ValueError(f"无效 CPU 范围：{field}")
            cpus.update(range(first, last + 1))
        else:
            cpus.add(int(field))
    if not cpus or min(cpus) < 0:
        raise ValueError("CPU 亲和性列表不能为空或包含负数")
    return sorted(cpus)


def windows_process_handle() -> tuple[Any, Any]:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.GetProcessAffinityMask.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_size_t),
    ]
    kernel32.GetProcessAffinityMask.restype = ctypes.c_int
    kernel32.SetProcessAffinityMask.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    kernel32.SetProcessAffinityMask.restype = ctypes.c_int
    process = ctypes.c_void_p(kernel32.GetCurrentProcess())
    return kernel32, process


def current_affinity() -> list[int] | None:
    if hasattr(os, "sched_getaffinity"):
        return sorted(os.sched_getaffinity(0))
    if sys.platform == "win32":
        kernel32, process = windows_process_handle()
        process_mask = ctypes.c_size_t()
        system_mask = ctypes.c_size_t()
        succeeded = kernel32.GetProcessAffinityMask(
            process,
            ctypes.byref(process_mask),
            ctypes.byref(system_mask),
        )
        if not succeeded:
            raise OSError(
                ctypes.get_last_error(), "GetProcessAffinityMask 失败"
            )
        return [
            index
            for index in range(ctypes.sizeof(ctypes.c_size_t) * 8)
            if process_mask.value & (1 << index)
        ]
    return None


def set_affinity(cpus: list[int]) -> None:
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(cpus))
        return
    if sys.platform == "win32":
        bit_count = ctypes.sizeof(ctypes.c_size_t) * 8
        if max(cpus) >= bit_count:
            raise ValueError(
                "Windows runner 当前只支持单 processor group 的亲和性"
            )
        mask = sum(1 << cpu for cpu in cpus)
        kernel32, process = windows_process_handle()
        succeeded = kernel32.SetProcessAffinityMask(
            process, ctypes.c_size_t(mask)
        )
        if not succeeded:
            raise OSError(
                ctypes.get_last_error(), "SetProcessAffinityMask 失败"
            )
        return
    raise RuntimeError("当前平台不支持由 benchmark runner 设置 CPU 亲和性")


def command_output(command: list[str], cwd: Path = ROOT) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return completed.stdout.strip()


def git_metadata() -> dict[str, Any]:
    revision = command_output(["git", "rev-parse", "HEAD"])
    status = command_output(["git", "status", "--porcelain"])
    return {
        "revision": revision,
        "dirty": bool(status),
        "status_entries": len(status.splitlines()) if status else 0,
    }


def build_tokio(
    cargo: str,
    manifest: Path,
    target_dir: Path,
) -> Path:
    command_output(
        [
            cargo,
            "build",
            "--release",
            "--locked",
            "--manifest-path",
            str(manifest),
            "--target-dir",
            str(target_dir),
        ]
    )
    suffix = ".exe" if sys.platform == "win32" else ""
    executable = target_dir / "release" / f"cio-tokio-benchmark{suffix}"
    if not executable.is_file():
        raise FileNotFoundError(f"找不到 Tokio benchmark：{executable}")
    return executable


def run_program(
    executable: Path,
    workload: str,
    workers: int,
    operations: int,
    warmups: int,
    samples: int,
) -> dict[str, Any]:
    config = f"{workload} {workers} {operations} {warmups} {samples}\n"
    completed = subprocess.run(
        [str(executable)],
        cwd=ROOT,
        input=config,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{executable.name} 退出码 {completed.returncode}："
            f"{completed.stderr.strip()}"
        )
    lines = [
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip()
    ]
    if len(lines) != 1:
        raise RuntimeError(
            f"{executable.name} 没有输出唯一 JSON 行：{completed.stdout!r}"
        )
    result = json.loads(lines[0])
    expected = {
        "workload": workload,
        "workers": workers,
        "operations": operations,
        "warmups": warmups,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise RuntimeError(
                f"{executable.name} 的 {key}={result.get(key)!r}，"
                f"预期 {value!r}"
            )
    expected_tasks = task_count(workload, workers, operations)
    if result.get("tasks") != expected_tasks:
        raise RuntimeError(
            f"{executable.name} 的 tasks={result.get('tasks')!r}，"
            f"预期 {expected_tasks}"
        )
    if workload == "mpsc_bounded":
        expected_channel = {
            "channel_capacity": BOUNDED_MPSC_CAPACITY,
            "producers": bounded_mpsc_producer_count(operations, workers),
            "consumers": BOUNDED_MPSC_CONSUMERS,
        }
        for key, value in expected_channel.items():
            if result.get(key) != value:
                raise RuntimeError(
                    f"{executable.name} 的 {key}={result.get(key)!r}，"
                    f"预期 {value!r}"
                )
    if workload in {"watch_fanout", "broadcast_fanout"}:
        subscribers = task_count(workload, workers, operations)
        expected_fanout = {
            "subscribers": subscribers,
            "deliveries": operations * subscribers,
        }
        if workload == "broadcast_fanout":
            expected_fanout["channel_capacity"] = BROADCAST_CAPACITY
        for key, value in expected_fanout.items():
            if result.get(key) != value:
                raise RuntimeError(
                    f"{executable.name} 的 {key}={result.get(key)!r}，"
                    f"预期 {value!r}"
                )
    if workload == "io_memory_ready":
        expected_io = {
            "bytes": operations * IO_MEMORY_PAYLOAD_BYTES,
            "payload_bytes": IO_MEMORY_PAYLOAD_BYTES,
        }
        for key, value in expected_io.items():
            if result.get(key) != value:
                raise RuntimeError(
                    f"{executable.name} 的 {key}={result.get(key)!r}，"
                    f"预期 {value!r}"
                )
    values = result.get("samples_ns")
    if not isinstance(values, list) or len(values) != samples:
        raise RuntimeError(
            f"{executable.name} 样本数错误：{len(values or [])}"
        )
    if not all(isinstance(value, int) and value > 0 for value in values):
        raise RuntimeError(f"{executable.name} 包含无效时间样本")
    return result


def task_count(workload: str, workers: int, operations: int) -> int:
    if workload in {
        "schedule",
        "once_cell_init",
        "set_once_fanout",
        "oneshot_wake",
    }:
        return operations
    if workload == "yield":
        return 1
    if workload == "mpsc_bounded":
        return (
            bounded_mpsc_producer_count(operations, workers)
            + BOUNDED_MPSC_CONSUMERS
        )
    return min(operations, max(2, workers * 4))


def bounded_mpsc_producer_count(operations: int, workers: int) -> int:
    return min(operations, max(2, workers * 4))


def parse_operations(value: str) -> dict[str, int]:
    operations: dict[str, int] = {}
    for item in value.split(","):
        name, count = item.split("=", 1)
        operations[name.strip()] = int(count)
    required = ALL_WORKLOADS
    if set(operations) != required:
        raise ValueError(
            "operations 必须恰好包含 schedule、yield、mutex、"
            "rwlock_read、rwlock_write、rwlock_mixed、once_cell_ready、"
            "once_cell_init、set_once_fanout、oneshot_wake、mpsc_bounded、"
            "watch_fanout、broadcast_fanout、io_memory_ready"
        )
    if min(operations.values()) <= 0:
        raise ValueError("operations 必须大于零")
    return operations


def summary(result: dict[str, Any]) -> dict[str, float]:
    samples = sorted(result["samples_ns"])
    median_ns = float(statistics.median(samples))

    def percentile(value: float) -> float:
        return float(samples[max(0, math.ceil(len(samples) * value) - 1)])

    p95_ns = percentile(0.95)
    p99_ns = percentile(0.99)
    p999_ns = percentile(0.999)
    mean_ns = float(statistics.fmean(samples))
    relative_stddev = (
        float(statistics.pstdev(samples)) / mean_ns
        if len(samples) > 1 and mean_ns > 0
        else 0.0
    )
    return {
        "median_ns": median_ns,
        "p95_ns": p95_ns,
        "p99_ns": p99_ns,
        "p999_ns": p999_ns,
        "throughput_ops_s": (
            result.get("deliveries", result["operations"])
            * 1_000_000_000
            / median_ns
        ),
        "relative_stddev": relative_stddev,
    }


def format_affinity(cpus: list[int] | None) -> str:
    return "未获取" if cpus is None else ",".join(map(str, cpus))


def write_markdown(payload: dict[str, Any], path: Path) -> None:
    metadata = payload["metadata"]
    results = payload["results"]
    skips = payload["skips"]
    tokio_medians = {
        (result["workload"], result["workers"]): result["summary"]["median_ns"]
        for result in results
        if result["runtime"] == "tokio"
    }

    lines = [
        (
            "# CIO dirty_smoke 性能链路报告"
            if metadata["evidence_label"] == "dirty_smoke"
            else "# CIO 性能对比报告"
        ),
        "",
        f"- 标签：`{metadata['mode']}`",
        f"- 证据标签：`{metadata['evidence_label']}`",
        f"- UTC 时间：`{metadata['timestamp_utc']}`",
        f"- 系统：`{metadata['platform']}`",
        f"- 处理器：`{metadata['processor']}`",
        f"- CPU 亲和性：`{format_affinity(metadata['cpu_affinity'])}`",
        f"- Git：`{metadata['git']['revision']}`，"
        f"dirty=`{str(metadata['git']['dirty']).lower()}`",
        f"- warmup/样本：`{metadata['warmups']}/{metadata['samples']}`",
        "",
        "| runtime | workload | worker | task | 操作数 | bytes | 容量 | producer | "
        "consumer | subscriber | delivery | p50 ms | p95 ms | p99 ms | "
        "p999 ms | 吞吐 ops/s | RSD | 相对 Tokio 耗时 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        "---:|---:|---:|---:|---:|",
    ]
    for result in results:
        item = result["summary"]
        baseline = tokio_medians[(result["workload"], result["workers"])]
        ratio = item["median_ns"] / baseline
        lines.append(
            f"| {result['runtime']} {result['runtime_version']} "
            f"| {result['workload']} | {result['workers']} "
            f"| {result['tasks']} "
            f"| {result['operations']} "
            f"| {result.get('bytes', '-')} "
            f"| {result.get('channel_capacity', '-')} "
            f"| {result.get('producers', '-')} "
            f"| {result.get('consumers', '-')} "
            f"| {result.get('subscribers', '-')} "
            f"| {result.get('deliveries', '-')} "
            f"| {item['median_ns'] / 1_000_000:.3f} "
            f"| {item['p95_ns'] / 1_000_000:.3f} "
            f"| {item['p99_ns'] / 1_000_000:.3f} "
            f"| {item['p999_ns'] / 1_000_000:.3f} "
            f"| {item['throughput_ops_s']:.0f} "
            f"| {item['relative_stddev'] * 100:.2f}% "
            f"| {ratio:.3f}x |"
        )

    if skips:
        lines.extend(
            [
                "",
                "## 明确跳过的对照",
                "",
                "| runtime | workload | worker | 原因 |",
                "|---|---|---:|---|",
            ]
        )
        for skipped in skips:
            lines.append(
                f"| {skipped['runtime']} | {skipped['workload']} "
                f"| {skipped['workers']} | {skipped['reason']} |"
            )

    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- `schedule` 比较 runtime 内控制 task 批量提交并等待空任务全部"
            "完成；Asio 行是 executor `post` 能力对照，不等同 Tokio task "
            "生命周期。",
            "- `yield` 比较 runtime 内 spawned task 连续重新排队；Asio 行是 "
            "executor 链式 `post`。",
            "- `mutex` 只比较 CIO 与 Tokio；Asio 没有对应的 Tokio 风格异步 "
            "Mutex，因此不生成虚假对照。",
            "- `rwlock_read`、`rwlock_write`、`rwlock_mixed` 分别比较共享读、"
            "排他写和 80% 读/20% 写；每把 guard 内外各让出一次以制造真实"
            "排队、跨 worker 恢复和写者头阻塞。Asio 无 Tokio 风格异步 "
            "RwLock，因此不生成虚假对照。",
            "- `once_cell_ready` 比较已初始化 OnceCell 的同步 get 快路径，"
            "operations 均匀分摊到与 worker 数相关的 task。",
            "- `once_cell_init` 让 operations 个 task 竞争初始化同一个空 "
            "OnceCell；唯一 factory 必须恰好执行一次并 yield。",
            "- `set_once_fanout` 先建立 operations 个 waiter，再统一 set 并等待"
            "全部恢复。Asio 没有对应的 Tokio 风格 OnceCell/SetOnce，明确 "
            "skip，不生成替代数值。",
            "- `oneshot_wake` 为每个 operation 创建独立 channel 和 receiver "
            "task；确认全部 receiver 已实际 poll 到 Pending 后，同步发送全部"
            "值并等待全部恢复，同时校验值守恒和完成数。Asio 没有 Tokio 风格"
            " oneshot channel，明确 skip。",
            "- `mpsc_bounded` 固定容量 64，使用与 worker 数相关的多个 producer "
            "和一个 consumer；producer 合计发送且仅发送 1..operations，consumer "
            "在全部 sender 释放后 drain 至关闭，并校验消息数与 64 位校验和。Asio "
            "没有语义等价的 Tokio bounded mpsc channel，明确 skip。",
            "- `watch_fanout` 使用与 worker 数相关的 subscriber；发布者每次发送"
            "一个版本后等待全部 subscriber 确认，才发布下一版本，从而禁止"
            "最新值 coalescing 偷减工作量。报告吞吐按 delivery（发布数乘"
            "subscriber 数）计算；Asio 无语义等价 watch channel，明确 skip。",
            "- `broadcast_fanout` 固定容量 64，使用与 worker 数相关的独立 "
            "Receiver；发布者每次发送后等待全部 Receiver 复制并确认，保证两端"
            "消息数、复制数和 wake 次数一致且不触发 lag。吞吐按 delivery "
            "计算；Asio 无语义等价 broadcast channel，明确 skip。",
            "- `io_memory_ready` 将 operations 次单次 64 B ready read 均匀"
            "分配到与 worker 数相关的独立 reader task；每个 task 复用自己的"
            " buffer，并校验调用数、总 bytes 与逐字节 checksum。吞吐仍按"
            " read operation 计算，报告另列总 bytes。Asio 没有等价的拥有式"
            " ReadBuf 与内存 AsyncRead ready 端点，明确 skip。",
            "- 本报告只测 wall-clock 吞吐与延迟。CPU、峰值内存和分配次数需按 "
            "`docs/benchmark-methodology.md` 使用平台 profiler 生成配套证据。",
        ]
    )
    if metadata["mode"] == "smoke":
        lines.extend(
            [
                "- 这是 smoke 报告，只验证框架和负载能运行，不得作为性能结论。",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cio", type=Path, required=True)
    parser.add_argument("--asio", type=Path, required=True)
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--target-dir", type=Path, default=DEFAULT_TARGET_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", default="1,4")
    parser.add_argument(
        "--workloads",
        default=(
            "schedule,yield,mutex,"
            "rwlock_read,rwlock_write,rwlock_mixed,"
            "once_cell_ready,once_cell_init,set_once_fanout,oneshot_wake,"
            "mpsc_bounded,watch_fanout,broadcast_fanout,"
            "io_memory_ready"
        ),
    )
    parser.add_argument(
        "--operations",
        default=(
            "schedule=100000,yield=100000,mutex=20000,"
            "rwlock_read=20000,rwlock_write=20000,rwlock_mixed=20000,"
            "once_cell_ready=100000,once_cell_init=10000,"
            "set_once_fanout=10000,oneshot_wake=10000,mpsc_bounded=100000,"
            "watch_fanout=10000,broadcast_fanout=10000,"
            "io_memory_ready=100000"
        ),
    )
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument(
        "--mode", choices=("smoke", "measurement"), default="measurement"
    )
    parser.add_argument(
        "--affinity",
        help="例如 2-5；runner 与全部子进程继承相同 CPU 亲和性",
    )
    return parser.parse_args()


def validate_args(
    args: argparse.Namespace,
    git: dict[str, Any],
) -> tuple[list[int], list[str], dict[str, int]]:
    workers = [int(value) for value in args.workers.split(",")]
    workloads = [value.strip() for value in args.workloads.split(",")]
    operations = parse_operations(args.operations)
    if not workers or min(workers) <= 0:
        raise ValueError("workers 必须大于零")
    if not workloads or not set(workloads) <= ALL_WORKLOADS:
        raise ValueError(
            "workloads 包含未知项；支持 schedule、yield、mutex、三类 "
            "rwlock、once_cell_ready、once_cell_init、set_once_fanout、"
            "oneshot_wake、mpsc_bounded、watch_fanout 与 "
            "broadcast_fanout、io_memory_ready"
        )
    if args.warmups < 0 or args.samples <= 0:
        raise ValueError("warmups 不能为负，samples 必须大于零")
    if args.mode == "measurement":
        if args.warmups < 3 or args.samples < 10:
            raise ValueError(
                "measurement 至少需要 3 次 warmup 和 10 个样本"
            )
        if not args.affinity:
            raise ValueError("measurement 必须显式设置 --affinity")
        if git["dirty"]:
            raise ValueError(
                "measurement 要求 clean Git 工作树；当前只能运行 smoke"
            )
    return workers, workloads, operations


def main() -> int:
    args = parse_args()
    git = git_metadata()
    workers, workloads, operations = validate_args(args, git)

    if args.affinity:
        set_affinity(parse_cpu_list(args.affinity))
    affinity = current_affinity()

    for executable in (args.cio, args.asio):
        if not executable.is_file():
            raise FileNotFoundError(f"找不到 benchmark：{executable}")
    tokio = build_tokio(args.cargo, args.manifest, args.target_dir)

    executables = [
        ("cio", args.cio),
        ("tokio", tokio),
        ("asio", args.asio),
    ]
    results: list[dict[str, Any]] = []
    skips: list[dict[str, Any]] = []
    for workload in workloads:
        for worker_count in workers:
            for runtime_name, executable in executables:
                if (
                    runtime_name == "asio"
                    and workload in ASIO_SKIPPED_WORKLOADS
                ):
                    skips.append(
                        {
                            "runtime": "asio",
                            "workload": workload,
                            "workers": worker_count,
                            "reason": ASIO_SKIP_REASONS.get(
                                workload,
                                "无语义等价的 Tokio 风格 API",
                            ),
                        }
                    )
                    continue
                result = run_program(
                    executable,
                    workload,
                    worker_count,
                    operations[workload],
                    args.warmups,
                    args.samples,
                )
                result["summary"] = summary(result)
                if (
                    args.mode == "measurement"
                    and result.get("build_mode") == "debug"
                ):
                    raise RuntimeError(
                        f"{runtime_name} benchmark 不是 Release 构建"
                    )
                results.append(result)

    timestamp = dt.datetime.now(dt.timezone.utc).replace(
        microsecond=0
    )
    evidence_label = (
        "dirty_smoke"
        if args.mode == "smoke" and git["dirty"]
        else args.mode
    )
    payload = {
        "schema_version": 1,
        "metadata": {
            "mode": args.mode,
            "evidence_label": evidence_label,
            "timestamp_utc": timestamp.isoformat(),
            "platform": platform.platform(),
            "processor": platform.processor()
            or os.environ.get("PROCESSOR_IDENTIFIER", "unknown"),
            "logical_cpu_count": os.cpu_count(),
            "cpu_affinity": affinity,
            "python": sys.version.split()[0],
            "rustc": command_output(["rustc", "--version"]),
            "cargo": command_output([args.cargo, "--version"]),
            "git": git,
            "warmups": args.warmups,
            "samples": args.samples,
        },
        "results": results,
        "skips": skips,
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_label = evidence_label.replace("_", "-")
    stem = timestamp.strftime("%Y%m%dT%H%M%SZ") + f"-{report_label}"
    json_path = args.output_dir / f"{stem}.json"
    markdown_path = args.output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    write_markdown(payload, markdown_path)

    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
