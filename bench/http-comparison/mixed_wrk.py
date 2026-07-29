#!/usr/bin/env python3
"""Paired latency-probe wrk while a second wrk drives pipelined bulk load."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import os
import pathlib
import signal
import subprocess
import sys
import time
from typing import Any


HERE = pathlib.Path(__file__).resolve().parent
HARNESS_PATH = pathlib.Path(__file__).resolve()
MATRIX_PATH = HERE / "matrix_wrk.py"
DEFAULT_BULK_SCRIPT = HERE / "wrk_pipeline_256.lua"
DEFAULT_PROBE_TAIL_SCRIPT = HERE / "wrk_tail.lua"
PIPELINE_256_BULK_SHA256 = (
    "3ab2878f1d4f638a52fbdc8adc43f668e71a4ca25c990c4ff17a39a15689046e"
)
REPORT_ONLY_PROBE_SHA256 = (
    "69c69764416758532800f53ce8a7c75cea7f628984a3613acaa3d23aa135e323"
)
SPEC = importlib.util.spec_from_file_location("cio_matrix_wrk", MATRIX_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {MATRIX_PATH}")
matrix = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = matrix
SPEC.loader.exec_module(matrix)

SUMMARY_FIELDS = (
    "bulk_rps",
    "probe_rps",
    "probe_latency_avg_us",
    "probe_p50_us",
    "probe_p75_us",
    "probe_p90_us",
    "probe_p99_us",
    "probe_p999_us",
    "probe_p9999_us",
    "probe_p99999_us",
    "probe_max_us",
)

RAW_FIELDS = (
    "pair",
    "order",
    "side",
    "sequence",
    "binary_sha256",
    "bulk_rps",
    "bulk_total_requests",
    "probe_rps",
    "probe_total_requests",
    "probe_latency_avg_us",
    "probe_p50_us",
    "probe_p75_us",
    "probe_p90_us",
    "probe_p99_us",
    "probe_p999_us",
    "probe_p9999_us",
    "probe_p99999_us",
    "probe_max_us",
    "server_cpu_seconds",
    "server_cores",
    "bulk_wall_seconds",
    "probe_wall_seconds",
    "probe_start_after_bulk_seconds",
    "client_overlap_seconds",
    "bulk_wrk_cores",
    "probe_wrk_cores",
    "client_saturation_warning",
    "server_underutilization_warning",
    "client_overlap_warning",
    "probe_socket_errors",
    "bulk_socket_errors",
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("binary_a", type=pathlib.Path)
    result.add_argument("binary_b", type=pathlib.Path)
    result.add_argument("--pairs", type=int, default=4)
    result.add_argument("--duration", type=int, default=15)
    result.add_argument("--bulk-warmup", type=int, default=5)
    result.add_argument("--cooldown", type=float, default=2.0)
    result.add_argument("--server-cores", default="0-7")
    result.add_argument("--server-threads", type=int, default=8)
    result.add_argument("--bulk-cores", default="8-17")
    result.add_argument("--bulk-threads", type=int, default=10)
    result.add_argument("--bulk-connections", type=int, default=64)
    result.add_argument("--probe-cores", default="18-21")
    result.add_argument("--probe-threads", type=int, default=4)
    result.add_argument("--probe-connections", type=int, default=4)
    result.add_argument(
        "--min-server-utilization",
        type=float,
        default=0.95,
        help="minimum measured server CPU / configured worker capacity",
    )
    result.add_argument("--wrk", type=pathlib.Path, default=pathlib.Path("/usr/bin/wrk"))
    result.add_argument(
        "--bulk-script",
        type=pathlib.Path,
        default=DEFAULT_BULK_SCRIPT,
    )
    result.add_argument(
        "--probe-tail-script",
        type=pathlib.Path,
        default=DEFAULT_PROBE_TAIL_SCRIPT,
    )
    result.add_argument("--expected-a-sha256", required=True)
    result.add_argument("--expected-b-sha256", required=True)
    result.add_argument("--expected-wrk-sha256", required=True)
    result.add_argument("--expected-bulk-script-sha256", required=True)
    result.add_argument("--expected-probe-tail-script-sha256", required=True)
    result.add_argument("--output", type=pathlib.Path, required=True)
    return result


def require_hash(path: pathlib.Path, expected: str, label: str) -> None:
    actual = matrix.sha256(path)
    if actual != expected:
        raise matrix.MatrixError(
            f"{label} hash mismatch: expected {expected}, got {actual}"
        )


def validate_args(args: argparse.Namespace) -> None:
    positive = {
        "pairs": args.pairs,
        "duration": args.duration,
        "bulk warmup": args.bulk_warmup,
        "server threads": args.server_threads,
        "bulk threads": args.bulk_threads,
        "bulk connections": args.bulk_connections,
        "probe threads": args.probe_threads,
        "probe connections": args.probe_connections,
    }
    for label, value in positive.items():
        if value < 1:
            raise matrix.MatrixError(f"{label} must be positive")
    if args.pairs < 4 or args.pairs % 2 != 0:
        raise matrix.MatrixError(
            "publication pairs must be even and at least 4"
        )
    if args.cooldown < 0:
        raise matrix.MatrixError("cooldown must not be negative")
    if not 0.95 <= args.min_server_utilization <= 1.0:
        raise matrix.MatrixError(
            "minimum server utilization must be in [0.95, 1]"
        )
    if args.bulk_threads > args.bulk_connections:
        raise matrix.MatrixError(
            "bulk threads cannot exceed bulk connections"
        )
    if args.probe_threads > args.probe_connections:
        raise matrix.MatrixError(
            "probe threads cannot exceed probe connections"
        )

    try:
        cpu_sets = {
            "server": matrix.parse_cpu_set(args.server_cores),
            "bulk": matrix.parse_cpu_set(args.bulk_cores),
            "probe": matrix.parse_cpu_set(args.probe_cores),
        }
    except argparse.ArgumentTypeError as error:
        raise matrix.MatrixError(str(error)) from error
    thread_counts = {
        "server": args.server_threads,
        "bulk": args.bulk_threads,
        "probe": args.probe_threads,
    }
    for name, threads in thread_counts.items():
        if threads > len(cpu_sets[name]):
            raise matrix.MatrixError(
                f"{name} threads ({threads}) exceed assigned CPUs "
                f"({len(cpu_sets[name])})"
            )
    names = list(cpu_sets)
    for index, first in enumerate(names):
        for second in names[index + 1 :]:
            overlap = cpu_sets[first] & cpu_sets[second]
            if overlap:
                raise matrix.MatrixError(
                    f"{first}/{second} CPU sets overlap: "
                    f"{sorted(overlap)}"
                )
    online_count = os.cpu_count() or 0
    selected = set().union(*cpu_sets.values())
    if selected and max(selected) >= online_count:
        raise matrix.MatrixError(
            f"selected CPU is outside 0-{online_count - 1}"
        )
    harness_overlap = set(os.sched_getaffinity(0)) & selected
    if harness_overlap:
        raise matrix.MatrixError(
            "pin this harness outside server/client CPUs; overlap="
            f"{sorted(harness_overlap)}"
        )
    if args.expected_probe_tail_script_sha256 != (
        REPORT_ONLY_PROBE_SHA256
    ):
        raise matrix.MatrixError(
            "probe must use the frozen report-only wrk_tail.lua; "
            "custom request-generating scripts are not valid here"
        )
    if args.expected_bulk_script_sha256 != (
        PIPELINE_256_BULK_SHA256
    ):
        raise matrix.MatrixError(
            "bulk load must use the frozen pipeline-256 script"
        )

    for label, path, executable in (
        ("A", args.binary_a, True),
        ("B", args.binary_b, True),
        ("wrk", args.wrk, True),
        ("bulk script", args.bulk_script, False),
        ("probe tail script", args.probe_tail_script, False),
    ):
        if not path.is_file():
            raise matrix.MatrixError(
                f"{label} is not a regular file: {path}"
            )
        if executable and not os.access(path, os.X_OK):
            raise matrix.MatrixError(
                f"{label} is not executable: {path}"
            )


def wrk_command(
    args: argparse.Namespace,
    port: int,
    *,
    cores: str,
    threads: int,
    connections: int,
    duration: int,
    latency: bool,
    script: pathlib.Path,
) -> list[str]:
    command = [
        "taskset",
        "-c",
        cores,
        str(args.wrk),
        f"-t{threads}",
        f"-c{connections}",
        f"-d{duration}s",
        "--timeout",
        "2s",
    ]
    if latency:
        command.append("--latency")
    command.extend(
        (
            "-s",
            str(script),
            f"http://127.0.0.1:{port}/",
        )
    )
    return command


def start_tracked_client(
    processes: dict[str, subprocess.Popen[Any]],
    name: str,
    command: list[str],
    log: Any,
    environment: dict[str, str],
) -> subprocess.Popen[Any]:
    deferred: list[int] = []

    def defer(signum: int, _frame: Any) -> None:
        deferred.append(signum)

    previous = {
        signum: signal.getsignal(signum)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    for signum in previous:
        signal.signal(signum, defer)
    try:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env=environment,
        )
        processes[name] = process
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if deferred:
        raise KeyboardInterrupt
    return process


def wait_client_group(
    processes: dict[str, subprocess.Popen[Any]],
    starts: dict[str, float],
    cpu_starts: dict[str, float],
    duration: int,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    cpu_ends = dict(cpu_starts)
    finishes: dict[str, float] = {}
    deadline = max(starts.values()) + duration + 30.0
    try:
        while len(finishes) != len(processes):
            now = time.monotonic()
            for name, process in processes.items():
                if name in finishes:
                    continue
                sampled_cpu = matrix.process_cpu_seconds(
                    process.pid
                )
                if sampled_cpu is not None:
                    cpu_ends[name] = sampled_cpu
                return_code = process.poll()
                if return_code is None:
                    continue
                finishes[name] = now
                if return_code != 0:
                    raise matrix.MatrixError(
                        f"{name} wrk exited with rc={return_code}"
                    )
            if len(finishes) == len(processes):
                break
            if now >= deadline:
                raise matrix.MatrixError(
                    f"wrk exceeded {duration + 30}s watchdog"
                )
            time.sleep(0.05)
    except BaseException:
        for process in processes.values():
            matrix.stop_process_group(process)
        raise

    cpu_seconds = {
        name: max(0.0, cpu_ends[name] - cpu_starts[name])
        for name in processes
    }
    wall_seconds = {
        name: max(0.0, finishes[name] - starts[name])
        for name in processes
    }
    return cpu_seconds, wall_seconds, finishes


def run_bulk_warmup(
    args: argparse.Namespace,
    port: int,
    log_path: pathlib.Path,
) -> matrix.WrkResult:
    command = wrk_command(
        args,
        port,
        cores=args.bulk_cores,
        threads=args.bulk_threads,
        connections=args.bulk_connections,
        duration=args.bulk_warmup,
        latency=False,
        script=args.bulk_script,
    )
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    with log_path.open("w") as log:
        start = time.monotonic()
        processes: dict[str, subprocess.Popen[Any]] = {}
        try:
            process = start_tracked_client(
                processes,
                "warmup",
                command,
                log,
                environment,
            )
            cpu_start = matrix.process_cpu_seconds(
                process.pid
            ) or 0.0
            cpu_seconds, wall_seconds, _ = wait_client_group(
                processes,
                {"warmup": start},
                {"warmup": cpu_start},
                args.bulk_warmup,
            )
        except BaseException:
            for tracked in processes.values():
                matrix.stop_process_group(tracked)
            raise
    return matrix.parse_wrk_output(
        log_path.read_text(errors="replace"),
        cpu_seconds=cpu_seconds["warmup"],
        wall_seconds=wall_seconds["warmup"],
        require_latency=False,
        require_tail=False,
    )


def run_side(
    binary: pathlib.Path,
    side: str,
    pair: int,
    order: str,
    args: argparse.Namespace,
    directory: pathlib.Path,
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=False)
    port = matrix.free_loopback_port()
    expected_binary_hash = (
        args.expected_a_sha256 if side == "A" else args.expected_b_sha256
    )
    require_hash(binary, expected_binary_hash, side)

    bulk_command = wrk_command(
        args,
        port,
        cores=args.bulk_cores,
        threads=args.bulk_threads,
        connections=args.bulk_connections,
        duration=args.duration,
        latency=False,
        script=args.bulk_script,
    )
    probe_command = wrk_command(
        args,
        port,
        cores=args.probe_cores,
        threads=args.probe_threads,
        connections=args.probe_connections,
        duration=args.duration,
        latency=True,
        script=args.probe_tail_script,
    )
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"

    with matrix.running_server(
        binary,
        args.server_cores,
        args.server_threads,
        port,
        directory / "server.log",
    ) as server:
        warmup = run_bulk_warmup(
            args, port, directory / "bulk-warmup.log"
        )
        if warmup.error_count:
            raise matrix.MatrixError(
                f"bulk warmup reported {warmup.error_count} errors"
            )
        if server.poll() is not None:
            raise matrix.MatrixError(
                "server exited during bulk warmup"
            )

        server_cpu_start = matrix.process_cpu_seconds(
            server.pid
        )
        if server_cpu_start is None:
            raise matrix.MatrixError(
                "cannot read server CPU before measurement"
            )
        measurement_started = time.monotonic()
        with (
            (directory / "bulk.log").open("w") as bulk_log,
            (directory / "probe.log").open("w") as probe_log,
        ):
            processes: dict[str, subprocess.Popen[Any]] = {}
            try:
                bulk_started = time.monotonic()
                start_tracked_client(
                    processes,
                    "bulk",
                    bulk_command,
                    bulk_log,
                    environment,
                )
                probe_started = time.monotonic()
                start_tracked_client(
                    processes,
                    "probe",
                    probe_command,
                    probe_log,
                    environment,
                )
                starts = {
                    "bulk": bulk_started,
                    "probe": probe_started,
                }
                cpu_starts = {
                    name: (
                        matrix.process_cpu_seconds(process.pid)
                        or 0.0
                    )
                    for name, process in processes.items()
                }
                client_cpu_seconds, client_wall_seconds, finishes = (
                    wait_client_group(
                        processes,
                        starts,
                        cpu_starts,
                        args.duration,
                    )
                )
            except BaseException:
                for process in processes.values():
                    matrix.stop_process_group(process)
                raise
        server_cpu_end = matrix.process_cpu_seconds(server.pid)
        if server_cpu_end is None or server.poll() is not None:
            raise matrix.MatrixError(
                "server exited during measurement"
            )
        measurement_finished = time.monotonic()

    bulk_output = (directory / "bulk.log").read_text(errors="replace")
    bulk_result = matrix.parse_wrk_output(
        bulk_output,
        cpu_seconds=client_cpu_seconds["bulk"],
        wall_seconds=client_wall_seconds["bulk"],
        require_latency=False,
        require_tail=False,
    )
    probe_result = matrix.parse_wrk_output(
        (directory / "probe.log").read_text(errors="replace"),
        cpu_seconds=client_cpu_seconds["probe"],
        wall_seconds=client_wall_seconds["probe"],
        require_latency=True,
        require_tail=True,
    )
    if bulk_result.error_count or probe_result.error_count:
        raise matrix.MatrixError(
            "wrk reported errors: "
            f"bulk={bulk_result.error_count}, "
            f"probe={probe_result.error_count}"
        )

    require_hash(binary, expected_binary_hash, side)
    server_cpu_seconds = max(
        0.0, float(server_cpu_end) - float(server_cpu_start)
    )
    measurement_wall_seconds = max(
        0.0, measurement_finished - measurement_started
    )
    if server_cpu_seconds <= 0.0 or measurement_wall_seconds <= 0.0:
        raise matrix.MatrixError(
            "server CPU measurement is zero"
        )
    client_overlap_seconds = max(
        0.0,
        min(finishes.values()) - max(starts.values()),
    )
    bulk_wrk_cores = (
        bulk_result.cpu_seconds / bulk_result.wall_seconds
    )
    probe_wrk_cores = (
        probe_result.cpu_seconds / probe_result.wall_seconds
    )
    server_cores = (
        server_cpu_seconds / measurement_wall_seconds
    )
    client_saturation_warning = (
        bulk_wrk_cores >= args.bulk_threads * 0.95 or
        probe_wrk_cores >= args.probe_threads * 0.95
    )
    server_underutilization_warning = (
        server_cores <
        args.server_threads * args.min_server_utilization
    )
    client_overlap_warning = (
        client_overlap_seconds < args.duration * 0.95
    )
    return {
        "pair": pair,
        "order": order,
        "side": side,
        "binary_sha256": expected_binary_hash,
        "bulk_rps": bulk_result.rps,
        "bulk_total_requests": bulk_result.total_requests,
        "probe_rps": probe_result.rps,
        "probe_total_requests": probe_result.total_requests,
        "probe_latency_avg_us": probe_result.latency_avg_us,
        "probe_p50_us": probe_result.latency_p50_us,
        "probe_p75_us": probe_result.latency_p75_us,
        "probe_p90_us": probe_result.latency_p90_us,
        "probe_p99_us": probe_result.latency_p99_us,
        "probe_p999_us": probe_result.latency_p999_us,
        "probe_p9999_us": probe_result.latency_p9999_us,
        "probe_p99999_us": probe_result.latency_p99999_us,
        "probe_max_us": probe_result.latency_max_us,
        "server_cpu_seconds": server_cpu_seconds,
        "server_cores": server_cores,
        "bulk_wall_seconds":
            bulk_result.wall_seconds,
        "probe_wall_seconds":
            probe_result.wall_seconds,
        "probe_start_after_bulk_seconds":
            probe_started - bulk_started,
        "client_overlap_seconds": client_overlap_seconds,
        "bulk_wrk_cores": bulk_wrk_cores,
        "probe_wrk_cores": probe_wrk_cores,
        "client_saturation_warning": client_saturation_warning,
        "server_underutilization_warning":
            server_underutilization_warning,
        "client_overlap_warning": client_overlap_warning,
        "probe_socket_errors": probe_result.error_count,
        "bulk_socket_errors": bulk_result.error_count,
    }


def paired_ratios(
    rows: list[dict[str, Any]],
    field: str,
    order: str | None = None,
) -> list[float]:
    by_pair: dict[int, dict[str, dict[str, Any]]] = {}
    for row in rows:
        by_pair.setdefault(int(row["pair"]), {})[str(row["side"])] = row
    return [
        float(sides["B"][field]) / float(sides["A"][field])
        for sides in by_pair.values()
        if order is None or sides["A"]["order"] == order
    ]


def geomean_delta(
    ratios: list[float],
) -> float | None:
    if not ratios:
        return None
    return (
        math.exp(
            sum(math.log(value) for value in ratios) /
            len(ratios)
        ) -
        1.0
    ) * 100.0


def summarize(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, float | list[float] | None]]:
    summary: dict[
        str, dict[str, float | list[float] | None]
    ] = {}
    for field in SUMMARY_FIELDS:
        ratios = paired_ratios(rows, field)
        interval = matrix.confidence_interval_95(ratios)
        summary[field] = {
            "paired_geomean_delta_pct":
                geomean_delta(ratios),
            "ci95_pct": (
                [
                    (interval[0] - 1.0) * 100.0,
                    (interval[1] - 1.0) * 100.0,
                ]
                if interval is not None
                else None
            ),
            "ab_geomean_delta_pct": geomean_delta(
                paired_ratios(rows, field, "AB")
            ),
            "ba_geomean_delta_pct": geomean_delta(
                paired_ratios(rows, field, "BA")
            ),
        }
    return summary


def main(argv: list[str]) -> int:
    args = parser().parse_args(argv)
    args.binary_a = args.binary_a.resolve()
    args.binary_b = args.binary_b.resolve()
    args.wrk = args.wrk.resolve()
    args.bulk_script = args.bulk_script.resolve()
    args.probe_tail_script = args.probe_tail_script.resolve()
    args.output = args.output.resolve()
    validate_args(args)
    if args.output.exists():
        raise matrix.MatrixError(
            f"output already exists: {args.output}"
        )
    args.output.mkdir(parents=True)

    for path, expected, label in (
        (args.binary_a, args.expected_a_sha256, "A"),
        (args.binary_b, args.expected_b_sha256, "B"),
        (args.wrk, args.expected_wrk_sha256, "wrk"),
        (
            args.bulk_script,
            args.expected_bulk_script_sha256,
            "bulk script",
        ),
        (
            args.probe_tail_script,
            args.expected_probe_tail_script_sha256,
            "probe tail script",
        ),
    ):
        require_hash(path, expected, label)
    harness_sha256 = matrix.sha256(HARNESS_PATH)
    matrix_helper_sha256 = matrix.sha256(MATRIX_PATH)

    manifest = {
        "schema": 2,
        "argv": [
            str(HARNESS_PATH),
            *argv,
        ],
        "harness": {
            "path": str(HARNESS_PATH),
            "sha256": harness_sha256,
        },
        "matrix_helper": {
            "path": str(MATRIX_PATH),
            "sha256": matrix_helper_sha256,
        },
        "binary_a": {
            "path": str(args.binary_a.resolve()),
            "sha256": args.expected_a_sha256,
        },
        "binary_b": {
            "path": str(args.binary_b.resolve()),
            "sha256": args.expected_b_sha256,
        },
        "wrk": {
            "path": str(args.wrk.resolve()),
            "sha256": args.expected_wrk_sha256,
            "version": matrix.command_output(
                [str(args.wrk), "--version"]
            ).splitlines()[0],
        },
        "bulk": {
            "cores": args.bulk_cores,
            "threads": args.bulk_threads,
            "connections": args.bulk_connections,
            "script": str(args.bulk_script.resolve()),
            "script_sha256": args.expected_bulk_script_sha256,
        },
        "probe": {
            "cores": args.probe_cores,
            "threads": args.probe_threads,
            "connections": args.probe_connections,
            "tail_script": str(args.probe_tail_script.resolve()),
            "tail_script_sha256": args.expected_probe_tail_script_sha256,
        },
        "server": {
            "cores": args.server_cores,
            "threads": args.server_threads,
            "minimum_utilization":
                args.min_server_utilization,
        },
        "pairs": args.pairs,
        "duration_seconds": args.duration,
        "bulk_warmup_seconds": args.bulk_warmup,
        "cooldown_seconds": args.cooldown,
        "git_head": matrix.command_output(
            ["git", "-C", str(HERE), "rev-parse", "HEAD"]
        ),
        "git_status": matrix.command_output(
            ["git", "-C", str(HERE), "status", "--short"]
        ).splitlines(),
        "environment_start": matrix.environment_snapshot(),
        "complete": False,
        "hashes_unchanged": False,
        "client_saturation_warning": None,
        "server_underutilization_warning": None,
        "client_overlap_warning": None,
        "publication_ready": False,
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )

    rows: list[dict[str, Any]] = []
    sequence = 0
    with (args.output / "raw.csv").open(
        "w", newline=""
    ) as output:
        writer = csv.DictWriter(
            output, fieldnames=RAW_FIELDS
        )
        writer.writeheader()
        output.flush()
        for pair in range(1, args.pairs + 1):
            order = "AB" if pair % 2 else "BA"
            print(
                f"pair {pair}/{args.pairs} order={order}",
                flush=True,
            )
            for side in order:
                sequence += 1
                binary = (
                    args.binary_a
                    if side == "A"
                    else args.binary_b
                )
                row = run_side(
                    binary,
                    side,
                    pair,
                    order,
                    args,
                    args.output /
                    f"pair-{pair:02d}-{order}" /
                    side,
                )
                row["sequence"] = sequence
                rows.append(row)
                writer.writerow(row)
                output.flush()
                print(
                    f"  {side} bulk={row['bulk_rps']:,.0f} "
                    f"probe={row['probe_rps']:,.0f} "
                    f"p99={row['probe_p99_us']:.1f}us "
                    f"p99.9={row['probe_p999_us']:.1f}us "
                    f"max={row['probe_max_us']:.1f}us",
                    flush=True,
                )
                time.sleep(args.cooldown)

    for path, expected, label in (
        (args.binary_a, args.expected_a_sha256, "A"),
        (args.binary_b, args.expected_b_sha256, "B"),
        (args.wrk, args.expected_wrk_sha256, "wrk"),
        (
            args.bulk_script,
            args.expected_bulk_script_sha256,
            "bulk script",
        ),
        (
            args.probe_tail_script,
            args.expected_probe_tail_script_sha256,
            "probe tail script",
        ),
        (HARNESS_PATH, harness_sha256, "mixed harness"),
        (
            MATRIX_PATH,
            matrix_helper_sha256,
            "matrix helper",
        ),
    ):
        require_hash(path, expected, label)

    if len(rows) != args.pairs * 2:
        raise matrix.MatrixError(
            "incomplete mixed matrix"
        )
    summary = summarize(rows)
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    manifest["environment_end"] = (
        matrix.environment_snapshot()
    )
    manifest["hashes_unchanged"] = True
    manifest["complete"] = True
    manifest["client_saturation_warning"] = any(
        bool(row["client_saturation_warning"])
        for row in rows
    )
    manifest["server_underutilization_warning"] = any(
        bool(row["server_underutilization_warning"])
        for row in rows
    )
    manifest["client_overlap_warning"] = any(
        bool(row["client_overlap_warning"])
        for row in rows
    )
    minimum_probe_requests = min(
        int(row["probe_total_requests"])
        for row in rows
    )
    manifest["probe_tail_resolution"] = {
        "minimum_requests_per_side":
            minimum_probe_requests,
        "minimum_expected_samples_above_p99_9":
            minimum_probe_requests * 0.001,
        "minimum_expected_samples_above_p99_99":
            minimum_probe_requests * 0.0001,
        "minimum_expected_samples_above_p99_999":
            minimum_probe_requests * 0.00001,
        "p99_99_diagnostic_unresolved":
            minimum_probe_requests < 10_000,
        "p99_999_diagnostic_unresolved":
            minimum_probe_requests < 100_000,
    }
    manifest["publication_ready"] = not any(
        (
            manifest["client_saturation_warning"],
            manifest["server_underutilization_warning"],
            manifest["client_overlap_warning"],
        )
    )
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )

    print("paired B/A geometric deltas:", flush=True)
    for field in SUMMARY_FIELDS:
        print(
            f"  {field}: "
            f"{summary[field]['paired_geomean_delta_pct']:+.2f}%",
            flush=True,
        )
    if not manifest["publication_ready"]:
        print(
            "error: mixed matrix failed a capacity/overlap gate",
            file=sys.stderr,
        )
        return 2
    return 0


def interrupt_on_sigterm(
    _signum: int, _frame: Any
) -> None:
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupt_on_sigterm)
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        print("error: interrupted", file=sys.stderr)
        raise SystemExit(130)
    except (matrix.MatrixError, OSError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2)
