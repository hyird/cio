#!/usr/bin/env python3
"""Reproducible, paired HTTP A/B matrix driven by a frozen wrk binary.

The two server binaries are always measured with identical client settings on
disjoint CPU sets.  Cells are interleaved by pair, and each pair alternates
AB/BA order.  Every raw log is retained and any wrk/socket/HTTP/server failure
invalidates the whole pair instead of silently dropping one side.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import platform
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
from collections.abc import Iterator, Sequence
from typing import Any


HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_A = HERE / "build" / "cio_http_v2_baseline"
DEFAULT_B = HERE / "build" / "cio_http_v2_spawn_direct_final"
DEFAULT_CELLS = "1:1,8:4,64:16,256:16,1024:16"
T95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}


@dataclasses.dataclass(frozen=True)
class Cell:
    connections: int
    wrk_threads: int

    @property
    def name(self) -> str:
        return f"c{self.connections}-t{self.wrk_threads}"


@dataclasses.dataclass
class WrkResult:
    rps: float
    total_requests: int
    latency_avg_us: float | None
    latency_p50_us: float | None
    latency_p75_us: float | None
    latency_p90_us: float | None
    latency_p99_us: float | None
    socket_connect: int
    socket_read: int
    socket_write: int
    socket_timeout: int
    non_2xx: int
    cpu_seconds: float
    wall_seconds: float

    @property
    def error_count(self) -> int:
        return (
            self.socket_connect
            + self.socket_read
            + self.socket_write
            + self.socket_timeout
            + self.non_2xx
        )


@dataclasses.dataclass
class SideResult:
    timestamp: str
    cell: Cell
    pair: int
    order: str
    sequence: int
    side: str
    binary_sha256: str
    port: int | None
    status: str
    error: str
    warmup_rps: float | None = None
    warmup_error_count: int | None = None
    rps: float | None = None
    total_requests: int | None = None
    latency_avg_us: float | None = None
    latency_p50_us: float | None = None
    latency_p75_us: float | None = None
    latency_p90_us: float | None = None
    latency_p99_us: float | None = None
    server_cpu_seconds: float | None = None
    wrk_cpu_seconds: float | None = None
    wall_seconds: float | None = None
    server_cores: float | None = None
    wrk_cores: float | None = None
    requests_per_server_cpu_second: float | None = None
    socket_connect: int | None = None
    socket_read: int | None = None
    socket_write: int | None = None
    socket_timeout: int | None = None
    non_2xx: int | None = None
    pair_valid: bool = False


RAW_FIELDS: list[str] = []
for side_result_field in dataclasses.fields(SideResult):
    RAW_FIELDS.append(side_result_field.name)
    if side_result_field.name == "cell":
        RAW_FIELDS.extend(("connections", "wrk_threads"))


class MatrixError(RuntimeError):
    pass


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(argv: Sequence[str]) -> str:
    try:
        return subprocess.run(
            argv,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.strip()
    except OSError as error:
        return f"<unavailable: {error}>"


def parse_cpu_set(spec: str) -> set[int]:
    cpus: set[int] = set()
    try:
        for part in spec.split(","):
            part = part.strip()
            if not part:
                raise argparse.ArgumentTypeError(
                    f"empty CPU component in {spec!r}"
                )
            if "-" in part:
                first_text, last_text = part.split("-", 1)
                first, last = int(first_text), int(last_text)
                if first > last:
                    raise argparse.ArgumentTypeError(
                        f"descending CPU range {part!r}"
                    )
                cpus.update(range(first, last + 1))
            else:
                cpus.add(int(part))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid CPU set {spec!r}") from error
    if not cpus:
        raise argparse.ArgumentTypeError("CPU set must not be empty")
    return cpus


def parse_cells(spec: str) -> list[Cell]:
    cells: list[Cell] = []
    seen: set[int] = set()
    for item in spec.split(","):
        try:
            connections_text, threads_text = item.split(":", 1)
            cell = Cell(int(connections_text), int(threads_text))
        except (ValueError, TypeError) as error:
            raise argparse.ArgumentTypeError(
                f"invalid cell {item!r}; expected connections:wrk_threads"
            ) from error
        if cell.connections < 1 or cell.wrk_threads < 1:
            raise argparse.ArgumentTypeError("connections and wrk threads must be > 0")
        if cell.wrk_threads > cell.connections:
            raise argparse.ArgumentTypeError(
                f"{item!r}: wrk threads cannot exceed connections"
            )
        if cell.connections in seen:
            raise argparse.ArgumentTypeError(
                f"duplicate connection count {cell.connections}"
            )
        seen.add(cell.connections)
        cells.append(cell)
    if not cells:
        raise argparse.ArgumentTypeError("matrix must contain at least one cell")
    return cells


def latency_us(text: str) -> float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|s)", text.strip())
    if match is None:
        raise MatrixError(f"cannot parse wrk latency {text!r}")
    value = float(match.group(1))
    unit = match.group(2)
    return value * {
        "ns": 0.001,
        "us": 1.0,
        "µs": 1.0,
        "ms": 1000.0,
        "s": 1_000_000.0,
    }[unit]


def parse_wrk_output(
    text: str,
    *,
    cpu_seconds: float,
    wall_seconds: float,
    require_latency: bool,
) -> WrkResult:
    rps_match = re.search(r"^Requests/sec:\s+([0-9]+(?:\.[0-9]+)?)", text, re.M)
    total_match = re.search(r"^\s*([0-9]+)\s+requests in\s+", text, re.M)
    if rps_match is None or total_match is None:
        raise MatrixError("wrk output has no parseable throughput")

    percentiles: dict[int, float] = {}
    for percentile, value in re.findall(
        r"^\s*(50|75|90|99)%\s+([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s))",
        text,
        re.M,
    ):
        percentiles[int(percentile)] = latency_us(value)

    latency_match = re.search(
        r"^\s*Latency\s+([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s))",
        text,
        re.M,
    )
    latency_avg = latency_us(latency_match.group(1)) if latency_match else None
    if require_latency and (latency_avg is None or 50 not in percentiles or 99 not in percentiles):
        raise MatrixError("wrk output has no complete latency distribution")

    socket_counts = {
        "connect": 0,
        "read": 0,
        "write": 0,
        "timeout": 0,
    }
    socket_match = re.search(
        r"Socket errors:\s+connect\s+([0-9]+),\s+read\s+([0-9]+),"
        r"\s+write\s+([0-9]+),\s+timeout\s+([0-9]+)",
        text,
    )
    if socket_match:
        socket_counts = dict(
            zip(socket_counts, (int(value) for value in socket_match.groups()))
        )

    non_2xx_match = re.search(
        r"Non-2xx or 3xx responses:\s+([0-9]+)", text
    )
    non_2xx = int(non_2xx_match.group(1)) if non_2xx_match else 0

    rps = float(rps_match.group(1))
    total_requests = int(total_match.group(1))
    if rps <= 0.0 or total_requests <= 0:
        raise MatrixError("wrk reported zero throughput")

    return WrkResult(
        rps=rps,
        total_requests=total_requests,
        latency_avg_us=latency_avg,
        latency_p50_us=percentiles.get(50),
        latency_p75_us=percentiles.get(75),
        latency_p90_us=percentiles.get(90),
        latency_p99_us=percentiles.get(99),
        socket_connect=socket_counts["connect"],
        socket_read=socket_counts["read"],
        socket_write=socket_counts["write"],
        socket_timeout=socket_counts["timeout"],
        non_2xx=non_2xx,
        cpu_seconds=cpu_seconds,
        wall_seconds=wall_seconds,
    )


def process_cpu_seconds(pid: int) -> float | None:
    try:
        raw = pathlib.Path(f"/proc/{pid}/stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None


def stop_process_group(process: subprocess.Popen[Any]) -> None:
    if process.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass

    # taskset execs the target, so neither benchmark normally leaves child
    # processes. Still clear any surviving member of the private process group
    # before returning from an interrupted or timed-out run.
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return
    os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired as error:
        raise MatrixError(
            f"process group {process.pid} survived SIGKILL"
        ) from error


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_for_server(
    process: subprocess.Popen[Any], port: int, timeout_seconds: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = "not attempted"
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise MatrixError(f"server exited during startup with rc={return_code}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError as error:
            last_error = str(error)
            time.sleep(0.05)
    raise MatrixError(f"server did not accept connections: {last_error}")


def validate_http_response(port: int) -> None:
    request = (
        b"GET / HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Connection: close\r\n"
        b"\r\n"
    )
    with socket.create_connection(("127.0.0.1", port), timeout=1.0) as client:
        client.settimeout(1.0)
        client.sendall(request)
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = client.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > 64 * 1024:
                raise MatrixError("HTTP correctness response header is too large")
        header, separator, body = bytes(response).partition(b"\r\n\r\n")
        if not separator:
            raise MatrixError("HTTP correctness probe returned no complete header")
        lines = header.split(b"\r\n")
        if not lines or lines[0] != b"HTTP/1.1 200 OK":
            status = lines[0].decode(errors="replace") if lines else "<missing>"
            raise MatrixError(f"HTTP correctness probe returned {status}")
        content_length = None
        for line in lines[1:]:
            name, colon, value = line.partition(b":")
            if colon and name.strip().lower() == b"content-length":
                try:
                    content_length = int(value.strip())
                except ValueError as error:
                    raise MatrixError(
                        "HTTP correctness probe has invalid Content-Length"
                    ) from error
                break
        if content_length != 13:
            raise MatrixError(
                f"HTTP correctness probe Content-Length={content_length!r}"
            )
        while len(body) < content_length:
            chunk = client.recv(content_length - len(body))
            if not chunk:
                break
            body += chunk
        if body[:content_length] != b"Hello, World!":
            raise MatrixError("HTTP correctness probe returned an unexpected body")


def run_wrk(
    wrk: pathlib.Path,
    client_cores: str,
    *,
    connections: int,
    threads: int,
    duration_seconds: int,
    timeout: str,
    port: int,
    latency: bool,
    log_path: pathlib.Path,
) -> WrkResult:
    command = [
        "taskset",
        "-c",
        client_cores,
        str(wrk),
        f"-t{threads}",
        f"-c{connections}",
        f"-d{duration_seconds}s",
        "--timeout",
        timeout,
    ]
    if latency:
        command.append("--latency")
    command.append(f"http://127.0.0.1:{port}/")

    start = time.monotonic()
    deadline = start + duration_seconds + 30.0
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env=environment,
        )
        first_cpu = process_cpu_seconds(process.pid) or 0.0
        last_cpu = first_cpu
        try:
            while True:
                sampled_cpu = process_cpu_seconds(process.pid)
                if sampled_cpu is not None:
                    last_cpu = sampled_cpu
                if process.poll() is not None:
                    break
                if time.monotonic() >= deadline:
                    raise MatrixError(
                        f"wrk exceeded {duration_seconds + 30}s watchdog"
                    )
                time.sleep(0.05)
        except BaseException:
            stop_process_group(process)
            raise
        return_code = process.returncode
    wall_seconds = time.monotonic() - start
    output = log_path.read_text(errors="replace")
    if return_code != 0:
        raise MatrixError(f"wrk exited with rc={return_code}")
    return parse_wrk_output(
        output,
        cpu_seconds=max(0.0, last_cpu - first_cpu),
        wall_seconds=wall_seconds,
        require_latency=latency,
    )


@contextlib.contextmanager
def running_server(
    binary: pathlib.Path,
    server_cores: str,
    server_threads: int,
    port: int,
    log_path: pathlib.Path,
) -> Iterator[subprocess.Popen[Any]]:
    command = [
        "taskset",
        "-c",
        server_cores,
        str(binary),
        str(port),
        str(server_threads),
    ]
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            wait_for_server(process, port)
            validate_http_response(port)
            yield process
        finally:
            stop_process_group(process)


def one_side(
    *,
    binary: pathlib.Path,
    binary_sha256: str,
    side: str,
    cell: Cell,
    pair: int,
    order: str,
    sequence: int,
    args: argparse.Namespace,
    run_directory: pathlib.Path,
) -> SideResult:
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    base = SideResult(
        timestamp=timestamp,
        cell=cell,
        pair=pair,
        order=order,
        sequence=sequence,
        side=side,
        binary_sha256=binary_sha256,
        port=None,
        status="invalid",
        error="",
    )
    try:
        run_directory.mkdir(parents=True, exist_ok=False)
        port = free_loopback_port()
        base.port = port
        if sha256(binary) != binary_sha256:
            raise MatrixError(f"{side} binary hash changed before run")
        with running_server(
            binary,
            args.server_cores,
            args.server_threads,
            port,
            run_directory / "server.log",
        ) as server:
            warmup = run_wrk(
                args.wrk,
                args.client_cores,
                connections=cell.connections,
                threads=cell.wrk_threads,
                duration_seconds=args.warmup,
                timeout=args.timeout,
                port=port,
                latency=False,
                log_path=run_directory / "warmup.log",
            )
            base.warmup_rps = warmup.rps
            base.warmup_error_count = warmup.error_count
            if warmup.error_count:
                raise MatrixError(f"warmup reported {warmup.error_count} errors")
            if server.poll() is not None:
                raise MatrixError("server exited during warmup")

            server_cpu_start = process_cpu_seconds(server.pid)
            if server_cpu_start is None:
                raise MatrixError("cannot read server CPU before measurement")
            measured = run_wrk(
                args.wrk,
                args.client_cores,
                connections=cell.connections,
                threads=cell.wrk_threads,
                duration_seconds=args.duration,
                timeout=args.timeout,
                port=port,
                latency=True,
                log_path=run_directory / "measure.log",
            )
            base.rps = measured.rps
            base.total_requests = measured.total_requests
            base.latency_avg_us = measured.latency_avg_us
            base.latency_p50_us = measured.latency_p50_us
            base.latency_p75_us = measured.latency_p75_us
            base.latency_p90_us = measured.latency_p90_us
            base.latency_p99_us = measured.latency_p99_us
            base.wrk_cpu_seconds = measured.cpu_seconds
            base.wall_seconds = measured.wall_seconds
            base.wrk_cores = measured.cpu_seconds / measured.wall_seconds
            base.socket_connect = measured.socket_connect
            base.socket_read = measured.socket_read
            base.socket_write = measured.socket_write
            base.socket_timeout = measured.socket_timeout
            base.non_2xx = measured.non_2xx
            server_cpu_end = process_cpu_seconds(server.pid)
            if server_cpu_end is None:
                raise MatrixError("server exited during measurement")
            if server.poll() is not None:
                raise MatrixError("server exited during measurement")
            if measured.error_count:
                raise MatrixError(
                    f"measurement reported {measured.error_count} errors"
                )

            server_cpu = max(0.0, server_cpu_end - server_cpu_start)
            if server_cpu <= 0.0:
                raise MatrixError("server CPU measurement is zero")
            base.server_cpu_seconds = server_cpu
            base.server_cores = server_cpu / measured.wall_seconds
            base.requests_per_server_cpu_second = (
                measured.total_requests / server_cpu
            )
        base.status = "valid"
    except (MatrixError, OSError, subprocess.SubprocessError) as error:
        base.status = "invalid"
        base.error = str(error)
    return base


def dataclass_row(result: SideResult) -> dict[str, Any]:
    row = dataclasses.asdict(result)
    row["cell"] = result.cell.name
    row["connections"] = result.cell.connections
    row["wrk_threads"] = result.cell.wrk_threads
    return row


def geometric_mean(values: Sequence[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def confidence_interval_95(values: Sequence[float]) -> tuple[float, float] | None:
    if len(values) < 2:
        return None
    logs = [math.log(value) for value in values]
    mean = statistics.fmean(logs)
    standard_error = statistics.stdev(logs) / math.sqrt(len(logs))
    critical = T95.get(len(logs) - 1, 1.96)
    return math.exp(mean - critical * standard_error), math.exp(
        mean + critical * standard_error
    )


def coefficient_of_variation(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values) / statistics.fmean(values)


def median_metric(results: Sequence[SideResult], name: str) -> float:
    values = [float(getattr(result, name)) for result in results]
    return statistics.median(values)


def summarize(
    cells: Sequence[Cell],
    results: Sequence[SideResult],
    expected_pairs: int,
    output_directory: pathlib.Path,
    *,
    interrupted: bool,
    hashes_unchanged: bool,
) -> tuple[list[dict[str, Any]], int, bool, list[str]]:
    summaries: list[dict[str, Any]] = []
    invalid_pairs = 0
    for cell in cells:
        cell_results = [result for result in results if result.cell == cell]
        by_pair: dict[int, dict[str, SideResult]] = {}
        for result in cell_results:
            by_pair.setdefault(result.pair, {})[result.side] = result
        pairs: list[tuple[SideResult, SideResult]] = []
        for pair in range(1, expected_pairs + 1):
            sides = by_pair.get(pair, {})
            if (
                set(sides) == {"A", "B"}
                and sides["A"].pair_valid
                and sides["B"].pair_valid
            ):
                pairs.append((sides["A"], sides["B"]))
            else:
                invalid_pairs += 1

        if not pairs:
            summaries.append(
                {
                    "connections": cell.connections,
                    "wrk_threads": cell.wrk_threads,
                    "valid_pairs": 0,
                }
            )
            continue

        a_runs = [pair[0] for pair in pairs]
        b_runs = [pair[1] for pair in pairs]
        ratios = [
            float(b.rps) / float(a.rps)
            for a, b in pairs
        ]
        ab_ratios = [
            float(b.rps) / float(a.rps)
            for a, b in pairs
            if a.order == "AB"
        ]
        ba_ratios = [
            float(b.rps) / float(a.rps)
            for a, b in pairs
            if a.order == "BA"
        ]
        interval = confidence_interval_95(ratios)
        summary = {
            "connections": cell.connections,
            "wrk_threads": cell.wrk_threads,
            "valid_pairs": len(pairs),
            "a_mean_rps": statistics.fmean(float(run.rps) for run in a_runs),
            "b_mean_rps": statistics.fmean(float(run.rps) for run in b_runs),
            "a_median_rps": statistics.median(float(run.rps) for run in a_runs),
            "b_median_rps": statistics.median(float(run.rps) for run in b_runs),
            "a_rps_cv_pct": coefficient_of_variation(
                [float(run.rps) for run in a_runs]
            )
            * 100.0,
            "b_rps_cv_pct": coefficient_of_variation(
                [float(run.rps) for run in b_runs]
            )
            * 100.0,
            "paired_geomean_delta_pct": (geometric_mean(ratios) - 1.0) * 100.0,
            "paired_median_delta_pct": (statistics.median(ratios) - 1.0)
            * 100.0,
            "ci95_low_pct": (interval[0] - 1.0) * 100.0 if interval else None,
            "ci95_high_pct": (interval[1] - 1.0) * 100.0 if interval else None,
            "ab_geomean_delta_pct": (geometric_mean(ab_ratios) - 1.0) * 100.0
            if ab_ratios
            else None,
            "ba_geomean_delta_pct": (geometric_mean(ba_ratios) - 1.0) * 100.0
            if ba_ratios
            else None,
            "a_median_p50_us": median_metric(a_runs, "latency_p50_us"),
            "b_median_p50_us": median_metric(b_runs, "latency_p50_us"),
            "a_median_p99_us": median_metric(a_runs, "latency_p99_us"),
            "b_median_p99_us": median_metric(b_runs, "latency_p99_us"),
            "a_mean_server_cpu_s": statistics.fmean(
                float(run.server_cpu_seconds) for run in a_runs
            ),
            "b_mean_server_cpu_s": statistics.fmean(
                float(run.server_cpu_seconds) for run in b_runs
            ),
            "a_mean_server_cores": statistics.fmean(
                float(run.server_cores) for run in a_runs
            ),
            "b_mean_server_cores": statistics.fmean(
                float(run.server_cores) for run in b_runs
            ),
            "a_mean_wrk_cores": statistics.fmean(
                float(run.wrk_cores) for run in a_runs
            ),
            "b_mean_wrk_cores": statistics.fmean(
                float(run.wrk_cores) for run in b_runs
            ),
            "a_mean_requests_per_server_cpu_second": statistics.fmean(
                float(run.requests_per_server_cpu_second) for run in a_runs
            ),
            "b_mean_requests_per_server_cpu_second": statistics.fmean(
                float(run.requests_per_server_cpu_second) for run in b_runs
            ),
            "client_saturation_warning": any(
                float(run.wrk_cores) >= cell.wrk_threads * 0.95
                for run in [*a_runs, *b_runs]
            ),
        }
        summaries.append(summary)

    summary_fields: list[str] = []
    for summary in summaries:
        for field in summary:
            if field not in summary_fields:
                summary_fields.append(field)
    with (output_directory / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summaries)

    matrix_valid = (
        not interrupted
        and hashes_unchanged
        and invalid_pairs == 0
        and all(
            int(summary.get("valid_pairs", 0)) == expected_pairs
            for summary in summaries
        )
    )
    saturated = [
        f"c{summary['connections']}/t{summary['wrk_threads']}"
        for summary in summaries
        if summary.get("client_saturation_warning")
    ]
    if matrix_valid and not saturated:
        status = (
            f"**Status: VALID AND PUBLICATION-READY.** All "
            f"{expected_pairs * len(cells)} expected pairs passed."
        )
    elif matrix_valid:
        status = (
            "**Status: VALID BUT NOT PUBLICATION-READY.** All expected pairs "
            "passed, but the client-capacity gate failed."
        )
    else:
        status = (
            f"**Status: INVALID.** invalid/incomplete pairs={invalid_pairs}, "
            f"interrupted={interrupted}, hashes_unchanged={hashes_unchanged}."
        )
    lines = [
        "# wrk frozen A/B matrix",
        "",
        status,
        "",
        "| connections | wrk threads | pairs | A mean req/s | B mean req/s | paired B/A geo | 95% CI | median p50 A/B us | median p99 A/B us | server cores A/B | wrk cores A/B |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for summary in summaries:
        if not summary.get("valid_pairs"):
            lines.append(
                f"| {summary['connections']} | {summary['wrk_threads']} | 0 | invalid | invalid | — | — | — | — | — | — |"
            )
            continue
        if summary["ci95_low_pct"] is None:
            ci = "—"
        else:
            ci = (
                f"{summary['ci95_low_pct']:+.2f}%…"
                f"{summary['ci95_high_pct']:+.2f}%"
            )
        lines.append(
            "| {connections} | {wrk_threads} | {valid_pairs} | "
            "{a_mean_rps:,.0f} | {b_mean_rps:,.0f} | "
            "{paired_geomean_delta_pct:+.2f}% | {ci} | "
            "{a_median_p50_us:.1f}/{b_median_p50_us:.1f} | "
            "{a_median_p99_us:.1f}/{b_median_p99_us:.1f} | "
            "{a_mean_server_cores:.2f}/{b_mean_server_cores:.2f} | "
            "{a_mean_wrk_cores:.2f}/{b_mean_wrk_cores:.2f} |".format(
                ci=ci, **summary
            )
        )
    lines.extend(
        [
            "",
            "Positive B/A means B is faster. Every included pair had zero socket",
            "errors, zero non-2xx responses, successful wrk exits and a live server",
            "through the complete measurement window.",
            "",
            "The confidence intervals are per-cell, unadjusted paired log-ratio",
            "Student-t intervals. Cross-cell family-wise claims require a multiple-",
            "comparison correction.",
            "",
        ]
    )
    if saturated:
        lines.extend(
            [
                "Client saturation warning: wrk reached at least 95% of its",
                f"configured thread capacity in {', '.join(saturated)}.",
                "",
            ]
        )
    (output_directory / "summary.md").write_text("\n".join(lines))
    return summaries, invalid_pairs, matrix_valid, saturated


def environment_snapshot() -> dict[str, Any]:
    proc_values: dict[str, str] = {}
    for path in (
        "/proc/loadavg",
        "/proc/sys/net/core/somaxconn",
        "/proc/sys/net/ipv4/tcp_max_syn_backlog",
        "/proc/sys/net/ipv4/ip_local_port_range",
        "/proc/sys/net/ipv4/tcp_tw_reuse",
    ):
        with contextlib.suppress(OSError):
            proc_values[path] = pathlib.Path(path).read_text().strip()
    return {
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "platform": platform.platform(),
        "uname": command_output(["uname", "-a"]),
        "lscpu": command_output(["lscpu"]),
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "loadavg_and_sysctls": proc_values,
        "top_processes": command_output(
            ["ps", "-eo", "pid,comm,psr,%cpu,%mem,args", "--sort=-%cpu"]
        ).splitlines()[:20],
    }


def validate_inputs(args: argparse.Namespace, cells: Sequence[Cell]) -> dict[str, Any]:
    for label, path in (("A", args.binary_a), ("B", args.binary_b), ("wrk", args.wrk)):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise MatrixError(f"{label} is not an executable file: {path}")

    server_cpus = parse_cpu_set(args.server_cores)
    client_cpus = parse_cpu_set(args.client_cores)
    allowed_cpus = set(os.sched_getaffinity(0))
    if not server_cpus <= allowed_cpus or not client_cpus <= allowed_cpus:
        raise MatrixError(
            f"CPU sets must be within allowed CPUs {sorted(allowed_cpus)}"
        )
    if server_cpus & client_cpus:
        raise MatrixError("server and wrk CPU sets overlap")
    if args.server_threads > len(server_cpus):
        raise MatrixError("server threads exceed pinned server CPUs")
    for cell in cells:
        if cell.wrk_threads > len(client_cpus):
            raise MatrixError(
                f"{cell.name}: wrk threads exceed pinned client CPUs"
            )

    hashes = {
        "A": sha256(args.binary_a),
        "B": sha256(args.binary_b),
        "wrk": sha256(args.wrk),
    }
    for label, expected in (
        ("A", args.expected_a_sha256),
        ("B", args.expected_b_sha256),
        ("wrk", args.expected_wrk_sha256),
    ):
        if expected and hashes[label] != expected.lower():
            raise MatrixError(
                f"{label} SHA-256 mismatch: expected {expected}, got {hashes[label]}"
            )
    return hashes


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a frozen, paired HTTP A/B connection matrix under wrk."
    )
    parser.add_argument("binary_a", nargs="?", type=pathlib.Path, default=DEFAULT_A)
    parser.add_argument("binary_b", nargs="?", type=pathlib.Path, default=DEFAULT_B)
    parser.add_argument(
        "--cells",
        default=DEFAULT_CELLS,
        help="comma-separated connections:wrk_threads cells",
    )
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--duration", type=int, default=15)
    parser.add_argument("--server-threads", type=int, default=8)
    parser.add_argument("--server-cores", default="0-7")
    parser.add_argument("--client-cores", default="8-23")
    parser.add_argument("--timeout", default="2s")
    parser.add_argument("--cooldown", type=float, default=0.5)
    parser.add_argument(
        "--wrk",
        type=pathlib.Path,
        default=pathlib.Path(shutil.which("wrk") or "/usr/bin/wrk"),
    )
    parser.add_argument("--expected-a-sha256")
    parser.add_argument("--expected-b-sha256")
    parser.add_argument("--expected-wrk-sha256")
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        help="new output directory; defaults under results/",
    )
    args = parser.parse_args(argv)
    if args.pairs < 1 or args.warmup < 1 or args.duration < 1:
        parser.error("pairs, warmup and duration must all be positive")
    if args.server_threads < 1:
        parser.error("server threads must be positive")
    if args.cooldown < 0:
        parser.error("cooldown must not be negative")
    args.binary_a = args.binary_a.resolve()
    args.binary_b = args.binary_b.resolve()
    args.wrk = args.wrk.resolve()
    return args


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    try:
        cells = parse_cells(args.cells)
        hashes = validate_inputs(args, cells)
    except (MatrixError, argparse.ArgumentTypeError, OSError) as error:
        print(f"matrix_wrk.py: {error}", file=sys.stderr)
        return 2

    timestamp = dt.datetime.now().astimezone().strftime("%Y%m%d-%H%M%S%z")
    output_directory = (
        args.output.resolve()
        if args.output
        else (HERE / "results" / f"wrk-matrix-{timestamp}")
    )
    try:
        output_directory.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        print(f"matrix_wrk.py: cannot create {output_directory}: {error}", file=sys.stderr)
        return 2

    manifest = {
        "schema": 1,
        "argv": [str(pathlib.Path(sys.argv[0]).resolve()), *argv],
        "binary_a": {
            "path": str(args.binary_a),
            "sha256": hashes["A"],
        },
        "binary_b": {
            "path": str(args.binary_b),
            "sha256": hashes["B"],
        },
        "wrk": {
            "path": str(args.wrk),
            "sha256": hashes["wrk"],
            "version": command_output([str(args.wrk), "--version"]).splitlines()[0],
        },
        "matrix": [dataclasses.asdict(cell) for cell in cells],
        "pairs": args.pairs,
        "warmup_seconds": args.warmup,
        "duration_seconds": args.duration,
        "server_threads": args.server_threads,
        "server_cores": args.server_cores,
        "client_cores": args.client_cores,
        "timeout": args.timeout,
        "cooldown_seconds": args.cooldown,
        "git_head": command_output(["git", "-C", str(HERE), "rev-parse", "HEAD"]),
        "git_status": command_output(
            ["git", "-C", str(HERE), "status", "--short"]
        ).splitlines(),
        "environment_start": environment_snapshot(),
    }
    manifest_path = output_directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"output: {output_directory}")
    print(f"A: {args.binary_a}  sha256={hashes['A']}")
    print(f"B: {args.binary_b}  sha256={hashes['B']}")
    print(f"wrk: {args.wrk}  sha256={hashes['wrk']}")
    print(
        f"server={args.server_cores}/{args.server_threads} workers  "
        f"wrk={args.client_cores}  warmup={args.warmup}s "
        f"measure={args.duration}s pairs={args.pairs}"
    )
    print("cells:", ", ".join(cell.name for cell in cells), flush=True)

    raw_path = output_directory / "raw.csv"
    results: list[SideResult] = []
    active_pair_results: list[SideResult] = []
    sequence = 0
    interrupted = False
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def handle_sigterm(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        with raw_path.open("w", newline="") as raw_output:
            writer = csv.DictWriter(raw_output, fieldnames=RAW_FIELDS)
            writer.writeheader()
            raw_output.flush()
            try:
                for pair in range(1, args.pairs + 1):
                    shift = (pair - 1) % len(cells)
                    round_cells = [*cells[shift:], *cells[:shift]]
                    order = "AB" if pair % 2 else "BA"
                    print(
                        f"\nround {pair}/{args.pairs} order={order} "
                        f"cells={','.join(cell.name for cell in round_cells)}",
                        flush=True,
                    )
                    for cell in round_cells:
                        active_pair_results = []
                        for side in order:
                            sequence += 1
                            binary = args.binary_a if side == "A" else args.binary_b
                            binary_hash = hashes[side]
                            run_directory = (
                                output_directory
                                / "logs"
                                / cell.name
                                / f"pair-{pair:02d}-{order}"
                                / side
                            )
                            result = one_side(
                                binary=binary,
                                binary_sha256=binary_hash,
                                side=side,
                                cell=cell,
                                pair=pair,
                                order=order,
                                sequence=sequence,
                                args=args,
                                run_directory=run_directory,
                            )
                            active_pair_results.append(result)
                            if result.status == "valid":
                                print(
                                    f"  {cell.name:11s} pair={pair:02d} {side} "
                                    f"rps={result.rps:10.0f} "
                                    f"p50={result.latency_p50_us:8.1f}us "
                                    f"p99={result.latency_p99_us:8.1f}us "
                                    f"srv={result.server_cores:4.2f}c "
                                    f"wrk={result.wrk_cores:4.2f}c",
                                    flush=True,
                                )
                            else:
                                print(
                                    f"  {cell.name:11s} pair={pair:02d} {side} "
                                    f"INVALID: {result.error}",
                                    flush=True,
                                )
                            if args.cooldown:
                                time.sleep(args.cooldown)

                        pair_is_valid = (
                            len(active_pair_results) == 2
                            and all(
                                result.status == "valid"
                                for result in active_pair_results
                            )
                        )
                        for result in active_pair_results:
                            result.pair_valid = pair_is_valid
                            writer.writerow(dataclass_row(result))
                            results.append(result)
                        raw_output.flush()
                        active_pair_results = []
            except KeyboardInterrupt:
                interrupted = True
                for result in active_pair_results:
                    result.pair_valid = False
                    writer.writerow(dataclass_row(result))
                    results.append(result)
                raw_output.flush()
                print(
                    "\ninterrupted; completed side results and logs were preserved",
                    file=sys.stderr,
                )
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)

    try:
        end_hashes = {
            "A": sha256(args.binary_a),
            "B": sha256(args.binary_b),
            "wrk": sha256(args.wrk),
        }
    except OSError as error:
        end_hashes = {}
        print(f"matrix invalid: cannot re-hash inputs: {error}", file=sys.stderr)
    hashes_unchanged = end_hashes == hashes
    summaries, invalid_pairs, matrix_valid, saturated = summarize(
        cells,
        results,
        args.pairs,
        output_directory,
        interrupted=interrupted,
        hashes_unchanged=hashes_unchanged,
    )
    manifest["environment_end"] = environment_snapshot()
    manifest["end_hashes"] = end_hashes
    manifest["interrupted"] = interrupted
    manifest["completed_runs"] = len(results)
    manifest["invalid_pairs"] = invalid_pairs
    manifest["hashes_unchanged"] = hashes_unchanged
    manifest["matrix_valid"] = matrix_valid
    manifest["client_saturation_cells"] = saturated
    manifest["publication_ready"] = matrix_valid and not saturated
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print()
    print((output_directory / "summary.md").read_text(), end="")
    if not manifest["publication_ready"]:
        print(
            f"matrix not publication-ready: interrupted={interrupted}, "
            f"invalid_pairs={invalid_pairs}, hashes_unchanged={hashes_unchanged}, "
            f"client_saturation_cells={saturated}",
            file=sys.stderr,
        )
        return 1
    print(f"raw data and logs: {output_directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
