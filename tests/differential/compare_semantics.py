#!/usr/bin/env python3
"""比较 CIO 与固定 Tokio 程序的稳定可观察语义。"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


EXPECTED_KEYS = {
    "spawn_deferred",
    "abort_before_poll",
    "join_drop_detaches",
    "panic_join_error",
    "abort_destroys_before_join",
    "nested_future_current_poll",
    "paused_sleep_rounding",
    "timeout_immediate_zero",
    "timeout_same_deadline",
    "timeout_drops_loser",
    "sleep_reset_after_elapsed",
    "interval_basic",
    "interval_missed_ticks",
    "consume_budget_yields",
    "multi_spawn_join",
    "blocking_running_abort_noop",
    "blocking_queued_abort",
    "blocking_paused_inhibits_time",
    "notify_permit_coalesces",
    "notify_fifo_lifo",
    "notify_waiters_snapshot",
    "notify_cancel_transfers",
    "semaphore_fifo_head_blocking",
    "semaphore_cancel_partial",
    "semaphore_close",
    "semaphore_permit_ops",
    "mutex_fifo",
    "mutex_cancel_transfers",
    "mutex_no_poison",
    "mutex_owned_map",
    "mutex_blocking_bridge",
    "rwlock_shared_max_readers",
    "rwlock_writer_priority_fifo",
    "rwlock_cancel_partial_writer",
    "rwlock_no_poison",
    "rwlock_owned_mapping",
    "rwlock_atomic_downgrade",
    "barrier_zero_single_leader",
    "barrier_lazy_unpolled",
    "barrier_reusable_unique_leader",
    "barrier_cancelled_arrival_retained",
    "once_cell_single_initializer",
    "once_cell_cancel_retry",
    "once_cell_try_error_retry",
    "once_cell_clone_independent",
    "once_cell_debug_format",
    "once_cell_set_error_format",
    "set_once_wait_unblocks",
    "set_once_single_winner_values",
    "set_once_cancel_safe",
    "set_once_clone_independent",
    "oneshot_send_receive",
    "oneshot_sender_drop_recv_error",
    "oneshot_receiver_drop_returns_value",
    "oneshot_close_preserves_sent",
    "oneshot_close_rejects_late_send",
    "oneshot_try_recv_empty_closed",
    "oneshot_receive_cancel_safe",
    "oneshot_sender_closed_wakes",
    "oneshot_empty_terminated_transitions",
    "oneshot_value_drop_once",
    "oneshot_ready_budget_yields",
    "mpsc_fifo_backpressure",
    "mpsc_send_reserve_fairness",
    "mpsc_cancel_send",
    "mpsc_cancel_reserve",
    "mpsc_permit_capacity",
    "mpsc_close_drain_permit",
    "mpsc_try_errors",
    "mpsc_receiver_drop",
    "mpsc_last_sender_weak",
    "mpsc_sender_counts",
    "mpsc_error_format",
    "mpsc_closed_wakes",
    "mpsc_closed_cancel_safe",
    "mpsc_same_channel",
    "mpsc_receiver_len_empty",
    "mpsc_try_reserve_errors",
    "mpsc_owned_permit_send_release",
    "mpsc_owned_permit_same_channel",
    "mpsc_owned_permit_lifetime",
    "mpsc_reserve_owned_closed_consumes_sender",
    "mpsc_try_reserve_owned_errors",
    "mpsc_reserve_owned_cancel_safe",
    "mpsc_unbounded_fifo_multi_sender",
    "mpsc_unbounded_send_try_errors",
    "mpsc_unbounded_close_drain",
    "mpsc_unbounded_receiver_drop",
    "mpsc_unbounded_closed_wakes",
    "mpsc_unbounded_closed_cancel_safe",
    "mpsc_unbounded_last_sender_weak",
    "mpsc_unbounded_same_channel_counts",
    "mpsc_unbounded_receiver_len_empty",
    "mpsc_unbounded_ready_recv_budget",
    "mpsc_unbounded_value_drop_once",
    "mpsc_unbounded_weak_upgrade_closed",
    "mpsc_unbounded_recv_cancel_safe",
    "mpsc_unbounded_noncoop_send_closed",
    "watch_initial_borrow",
    "watch_send_changed_borrow_update",
    "watch_marks_and_has_changed",
    "watch_independent_receivers_subscribe",
    "watch_last_sender_close_retains_value",
    "watch_last_receiver_closes_sender",
    "watch_changed_cancel_safe",
    "watch_same_channel_counts",
    "watch_send_replace",
    "watch_wait_for",
    "watch_value_drop_and_clone",
    "watch_error_format",
    "watch_cooperative_ready_paths",
    "watch_coop_changed_success_boundary",
    "watch_coop_changed_error_boundary",
    "watch_coop_closed_boundary",
    "watch_coop_wait_for_success_boundary",
    "watch_coop_wait_for_error_boundary",
    "watch_coop_changed_fresh_wake_budget",
    "watch_coop_wait_for_fresh_wake_budget",
    "watch_coop_closed_fresh_wake_budget",
    "broadcast_capacity_rounding_lag",
    "broadcast_failed_send_then_subscribe",
    "broadcast_independent_receivers",
    "broadcast_resubscribe_skips_backlog",
    "broadcast_drain_then_closed",
    "broadcast_lagged_exact",
    "broadcast_try_recv_empty_closed",
    "broadcast_send_receiver_count",
    "broadcast_counts",
    "broadcast_weak_upgrade",
    "broadcast_copy_panic_advances_cursor",
    "broadcast_recv_cooperative_ready_budget",
    "broadcast_recv_cooperative_pending_budget",
    "broadcast_closed_noncooperative",
    "io_readbuf_regions_clear",
    "io_partial_read_eof_zero_capacity",
    "io_read_exact_partial_success",
    "io_read_exact_early_eof",
    "io_read_exact_partial_error",
    "io_write_all_partial_zero",
    "io_write_all_partial_error",
    "io_exact_cancel_partial_late_wake",
    "io_exact_empty_no_poll",
    "io_partial_write_single_attempt",
    "io_write_zero_success",
    "io_write_vectored_default_first_nonempty",
    "io_flush_shutdown_order",
    "io_shutdown_terminal",
    "io_ready_ext_noncooperative",
}


def run(command: list[str], environment: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    if completed.returncode != 0:
        print(completed.stdout, file=sys.stderr)
        print(completed.stderr, file=sys.stderr)
        raise RuntimeError(
            f"命令失败（退出码 {completed.returncode}）：{' '.join(command)}"
        )
    return completed.stdout


def parse(output: str, source: str) -> dict[str, bool]:
    values: dict[str, bool] = {}
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        name, separator, raw_value = stripped.partition("=")
        if not separator:
            raise RuntimeError(f"{source} 输出非法行：{line}")
        if name not in EXPECTED_KEYS:
            raise RuntimeError(f"{source} 输出未知差分项：{name}")
        if name in values:
            raise RuntimeError(f"{source} 重复输出差分项：{name}")
        if raw_value not in {"0", "1"}:
            raise RuntimeError(f"{source} 输出非法布尔值：{line}")
        values[name] = raw_value == "1"

    missing = EXPECTED_KEYS - values.keys()
    if missing:
        raise RuntimeError(f"{source} 缺少差分项：{sorted(missing)}")
    if values.keys() != EXPECTED_KEYS:
        raise RuntimeError(f"{source} 差分项集合与基线不一致")
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cio", required=True)
    parser.add_argument("--cargo", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--target-dir", required=True)
    arguments = parser.parse_args()

    cio_values = parse(run([arguments.cio]), "CIO")

    environment = dict(os.environ)
    environment["CARGO_TERM_COLOR"] = "never"
    tokio_output = run(
        [
            arguments.cargo,
            "run",
            "--quiet",
            "--manifest-path",
            str(Path(arguments.manifest).resolve()),
            "--target-dir",
            str(Path(arguments.target_dir).resolve()),
        ],
        environment,
    )
    tokio_values = parse(tokio_output, "Tokio 1.53.1")

    failures: list[str] = []
    for key in sorted(EXPECTED_KEYS):
        if cio_values[key] != tokio_values[key]:
            failures.append(
                f"{key}: CIO={int(cio_values[key])}, "
                f"Tokio={int(tokio_values[key])}"
            )
        elif not cio_values[key]:
            failures.append(f"{key}: 两端都未满足契约")

    if failures:
        print("Tokio 语义差分失败：", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"Tokio 1.53.1 语义差分通过：{len(EXPECTED_KEYS)} 项")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
