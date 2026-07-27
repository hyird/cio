#!/usr/bin/env bash
# The full matrix. run_comparison.sh answers "which is faster on one workload";
# this one asks where each design's advantage actually comes from and where it
# stops applying.
#
# Five sweeps:
#   payload      does the gap scale with bytes moved, or is it fixed overhead?
#   threads      does it widen with more cores sharing state?
#   connections  latency-bound at one end, saturated at the other
#   skew         some connections cost 50x more CPU than others. This is the one
#                that charges shared-nothing for the load balancing it gives up,
#                and the reason the flat echo benchmark cannot be the whole story
#   churn        reconnect every N requests: exercises accept, where asio has a
#                SO_REUSEPORT acceptor per thread and cio has one for all of them
#
#   ./run_matrix.sh [duration_s]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
B="${HERE}/build"
DUR="${1:-5}"
WARMUP=3
SERVER_CORES="${SERVER_CORES:-0-7}"
CLIENT_CORES="${CLIENT_CORES:-8-23}"
CLIENT_WORKERS="${CLIENT_WORKERS:-16}"
CLK_TCK="$(getconf CLK_TCK)"

declare -A CMD=(
    [cio]="$B/cio_echo"
    [asio-cb]="$B/asio_echo_callback"
    [asio-coro]="$B/asio_echo_coro"
    [go]="$B/go_echo"
)
ORDER=(cio asio-cb asio-coro go)

srv_cpu() {
    local raw
    raw="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo 0; return; }
    raw="${raw#*) }"
    echo "$raw" | awk -v c="$CLK_TCK" '{printf "%.2f", ($12+$13)/c}'
}

# label threads conns payload work_us heavy_pct churn
run_one() {
    local label="$1" threads="$2" conns="$3" payload="$4" work="$5" heavy="$6" churn="$7"
    for name in "${ORDER[@]}"; do
        [ -x "${CMD[$name]}" ] || continue
        local port=$((10000 + RANDOM % 1000))  # inside ip_local_reserved_ports, so a client ephemeral port cannot squat on it
        taskset -c "$SERVER_CORES" "${CMD[$name]}" "$port" "$threads" > /tmp/mx-srv.log 2>&1 &
        local pid=$!
        local up=0
        for _ in $(seq 1 100); do
            grep -q "echo server" /tmp/mx-srv.log 2>/dev/null && { up=1; break; }
            sleep 0.1
        done
        if [ "$up" -eq 0 ]; then
            echo "$label,$name,FAILED_TO_START,,,,,"
            kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
            continue
        fi

        taskset -c "$CLIENT_CORES" "$B/loadgen" 127.0.0.1 "$port" "$conns" "$WARMUP" \
            "$DUR" "$payload" "$CLIENT_WORKERS" "$work" "$heavy" "$churn" \
            > /tmp/mx-load.log 2>&1 &
        local lp=$!
        sleep "$WARMUP"
        local c0; c0="$(srv_cpu "$pid")"
        sleep "$DUR"
        local c1; c1="$(srv_cpu "$pid")"
        wait "$lp" 2>/dev/null

        local rps p50 p99 ccpu
        rps="$(grep -o 'rps=[0-9]*' /tmp/mx-load.log | cut -d= -f2)"
        p50="$(grep -o 'p50=[0-9.]*' /tmp/mx-load.log | cut -d= -f2)"
        p99="$(grep -o 'p99=[0-9.]*' /tmp/mx-load.log | cut -d= -f2)"
        ccpu="$(grep -o 'client_cpu=[0-9.]*' /tmp/mx-load.log | cut -d= -f2)"
        kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

        local scpu; scpu="$(echo "$c1 - $c0" | bc)"
        echo "$label,$name,${rps:-0},${p50:-0},${p99:-0},${scpu},${ccpu:-0}"
        sleep 0.5
    done
}

echo "config,server,rps,p50_us,p99_us,server_cpu_s,client_cpu_s"

echo "# sweep: payload (8 threads, 512 connections)" >&2
for p in 16 128 1024 4096 16384; do
    run_one "payload=$p" 8 512 "$p" 0 0 0
done

echo "# sweep: server threads (512 connections, 128B, 8 cores throughout)" >&2
for t in 1 2 4 8 16; do
    run_one "threads=$t" "$t" 512 128 0 0 0
done

echo "# sweep: connections (8 threads, 128B)" >&2
for c in 1 8 64 256 1024 4096; do
    run_one "conns=$c" 8 "$c" 128 0 0 0
done

echo "# sweep: skew (8 threads, 256 connections, heavy = 50us of CPU)" >&2
for h in 0 5 25 100; do
    run_one "skew=${h}pct" 8 256 128 50 "$h" 0
done

echo "# sweep: connection churn (8 threads, 256 connections)" >&2
for k in 0 100 10 1; do
    run_one "churn=$k" 8 256 128 0 0 "$k"
done
