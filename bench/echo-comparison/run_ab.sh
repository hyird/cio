#!/usr/bin/env bash
# Interleaved A/B for two echo servers.
#
# Run-to-run drift on this class of machine is larger than the effects worth
# measuring — two sweeps minutes apart have disagreed by 15% on every server at
# once. So a before/after claim can only come from alternating the two builds
# inside one run, never from comparing two sweeps. This script is that: it runs
# A, B, A, B, ... and reports the mean of each side plus the delta.
#
#   ./run_ab.sh <binA> <binB> [conns] [dur_s] [payload] [repeats] [threads] \
#               [work_us] [heavy_pct] [churn]
#
# Both binaries must take "<port> <threads>" and print a line matching
# "echo server" once they are listening; the Go server satisfies the same
# contract.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="${HERE}/build"

BIN_A="${1:?usage: run_ab.sh <binA> <binB> [conns] [dur] [payload] [repeats] [threads]}"
BIN_B="${2:?usage: run_ab.sh <binA> <binB> [conns] [dur] [payload] [repeats] [threads]}"
CONNS="${3:-256}"
DUR="${4:-5}"
PAYLOAD="${5:-128}"
REPEATS="${6:-4}"
THREADS="${7:-8}"
WORK_US="${8:-0}"
HEAVY_PCT="${9:-0}"
CHURN="${10:-0}"
WARMUP="${WARMUP:-3}"

SERVER_CORES="${SERVER_CORES:-0-7}"
CLIENT_CORES="${CLIENT_CORES:-8-23}"
CLIENT_WORKERS="${CLIENT_WORKERS:-16}"
CLK_TCK="$(getconf CLK_TCK)"

for bin in "$BIN_A" "$BIN_B"; do
    [ -x "$bin" ] || { echo "not executable: $bin" >&2; exit 1; }
done
[ -x "${BUILD}/loadgen" ] || { echo "missing ${BUILD}/loadgen" >&2; exit 1; }

srv_cpu() {
    local raw
    raw="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo 0; return; }
    raw="${raw#*) }"
    echo "$raw" | awk -v c="$CLK_TCK" '{printf "%.2f", ($12+$13)/c}'
}

# $1 = binary, $2 = tag. Echoes "rps p50 p99 mean server_cpu".
one_run() {
    local bin="$1" tag="$2"
    # Inside ip_local_reserved_ports on this host, so a client ephemeral port
    # cannot squat on the one we are about to bind.
    local port=$((10000 + RANDOM % 1000))
    local log="/tmp/ab-${tag}-srv.log" out="/tmp/ab-${tag}-load.log"
    : > "$log"

    taskset -c "$SERVER_CORES" "$bin" "$port" "$THREADS" > "$log" 2>&1 &
    local pid=$!
    local up=0
    for _ in $(seq 1 100); do
        grep -q "echo server" "$log" 2>/dev/null && { up=1; break; }
        sleep 0.1
    done
    if [ "$up" -eq 0 ]; then
        kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
        echo "FAILED"
        return
    fi

    taskset -c "$CLIENT_CORES" "${BUILD}/loadgen" 127.0.0.1 "$port" "$CONNS" \
        "$WARMUP" "$DUR" "$PAYLOAD" "$CLIENT_WORKERS" "$WORK_US" "$HEAVY_PCT" \
        "$CHURN" > "$out" 2>&1 &
    local lp=$!
    sleep "$WARMUP"
    local c0; c0="$(srv_cpu "$pid")"
    sleep "$DUR"
    local c1; c1="$(srv_cpu "$pid")"
    wait "$lp" 2>/dev/null

    kill -INT "$pid" 2>/dev/null
    sleep 0.2
    kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

    local line; line="$(grep '^RESULT' "$out" || true)"
    if [ -z "$line" ]; then echo "FAILED"; return; fi

    local rps p50 p99 mean
    rps="$(sed -n 's/.*rps=\([0-9]*\).*/\1/p' <<< "$line")"
    p50="$(sed -n 's/.*p50=\([0-9.]*\).*/\1/p' <<< "$line")"
    p99="$(sed -n 's/.*p99=\([0-9.]*\).*/\1/p' <<< "$line")"
    mean="$(sed -n 's/.*mean=\([0-9.]*\).*/\1/p' <<< "$line")"
    echo "$rps $p50 $p99 $mean $(echo "$c1 - $c0" | bc)"
}

echo "== A/B  conns=${CONNS} payload=${PAYLOAD} threads=${THREADS} dur=${DUR}s repeats=${REPEATS}"
echo "   A = ${BIN_A}"
echo "   B = ${BIN_B}"
[ "$WORK_US" != 0 ] && echo "   work=${WORK_US}us heavy=${HEAVY_PCT}% churn=${CHURN}"
echo

a_rps=(); b_rps=(); a_cpu=(); b_cpu=(); a_mean=(); b_mean=()
for i in $(seq 1 "$REPEATS"); do
    # Alternate which side goes first, so a systematic drift over the run
    # (thermal, TIME_WAIT accumulation) cannot land on one side only.
    if [ $((i % 2)) -eq 1 ]; then seq_order=(A B); else seq_order=(B A); fi
    for side in "${seq_order[@]}"; do
        if [ "$side" = A ]; then r="$(one_run "$BIN_A" a)"; else r="$(one_run "$BIN_B" b)"; fi
        if [ "$r" = FAILED ]; then echo "  run${i} ${side}: FAILED"; continue; fi
        read -r rps p50 p99 mean scpu <<< "$r"
        printf "  run%-2d %s  rps=%-9s p50=%-8s p99=%-9s mean=%-8s cpu=%s\n" \
               "$i" "$side" "$rps" "$p50" "$p99" "$mean" "$scpu"
        if [ "$side" = A ]; then
            a_rps+=("$rps"); a_cpu+=("$scpu"); a_mean+=("$mean")
        else
            b_rps+=("$rps"); b_cpu+=("$scpu"); b_mean+=("$mean")
        fi
    done
    sleep 1  # let TIME_WAIT drain between repeats
done

mean_of() { printf '%s\n' "$@" | awk '{s+=$1; n++} END {if (n) printf "%.1f", s/n; else print 0}'; }

am="$(mean_of "${a_rps[@]}")"; bm="$(mean_of "${b_rps[@]}")"
ac="$(mean_of "${a_cpu[@]}")"; bc_="$(mean_of "${b_cpu[@]}")"
al="$(mean_of "${a_mean[@]}")"; bl="$(mean_of "${b_mean[@]}")"

echo
printf "  A mean rps %-10s cpu %-7s latency %s us\n" "$am" "$ac" "$al"
printf "  B mean rps %-10s cpu %-7s latency %s us\n" "$bm" "$bc_" "$bl"
awk -v a="$am" -v b="$bm" 'BEGIN {
    if (a > 0) printf "  B/A = %+.2f%%\n", (b - a) / a * 100
}'
