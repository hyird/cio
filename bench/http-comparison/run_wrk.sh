#!/usr/bin/env bash
# HTTP comparison driven by wrk.
#
# The echo comparison next door has a load generator built on cio, which is a
# conflict of interest: an improvement to cio makes the generator faster too, so
# a server-side gain and a client-side gain are indistinguishable. That is not a
# hypothetical — the same scheduler change measured +7.9% against the old
# generator and +50% against a rebuilt one, and neither number is the server on
# its own.
#
# wrk is a third party. It is not built on any runtime under test, it does not
# change when they do, and it is the same binary for all three.
#
#   ./run_wrk.sh [connections] [duration_s] [repeats] [threads]
#
# Servers are interleaved rather than run in sequence, and the order rotates
# each repeat, so drift across the run cannot land on one server only.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
B="$HERE/build"

CONNS="${1:-256}"
DUR="${2:-10}"
REPEATS="${3:-3}"
THREADS="${4:-8}"
WARMUP="${WARMUP:-3}"

SERVER_CORES="${SERVER_CORES:-0-7}"
CLIENT_CORES="${CLIENT_CORES:-8-23}"
WRK_THREADS="${WRK_THREADS:-8}"
CLK="$(getconf CLK_TCK)"

command -v wrk >/dev/null || { echo "wrk not found" >&2; exit 1; }

declare -A CMD=([cio]="$B/cio_http" [asio]="$B/asio_http" [go]="$B/go_http")
ORDER=(cio asio go)

srv_cpu() {
    local raw
    raw="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo 0; return; }
    raw="${raw#*) }"
    echo "$raw" | awk -v c="$CLK" '{printf "%.2f", ($12+$13)/c}'
}

declare -A SUM_RPS SUM_CPU SUM_P50 SUM_P99 N

run_one() {
    local name="$1"
    local bin="${CMD[$name]}"
    [ -x "$bin" ] || { echo "  $name: missing $bin"; return; }
    local port=$((14000 + RANDOM % 800))
    local log="/tmp/wrk-$name.log"

    taskset -c "$SERVER_CORES" "$bin" "$port" "$THREADS" > "/tmp/wrk-$name-srv.log" 2>&1 &
    local pid=$!
    local up=0
    for _ in $(seq 1 100); do
        grep -qi "http server" "/tmp/wrk-$name-srv.log" 2>/dev/null && { up=1; break; }
        sleep 0.1
    done
    if [ "$up" -eq 0 ]; then
        echo "  $name: failed to start"; kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; return
    fi

    # Warm up: connections established, allocator and pools settled, and for Go
    # the GC and the scheduler's Ms grown to steady state.
    taskset -c "$CLIENT_CORES" wrk -t"$WRK_THREADS" -c"$CONNS" -d"${WARMUP}s" \
        "http://127.0.0.1:$port/" >/dev/null 2>&1

    local c0; c0="$(srv_cpu "$pid")"
    taskset -c "$CLIENT_CORES" wrk -t"$WRK_THREADS" -c"$CONNS" -d"${DUR}s" --latency \
        "http://127.0.0.1:$port/" > "$log" 2>&1
    local c1; c1="$(srv_cpu "$pid")"

    kill -INT "$pid" 2>/dev/null; sleep 0.2; kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

    local rps p50 p99 errs
    rps="$(awk '/^Requests\/sec:/ {print $2}' "$log")"
    p50="$(awk '/50%/ {print $2}' "$log")"
    p99="$(awk '/99%/ {print $2}' "$log")"
    errs="$(awk '/Socket errors/ {print $0}' "$log")"
    local non2xx; non2xx="$(awk '/Non-2xx/ {print $NF}' "$log")"
    local cpu; cpu="$(echo "$c1 - $c0" | bc)"

    if [ -z "$rps" ]; then echo "  $name: no result"; sed -n '1,6p' "$log"; return; fi

    printf "  %-5s rps=%-11s p50=%-9s p99=%-9s cpu=%-6s %s%s\n" \
        "$name" "$rps" "$p50" "$p99" "$cpu" "${errs:+[$errs] }" "${non2xx:+[non2xx=$non2xx]}"

    SUM_RPS[$name]="$(echo "${SUM_RPS[$name]:-0} + $rps" | bc)"
    SUM_CPU[$name]="$(echo "${SUM_CPU[$name]:-0} + $cpu" | bc)"
    N[$name]=$(( ${N[$name]:-0} + 1 ))
    sleep 0.5
}

echo "== wrk  conns=$CONNS threads=$THREADS(server) ${WRK_THREADS}(wrk) dur=${DUR}s repeats=$REPEATS"
echo "   server cores $SERVER_CORES, wrk cores $CLIENT_CORES"
wrk --version 2>&1 | head -1
echo

for r in $(seq 1 "$REPEATS"); do
    echo "-- repeat $r"
    # Rotate the order so no server always runs first.
    for i in $(seq 0 $((${#ORDER[@]} - 1))); do
        run_one "${ORDER[$(( (i + r - 1) % ${#ORDER[@]} ))]}"
    done
done

echo
printf "%-6s %14s %12s\n" "server" "mean rps" "mean cpu_s"
for name in "${ORDER[@]}"; do
    [ "${N[$name]:-0}" -gt 0 ] || continue
    printf "%-6s %14.0f %12.2f\n" "$name" \
        "$(echo "${SUM_RPS[$name]} / ${N[$name]}" | bc -l)" \
        "$(echo "${SUM_CPU[$name]} / ${N[$name]}" | bc -l)"
done
