#!/usr/bin/env bash
# Interleaved A/B of two HTTP server binaries under wrk.
#
# Like echo-comparison/run_ab.sh, this holds the load generator fixed while
# interleaving the two server binaries. Here the fixed generator is third-party
# wrk; echo uses a frozen cio-based load-generator binary.
#
#   ./ab_wrk.sh <binA> <binB> [connections] [duration_s] [repeats] [threads]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_A="${1:?usage: ab_wrk.sh <binA> <binB> [conns] [dur] [repeats] [threads]}"
BIN_B="${2:?usage: ab_wrk.sh <binA> <binB> [conns] [dur] [repeats] [threads]}"
CONNS="${3:-256}"
DUR="${4:-8}"
REPEATS="${5:-4}"
THREADS="${6:-8}"
WARMUP="${WARMUP:-3}"

SERVER_CORES="${SERVER_CORES:-0-7}"
CLIENT_CORES="${CLIENT_CORES:-8-23}"
WRK_THREADS="${WRK_THREADS:-8}"
CLK="$(getconf CLK_TCK)"

command -v wrk >/dev/null || { echo "wrk not found" >&2; exit 1; }
for b in "$BIN_A" "$BIN_B"; do [ -x "$b" ] || { echo "not executable: $b" >&2; exit 1; }; done

srv_cpu() {
    local raw; raw="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo 0; return; }
    raw="${raw#*) }"; echo "$raw" | awk -v c="$CLK" '{printf "%.2f", ($12+$13)/c}'
}

one_run() {   # $1 = binary, $2 = tag; echoes "rps cpu"
    local bin="$1" tag="$2"
    local port=$((15000 + RANDOM % 800))
    local srvlog="/tmp/abwrk-$tag-srv.log" log="/tmp/abwrk-$tag.log"
    taskset -c "$SERVER_CORES" "$bin" "$port" "$THREADS" > "$srvlog" 2>&1 &
    local pid=$!
    local up=0
    for _ in $(seq 1 100); do
        grep -qi "http server" "$srvlog" 2>/dev/null && { up=1; break; }; sleep 0.1
    done
    [ "$up" -eq 0 ] && { kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; echo "FAILED"; return; }

    taskset -c "$CLIENT_CORES" wrk -t"$WRK_THREADS" -c"$CONNS" -d"${WARMUP}s" \
        "http://127.0.0.1:$port/" >/dev/null 2>&1
    local c0; c0="$(srv_cpu "$pid")"
    taskset -c "$CLIENT_CORES" wrk -t"$WRK_THREADS" -c"$CONNS" -d"${DUR}s" --latency \
        "http://127.0.0.1:$port/" > "$log" 2>&1
    local c1; c1="$(srv_cpu "$pid")"
    kill -INT "$pid" 2>/dev/null; sleep 0.2; kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

    local rps p50 p99
    rps="$(awk '/^Requests\/sec:/ {print $2}' "$log")"
    p50="$(awk '/^[[:space:]]*50%[[:space:]]/ {print $2; exit}' "$log")"
    p99="$(awk '/^[[:space:]]*99%[[:space:]]/ {print $2; exit}' "$log")"
    [ -z "$rps" ] && { echo "FAILED"; return; }
    echo "$rps $p50 $p99 $(echo "$c1 - $c0" | bc)"
}

echo "== wrk A/B  conns=$CONNS threads=$THREADS dur=${DUR}s repeats=$REPEATS"
echo "   A = $BIN_A"
echo "   B = $BIN_B"
echo

a_rps=(); b_rps=(); a_cpu=(); b_cpu=()
for i in $(seq 1 "$REPEATS"); do
    if [ $((i % 2)) -eq 1 ]; then order=(A B); else order=(B A); fi
    for side in "${order[@]}"; do
        if [ "$side" = A ]; then r="$(one_run "$BIN_A" a)"; else r="$(one_run "$BIN_B" b)"; fi
        [ "$r" = FAILED ] && { echo "  run$i $side: FAILED"; continue; }
        read -r rps p50 p99 cpu <<< "$r"
        printf "  run%-2d %s  rps=%-12s p50=%-9s p99=%-9s cpu=%s\n" "$i" "$side" "$rps" "$p50" "$p99" "$cpu"
        if [ "$side" = A ]; then a_rps+=("$rps"); a_cpu+=("$cpu"); else b_rps+=("$rps"); b_cpu+=("$cpu"); fi
    done
done

mean_of() { printf '%s\n' "$@" | awk '{s+=$1; n++} END {if (n) printf "%.0f", s/n; else print 0}'; }
am="$(mean_of "${a_rps[@]}")"; bm="$(mean_of "${b_rps[@]}")"
ac="$(mean_of "${a_cpu[@]}")"; bc_="$(mean_of "${b_cpu[@]}")"
echo
printf "  A mean rps %-12s cpu %s\n" "$am" "$ac"
printf "  B mean rps %-12s cpu %s\n" "$bm" "$bc_"
awk -v a="$am" -v b="$bm" 'BEGIN { if (a>0) printf "  B/A = %+.2f%%\n", (b-a)/a*100 }'
