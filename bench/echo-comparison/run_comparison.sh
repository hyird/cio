#!/usr/bin/env bash
# Echo-server comparison: cio vs Boost.Asio (shared-nothing) vs Go.
#
# Every server gets the same 8 cores, the same load generator on a disjoint set
# of cores, and the same payload, connection count and measurement window.
# Server CPU is sampled only across the measurement window, so warm-up and
# connection setup stay out of the efficiency numbers.
#
#   ./run_comparison.sh [connections] [duration_s] [payload] [repeats]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="${HERE}/build"

CONNECTIONS="${1:-256}"
DURATION="${2:-10}"
PAYLOAD="${3:-128}"
REPEATS="${4:-3}"
WARMUP=3

SERVER_CORES="${SERVER_CORES:-0-7}"
CLIENT_CORES="${CLIENT_CORES:-8-23}"
SERVER_THREADS="${SERVER_THREADS:-8}"
CLIENT_WORKERS="${CLIENT_WORKERS:-16}"

CLK_TCK="$(getconf CLK_TCK)"

# ---------------------------------------------------------------- helpers ---

server_cpu_seconds() {  # $1 = pid; sums utime+stime over all threads of the process
    local stat
    stat="$(cat "/proc/$1/stat" 2>/dev/null)" || { echo 0; return; }
    # Fields after the (possibly space-containing) comm: utime is 14, stime 15.
    local rest="${stat#*) }"
    local utime stime
    utime="$(echo "$rest" | awk '{print $12}')"
    stime="$(echo "$rest" | awk '{print $13}')"
    echo "scale=3; ($utime + $stime) / $CLK_TCK" | bc
}

wait_for_line() {  # $1 = logfile, $2 = pattern, $3 = timeout deciseconds
    local i=0
    while [ "$i" -lt "$3" ]; do
        grep -q "$2" "$1" 2>/dev/null && return 0
        sleep 0.1
        i=$((i + 1))
    done
    return 1
}

# $1 = label, $2 = port, $3... = server command
run_case() {
    local label="$1" port="$2"
    shift 2
    local log="/tmp/echocmp-${label}.log"

    for attempt in $(seq 1 "$REPEATS"); do
        : > "$log"
        taskset -c "$SERVER_CORES" "$@" > "$log" 2>&1 &
        local pid=$!

        if ! wait_for_line "$log" "echo server" 100; then
            echo "${label}: server failed to start"
            sed -n '1,5p' "$log"
            kill -9 "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
            return 1
        fi

        # Load generator on disjoint cores, in the background so we can sample
        # the server's CPU across exactly the measurement window.
        local out="/tmp/echocmp-${label}-load.log"
        taskset -c "$CLIENT_CORES" "${BUILD}/loadgen" 127.0.0.1 "$port" \
            "$CONNECTIONS" "$WARMUP" "$DURATION" "$PAYLOAD" "$CLIENT_WORKERS" \
            > "$out" 2>&1 &
        local load_pid=$!

        sleep "$((WARMUP))"
        local cpu_before
        cpu_before="$(server_cpu_seconds "$pid")"
        sleep "$DURATION"
        local cpu_after
        cpu_after="$(server_cpu_seconds "$pid")"

        wait "$load_pid" 2>/dev/null
        local result
        result="$(grep '^RESULT' "$out" || true)"

        kill -INT "$pid" 2>/dev/null
        sleep 0.3
        kill -9 "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null

        if [ -z "$result" ]; then
            echo "${label}: load generator produced no result"
            sed -n '1,5p' "$out"
            return 1
        fi

        local server_cpu
        server_cpu="$(echo "scale=3; $cpu_after - $cpu_before" | bc)"
        echo "${label} run${attempt} ${result} server_cpu=${server_cpu}"

        sleep 1  # let TIME_WAIT sockets drain between runs
    done
}

# ------------------------------------------------------------------ build ---

echo "== building"
mkdir -p "$BUILD"

CIO_ROOT="$(cd "${HERE}/../.." && pwd)"
CIO_LIB="${CIO_ROOT}/build/libcio.a"
if [ ! -f "$CIO_LIB" ]; then
    echo "cio library not found at $CIO_LIB — build the project first" >&2
    exit 1
fi
CXXFLAGS="-std=c++20 -O3 -DNDEBUG -march=native -foptimize-sibling-calls -I${CIO_ROOT}/include"

g++ $CXXFLAGS "${HERE}/cio_echo.cpp" "$CIO_LIB" -o "${BUILD}/cio_echo" -pthread || exit 1
g++ $CXXFLAGS "${HERE}/loadgen.cpp" "$CIO_LIB" -o "${BUILD}/loadgen" -pthread || exit 1
g++ $CXXFLAGS "${HERE}/asio_echo_callback.cpp" -o "${BUILD}/asio_echo_callback" -pthread || exit 1
g++ $CXXFLAGS "${HERE}/asio_echo_coro.cpp" -o "${BUILD}/asio_echo_coro" -pthread || exit 1

if [ -n "${GOROOT:-}" ] && [ -x "${GOROOT}/bin/go" ]; then
    (cd "$HERE" && GOROOT="$GOROOT" GOCACHE=/tmp/gocache GOPATH=/tmp/gopath \
        "${GOROOT}/bin/go" build -o "${BUILD}/go_echo" go_echo.go) || exit 1
else
    echo "GOROOT not set or go missing — skipping the Go server" >&2
fi

# -------------------------------------------------------------------- run ---

echo
echo "== configuration"
echo "server cores    ${SERVER_CORES} (${SERVER_THREADS} threads)"
echo "client cores    ${CLIENT_CORES} (${CLIENT_WORKERS} workers)"
echo "connections     ${CONNECTIONS}"
echo "payload         ${PAYLOAD} bytes"
echo "warmup/measure  ${WARMUP}s / ${DURATION}s, ${REPEATS} repeats"
echo

echo "== results"
run_case "cio"           9201 "${BUILD}/cio_echo"           9201 "$SERVER_THREADS"
run_case "asio-callback" 9202 "${BUILD}/asio_echo_callback" 9202 "$SERVER_THREADS"
run_case "asio-coro"     9203 "${BUILD}/asio_echo_coro"     9203 "$SERVER_THREADS"
if [ -x "${BUILD}/go_echo" ]; then
    run_case "go"        9204 "${BUILD}/go_echo"            9204 "$SERVER_THREADS"
fi
