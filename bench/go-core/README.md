# Go 1.24 core benchmark

This is the Go counterpart of [`../bench_core.cpp`](../bench_core.cpp). It is
kept in its own Go module and does not link to or modify the cio runtime.

The default operation counts and denominators intentionally match `bench_core`:

| Go row | `bench_core` operation definition |
|---|---|
| `goroutine spawn+join` | fan out detached work, then join one `WaitGroup`; one goroutine is one op |
| `runtime.Gosched()` | one scheduler yield is one op |
| `sleep(0) already-expired path` | a zero-duration sleep which does not park is one op |
| `timer arm+fire (concurrent)` | create task, arm 1 ms timer, park, fire, resume and join; one timer is one op |
| unbuffered / buffered round trip | two channels, one transfer in each direction; one round trip is one op |
| 1p1c / 8p1c / 1p8c / 8p8c throughput | a 1024-slot jobs channel; one complete message is one op |

The extra join at the end of each ping-pong run only prevents a benchmark
goroutine leaking into the next sample; its fixed cost is amortized over the
reported round trips.

## Run

Use Go 1.24 and the same CPU set and scheduler width as cio:

```sh
cd bench/go-core
go build -o go_core .
taskset -c 0-23 ./go_core -gomaxprocs=24 -warmup=1 -repeat=5
taskset -c 0-23 ../../build/bench_core 24
```

The default table reports the median and range of all measured suites. Warm-up
suites execute the complete selected workload but are excluded from every
reported statistic. For raw samples and run metadata, use JSON:

```sh
./go_core -gomaxprocs=24 -warmup=1 -repeat=5 -format=json > go-core.json
```

Useful controls:

```text
-bench regexp       select rows by name (default ".")
-scale number       scale all bench_core operation counts (default 1)
-timer-delay d      concurrent timer delay (default 1ms)
-warmup n           unreported complete suites (default 1)
-repeat n           measured complete suites (default 5)
-format table|json  summary table or raw samples plus summaries
```

`-scale` exists for smoke tests; performance comparisons should use `-scale=1`.
For meaningful same-machine results, pin both binaries to the same CPUs, use
the same `-gomaxprocs` / worker count, keep the machine otherwise idle, and
interleave repeated Go and cio runs if measuring a small difference.
