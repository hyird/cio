// Command go-core is the Go 1.24 counterpart of ../bench_core.cpp.
//
// It deliberately uses fixed operation counts instead of testing.B's adaptive
// counts so a Go row and the corresponding cio row have the same workload and
// denominator.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultSpawns     = 1_000_000
	defaultHops       = 200_000
	defaultYields     = 2_000_000
	defaultExpired    = defaultYields / 4
	defaultTimers     = 200_000
	defaultThroughput = 500_000
)

type options struct {
	gomaxprocs int
	warmup     int
	repeats    int
	scale      float64
	format     string
	pattern    string
	timerDelay time.Duration
}

type workloadCounts struct {
	Spawns             int `json:"spawns"`
	ChannelRoundTrips  int `json:"channel_round_trips"`
	Yields             int `json:"yields"`
	ExpiredSleeps      int `json:"expired_sleeps"`
	ConcurrentTimers   int `json:"concurrent_timers"`
	ThroughputMessages int `json:"throughput_messages"`
}

type benchmark struct {
	name       string
	operations int
	run        func()
}

type sample struct {
	Benchmark  string  `json:"benchmark"`
	Repeat     int     `json:"repeat"`
	Operations int     `json:"operations"`
	ElapsedNS  int64   `json:"elapsed_ns"`
	NSPerOp    float64 `json:"ns_per_op"`
	OpsPerSec  float64 `json:"ops_per_sec"`
}

type summary struct {
	Benchmark       string  `json:"benchmark"`
	Operations      int     `json:"operations_per_repeat"`
	Samples         int     `json:"samples"`
	MedianNSPerOp   float64 `json:"median_ns_per_op"`
	MeanNSPerOp     float64 `json:"mean_ns_per_op"`
	MinNSPerOp      float64 `json:"min_ns_per_op"`
	MaxNSPerOp      float64 `json:"max_ns_per_op"`
	MedianOpsPerSec float64 `json:"median_ops_per_sec"`
}

type metadata struct {
	Schema           string         `json:"schema"`
	StartedAt        time.Time      `json:"started_at"`
	TotalElapsedNS   int64          `json:"total_elapsed_ns"`
	Hostname         string         `json:"hostname"`
	GoVersion        string         `json:"go_version"`
	GOOS             string         `json:"goos"`
	GOARCH           string         `json:"goarch"`
	NumCPU           int            `json:"num_cpu"`
	GOMAXPROCS       int            `json:"gomaxprocs"`
	WarmupSuites     int            `json:"warmup_suites"`
	Repeats          int            `json:"repeats"`
	Scale            float64        `json:"scale"`
	TimerDelayNS     int64          `json:"timer_delay_ns"`
	BenchmarkPattern string         `json:"benchmark_pattern"`
	Counts           workloadCounts `json:"counts"`
}

type report struct {
	Metadata  metadata  `json:"metadata"`
	Samples   []sample  `json:"samples"`
	Summaries []summary `json:"summaries"`
}

func parseOptions() (options, error) {
	var opts options
	flag.IntVar(&opts.gomaxprocs, "gomaxprocs", 0,
		"GOMAXPROCS to use; 0 preserves the current setting")
	flag.IntVar(&opts.warmup, "warmup", 1,
		"number of complete, unreported warm-up suites")
	flag.IntVar(&opts.repeats, "repeat", 5,
		"number of measured suites")
	flag.Float64Var(&opts.scale, "scale", 1,
		"multiply bench_core's operation counts (use a small value for smoke tests)")
	flag.StringVar(&opts.format, "format", "table",
		"output format: table or json")
	flag.StringVar(&opts.pattern, "bench", ".",
		"regular expression selecting benchmark names")
	flag.DurationVar(&opts.timerDelay, "timer-delay", time.Millisecond,
		"delay used by the concurrent timer benchmark")
	flag.Parse()

	switch {
	case opts.gomaxprocs < 0:
		return options{}, errors.New("-gomaxprocs must be >= 0")
	case opts.warmup < 0:
		return options{}, errors.New("-warmup must be >= 0")
	case opts.repeats < 1:
		return options{}, errors.New("-repeat must be >= 1")
	case math.IsNaN(opts.scale) || math.IsInf(opts.scale, 0) || opts.scale <= 0:
		return options{}, errors.New("-scale must be a finite number > 0")
	case opts.timerDelay < 0:
		return options{}, errors.New("-timer-delay must be >= 0")
	}
	opts.format = strings.ToLower(opts.format)
	if opts.format != "table" && opts.format != "json" {
		return options{}, errors.New("-format must be table or json")
	}
	if _, err := regexp.Compile(opts.pattern); err != nil {
		return options{}, fmt.Errorf("invalid -bench regular expression: %w", err)
	}
	return opts, nil
}

func scaledCount(base int, scale float64) (int, error) {
	scaled := math.Round(float64(base) * scale)
	maxInt := int(^uint(0) >> 1)
	if scaled > float64(maxInt) {
		return 0, fmt.Errorf("scaled operation count %.0f exceeds int capacity", scaled)
	}
	if scaled < 1 {
		return 1, nil
	}
	return int(scaled), nil
}

func makeCounts(scale float64) (workloadCounts, error) {
	var counts workloadCounts
	var err error
	if counts.Spawns, err = scaledCount(defaultSpawns, scale); err != nil {
		return workloadCounts{}, err
	}
	if counts.ChannelRoundTrips, err = scaledCount(defaultHops, scale); err != nil {
		return workloadCounts{}, err
	}
	if counts.Yields, err = scaledCount(defaultYields, scale); err != nil {
		return workloadCounts{}, err
	}
	if counts.ExpiredSleeps, err = scaledCount(defaultExpired, scale); err != nil {
		return workloadCounts{}, err
	}
	if counts.ConcurrentTimers, err = scaledCount(defaultTimers, scale); err != nil {
		return workloadCounts{}, err
	}
	if counts.ThroughputMessages, err = scaledCount(defaultThroughput, scale); err != nil {
		return workloadCounts{}, err
	}
	return counts, nil
}

func waitGroupDone(group *sync.WaitGroup) {
	group.Done()
}

// goroutineSpawnJoin matches bench_core's detached go() fan-out followed by a
// WaitGroup join. One operation is one goroutine created and joined.
func goroutineSpawnJoin(count int) {
	var group sync.WaitGroup
	group.Add(count)
	for i := 0; i < count; i++ {
		go waitGroupDone(&group)
	}
	group.Wait()
}

// yieldLoop matches one co_await cio::yield() per operation.
func yieldLoop(count int) {
	for i := 0; i < count; i++ {
		runtime.Gosched()
	}
}

// expiredSleepLoop is Go's no-park counterpart to cio::sleep(0).
func expiredSleepLoop(count int) {
	for i := 0; i < count; i++ {
		time.Sleep(0)
	}
}

func timerWorker(group *sync.WaitGroup, delay time.Duration) {
	time.Sleep(delay)
	group.Done()
}

// concurrentTimers includes goroutine creation, timer arm, park, fire, resume,
// and the final join, just like bench_core's concurrent_timers.
func concurrentTimers(count int, delay time.Duration) {
	var group sync.WaitGroup
	group.Add(count)
	for i := 0; i < count; i++ {
		go timerWorker(&group, delay)
	}
	group.Wait()
}

// pingPong uses the same two-channel topology as bench_core. One reported
// operation is one round trip, i.e. two channel transfers.
func pingPong(rounds, capacity int) {
	toPong := make(chan int, capacity)
	toPing := make(chan int, capacity)
	done := make(chan struct{})

	go func() {
		for i := 0; i < rounds; i++ {
			value := <-toPong
			toPing <- value
		}
		close(done)
	}()

	for i := 0; i < rounds; i++ {
		toPong <- 1
		<-toPing
	}
	<-done
}

func producer(out chan<- int, count int, group *sync.WaitGroup) {
	for i := 0; i < count; i++ {
		out <- 1
	}
	group.Done()
}

func consumer(in <-chan int, group *sync.WaitGroup) {
	for range in {
	}
	group.Done()
}

// channelThroughput copies bench_core's 1024-slot jobs channel, producer join,
// close, and consumer join. One operation is one complete message transfer.
func channelThroughput(messages, producers, consumers int) {
	jobs := make(chan int, 1024)
	var producing sync.WaitGroup
	producing.Add(producers)
	base := messages / producers
	remainder := messages % producers
	for p := 0; p < producers; p++ {
		count := base
		if p < remainder {
			count++
		}
		go producer(jobs, count, &producing)
	}

	var consuming sync.WaitGroup
	consuming.Add(consumers)
	for c := 0; c < consumers; c++ {
		go consumer(jobs, &consuming)
	}

	producing.Wait()
	close(jobs)
	consuming.Wait()
}

func makeBenchmarks(counts workloadCounts, timerDelay time.Duration) []benchmark {
	return []benchmark{
		{
			name:       "goroutine spawn+join",
			operations: counts.Spawns,
			run:        func() { goroutineSpawnJoin(counts.Spawns) },
		},
		{
			name:       "runtime.Gosched()",
			operations: counts.Yields,
			run:        func() { yieldLoop(counts.Yields) },
		},
		{
			name:       "sleep(0) already-expired path",
			operations: counts.ExpiredSleeps,
			run:        func() { expiredSleepLoop(counts.ExpiredSleeps) },
		},
		{
			name:       "timer arm+fire (concurrent)",
			operations: counts.ConcurrentTimers,
			run: func() {
				concurrentTimers(counts.ConcurrentTimers, timerDelay)
			},
		},
		{
			name:       "unbuffered chan round trip",
			operations: counts.ChannelRoundTrips,
			run: func() {
				pingPong(counts.ChannelRoundTrips, 0)
			},
		},
		{
			name:       "buffered chan round trip",
			operations: counts.ChannelRoundTrips,
			run: func() {
				pingPong(counts.ChannelRoundTrips, 64)
			},
		},
		{
			name:       "chan 1p1c throughput",
			operations: counts.ThroughputMessages,
			run: func() {
				channelThroughput(counts.ThroughputMessages, 1, 1)
			},
		},
		{
			name:       "chan 8p1c throughput",
			operations: counts.ThroughputMessages,
			run: func() {
				channelThroughput(counts.ThroughputMessages, 8, 1)
			},
		},
		{
			name:       "chan 1p8c throughput",
			operations: counts.ThroughputMessages,
			run: func() {
				channelThroughput(counts.ThroughputMessages, 1, 8)
			},
		},
		{
			name:       "chan 8p8c throughput",
			operations: counts.ThroughputMessages,
			run: func() {
				channelThroughput(counts.ThroughputMessages, 8, 8)
			},
		},
	}
}

func selectBenchmarks(all []benchmark, pattern string) ([]benchmark, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	selected := make([]benchmark, 0, len(all))
	for _, bench := range all {
		if re.MatchString(bench.name) {
			selected = append(selected, bench)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("-bench=%q matched no benchmarks", pattern)
	}
	return selected, nil
}

func measure(bench benchmark, repeat int) sample {
	start := time.Now()
	bench.run()
	elapsed := time.Since(start)
	ns := elapsed.Nanoseconds()
	nsPerOp := float64(ns) / float64(bench.operations)
	return sample{
		Benchmark:  bench.name,
		Repeat:     repeat,
		Operations: bench.operations,
		ElapsedNS:  ns,
		NSPerOp:    nsPerOp,
		OpsPerSec:  1e9 / nsPerOp,
	}
}

func runBenchmarks(benches []benchmark, warmup, repeats int) []sample {
	for suite := 0; suite < warmup; suite++ {
		for _, bench := range benches {
			bench.run()
		}
	}

	samples := make([]sample, 0, len(benches)*repeats)
	for repeat := 1; repeat <= repeats; repeat++ {
		for _, bench := range benches {
			samples = append(samples, measure(bench, repeat))
		}
	}
	return samples
}

func median(sorted []float64) float64 {
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func summarize(benches []benchmark, samples []sample) []summary {
	summaries := make([]summary, 0, len(benches))
	for _, bench := range benches {
		values := make([]float64, 0)
		total := 0.0
		for _, current := range samples {
			if current.Benchmark == bench.name {
				values = append(values, current.NSPerOp)
				total += current.NSPerOp
			}
		}
		sort.Float64s(values)
		medianNS := median(values)
		summaries = append(summaries, summary{
			Benchmark:       bench.name,
			Operations:      bench.operations,
			Samples:         len(values),
			MedianNSPerOp:   medianNS,
			MeanNSPerOp:     total / float64(len(values)),
			MinNSPerOp:      values[0],
			MaxNSPerOp:      values[len(values)-1],
			MedianOpsPerSec: 1e9 / medianNS,
		})
	}
	return summaries
}

func printTable(out *os.File, result report) {
	m := result.Metadata
	fmt.Fprintf(out, "Go core benchmarks — %s %s/%s, GOMAXPROCS=%d, CPUs=%d\n",
		m.GoVersion, m.GOOS, m.GOARCH, m.GOMAXPROCS, m.NumCPU)
	fmt.Fprintf(out,
		"warm-up suites=%d, measured suites=%d, scale=%g, timer delay=%s, total=%s\n\n",
		m.WarmupSuites, m.Repeats, m.Scale, time.Duration(m.TimerDelayNS),
		time.Duration(m.TotalElapsedNS))
	fmt.Fprintf(out, "%-31s %11s %7s %14s %14s %14s %16s\n",
		"benchmark", "ops/run", "samples", "median ns/op", "min ns/op",
		"max ns/op", "median ops/sec")
	fmt.Fprintf(out, "%-31s %11s %7s %14s %14s %14s %16s\n",
		"-------------------------------", "-----------", "-------",
		"--------------", "--------------", "--------------", "----------------")
	for _, current := range result.Summaries {
		fmt.Fprintf(out, "%-31s %11d %7d %14.1f %14.1f %14.1f %16.0f\n",
			current.Benchmark, current.Operations, current.Samples,
			current.MedianNSPerOp, current.MinNSPerOp, current.MaxNSPerOp,
			current.MedianOpsPerSec)
	}
}

func main() {
	opts, err := parseOptions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "go-core: %v\n", err)
		os.Exit(2)
	}
	if opts.gomaxprocs > 0 {
		runtime.GOMAXPROCS(opts.gomaxprocs)
	}

	counts, err := makeCounts(opts.scale)
	if err != nil {
		fmt.Fprintf(os.Stderr, "go-core: %v\n", err)
		os.Exit(2)
	}
	benches, err := selectBenchmarks(makeBenchmarks(counts, opts.timerDelay), opts.pattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "go-core: %v\n", err)
		os.Exit(2)
	}

	hostname, _ := os.Hostname()
	started := time.Now()
	samples := runBenchmarks(benches, opts.warmup, opts.repeats)
	elapsed := time.Since(started)
	result := report{
		Metadata: metadata{
			Schema:           "cio.go-core.v1",
			StartedAt:        started,
			TotalElapsedNS:   elapsed.Nanoseconds(),
			Hostname:         hostname,
			GoVersion:        runtime.Version(),
			GOOS:             runtime.GOOS,
			GOARCH:           runtime.GOARCH,
			NumCPU:           runtime.NumCPU(),
			GOMAXPROCS:       runtime.GOMAXPROCS(0),
			WarmupSuites:     opts.warmup,
			Repeats:          opts.repeats,
			Scale:            opts.scale,
			TimerDelayNS:     opts.timerDelay.Nanoseconds(),
			BenchmarkPattern: opts.pattern,
			Counts:           counts,
		},
		Samples:   samples,
		Summaries: summarize(benches, samples),
	}

	if opts.format == "json" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "go-core: encode JSON: %v\n", err)
			os.Exit(1)
		}
		return
	}
	printTable(os.Stdout, result)
}
