package main

import (
	"math"
	"testing"
	"time"
)

func TestWorkloadsComplete(t *testing.T) {
	tests := []struct {
		name string
		run  func()
	}{
		{"goroutine spawn+join", func() { goroutineSpawnJoin(32) }},
		{"yield", func() { yieldLoop(32) }},
		{"expired sleep", func() { expiredSleepLoop(32) }},
		{"concurrent timers", func() { concurrentTimers(16, time.Microsecond) }},
		{"unbuffered ping-pong", func() { pingPong(32, 0) }},
		{"buffered ping-pong", func() { pingPong(32, 64) }},
		{"1p1c throughput", func() { channelThroughput(37, 1, 1) }},
		{"8p1c throughput", func() { channelThroughput(37, 8, 1) }},
		{"1p8c throughput", func() { channelThroughput(37, 1, 8) }},
		{"8p8c throughput", func() { channelThroughput(37, 8, 8) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				test.run()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("workload did not complete")
			}
		})
	}
}

func TestMakeCountsMatchesBenchCoreDefaults(t *testing.T) {
	counts, err := makeCounts(1)
	if err != nil {
		t.Fatal(err)
	}
	want := workloadCounts{
		Spawns:             defaultSpawns,
		ChannelRoundTrips:  defaultHops,
		Yields:             defaultYields,
		ExpiredSleeps:      defaultExpired,
		ConcurrentTimers:   defaultTimers,
		ThroughputMessages: defaultThroughput,
	}
	if counts != want {
		t.Fatalf("counts = %+v, want %+v", counts, want)
	}
}

func TestSummarizeUsesMedian(t *testing.T) {
	benches := []benchmark{{name: "x", operations: 10}}
	samples := []sample{
		{Benchmark: "x", NSPerOp: 30},
		{Benchmark: "x", NSPerOp: 10},
		{Benchmark: "x", NSPerOp: 20},
	}
	got := summarize(benches, samples)
	if len(got) != 1 {
		t.Fatalf("got %d summaries, want 1", len(got))
	}
	if got[0].MedianNSPerOp != 20 || got[0].MinNSPerOp != 10 ||
		got[0].MaxNSPerOp != 30 || got[0].MeanNSPerOp != 20 {
		t.Fatalf("unexpected summary: %+v", got[0])
	}
	if math.Abs(got[0].MedianOpsPerSec-50_000_000) > 0.01 {
		t.Fatalf("median ops/sec = %f", got[0].MedianOpsPerSec)
	}
}
