package intrinio

import (
	"testing"
	"time"
)

// newTestAggregator builds a CandleAggregator without starting the real
// minute ticker (which would sleep until the next minute boundary).
func newTestAggregator() *CandleAggregator {
	return &CandleAggregator{
		tradesByMinute: make(map[string]map[float64][]EquityTrade),
		onCandle:       func(EquityCandle) {},
	}
}

// Regression test for issue #127: ProcessCandles must evict EVERY completed
// minute bucket, not just currentMinute-60. A delayed feed (e.g. DELAYED_SIP,
// ~15-min delay) files trades into buckets ~900s in the past; the old code only
// ever evicted currentMinute-60, so those buckets accumulated forever and the
// process OOM-killed at ~64 GiB roughly every 48h.
func TestProcessCandlesEvictsAllCompletedMinutes(t *testing.T) {
	ca := newTestAggregator()

	now := float64(time.Now().Unix())
	// 15 distinct minute buckets, all well in the past (a delayed feed).
	const buckets = 15
	for i := 0; i < buckets; i++ {
		ts := now - 1800 + float64(i*60) // strictly < now-60, no boundary race
		for j := 0; j < 100; j++ {
			ca.AddTrade(EquityTrade{Symbol: "SPY", Price: 100, Size: 1, Timestamp: ts})
		}
	}

	if got := ca.GetStats()["total"]; got != buckets*100 {
		t.Fatalf("setup: expected %d trades buffered, got %d", buckets*100, got)
	}

	ca.ProcessCandles()

	if got := ca.GetStats()["total"]; got != 0 {
		t.Fatalf("leak: expected all completed-minute trades evicted, %d retained", got)
	}
	if got := len(ca.tradesByMinute); got != 0 {
		t.Fatalf("leak: expected empty symbol map after eviction, got %d symbols", got)
	}
}

// Trades for the in-progress minute (>= currentMinute) must be retained so the
// next tick can still complete that candle.
func TestProcessCandlesKeepsInProgressMinute(t *testing.T) {
	ca := newTestAggregator()

	now := float64(time.Now().Unix())
	// +120s is unambiguously >= currentMinute regardless of small clock movement.
	ts := now + 120
	for j := 0; j < 50; j++ {
		ca.AddTrade(EquityTrade{Symbol: "AAPL", Price: 10, Size: 1, Timestamp: ts})
	}

	ca.ProcessCandles()

	if got := ca.GetStats()["total"]; got != 50 {
		t.Fatalf("expected 50 in-progress trades retained, got %d", got)
	}
}

// A completed bucket must still produce a candle when evicted.
func TestProcessCandlesEmitsCandleForCompletedMinute(t *testing.T) {
	var emitted []EquityCandle
	ca := &CandleAggregator{
		tradesByMinute: make(map[string]map[float64][]EquityTrade),
		onCandle:       func(c EquityCandle) { emitted = append(emitted, c) },
	}

	now := float64(time.Now().Unix())
	ts := now - 120 // one completed minute in the past
	ca.AddTrade(EquityTrade{Symbol: "SPY", Price: 100, Size: 1, Timestamp: ts})
	ca.AddTrade(EquityTrade{Symbol: "SPY", Price: 105, Size: 2, Timestamp: ts})
	ca.AddTrade(EquityTrade{Symbol: "SPY", Price: 95, Size: 3, Timestamp: ts})

	ca.ProcessCandles()

	if len(emitted) != 1 {
		t.Fatalf("expected 1 candle emitted, got %d", len(emitted))
	}
	c := emitted[0]
	if c.Open != 100 || c.High != 105 || c.Low != 95 || c.Close != 95 || c.Volume != 6 {
		t.Fatalf("unexpected candle: %+v", c)
	}
}
