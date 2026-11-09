package intrinio

import (
	"log"
	"math"
	"sync"
	"time"
)

// CandleAggregator handles the aggregation of trades into minute candles
type CandleAggregator struct {
	tradesByMinute map[string]map[float64][]EquityTrade // symbol -> minute -> trades
	mu             sync.RWMutex
	ticker         *time.Ticker
	stopChan       chan bool
	onCandle       func(EquityCandle)
}

// AggregateCandle creates a new candle aggregator
func AggregateCandle(onCandle func(EquityCandle)) *CandleAggregator {
	aggregator := &CandleAggregator{
		tradesByMinute: make(map[string]map[float64][]EquityTrade),
		onCandle:       onCandle,
		stopChan:       make(chan bool),
	}

	// Start the ticker to process candles every minute
	aggregator.startTicker()

	return aggregator
}

// AddTrade adds a trade to the aggregator
func (ca *CandleAggregator) AddTrade(trade EquityTrade) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	// Calculate the minute timestamp for this trade
	minuteTimestamp := math.Floor(trade.Timestamp/60) * 60

	// Initialize the symbol map if it doesn't exist
	if ca.tradesByMinute[trade.Symbol] == nil {
		ca.tradesByMinute[trade.Symbol] = make(map[float64][]EquityTrade)
	}

	// Add the trade to the appropriate minute
	ca.tradesByMinute[trade.Symbol][minuteTimestamp] = append(
		ca.tradesByMinute[trade.Symbol][minuteTimestamp],
		trade,
	)

	log.Printf("DEBUG: Added trade for %s at minute %.2f, total trades in minute: %d",
		trade.Symbol, minuteTimestamp, len(ca.tradesByMinute[trade.Symbol][minuteTimestamp]))
}

// tradeHandler returns a trade callback that adds each trade to the aggregator
// and then calls onTrade.
func (ca *CandleAggregator) tradeHandler(onTrade func(EquityTrade)) func(EquityTrade) {
	return func(trade EquityTrade) {
		ca.AddTrade(trade)
		if onTrade != nil {
			onTrade(trade)
		}
	}
}

// startTicker starts the ticker that processes candles every minute
func (ca *CandleAggregator) startTicker() {
	// Calculate time until next minute boundary
	now := time.Now()
	nextMinute := now.Truncate(time.Minute).Add(time.Minute)
	initialDelay := nextMinute.Sub(now)

	// Wait for the next minute boundary, then start ticking every minute
	time.Sleep(initialDelay)

	ca.ticker = time.NewTicker(time.Minute)

	go func() {
		for {
			select {
			case <-ca.ticker.C:
				ca.ProcessCandles()
			case <-ca.stopChan:
				ca.ticker.Stop()
				return
			}
		}
	}()
}

// ProcessCandles processes all trades and generates candles for completed minutes
func (ca *CandleAggregator) ProcessCandles() {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	currentTime := float64(time.Now().Unix())
	currentMinute := math.Floor(currentTime/60) * 60

	// Process each symbol
	for symbol, minuteTrades := range ca.tradesByMinute {
		// Find the minute that just completed (previous minute)
		completedMinute := currentMinute - 60

		if trades, exists := minuteTrades[completedMinute]; exists && len(trades) > 0 {
			// Generate candle for this symbol and minute
			candle := ca.buildCandle(symbol, trades, completedMinute)

			log.Printf("DEBUG: Generated candle for %s at minute %.2f: O:%.2f H:%.2f L:%.2f C:%.2f V:%d",
				symbol, completedMinute, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume)

			// Send the candle
			if ca.onCandle != nil {
				ca.onCandle(candle)
			}

			// Remove the processed trades
			delete(minuteTrades, completedMinute)
		}
	}
}

// buildCandle builds a candle from a slice of trades
func (ca *CandleAggregator) buildCandle(symbol string, trades []EquityTrade, minuteTimestamp float64) EquityCandle {
	if len(trades) == 0 {
		return EquityCandle{}
	}

	candle := EquityCandle{
		Symbol:    symbol,
		Open:      trades[0].Price,
		High:      trades[0].Price,
		Low:       trades[0].Price,
		Close:     trades[len(trades)-1].Price,
		Volume:    0,
		Timestamp: minuteTimestamp,
	}

	for _, trade := range trades {
		if trade.Price > candle.High {
			candle.High = trade.Price
		}
		if trade.Price < candle.Low {
			candle.Low = trade.Price
		}
		candle.Volume += trade.Size
	}

	return candle
}

// Stop stops the candle aggregator
func (ca *CandleAggregator) Stop() {
	close(ca.stopChan)
}

// GetStats returns statistics about the aggregator
func (ca *CandleAggregator) GetStats() map[string]int {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	stats := make(map[string]int)
	totalTrades := 0

	for symbol, minuteTrades := range ca.tradesByMinute {
		symbolTrades := 0
		for _, trades := range minuteTrades {
			symbolTrades += len(trades)
		}
		stats[symbol] = symbolTrades
		totalTrades += symbolTrades
	}

	stats["total"] = totalTrades
	return stats
}
