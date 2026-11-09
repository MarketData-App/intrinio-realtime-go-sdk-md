package main

import (
	"log"
	"sync"
	"time"

	"github.com/intrinio/intrinio-realtime-go-sdk"
)

var eTradeCount int = 0
var eTradeCountLock sync.RWMutex
var eQuoteCount int = 0
var eQuoteCountLock sync.RWMutex
var eCandleCount int = 0
var eCandleCountLock sync.RWMutex

func handleEquityTrade(trade intrinio.EquityTrade) {
	eTradeCountLock.Lock()
	eTradeCount++
	eTradeCountLock.Unlock()
	// if eTradeCount%10 == 0 {
	// 	log.Printf("%+v\n", trade)
	// }
}

func handleEquityQuote(quote intrinio.EquityQuote) {
	eQuoteCountLock.Lock()
	eQuoteCount++
	eQuoteCountLock.Unlock()
	// if eQuoteCount%100 == 0 {
	// 	log.Printf("%+v\n", quote)
	// }
}

func handleEquityCandle(candle intrinio.EquityCandle) {
	eCandleCountLock.Lock()
	eCandleCount++
	eCandleCountLock.Unlock()
	log.Printf("Candle for %s: O: %.2f, H: %.2f, L: %.2f, C: %.2f, V: %d",
		candle.Symbol, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume)
}

func reportEquities(ticker <-chan time.Time) {
	for {
		<-ticker
		eTradeCountLock.RLock()
		tc := eTradeCount
		eTradeCountLock.RUnlock()
		eQuoteCountLock.RLock()
		qc := eQuoteCount
		eQuoteCountLock.RUnlock()
		eCandleCountLock.RLock()
		cc := eCandleCount
		eCandleCountLock.RUnlock()
		log.Printf("Equity Trade Count: %d, Equity Quote Count: %d, Equity Candle Count: %d\n", tc, qc, cc)
	}
}

func runEquitiesExample() *intrinio.Client {
	var config intrinio.Config = intrinio.LoadConfig("equities-config.json")

	// Create candle aggregator using the new AggregateCandle function
	candleAggregator := intrinio.AggregateCandle(handleEquityCandle)

	var client *intrinio.Client = intrinio.NewEquitiesClient(config,
		func(trade intrinio.EquityTrade) {
			// Handle trade counting
			handleEquityTrade(trade)
			// Add trade to candle aggregator
			candleAggregator.AddTrade(trade)
		},
		handleEquityQuote,
		nil) // Don't use built-in candle handling, use our aggregator

	client.Start()
	symbols := []string{"AAPL", "MSFT"}
	client.JoinMany(symbols)
	//client.Join("GOOG")
	//client.JoinLobby()
	var ticker *time.Ticker = time.NewTicker(30 * time.Second)
	go reportEquities(ticker.C)
	return client
}
