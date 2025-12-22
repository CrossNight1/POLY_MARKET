package main

import (
	"log"
	"time"
)

func main() {
	// Redis
	redisClient := NewRedis()
	defer redisClient.Close()

	// Time slot (UTC, 15m aligned)
	now := time.Now().UTC()
	currentSlot := now.Truncate(15 * time.Minute)

	// Assets
	assets := []string{"xrp", "eth", "btc", "sol"}

	// Fetch initial markets
	markets, slot := fetch15mMarkets(assets, currentSlot, false)
	if len(markets) == 0 {
		log.Fatal("No initial markets found")
	}

	// Init WS client
	ws := NewWS(assets, slot, redisClient)

	// Start WS
	ws.Start(markets)
}
