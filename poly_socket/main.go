package main

import (
	"fmt"
	"log"
	"time"
)

func runOnce() {
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
		log.Println("No markets found for slot", slot)
		return
	}

	// Init WS client
	ws := NewWS(assets, slot, redisClient)

	// Start WS (blocking or non-blocking depending on your impl)
	ws.Start(markets)
}

func waitUntilNext15m() {
	now := time.Now().UTC()
	next := now.Add(5 * time.Minute).Truncate(5 * time.Minute).Add(10 * time.Second)
	d := time.Until(next)
	if d > 0 {
		time.Sleep(d)
	}
	fmt.Println("Woke up at:", time.Now().UTC())
}

func main() {
	for {
		go runOnce()
		waitUntilNext15m()
	}
}
