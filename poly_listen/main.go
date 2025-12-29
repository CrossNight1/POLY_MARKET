package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	DATA_API_URL  = "https://data-api.polymarket.com"
	GAMMA_API_URL = "https://gamma-api.polymarket.com"
	WALLETS       = map[string]string{
		"Andromeda":         "0x39932ca2b7a1b8ab6cbf0b8f7419261b950ccded",
		"hopedieslast":      "0x5739ddf8672627ce076eff5f444610a250075f1a",
		"distinct-baguette": "0xe00740bce98a594e26861838885ab310ec3b548c",
	}
	ctx = context.Background()
)

type Trade map[string]interface{}
type Signal map[string]interface{}

type MarketSlugResolver struct {
	cache map[string]string
	mu    sync.RWMutex
}

func NewResolver() *MarketSlugResolver {
	return &MarketSlugResolver{cache: make(map[string]string)}
}

func (r *MarketSlugResolver) Get(marketID string) string {
	if marketID == "" {
		return ""
	}
	r.mu.RLock()
	if slug, ok := r.cache[marketID]; ok {
		r.mu.RUnlock()
		return slug
	}
	r.mu.RUnlock()

	slug := r.fetch(marketID)
	r.mu.Lock()
	r.cache[marketID] = slug
	r.mu.Unlock()
	return slug
}

func (r *MarketSlugResolver) fetch(marketID string) string {
	url := fmt.Sprintf("%s/markets/%s", GAMMA_API_URL, marketID)
	resp, err := http.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	if slug, ok := data["slug"].(string); ok {
		return slug
	}
	return ""
}

type RedisSink struct {
	client  *redis.Client
	signals string
	seen    string
	wallet  string
}

func NewRedisSink(wallet string) *RedisSink {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	return &RedisSink{
		client:  rdb,
		signals: fmt.Sprintf("poly:%s:signals", wallet),
		seen:    fmt.Sprintf("poly:%s:seen", wallet),
		wallet:  wallet,
	}
}

func (s *RedisSink) Push(signal Signal) {
	tid := signal["trade_id"].(string)
	s.client.SAdd(ctx, s.seen, tid)
	data, _ := json.Marshal(signal)
	s.client.RPush(ctx, s.signals, data)
}

func (s *RedisSink) SeenTrade(tid string) bool {
	exists, _ := s.client.SIsMember(ctx, s.seen, tid).Result()
	return exists
}

func getMarketId(slug string) string {
	url := fmt.Sprintf("%s/markets/slug/%s", GAMMA_API_URL, slug)
	resp, err := http.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	if id, ok := data["id"].(string); ok {
		return id
	}
	return ""
}

func getTrades(wallet string) []Trade {
	url := fmt.Sprintf("%s/trades?user=%s", DATA_API_URL, strings.ToLower(wallet))
	resp, err := http.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var trades []Trade
	_ = json.Unmarshal(body, &trades)
	return trades
}

func tradeID(trade Trade) string {
	for _, k := range []string{"transactionHash", "transaction_hash", "id", "hash"} {
		if v, ok := trade[k]; ok {
			return fmt.Sprint(v)
		}
	}
	b, _ := json.Marshal(trade)
	return string(b)
}

func normalizeTS(ts interface{}) int64 {
	switch t := ts.(type) {
	case float64:
		if t < 1e10 {
			return int64(t * 1000)
		}
		return int64(t)
	case int64:
		return t
	default:
		return time.Now().UnixMilli()
	}
}

func tradeToSignal(wallet string, trade Trade) Signal {
	return Signal{
		"wallet":    wallet,
		"trade_id":  tradeID(trade),
		"market":    trade["marketId"],
		"outcome":   trade["outcome"],
		"side":      strings.ToUpper(fmt.Sprint(trade["side"])),
		"price":     trade["price"],
		"size":      trade["size"],
		"timestamp": normalizeTS(trade["timestamp"]),
		"raw":       trade,
		"slug":      trade["slug"],
	}
}

func monitor(name string, wallet string, interval time.Duration, useRedis bool, wg *sync.WaitGroup) {
	defer wg.Done()
	resolver := NewResolver()
	var sink *RedisSink
	if useRedis {
		sink = NewRedisSink(wallet)
	}

	seenLocal := make(map[string]struct{})
	lastTS := time.Now().Add(-1 * time.Hour).UnixMilli()

	for {
		trades := getTrades(wallet)
		newCount := 0
		for i := len(trades) - 1; i >= 0; i-- {
			t := trades[i]
			tid := tradeID(t)
			if _, ok := seenLocal[tid]; ok {
				continue
			}

			signal := tradeToSignal(wallet, t)

			if signal["timestamp"].(int64) <= lastTS {
				continue
			}
			lastTS = signal["timestamp"].(int64)
			seenLocal[tid] = struct{}{}
			if sink != nil && sink.SeenTrade(tid) {
				continue
			}
			market := fmt.Sprint(signal["market"])
			if slug := resolver.Get(market); slug != "" {
				signal["slug"] = slug
			}

			signal["market"] = getMarketId(signal["slug"].(string))

			if sink != nil {
				sink.Push(signal)
			}
			newCount++
		}

		if newCount == 0 {
			interval = time.Duration(float64(interval) * 1.5)
			if interval > 30*time.Second {
				interval = 30 * time.Second
			}
		} else {
			interval = 5 * time.Second
		}
		time.Sleep(interval)
	}
}

func main() {
	var wg sync.WaitGroup
	for name, wallet := range WALLETS {
		wg.Add(1)
		go monitor(name, wallet, 1*time.Second, true, &wg)
	}
	wg.Wait()
}
