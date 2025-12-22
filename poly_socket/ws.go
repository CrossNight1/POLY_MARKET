package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

/* ============================
   CONFIG
============================ */

const (
	GAMMA_API_URL   = "https://gamma-api.polymarket.com"
	WSS_URL         = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	HTTP_TIMEOUT    = 20 * time.Second
	WS_PING_SECONDS = 10 * time.Second
	MAX_RECONNECTS  = 10
)

/* ============================
   REDIS
============================ */

func NewRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
}

/*
	============================
	  MARKET STRUCTS

============================
*/
type Market struct {
	MarketID        string
	ConditionID     string
	Slug            string
	Question        string
	TokenIDs        []string
	Outcomes        []string
	TokenOutcomeMap map[string]string
	EndDate         *time.Time
}

type MarketResponse struct {
	ID           string `json:"id"`
	ConditionID  string `json:"conditionId"`
	Question     string `json:"question"`
	Slug         string `json:"slug"`
	ClobTokenIds string `json:"clobTokenIds"` // JSON string
	Outcomes     string `json:"outcomes"`     // JSON string
	EndDateIso   string `json:"endDateIso"`
}

type OrderBook map[float64]float64

/* ============================
   GAMMA API
============================ */

func fetchMarket(slug string) (*Market, error) {
	url := fmt.Sprintf("%s/markets/slug/%s", GAMMA_API_URL, slug)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if !json.Valid(body) {
		return nil, fmt.Errorf("response not valid JSON")
	}

	var raw MarketResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	// Parse JSON strings to actual slices
	var tokenIDs []string
	if err := json.Unmarshal([]byte(raw.ClobTokenIds), &tokenIDs); err != nil {
		return nil, fmt.Errorf("failed parsing token IDs: %w", err)
	}

	var outcomes []string
	if err := json.Unmarshal([]byte(raw.Outcomes), &outcomes); err != nil {
		return nil, fmt.Errorf("failed parsing outcomes: %w", err)
	}

	if len(tokenIDs) == 0 {
		return nil, fmt.Errorf("no tokens")
	}

	var endDate *time.Time
	if raw.EndDateIso != "" {
		if t, err := time.Parse(time.RFC3339, raw.EndDateIso+"T00:00:00Z"); err == nil {
			endDate = &t
		}
	}

	tokenOutcome := make(map[string]string)
	for i, tid := range tokenIDs {
		if i < len(outcomes) {
			tokenOutcome[tid] = outcomes[i]
		}
	}

	return &Market{
		MarketID:        raw.ID,
		ConditionID:     raw.ConditionID,
		Slug:            raw.Slug,
		Question:        raw.Question,
		TokenIDs:        tokenIDs,
		Outcomes:        outcomes,
		TokenOutcomeMap: tokenOutcome,
		EndDate:         endDate,
	}, nil
}

func fetch15mMarkets(
	assets []string,
	slot time.Time,
	nextMarket bool,
) ([]*Market, time.Time) {

	targetSlot := slot
	if nextMarket {
		targetSlot = slot.Add(15 * time.Minute)
	}

	ts := targetSlot.Unix()

	var markets []*Market
	for _, a := range assets {
		slug := fmt.Sprintf("%s-updown-15m-%d", a, ts)
		m, err := fetchMarket(slug)
		if err == nil && m != nil {
			log.Println("[OK]", slug)
			markets = append(markets, m)
		} else {
			log.Println("[WAIT]", slug)
		}
	}

	return markets, targetSlot
}

/* ============================
   ORDERBOOK
============================ */

func parseOrderBook(raw any) OrderBook {
	book := make(OrderBook)
	arr, ok := raw.([]any)
	if !ok {
		return book
	}

	for _, v := range arr {
		x, ok := v.(map[string]any)
		if !ok {
			continue
		}
		price := toFloat(x["price"])
		size := toFloat(x["size"])

		if price > 0 && size > 0 {
			book[price] = size
		}
	}
	return book
}

/* ============================
   WS CLIENT
============================ */

type PolymarketWS struct {
	assets      []string
	currentSlot time.Time
	redis       *redis.Client

	marketMap  map[string]*Market
	tokenMap   map[string]string
	assetIDs   []string
	tickers    map[string]map[string]any
	conn       *websocket.Conn
	running    bool
	reconnects int
}

func NewWS(assets []string, slot time.Time, r *redis.Client) *PolymarketWS {
	return &PolymarketWS{
		assets:      assets,
		currentSlot: slot.UTC(),
		redis:       r,
		marketMap:   make(map[string]*Market),
		tokenMap:    make(map[string]string),
		tickers:     make(map[string]map[string]any),
	}
}

func (p *PolymarketWS) updateMarkets(markets []*Market) {
	p.marketMap = make(map[string]*Market)
	p.tokenMap = make(map[string]string)
	p.assetIDs = []string{}

	for _, m := range markets {
		for _, tid := range m.TokenIDs {
			p.marketMap[tid] = m
			p.tokenMap[tid] = m.TokenOutcomeMap[tid]
			p.assetIDs = append(p.assetIDs, tid)
		}
	}
	log.Println("[STATE] tracking", len(p.assetIDs), "tokens")
}

func (p *PolymarketWS) handleMsg(msg []byte) {
	if string(msg) == "PONG" {
		return
	}

	strMsg := string(msg)

	// Handle array messages (multiple book updates)
	if strings.HasPrefix(strMsg, "[") {
		var arr []map[string]any
		if err := json.Unmarshal(msg, &arr); err != nil {
			fmt.Println("unmarshal array error:", err)
			return
		}
		for _, d := range arr {
			p.processTickerUpdate(d)
		}
		return
	}

	// Handle single object messages
	var d map[string]any
	if err := json.Unmarshal(msg, &d); err != nil {
		fmt.Println("unmarshal object error:", err)
		return
	}

	p.processTickerUpdate(d)
}

// processTickerUpdate handles a single update, updating existing ticker values
func (p *PolymarketWS) processTickerUpdate(d map[string]any) {
	eventType := str(d["event_type"])

	if eventType != "book" && eventType != "price_change" {
		return
	}

	// Collect per-asset update objects
	type pcObj struct {
		tid string
		obj map[string]any
	}

	var updates []pcObj

	switch eventType {

	case "book":
		tid := str(d["asset_id"])
		if tid == "" {
			return
		}
		updates = append(updates, pcObj{tid: tid, obj: d})

	case "price_change":
		raw, ok := d["price_changes"].([]any)
		if !ok {
			return
		}

		for _, v := range raw {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}

			tid := str(m["asset_id"])
			if tid == "" {
				continue
			}

			updates = append(updates, pcObj{tid: tid, obj: m})
		}
	}

	// Apply updates
	for _, u := range updates {
		market, ok := p.marketMap[u.tid]
		if !ok {
			continue
		}

		t := p.tickers[u.tid]
		if t == nil {
			t = make(map[string]any)
			p.tickers[u.tid] = t
		}

		var bestBid, bidSz, bestAsk, askSz float64

		switch eventType {

		case "price_change":
			if v := toFloat(u.obj["best_bid"]); v != 0 {
				bestBid = v
			} else if prev, ok := t["bestBid"].(float64); ok {
				bestBid = prev
			}

			if v := toFloat(u.obj["bid_size"]); v != 0 {
				bidSz = v
			} else if prev, ok := t["bidSz"].(float64); ok {
				bidSz = prev
			}

			if v := toFloat(u.obj["best_ask"]); v != 0 {
				bestAsk = v
			} else if prev, ok := t["bestAsk"].(float64); ok {
				bestAsk = prev
			}

			if v := toFloat(u.obj["ask_size"]); v != 0 {
				askSz = v
			} else if prev, ok := t["askSz"].(float64); ok {
				askSz = prev
			}

		case "book":
			bids := parseOrderBook(u.obj["bids"])
			asks := parseOrderBook(u.obj["asks"])
			bestBid, bidSz = maxBook(bids)
			bestAsk, askSz = minBook(asks)
		}

		// Persist ticker
		t["bestBid"] = bestBid
		t["bidSz"] = bidSz
		t["bestAsk"] = bestAsk
		t["askSz"] = askSz
		t["token_id"] = u.tid
		t["slug"] = market.Slug
		t["ts"] = int64(toFloat(d["timestamp"]))
		t["ts_sv"] = time.Now().UnixMilli()

		asset := strings.ToUpper(strings.Split(market.Slug, "-")[0])
		outcome := strings.ToLower(p.tokenMap[u.tid])
		key := fmt.Sprintf("%s_%s_15m_polymarket_ticker", asset, outcome)

		b, _ := json.Marshal(t)
		p.redis.Set(context.Background(), key, b, 0)
	}
}

func (p *PolymarketWS) connect() {
	for p.running && p.reconnects < MAX_RECONNECTS {
		c, _, err := websocket.DefaultDialer.Dial(WSS_URL, nil)
		if err != nil {
			p.reconnects++
			time.Sleep(time.Duration(math.Pow(2, float64(p.reconnects))) * time.Second)
			continue
		}

		p.conn = c
		p.reconnects = 0

		sub, _ := json.Marshal(map[string]any{
			"type":       "market",
			"assets_ids": p.assetIDs,
		})
		c.WriteMessage(websocket.TextMessage, sub)

		go p.ping()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				break
			}
			p.handleMsg(msg)
		}
	}
}

func (p *PolymarketWS) ping() {
	for p.running {
		p.conn.WriteMessage(websocket.TextMessage, []byte("PING"))
		time.Sleep(WS_PING_SECONDS)
	}
}

func (p *PolymarketWS) Start(markets []*Market) {
	p.updateMarkets(markets)
	p.running = true
	p.connect()
}

/*
	============================
	  HELPERS

============================
*/
func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func toFloat(v any) float64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0
		}
		return f
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}

func maxBook(b OrderBook) (float64, float64) {
	var p, s float64
	for k, v := range b {
		if k > p {
			p, s = k, v
		}
	}
	return p, s
}

func minBook(a OrderBook) (float64, float64) {
	p := math.MaxFloat64
	var s float64
	for k, v := range a {
		if k < p {
			p, s = k, v
		}
	}
	if p == math.MaxFloat64 {
		return 0, 0
	}
	return p, s
}
