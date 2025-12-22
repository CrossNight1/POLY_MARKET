import redis
import json
import time
import threading
from logger import logger_arb

r = redis.Redis(host='localhost', port=6379, db=0)

def monitor_ticker(ticker: str):
    key_up = f"{ticker}_up_15m_polymarket_ticker"
    key_down = f"{ticker}_down_15m_polymarket_ticker"

    EDGE = 0.002        # fees + slippage buffer
    SLEEP = 0.1         # loop delay
    STALE_UP = 1000     # ms
    STALE_DOWN = 1000   # ms

    u_bb = u_bsz = u_ba = u_asz = None
    d_bb = d_bsz = d_ba = d_asz = None

    i = 0
    while True:
        try:
            i += 1
            miss_u = None
            miss_d = None

            now_ms = time.time() * 1000

            up_raw = r.get(key_up)
            down_raw = r.get(key_down)

            if up_raw:
                t = json.loads(up_raw)
                u_ts = t.get("ts")
                if u_ts is None or now_ms - u_ts > STALE_UP:
                    time.sleep(SLEEP)
                    continue

                u_bb  = t.get("bestBid", u_bb)
                u_bsz = t.get("bidSz", u_bsz)
                u_ba  = t.get("bestAsk", u_ba)
                u_asz = t.get("askSz", u_asz)

            if down_raw:
                t = json.loads(down_raw)
                d_ts = t.get("ts")
                if d_ts is None or now_ms - d_ts > STALE_DOWN:
                    time.sleep(SLEEP)
                    continue

                d_bb  = t.get("bestBid", d_bb)
                d_bsz = t.get("bidSz", d_bsz)
                d_ba  = t.get("bestAsk", d_ba)
                d_asz = t.get("askSz", d_asz)

            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            # DOWN ask vs UP bid
            if u_bb is not None and d_ba is not None:
                miss_u = 1 - (u_bb + d_ba)
                if miss_u > EDGE:
                    size = min(u_bsz or 0, d_asz or 0)
                    if size > 0:
                        logger_arb.info(f"[{ts}] {ticker} ARB UP | edge={miss_u:.4f} | BUY {size}")

            # UP ask vs DOWN bid
            if u_ba is not None and d_bb is not None:
                miss_d = 1 - (u_ba + d_bb)
                if miss_d > EDGE:
                    size = min(d_bsz or 0, u_asz or 0)
                    if size > 0:
                        logger_arb.info(f"[{ts}] {ticker} ARB DOWN | edge={miss_d:.4f} | BUY {size}")
            
            if i >= 50: 
                logger_arb.debug(f"[{ts}] {ticker} | miss_u={miss_u:.4f} - {u_ba:.4f} vs {d_bb:.4f} | miss_d={miss_d:.4f} - {u_bb:.4f} vs {d_ba:.4f}")
                i = 0
            time.sleep(SLEEP)

        except Exception as e:
            logger_arb.error(f"[{ticker}] error: {e}")
            time.sleep(5)

threads = []
TICKERS = ["ETH"]

for t in TICKERS:
    th = threading.Thread(target=monitor_ticker, args=(t,), daemon=True)
    th.start()
    threads.append(th)

while True:
    time.sleep(10)
