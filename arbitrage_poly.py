import redis
import json
import time
import threading
from datetime import datetime
from logger import setup_logger

r = redis.Redis(host='localhost', port=6379, db=0)

class PolyArbitrage:
    def __init__(self, client, symbol, params, redis):
        self.client = client
        self.symbol = symbol
        self.edge = params["edge"]
        self.sleep = params.get("sleep_time", 0.01)
        self.logger = setup_logger(f"arbitrage_{symbol}", f'./logger/{symbol}_poly_arbitrage.log')
        self.redis = redis

        self.key_up = f"{symbol}_up_15m_polymarket_ticker"
        self.key_down = f"{symbol}_down_15m_polymarket_ticker"

        self.up_id = None
        self.down_id = None

        self.last_trade_ts = 0

    def _read(self, key):
        raw = self.redis.get(key)
        if not raw:
            return None
        t = json.loads(raw)
        if time.time() * 1000 - t["ts"] > 500:
            return None
        return t

    def _place_leg(self, market, side, size, price, result, key):
        try:
            # resp = self.client.place_order(
            #     market=market,
            #     order_side=side,
            #     quantity=size,
            #     order_type="LIMIT",
            #     price=price
            # )
            resp = {"code": 0}
            result[key] = resp
        except Exception as e:
            result[key] = e

    def _execute(self, mkt_1, mkt_2, side, size, buy_px, sell_px, edge):
        ts = time.strftime("%H:%M:%S")
        self.logger.info(
            f"[{ts}] {self.symbol} ARB {side} edge={edge:.4f} size={size}"
        )

        result = {}

        t_buy = threading.Thread(
            target=self._place_leg,
            args=(mkt_1, side.upper(), size, buy_px, result, side.lower()),
            daemon=True
        )

        t_sell = threading.Thread(
            target=self._place_leg,
            args=(mkt_2, side.upper(), size, sell_px, result, side.lower()),
            daemon=True
        )

        t_buy.start()
        t_sell.start()

        t_buy.join(timeout=2)
        t_sell.join(timeout=2)

        # -------- logging & sanity --------
        buy_ok = isinstance(result.get("buy"), dict) and result["buy"].get("code") == 0
        sell_ok = isinstance(result.get("sell"), dict) and result["sell"].get("code") == 0

        if buy_ok and sell_ok:
            self.logger.info(f"{self.symbol} BOTH LEGS OK")
        elif buy_ok and not sell_ok:
            self.logger.error(f"{self.symbol} BUY OK / SELL FAIL → HEDGE REQUIRED")
        elif sell_ok and not buy_ok:
            self.logger.error(f"{self.symbol} SELL OK / BUY FAIL → HEDGE REQUIRED")
        else:
            self.logger.error(f"{self.symbol} BOTH LEGS FAILED")

    def check_run_time(self):
        minute = datetime.now().minute

        # 14–16, 29–31, 44–46, 59–01
        if minute % 15 in (14, 15, 0):
            return False

        return True        

    def monitor(self):
        self.logger.info(f"Start Running POLY ARBITRAGE {self.symbol}")
        
        while True:
            try:
                if not self.check_run_time:
                    time.sleep(10)
                    continue
                
                up = self._read(self.key_up)
                down = self._read(self.key_down)
                
                if not up or not down:
                    time.sleep(self.sleep)
                    continue

                self.up_id = up["token_id"]
                self.down_id = down["token_id"]

                # ---------------- ARB BUY ----------------
                miss_b = 1.0 - (up["bestAsk"] + down["bestAsk"])
                if miss_b > self.edge:
                    size = min(up["askSz"], down["askSz"])
                    if size > 0:
                        self._execute(
                            "UP", "DOWN",
                            "BUY",
                            size,
                            up["bestAsk"],
                            down["bestAsk"],
                            miss_b
                        )
                        continue  # prevent double fire same tick

                # ---------------- ARB SELL ----------------
                miss_s = (up["bestBid"] + down["bestBid"]) - 1
                if miss_s > self.edge:
                    size = min(down["bidSz"], up["bidSz"])
                    if size > 0:
                        self._execute(
                            "UP", "DOWN",
                            "SELL",
                            size,
                            up["bestBid"],
                            down["bestBid"],
                            miss_s
                        )

                time.sleep(self.sleep)

            except Exception as e:
                self.logger.error(f"{self.symbol} error: {e}")
                time.sleep(1)




TICKERS = ["ETH", "BTC", "SOL", "XRP"]
threads = []

for symbol in TICKERS:
    arb = PolyArbitrage(
        client="client",                      # real client, not string
        symbol=symbol,
        params={"edge": 0.002, "sleep_time": 0.05},
        redis=r
    )

    th = threading.Thread(
        target=arb.monitor,   
        daemon=True
    )
    th.start()
    threads.append(th)

while True:
    time.sleep(10)