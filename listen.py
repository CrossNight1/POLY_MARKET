import json
import time
from typing import Any, Dict, List, Optional, Set
from collections import deque

import requests
import redis


DATA_API_URL = "https://data-api.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "poly-wallet-monitor/1.0"})


def get_trades(wallet: str) -> List[Dict[str, Any]]:
    """Fetch trades for a wallet."""
    try:
        r = SESSION.get(
            f"{DATA_API_URL}/trades",
            params={"user": wallet.lower()},
            timeout=10,
        )
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []


def trade_id(trade: Dict[str, Any]) -> str:
    """Return unique trade identifier."""
    return (
        str(trade.get("transactionHash"))
        or str(trade.get("transaction_hash"))
        or str(trade.get("id"))
        or str(trade.get("hash"))
        or json.dumps(trade, sort_keys=True)
    )


def normalize_ts(ts: Any) -> int:
    """Normalize timestamp to ms."""
    if isinstance(ts, (int, float)):
        return int(ts * 1000) if ts < 10_000_000_000 else int(ts)
    return int(time.time() * 1000)


def trade_to_signal(wallet: str, trade: Dict[str, Any]) -> Dict[str, Any]:
    """Convert trade to signal."""
    return {
        "wallet": wallet,
        "trade_id": trade_id(trade),
        "market": trade.get("market")
        or trade.get("marketId")
        or trade.get("conditionId"),
        "outcome": trade.get("outcome"),
        "side": (trade.get("side") or "").upper(),
        "price": float(trade.get("price") or trade.get("avgPrice") or 0) or None,
        "size": float(trade.get("size") or trade.get("amount") or 0) or None,
        "timestamp": normalize_ts(
            trade.get("timestamp")
            or trade.get("time")
            or trade.get("createdAt")
            or trade.get("created_at")
        ),
        "raw": trade,
    }


class MarketSlugResolver:
    """Resolve and cache market slugs."""

    def __init__(self) -> None:
        self.cache: Dict[str, Optional[str]] = {}

    def get(self, market_id: Optional[str]) -> Optional[str]:
        if not market_id:
            return None
        if market_id in self.cache:
            return self.cache[market_id]
        slug = self._fetch(market_id)
        self.cache[market_id] = slug
        return slug

    def _fetch(self, market_id: str) -> Optional[str]:
        try:
            r = SESSION.get(f"{GAMMA_API_URL}/markets/{market_id}", timeout=10)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict):
                    return data.get("slug")
                if isinstance(data, list) and data:
                    return data[0].get("slug")
        except Exception:
            pass
        return None


class MessageSink:
    """Message signal sink."""

    def __init__(self, wallet: str) -> None:
        self.wallet = wallet
        self.client = redis.Redis(decode_responses=True)  # type: ignore
        self.signals = f"poly:{wallet}:signals"
        self.seen = f"poly:{wallet}:seen"
        self.positions = f"poly:{wallet}:positions"

    def push(self, signal: Dict[str, Any]) -> None:
        pipe = self.client.pipeline()
        pipe.sadd(self.seen, signal["trade_id"])
        pipe.rpush(self.signals, json.dumps(signal, separators=(",", ":")))
        self._update_position(pipe, signal)
        pipe.execute()

    def seen_trade(self, tid: str) -> bool:
        return bool(self.client.sismember(self.seen, tid))

    def _update_position(self, pipe, signal: Dict[str, Any]) -> None:
        m, o = signal.get("market"), signal.get("outcome")
        s, p, side = signal.get("size"), signal.get("price"), signal.get("side")
        if not m or s is None or p is None or side not in ("BUY", "SELL"):
            return
        key = f"{m}:{o}"
        raw = self.client.hget(self.positions, key)
        net = float(json.loads(raw).get("net_size", 0)) if raw else 0.0
        net += s if side == "BUY" else -s
        if abs(net) < 1e-9:
            pipe.hdel(self.positions, key)
            return
        pipe.hset(
            self.positions,
            key,
            json.dumps(
                {
                    "wallet": self.wallet,
                    "market": m,
                    "outcome": o,
                    "slug": signal.get("slug"),
                    "net_size": net,
                    "direction": "LONG" if net > 0 else "SHORT",
                    "updated_at": signal["timestamp"],
                },
                separators=(",", ":"),
            ),
        )


def monitor(name: str, wallet: str, interval: float = 5.0, use_redis: bool = False) -> None:
    """Monitor wallet trades."""
    resolver = MarketSlugResolver()
    sink = MessageSink(wallet) if use_redis else None
    seen_local: Set[str] = set()
    last_ts = time.time() * 1000 - 60_000 * 60 * 1
    seen_fifo = deque(maxlen=5000)

    while True:
        trades = get_trades(wallet)
        trades.reverse()
        new = 0

        for t in trades:
            tid = trade_id(t)
            if tid in seen_local:
                continue
            
            signal = trade_to_signal(wallet, t)
            if signal["timestamp"] <= last_ts:
                continue
            
            last_ts = max(last_ts, signal["timestamp"])
            seen_local.add(tid)
            seen_fifo.append(tid)
            if len(seen_local) > seen_fifo.maxlen:
                seen_local.remove(seen_fifo.popleft())

            if sink and sink.seen_trade(tid):
                continue

            slug = resolver.get(signal.get("market"))
            if slug:
                signal["slug"] = slug

            payload = json.dumps(signal, ensure_ascii=False)

            if sink:
                sink.push(signal)

            new += 1

        interval = 5.0 if new else min(30.0, interval * 1.5)
        time.sleep(interval)

WALLETS = {
    "Andromeda": "0x39932ca2b7a1b8ab6cbf0b8f7419261b950ccded",
    "hopedieslast": "0x5739ddf8672627ce076eff5f444610a250075f1a",
    "distinct-baguette": "0xe00740bce98a594e26861838885ab310ec3b548c"
}

from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=len(WALLETS)) as pool:
        for name, wallet in WALLETS.items():
            pool.submit(monitor, name, wallet, 1.0, True)
