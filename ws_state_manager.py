#!/usr/bin/env python3
"""
WS State Manager - REST polling version (v3).

Replaces Binance WebSocket with direct REST API calls using urllib (stdlib).
No dependency on python-binance for market data or requests.
Still uses Binance client for trading operations.
"""
import argparse
import json
import sqlite3
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from binance.client import Client

ROOT = Path(__file__).resolve().parent
STATE_DB = ROOT / "trades.db"
AUDIT_LOG = ROOT / "ws_state_audit.jsonl"
ALERT_LOG = ROOT / "pending_alerts.jsonl"
CFG_PATH = ROOT / "ws_strategy_config.json"

POLL_INTERVAL_NORMAL = 60
POLL_INTERVAL_NEAR_TP = 10
BINANCE_FUTURES_BASE = "https://fapi.binance.com"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_keys():
    env = Path("/home/azureuser/.benv").read_text()
    key = None
    sec = None
    for line in env.splitlines():
        if line.startswith("BINANCE_API_KEY="):
            key = line.split("=", 1)[1].strip()
        if line.startswith("BINANCE_API_SECRET="):
            sec = line.split("=", 1)[1].strip()
    if not key or not sec:
        raise RuntimeError("Missing Binance keys in /home/azureuser/.benv")
    return key, sec


def fetch_mark_price_rest(symbol: str) -> float:
    """Fetch mark price via Binance Futures REST API using urllib (no auth needed)."""
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex?symbol={symbol}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    return float(data["markPrice"])


def audit(symbol: str, stage_from: str, stage_to: str, action: str, request: dict, response: dict, success: bool):
    row = {
        "ts": now_iso(),
        "symbol": symbol,
        "stage_from": stage_from,
        "stage_to": stage_to,
        "action": action,
        "request": request,
        "response": response,
        "success": success,
    }
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def alert(text: str):
    row = {"ts": now_iso(), "msg": text}
    with ALERT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


@dataclass
class SymbolCfg:
    symbol: str
    enabled: bool
    trail_callback_rate: float = 1.5


class WSStateManager:
    """
    State manager using REST polling instead of WebSocket.
    Class name kept for compatibility. No WebSocket connection.
    Uses urllib for mark price, Binance client for trading.
    """

    def __init__(self, client: Client, cfg: Dict, simulate: bool = False):
        self.client = client
        self.cfg = cfg
        self.simulate = simulate
        self.latest_mark: Dict[str, float] = {}
        self._init_db()

    def _init_db(self):
        con = sqlite3.connect(STATE_DB)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ws_strategy_state (
              symbol TEXT PRIMARY KEY,
              stage TEXT NOT NULL DEFAULT 'INIT',
              be_moved INTEGER DEFAULT 0,
              tp1_seen INTEGER DEFAULT 0,
              tp2_seen INTEGER DEFAULT 0,
              trailing_active INTEGER DEFAULT 0,
              last_action_key TEXT,
              updated_at TEXT
            )
            """
        )
        con.commit()
        con.close()

    def _get_state(self, symbol: str):
        con = sqlite3.connect(STATE_DB)
        row = con.execute("SELECT stage,be_moved,tp1_seen,tp2_seen,trailing_active,last_action_key FROM ws_strategy_state WHERE symbol=?", (symbol,)).fetchone()
        if not row:
            con.execute("INSERT INTO ws_strategy_state(symbol,stage,updated_at) VALUES(?,?,?)", (symbol, "INIT", now_iso()))
            con.commit()
            row = ("INIT", 0, 0, 0, 0, None)
        con.close()
        return {
            "stage": row[0],
            "be_moved": int(row[1]),
            "tp1_seen": int(row[2]),
            "tp2_seen": int(row[3]),
            "trailing_active": int(row[4]),
            "last_action_key": row[5],
        }

    def _save_state(self, symbol: str, st: dict):
        con = sqlite3.connect(STATE_DB)
        con.execute(
            "UPDATE ws_strategy_state SET stage=?,be_moved=?,tp1_seen=?,tp2_seen=?,trailing_active=?,last_action_key=?,updated_at=? WHERE symbol=?",
            (st["stage"], st["be_moved"], st["tp1_seen"], st["tp2_seen"], st["trailing_active"], st.get("last_action_key"), now_iso(), symbol),
        )
        con.commit()
        con.close()

    def _idempotent(self, st: dict, symbol: str, action: str):
        key = f"{symbol}:{st['stage']}:{action}"
        if st.get("last_action_key") == key:
            return False
        st["last_action_key"] = key
        return True

    def _fetch_mark_price(self, symbol: str) -> float:
        """Fetch mark price via REST."""
        try:
            price = fetch_mark_price_rest(symbol)
            self.latest_mark[symbol] = price
            return price
        except Exception as e:
            alert(f"REST mark_price fetch failed {symbol}: {e}")
            if symbol in self.latest_mark:
                return self.latest_mark[symbol]
            raise

    def _position(self, symbol: str):
        positions = self.client.futures_position_information(symbol=symbol)
        p = next((x for x in positions if x.get("positionSide") == "LONG" and float(x.get("positionAmt", 0)) > 0), None)
        return p

    def _open_algo(self, symbol: str):
        return self.client.futures_get_open_algo_orders(symbol=symbol)

    def _cancel_algo(self, symbol: str, algo_id: int):
        return self.client.futures_cancel_algo_order(symbol=symbol, algoId=algo_id)

    def _place_stop_market(self, symbol: str, qty: float, trigger: float):
        return self.client.futures_create_algo_order(
            algoType="CONDITIONAL",
            symbol=symbol,
            side="SELL",
            positionSide="LONG",
            type="STOP_MARKET",
            quantity=f"{qty:.3f}",
            triggerPrice=f"{trigger:.2f}",
            workingType="MARK_PRICE",
            priceProtect="TRUE",
            newOrderRespType="ACK",
        )

    def _place_trailing(self, symbol: str, qty: float, callback_rate: float):
        return self.client.futures_create_algo_order(
            algoType="CONDITIONAL",
            symbol=symbol,
            side="SELL",
            positionSide="LONG",
            type="TRAILING_STOP_MARKET",
            quantity=f"{qty:.3f}",
            callbackRate=str(callback_rate),
            workingType="MARK_PRICE",
            newOrderRespType="ACK",
        )

    def _latest_price(self, symbol: str):
        if symbol in self.latest_mark:
            return self.latest_mark[symbol]
        return self._fetch_mark_price(symbol)

    def _check_symbol(self, scfg: dict):
        symbol = scfg["symbol"]
        callback = float(scfg.get("trail_callback_rate", 1.5))
        st = self._get_state(symbol)
        pos = self._position(symbol)
        if not pos:
            if st["stage"] != "DONE":
                prev = st["stage"]
                st.update({"stage": "DONE"})
                self._save_state(symbol, st)
                audit(symbol, prev, "DONE", "position_closed", {}, {"msg": "no long position"}, True)
            return

        entry = float(pos["entryPrice"])
        qty = abs(float(pos["positionAmt"]))
        tp1_pct = float(scfg.get("tp1_trigger_pct", 3.0))
        tp2_pct = float(scfg.get("tp2_trigger_pct", 4.0))
        tp1_qty_threshold = qty <= round((0.079 - 0.026) + 1e-6, 3)
        tp2_qty_threshold = qty <= round((0.079 - 0.052) + 1e-6, 3)

        try:
            self._fetch_mark_price(symbol)
        except Exception:
            pass

        if not st["tp1_seen"] and tp1_qty_threshold:
            prev = st["stage"]
            st["tp1_seen"] = 1
            st["stage"] = "TP1_OBSERVED"
            self._save_state(symbol, st)
            audit(symbol, prev, st["stage"], "detect_tp1_qty_drop", {"qty": qty}, {"ok": True}, True)

            if (not st["be_moved"]) and self._idempotent(st, symbol, "move_be_on_tp1"):
                req = {"entry": entry, "trigger": f"tp1@{tp1_pct}%", "qty": qty}
                try:
                    open_algo = self._open_algo(symbol)
                    cancelled = []
                    for a in open_algo:
                        if a.get("side") == "SELL" and a.get("positionSide") == "LONG":
                            tp = float(a.get("triggerPrice", "0") or 0)
                            if tp > 0 and tp < entry:
                                self._cancel_algo(symbol, int(a["algoId"]))
                                cancelled.append(a["algoId"])
                    placed = self._place_stop_market(symbol, qty, entry)
                    st["be_moved"] = 1
                    st["stage"] = "TP1_BE_MOVED"
                    self._save_state(symbol, st)
                    audit(symbol, "TP1_OBSERVED", st["stage"], "move_sl_to_breakeven", req, {"cancelled": cancelled, "placed": placed}, True)
                except Exception as e:
                    audit(symbol, "TP1_OBSERVED", "TP1_OBSERVED", "move_sl_to_breakeven", req, {"error": str(e)}, False)
                    alert(f"WS_STATE move SL on TP1 failed {symbol}: {e}")

        if st["tp1_seen"] and (not st["tp2_seen"]) and tp2_qty_threshold:
            prev = st["stage"]
            st["tp2_seen"] = 1
            st["stage"] = "TP2_OBSERVED"
            self._save_state(symbol, st)
            audit(symbol, prev, st["stage"], "detect_tp2_qty_drop", {"qty": qty}, {"ok": True}, True)

        if st["tp2_seen"] and (not st["trailing_active"]):
            if self._idempotent(st, symbol, "place_trailing"):
                prev = st["stage"]
                req = {"qty": qty, "callbackRate": callback}
                try:
                    res = self._place_trailing(symbol, qty, callback)
                    st["trailing_active"] = 1
                    st["stage"] = "TRAILING_ACTIVE"
                    self._save_state(symbol, st)
                    audit(symbol, prev, st["stage"], "place_trailing", req, {"response": res}, True)
                except Exception as e:
                    audit(symbol, prev, prev, "place_trailing", req, {"error": str(e)}, False)
                    alert(f"WS_STATE place trailing failed {symbol}: {e}")

    def run(self):
        if self.simulate:
            self.simulate_run()
            return

        alert("State Manager starting in REST polling mode (WebSocket removed)")

        backoffs = [1, 2, 5, 10]
        i = 0
        while True:
            try:
                i = 0
                while True:
                    for scfg in self.cfg["symbols"]:
                        if scfg.get("enabled", True):
                            self._check_symbol(scfg)

                    sleep_s = POLL_INTERVAL_NORMAL
                    for scfg in self.cfg["symbols"]:
                        if not scfg.get("enabled", True):
                            continue
                        symbol = scfg["symbol"]
                        pos = self._position(symbol)
                        if not pos:
                            continue
                        entry = float(pos["entryPrice"])
                        p = self._latest_price(symbol)
                        tp1 = entry * (1 + float(scfg.get("tp1_trigger_pct", 3.0)) / 100)
                        tp2 = entry * (1 + float(scfg.get("tp2_trigger_pct", 4.0)) / 100)
                        if abs(p - tp1) / tp1 < 0.003 or abs(p - tp2) / tp2 < 0.003:
                            sleep_s = POLL_INTERVAL_NEAR_TP
                            break
                    time.sleep(sleep_s)
            except Exception as e:
                wait = backoffs[min(i, len(backoffs) - 1)]
                i += 1
                alert(f"REST poll loop error: {e}; retry in {wait}s")
                time.sleep(wait)

    def simulate_run(self):
        symbol = self.cfg["symbols"][0]["symbol"]
        stages = [
            ("INIT", "INIT", "boot", True),
            ("INIT", "TP1_OBSERVED", "detect_tp1_qty_drop", True),
            ("TP1_OBSERVED", "TP1_BE_MOVED", "move_sl_to_breakeven", True),
            ("TP1_BE_MOVED", "TP2_OBSERVED", "detect_tp2_qty_drop", True),
            ("TP2_OBSERVED", "TRAILING_ACTIVE", "place_trailing", True),
        ]
        for sf, st, action, ok in stages:
            audit(symbol, sf, st, action, {"simulate": True}, {"simulate": True}, ok)
        print("simulation_complete")


def default_cfg():
    return {
        "symbols": [
            {"symbol": "ETHUSDT", "enabled": True, "tp1_trigger_pct": 3.0, "tp2_trigger_pct": 4.0, "trail_callback_rate": 1.5}
        ]
    }


def main():
    ap = argparse.ArgumentParser(description="AKQ State Manager (REST polling mode)")
    ap.add_argument("--simulate", action="store_true", help="write one simulated state-transition flow")
    args = ap.parse_args()

    if not CFG_PATH.exists():
        CFG_PATH.write_text(json.dumps(default_cfg(), indent=2), encoding="utf-8")

    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    key, sec = load_keys()
    client = Client(key, sec)

    mgr = WSStateManager(client, cfg, simulate=args.simulate)
    mgr.run()


if __name__ == "__main__":
    main()
