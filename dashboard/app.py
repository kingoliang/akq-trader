"""
AKQ Trading API v2.1 — Unified Flask service (port 5001)
- Dashboard (public)
- REST API with Bearer token auth
- POST /api/trade/buy — open long (sl mandatory, tp optional)
- POST /api/trade/sell — open short (sl mandatory, tp optional)
- POST /api/trade/close — close positions
- Watchdog — background condition-based alerts + Discord push
- Percentage-based position limits
- Audit logging
"""
import os
import re
import json
import math
import sqlite3
import logging
import threading
import time as _time
import hmac
import hashlib
import requests as http_requests
import numpy as np
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlencode
from flask import Flask, jsonify, request

DB_PATH = "/home/azureuser/akq-trader/trades.db"
ENV_PATH = "/home/azureuser/.benv"
API_TOKEN_PATH = "/home/azureuser/akq-trader/.api_token"
DASHBOARD_TOKEN_PATH = "/home/azureuser/akq-trader/.dashboard_token"
AUDIT_LOG_PATH = "/home/azureuser/akq-trader/audit.log"
WATCHDOG_CONFIG_PATH = "/home/azureuser/akq-trader/watchdog_config.json"

# ── Logging ───────────────────────────────────────────────
audit_logger = logging.getLogger("audit")
audit_logger.setLevel(logging.INFO)
_fh = logging.FileHandler(AUDIT_LOG_PATH)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"))
audit_logger.addHandler(_fh)

watchdog_logger = logging.getLogger("watchdog")
watchdog_logger.setLevel(logging.INFO)
_wh = logging.FileHandler("/home/azureuser/akq-trader/watchdog.log")
_wh.setFormatter(logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"))
watchdog_logger.addHandler(_wh)

# ── Binance client ────────────────────────────────────────
def load_env(path=ENV_PATH):
    env = open(path).read()
    k = re.search(r'BINANCE_API_KEY=(\S+)', env).group(1)
    s = re.search(r'BINANCE_API_SECRET=(\S+)', env).group(1)
    return k, s

from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, FUTURE_ORDER_TYPE_MARKET
KEY, SECRET = load_env()
client = Client(KEY, SECRET)

# ── API Token ─────────────────────────────────────────────
def load_api_token():
    try:
        return open(API_TOKEN_PATH).read().strip()
    except FileNotFoundError:
        return None


def load_dashboard_token():
    try:
        return open(DASHBOARD_TOKEN_PATH).read().strip()
    except FileNotFoundError:
        return None

API_TOKEN = load_api_token()
DASHBOARD_TOKEN = load_dashboard_token()

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════
# POSITION LIMITS (percentage-based)
# ═══════════════════════════════════════════════════════════
LIMITS = {
    "steady_first_pct": 40,     # 稳健仓第一仓 ≤ 权益×40%
    "steady_add_pct": 27,       # 稳健仓加仓 ≤ 权益×27%
    "aggressive_pct": 32,       # 激进仓 ≤ 权益×32%
    "daily_loss_pct": 10,       # 日亏损熔断 ≤ 权益×10%
}

def get_equity():
    """Get current total equity (wallet + unrealized PnL)"""
    balances = client.futures_account_balance()
    usdt = next((b for b in balances if b["asset"] == "USDT"), None)
    account = client.futures_account()
    balance = float(usdt["balance"]) if usdt else 0
    unrealized = float(account.get("totalUnrealizedProfit", 0))
    return balance + unrealized

def get_daily_realized_loss():
    """Calculate today's realized losses from DB"""
    conn = get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT COALESCE(SUM(pnl_usdt), 0) as total_loss FROM trades WHERE status='CLOSED' AND close_time >= ? AND pnl_usdt < 0",
        (today,)
    ).fetchone()
    conn.close()
    return abs(rows["total_loss"]) if rows else 0

def check_daily_loss_limit():
    """Returns (ok, msg). If daily loss exceeds limit, returns (False, reason)."""
    equity = get_equity()
    max_loss = equity * LIMITS["daily_loss_pct"] / 100
    daily_loss = get_daily_realized_loss()
    if daily_loss >= max_loss:
        return False, f"日亏损熔断：已亏 ${daily_loss:.2f}，超过限额 ${max_loss:.2f}（权益 ${equity:.2f} × {LIMITS['daily_loss_pct']}%）"
    return True, ""


# ── Auth middleware ────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not API_TOKEN:
            return jsonify({"ok": False, "error": "API token not configured on server"}), 500
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth[7:] != API_TOKEN:
            audit_logger.info("AUTH_FAIL | %s %s | ip=%s", request.method, request.path, request.remote_addr)
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def require_dashboard_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 允许本机调用（dashboard 页面与本地探针）
        if request.remote_addr in {"127.0.0.1", "::1", "localhost"}:
            return f(*args, **kwargs)

        token = request.headers.get("X-Dashboard-Token") or request.args.get("token")
        if DASHBOARD_TOKEN and token == DASHBOARD_TOKEN:
            return f(*args, **kwargs)

        audit_logger.info("DASHBOARD_AUTH_FAIL | %s %s | ip=%s", request.method, request.path, request.remote_addr)
        return jsonify({"ok": False, "error": "Dashboard Unauthorized"}), 401

    return decorated

# ── helpers ───────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def safe_leverage(position):
    lev_raw = position.get("leverage")
    try:
        if lev_raw not in (None, "", "?"):
            return int(float(lev_raw))
    except Exception:
        pass
    try:
        notional = abs(float(position.get("notional", 0) or 0))
        margin = float(position.get("positionInitialMargin") or position.get("initialMargin") or 0)
        if notional > 0 and margin > 0:
            return max(1, round(notional / margin))
    except Exception:
        pass
    return 1

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        open_time TEXT, close_time TEXT, symbol TEXT, side TEXT DEFAULT 'LONG',
        qty REAL, entry_price REAL, exit_price REAL, leverage INTEGER,
        sl_price REAL, tp_price REAL, status TEXT DEFAULT 'OPEN',
        margin_usdt REAL, pnl_usdt REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS equity_curve (
        id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, equity REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS idempotency_keys (
        task_id TEXT PRIMARY KEY,
        created_at TEXT,
        result_json TEXT
    )""")
    conn.commit()
    conn.close()

init_db()

# ═══════════════════════════════════════════════════════════
# PUBLIC ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.route("/api/health")
def api_health():
    return jsonify({
        "ok": True,
        "service": "akq-trading-api",
        "version": "2.1",
        "watchdog": watchdog_running,
        "time": datetime.now(timezone.utc).isoformat(),
    })

# ═══════════════════════════════════════════════════════════
# AUTHENTICATED API — READ
# ═══════════════════════════════════════════════════════════
@app.route("/api/balance")
@require_auth
def api_balance():
    try:
        balances = client.futures_account_balance()
        usdt = next((b for b in balances if b["asset"] == "USDT"), None)
        account = client.futures_account()
        balance = float(usdt["balance"]) if usdt else 0
        unrealized = float(account.get("totalUnrealizedProfit", 0))
        equity = balance + unrealized
        result = {
            "available": float(usdt["availableBalance"]) if usdt else 0,
            "balance": balance,
            "totalEquity": round(equity, 4),
            "unrealizedPnl": round(unrealized, 4),
            "limits": {
                "steadyFirst": round(equity * LIMITS["steady_first_pct"] / 100, 2),
                "steadyAdd": round(equity * LIMITS["steady_add_pct"] / 100, 2),
                "aggressive": round(equity * LIMITS["aggressive_pct"] / 100, 2),
                "dailyLossMax": round(equity * LIMITS["daily_loss_pct"] / 100, 2),
                "dailyLossUsed": round(get_daily_realized_loss(), 2),
            },
        }
        audit_logger.info("GET /api/balance | ip=%s | equity=%.4f", request.remote_addr, result["totalEquity"])
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        audit_logger.info("GET /api/balance | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/positions")
@require_auth
def api_positions():
    try:
        positions = client.futures_position_information()
        active = []
        for p in positions:
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            entry = float(p["entryPrice"])
            mark = float(p["markPrice"])
            pnl = float(p["unRealizedProfit"])
            pnl_pct = ((mark - entry) / entry * 100) if entry else 0
            if amt < 0:
                pnl_pct = -pnl_pct
            leverage = safe_leverage(p)
            notional_entry = abs(entry * amt)
            active.append({
                "symbol": p["symbol"],
                "entryPrice": entry,
                "qty": amt,
                "markPrice": mark,
                "cost": round(notional_entry / leverage, 4) if leverage else round(notional_entry, 4),
                "value": round(abs(mark * amt), 4),
                "pnlUsdt": round(pnl, 4),
                "pnlPct": round(pnl_pct, 2),
                "leverage": leverage,
                "positionSide": p.get("positionSide", "BOTH"),
            })
        audit_logger.info("GET /api/positions | ip=%s | count=%d", request.remote_addr, len(active))
        return jsonify({"ok": True, "data": active})
    except Exception as e:
        audit_logger.info("GET /api/positions | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/orders")
@require_auth
def api_orders():
    try:
        orders = client.futures_get_open_orders()
        result = []
        for o in orders:
            result.append({
                "orderId": o["orderId"],
                "symbol": o["symbol"],
                "type": o["type"],
                "side": o["side"],
                "stopPrice": o.get("stopPrice"),
                "price": o.get("price"),
                "origQty": o.get("origQty"),
                "status": o.get("status"),
            })
        audit_logger.info("GET /api/orders | ip=%s | count=%d", request.remote_addr, len(result))
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        audit_logger.info("GET /api/orders | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/trades")
@require_auth
def api_trades():
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM trades ORDER BY open_time DESC, id DESC LIMIT 100").fetchall()
        conn.close()
        audit_logger.info("GET /api/trades | ip=%s | count=%d", request.remote_addr, len(rows))
        return jsonify({"ok": True, "data": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/equity")
@require_auth
def api_equity():
    try:
        conn = get_db()
        rows = conn.execute("SELECT time, equity FROM equity_curve ORDER BY id ASC").fetchall()
        conn.close()
        return jsonify({"ok": True, "data": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/limits")
@require_auth
def api_limits():
    """Current position limits based on equity"""
    try:
        equity = get_equity()
        return jsonify({"ok": True, "data": {
            "equity": round(equity, 2),
            "steadyFirst": round(equity * LIMITS["steady_first_pct"] / 100, 2),
            "steadyAdd": round(equity * LIMITS["steady_add_pct"] / 100, 2),
            "aggressive": round(equity * LIMITS["aggressive_pct"] / 100, 2),
            "dailyLossMax": round(equity * LIMITS["daily_loss_pct"] / 100, 2),
            "dailyLossUsed": round(get_daily_realized_loss(), 2),
        }})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ═══════════════════════════════════════════════════════════
# AUTHENTICATED API — TRADE EXECUTION
# ═══════════════════════════════════════════════════════════
@app.route("/api/trade/close", methods=["POST"])
@require_auth
def api_trade_close():
    """Close a position. Body: {"symbol": "ETHUSDT"} or {"symbol": "ETHUSDT", "qty": 0.05}"""
    try:
        body = request.get_json(force=True) or {}
        symbol = body.get("symbol", "").upper()
        partial_qty = body.get("qty")  # optional: partial close

        if not symbol:
            return jsonify({"ok": False, "error": "symbol is required"}), 400

        # Daily loss check
        ok, msg = check_daily_loss_limit()
        # Note: we allow closing even if loss limit hit (closing reduces exposure)
        # But we log a warning
        if not ok:
            audit_logger.info("WARN | close despite loss limit | %s | %s", symbol, msg)

        # Cancel open orders for this symbol
        try:
            client.futures_cancel_all_open_orders(symbol=symbol)
        except Exception:
            pass

        # Find active position (LONG or SHORT)
        positions = client.futures_position_information(symbol=symbol)

        long_pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
        short_pos = next((p for p in positions if p.get("positionSide") == "SHORT" and float(p["positionAmt"]) < 0), None)

        # Fallback for one-way mode
        one_way_pos = None
        if not long_pos and not short_pos:
            one_way_pos = next((p for p in positions if float(p["positionAmt"]) != 0), None)

        pos = long_pos or short_pos or one_way_pos
        if not pos:
            audit_logger.info("POST /api/trade/close | ip=%s | %s | no position", request.remote_addr, symbol)
            return jsonify({"ok": False, "error": f"No open position for {symbol}"}), 404

        position_amt = float(pos["positionAmt"])
        position_qty = abs(position_amt)
        entry_price = float(pos["entryPrice"])

        # Determine direction + close side
        if pos.get("positionSide") == "SHORT" or position_amt < 0:
            direction = "SHORT"
            close_side = SIDE_BUY
        else:
            direction = "LONG"
            close_side = SIDE_SELL

        # Determine close quantity
        close_qty = position_qty
        if partial_qty is not None:
            partial_qty = float(partial_qty)
            if partial_qty <= 0 or partial_qty > position_qty:
                return jsonify({"ok": False, "error": f"Invalid qty: {partial_qty} (position has {position_qty})"}), 400
            close_qty = partial_qty

        # Execute market close
        order_params = {
            "symbol": symbol,
            "side": close_side,
            "type": FUTURE_ORDER_TYPE_MARKET,
            "quantity": close_qty,
        }
        if pos.get("positionSide") in ("LONG", "SHORT"):
            order_params["positionSide"] = pos["positionSide"]
        else:
            order_params["reduceOnly"] = True

        order = client.futures_create_order(**order_params)

        # Get actual exit price from trade fills
        exit_price = None
        try:
            trades = client.futures_account_trades(symbol=symbol, limit=10)
            trade = next((t for t in reversed(trades) if str(t.get("orderId")) == str(order.get("orderId"))), None)
            if trade:
                exit_price = float(trade["price"])
        except Exception:
            pass
        if exit_price is None:
            mark_data = client.futures_mark_price(symbol=symbol)
            exit_price = float(order.get("avgPrice") or mark_data["markPrice"])

        if direction == "SHORT":
            pnl = (entry_price - exit_price) * close_qty
        else:
            pnl = (exit_price - entry_price) * close_qty

        # Update DB
        try:
            conn = sqlite3.connect(DB_PATH)
            if close_qty >= position_qty:
                # Full close
                conn.execute(
                    "UPDATE trades SET close_time=?, exit_price=?, pnl_usdt=?, status='CLOSED' WHERE symbol=? AND side=? AND status='OPEN' ORDER BY id DESC LIMIT 1",
                    (datetime.now(timezone.utc).isoformat(), exit_price, pnl, symbol, direction)
                )
            # Record equity
            balances = client.futures_account_balance()
            usdt_bal = next((b for b in balances if b["asset"] == "USDT"), None)
            if usdt_bal:
                conn.execute("INSERT INTO equity_curve (time, equity) VALUES (?,?)",
                             (datetime.now(timezone.utc).isoformat(), float(usdt_bal["balance"])))
            conn.commit()
            conn.close()
        except Exception as e:
            audit_logger.info("DB_ERROR | trade/close | %s", str(e))

        result = {
            "symbol": symbol,
            "direction": direction,
            "qty": close_qty,
            "entryPrice": entry_price,
            "exitPrice": exit_price,
            "pnlUsdt": round(pnl, 4),
            "orderId": order["orderId"],
            "partial": close_qty < position_qty,
        }

        audit_logger.info(
            "POST /api/trade/close | ip=%s | %s | dir=%s | qty=%.4f | exit=%.2f | pnl=%.4f",
            request.remote_addr, symbol, direction, close_qty, exit_price, pnl
        )
        return jsonify({"ok": True, "data": result})

    except Exception as e:
        audit_logger.info("POST /api/trade/close | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Trading helpers ────────────────────────────────────────
def get_symbol_info(symbol):
    """Get contract precision: stepSize, tickSize"""
    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            lot = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
            price = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
            return {
                "stepSize": float(lot["stepSize"]),
                "tickSize": float(price["tickSize"]),
            }
    raise ValueError(f"Symbol not found: {symbol}")

def round_step(value, step):
    precision = max(0, round(-math.log10(step)))
    return round(round(value / step) * step, precision)

def _open_position(symbol, usdt_amount, leverage, sl_pct, tp_pct, side, task_id=None):
    """
    Core: open LONG or SHORT position with mandatory SL and optional TP.
    side: 'LONG' or 'SHORT'
    task_id: optional idempotency key — if provided and already executed, returns cached result.
    Returns result dict or raises.
    """
    # Idempotency check
    if task_id:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT result_json FROM idempotency_keys WHERE task_id=?", (task_id,)).fetchone()
        conn.close()
        if row:
            return {"idempotent_hit": True, **json.loads(row[0])}

    info = get_symbol_info(symbol)

    # Check daily loss limit
    ok, msg = check_daily_loss_limit()
    if not ok:
        raise ValueError(msg)

    # Check position limit (use steady_first as default; caller can override)
    equity = get_equity()
    max_margin = equity * LIMITS["steady_first_pct"] / 100
    if usdt_amount > max_margin:
        raise ValueError(
            f"超限：{usdt_amount}U > 权益 ${equity:.2f} × {LIMITS['steady_first_pct']}% = ${max_margin:.2f}"
        )

    # Set leverage
    client.futures_change_leverage(symbol=symbol, leverage=leverage)

    # Calculate qty
    mark_data = client.futures_mark_price(symbol=symbol)
    mark = float(mark_data["markPrice"])
    notional = usdt_amount * leverage
    qty = round_step(notional / mark, info["stepSize"])
    if qty <= 0:
        raise ValueError(f"数量过小: notional={notional}, mark={mark}")

    # Market order
    buy_side = "BUY" if side == "LONG" else "SELL"
    order = client.futures_create_order(
        symbol=symbol,
        side=buy_side,
        positionSide=side,
        type="MARKET",
        quantity=qty,
    )

    # Get actual entry price
    entry_price = None
    try:
        trades = client.futures_account_trades(symbol=symbol, limit=10)
        trade = next((t for t in reversed(trades) if str(t.get("orderId")) == str(order.get("orderId"))), None)
        if trade:
            entry_price = float(trade["price"])
    except Exception:
        pass
    if entry_price is None:
        entry_price = float(order.get("avgPrice") or mark)

    # SL direction: LONG止损卖出在下方, SHORT止损买入在上方
    if side == "LONG":
        sl_price = round_step(entry_price * (1 - sl_pct / 100), info["tickSize"])
        sl_side = "SELL"
    else:
        sl_price = round_step(entry_price * (1 + sl_pct / 100), info["tickSize"])
        sl_side = "BUY"

    sl_order = client.futures_create_order(
        symbol=symbol,
        side=sl_side,
        positionSide=side,
        type="STOP_MARKET",
        quantity=qty,
        stopPrice=sl_price,
        timeInForce="GTE_GTC",
    )

    # TP (optional)
    tp_price = None
    tp_order_id = None
    if tp_pct is not None:
        if side == "LONG":
            tp_price = round_step(entry_price * (1 + tp_pct / 100), info["tickSize"])
            tp_side = "SELL"
        else:
            tp_price = round_step(entry_price * (1 - tp_pct / 100), info["tickSize"])
            tp_side = "BUY"

        tp_order = client.futures_create_order(
            symbol=symbol,
            side=tp_side,
            positionSide=side,
            type="TAKE_PROFIT_MARKET",
            quantity=qty,
            stopPrice=tp_price,
            timeInForce="GTE_GTC",
        )
        tp_order_id = tp_order.get("orderId")

    # Write to DB
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO trades (open_time, symbol, side, qty, entry_price, leverage, sl_price, tp_price, status, margin_usdt) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), symbol, side, qty, entry_price, leverage, sl_price, tp_price, "OPEN", usdt_amount)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        audit_logger.info("DB_ERROR | open_%s | %s", side.lower(), str(e))

    result = {
        "orderId": order["orderId"],
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entryPrice": entry_price,
        "slPrice": sl_price,
        "tpPrice": tp_price,
        "leverage": leverage,
        "usdtMargin": usdt_amount,
        "slOrderId": sl_order.get("orderId"),
        "tpOrderId": tp_order_id,
    }

    # Save idempotency key
    if task_id:
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys (task_id, created_at, result_json) VALUES (?,?,?)",
                (task_id, datetime.now(timezone.utc).isoformat(), json.dumps(result))
            )
            conn.commit()
            conn.close()
        except Exception as e:
            audit_logger.info("IDEMPOTENCY_SAVE_ERROR | %s | %s", task_id, str(e))

    return result


@app.route("/api/trade/buy", methods=["POST"])
@require_auth
def api_trade_buy():
    """Open LONG. Body: {"symbol":"ETHUSDT","usdt_amount":60,"leverage":3,"sl_pct":2.0,"tp_pct":4.0,"task_id":"IC-561-ETH-BUY"}"""
    try:
        body = request.get_json(force=True) or {}
        symbol = body.get("symbol", "").upper()
        usdt_amount = body.get("usdt_amount")
        leverage = body.get("leverage")
        sl_pct = body.get("sl_pct")
        tp_pct = body.get("tp_pct")  # optional
        task_id = body.get("task_id")  # optional idempotency key

        # Validate required fields
        errors = []
        if not symbol:
            errors.append("symbol is required")
        if usdt_amount is None:
            errors.append("usdt_amount is required")
        if leverage is None:
            errors.append("leverage is required")
        if sl_pct is None:
            errors.append("sl_pct is required (裸仓不允许)")
        if errors:
            return jsonify({"ok": False, "error": "; ".join(errors)}), 400

        usdt_amount = float(usdt_amount)
        leverage = int(leverage)
        sl_pct = float(sl_pct)
        if tp_pct is not None:
            tp_pct = float(tp_pct)

        if usdt_amount <= 0 or leverage < 1 or sl_pct <= 0:
            return jsonify({"ok": False, "error": "Invalid params: amount/leverage/sl must be positive"}), 400

        result = _open_position(symbol, usdt_amount, leverage, sl_pct, tp_pct, "LONG", task_id=task_id)

        if result.get("idempotent_hit"):
            audit_logger.info("POST /api/trade/buy | ip=%s | IDEMPOTENT_HIT task_id=%s", request.remote_addr, task_id)
            return jsonify({"ok": True, "data": result, "idempotent": True})

        audit_logger.info(
            "POST /api/trade/buy | ip=%s | %s LONG | %.2fU %dx | entry=%.2f sl=%.2f tp=%s | task_id=%s",
            request.remote_addr, symbol, usdt_amount, leverage,
            result["entryPrice"], result["slPrice"],
            f'{result["tpPrice"]:.2f}' if result["tpPrice"] else "none",
            task_id or "none"
        )
        return jsonify({"ok": True, "data": result})

    except ValueError as e:
        audit_logger.info("POST /api/trade/buy | ip=%s | REJECTED: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        audit_logger.info("POST /api/trade/buy | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/trade/sell", methods=["POST"])
@require_auth
def api_trade_sell():
    """Open SHORT. Body: {"symbol":"ETHUSDT","usdt_amount":60,"leverage":3,"sl_pct":2.0,"tp_pct":4.0,"task_id":"IC-561-ETH-SELL"}"""
    try:
        body = request.get_json(force=True) or {}
        symbol = body.get("symbol", "").upper()
        usdt_amount = body.get("usdt_amount")
        leverage = body.get("leverage")
        sl_pct = body.get("sl_pct")
        tp_pct = body.get("tp_pct")  # optional
        task_id = body.get("task_id")  # optional idempotency key

        # Validate required fields
        errors = []
        if not symbol:
            errors.append("symbol is required")
        if usdt_amount is None:
            errors.append("usdt_amount is required")
        if leverage is None:
            errors.append("leverage is required")
        if sl_pct is None:
            errors.append("sl_pct is required (裸仓不允许)")
        if errors:
            return jsonify({"ok": False, "error": "; ".join(errors)}), 400

        usdt_amount = float(usdt_amount)
        leverage = int(leverage)
        sl_pct = float(sl_pct)
        if tp_pct is not None:
            tp_pct = float(tp_pct)

        if usdt_amount <= 0 or leverage < 1 or sl_pct <= 0:
            return jsonify({"ok": False, "error": "Invalid params: amount/leverage/sl must be positive"}), 400

        result = _open_position(symbol, usdt_amount, leverage, sl_pct, tp_pct, "SHORT", task_id=task_id)

        if result.get("idempotent_hit"):
            audit_logger.info("POST /api/trade/sell | ip=%s | IDEMPOTENT_HIT task_id=%s", request.remote_addr, task_id)
            return jsonify({"ok": True, "data": result, "idempotent": True})

        audit_logger.info(
            "POST /api/trade/sell | ip=%s | %s SHORT | %.2fU %dx | entry=%.2f sl=%.2f tp=%s | task_id=%s",
            request.remote_addr, symbol, usdt_amount, leverage,
            result["entryPrice"], result["slPrice"],
            f'{result["tpPrice"]:.2f}' if result["tpPrice"] else "none",
            task_id or "none"
        )
        return jsonify({"ok": True, "data": result})

    except ValueError as e:
        audit_logger.info("POST /api/trade/sell | ip=%s | REJECTED: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        audit_logger.info("POST /api/trade/sell | ip=%s | ERROR: %s", request.remote_addr, str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════
# WATCHDOG — Background condition-based alerts
# ═══════════════════════════════════════════════════════════
watchdog_running = False
_alert_cooldowns = {}  # key -> last alert timestamp (prevent spam)
ALERT_COOLDOWN_SECS = 3600  # 1 hour cooldown per condition

DEFAULT_WATCHDOG_CONFIG = {
    "enabled": True,
    "interval_seconds": 300,
    "symbols": ["ETHUSDT", "SOLUSDT", "BNBUSDT"],
    "alerts": {
        "rsi_low": 30,
        "rsi_high": 75,
        "pnl_gain_pct": 1.0,
        "pnl_loss_pct": 1.0,
        "price_near_sl_pct": 3.0,
    }
}

def load_watchdog_config():
    try:
        with open(WATCHDOG_CONFIG_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        # Write default config
        with open(WATCHDOG_CONFIG_PATH, "w") as f:
            json.dump(DEFAULT_WATCHDOG_CONFIG, f, indent=2)
        return DEFAULT_WATCHDOG_CONFIG

def save_watchdog_config(config):
    with open(WATCHDOG_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

def should_alert(key):
    """Check if we should fire an alert (respects cooldown)"""
    now = _time.time()
    last = _alert_cooldowns.get(key, 0)
    if now - last < ALERT_COOLDOWN_SECS:
        return False
    _alert_cooldowns[key] = now
    return True

def calc_rsi(closes, period=14):
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    ag = np.mean(gains[-period:])
    al = np.mean(losses[-period:])
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))

def calc_bb(closes, period=20):
    mid = np.mean(closes[-period:])
    std = np.std(closes[-period:])
    upper = mid + 2 * std
    lower = mid - 2 * std
    return upper, mid, lower

def calc_ma(closes, period):
    return np.mean(closes[-period:])

OPENCLAW_GATEWAY_URL = "http://127.0.0.1:18789/tools/invoke"
OPENCLAW_GATEWAY_TOKEN = os.environ.get("OPENCLAW_GATEWAY_TOKEN", "")
DISCORD_OPC_CHANNEL = "1474475240502464542"

def send_discord_alert(message):
    """Send alert to Discord #opc via OpenClaw Gateway API, fallback to file"""
    watchdog_logger.info("ALERT: %s", message)

    token = OPENCLAW_GATEWAY_TOKEN
    if not token:
        # Try reading from env file
        try:
            for line in open("/home/azureuser/.benv"):
                if line.startswith("OPENCLAW_GATEWAY_TOKEN="):
                    token = line.split("=", 1)[1].strip()
        except Exception:
            pass

    if token:
        try:
            payload = {
                "tool": "message",
                "args": {
                    "action": "send",
                    "channel": "discord",
                    "target": DISCORD_OPC_CHANNEL,
                    "message": f"🐕 **Watchdog Alert**\n{message}"
                }
            }
            resp = http_requests.post(
                OPENCLAW_GATEWAY_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                timeout=15
            )
            result = resp.json()
            if result.get("ok"):
                watchdog_logger.info("Discord alert sent via OpenClaw Gateway")
                return
            else:
                watchdog_logger.warning("Gateway send failed: %s", resp.text[:200])
        except Exception as e:
            watchdog_logger.warning("Gateway send error: %s", e)

    # Fallback: write to file
    alert_file = "/home/azureuser/akq-trader/pending_alerts.jsonl"
    alert = {
        "time": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }
    with open(alert_file, "a") as f:
        f.write(json.dumps(alert) + "\n")
    watchdog_logger.info("Alert written to file (fallback)")

def watchdog_check():
    """Single watchdog check cycle"""
    config = load_watchdog_config()
    if not config.get("enabled", True):
        return

    alerts_cfg = config.get("alerts", {})

    # Check positions first
    try:
        positions = client.futures_position_information()
        for p in positions:
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            symbol = p["symbol"]
            entry = float(p["entryPrice"])
            mark = float(p["markPrice"])
            pnl_pct = ((mark - entry) / entry * 100) if entry else 0
            if amt < 0:
                pnl_pct = -pnl_pct

            # Check PnL thresholds
            gain_thresh = alerts_cfg.get("pnl_gain_pct", 1.0)
            loss_thresh = alerts_cfg.get("pnl_loss_pct", 1.0)

            if pnl_pct >= gain_thresh and should_alert(f"pnl_gain_{symbol}"):
                send_discord_alert(f"📈 {symbol} 浮盈 +{pnl_pct:.2f}% (mark=${mark:.2f}, entry=${entry:.2f})")
            elif pnl_pct <= -loss_thresh and should_alert(f"pnl_loss_{symbol}"):
                send_discord_alert(f"📉 {symbol} 浮亏 {pnl_pct:.2f}% (mark=${mark:.2f}, entry=${entry:.2f})")

            # Check price near stop-loss
            # Try to find SL from DB
            conn = get_db()
            row = conn.execute("SELECT sl_price FROM trades WHERE symbol=? AND status='OPEN' ORDER BY id DESC LIMIT 1", (symbol,)).fetchone()
            conn.close()
            if row and row["sl_price"]:
                sl = row["sl_price"]
                dist_pct = ((mark - sl) / mark * 100) if mark > 0 else 999
                near_sl = alerts_cfg.get("price_near_sl_pct", 3.0)
                if 0 < dist_pct < near_sl and should_alert(f"near_sl_{symbol}"):
                    send_discord_alert(f"⚠️ {symbol} 距止损 ${sl:.2f} 仅 {dist_pct:.1f}% (mark=${mark:.2f})")

    except Exception as e:
        watchdog_logger.info("ERROR checking positions: %s", str(e))

    # Check market indicators for watched symbols
    for symbol in config.get("symbols", []):
        try:
            klines = client.futures_klines(symbol=symbol, interval="1h", limit=30)
            closes = np.array([float(k[4]) for k in klines])
            price = closes[-1]

            rsi = calc_rsi(closes)
            ma7 = calc_ma(closes, 7)
            ma25 = calc_ma(closes, min(25, len(closes)))
            bb_upper, bb_mid, bb_lower = calc_bb(closes)

            rsi_high = alerts_cfg.get("rsi_high", 75)
            rsi_low = alerts_cfg.get("rsi_low", 30)

            if rsi >= rsi_high and should_alert(f"rsi_high_{symbol}"):
                send_discord_alert(f"🔴 {symbol} RSI={rsi:.1f} 超买 (>{rsi_high}) | ${price:.2f}")
            elif rsi <= rsi_low and should_alert(f"rsi_low_{symbol}"):
                send_discord_alert(f"🟢 {symbol} RSI={rsi:.1f} 超卖 (<{rsi_low}) | ${price:.2f}")

            # Price touching BB bands
            if price >= bb_upper * 0.998 and should_alert(f"bb_upper_{symbol}"):
                send_discord_alert(f"🔵 {symbol} 触及 BB 上轨 ${bb_upper:.2f} | 当前 ${price:.2f} | RSI={rsi:.1f}")
            elif price <= bb_lower * 1.002 and should_alert(f"bb_lower_{symbol}"):
                send_discord_alert(f"🔵 {symbol} 触及 BB 下轨 ${bb_lower:.2f} | 当前 ${price:.2f} | RSI={rsi:.1f}")

            # Price crossing MA7/MA25
            prev_price = closes[-2] if len(closes) >= 2 else price
            if prev_price < ma7 <= price and should_alert(f"ma7_cross_up_{symbol}"):
                send_discord_alert(f"📊 {symbol} 站上 MA7 ${ma7:.2f} | ${price:.2f}")
            elif prev_price > ma7 >= price and should_alert(f"ma7_cross_down_{symbol}"):
                send_discord_alert(f"📊 {symbol} 跌破 MA7 ${ma7:.2f} | ${price:.2f}")

        except Exception as e:
            watchdog_logger.info("ERROR checking %s: %s", symbol, str(e))

def watchdog_loop():
    """Background watchdog thread"""
    global watchdog_running
    watchdog_running = True
    watchdog_logger.info("Watchdog started")
    while watchdog_running:
        try:
            watchdog_check()
        except Exception as e:
            watchdog_logger.info("Watchdog cycle error: %s", str(e))
        config = load_watchdog_config()
        interval = config.get("interval_seconds", 300)
        _time.sleep(interval)

# Watchdog API endpoints
@app.route("/api/watchdog/status")
@require_auth
def api_watchdog_status():
    return jsonify({
        "ok": True,
        "data": {
            "running": watchdog_running,
            "config": load_watchdog_config(),
            "cooldowns": {k: datetime.fromtimestamp(v, tz=timezone.utc).isoformat() for k, v in _alert_cooldowns.items()},
        }
    })

@app.route("/api/watchdog/config", methods=["GET", "PUT"])
@require_auth
def api_watchdog_config():
    if request.method == "GET":
        return jsonify({"ok": True, "data": load_watchdog_config()})
    else:
        body = request.get_json(force=True) or {}
        config = load_watchdog_config()
        config.update(body)
        save_watchdog_config(config)
        audit_logger.info("PUT /api/watchdog/config | ip=%s | %s", request.remote_addr, json.dumps(body))
        return jsonify({"ok": True, "data": config})

@app.route("/api/watchdog/alerts")
@require_auth
def api_watchdog_alerts():
    """Read pending alerts"""
    alert_file = "/home/azureuser/akq-trader/pending_alerts.jsonl"
    alerts = []
    try:
        with open(alert_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    alerts.append(json.loads(line))
    except FileNotFoundError:
        pass
    return jsonify({"ok": True, "data": alerts})


# ═══════════════════════════════════════════════════════════
# DASHBOARD (public, no auth)
# ═══════════════════════════════════════════════════════════
@app.route("/dashboard/api/balance")
@require_dashboard_auth
def dashboard_balance():
    try:
        balances = client.futures_account_balance()
        usdt = next((b for b in balances if b["asset"] == "USDT"), None)
        account = client.futures_account()
        balance = float(usdt["balance"]) if usdt else 0
        unrealized = float(account.get("totalUnrealizedProfit", 0))
        return jsonify({
            "available": float(usdt["availableBalance"]) if usdt else 0,
            "balance": balance,
            "totalEquity": balance + unrealized,
            "unrealizedPnl": unrealized,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/api/positions")
@require_dashboard_auth
def dashboard_positions():
    try:
        positions = client.futures_position_information()
        active = []
        for p in positions:
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            entry = float(p["entryPrice"])
            mark = float(p["markPrice"])
            pnl = float(p["unRealizedProfit"])
            pnl_pct = ((mark - entry) / entry * 100) if entry else 0
            if amt < 0:
                pnl_pct = -pnl_pct
            leverage = safe_leverage(p)
            notional_entry = abs(entry * amt)
            active.append({
                "symbol": p["symbol"],
                "entryPrice": entry,
                "qty": amt,
                "markPrice": mark,
                "cost": round(notional_entry / leverage, 4) if leverage else round(notional_entry, 4),
                "value": round(abs(mark * amt), 4),
                "pnlUsdt": round(pnl, 4),
                "pnlPct": round(pnl_pct, 2),
                "leverage": leverage,
            })
        return jsonify(active)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/api/trades")
@require_dashboard_auth
def dashboard_trades():
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM trades ORDER BY open_time DESC, id DESC LIMIT 100").fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/api/equity_curve")
def dashboard_equity():
    try:
        conn = get_db()
        rows = conn.execute("SELECT time, equity FROM equity_curve ORDER BY id ASC").fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def index():
    return HTML_PAGE

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AKQ Futures Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:'Segoe UI',system-ui,sans-serif;padding:20px}
h1{color:#58a6ff;margin-bottom:18px;font-size:1.5rem}
h2{color:#8b949e;font-size:1.1rem;margin-bottom:10px;border-bottom:1px solid #21262d;padding-bottom:6px}
.header-meta{font-size:.75rem;color:#6e7681;white-space:nowrap;position:fixed;top:10px;right:16px;z-index:1000;background:rgba(13,17,23,.85);padding:2px 6px;border-radius:4px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:16px}
table{width:100%;border-collapse:collapse;font-size:.85rem}
th{text-align:left;padding:8px 6px;color:#8b949e;border-bottom:1px solid #21262d}
td{padding:7px 6px;border-bottom:1px solid #161b22}
.pos{color:#3fb950}.neg{color:#f85149}
.stat-row{display:flex;gap:24px;flex-wrap:wrap}
.stat-item{text-align:center}
.stat-item .val{font-size:1.6rem;font-weight:700}
.stat-item .lbl{font-size:.75rem;color:#8b949e}
#chart-box{height:260px}
.loader{color:#484f58;font-style:italic}
</style>
</head>
<body>
<h1>AKQ Futures Dashboard v2</h1>
<div id="last-refresh" class="header-meta">Last refresh: Loading...</div>

<div class="grid">
  <div class="card">
    <h2>Account Balance</h2>
    <div class="stat-row" id="bal"><span class="loader">Loading...</span></div>
  </div>
  <div class="card">
    <h2>Statistics</h2>
    <div class="stat-row" id="stats"><span class="loader">Loading...</span></div>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <h2>Open Positions</h2>
  <table>
    <thead><tr><th>Symbol</th><th>Entry</th><th>Qty</th><th>Mark</th><th>PnL (USDT)</th><th>PnL %</th><th>Leverage</th><th>Cost</th><th>Value</th></tr></thead>
    <tbody id="pos"><tr><td colspan="9" class="loader">Loading...</td></tr></tbody>
  </table>
</div>

<div class="card" style="margin-bottom:16px">
  <h2>Equity Curve</h2>
  <div id="chart-box"><canvas id="eqChart"></canvas></div>
</div>

<div class="card">
  <h2>Trade History</h2>
  <table>
    <thead><tr><th>Open Time</th><th>Close Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Entry</th><th>Exit</th><th>PnL (USDT)</th><th>Status</th></tr></thead>
    <tbody id="trades"><tr><td colspan="9" class="loader">Loading...</td></tr></tbody>
  </table>
</div>

<script>
const $=s=>document.getElementById(s);
const fmt=(v,d=2)=>v!=null?Number(v).toFixed(d):'-';
const cls=v=>v>=0?'pos':'neg';
const INITIAL_CAPITAL_USDT = 147.20; // Kingo 入金本金，可按需调整
const DASH_TOKEN = new URLSearchParams(window.location.search).get('token');

function updateLastRefresh(){
  const ts=new Date().toLocaleString('en-GB',{year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
  $('last-refresh').textContent=`Last refresh: ${ts}`;
}

async function fetchJSON(url){
  try{
    let finalUrl=url;
    if(DASH_TOKEN){
      finalUrl += (url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(DASH_TOKEN);
    }
    const r=await fetch(finalUrl);
    return await r.json();
  }catch(e){return{error:e.message};}
}

async function loadBalance(){
  const d=await fetchJSON('/dashboard/api/balance');
  if(d.error){$('bal').innerHTML='<span class="neg">Error</span>';return;}
  $('bal').innerHTML=`
    <div class="stat-item"><div class="val">${fmt(d.balance)}</div><div class="lbl">Total USDT</div></div>
    <div class="stat-item"><div class="val">${fmt(d.available)}</div><div class="lbl">Available</div></div>
    <div class="stat-item"><div class="val ${cls(d.unrealizedPnl)}">${fmt(d.unrealizedPnl)}</div><div class="lbl">Unrealized PnL</div></div>
    <div class="stat-item"><div class="val">${fmt(d.totalEquity)}</div><div class="lbl">Total Equity</div></div>`;
}

async function loadPositions(){
  const d=await fetchJSON('/dashboard/api/positions');
  if(d.error){$('pos').innerHTML='<tr><td colspan="9" class="neg">Error</td></tr>';return;}
  if(!d.length){$('pos').innerHTML='<tr><td colspan="9" style="color:#484f58">No open positions</td></tr>';return;}
  $('pos').innerHTML=d.map(p=>`<tr>
    <td>${p.symbol}</td><td>${fmt(p.entryPrice,4)}</td><td>${fmt(p.qty,4)}</td>
    <td>${fmt(p.markPrice,4)}</td>
    <td class="${cls(p.pnlUsdt)}">${fmt(p.pnlUsdt)}</td>
    <td class="${cls(p.pnlPct)}">${fmt(p.pnlPct)}%</td>
    <td>${p.leverage}x</td>
    <td>${fmt(p.cost)}</td>
    <td>${fmt(p.value)}</td></tr>`).join('');
}

async function loadTrades(){
  const d=await fetchJSON('/dashboard/api/trades');
  if(d.error){$('stats').innerHTML='<span class="neg">Error</span>';$('trades').innerHTML='<tr><td colspan="9" class="neg">Error</td></tr>';return;}
  const closed=d.filter(t=>t.status==='CLOSED');
  const totalPnl=closed.reduce((s,t)=>s+(t.pnl_usdt||0),0);
  const roiPct=INITIAL_CAPITAL_USDT>0?(totalPnl/INITIAL_CAPITAL_USDT*100):0;
  const roiText=`${totalPnl>=0?'+':''}${fmt(roiPct,2)}%`;
  const wins=closed.filter(t=>(t.pnl_usdt||0)>0).length;
  const winRate=closed.length?(wins/closed.length*100):0;
  $('stats').innerHTML=`
    <div class="stat-item"><div class="val">${d.length}</div><div class="lbl">Total Trades</div></div>
    <div class="stat-item"><div class="val ${cls(totalPnl)}">${fmt(totalPnl)} (${roiText})</div><div class="lbl">Total PnL / ROI</div></div>
    <div class="stat-item"><div class="val">${fmt(winRate,1)}%</div><div class="lbl">Win Rate</div></div>
    <div class="stat-item"><div class="val">${closed.length}</div><div class="lbl">Closed</div></div>`;
  if(!d.length){$('trades').innerHTML='<tr><td colspan="9" style="color:#484f58">No trades yet</td></tr>';return;}
  $('trades').innerHTML=d.map(t=>`<tr>
    <td>${t.open_time?t.open_time.replace('T',' ').slice(0,19):'-'}</td>
    <td>${t.close_time?t.close_time.replace('T',' ').slice(0,19):'-'}</td>
    <td>${t.symbol}</td>
    <td>${t.side||'-'}</td>
    <td>${t.qty!=null?fmt(t.qty,4):'-'}</td>
    <td>${fmt(t.entry_price,4)}</td>
    <td>${t.exit_price?fmt(t.exit_price,4):'-'}</td>
    <td class="${cls(t.pnl_usdt)}">${t.pnl_usdt!=null?fmt(t.pnl_usdt):'-'}</td>
    <td>${t.status}</td></tr>`).join('');
}

let eqChart;
async function loadEquity(){
  const d=await fetchJSON('/dashboard/api/equity_curve');
  if(d.error||!Array.isArray(d)||!d.length){$('chart-box').innerHTML='<div class="loader" style="padding-top:110px;text-align:center">No equity data yet</div>';return;}
  if(!$('eqChart')){$('chart-box').innerHTML='<canvas id="eqChart"></canvas>';}
  const labels=d.map(p=>p.time.replace('T',' ').slice(0,16));
  const data=d.map(p=>p.equity);
  if(eqChart){eqChart.data.labels=labels;eqChart.data.datasets[0].data=data;eqChart.update();return;}
  eqChart=new Chart($('eqChart'),{type:'line',data:{labels,datasets:[{label:'Equity (USDT)',data,borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,.1)',fill:true,tension:.3,pointRadius:3}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#8b949e'}}},scales:{x:{ticks:{color:'#484f58',maxTicksLimit:10}},y:{ticks:{color:'#484f58'}}}}});
}

async function refresh(){await Promise.all([loadBalance(),loadPositions(),loadTrades(),loadEquity()]);updateLastRefresh();}
refresh();
setInterval(refresh,30000);
</script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Start watchdog thread
    t = threading.Thread(target=watchdog_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5001, debug=False)
