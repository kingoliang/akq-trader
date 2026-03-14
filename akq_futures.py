"""
AKQ Futures Executor — 真实币安U本位合约交易
用法:
  python3 akq_futures.py buy ETHUSDT 20 3 2.0 4.0   # 做多
  python3 akq_futures.py sell ETHUSDT                # 平多仓
  python3 akq_futures.py close ETHUSDT               # 平多仓（别名）
  python3 akq_futures.py short ETHUSDT 20 2 2.0 4.0  # 做空
  python3 akq_futures.py cover ETHUSDT               # 平空仓
  python3 akq_futures.py status
  python3 akq_futures.py snapshot
  python3 akq_futures.py manage-long ETHUSDT [TRAIL_GAP_PCT] [force_tp1] [fg_now]
  python3 akq_futures.py sync [SYMBOL] [LIMIT]
"""

import sys
import re
import json
import math
import sqlite3
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
import hmac
import hashlib
import requests
from binance.client import Client
from binance.enums import *

DB_PATH = "/home/azureuser/akq-trader/trades.db"
TAKER_FEE_RATE = 0.0004  # 0.04% each side

# ── 加载 API Key ─────────────────────────────────────────
def load_env(path="/home/azureuser/.benv"):
    env = open(path).read()
    k = re.search(r'BINANCE_API_KEY=(\S+)', env).group(1)
    s = re.search(r'BINANCE_API_SECRET=(\S+)', env).group(1)
    return k, s

KEY, SECRET = load_env()
client = Client(KEY, SECRET)

# ── SQLite ────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        open_time TEXT,
        close_time TEXT,
        symbol TEXT,
        side TEXT DEFAULT 'LONG',
        qty REAL,
        entry_price REAL,
        exit_price REAL,
        leverage INTEGER,
        sl_price REAL,
        tp_price REAL,
        status TEXT DEFAULT 'OPEN',
        margin_usdt REAL,
        pnl_usdt REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS equity_curve (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time TEXT,
        equity REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS trade_strategy_state (
        symbol TEXT,
        side TEXT,
        status TEXT DEFAULT 'ACTIVE',
        stage TEXT DEFAULT 'INIT',
        entry_price REAL,
        qty_initial REAL,
        qty_remaining REAL,
        break_even_set INTEGER DEFAULT 0,
        half_taken INTEGER DEFAULT 0,
        trail_gap_pct REAL DEFAULT 1.5,
        max_price REAL,
        opened_at TEXT,
        updated_at TEXT,
        first_tp_taken INTEGER DEFAULT 0,
        second_tp_taken INTEGER DEFAULT 0,
        fake_break_count INTEGER DEFAULT 0,
        touched_2pct INTEGER DEFAULT 0,
        fg_min_seen REAL,
        fg_current REAL,
        timeout_review_due_at TEXT,
        review_notified INTEGER DEFAULT 0,
        PRIMARY KEY(symbol, side)
    )""")

    # 兼容老库：增量补列
    cols = {r[1] for r in c.execute("PRAGMA table_info(trade_strategy_state)").fetchall()}
    add_cols = {
        "first_tp_taken": "INTEGER DEFAULT 0",
        "second_tp_taken": "INTEGER DEFAULT 0",
        "fake_break_count": "INTEGER DEFAULT 0",
        "touched_2pct": "INTEGER DEFAULT 0",
        "fg_min_seen": "REAL",
        "fg_current": "REAL",
        "timeout_review_due_at": "TEXT",
        "review_notified": "INTEGER DEFAULT 0",
    }
    for col, ddl in add_cols.items():
        if col not in cols:
            c.execute(f"ALTER TABLE trade_strategy_state ADD COLUMN {col} {ddl}")

    conn.commit()
    conn.close()

init_db()

# ── 工具函数 ─────────────────────────────────────────────
def get_symbol_info(symbol):
    """获取合约精度/stepSize/tickSize"""
    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            lot = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
            price = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
            return {
                "stepSize": float(lot["stepSize"]),
                "tickSize": float(price["tickSize"]),
                "pricePrecision": s["pricePrecision"],
                "quantityPrecision": s["quantityPrecision"],
            }
    raise ValueError(f"Symbol not found: {symbol}")

def round_step(value, step):
    precision = max(0, round(-math.log10(step)))
    return round(round(value / step) * step, precision)

def get_mark_price(symbol):
    r = client.futures_mark_price(symbol=symbol)
    return float(r["markPrice"])


def _upsert_long_strategy_state(symbol: str, entry_price: float, qty: float, trail_gap_pct: float = 1.5, fg_now: float | None = None):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    due_iso = (now + timedelta(hours=48)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO trade_strategy_state
          (symbol, side, status, stage, entry_price, qty_initial, qty_remaining,
           break_even_set, half_taken, trail_gap_pct, max_price, opened_at, updated_at,
           first_tp_taken, second_tp_taken, fake_break_count, touched_2pct,
           fg_min_seen, fg_current, timeout_review_due_at, review_notified)
        VALUES (?, 'LONG', 'ACTIVE', 'INIT', ?, ?, ?, 0, 0, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, ?, 0)
        ON CONFLICT(symbol, side) DO UPDATE SET
          status='ACTIVE', stage='INIT', entry_price=excluded.entry_price,
          qty_initial=excluded.qty_initial, qty_remaining=excluded.qty_remaining,
          break_even_set=0, half_taken=0, trail_gap_pct=excluded.trail_gap_pct,
          max_price=excluded.max_price, opened_at=excluded.opened_at, updated_at=excluded.updated_at,
          first_tp_taken=0, second_tp_taken=0, fake_break_count=0, touched_2pct=0,
          fg_min_seen=excluded.fg_min_seen, fg_current=excluded.fg_current,
          timeout_review_due_at=excluded.timeout_review_due_at, review_notified=0
        """,
        (symbol, entry_price, qty, qty, trail_gap_pct, entry_price, now_iso, now_iso,
         fg_now, fg_now, due_iso)
    )
    conn.commit()
    conn.close()


def _load_long_strategy_state(symbol: str):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT symbol, entry_price, qty_initial, qty_remaining, break_even_set, half_taken, trail_gap_pct, max_price, stage, first_tp_taken, second_tp_taken, fake_break_count, touched_2pct, fg_min_seen, fg_current, timeout_review_due_at, review_notified FROM trade_strategy_state WHERE symbol=? AND side='LONG' AND status='ACTIVE'",
        (symbol,)
    ).fetchone()
    conn.close()
    return row


def _save_long_strategy_state(symbol: str, **kwargs):
    if not kwargs:
        return
    now = datetime.now(timezone.utc).isoformat()
    fields = []
    vals = []
    for k, v in kwargs.items():
        fields.append(f"{k}=?")
        vals.append(v)
    fields.append("updated_at=?")
    vals.append(now)
    vals.append(symbol)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"UPDATE trade_strategy_state SET {', '.join(fields)} WHERE symbol=? AND side='LONG'", vals)
    conn.commit()
    conn.close()


def _deactivate_long_strategy_state(symbol: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE trade_strategy_state SET status='INACTIVE', updated_at=? WHERE symbol=? AND side='LONG'", (datetime.now(timezone.utc).isoformat(), symbol))
    conn.commit()
    conn.close()


def _cancel_open_long_exit_orders(symbol: str, cancel_tp: bool = True, cancel_sl: bool = False, cancel_trailing: bool = False):
    try:
        orders = client.futures_get_open_orders(symbol=symbol)
    except Exception:
        return
    for o in orders:
        if o.get("side") != "SELL":
            continue
        if o.get("positionSide") not in ("LONG", None, "BOTH"):
            continue
        typ = o.get("type")
        if typ == "TAKE_PROFIT_MARKET" and cancel_tp:
            client.futures_cancel_order(symbol=symbol, orderId=o["orderId"])
        if typ == "STOP_MARKET" and cancel_sl:
            client.futures_cancel_order(symbol=symbol, orderId=o["orderId"])
        if typ == "TRAILING_STOP_MARKET" and cancel_trailing:
            client.futures_cancel_order(symbol=symbol, orderId=o["orderId"])


def _place_or_replace_long_stop(symbol: str, qty: float, stop_price: float):
    info = get_symbol_info(symbol)
    qty = round_step(qty, info["stepSize"])
    stop_price = round_step(stop_price, info["tickSize"])
    if qty <= 0:
        return None
    _cancel_open_long_exit_orders(symbol, cancel_tp=False, cancel_sl=True)
    return client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        positionSide="LONG",
        type="STOP_MARKET",
        quantity=qty,
        stopPrice=stop_price,
        reduceOnly=True,
        timeInForce="GTE_GTC",
    )


def _place_or_replace_long_trailing_stop(symbol: str, qty: float, callback_rate_pct: float):
    info = get_symbol_info(symbol)
    qty = round_step(qty, info["stepSize"])
    callback_rate_pct = max(0.1, min(10.0, float(callback_rate_pct)))
    if qty <= 0:
        return None
    _cancel_open_long_exit_orders(symbol, cancel_tp=False, cancel_sl=False, cancel_trailing=True)
    return client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        positionSide="LONG",
        type="TRAILING_STOP_MARKET",
        quantity=qty,
        callbackRate=round(callback_rate_pct, 2),
        reduceOnly=True,
        workingType="MARK_PRICE",
    )


def _read_fg_now(default=None):
    paths = [
        "/home/azureuser/.openclaw/workspace/akq-crypto-trader/tools/fg-monitor/.fg_state.json",
        "/home/azureuser/akq-crypto-trader/tools/fg-monitor/.fg_state.json",
    ]
    for p in paths:
        try:
            with open(p, "r") as f:
                data = json.load(f)
            for key in ("value", "fg", "fear_greed", "index"):
                if key in data and data[key] is not None:
                    return float(data[key])
        except Exception:
            continue
    return default


def _compute_trend_ok(symbol: str):
    kl = client.futures_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_15MINUTE, limit=60)
    closes = [float(k[4]) for k in kl]
    if len(closes) < 35:
        return False, {"reason": "kline_insufficient"}

    def ema(vals, n):
        alpha = 2 / (n + 1)
        out = vals[0]
        for v in vals[1:]:
            out = alpha * v + (1 - alpha) * out
        return out

    ma7 = sum(closes[-7:]) / 7
    ma25 = sum(closes[-25:]) / 25
    ema12 = ema(closes[-35:], 12)
    ema26 = ema(closes[-35:], 26)
    macd = ema12 - ema26
    ok = (ma7 > ma25) and (macd > 0)
    return ok, {"ma7": ma7, "ma25": ma25, "macd": macd}


def manage_long_tp(symbol: str, trail_gap_pct: float = 1.5, force_tp1: bool = False, fg_now: float | None = None):
    """方案C(v1.1)执行器：+1.5%保本，+3%平1/3，+4%再平1/3，余仓trailing；含FG/假突破/48h审查。"""
    positions = client.futures_position_information(symbol=symbol)
    pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
    if not pos:
        _deactivate_long_strategy_state(symbol)
        out = {"status": "no_long_position", "symbol": symbol}
        print(json.dumps(out, indent=2))
        return out

    qty = abs(float(pos["positionAmt"]))
    entry = float(pos["entryPrice"])
    mark = float(pos["markPrice"])
    info = get_symbol_info(symbol)

    if fg_now is None:
        fg_now = _read_fg_now(default=None)

    st = _load_long_strategy_state(symbol)
    if not st:
        _upsert_long_strategy_state(symbol, entry, qty, trail_gap_pct, fg_now=fg_now)
        st = _load_long_strategy_state(symbol)

    (_, st_entry, qty_init, qty_rem, be_set, half_taken, gap, max_price, stage,
     tp1_taken, tp2_taken, fake_break_count, touched_2pct,
     fg_min_seen, fg_current, timeout_review_due_at, review_notified) = st

    gap = float(gap if gap else trail_gap_pct)
    max_price = max(float(max_price if max_price else mark), mark)

    # 进入方案C后，关闭原始整仓 TP，避免冲突
    _cancel_open_long_exit_orders(symbol, cancel_tp=True, cancel_sl=False, cancel_trailing=False)

    actions = []
    alerts = []
    pnl_pct = (mark - st_entry) / st_entry * 100 if st_entry else 0.0

    # FG 极端反转：<20 -> >50 立即平仓
    if fg_now is not None:
        if fg_min_seen is None:
            fg_min_seen = fg_now
        fg_min_seen = min(float(fg_min_seen), float(fg_now))
        fg_current = float(fg_now)
        if fg_min_seen < 20 and fg_current > 50:
            actions.append("fg_extreme_reversal_close")
            sell(symbol)
            out = {
                "status": "closed_by_fg_reversal",
                "symbol": symbol,
                "fg_min_seen": fg_min_seen,
                "fg_now": fg_current,
                "actions": actions,
            }
            print(json.dumps(out, indent=2))
            return out

    # 假突破计数：触及+2%后回落到成本（<=0.1%）计数；2次强平
    if pnl_pct >= 2.0:
        touched_2pct = 1
    if touched_2pct and pnl_pct <= 0.1:
        fake_break_count = int(fake_break_count or 0) + 1
        touched_2pct = 0
        actions.append(f"fake_break_count:{fake_break_count}")
        if fake_break_count >= 2:
            actions.append("force_close_fake_break_x2")
            sell(symbol)
            out = {
                "status": "closed_by_fake_breakout",
                "symbol": symbol,
                "fake_break_count": fake_break_count,
                "actions": actions,
            }
            print(json.dumps(out, indent=2))
            return out

    # +1.5% 移到保本
    if (not be_set) and pnl_pct >= 1.5:
        _place_or_replace_long_stop(symbol, qty, st_entry)
        be_set = 1
        stage = "BREAKEVEN"
        actions.append("move_sl_to_breakeven")

    # +3% 平1/3
    if (not tp1_taken) and (pnl_pct >= 3.0 or force_tp1):
        close_qty = round_step(max(qty_init / 3.0, info["stepSize"]), info["stepSize"])
        close_qty = min(close_qty, qty)
        if close_qty > 0 and close_qty < qty:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL,
                positionSide="LONG",
                type=FUTURE_ORDER_TYPE_MARKET,
                quantity=close_qty,
                reduceOnly=True,
            )
            actions.append(f"take_profit_1_3_at_3pct:{close_qty}")
        positions = client.futures_position_information(symbol=symbol)
        pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
        qty = abs(float(pos["positionAmt"])) if pos else 0.0
        tp1_taken = 1
        half_taken = 1
        stage = "TP1_TAKEN"

    # +4% 再平1/3
    if tp1_taken and (not tp2_taken) and pnl_pct >= 4.0 and qty > 0:
        close_qty = round_step(max(qty_init / 3.0, info["stepSize"]), info["stepSize"])
        close_qty = min(close_qty, qty)
        if close_qty > 0 and close_qty < qty:
            client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL,
                positionSide="LONG",
                type=FUTURE_ORDER_TYPE_MARKET,
                quantity=close_qty,
                reduceOnly=True,
            )
            actions.append(f"take_profit_1_3_at_4pct:{close_qty}")
        positions = client.futures_position_information(symbol=symbol)
        pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
        qty = abs(float(pos["positionAmt"])) if pos else 0.0
        tp2_taken = 1
        stage = "TP2_TAKEN"

    # 余仓 trailing（策略层 + 交易所 TRAILING_STOP_MARKET 兜底）
    if tp2_taken and qty > 0:
        trail_stop = max(st_entry, max_price * (1 - gap / 100))
        _place_or_replace_long_stop(symbol, qty, trail_stop)  # 策略层硬止损
        try:
            _place_or_replace_long_trailing_stop(symbol, qty, gap)  # 交易所 trailing 兜底
            actions.append(f"exchange_trailing_stop:{gap}%")
        except Exception as e:
            alerts.append(f"trailing_stop_failed:{e}")
        actions.append(f"trail_sl:{round(trail_stop,4)}")
        stage = "TRAILING"

    # 48h 审查事件（不自动平仓）
    if timeout_review_due_at:
        try:
            due_dt = datetime.fromisoformat(timeout_review_due_at)
            now_dt = datetime.now(timezone.utc)
            if now_dt >= due_dt and not review_notified:
                trend_ok, trend_meta = _compute_trend_ok(symbol)
                recommendation = "close"
                if pnl_pct > 3 and trend_ok:
                    recommendation = "extend_24h"
                elif pnl_pct < 0:
                    recommendation = "close_now"
                alerts.append(
                    f"REVIEW_REQUIRED_48H symbol={symbol} pnl_pct={round(pnl_pct,3)} recommendation={recommendation} trend={json.dumps(trend_meta)}"
                )
                review_notified = 1
                if recommendation == "extend_24h":
                    timeout_review_due_at = (now_dt + timedelta(hours=24)).isoformat()
                    review_notified = 0
        except Exception as e:
            alerts.append(f"review_check_error:{e}")

    _save_long_strategy_state(
        symbol,
        qty_remaining=qty,
        break_even_set=be_set,
        half_taken=half_taken,
        trail_gap_pct=gap,
        max_price=max_price,
        stage=stage,
        first_tp_taken=tp1_taken,
        second_tp_taken=tp2_taken,
        fake_break_count=fake_break_count,
        touched_2pct=touched_2pct,
        fg_min_seen=fg_min_seen,
        fg_current=fg_current,
        timeout_review_due_at=timeout_review_due_at,
        review_notified=review_notified,
    )

    out = {
        "status": "ok",
        "symbol": symbol,
        "entry_price": round(st_entry, 6),
        "mark_price": round(mark, 6),
        "pnl_pct": round(pnl_pct, 4),
        "qty_remaining": round(qty, 6),
        "stage": stage,
        "trail_gap_pct": gap,
        "fake_break_count": int(fake_break_count or 0),
        "fg_now": fg_now,
        "actions": actions,
        "alerts": alerts,
    }
    print(json.dumps(out, indent=2))
    return out


def get_futures_algo_open_orders(symbol: str | None = None):
    """查询 Binance Futures Algo/Conditional 条件单。"""
    params = {"timestamp": int(datetime.now(timezone.utc).timestamp() * 1000)}
    if symbol:
        params["symbol"] = symbol
    query = urlencode(params)
    sig = hmac.new(SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": KEY}
    url = f"https://api.binance.com/sapi/v1/algo/futures/openOrders?{query}&signature={sig}"
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and data.get("code") not in (None, 0):
        raise RuntimeError(f"Algo query failed: {data}")
    return data

# ── 核心函数 ─────────────────────────────────────────────
def buy(symbol: str, usdt_amount: float, leverage: int,
        sl_pct: float, tp_pct: float) -> dict:
    """
    市价开多仓 + 挂止损止盈单
    sl_pct / tp_pct: 百分比数字，如 2.0 表示 2%
    返回 dict: order_id, entry_price, sl_price, tp_price, qty
    """
    info = get_symbol_info(symbol)

    # 1. 设置杠杆
    client.futures_change_leverage(symbol=symbol, leverage=leverage)

    # 2. 计算开仓数量（名义价值 = usdt_amount * leverage）
    mark = get_mark_price(symbol)
    notional = usdt_amount * leverage
    qty = round_step(notional / mark, info["stepSize"])
    if qty <= 0:
        raise ValueError(f"数量过小: {qty} (notional={notional}, mark={mark})")

    # 3. 市价开多（兼容 hedge mode）
    order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_BUY,
        positionSide="LONG",
        type=FUTURE_ORDER_TYPE_MARKET,
        quantity=qty,
    )

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
    order_id = order["orderId"]

    # 4. 计算 SL/TP 价格
    sl_price = round_step(entry_price * (1 - sl_pct / 100), info["tickSize"])
    tp_price = round_step(entry_price * (1 + tp_pct / 100), info["tickSize"])

    # 5. 挂止损单 (STOP_MARKET) / 止盈单 (TAKE_PROFIT_MARKET)
    # 不再使用 closePosition=True，改为指定数量，确保 open_orders 可见、可核验
    sl_order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        positionSide="LONG",
        type="STOP_MARKET",
        quantity=qty,
        stopPrice=sl_price,
        reduceOnly=True,
        timeInForce="GTE_GTC",
    )

    tp_order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        positionSide="LONG",
        type="TAKE_PROFIT_MARKET",
        quantity=qty,
        stopPrice=tp_price,
        reduceOnly=True,
        timeInForce="GTE_GTC",
    )

    result = {
        "order_id": order_id,
        "symbol": symbol,
        "qty": qty,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "leverage": leverage,
        "usdt_margin": usdt_amount,
        "sl_order_id": sl_order.get("orderId"),
        "tp_order_id": tp_order.get("orderId"),
    }

    # 写入交易记录 + 初始化方案C状态
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO trades (open_time, symbol, side, qty, entry_price, leverage, sl_price, tp_price, status, margin_usdt) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), symbol, "LONG", qty, entry_price, leverage, sl_price, tp_price, "OPEN", usdt_amount)
        )
        conn.commit()
        conn.close()
        _upsert_long_strategy_state(symbol, entry_price, qty, trail_gap_pct=1.5, fg_now=_read_fg_now(default=None))
    except Exception as e:
        print(f"[DB] 写入失败: {e}")

    print(json.dumps(result, indent=2))
    return result


def _net_pnl_after_taker_fee(entry_price: float, exit_price: float, qty: float, side: str):
    """返回 (gross_pnl, fee_usdt, net_pnl)，手续费按开平双边 0.04% 估算。"""
    gross = (exit_price - entry_price) * qty if side == "LONG" else (entry_price - exit_price) * qty
    fee = (entry_price * qty + exit_price * qty) * TAKER_FEE_RATE
    net = gross - fee
    return gross, fee, net


def sell(symbol: str) -> dict:
    """市价平掉 symbol 的 LONG 仓位；兼容 hedge mode。"""
    # 取消所有挂单
    client.futures_cancel_all_open_orders(symbol=symbol)

    # 查询持仓（优先找 hedge mode 的 LONG 仓）
    positions = client.futures_position_information(symbol=symbol)
    pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
    if not pos:
        # 兼容单向持仓模式
        pos = next((p for p in positions if float(p["positionAmt"]) > 0), None)
    if not pos:
        print(json.dumps({"status": "no_position", "symbol": symbol}))
        return {"status": "no_position"}

    qty = abs(float(pos["positionAmt"]))
    entry_price = float(pos["entryPrice"])
    order_params = {
        "symbol": symbol,
        "side": SIDE_SELL,
        "type": FUTURE_ORDER_TYPE_MARKET,
        "quantity": qty,
    }
    # hedge mode 必须带 positionSide；单向模式则使用 reduceOnly
    if pos.get("positionSide") in {"LONG", "SHORT"}:
        order_params["positionSide"] = pos["positionSide"]
    else:
        order_params["reduceOnly"] = True

    order = client.futures_create_order(**order_params)

    # 尝试从成交回报里拿真实成交价；没有再退回 avgPrice / markPrice
    exit_price = None
    try:
        trades = client.futures_account_trades(symbol=symbol, limit=10)
        trade = next((t for t in reversed(trades) if str(t.get("orderId")) == str(order.get("orderId"))), None)
        if trade:
            exit_price = float(trade["price"])
    except Exception:
        pass
    if exit_price is None:
        exit_price = float(order.get("avgPrice") or get_mark_price(symbol))

    gross_pnl, fee_usdt, pnl = _net_pnl_after_taker_fee(entry_price, exit_price, qty, "LONG")

    # 更新交易记录 + 记录权益曲线
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE trades SET close_time=?, exit_price=?, pnl_usdt=?, status='CLOSED' WHERE symbol=? AND status='OPEN' ORDER BY id DESC LIMIT 1",
            (datetime.now(timezone.utc).isoformat(), exit_price, pnl, symbol)
        )
        # 记录权益曲线
        balances = client.futures_account_balance()
        usdt_bal = next((b for b in balances if b["asset"] == "USDT"), None)
        if usdt_bal:
            conn.execute("INSERT INTO equity_curve (time, equity) VALUES (?,?)",
                         (datetime.now(timezone.utc).isoformat(), float(usdt_bal["balance"])))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 更新失败: {e}")

    _deactivate_long_strategy_state(symbol)

    result = {
        "status": "closed",
        "symbol": symbol,
        "qty": qty,
        "exit_price": exit_price,
        "gross_pnl_usdt": round(gross_pnl, 4),
        "fee_usdt": round(fee_usdt, 4),
        "pnl_usdt": round(pnl, 4),
        "order_id": order["orderId"],
    }
    print(json.dumps(result, indent=2))
    return result


def status():
    """显示合约账户余额和当前持仓"""
    # 余额
    balances = client.futures_account_balance()
    usdt = next((b for b in balances if b["asset"] == "USDT"), None)
    print(f"=== 合约余额 ===")
    if usdt:
        print(f"USDT: {usdt['balance']} (可用: {usdt['availableBalance']})")

    # 持仓
    positions = client.futures_position_information()
    active = [p for p in positions if float(p["positionAmt"]) != 0]
    print(f"\n=== 持仓 ({len(active)}个) ===")
    for p in active:
        amt = float(p["positionAmt"])
        pnl = float(p["unRealizedProfit"])
        print(f"{p['symbol']}: amt={amt}, entry={p['entryPrice']}, mark={p['markPrice']}, PnL={pnl:.4f}U")

    # 普通挂单
    orders = client.futures_get_open_orders()
    print(f"\n=== 挂单 ({len(orders)}个) ===")
    for o in orders:
        print(f"{o['symbol']} {o['type']} {o['side']} stop={o.get('stopPrice','N/A')}")

    # Algo / Conditional 条件单
    try:
        algo_orders = get_futures_algo_open_orders()
    except Exception as e:
        print(f"\n=== 条件单 (algo) 查询失败 ===")
        print(str(e))
    else:
        if isinstance(algo_orders, dict) and "rows" in algo_orders:
            rows = algo_orders.get("rows", [])
        elif isinstance(algo_orders, list):
            rows = algo_orders
        else:
            rows = []
        print(f"\n=== 条件单 (algo) ({len(rows)}个) ===")
        for o in rows:
            print(
                f"{o.get('symbol')} {o.get('orderType') or o.get('algoType')} {o.get('side')} "
                f"pos={o.get('positionSide')} qty={o.get('quantity')} trigger={o.get('triggerPrice')} "
                f"status={o.get('algoStatus')} algoId={o.get('algoId')}"
            )


def short(symbol: str, usdt_amount: float, leverage: int,
          sl_pct: float, tp_pct: float) -> dict:
    """
    市价开空仓 + 挂止损止盈单
    sl_pct: 止损幅度（价格上涨%），如 2.0 表示入场价 +2% 触发止损
    tp_pct: 止盈幅度（价格下跌%），如 4.0 表示入场价 -4% 触发止盈
    返回 dict: order_id, entry_price, sl_price, tp_price, qty
    """
    # 安全检查：如有同标的多仓，拒绝开空
    positions = client.futures_position_information(symbol=symbol)
    long_pos = next((p for p in positions if p.get("positionSide") == "LONG" and float(p["positionAmt"]) > 0), None)
    if long_pos:
        raise ValueError(f"[安全阻断] {symbol} 存在多仓 qty={long_pos['positionAmt']}，请先平多仓再开空")

    info = get_symbol_info(symbol)
    client.futures_change_leverage(symbol=symbol, leverage=leverage)

    mark = get_mark_price(symbol)
    notional = usdt_amount * leverage
    qty = round_step(notional / mark, info["stepSize"])
    if qty <= 0:
        raise ValueError(f"数量过小: {qty} (notional={notional}, mark={mark})")

    # 市价开空（hedge mode: side=SELL + positionSide=SHORT）
    order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        positionSide="SHORT",
        type=FUTURE_ORDER_TYPE_MARKET,
        quantity=qty,
    )

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
    order_id = order["orderId"]

    # 做空：SL = 价格上涨 sl_pct%，TP = 价格下跌 tp_pct%
    sl_price = round_step(entry_price * (1 + sl_pct / 100), info["tickSize"])
    tp_price = round_step(entry_price * (1 - tp_pct / 100), info["tickSize"])

    # 止损单：STOP_MARKET，空仓平仓用 BUY + positionSide=SHORT
    sl_order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_BUY,
        positionSide="SHORT",
        type="STOP_MARKET",
        stopPrice=sl_price,
        closePosition=True,
        workingType="MARK_PRICE",
    )

    # 止盈单：TAKE_PROFIT_MARKET，市价平仓
    tp_order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_BUY,
        positionSide="SHORT",
        type="TAKE_PROFIT_MARKET",
        stopPrice=tp_price,
        closePosition=True,
        workingType="MARK_PRICE",
    )

    result = {
        "order_id": order_id,
        "symbol": symbol,
        "direction": "SHORT",
        "qty": qty,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "leverage": leverage,
        "usdt_margin": usdt_amount,
        "sl_order_id": sl_order.get("orderId"),
        "tp_order_id": tp_order.get("orderId"),
    }

    # 写入交易记录
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO trades (open_time, symbol, side, qty, entry_price, leverage, sl_price, tp_price, status, margin_usdt) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), symbol, "SHORT", qty, entry_price, leverage, sl_price, tp_price, "OPEN", usdt_amount)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 写入失败: {e}")

    print(json.dumps(result, indent=2))
    return result


def cover(symbol: str) -> dict:
    """市价平掉 symbol 的 SHORT 仓位；兼容 hedge mode。"""
    client.futures_cancel_all_open_orders(symbol=symbol)

    positions = client.futures_position_information(symbol=symbol)
    pos = next((p for p in positions if p.get("positionSide") == "SHORT" and float(p["positionAmt"]) < 0), None)
    if not pos:
        print(json.dumps({"status": "no_short_position", "symbol": symbol}))
        return {"status": "no_short_position"}

    qty = abs(float(pos["positionAmt"]))
    entry_price = float(pos["entryPrice"])

    order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_BUY,
        positionSide="SHORT",
        type=FUTURE_ORDER_TYPE_MARKET,
        quantity=qty,
    )

    exit_price = None
    try:
        trades = client.futures_account_trades(symbol=symbol, limit=10)
        trade = next((t for t in reversed(trades) if str(t.get("orderId")) == str(order.get("orderId"))), None)
        if trade:
            exit_price = float(trade["price"])
    except Exception:
        pass
    if exit_price is None:
        exit_price = float(order.get("avgPrice") or get_mark_price(symbol))

    # 做空净盈亏：毛利 - 开平双边 taker 手续费
    gross_pnl, fee_usdt, pnl = _net_pnl_after_taker_fee(entry_price, exit_price, qty, "SHORT")

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE trades SET close_time=?, exit_price=?, pnl_usdt=?, status='CLOSED' WHERE symbol=? AND side='SHORT' AND status='OPEN' ORDER BY id DESC LIMIT 1",
            (datetime.now(timezone.utc).isoformat(), exit_price, pnl, symbol)
        )
        balances = client.futures_account_balance()
        usdt_bal = next((b for b in balances if b["asset"] == "USDT"), None)
        if usdt_bal:
            conn.execute("INSERT INTO equity_curve (time, equity) VALUES (?,?)",
                         (datetime.now(timezone.utc).isoformat(), float(usdt_bal["balance"])))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 更新失败: {e}")

    result = {
        "status": "covered",
        "symbol": symbol,
        "direction": "SHORT",
        "qty": qty,
        "exit_price": exit_price,
        "gross_pnl_usdt": round(gross_pnl, 4),
        "fee_usdt": round(fee_usdt, 4),
        "pnl_usdt": round(pnl, 4),
        "order_id": order["orderId"],
    }
    print(json.dumps(result, indent=2))
    return result


def sync_closed_trades(symbol: str = "ETHUSDT", limit: int = 50):
    """
    从 Binance 成交历史补录未入库的平仓记录（LONG/SHORT 兼容）。
    识别规则：
      - LONG 开仓:  BUY + positionSide=LONG
      - LONG 平仓:  SELL + positionSide=LONG
      - SHORT 开仓: SELL + positionSide=SHORT
      - SHORT 平仓: BUY + positionSide=SHORT
    """
    raw = client.futures_account_trades(symbol=symbol, limit=limit)

    from collections import defaultdict

    # 按 orderId 聚合（同一订单可能多笔成交）
    order_map = defaultdict(lambda: {
        "side": None,
        "positionSide": None,
        "qty": 0.0,
        "pnl": 0.0,
        "price_sum": 0.0,
        "qty_sum": 0.0,
        "time": None,
    })

    for t in raw:
        oid = t["orderId"]
        entry = order_map[oid]
        entry["side"] = t["side"]
        entry["positionSide"] = t.get("positionSide") or "BOTH"
        qty = float(t["qty"])
        price = float(t["price"])
        pnl = float(t["realizedPnl"])
        entry["qty"] += qty
        entry["pnl"] += pnl
        entry["price_sum"] += price * qty
        entry["qty_sum"] += qty
        if entry["time"] is None:
            entry["time"] = t["time"]

    # 分方向构建开仓/平仓集合
    long_opens, long_closes = [], []
    short_opens, short_closes = [], []

    for oid, d in order_map.items():
        side = d["side"]
        pos_side = d["positionSide"]
        if side == "BUY" and pos_side == "LONG":
            long_opens.append((oid, d))
        elif side == "SELL" and pos_side == "LONG":
            long_closes.append((oid, d))
        elif side == "SELL" and pos_side == "SHORT":
            short_opens.append((oid, d))
        elif side == "BUY" and pos_side == "SHORT":
            short_closes.append((oid, d))

    long_opens.sort(key=lambda x: x[1]["time"])
    long_closes.sort(key=lambda x: x[1]["time"])
    short_opens.sort(key=lambda x: x[1]["time"])
    short_closes.sort(key=lambda x: x[1]["time"])

    def find_last_open(opens, close_time_ms):
        for _, od in reversed(opens):
            if od["time"] < close_time_ms:
                return od
        return None

    conn = sqlite3.connect(DB_PATH)
    inserted = 0

    def insert_close(close_d, direction, opens):
        nonlocal inserted
        exit_price = close_d["price_sum"] / close_d["qty_sum"]
        exit_time = datetime.fromtimestamp(close_d["time"] / 1000, tz=timezone.utc).isoformat()
        qty = close_d["qty"]
        pnl = close_d["pnl"]

        open_d = find_last_open(opens, close_d["time"])
        if open_d:
            entry_price = open_d["price_sum"] / open_d["qty_sum"]
            open_time = datetime.fromtimestamp(open_d["time"] / 1000, tz=timezone.utc).isoformat()
        else:
            entry_price = exit_price
            open_time = exit_time

        # 去重（增强版）：
        # 1) 精确去重：symbol + side + close_time
        existing = conn.execute(
            "SELECT id FROM trades WHERE symbol=? AND side=? AND close_time=? AND status='CLOSED'",
            (symbol, direction, exit_time)
        ).fetchone()
        if existing:
            return

        # 2) 近似去重：防止同一笔被“实时写入 + sync补录”重复记录
        #    注意：pnl_usdt 可能因手续费口径不同而有差异，不能作为去重条件
        #    条件：同 symbol/side + qty/entry/exit 基本一致，开平仓时间在 ±180 秒内
        fuzzy_existing = conn.execute(
            """
            SELECT id FROM trades
            WHERE symbol=? AND side=? AND status='CLOSED'
              AND ABS(COALESCE(qty,0) - ?) < 1e-6
              AND ABS(COALESCE(entry_price,0) - ?) < 0.02
              AND ABS(COALESCE(exit_price,0) - ?) < 0.02
              AND ABS(strftime('%s', close_time) - strftime('%s', ?)) <= 180
              AND ABS(strftime('%s', open_time) - strftime('%s', ?)) <= 180
            LIMIT 1
            """,
            (symbol, direction, qty, round(entry_price, 4), round(exit_price, 4), exit_time, open_time)
        ).fetchone()
        if fuzzy_existing:
            return

        conn.execute(
            """INSERT INTO trades
               (open_time, close_time, symbol, side, qty, entry_price, exit_price,
                leverage, sl_price, tp_price, status, margin_usdt, pnl_usdt)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (open_time, exit_time, symbol, direction, qty,
             round(entry_price, 4), round(exit_price, 4),
             None, None, None, "CLOSED", None, round(pnl, 6))
        )
        inserted += 1
        print(f"[sync] 补录: {symbol} {direction} {exit_time} entry={entry_price:.2f} exit={exit_price:.2f} pnl={pnl:.4f}")

    for _, cd in long_closes:
        insert_close(cd, "LONG", long_opens)
    for _, cd in short_closes:
        insert_close(cd, "SHORT", short_opens)

    conn.commit()
    conn.close()
    print(f"[sync] 完成，共补录 {inserted} 条")
    return inserted


def snapshot_equity():
    """记录当前总权益（walletBalance + unrealizedPnL）到 equity_curve"""
    balances = client.futures_account_balance()
    usdt = next((b for b in balances if b["asset"] == "USDT"), None)
    account = client.futures_account()

    wallet_balance = float(usdt["balance"]) if usdt else float(account.get("totalWalletBalance", 0))
    unrealized_pnl = float(account.get("totalUnrealizedProfit", 0))
    equity = wallet_balance + unrealized_pnl
    now = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO equity_curve (time, equity) VALUES (?, ?)", (now, equity))
    conn.commit()
    conn.close()

    result = {
        "status": "ok",
        "time": now,
        "wallet_balance": round(wallet_balance, 8),
        "unrealized_pnl": round(unrealized_pnl, 8),
        "equity": round(equity, 8),
    }
    print(json.dumps(result, indent=2))
    return result


# ── CLI 入口 ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "buy":
        # buy SYMBOL USDT_AMOUNT LEVERAGE SL_PCT TP_PCT
        symbol     = sys.argv[2].upper()
        usdt_amt   = float(sys.argv[3])
        leverage   = int(sys.argv[4])
        sl_pct     = float(sys.argv[5])
        tp_pct     = float(sys.argv[6])
        buy(symbol, usdt_amt, leverage, sl_pct, tp_pct)

    elif cmd in {"sell", "close"}:
        symbol = sys.argv[2].upper()
        sell(symbol)

    elif cmd == "short":
        # short SYMBOL USDT_AMOUNT LEVERAGE SL_PCT TP_PCT
        symbol     = sys.argv[2].upper()
        usdt_amt   = float(sys.argv[3])
        leverage   = int(sys.argv[4])
        sl_pct     = float(sys.argv[5])
        tp_pct     = float(sys.argv[6])
        short(symbol, usdt_amt, leverage, sl_pct, tp_pct)

    elif cmd == "cover":
        symbol = sys.argv[2].upper()
        cover(symbol)

    elif cmd == "status":
        status()

    elif cmd == "snapshot":
        snapshot_equity()

    elif cmd == "manage-long":
        symbol = sys.argv[2].upper()
        gap = float(sys.argv[3]) if len(sys.argv) > 3 else 1.5
        force_tp1 = (len(sys.argv) > 4 and sys.argv[4].lower() in {"1", "true", "yes", "force", "force_tp1"})
        fg_now = float(sys.argv[5]) if len(sys.argv) > 5 else None
        manage_long_tp(symbol, trail_gap_pct=gap, force_tp1=force_tp1, fg_now=fg_now)

    elif cmd == "sync":
        symbol = sys.argv[2].upper() if len(sys.argv) > 2 else "ETHUSDT"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 50
        sync_closed_trades(symbol, limit)

    else:
        print(f"未知命令: {cmd}")
        print(__doc__)
        sys.exit(1)
