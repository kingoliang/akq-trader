"""
AKQ Futures Executor — 真实币安U本位合约交易
用法:
  python3 akq_futures.py buy ETHUSDT 20 3 2.0 4.0
  python3 akq_futures.py sell ETHUSDT
  python3 akq_futures.py close ETHUSDT
  python3 akq_futures.py status
  python3 akq_futures.py snapshot
"""

import sys
import re
import json
import math
import sqlite3
from datetime import datetime, timezone
from binance.client import Client
from binance.enums import *

DB_PATH = "/home/azureuser/akq-trader/trades.db"

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

    # 3. 市价开多
    order = client.futures_create_order(
        symbol=symbol,
        side=SIDE_BUY,
        type=FUTURE_ORDER_TYPE_MARKET,
        quantity=qty,
    )
    entry_price = float(order.get("avgPrice") or mark)
    order_id = order["orderId"]

    # 4. 计算 SL/TP 价格
    sl_price = round_step(entry_price * (1 - sl_pct / 100), info["tickSize"])
    tp_price = round_step(entry_price * (1 + tp_pct / 100), info["tickSize"])

    # 5. 挂止损单 (STOP_MARKET)
    client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        type="STOP_MARKET",
        stopPrice=sl_price,
        closePosition=True,
        timeInForce="GTE_GTC",
    )

    # 6. 挂止盈单 (TAKE_PROFIT_MARKET)
    client.futures_create_order(
        symbol=symbol,
        side=SIDE_SELL,
        type="TAKE_PROFIT_MARKET",
        stopPrice=tp_price,
        closePosition=True,
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
    }

    # 写入交易记录
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO trades (open_time, symbol, side, qty, entry_price, leverage, sl_price, tp_price, status, margin_usdt) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), symbol, "LONG", qty, entry_price, leverage, sl_price, tp_price, "OPEN", usdt_amount)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 写入失败: {e}")

    print(json.dumps(result, indent=2))
    return result


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
    # hedge mode 必须带 positionSide，且不需要 reduceOnly
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

    pnl = (exit_price - entry_price) * qty

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

    result = {
        "status": "closed",
        "symbol": symbol,
        "qty": qty,
        "exit_price": exit_price,
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

    # 挂单
    orders = client.futures_get_open_orders()
    print(f"\n=== 挂单 ({len(orders)}个) ===")
    for o in orders:
        print(f"{o['symbol']} {o['type']} {o['side']} stop={o.get('stopPrice','N/A')}")


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

    elif cmd == "status":
        status()

    elif cmd == "snapshot":
        snapshot_equity()

    else:
        print(f"未知命令: {cmd}")
        print(__doc__)
        sys.exit(1)
