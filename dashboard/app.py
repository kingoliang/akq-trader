"""
AKQ Futures Trading Dashboard — Flask app on port 5003
"""
import re
import sqlite3
from flask import Flask, jsonify

DB_PATH = "/home/azureuser/akq-trader/trades.db"
ENV_PATH = "/home/azureuser/.benv"

# ── Binance client ────────────────────────────────────────
def load_env(path=ENV_PATH):
    env = open(path).read()
    k = re.search(r'BINANCE_API_KEY=(\S+)', env).group(1)
    s = re.search(r'BINANCE_API_SECRET=(\S+)', env).group(1)
    return k, s

from binance.client import Client
KEY, SECRET = load_env()
client = Client(KEY, SECRET)

app = Flask(__name__)

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
        margin = float(
            position.get("positionInitialMargin")
            or position.get("initialMargin")
            or 0
        )
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
    conn.commit()
    conn.close()

init_db()

# ── API routes ────────────────────────────────────────────
@app.route("/api/balance")
def api_balance():
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

@app.route("/api/positions")
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
            })
        return jsonify(active)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/trades")
def api_trades():
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100").fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/equity_curve")
def api_equity_curve():
    try:
        conn = get_db()
        rows = conn.execute("SELECT time, equity FROM equity_curve ORDER BY id ASC").fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Main page ─────────────────────────────────────────────
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
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:16px}
.full{grid-column:1/-1}
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
<h1>AKQ Futures Dashboard</h1>

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
    <thead><tr><th>Open Time</th><th>Symbol</th><th>Entry</th><th>Exit</th><th>PnL (USDT)</th><th>Status</th></tr></thead>
    <tbody id="trades"><tr><td colspan="6" class="loader">Loading...</td></tr></tbody>
  </table>
</div>

<script>
const $ = s => document.getElementById(s);
const fmt = (v,d=2) => v!=null ? Number(v).toFixed(d) : '-';
const cls = v => v >= 0 ? 'pos' : 'neg';

async function fetchJSON(url) {
  try { const r = await fetch(url); return await r.json(); }
  catch(e) { return {error: e.message}; }
}

async function loadBalance() {
  const d = await fetchJSON('/api/balance');
  if (d.error) { $('bal').innerHTML = '<span class="neg">Error: '+d.error+'</span>'; return; }
  $('bal').innerHTML = `
    <div class="stat-item"><div class="val">${fmt(d.balance)}</div><div class="lbl">Total USDT</div></div>
    <div class="stat-item"><div class="val">${fmt(d.available)}</div><div class="lbl">Available</div></div>
    <div class="stat-item"><div class="val ${cls(d.unrealizedPnl)}">${fmt(d.unrealizedPnl)}</div><div class="lbl">Unrealized PnL</div></div>
    <div class="stat-item"><div class="val">${fmt(d.totalEquity)}</div><div class="lbl">Total Equity</div></div>`;
}

async function loadPositions() {
  const d = await fetchJSON('/api/positions');
  if (d.error) { $('pos').innerHTML = '<tr><td colspan="9" class="neg">Error</td></tr>'; return; }
  if (!d.length) { $('pos').innerHTML = '<tr><td colspan="9" style="color:#484f58">No open positions</td></tr>'; return; }
  $('pos').innerHTML = d.map(p => `<tr>
    <td>${p.symbol}</td><td>${fmt(p.entryPrice,4)}</td><td>${fmt(p.qty,4)}</td>
    <td>${fmt(p.markPrice,4)}</td>
    <td class="${cls(p.pnlUsdt)}">${fmt(p.pnlUsdt)}</td>
    <td class="${cls(p.pnlPct)}">${fmt(p.pnlPct)}%</td>
    <td>${p.leverage}x</td>
    <td>${fmt(p.cost)}</td>
    <td>${fmt(p.value)}</td></tr>`).join('');
}

async function loadTrades() {
  const d = await fetchJSON('/api/trades');
  if (d.error) {
    $('stats').innerHTML = '<span class="neg">Error</span>';
    $('trades').innerHTML = '<tr><td colspan="6" class="neg">Error</td></tr>';
    return;
  }

  const closed = d.filter(t => t.status === 'CLOSED');
  const totalPnl = closed.reduce((s,t) => s + (t.pnl_usdt||0), 0);
  const wins = closed.filter(t => (t.pnl_usdt||0) > 0).length;
  const winRate = closed.length ? (wins/closed.length*100) : 0;
  $('stats').innerHTML = `
    <div class="stat-item"><div class="val">${d.length}</div><div class="lbl">Total Trades</div></div>
    <div class="stat-item"><div class="val ${cls(totalPnl)}">${fmt(totalPnl)}</div><div class="lbl">Total PnL</div></div>
    <div class="stat-item"><div class="val">${fmt(winRate,1)}%</div><div class="lbl">Win Rate</div></div>
    <div class="stat-item"><div class="val">${closed.length}</div><div class="lbl">Closed</div></div>`;

  if (!d.length) {
    $('trades').innerHTML = '<tr><td colspan="6" style="color:#484f58">No trades yet</td></tr>';
    return;
  }

  $('trades').innerHTML = d.map(t => `<tr>
    <td>${t.open_time ? t.open_time.replace('T',' ').slice(0,19) : '-'}</td>
    <td>${t.symbol}</td><td>${fmt(t.entry_price,4)}</td>
    <td>${t.exit_price ? fmt(t.exit_price,4) : '-'}</td>
    <td class="${cls(t.pnl_usdt)}">${t.pnl_usdt!=null ? fmt(t.pnl_usdt) : '-'}</td>
    <td>${t.status}</td></tr>`).join('');
}

let eqChart;
async function loadEquity() {
  const d = await fetchJSON('/api/equity_curve');
  if (d.error || !Array.isArray(d) || !d.length) {
    $('chart-box').innerHTML = '<div class="loader" style="padding-top:110px;text-align:center">No equity data yet</div>';
    return;
  }
  if (!$('eqChart')) {
    $('chart-box').innerHTML = '<canvas id="eqChart"></canvas>';
  }
  const labels = d.map(p => p.time.replace('T',' ').slice(0,16));
  const data = d.map(p => p.equity);
  if (eqChart) { eqChart.data.labels=labels; eqChart.data.datasets[0].data=data; eqChart.update(); return; }
  eqChart = new Chart($('eqChart'), {
    type: 'line',
    data: { labels, datasets: [{ label:'Equity (USDT)', data, borderColor:'#58a6ff', backgroundColor:'rgba(88,166,255,.1)', fill:true, tension:.3, pointRadius:3 }] },
    options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{labels:{color:'#8b949e'}}},
      scales:{ x:{ticks:{color:'#484f58',maxTicksLimit:10}}, y:{ticks:{color:'#484f58'}} } }
  });
}

async function refresh() {
  await Promise.all([loadBalance(), loadPositions(), loadTrades(), loadEquity()]);
}
refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
