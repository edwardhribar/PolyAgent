"""
PolyAgent Conservative Bot + Live API Server
Runs the trading bot AND serves real trade data at /api/stats
"""

import os, json, logging, asyncio, aiohttp, sqlite3, threading
from datetime import datetime, timezone
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/agent.log"), logging.StreamHandler()])
log = logging.getLogger("polyagent")

WALLET          = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
POLY_API_KEY    = os.getenv("POLYMARKET_API_KEY", "")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
MAX_TRADE       = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_POSITIONS   = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
MIN_LIQUIDITY   = float(os.getenv("MIN_LIQUIDITY", "5000"))
MIN_VOLUME      = float(os.getenv("MIN_VOLUME", "10000"))
BASE_CONFIDENCE = int(os.getenv("BASE_CONFIDENCE", "75"))
SCAN_INTERVAL   = int(os.getenv("SCAN_INTERVAL_SECS", "300"))
MARKETS_PER_SCAN= int(os.getenv("MARKETS_PER_SCAN", "20"))
API_PORT        = int(os.getenv("PORT", "8080"))
BOT_NAME        = "Conservative"

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
ANT_API   = "https://api.anthropic.com/v1/messages"

KEYWORDS = {
    "Politics":     ["politics","election","president","congress","vote","government","senate","republican","democrat"],
    "Sports":       ["nfl","nba","mlb","nhl","soccer","championship","win","super bowl","world cup","playoff"],
    "Crypto":       ["bitcoin","ethereum","btc","eth","crypto","defi","token","blockchain"],
    "World Events": ["war","economy","climate","ai","tech","recession","gdp","rate","fed"],
}

class DB:
    def __init__(self, path="data/polyagent.db"):
        self.path = path
        with self._c() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, question TEXT,
                    category TEXT, side TEXT, price REAL, size REAL, confidence INTEGER,
                    fair_value REAL, reasoning TEXT, paper INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'open', outcome REAL, pnl REAL,
                    placed_at TEXT, resolved_at TEXT, order_id TEXT);
                CREATE TABLE IF NOT EXISTS strategy_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, version INTEGER,
                    notes TEXT, thresholds TEXT, win_rate REAL, total_trades INTEGER);
            """)

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def record(self, market, analysis, size, paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,category,side,price,size,confidence,fair_value,reasoning,paper,status,placed_at,order_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"],market["question"],market.get("category"),side,price,size,
                 analysis.get("confidence"),analysis.get("fair_value"),analysis.get("reasoning"),
                 1 if paper else 0,"open",now,order_id))
            return cur.lastrowid

    def open_trades(self):
        with self._c() as db: return [dict(r) for r in db.execute("SELECT * FROM trades WHERE status='open'").fetchall()]
    def get(self, tid):
        with self._c() as db:
            r = db.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
            return dict(r) if r else None
    def resolve(self, tid, won, payout):
        t = self.get(tid); pnl = payout - t["size"]
        with self._c() as db:
            db.execute("UPDATE trades SET status=?,outcome=?,pnl=?,resolved_at=? WHERE id=?",
                       ("won" if won else "lost",payout,pnl,datetime.now(timezone.utc).isoformat(),tid))
    def has_open(self, mid):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",(mid,)).fetchone() is not None
    def recent(self, n=10):
        with self._c() as db: return [dict(r) for r in db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]
    def stats(self):
        with self._c() as db:
            tot = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            op  = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lst = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()[0]
            inv = db.execute("SELECT SUM(size) FROM trades WHERE status='open'").fetchone()[0]
        res = won + lst
        return {"bot":BOT_NAME,"total_trades":tot,"open_positions":op,"won":won,"lost":lst,
                "win_rate":round((won/res*100) if res>0 else 0,1),"total_pnl":round(pnl or 0,2),
                "invested":round(inv or 0,2),"paper_mode":PAPER_MODE}

db_g = None

class API(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        if self.path == "/api/stats":
            s = db_g.stats(); t = db_g.recent(10)
            payload = {**s,"recent_trades":[{"question":x["question"][:60],"side":x["side"],"price":x["price"],
                "size":x["size"],"confidence":x["confidence"],"status":x["status"],"pnl":x["pnl"],"placed_at":x["placed_at"]} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else: self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

async def markets():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets",
                params={"active":"true","closed":"false","limit":50,"order":"volume","ascending":"false"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200: return []
                data = await r.json()
    except Exception as e: log.error(f"Fetch: {e}"); return []
    out = []
    for m in data:
        try:
            vol = float(m.get("volume") or 0); liq = float(m.get("liquidity") or 0)
            if vol < MIN_VOLUME or liq < MIN_LIQUIDITY: continue
            yp = 0.5
            try: yp = float(json.loads(m.get("outcomePrices") or "[0.5]")[0])
            except: pass
            if yp < 0.05 or yp > 0.95: continue
            txt = (m.get("question") or m.get("title") or "").lower()
            cat = "World Events"
            for c, kw in KEYWORDS.items():
                if any(k in txt for k in kw): cat = c; break
            out.append({"id":m.get("id") or m.get("conditionId"),"question":m.get("question") or m.get("title") or "Unknown",
                "yes_price":yp,"no_price":1-yp,"volume":vol,"liquidity":liq,"end_date":m.get("endDate"),"category":cat})
        except: continue
    return out[:MARKETS_PER_SCAN]

async def analyze(market, stats, ctx=""):
    if not ANTHROPIC_KEY: return None
    prompt = f"""Prediction market trading agent. Maximize profit.
Market: "{market['question']}"
YES: {market['yes_price']:.3f} | NO: {market['no_price']:.3f}
Volume: ${market['volume']:,.0f} | Category: {market['category']}
Stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% WR | ${stats['total_pnl']:.2f} P&L
{f"Strategy: {ctx}" if ctx else ""}
Respond ONLY with valid JSON:
{{"recommendation":"BUY_YES","confidence":78,"edge":"reason","reasoning":"2 sentences.","risk_level":"MEDIUM","fair_value":0.72,"suggested_size_pct":0.5}}"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":"claude-opus-4-6","max_tokens":300,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200: return None
                d = await r.json()
        txt = "".join(b.get("text","") for b in d.get("content",[]))
        res = json.loads(txt.replace("```json","").replace("```","").strip())
        assert res.get("recommendation") in ("BUY_YES","BUY_NO","HOLD")
        res["category"] = market.get("category"); return res
    except Exception as e: log.error(f"AI: {e}"); return None

async def trade(market, analysis, db):
    side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
    price = market["yes_price"] if side == "YES" else market["no_price"]
    size = round(MAX_TRADE * analysis.get("suggested_size_pct",1.0), 2)
    if db.stats()["open_positions"] >= MAX_POSITIONS: return False
    if PAPER_MODE:
        tid = db.record(market, analysis, size, paper=True)
        log.info(f"PAPER #{tid}: {side} '{market['question'][:50]}' @ {price:.3f} ${size}"); return True
    if not POLY_API_KEY: return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,"price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market, analysis, size, paper=False, order_id=d["orderId"]); return True
                return False
    except: return False

async def resolve(db):
    settled = []
    for t in db.open_trades():
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{GAMMA_API}/markets/{t['market_id']}",timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status!=200: continue
                    m = await r.json()
            if not m.get("closed") and not m.get("resolved"): continue
            w = m.get("winningOutcome","").upper()
            if not w: continue
            won = t["side"]==w; payout = t["size"]/t["price"] if won else 0.0
            db.resolve(t["id"],won,payout); settled.append({**t,"won":won})
        except: continue
    return settled

THRESH = {"Politics":75,"Sports":75,"Crypto":75,"World Events":75}
thresh_ctx = ""

async def main():
    global db_g
    db = DB(); db_g = db
    threading.Thread(target=api_thread, daemon=True).start()
    log.info(f"[{BOT_NAME}] Starting | Paper:{PAPER_MODE} | Port:{API_PORT}")
    cycle = 0
    while True:
        try:
            cycle += 1
            log.info(f"\n── Cycle #{cycle} ──")
            await resolve(db)
            mkts = await markets()
            log.info(f"Analyzing {len(mkts)} markets...")
            traded = 0
            for m in mkts:
                if db.has_open(m["id"]): continue
                s = db.stats()
                a = await analyze(m, s, thresh_ctx)
                if not a: continue
                log.info(f"  {m['question'][:50]} → {a['recommendation']} | {a['confidence']}%")
                if a["confidence"] >= THRESH.get(m["category"],75) and a["recommendation"] != "HOLD":
                    if await trade(m, a, db): traded += 1
                await asyncio.sleep(1)
            s = db.stats()
            log.info(f"Cycle #{cycle} — {traded} trades | WR:{s['win_rate']:.1f}% | P&L:${s['total_pnl']:.2f}")
        except Exception as e: log.error(f"Cycle error: {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
