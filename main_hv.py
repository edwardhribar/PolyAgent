"""
PolyAgent HV Bot + Live API Server
Two-tier: price monitor every 5s + AI deep scan every 30s
Serves real trade data at /api/stats
"""

import os, json, logging, asyncio, aiohttp, sqlite3, threading
from datetime import datetime, timezone
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/hv.log"), logging.StreamHandler()])
log = logging.getLogger("polyagent.hv")

WALLET          = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
POLY_API_KEY    = os.getenv("POLYMARKET_API_KEY", "")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
MAX_TRADE       = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_POSITIONS   = int(os.getenv("MAX_OPEN_POSITIONS", "25"))
MIN_LIQUIDITY   = float(os.getenv("MIN_LIQUIDITY", "2000"))
MIN_VOLUME      = float(os.getenv("MIN_VOLUME", "5000"))
CONFIDENCE_MIN  = int(os.getenv("BASE_CONFIDENCE", "55"))
PRICE_SCAN_SECS = int(os.getenv("PRICE_SCAN_SECS", "5"))
DEEP_SCAN_SECS  = int(os.getenv("DEEP_SCAN_SECS", "30"))
MARKETS_PER_SCAN= int(os.getenv("MARKETS_PER_SCAN", "50"))
PRICE_SPIKE_PCT = float(os.getenv("PRICE_SPIKE_PCT", "3.0"))
MAX_AI_PER_MIN  = int(os.getenv("MAX_AI_PER_MIN", "20"))
API_PORT        = int(os.getenv("PORT", "8080"))
BOT_NAME        = "HV"

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
ANT_API   = "https://api.anthropic.com/v1/messages"

KEYWORDS = {
    "Politics":     ["politics","election","president","congress","vote","government","senate"],
    "Sports":       ["nfl","nba","mlb","nhl","soccer","championship","win","world cup","playoff"],
    "Crypto":       ["bitcoin","ethereum","btc","eth","crypto","defi","token","blockchain"],
    "World Events": ["war","economy","climate","ai","tech","recession","gdp","rate","fed"],
}

class DB:
    def __init__(self, path="data/hv.db"):
        self.path = path
        with self._c() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, question TEXT,
                    category TEXT, side TEXT, price REAL, size REAL, confidence INTEGER,
                    fair_value REAL, reasoning TEXT, trigger TEXT, paper INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'open', outcome REAL, pnl REAL,
                    placed_at TEXT, resolved_at TEXT, order_id TEXT);
                CREATE TABLE IF NOT EXISTS prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, yes_price REAL, recorded_at TEXT);
            """)

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def record(self, market, analysis, size, trigger="deep_scan", paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,category,side,price,size,confidence,fair_value,reasoning,trigger,paper,status,placed_at,order_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"],market["question"],market.get("category"),side,price,size,
                 analysis.get("confidence"),analysis.get("fair_value"),analysis.get("reasoning"),
                 trigger,1 if paper else 0,"open",now,order_id))
            return cur.lastrowid

    def rec_price(self, mid, yp):
        with self._c() as db:
            db.execute("INSERT INTO prices (market_id,yes_price,recorded_at) VALUES (?,?,?)",
                       (mid,yp,datetime.now(timezone.utc).isoformat()))

    def last_price(self, mid):
        with self._c() as db:
            r = db.execute("SELECT yes_price FROM prices WHERE market_id=? ORDER BY id DESC LIMIT 1",(mid,)).fetchone()
            return r["yes_price"] if r else None

    def open_trades(self):
        with self._c() as db: return [dict(r) for r in db.execute("SELECT * FROM trades WHERE status='open'").fetchall()]

    def get(self, tid):
        with self._c() as db:
            r = db.execute("SELECT * FROM trades WHERE id=?",(tid,)).fetchone()
            return dict(r) if r else None

    def resolve(self, tid, won, payout):
        t = self.get(tid)
        with self._c() as db:
            db.execute("UPDATE trades SET status=?,outcome=?,pnl=?,resolved_at=? WHERE id=?",
                       ("won" if won else "lost",payout,payout-t["size"],datetime.now(timezone.utc).isoformat(),tid))

    def has_open(self, mid):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",(mid,)).fetchone() is not None

    def recent(self, n=10):
        with self._c() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]

    def stats(self):
        with self._c() as db:
            tot = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            op  = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lst = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()[0]
            inv = db.execute("SELECT SUM(size) FROM trades WHERE status='open'").fetchone()[0]
            spk = db.execute("SELECT COUNT(*) FROM trades WHERE trigger='price_spike'").fetchone()[0]
        res = won+lst
        return {"bot":BOT_NAME,"total_trades":tot,"open_positions":op,"won":won,"lost":lst,
                "win_rate":round((won/res*100) if res>0 else 0,1),"total_pnl":round(pnl or 0,2),
                "invested":round(inv or 0,2),"spike_trades":spk,"paper_mode":PAPER_MODE}

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
                "size":x["size"],"confidence":x["confidence"],"status":x["status"],"pnl":x["pnl"],
                "placed_at":x["placed_at"],"trigger":x.get("trigger")} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else: self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

async def fetch_markets():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets",
                params={"active":"true","closed":"false","limit":MARKETS_PER_SCAN,"order":"volume","ascending":"false"},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status!=200: return []
                data = await r.json()
    except: return []
    out = []
    for m in data:
        try:
            vol=float(m.get("volume") or 0); liq=float(m.get("liquidity") or 0)
            if vol<MIN_VOLUME or liq<MIN_LIQUIDITY: continue
            yp=0.5
            try: yp=float(json.loads(m.get("outcomePrices") or "[0.5]")[0])
            except: pass
            if yp<0.04 or yp>0.96: continue
            txt=(m.get("question") or m.get("title") or "").lower()
            cat="World Events"
            for c,kw in KEYWORDS.items():
                if any(k in txt for k in kw): cat=c; break
            out.append({"id":m.get("id") or m.get("conditionId"),"question":m.get("question") or m.get("title") or "Unknown",
                "yes_price":yp,"no_price":1-yp,"volume":vol,"liquidity":liq,"end_date":m.get("endDate"),"category":cat})
        except: continue
    return out

async def analyze(market, stats, ctx="", trigger="deep_scan"):
    if not ANTHROPIC_KEY: return None
    urg = "URGENT - price spiked, act fast" if trigger=="price_spike" else "routine scan"
    prompt = f"""Aggressive prediction market trader. Maximize trades and P&L.
Market: "{market['question']}"
YES: {market['yes_price']:.3f} | NO: {market['no_price']:.3f}
Volume: ${market['volume']:,.0f} | Trigger: {urg}
Stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% WR | ${stats['total_pnl']:.2f} P&L
Be aggressive. Bias toward trading. Only HOLD if truly 50/50.
Respond ONLY with valid JSON:
{{"recommendation":"BUY_YES","confidence":68,"edge":"brief","reasoning":"1-2 sentences.","risk_level":"MEDIUM","fair_value":0.65,"suggested_size_pct":0.75}}"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":"claude-opus-4-6","max_tokens":300,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status!=200: return None
                d = await r.json()
        txt="".join(b.get("text","") for b in d.get("content",[]))
        res=json.loads(txt.replace("```json","").replace("```","").strip())
        assert res.get("recommendation") in ("BUY_YES","BUY_NO","HOLD")
        res["category"]=market.get("category"); return res
    except: return None

async def do_trade(market, analysis, db, trigger="deep_scan"):
    side="YES" if analysis["recommendation"]=="BUY_YES" else "NO"
    price=market["yes_price"] if side=="YES" else market["no_price"]
    size=round(MAX_TRADE*analysis.get("suggested_size_pct",1.0),2)
    if db.stats()["open_positions"]>=MAX_POSITIONS: return False
    if PAPER_MODE:
        tid=db.record(market,analysis,size,trigger=trigger,paper=True)
        log.info(f"[HV] PAPER #{tid} [{trigger}]: {side} '{market['question'][:45]}' @ {price:.3f} ${size}"); return True
    if not POLY_API_KEY: return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,"price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                d=await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market,analysis,size,trigger=trigger,paper=False,order_id=d["orderId"]); return True
                return False
    except: return False

async def resolve(db):
    settled=[]
    for t in db.open_trades():
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{GAMMA_API}/markets/{t['market_id']}",timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status!=200: continue
                    m=await r.json()
            if not m.get("closed") and not m.get("resolved"): continue
            w=m.get("winningOutcome","").upper()
            if not w: continue
            won=t["side"]==w; payout=t["size"]/t["price"] if won else 0.0
            db.resolve(t["id"],won,payout); settled.append({**t,"won":won})
        except: continue
    return settled

THRESH={"Politics":55,"Sports":55,"Crypto":55,"World Events":55}

async def main():
    global db_g
    db=DB(); db_g=db
    threading.Thread(target=api_thread,daemon=True).start()
    log.info(f"[HV] Starting | Paper:{PAPER_MODE} | Conf:{CONFIDENCE_MIN}%+ | Port:{API_PORT}")

    price_cache={}; ai_pm=0; ai_win=asyncio.get_event_loop().time()
    deep_due=0; resolve_due=0
    markets=await fetch_markets()
    log.info(f"[HV] Loaded {len(markets)} markets")

    while True:
        now=asyncio.get_event_loop().time()
        if now-ai_win>=60: ai_pm=0; ai_win=now

        spikes=[]
        for m in markets:
            mid=m["id"]; cur=m["yes_price"]
            last=price_cache.get(mid)
            if last is not None:
                mv=abs(cur-last)/last*100
                if mv>=PRICE_SPIKE_PCT: spikes.append((m,mv)); log.info(f"[HV] SPIKE {mv:.1f}% '{m['question'][:45]}'")
            price_cache[mid]=cur; db.rec_price(mid,cur)

        for m,mv in spikes:
            if db.has_open(m["id"]) or ai_pm>=MAX_AI_PER_MIN: continue
            s=db.stats(); a=await analyze(m,s,"","price_spike"); ai_pm+=1
            if not a: continue
            if a["confidence"]>=THRESH.get(m["category"],55) and a["recommendation"]!="HOLD":
                await do_trade(m,a,db,"price_spike")

        if now>=deep_due:
            deep_due=now+DEEP_SCAN_SECS
            markets=await fetch_markets(); traded=0
            for m in markets:
                if db.has_open(m["id"]) or ai_pm>=MAX_AI_PER_MIN: continue
                s=db.stats(); a=await analyze(m,s,"","deep_scan"); ai_pm+=1
                if not a: continue
                log.info(f"[HV] {m['question'][:50]} → {a['recommendation']} | {a['confidence']}%")
                if a["confidence"]>=THRESH.get(m["category"],55) and a["recommendation"]!="HOLD":
                    if await do_trade(m,a,db,"deep_scan"): traded+=1
                await asyncio.sleep(0.3)
            s=db.stats()
            log.info(f"[HV] Deep scan — {traded} trades | WR:{s['win_rate']:.1f}% | P&L:${s['total_pnl']:.2f}")

        if now>=resolve_due:
            resolve_due=now+300; await resolve(db)

        await asyncio.sleep(PRICE_SCAN_SECS)

if __name__=="__main__":
    asyncio.run(main())
