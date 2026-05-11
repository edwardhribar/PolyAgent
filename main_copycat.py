"""
PolyAgent Bot C — Copycat v2
Mirrors trades from a target whale wallet on Polymarket.
Uses correct Data API endpoint: data-api.polymarket.com/positions?user=WALLET
"""

import os, json, logging, asyncio, aiohttp, sqlite3, threading
from datetime import datetime, timezone
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/copycat.log"), logging.StreamHandler()])
log = logging.getLogger("polyagent.copycat")

WALLET         = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
POLY_API_KEY   = os.getenv("POLYMARKET_API_KEY", "")
PAPER_MODE     = os.getenv("PAPER_MODE", "true").lower() == "true"
MAX_TRADE      = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_POSITIONS  = int(os.getenv("MAX_OPEN_POSITIONS", "20"))
SCAN_INTERVAL  = int(os.getenv("SCAN_INTERVAL_SECS", "120"))
MIN_WHALE_SIZE = float(os.getenv("MIN_WHALE_SIZE", "50"))
API_PORT       = int(os.getenv("PORT", "8080"))
BOT_NAME       = "Copycat"
TARGET_WALLET  = os.getenv("TARGET_WALLET", "0xe1D6b51521Bd4365769199f392F9818661BD907")

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"

class DB:
    def __init__(self, path="data/copycat.db"):
        self.path = path
        with self._c() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, question TEXT, side TEXT,
                    price REAL, size REAL, whale_size REAL,
                    paper INTEGER DEFAULT 1, status TEXT DEFAULT 'open',
                    outcome REAL, pnl REAL,
                    placed_at TEXT, resolved_at TEXT, order_id TEXT, end_date TEXT);
                CREATE TABLE IF NOT EXISTS seen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, side TEXT, seen_at TEXT,
                    UNIQUE(market_id, side));
            """)

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def already_seen(self, mid, side):
        with self._c() as db:
            return db.execute("SELECT id FROM seen WHERE market_id=? AND side=?",(mid,side)).fetchone() is not None

    def mark_seen(self, mid, side):
        with self._c() as db:
            try: db.execute("INSERT INTO seen (market_id,side,seen_at) VALUES (?,?,?)",
                            (mid,side,datetime.now(timezone.utc).isoformat()))
            except: pass

    def record(self, market, side, size, whale_size, paper=True, order_id=None):
        price = market.get("yes_price",0.5) if side=="YES" else market.get("no_price",0.5)
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,side,price,size,whale_size,paper,status,placed_at,order_id,end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"],market.get("question","Unknown"),side,price,size,whale_size,
                 1 if paper else 0,"open",now,order_id,market.get("end_date","")))
            return cur.lastrowid

    def open_trades(self):
        with self._c() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades WHERE status='open'").fetchall()]

    def get(self, tid):
        with self._c() as db:
            r = db.execute("SELECT * FROM trades WHERE id=?",(tid,)).fetchone()
            return dict(r) if r else None

    def resolve(self, tid, won, payout):
        t = self.get(tid); pnl = payout - t["size"]
        with self._c() as db:
            db.execute("UPDATE trades SET status=?,outcome=?,pnl=?,resolved_at=? WHERE id=?",
                       ("won" if won else "lost",payout,pnl,datetime.now(timezone.utc).isoformat(),tid))
        log.info(f"[{BOT_NAME}] #{tid} {'WON' if won else 'LOST'} P&L:${pnl:.2f}"); return pnl

    def has_open(self, mid):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",(mid,)).fetchone() is not None

    def recent(self, n=10):
        with self._c() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]

    def daily_pnl(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._c() as db:
            row = db.execute("SELECT SUM(pnl) FROM trades WHERE resolved_at LIKE ? AND pnl IS NOT NULL",(f"{today}%",)).fetchone()
        return round(row[0] or 0, 2)

    def stats(self):
        with self._c() as db:
            tot = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            op  = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lst = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()[0]
            inv = db.execute("SELECT SUM(size) FROM trades WHERE status='open'").fetchone()[0]
        res = won+lst
        return {"bot":BOT_NAME,"total_trades":tot,"open_positions":op,"won":won,"lost":lst,
                "win_rate":round((won/res*100) if res>0 else 0,1),"total_pnl":round(pnl or 0,2),
                "daily_pnl":self.daily_pnl(),"invested":round(inv or 0,2),"paper_mode":PAPER_MODE,
                "target_wallet":TARGET_WALLET}

db_g = None

class API(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        if self.path == "/api/stats":
            s = db_g.stats(); t = db_g.recent(10)
            payload = {**s,"recent_trades":[{"question":x["question"][:60],"side":x["side"],
                "price":x["price"],"size":x["size"],"whale_size":x.get("whale_size"),
                "status":x["status"],"pnl":x["pnl"],"placed_at":x["placed_at"],
                "end_date":x.get("end_date")} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else: self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

async def fetch_whale_positions():
    """
    Fetch positions for target wallet using Polymarket Data API.
    Returns list of position objects with conditionId, outcome, currentValue etc.
    """
    # Try positions endpoint first (current open holdings)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DATA_API}/positions",
                params={"user": TARGET_WALLET, "limit": 100, "sizeThreshold": "1"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                log.info(f"[{BOT_NAME}] Positions API → {r.status}")
                if r.status == 200:
                    data = await r.json()
                    positions = data if isinstance(data, list) else []
                    log.info(f"[{BOT_NAME}] Whale has {len(positions)} open positions")
                    return positions, "positions"
    except Exception as e:
        log.debug(f"Positions error: {e}")

    # Fallback: activity endpoint (recent trades)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DATA_API}/activity",
                params={"user": TARGET_WALLET, "limit": 50, "type": "TRADE", "side": "BUY"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                log.info(f"[{BOT_NAME}] Activity API → {r.status}")
                if r.status == 200:
                    data = await r.json()
                    trades = data if isinstance(data, list) else []
                    log.info(f"[{BOT_NAME}] Got {len(trades)} recent whale trades")
                    return trades, "activity"
    except Exception as e:
        log.debug(f"Activity error: {e}")

    log.warning(f"[{BOT_NAME}] Could not fetch whale data")
    return [], "none"

async def fetch_market_info(market_id):
    """Get market details from Gamma API."""
    for params in [None, {"conditionId": market_id}]:
        try:
            async with aiohttp.ClientSession() as s:
                url = f"{GAMMA_API}/markets/{market_id}" if not params else f"{GAMMA_API}/markets"
                kwargs = {"params": params} if params else {}
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10), **kwargs) as r:
                    if r.status != 200: continue
                    data = await r.json()
                    market = data[0] if isinstance(data, list) and data else data
                    if isinstance(market, dict) and market.get("question"):
                        return market
        except: continue
    return None

def kelly_size(price, fair_value, max_bet):
    try:
        p = max(0.05, min(0.95, float(fair_value)))
        b = (1/price) - 1
        if b <= 0: return round(max_bet*0.25, 2)
        full_kelly = (b*p - (1-p)) / b
        half_kelly = full_kelly / 2
        fraction = max(0.25, min(1.0, half_kelly))
        return max(round(max_bet*fraction, 2), round(max_bet*0.25, 2))
    except:
        return round(max_bet*0.5, 2)

async def do_trade(market, side, size, whale_size, db):
    if db.stats()["open_positions"] >= MAX_POSITIONS: return False
    if PAPER_MODE:
        tid = db.record(market, side, size, whale_size, paper=True)
        price = market.get("yes_price",0.5) if side=="YES" else market.get("no_price",0.5)
        log.info(f"[{BOT_NAME}] 🐋 PAPER #{tid}: {side} '{market.get('question','?')[:50]}' @ {price:.3f} ${size} (whale:${whale_size:.0f})")
        return True
    if not POLY_API_KEY: return False
    try:
        price = market.get("yes_price",0.5) if side=="YES" else market.get("no_price",0.5)
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,
                      "price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market,side,size,whale_size,paper=False,order_id=d["orderId"]); return True
                return False
    except Exception as e:
        log.error(f"Trade error: {e}"); return False

async def process_positions(positions, db):
    """Process positions-style data (current open holdings)."""
    copied = 0
    for pos in positions:
        try:
            market_id = pos.get("conditionId") or pos.get("market") or ""
            if not market_id: continue

            outcome = (pos.get("outcome") or "YES").upper()
            side = "YES" if "YES" in outcome or outcome in ["YES","1","0"] else "NO"
            if "No" in pos.get("outcome","") or outcome == "NO": side = "NO"

            current_value = float(pos.get("currentValue") or pos.get("initialValue") or 0)
            if current_value < MIN_WHALE_SIZE: continue
            if db.already_seen(market_id, side): continue
            if db.has_open(market_id): db.mark_seen(market_id, side); continue

            market = await fetch_market_info(market_id)
            if not market or market.get("closed") or market.get("resolved"):
                db.mark_seen(market_id, side); continue

            yp = 0.5
            try: yp = float(json.loads(market.get("outcomePrices") or "[0.5]")[0])
            except: pass
            market["yes_price"] = yp
            market["no_price"] = 1 - yp
            market["id"] = market_id

            price = yp if side=="YES" else 1-yp
            size = kelly_size(price, pos.get("curPrice", price), MAX_TRADE)

            log.info(f"[{BOT_NAME}] 🐋 Whale: {side} '{market.get('question','?')[:55]}' worth ${current_value:.0f}")
            if await do_trade(market, side, size, current_value, db):
                copied += 1
            db.mark_seen(market_id, side)
            await asyncio.sleep(0.5)
        except Exception as e:
            log.debug(f"Position processing error: {e}")
    return copied

async def process_activity(activity, db):
    """Process activity-style data (recent trade events)."""
    copied = 0
    for trade in activity:
        try:
            market_id = trade.get("conditionId") or trade.get("market") or ""
            if not market_id: continue

            side_raw = (trade.get("outcome") or trade.get("side") or "YES").upper()
            side = "YES" if "YES" in side_raw else "NO"

            size_usd = float(trade.get("usdcSize") or trade.get("size") or 0)
            if size_usd < MIN_WHALE_SIZE: continue
            if db.already_seen(market_id, side): continue
            if db.has_open(market_id): db.mark_seen(market_id, side); continue

            market = await fetch_market_info(market_id)
            if not market or market.get("closed") or market.get("resolved"):
                db.mark_seen(market_id, side); continue

            yp = 0.5
            try: yp = float(json.loads(market.get("outcomePrices") or "[0.5]")[0])
            except: pass
            market["yes_price"] = yp
            market["no_price"] = 1 - yp
            market["id"] = market_id

            price = yp if side=="YES" else 1-yp
            whale_price = float(trade.get("price") or price)
            size = kelly_size(price, whale_price, MAX_TRADE)

            log.info(f"[{BOT_NAME}] 🐋 Whale trade: {side} '{market.get('question','?')[:55]}' ${size_usd:.0f}")
            if await do_trade(market, side, size, size_usd, db):
                copied += 1
            db.mark_seen(market_id, side)
            await asyncio.sleep(0.5)
        except Exception as e:
            log.debug(f"Activity processing error: {e}")
    return copied

async def check_market_resolved(mid, session):
    for params in [None, {"closed":"true"}, {"archived":"true"}]:
        try:
            url = f"{GAMMA_API}/markets/{mid}" if not params else f"{GAMMA_API}/markets"
            kwargs = {"params": {"id": mid, **params}} if params else {}
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8), **kwargs) as r:
                if r.status != 200: continue
                data = await r.json()
                market = data[0] if isinstance(data, list) and data else data
                if not isinstance(market, dict): continue
                if market.get("resolved") or market.get("closed") or bool(market.get("winningOutcome")):
                    return market
        except: continue
    return None

async def resolve_settled(db):
    open_trades = db.open_trades()
    if not open_trades: return []
    settled = []
    async with aiohttp.ClientSession() as session:
        for trade in open_trades:
            mid = trade["market_id"]
            try:
                end_date = trade.get("end_date","")
                if end_date:
                    try:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        if datetime.now(timezone.utc) < end_dt: continue
                    except: pass
                market = await check_market_resolved(mid, session)
                if not market: continue
                winning = (market.get("winningOutcome") or "").upper().strip()
                if not winning:
                    try:
                        prices = json.loads(market.get("outcomePrices") or "[]")
                        outcomes = market.get("outcomes") or ["YES","NO"]
                        if prices:
                            fp = [float(p) for p in prices]
                            mv = max(fp)
                            if mv > 0.95:
                                winning = str(outcomes[fp.index(mv)]).upper()
                    except: pass
                if not winning: continue
                won = trade["side"].upper() == winning
                payout = trade["size"]/trade["price"] if won else 0.0
                pnl = db.resolve(trade["id"], won, payout)
                settled.append({**trade,"won":won,"payout":payout,"pnl":pnl})
            except Exception as e:
                log.debug(f"Resolution check failed {mid}: {e}")
    return settled

async def main():
    global db_g
    db = DB(); db_g = db
    threading.Thread(target=api_thread, daemon=True).start()
    log.info(f"[{BOT_NAME}] Starting v2 | Paper:{PAPER_MODE} | Port:{API_PORT}")
    log.info(f"Target wallet: {TARGET_WALLET}")
    log.info(f"Min whale size: ${MIN_WHALE_SIZE} | Our bet: ${MAX_TRADE} | Scan: {SCAN_INTERVAL}s")

    cycle = 0
    while True:
        try:
            cycle += 1
            log.info(f"\n── Cycle #{cycle} ──")

            settled = await resolve_settled(db)
            if settled:
                log.info(f"[{BOT_NAME}] Resolved {len(settled)} trades!")

            data, source = await fetch_whale_positions()

            if source == "positions" and data:
                copied = await process_positions(data, db)
            elif source == "activity" and data:
                copied = await process_activity(data, db)
            else:
                copied = 0

            s = db.stats()
            log.info(f"Cycle #{cycle} — {copied} copied | Open:{s['open_positions']} | Today:${s['daily_pnl']:.2f} | Total:${s['total_pnl']:.2f} | WR:{s['win_rate']:.1f}%")

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
