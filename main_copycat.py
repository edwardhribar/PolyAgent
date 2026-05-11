"""
PolyAgent Bot C — Copycat
Mirrors trades from a target whale wallet on Polymarket.
Checks the wallet's positions every 2 minutes, detects new bets,
and mirrors the direction with our own sizing.
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

WALLET          = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
POLY_API_KEY    = os.getenv("POLYMARKET_API_KEY", "")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE      = os.getenv("PAPER_MODE", "true").lower() == "true"
MAX_TRADE       = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_POSITIONS   = int(os.getenv("MAX_OPEN_POSITIONS", "20"))
SCAN_INTERVAL   = int(os.getenv("SCAN_INTERVAL_SECS", "120"))
MIN_WHALE_SIZE  = float(os.getenv("MIN_WHALE_SIZE", "50"))  # Only copy bets where whale put in $50+
API_PORT        = int(os.getenv("PORT", "8080"))
BOT_NAME        = "Copycat"

# The whale wallet to follow
TARGET_WALLET   = os.getenv("TARGET_WALLET", "0xe1d6b51521bd4365769199f392f9818661bd907")

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
                    market_id TEXT, question TEXT, category TEXT,
                    side TEXT, price REAL, size REAL,
                    whale_size REAL, whale_address TEXT,
                    paper INTEGER DEFAULT 1, status TEXT DEFAULT 'open',
                    outcome REAL, pnl REAL,
                    placed_at TEXT, resolved_at TEXT,
                    order_id TEXT, end_date TEXT);
                CREATE TABLE IF NOT EXISTS seen_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, side TEXT, seen_at TEXT,
                    UNIQUE(market_id, side));
            """)

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def already_seen(self, market_id, side):
        with self._c() as db:
            return db.execute("SELECT id FROM seen_positions WHERE market_id=? AND side=?",
                              (market_id, side)).fetchone() is not None

    def mark_seen(self, market_id, side):
        with self._c() as db:
            try:
                db.execute("INSERT INTO seen_positions (market_id,side,seen_at) VALUES (?,?,?)",
                           (market_id, side, datetime.now(timezone.utc).isoformat()))
            except: pass

    def record(self, market, side, size, whale_size, paper=True, order_id=None):
        price = market.get("yes_price", 0.5) if side == "YES" else market.get("no_price", 0.5)
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,side,price,size,whale_size,whale_address,paper,status,placed_at,order_id,end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"], market.get("question","Unknown"), side, price, size,
                 whale_size, TARGET_WALLET, 1 if paper else 0, "open", now, order_id,
                 market.get("end_date","")))
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
                       ("won" if won else "lost", payout, pnl,
                        datetime.now(timezone.utc).isoformat(), tid))
        log.info(f"[{BOT_NAME}] #{tid} {'WON' if won else 'LOST'} P&L:${pnl:.2f}")
        return pnl

    def has_open(self, market_id):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",
                              (market_id,)).fetchone() is not None

    def recent(self, n=10):
        with self._c() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]

    def get_resolved(self, limit=50):
        with self._c() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades WHERE status IN ('won','lost') ORDER BY resolved_at DESC LIMIT ?",(limit,)).fetchall()]

    def daily_pnl(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._c() as db:
            row = db.execute("SELECT SUM(pnl) FROM trades WHERE resolved_at LIKE ? AND pnl IS NOT NULL",
                             (f"{today}%",)).fetchone()
        return round(row[0] or 0, 2)

    def stats(self):
        with self._c() as db:
            tot = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            op  = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lst = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()[0]
            inv = db.execute("SELECT SUM(size) FROM trades WHERE status='open'").fetchone()[0]
        res = won + lst
        return {
            "bot": BOT_NAME,
            "total_trades": tot, "open_positions": op,
            "won": won, "lost": lst,
            "win_rate": round((won/res*100) if res>0 else 0, 1),
            "total_pnl": round(pnl or 0, 2),
            "daily_pnl": self.daily_pnl(),
            "invested": round(inv or 0, 2),
            "paper_mode": PAPER_MODE,
            "target_wallet": TARGET_WALLET,
        }

db_g = None

class API(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        if self.path == "/api/stats":
            s = db_g.stats(); t = db_g.recent(10)
            payload = {**s, "recent_trades": [{
                "question": x["question"][:60], "side": x["side"],
                "price": x["price"], "size": x["size"],
                "whale_size": x.get("whale_size"), "status": x["status"],
                "pnl": x["pnl"], "placed_at": x["placed_at"],
                "end_date": x.get("end_date")} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else:
            self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

async def fetch_whale_positions():
    """Fetch current open positions for the target wallet - tries multiple endpoints."""
    endpoints = [
        (f"{DATA_API}/positions", {"user": TARGET_WALLET, "limit": 100}),
        (f"{DATA_API}/positions", {"maker": TARGET_WALLET, "limit": 100}),
        (f"{DATA_API}/activity", {"user": TARGET_WALLET, "limit": 100}),
    ]
    for url, params in endpoints:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params,
                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        log.debug(f"[{BOT_NAME}] {url} → {r.status}")
                        continue
                    data = await r.json()
                    positions = data if isinstance(data, list) else data.get("positions", data.get("data", []))
                    if positions:
                        log.info(f"[{BOT_NAME}] Whale has {len(positions)} positions via {url}")
                        return positions
        except Exception as e:
            log.debug(f"[{BOT_NAME}] Fetch error {url}: {e}")
            continue
    log.warning(f"[{BOT_NAME}] Could not fetch whale positions - will retry next cycle")
    return []

async def fetch_market_info(market_id):
    """Get market details from Gamma API."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets/{market_id}",
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return await r.json()
    except: pass

    # Fallback: search by condition ID
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets",
                params={"conditionId": market_id},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data and len(data) > 0:
                        return data[0]
    except: pass
    return None

async def do_trade(market, side, size, whale_size, db):
    """Place a paper or real trade."""
    if db.stats()["open_positions"] >= MAX_POSITIONS:
        log.info(f"[{BOT_NAME}] Max positions reached, skipping")
        return False

    if PAPER_MODE:
        tid = db.record(market, side, size, whale_size, paper=True)
        price = market.get("yes_price", 0.5) if side == "YES" else market.get("no_price", 0.5)
        log.info(f"[{BOT_NAME}] PAPER #{tid}: {side} '{market.get('question','?')[:50]}' @ {price:.3f} ${size} (whale bet ${whale_size:.0f})")
        return True

    if not POLY_API_KEY:
        log.warning(f"[{BOT_NAME}] No API key for live trading")
        return False

    try:
        price = market.get("yes_price", 0.5) if side == "YES" else market.get("no_price", 0.5)
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,
                      "price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market, side, size, whale_size, paper=False, order_id=d["orderId"])
                    return True
                return False
    except Exception as e:
        log.error(f"Trade error: {e}")
        return False

async def check_market_resolved(mid, session):
    """Try multiple methods to find if market resolved."""
    for params in [None, {"closed":"true"}, {"archived":"true"}]:
        try:
            url = f"{GAMMA_API}/markets/{mid}" if not params else f"{GAMMA_API}/markets"
            kwargs = {"params": {"id": mid, **params}} if params else {}
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8), **kwargs) as r:
                if r.status != 200: continue
                data = await r.json()
                market = data[0] if isinstance(data, list) and data else data
                if not isinstance(market, dict): continue
                is_resolved = (market.get("resolved") or market.get("closed") or
                               bool(market.get("winningOutcome")) or bool(market.get("resolutionTime")))
                if is_resolved:
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
                # Skip if not yet past end date
                end_date = trade.get("end_date","")
                if end_date:
                    try:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        if datetime.now(timezone.utc) < end_dt:
                            continue
                    except: pass

                market = await check_market_resolved(mid, session)
                if not market: continue

                winning = (market.get("winningOutcome") or "").upper().strip()
                if not winning:
                    try:
                        prices = json.loads(market.get("outcomePrices") or "[]")
                        outcomes = market.get("outcomes") or ["YES","NO"]
                        if prices:
                            float_prices = [float(p) for p in prices]
                            max_val = max(float_prices)
                            if max_val > 0.95:
                                max_idx = float_prices.index(max_val)
                                winning = str(outcomes[max_idx]).upper()
                    except: pass

                if not winning: continue

                won = trade["side"].upper() == winning
                payout = trade["size"] / trade["price"] if won else 0.0
                pnl = db.resolve(trade["id"], won, payout)
                settled.append({**trade,"won":won,"payout":payout,"pnl":pnl})
                log.info(f"[{BOT_NAME}] ✓ {'WIN' if won else 'LOSS'} ${pnl:.2f} — {trade['question'][:50]}")
            except Exception as e:
                log.debug(f"Resolution check failed {mid}: {e}")
    return settled

async def main():
    global db_g
    db = DB(); db_g = db
    threading.Thread(target=api_thread, daemon=True).start()

    log.info(f"[{BOT_NAME}] Starting | Paper:{PAPER_MODE} | Port:{API_PORT}")
    log.info(f"Target wallet: {TARGET_WALLET}")
    log.info(f"Min whale bet size to copy: ${MIN_WHALE_SIZE}")
    log.info(f"Our bet size: ${MAX_TRADE} | Scan every: {SCAN_INTERVAL}s")

    cycle = 0
    while True:
        try:
            cycle += 1
            log.info(f"\n── Cycle #{cycle} ──")

            # Resolve settled trades
            settled = await resolve_settled(db)
            if settled:
                log.info(f"[{BOT_NAME}] Resolved {len(settled)} trades!")

            # Fetch whale positions
            positions = await fetch_whale_positions()
            new_copies = 0

            for pos in positions:
                try:
                    # Extract position details
                    # Data API returns positions with asset/token info
                    market_id = pos.get("conditionId") or pos.get("market") or pos.get("marketId") or ""
                    if not market_id:
                        # Try to get from asset
                        asset = pos.get("asset", {})
                        market_id = asset.get("conditionId") or asset.get("market") or ""

                    if not market_id:
                        continue

                    # Determine side
                    outcome = (pos.get("outcome") or pos.get("side") or "YES").upper()
                    side = "YES" if "YES" in outcome or outcome == "1" else "NO"

                    # Get size of whale's position
                    whale_size = float(pos.get("size") or pos.get("value") or pos.get("currentValue") or 0)
                    if whale_size < MIN_WHALE_SIZE:
                        log.debug(f"Skipping small position ${whale_size:.0f} on {market_id[:20]}")
                        continue

                    # Skip if already seen or already have open position
                    if db.already_seen(market_id, side):
                        continue
                    if db.has_open(market_id):
                        db.mark_seen(market_id, side)
                        continue

                    # Get market details
                    market = await fetch_market_info(market_id)
                    if not market:
                        log.debug(f"Could not fetch market {market_id[:20]}")
                        db.mark_seen(market_id, side)
                        continue

                    # Skip if market is already closed
                    if market.get("closed") or market.get("resolved"):
                        db.mark_seen(market_id, side)
                        continue

                    # Get current price
                    yes_price = 0.5
                    try:
                        yes_price = float(json.loads(market.get("outcomePrices") or "[0.5]")[0])
                    except: pass

                    market["yes_price"] = yes_price
                    market["no_price"] = 1 - yes_price
                    market["id"] = market_id

                    question = market.get("question") or market.get("title") or "Unknown market"
                    price = yes_price if side == "YES" else 1 - yes_price

                    log.info(f"[{BOT_NAME}] 🐋 New whale position detected!")
                    log.info(f"  Market: {question[:60]}")
                    log.info(f"  Side: {side} @ {price:.3f} | Whale size: ${whale_size:.0f}")

                    # Mirror the trade
                    if await do_trade(market, side, MAX_TRADE, whale_size, db):
                        new_copies += 1

                    db.mark_seen(market_id, side)
                    await asyncio.sleep(0.5)

                except Exception as e:
                    log.debug(f"Position processing error: {e}")
                    continue

            s = db.stats()
            log.info(f"Cycle #{cycle} — {new_copies} copied | Open:{s['open_positions']} | Today:${s['daily_pnl']:.2f} | Total:${s['total_pnl']:.2f} | WR:{s['win_rate']:.1f}%")

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
