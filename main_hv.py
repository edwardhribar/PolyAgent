"""
PolyAgent HV Bot v5
- Fixed resolution: checks closed/archived markets properly
- Handles 5-minute crypto markets and other short-term markets
- Daily P&L tracking
- Self-improving every 20 resolved trades
"""

import os, json, logging, asyncio, aiohttp, sqlite3, threading
from datetime import datetime, timezone, timedelta
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
MIN_LIQUIDITY   = float(os.getenv("MIN_LIQUIDITY", "500"))
MIN_VOLUME      = float(os.getenv("MIN_VOLUME", "1000"))
CONFIDENCE_MIN  = int(os.getenv("BASE_CONFIDENCE", "40"))
PRICE_SCAN_SECS = int(os.getenv("PRICE_SCAN_SECS", "5"))
DEEP_SCAN_SECS  = int(os.getenv("DEEP_SCAN_SECS", "30"))
MARKETS_PER_SCAN= int(os.getenv("MARKETS_PER_SCAN", "50"))
PRICE_SPIKE_PCT = float(os.getenv("PRICE_SPIKE_PCT", "3.0"))
MAX_AI_PER_MIN  = int(os.getenv("MAX_AI_PER_MIN", "5"))
MAX_HOURS       = int(os.getenv("MAX_HOURS_TO_CLOSE", "72"))
IMPROVE_EVERY   = int(os.getenv("IMPROVE_EVERY", "20"))
API_PORT        = int(os.getenv("PORT", "8080"))
BOT_NAME        = "HV"
MODEL           = "claude-sonnet-4-6"

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
ANT_API   = "https://api.anthropic.com/v1/messages"

KEYWORDS = {
    "Politics":     ["politics","election","president","congress","vote","government","senate","republican","democrat"],
    "Sports":       ["nfl","nba","mlb","nhl","soccer","championship","win","world cup","playoff","relegated","tonight","game","match","league","golden knights","ducks","lakers","celtics"],
    "Crypto":       ["bitcoin","ethereum","btc","eth","crypto","price","above","below","reach","up or down"],
    "World Events": ["war","economy","climate","ai","tech","recession","gdp","rate","fed"],
}

class DB:
    def __init__(self, path="data/hv.db"):
        self.path = path
        with self._c() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, question TEXT, category TEXT,
                    side TEXT, price REAL, size REAL, confidence INTEGER,
                    fair_value REAL, reasoning TEXT, trigger TEXT,
                    paper INTEGER DEFAULT 1, status TEXT DEFAULT 'open',
                    outcome REAL, pnl REAL, placed_at TEXT,
                    resolved_at TEXT, order_id TEXT, end_date TEXT);
                CREATE TABLE IF NOT EXISTS prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, yes_price REAL, recorded_at TEXT);
                CREATE TABLE IF NOT EXISTS strategy_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, version INTEGER, notes TEXT,
                    thresholds TEXT, win_rate REAL, total_trades INTEGER, strategy TEXT);
            """)

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def record(self, market, analysis, size, trigger="deep_scan", paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,category,side,price,size,confidence,fair_value,reasoning,trigger,paper,status,placed_at,order_id,end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"],market["question"],market.get("category"),side,price,size,
                 analysis.get("confidence"),analysis.get("fair_value"),analysis.get("reasoning"),
                 trigger,1 if paper else 0,"open",now,order_id,market.get("end_date")))
            return cur.lastrowid

    def rec_price(self, mid, yp):
        with self._c() as db:
            db.execute("INSERT INTO prices (market_id,yes_price,recorded_at) VALUES (?,?,?)",
                       (mid,yp,datetime.now(timezone.utc).isoformat()))

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
        log.info(f"[{BOT_NAME}] #{tid} {'WON' if won else 'LOST'} P&L:${pnl:.2f}")
        return pnl

    def has_open(self, mid):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",(mid,)).fetchone() is not None

    def recent(self, n=10):
        with self._c() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]

    def get_resolved(self, limit=50):
        with self._c() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades WHERE status IN ('won','lost') ORDER BY resolved_at DESC LIMIT ?",(limit,)).fetchall()]

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
            spk = db.execute("SELECT COUNT(*) FROM trades WHERE trigger='price_spike'").fetchone()[0]
        res = won + lst
        return {"bot":BOT_NAME,"total_trades":tot,"open_positions":op,"won":won,"lost":lst,
                "win_rate":round((won/res*100) if res>0 else 0,1),"total_pnl":round(pnl or 0,2),
                "daily_pnl":self.daily_pnl(),"invested":round(inv or 0,2),
                "spike_trades":spk,"paper_mode":PAPER_MODE}

    def save_strategy(self, version, notes, thresholds, win_rate, total_trades, strategy_text):
        with self._c() as db:
            db.execute("INSERT INTO strategy_log (timestamp,version,notes,thresholds,win_rate,total_trades,strategy) VALUES (?,?,?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(),version,notes,json.dumps(thresholds),win_rate,total_trades,strategy_text))

    def get_latest_strategy(self):
        with self._c() as db:
            row = db.execute("SELECT * FROM strategy_log ORDER BY version DESC LIMIT 1").fetchone()
            return dict(row) if row else None

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
                "price":x["price"],"size":x["size"],"confidence":x["confidence"],
                "status":x["status"],"pnl":x["pnl"],"placed_at":x["placed_at"],
                "trigger":x.get("trigger"),"end_date":x.get("end_date")} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else: self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

async def fetch_markets():
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=MAX_HOURS)
    params = {
        "active": "true",
        "closed": "false",
        "limit": 100,
        "order": "endDate",
        "ascending": "true",
        "end_date_min": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date_max": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets", params=params,
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: return []
                data = await r.json()
    except Exception as e:
        log.debug(f"Fetch: {e}"); return []

    out = []
    for m in data:
        try:
            vol = float(m.get("volume") or 0)
            liq = float(m.get("liquidity") or 0)
            if vol < MIN_VOLUME or liq < MIN_LIQUIDITY: continue
            yp = 0.5
            try: yp = float(json.loads(m.get("outcomePrices") or "[0.5]")[0])
            except: pass
            if yp < 0.04 or yp > 0.96: continue
            txt = (m.get("question") or m.get("title") or "").lower()
            cat = "World Events"
            for c, kw in KEYWORDS.items():
                if any(k in txt for k in kw): cat = c; break
            out.append({
                "id": m.get("id") or m.get("conditionId"),
                "question": m.get("question") or m.get("title") or "Unknown",
                "yes_price": yp, "no_price": 1-yp,
                "volume": vol, "liquidity": liq,
                "end_date": m.get("endDate",""), "category": cat,
            })
        except: continue

    log.info(f"[{BOT_NAME}] Found {len(out)} markets closing within {MAX_HOURS}hrs")
    return out[:MARKETS_PER_SCAN]

async def analyze(market, stats, strategy_context="", trigger="deep_scan"):
    if not ANTHROPIC_KEY: return None
    hours_left = "unknown"
    try:
        end_dt = datetime.strptime(market["end_date"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        diff = end_dt - datetime.now(timezone.utc)
        hours_left = f"{diff.total_seconds()/3600:.1f}hrs"
    except: pass

    urg = "URGENT - price just spiked" if trigger=="price_spike" else "routine scan"
    strategy_section = f"\n\nLearned strategy:\n{strategy_context}" if strategy_context else ""
    prompt = f"""Aggressive prediction market trader. Trade markets resolving within {MAX_HOURS} hours.

Market: "{market['question']}"
Closes in: {hours_left} | Trigger: {urg}
YES: {market['yes_price']:.3f} | NO: {market['no_price']:.3f}
Volume: ${market['volume']:,.0f} | Category: {market['category']}

Stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% WR | Total:${stats['total_pnl']:.2f} | Today:${stats['daily_pnl']:.2f}
{strategy_section}

Be aggressive. Bias toward trading. Only HOLD if truly 50/50.
Respond ONLY with valid JSON:
{{"recommendation":"BUY_YES","confidence":68,"edge":"brief","reasoning":"1-2 sentences.","risk_level":"MEDIUM","fair_value":0.65,"suggested_size_pct":0.75}}"""

    raw_text = ""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":MODEL,"max_tokens":300,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != 200:
                    body = await r.text()
                    log.error(f"AI HTTP {r.status}: {body[:300]}")
                    return None
                d = await r.json()
        raw_text = "".join(b.get("text","") for b in d.get("content",[]))
        res = json.loads(raw_text.replace("```json","").replace("```","").strip())
        if res.get("recommendation") not in ("BUY_YES","BUY_NO","HOLD"):
            log.error(f"AI bad recommendation: {res.get('recommendation')} | raw: {raw_text[:200]}")
            return None
        res["category"] = market.get("category")
        return res
    except Exception as e:
        log.error(f"AI error: {type(e).__name__}: {e} | raw: {raw_text[:200]}", exc_info=True)
        return None

def kelly_size(analysis, price, max_bet):
    try:
        fair = float(analysis.get("fair_value") or price)
        fair = max(0.05, min(0.95, fair))
        b = (1 / price) - 1
        if b <= 0: return round(max_bet * 0.25, 2)
        full_kelly = (b * fair - (1-fair)) / b
        half_kelly = full_kelly / 2
        fraction = max(0.25, min(1.0, half_kelly))
        return max(round(max_bet * fraction, 2), round(max_bet * 0.25, 2))
    except:
        return round(max_bet * 0.5, 2)

async def do_trade(market, analysis, db, trigger="deep_scan"):
    side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
    price = market["yes_price"] if side == "YES" else market["no_price"]
    size = kelly_size(analysis, price, MAX_TRADE)
    log.info(f"[{BOT_NAME}] Kelly: ${size} (fair:{analysis.get('fair_value','?')} price:{price:.3f})")
    if db.stats()["open_positions"] >= MAX_POSITIONS: return False
    if PAPER_MODE:
        tid = db.record(market, analysis, size, trigger=trigger, paper=True)
        log.info(f"[{BOT_NAME}] PAPER #{tid} [{trigger}]: {side} '{market['question'][:45]}' closes:{market.get('end_date','?')[11:16]} @ {price:.3f} ${size}")
        return True
    if not POLY_API_KEY: return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,"price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                d = await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market,analysis,size,trigger=trigger,paper=False,order_id=d["orderId"]); return True
                return False
    except: return False

async def check_market_resolved(mid, session):
    """Try multiple methods to check if a market resolved."""
    # Method 1: Direct lookup
    try:
        async with session.get(f"{GAMMA_API}/markets/{mid}",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                market = await r.json()
                is_resolved = (
                    market.get("resolved") == True or
                    market.get("closed") == True or
                    bool(market.get("winningOutcome")) or
                    bool(market.get("resolutionTime"))
                )
                if is_resolved:
                    return market
    except: pass

    # Method 2: Search closed markets
    try:
        async with session.get(f"{GAMMA_API}/markets",
            params={"id": mid, "closed": "true"},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                if data and len(data) > 0:
                    return data[0]
    except: pass

    # Method 3: Search archived markets
    try:
        async with session.get(f"{GAMMA_API}/markets",
            params={"id": mid, "archived": "true"},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                if data and len(data) > 0:
                    return data[0]
    except: pass

    return None

async def resolve_settled(db):
    open_trades = db.open_trades()
    if not open_trades: return []
    settled = []

    async with aiohttp.ClientSession() as session:
        for trade in open_trades:
            mid = trade["market_id"]
            try:
                # Skip if market hasn't closed yet
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
                                winning = str(outcomes[max_idx]).upper() if max_idx < len(outcomes) else ""
                    except: pass

                if not winning: continue

                outcomes = market.get("outcomes") or ["YES","NO"]
                winning_idx = None
                for i, o in enumerate(outcomes):
                    if str(o).upper().strip() == winning:
                        winning_idx = i
                        break
                our_side = trade["side"].upper()
                if winning_idx is not None:
                    won = (our_side == "YES" and winning_idx == 0) or (our_side == "NO" and winning_idx == 1)
                else:
                    won = our_side == winning
                log.info(f"[{BOT_NAME}] Resolving: side={our_side} winning={winning} outcomes={outcomes} idx={winning_idx} won={won}")
                payout = trade["size"] / trade["price"] if won else 0.0
                pnl = db.resolve(trade["id"], won, payout)
                settled.append({**trade,"won":won,"payout":payout,"pnl":pnl})
                log.info(f"[{BOT_NAME}] ✓ {'WIN' if won else 'LOSS'} ${pnl:.2f} — {trade['question'][:50]}")

            except Exception as e:
                log.debug(f"Resolution check failed {mid}: {e}")

    return settled

async def improve_strategy(db, current_strategy, version):
    resolved = db.get_resolved(50)
    if len(resolved) < 5:
        log.info(f"[{BOT_NAME}] Not enough resolved trades for review")
        return current_strategy, version
    stats = db.stats()
    by_cat = {}
    for t in resolved:
        cat = t.get("category","Unknown")
        if cat not in by_cat: by_cat[cat] = {"won":0,"lost":0,"pnl":0,"spike":0}
        if t["status"]=="won": by_cat[cat]["won"]+=1; by_cat[cat]["pnl"]+=t.get("pnl") or 0
        else: by_cat[cat]["lost"]+=1; by_cat[cat]["pnl"]+=t.get("pnl") or 0
        if t.get("trigger")=="price_spike": by_cat[cat]["spike"]+=1
    trade_summary = "\n".join([
        f"- [{t['status'].upper()}] {t['side']} '{t['question'][:50]}' conf={t['confidence']}% P&L=${t.get('pnl') or 0:.2f}"
        for t in resolved[:20]])
    prompt = f"""Review aggressive HV Polymarket bot (trades within {MAX_HOURS} hours).

Stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% WR | Total:${stats['total_pnl']:.2f} | Today:${stats['daily_pnl']:.2f}
Spike trades: {stats.get('spike_trades',0)}
By category: {json.dumps(by_cat, indent=2)}
Recent: {trade_summary}
Current strategy v{version}: {current_strategy or "None yet"}

Write improved aggressive strategy for short-term markets.
Respond ONLY with valid JSON:
{{"strategy":"detailed strategy","key_insight":"main finding","thresholds":{{"Politics":45,"Sports":42,"Crypto":40,"World Events":45}},"version":{version+1}}}"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":MODEL,"max_tokens":800,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200: return current_strategy, version
                d = await r.json()
        txt = "".join(b.get("text","") for b in d.get("content",[]))
        res = json.loads(txt.replace("```json","").replace("```","").strip())
        new_strategy = res.get("strategy","")
        new_version = res.get("version", version+1)
        key_insight = res.get("key_insight","")
        thresholds = res.get("thresholds",{})
        db.save_strategy(new_version, key_insight, thresholds, stats["win_rate"], stats["total_trades"], new_strategy)
        log.info(f"[{BOT_NAME}] Strategy v{new_version}: {key_insight}")
        return new_strategy, new_version
    except Exception as e:
        log.error(f"Strategy review: {e}")
        return current_strategy, version

async def main():
    global db_g
    db = DB(); db_g = db
    threading.Thread(target=api_thread, daemon=True).start()
    log.info(f"[{BOT_NAME}] Starting v5 | Paper:{PAPER_MODE} | Port:{API_PORT}")
    log.info(f"Model:{MODEL} | Max hours:{MAX_HOURS}hrs | Confidence:{CONFIDENCE_MIN}%+")

    saved = db.get_latest_strategy()
    strategy_context = saved["strategy"] if saved else ""
    strategy_version = saved["version"] if saved else 0
    resolved_at_last_review = db.stats()["won"] + db.stats()["lost"]

    price_cache = {}; ai_pm = 0; ai_win = asyncio.get_event_loop().time()
    deep_due = 0; resolve_due = 0
    markets = await fetch_markets()
    log.info(f"[{BOT_NAME}] Loaded {len(markets)} markets within {MAX_HOURS}hrs")

    while True:
        now = asyncio.get_event_loop().time()
        if now - ai_win >= 60: ai_pm = 0; ai_win = now

        # Tier 1: price spike detection
        spikes = []
        for m in markets:
            mid = m["id"]; cur = m["yes_price"]
            last = price_cache.get(mid)
            if last is not None:
                mv = abs(cur-last)/last*100
                if mv >= PRICE_SPIKE_PCT:
                    spikes.append((m,mv))
                    log.info(f"[{BOT_NAME}] SPIKE {mv:.1f}% '{m['question'][:45]}'")
            price_cache[mid] = cur
            db.rec_price(mid, cur)

        for m, mv in spikes:
            if db.has_open(m["id"]) or ai_pm >= MAX_AI_PER_MIN: continue
            s = db.stats(); a = await analyze(m, s, strategy_context, "price_spike"); ai_pm += 1
            if not a: continue
            if a["confidence"] >= CONFIDENCE_MIN and a["recommendation"] != "HOLD":
                await do_trade(m, a, db, "price_spike")

        # Tier 2: deep scan
        if now >= deep_due:
            deep_due = now + DEEP_SCAN_SECS
            markets = await fetch_markets(); traded = 0

            # Resolve settled trades
            settled = await resolve_settled(db)
            if settled:
                log.info(f"[{BOT_NAME}] Resolved {len(settled)} trades!")

            # Strategy improvement check
            s = db.stats()
            resolved_total = s["won"] + s["lost"]
            if resolved_total - resolved_at_last_review >= IMPROVE_EVERY:
                log.info(f"[{BOT_NAME}] Running strategy review...")
                strategy_context, strategy_version = await improve_strategy(db, strategy_context, strategy_version)
                resolved_at_last_review = resolved_total

            log.info(f"[{BOT_NAME}] Today:${s['daily_pnl']:.2f} | Total:${s['total_pnl']:.2f} | WR:{s['win_rate']:.1f}%")

            for m in markets:
                if db.has_open(m["id"]) or ai_pm >= MAX_AI_PER_MIN: continue
                s = db.stats()
                if s["open_positions"] >= MAX_POSITIONS: break
                a = await analyze(m, s, strategy_context, "deep_scan"); ai_pm += 1
                if not a: continue
                log.info(f"[{BOT_NAME}] '{m['question'][:50]}' → {a['recommendation']} | {a['confidence']}% | closes:{m.get('end_date','?')[11:16]}")
                if a["confidence"] >= CONFIDENCE_MIN and a["recommendation"] != "HOLD":
                    if await do_trade(m, a, db, "deep_scan"): traded += 1
                await asyncio.sleep(0.3)

            s = db.stats()
            log.info(f"[{BOT_NAME}] Deep scan — {traded} new | Open:{s['open_positions']} | Today:${s['daily_pnl']:.2f} | Total:${s['total_pnl']:.2f}")

        await asyncio.sleep(PRICE_SCAN_SECS)

if __name__ == "__main__":
    asyncio.run(main())
