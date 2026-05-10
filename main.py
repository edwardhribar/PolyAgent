"""
PolyAgent Conservative Bot v3
- Only trades markets resolving within 48 hours
- Daily P&L tracking
- Self-improving: reviews every 20 resolved trades and rewrites strategy
"""

import os, json, logging, asyncio, aiohttp, sqlite3, threading
from datetime import datetime, timezone, timedelta
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
MAX_HOURS_TO_CLOSE = int(os.getenv("MAX_HOURS_TO_CLOSE", "72"))
IMPROVE_EVERY   = int(os.getenv("IMPROVE_EVERY", "20"))
API_PORT        = int(os.getenv("PORT", "8080"))
BOT_NAME        = "Conservative"
MODEL           = "claude-sonnet-4-5-20250929"

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
ANT_API   = "https://api.anthropic.com/v1/messages"

KEYWORDS = {
    "Politics":     ["politics","election","president","congress","vote","government","senate","republican","democrat"],
    "Sports":       ["nfl","nba","mlb","nhl","soccer","championship","win","super bowl","world cup","playoff","relegated","tonight","game"],
    "Crypto":       ["bitcoin","ethereum","btc","eth","crypto","price","above","below","reach"],
    "World Events": ["war","economy","climate","ai","tech","recession","gdp","rate","fed"],
}

class DB:
    def __init__(self, path="data/polyagent.db"):
        self.path = path
        with self._c() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT, question TEXT, category TEXT,
                    side TEXT, price REAL, size REAL, confidence INTEGER,
                    fair_value REAL, reasoning TEXT, paper INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'open', outcome REAL, pnl REAL,
                    placed_at TEXT, resolved_at TEXT, order_id TEXT,
                    end_date TEXT);
                CREATE TABLE IF NOT EXISTS strategy_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, version INTEGER, notes TEXT,
                    thresholds TEXT, win_rate REAL, total_trades INTEGER,
                    strategy TEXT);
            """)
            # Add end_date column if missing (migration)
            try:
                db.execute("ALTER TABLE trades ADD COLUMN end_date TEXT")
            except: pass

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def record(self, market, analysis, size, paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._c() as db:
            cur = db.execute(
                "INSERT INTO trades (market_id,question,category,side,price,size,confidence,fair_value,reasoning,paper,status,placed_at,order_id,end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (market["id"],market["question"],market.get("category"),side,price,size,
                 analysis.get("confidence"),analysis.get("fair_value"),analysis.get("reasoning"),
                 1 if paper else 0,"open",now,order_id,market.get("end_date")))
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
        log.info(f"[{BOT_NAME}] #{tid} {'WON' if won else 'LOST'} P&L:${pnl:.2f}")
        return pnl

    def has_open(self, mid):
        with self._c() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",(mid,)).fetchone() is not None

    def recent(self, n=10):
        with self._c() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?",(n,)).fetchall()]

    def resolved_since_last_review(self, last_count):
        with self._c() as db:
            count = db.execute("SELECT COUNT(*) FROM trades WHERE status IN ('won','lost')").fetchone()[0]
        return count - last_count

    def get_resolved(self, limit=50):
        with self._c() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades WHERE status IN ('won','lost') ORDER BY resolved_at DESC LIMIT ?",(limit,)).fetchall()]

    def daily_pnl(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._c() as db:
            row = db.execute(
                "SELECT SUM(pnl) FROM trades WHERE resolved_at LIKE ? AND pnl IS NOT NULL",(f"{today}%",)).fetchone()
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
        }

    def save_strategy(self, version, notes, thresholds, win_rate, total_trades, strategy_text):
        with self._c() as db:
            db.execute(
                "INSERT INTO strategy_log (timestamp,version,notes,thresholds,win_rate,total_trades,strategy) VALUES (?,?,?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(),version,notes,
                 json.dumps(thresholds),win_rate,total_trades,strategy_text))

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
            payload = {**s,"recent_trades":[{
                "question":x["question"][:60],"side":x["side"],"price":x["price"],
                "size":x["size"],"confidence":x["confidence"],"status":x["status"],
                "pnl":x["pnl"],"placed_at":x["placed_at"],"end_date":x.get("end_date")} for x in t]}
            self.wfile.write(json.dumps(payload).encode())
        else:
            self.wfile.write(b'{"status":"ok"}')
    def log_message(self, *a): pass

def api_thread():
    HTTPServer(("0.0.0.0", API_PORT), API).serve_forever()

def is_within_hours(end_date_str, hours):
    """Check if market closes within X hours from now."""
    if not end_date_str:
        return False
    try:
        # Handle various date formats
        for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"]:
            try:
                end_dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
                break
            except: continue
        else:
            return False
        now = datetime.now(timezone.utc)
        diff = end_dt - now
        return timedelta(0) < diff <= timedelta(hours=hours)
    except:
        return False

async def fetch_markets():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GAMMA_API}/markets",
                params={"active":"true","closed":"false","limit":100,"order":"endDate","ascending":"true"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200: return []
                data = await r.json()
    except Exception as e:
        log.error(f"Fetch: {e}"); return []

    out = []
    for m in data:
        try:
            vol=float(m.get("volume") or 0)
            liq=float(m.get("liquidity") or 0)
            if vol<MIN_VOLUME or liq<MIN_LIQUIDITY: continue

            # KEY FILTER: only markets closing within 48 hours
            end_date = m.get("endDate") or m.get("endDateIso") or ""
            if not is_within_hours(end_date, MAX_HOURS_TO_CLOSE):
                continue

            yp=0.5
            try: yp=float(json.loads(m.get("outcomePrices") or "[0.5]")[0])
            except: pass
            if yp<0.05 or yp>0.95: continue

            txt=(m.get("question") or m.get("title") or "").lower()
            cat="World Events"
            for c,kw in KEYWORDS.items():
                if any(k in txt for k in kw): cat=c; break

            out.append({
                "id": m.get("id") or m.get("conditionId"),
                "question": m.get("question") or m.get("title") or "Unknown",
                "yes_price": yp, "no_price": 1-yp,
                "volume": vol, "liquidity": liq,
                "end_date": end_date, "category": cat,
            })
        except: continue

    log.info(f"Found {len(out)} markets closing within {MAX_HOURS_TO_CLOSE}hrs")
    return out[:MARKETS_PER_SCAN]

async def analyze(market, stats, strategy_context=""):
    if not ANTHROPIC_KEY: return None

    # Calculate hours until close
    hours_left = "unknown"
    if market.get("end_date"):
        try:
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                try:
                    end_dt = datetime.strptime(market["end_date"], fmt).replace(tzinfo=timezone.utc)
                    diff = end_dt - datetime.now(timezone.utc)
                    hours_left = f"{diff.total_seconds()/3600:.1f}hrs"
                    break
                except: continue
        except: pass

    strategy_section = f"\n\nLearned strategy:\n{strategy_context}" if strategy_context else ""

    prompt = f"""You are an autonomous prediction market trading agent. You ONLY trade markets resolving within 48 hours.

Market: "{market['question']}"
Closes in: {hours_left}
YES: {market['yes_price']:.3f} ({market['yes_price']*100:.1f}%)
NO: {market['no_price']:.3f}
Volume: ${market['volume']:,.0f} | Liquidity: ${market['liquidity']:,.0f}
Category: {market['category']}

Agent performance:
- Total trades: {stats['total_trades']}
- Win rate: {stats['win_rate']:.1f}%
- Total P&L: ${stats['total_pnl']:.2f}
- Today P&L: ${stats['daily_pnl']:.2f}
{strategy_section}

Analyze this short-term market carefully. Look for clear edges.
Respond ONLY with valid JSON:
{{"recommendation":"BUY_YES","confidence":78,"edge":"brief reason","reasoning":"2 sentences.","risk_level":"MEDIUM","fair_value":0.72,"suggested_size_pct":0.5}}

recommendation: BUY_YES | BUY_NO | HOLD
confidence: 0-100
risk_level: LOW | MEDIUM | HIGH
fair_value: 0-1
suggested_size_pct: 0.25-1.0"""

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":MODEL,"max_tokens":400,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    log.warning(f"Anthropic {r.status}"); return None
                d = await r.json()
        txt="".join(b.get("text","") for b in d.get("content",[]))
        res=json.loads(txt.replace("```json","").replace("```","").strip())
        assert res.get("recommendation") in ("BUY_YES","BUY_NO","HOLD")
        res["category"]=market.get("category"); return res
    except Exception as e:
        log.error(f"AI: {e}"); return None

async def do_trade(market, analysis, db):
    side="YES" if analysis["recommendation"]=="BUY_YES" else "NO"
    price=market["yes_price"] if side=="YES" else market["no_price"]
    size=round(MAX_TRADE*analysis.get("suggested_size_pct",1.0),2)
    if db.stats()["open_positions"]>=MAX_POSITIONS: return False
    if PAPER_MODE:
        tid=db.record(market,analysis,size,paper=True)
        log.info(f"[{BOT_NAME}] PAPER #{tid}: {side} '{market['question'][:50]}' closes:{market.get('end_date','?')[:10]} @ {price:.3f} ${size}")
        return True
    if not POLY_API_KEY: return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json","POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,
                      "price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                d=await r.json()
                if r.status==200 and d.get("orderId"):
                    db.record(market,analysis,size,paper=False,order_id=d["orderId"]); return True
                return False
    except Exception as e:
        log.error(f"Trade: {e}"); return False

async def resolve_settled(db):
    open_trades = db.open_trades()
    if not open_trades: return []
    settled = []

    for trade in open_trades:
        mid = trade["market_id"]
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{GAMMA_API}/markets/{mid}",
                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200: continue
                    market = await r.json()

            is_resolved = (
                market.get("resolved") == True or
                market.get("closed") == True or
                bool(market.get("winningOutcome")) or
                bool(market.get("resolutionTime"))
            )
            if not is_resolved: continue

            winning = (market.get("winningOutcome") or "").upper().strip()
            if not winning:
                try:
                    prices = json.loads(market.get("outcomePrices") or "[]")
                    outcomes = market.get("outcomes") or ["YES","NO"]
                    if prices:
                        max_idx = prices.index(max(float(p) for p in prices))
                        winning = str(outcomes[max_idx]).upper() if max_idx < len(outcomes) else ""
                except: pass

            if not winning: continue

            won = trade["side"].upper() == winning
            payout = trade["size"] / trade["price"] if won else 0.0
            pnl = db.resolve(trade["id"], won, payout)
            settled.append({**trade,"won":won,"payout":payout,"pnl":pnl})

        except Exception as e:
            log.debug(f"Resolution check failed {mid}: {e}")
            continue

    return settled

async def improve_strategy(db, current_strategy, version):
    """Review last 20 resolved trades and rewrite strategy using Claude."""
    resolved = db.get_resolved(50)
    if len(resolved) < 5:
        log.info(f"[{BOT_NAME}] Not enough resolved trades for strategy review")
        return current_strategy, version

    stats = db.stats()

    # Build performance summary
    by_category = {}
    for t in resolved:
        cat = t.get("category","Unknown")
        if cat not in by_category:
            by_category[cat] = {"won":0,"lost":0,"pnl":0}
        if t["status"] == "won":
            by_category[cat]["won"] += 1
            by_category[cat]["pnl"] += t.get("pnl") or 0
        else:
            by_category[cat]["lost"] += 1
            by_category[cat]["pnl"] += t.get("pnl") or 0

    trade_summary = "\n".join([
        f"- [{t['status'].upper()}] {t['side']} '{t['question'][:50]}' conf={t['confidence']}% P&L=${t.get('pnl') or 0:.2f}"
        for t in resolved[:20]
    ])

    prompt = f"""You are reviewing the performance of an autonomous Polymarket trading agent that ONLY trades markets resolving within 48 hours.

Overall stats:
- Total trades: {stats['total_trades']}
- Win rate: {stats['win_rate']:.1f}%
- Total P&L: ${stats['total_pnl']:.2f}
- Today P&L: ${stats['daily_pnl']:.2f}

Performance by category:
{json.dumps(by_category, indent=2)}

Recent trade outcomes:
{trade_summary}

Current strategy (v{version}):
{current_strategy or "No strategy yet - this is the first review"}

Based on this data, write an improved trading strategy. Be specific about:
1. Which market types/categories to prioritize or avoid
2. What confidence level to require for each category
3. What patterns you noticed in winning vs losing trades
4. What to look for when analyzing short-term markets

Respond ONLY with valid JSON:
{{"strategy":"detailed strategy text here","key_insight":"single most important finding","thresholds":{{"Politics":75,"Sports":70,"Crypto":72,"World Events":75}},"version":{version+1}}}"""

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(ANT_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":MODEL,"max_tokens":800,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    log.warning(f"Strategy review Anthropic {r.status}"); return current_strategy, version
                d = await r.json()

        txt="".join(b.get("text","") for b in d.get("content",[]))
        res=json.loads(txt.replace("```json","").replace("```","").strip())

        new_strategy = res.get("strategy","")
        new_version = res.get("version", version+1)
        key_insight = res.get("key_insight","")
        thresholds = res.get("thresholds", {})

        db.save_strategy(new_version, key_insight, thresholds, stats["win_rate"], stats["total_trades"], new_strategy)
        log.info(f"[{BOT_NAME}] Strategy updated to v{new_version}")
        log.info(f"[{BOT_NAME}] Key insight: {key_insight}")

        return new_strategy, new_version

    except Exception as e:
        log.error(f"Strategy review error: {e}")
        return current_strategy, version

async def main():
    global db_g
    db = DB(); db_g = db
    threading.Thread(target=api_thread, daemon=True).start()

    log.info(f"[{BOT_NAME}] Starting v3 | Paper:{PAPER_MODE} | Port:{API_PORT}")
    log.info(f"Model: {MODEL} | Max hours to close: {MAX_HOURS_TO_CLOSE}hrs")
    log.info(f"Confidence: {BASE_CONFIDENCE}% | Improve every: {IMPROVE_EVERY} trades")

    # Load existing strategy
    saved = db.get_latest_strategy()
    strategy_context = saved["strategy"] if saved else ""
    strategy_version = saved["version"] if saved else 0
    resolved_count_at_last_review = db.stats()["won"] + db.stats()["lost"]

    if strategy_context:
        log.info(f"[{BOT_NAME}] Loaded strategy v{strategy_version}")

    cycle = 0
    while True:
        try:
            cycle += 1
            log.info(f"\n── Cycle #{cycle} ──")

            # Resolve settled trades
            settled = await resolve_settled(db)
            if settled:
                log.info(f"[{BOT_NAME}] Resolved {len(settled)} trades!")
                for t in settled:
                    log.info(f"  {'✓ WON' if t['won'] else '✗ LOST'} P&L:${t.get('pnl',0):.2f} — {t['question'][:50]}")

            # Check if we need to improve strategy
            s = db.stats()
            resolved_total = s["won"] + s["lost"]
            new_resolved = resolved_total - resolved_count_at_last_review

            if new_resolved >= IMPROVE_EVERY:
                log.info(f"[{BOT_NAME}] {new_resolved} new resolved trades — running strategy review...")
                strategy_context, strategy_version = await improve_strategy(db, strategy_context, strategy_version)
                resolved_count_at_last_review = resolved_total

            # Print daily P&L
            log.info(f"[{BOT_NAME}] Today P&L: ${s['daily_pnl']:.2f} | Total P&L: ${s['total_pnl']:.2f} | WR: {s['win_rate']:.1f}%")

            # Fetch and analyze markets
            mkts = await fetch_markets()
            traded = 0

            for m in mkts:
                if db.has_open(m["id"]): continue
                s = db.stats()
                if s["open_positions"] >= MAX_POSITIONS:
                    log.info(f"[{BOT_NAME}] Max positions reached")
                    break

                a = await analyze(m, s, strategy_context)
                if not a: continue

                log.info(f"  '{m['question'][:50]}' → {a['recommendation']} | {a['confidence']}% | {a['risk_level']} risk")

                if a["confidence"] >= BASE_CONFIDENCE and a["recommendation"] != "HOLD":
                    if await do_trade(m, a, db): traded += 1

                await asyncio.sleep(1)

            s = db.stats()
            log.info(f"Cycle #{cycle} done — {traded} new | Open:{s['open_positions']} | Today:${s['daily_pnl']:.2f} | Total:${s['total_pnl']:.2f}")

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
