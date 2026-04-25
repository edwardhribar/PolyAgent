"""
PolyAgent HV - High Velocity Polymarket Trading Bot
Two-tier system:
  Tier 1: Price monitor every 5 seconds (no AI cost)
  Tier 2: AI analysis only fires on price spikes / new opportunities
"""

import os
import json
import logging
import asyncio
import aiohttp
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/hv_agent.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("polyagent.hv")

# ── Config ────────────────────────────────────────────────────────
WALLET           = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
POLY_API_KEY     = os.getenv("POLYMARKET_API_KEY", "")
ANTHROPIC_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
PAPER_MODE       = os.getenv("PAPER_MODE", "true").lower() == "true"
MAX_TRADE        = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_POSITIONS    = int(os.getenv("MAX_OPEN_POSITIONS", "25"))
MIN_LIQUIDITY    = float(os.getenv("MIN_LIQUIDITY", "2000"))
MIN_VOLUME       = float(os.getenv("MIN_VOLUME", "5000"))
CONFIDENCE_MIN   = int(os.getenv("BASE_CONFIDENCE", "55"))
PRICE_SCAN_SECS  = int(os.getenv("PRICE_SCAN_SECS", "5"))     # Tier 1: price check
DEEP_SCAN_SECS   = int(os.getenv("DEEP_SCAN_SECS", "30"))     # Tier 2: full AI scan
MARKETS_PER_SCAN = int(os.getenv("MARKETS_PER_SCAN", "50"))
PRICE_SPIKE_PCT  = float(os.getenv("PRICE_SPIKE_PCT", "3.0")) # % move to trigger AI
MAX_AI_PER_MIN   = int(os.getenv("MAX_AI_PER_MIN", "20"))     # rate limit AI calls
BOT_NAME         = os.getenv("BOT_NAME", "HV")

GAMMA_API     = "https://gamma-api.polymarket.com"
CLOB_API      = "https://clob.polymarket.com"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"

CATEGORY_KEYWORDS = {
    "Politics":     ["politics", "election", "president", "congress", "vote", "government", "senate", "republican", "democrat", "policy"],
    "Sports":       ["nfl", "nba", "mlb", "nhl", "soccer", "championship", "win", "super bowl", "world cup", "playoff", "tournament"],
    "Crypto":       ["bitcoin", "ethereum", "btc", "eth", "crypto", "defi", "token", "blockchain", "coinbase", "solana"],
    "World Events": ["war", "economy", "climate", "ai", "tech", "recession", "gdp", "rate", "fed", "nuclear", "treaty"],
}

# ── Database ──────────────────────────────────────────────────────
class Database:
    def __init__(self, path="data/hv_agent.db"):
        self.path = path
        self._init()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id   TEXT NOT NULL,
                    question    TEXT NOT NULL,
                    category    TEXT,
                    side        TEXT NOT NULL,
                    price       REAL NOT NULL,
                    size        REAL NOT NULL,
                    confidence  INTEGER,
                    fair_value  REAL,
                    reasoning   TEXT,
                    trigger     TEXT,
                    paper       INTEGER DEFAULT 1,
                    status      TEXT DEFAULT 'open',
                    outcome     REAL,
                    pnl         REAL,
                    placed_at   TEXT,
                    resolved_at TEXT,
                    order_id    TEXT
                );
                CREATE TABLE IF NOT EXISTS price_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id   TEXT NOT NULL,
                    yes_price   REAL NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS strategy_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp    TEXT,
                    version      INTEGER,
                    notes        TEXT,
                    thresholds   TEXT,
                    win_rate     REAL,
                    total_trades INTEGER
                );
            """)
        log.info(f"[{BOT_NAME}] Database ready")

    def record_trade(self, market, analysis, size, trigger="deep_scan", paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            cur = db.execute("""
                INSERT INTO trades
                (market_id, question, category, side, price, size, confidence,
                 fair_value, reasoning, trigger, paper, status, placed_at, order_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                market["id"], market["question"], market.get("category"),
                side, price, size, analysis.get("confidence"),
                analysis.get("fair_value"), analysis.get("reasoning"),
                trigger, 1 if paper else 0, "open", now, order_id
            ))
            return cur.lastrowid

    def record_price(self, market_id, yes_price):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            db.execute("INSERT INTO price_history (market_id, yes_price, recorded_at) VALUES (?,?,?)",
                       (market_id, yes_price, now))

    def get_last_price(self, market_id):
        with self._conn() as db:
            row = db.execute(
                "SELECT yes_price FROM price_history WHERE market_id=? ORDER BY id DESC LIMIT 1",
                (market_id,)).fetchone()
            return row["yes_price"] if row else None

    def get_open_trades(self):
        with self._conn() as db:
            return [dict(r) for r in db.execute("SELECT * FROM trades WHERE status='open'").fetchall()]

    def get_trade(self, trade_id):
        with self._conn() as db:
            row = db.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
            return dict(row) if row else None

    def resolve_trade(self, trade_id, won, payout):
        trade = self.get_trade(trade_id)
        pnl = payout - trade["size"]
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            db.execute("UPDATE trades SET status=?, outcome=?, pnl=?, resolved_at=? WHERE id=?",
                       ("won" if won else "lost", payout, pnl, now, trade_id))

    def has_open_position(self, market_id):
        with self._conn() as db:
            return db.execute("SELECT id FROM trades WHERE market_id=? AND status='open'",
                              (market_id,)).fetchone() is not None

    def get_resolved_trades(self, limit=100):
        with self._conn() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades WHERE status IN ('won','lost') ORDER BY resolved_at DESC LIMIT ?",
                (limit,)).fetchall()]

    def get_trades_by_category(self, category):
        with self._conn() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM trades WHERE category=? AND status IN ('won','lost')",
                (category,)).fetchall()]

    def get_stats(self):
        with self._conn() as db:
            total    = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            open_pos = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won      = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lost     = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl_row  = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()
            spike    = db.execute("SELECT COUNT(*) FROM trades WHERE trigger='price_spike'").fetchone()[0]
        resolved = won + lost
        return {
            "total_trades":   total,
            "open_positions": open_pos,
            "won": won, "lost": lost,
            "win_rate":  (won / resolved * 100) if resolved > 0 else 0.0,
            "total_pnl": pnl_row[0] or 0.0,
            "spike_trades": spike,
        }

    def log_strategy(self, version, notes, thresholds, win_rate, total_trades):
        with self._conn() as db:
            db.execute("""INSERT INTO strategy_log
                (timestamp, version, notes, thresholds, win_rate, total_trades)
                VALUES (?,?,?,?,?,?)""",
                (datetime.now(timezone.utc).isoformat(), version, notes,
                 json.dumps(thresholds), win_rate, total_trades))

    def get_latest_strategy(self):
        with self._conn() as db:
            row = db.execute("SELECT * FROM strategy_log ORDER BY version DESC LIMIT 1").fetchone()
            return dict(row) if row else None

# ── Market Fetcher ────────────────────────────────────────────────
async def fetch_markets():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API}/markets",
                params={"active":"true","closed":"false","limit":MARKETS_PER_SCAN,
                        "order":"volume","ascending":"false"},
                timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
    except Exception as e:
        log.debug(f"Market fetch error: {e}")
        return []

    markets = []
    for m in data:
        try:
            volume    = float(m.get("volume") or 0)
            liquidity = float(m.get("liquidity") or 0)
            if volume < MIN_VOLUME or liquidity < MIN_LIQUIDITY:
                continue
            yes_price = 0.5
            try:
                yes_price = float(json.loads(m.get("outcomePrices") or "[0.5,0.5]")[0])
            except Exception:
                pass
            if yes_price < 0.04 or yes_price > 0.96:
                continue
            text = (m.get("question") or m.get("title") or "").lower()
            category = "World Events"
            for cat, keywords in CATEGORY_KEYWORDS.items():
                if any(k in text for k in keywords):
                    category = cat
                    break
            markets.append({
                "id":        m.get("id") or m.get("conditionId"),
                "question":  m.get("question") or m.get("title") or "Unknown",
                "yes_price": yes_price,
                "no_price":  1 - yes_price,
                "volume":    volume,
                "liquidity": liquidity,
                "end_date":  m.get("endDate") or m.get("endDateIso"),
                "category":  category,
            })
        except Exception:
            continue
    return markets

# ── AI Analyst ────────────────────────────────────────────────────
async def analyze_market(market, stats, strategy_context="", trigger="deep_scan"):
    if not ANTHROPIC_KEY:
        return None

    urgency = "URGENT - price just spiked, act fast" if trigger == "price_spike" else "routine scan"
    prompt = f"""You are an aggressive autonomous prediction market trader. Goal: maximize trade volume and P&L.

Market: "{market['question']}"
YES: {market['yes_price']:.3f} ({market['yes_price']*100:.1f}%)
NO:  {market['no_price']:.3f}
Volume: ${market['volume']:,.0f} | Liquidity: ${market['liquidity']:,.0f}
Category: {market['category']} | Trigger: {urgency}

Agent stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% win | ${stats['total_pnl']:.2f} P&L
{f"Strategy: {strategy_context}" if strategy_context else ""}

Be aggressive. Look for any edge. Bias toward trading over holding.
Only HOLD if the market is truly 50/50 with no edge.

Respond ONLY with valid JSON:
{{"recommendation":"BUY_YES","confidence":68,"edge":"brief edge","reasoning":"1-2 sentences.","risk_level":"MEDIUM","fair_value":0.65,"suggested_size_pct":0.75}}

recommendation: BUY_YES | BUY_NO | HOLD
confidence: 0-100 (be willing to trade at 55+)
risk_level: LOW | MEDIUM | HIGH
fair_value: 0-1
suggested_size_pct: 0.5-1.0"""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ANTHROPIC_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                         "anthropic-version":"2023-06-01"},
                json={"model":"claude-opus-4-6","max_tokens":400,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    log.warning(f"Anthropic error: {resp.status}")
                    return None
                data = await resp.json()
        text = "".join(b.get("text","") for b in data.get("content",[]))
        result = json.loads(text.replace("```json","").replace("```","").strip())
        assert result.get("recommendation") in ("BUY_YES","BUY_NO","HOLD")
        result["category"] = market.get("category")
        return result
    except Exception as e:
        log.debug(f"Analysis error: {e}")
        return None

# ── Trade Executor ────────────────────────────────────────────────
async def place_trade(market, analysis, db, trigger="deep_scan"):
    side  = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
    price = market["yes_price"] if side == "YES" else market["no_price"]
    size  = round(MAX_TRADE * analysis.get("suggested_size_pct", 1.0), 2)

    stats = db.get_stats()
    if stats["open_positions"] >= MAX_POSITIONS:
        return False

    if PAPER_MODE:
        trade_id = db.record_trade(market, analysis, size, trigger=trigger, paper=True)
        log.info(f"[{BOT_NAME}] 📄 PAPER #{trade_id} [{trigger}]: {side} '{market['question'][:45]}' @ {price:.3f} ${size}")
        return True

    if not POLY_API_KEY:
        return False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json",
                         "POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,
                      "price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("orderId"):
                    trade_id = db.record_trade(market, analysis, size, trigger=trigger,
                                               paper=False, order_id=data["orderId"])
                    log.info(f"[{BOT_NAME}] 💰 REAL #{trade_id}: {side} '{market['question'][:45]}' @ {price:.3f} ${size}")
                    return True
                return False
    except Exception as e:
        log.debug(f"Trade error: {e}")
        return False

async def resolve_settled(db):
    open_trades = db.get_open_trades()
    if not open_trades:
        return []
    settled = []
    for trade in open_trades:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{GAMMA_API}/markets/{trade['market_id']}",
                    timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status != 200:
                        continue
                    market = await resp.json()
            if not market.get("closed") and not market.get("resolved"):
                continue
            winning = market.get("winningOutcome","").upper()
            if not winning:
                continue
            won    = trade["side"] == winning
            payout = trade["size"] / trade["price"] if won else 0.0
            db.resolve_trade(trade["id"], won, payout)
            settled.append({**trade,"won":won,"payout":payout})
        except Exception:
            continue
    return settled

# ── Learning Engine ───────────────────────────────────────────────
DEFAULT_THRESHOLDS = {"Politics":55,"Sports":55,"Crypto":55,"World Events":55}

class LearningEngine:
    def __init__(self, db):
        self.db = db
        self.version    = 0
        self.thresholds = DEFAULT_THRESHOLDS.copy()
        self.context    = ""
        self.path       = Path("data/hv_strategy.json")

    def load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                self.thresholds = data.get("thresholds", DEFAULT_THRESHOLDS)
                self.version    = data.get("version", 0)
                self.context    = data.get("context", "")
                log.info(f"[{BOT_NAME}] Loaded strategy v{self.version}")
                return
            except Exception:
                pass
        log.info(f"[{BOT_NAME}] Using default aggressive strategy")

    def threshold(self, category):
        return self.thresholds.get(category, CONFIDENCE_MIN)

    def update_from_outcomes(self, settled):
        for trade in settled:
            cat     = trade.get("category","World Events")
            current = self.thresholds.get(cat, 55)
            if trade["won"]:
                self.thresholds[cat] = max(50, current - 1)
            else:
                self.thresholds[cat] = min(85, current + 2)
        self._save("Auto-updated")

    async def refine(self, stats):
        resolved = self.db.get_resolved_trades(100)
        if len(resolved) < 10:
            return
        cat_stats = {}
        for cat in CATEGORY_KEYWORDS:
            trades = self.db.get_trades_by_category(cat)
            if not trades:
                cat_stats[cat] = {"trades":0,"win_rate":0,"pnl":0}
                continue
            won = sum(1 for t in trades if t["status"]=="won")
            pnl = sum(t.get("pnl") or 0 for t in trades)
            cat_stats[cat] = {"trades":len(trades),"win_rate":round(won/len(trades)*100,1),"pnl":round(pnl,2)}

        spike_trades = [t for t in resolved if t.get("trigger") == "price_spike"]
        spike_wins   = sum(1 for t in spike_trades if t["status"] == "won")
        spike_wr     = (spike_wins / len(spike_trades) * 100) if spike_trades else 0

        prompt = f"""Aggressive Polymarket HV bot performance review.

Stats: {stats['total_trades']} trades | {stats['win_rate']:.1f}% WR | ${stats['total_pnl']:.2f} P&L
Price spike trades: {len(spike_trades)} | Spike win rate: {spike_wr:.1f}%
Category: {json.dumps(cat_stats)}
Thresholds: {json.dumps(self.thresholds)}

This is a HIGH VELOCITY aggressive bot. Keep thresholds low (50-75 range).
Are price spike triggers working? Which categories are profitable?

Respond ONLY with valid JSON:
{{"thresholds":{{"Politics":55,"Sports":60,"Crypto":55,"World Events":58}},"analysis":"brief","key_insight":"main pattern","recommended_changes":"specific"}}"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(ANTHROPIC_API,
                    headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                             "anthropic-version":"2023-06-01"},
                    json={"model":"claude-opus-4-6","max_tokens":400,
                          "messages":[{"role":"user","content":prompt}]},
                    timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    data = await resp.json()
            text   = "".join(b.get("text","") for b in data.get("content",[]))
            result = json.loads(text.replace("```json","").replace("```","").strip())
            new_t  = result.get("thresholds", self.thresholds)
            self.thresholds = {k: max(48, min(80, int(v))) for k,v in new_t.items()}
            self.context = f"v{self.version+1} — {result.get('analysis','')} | {result.get('key_insight','')}"
            self.version += 1
            self._save(self.context)
            self.db.log_strategy(self.version, self.context, self.thresholds, stats["win_rate"], stats["total_trades"])
            log.info(f"[{BOT_NAME}] Strategy v{self.version}: {self.thresholds}")
        except Exception as e:
            log.debug(f"Refine error: {e}")

    def _save(self, context=""):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"version":self.version,"thresholds":self.thresholds,"context":context},indent=2))

# ── Main Agent ────────────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info(f"[{BOT_NAME}] PolyAgent HV starting")
    log.info(f"Paper mode:    {PAPER_MODE}")
    log.info(f"Max trade:     ${MAX_TRADE}")
    log.info(f"Max positions: {MAX_POSITIONS}")
    log.info(f"Price scan:    every {PRICE_SCAN_SECS}s")
    log.info(f"Deep scan:     every {DEEP_SCAN_SECS}s")
    log.info(f"Confidence:    {CONFIDENCE_MIN}%+")
    log.info(f"Spike trigger: {PRICE_SPIKE_PCT}% move")
    log.info("=" * 60)

    db      = Database()
    learner = LearningEngine(db)
    learner.load()

    # State
    price_cache    = {}   # market_id -> last known price
    ai_calls_min   = 0    # AI rate limiter
    ai_window_start = asyncio.get_event_loop().time()
    deep_scan_due  = 0
    resolve_due    = 0
    refine_cycle   = 0
    total_scans    = 0

    markets = await fetch_markets()
    log.info(f"[{BOT_NAME}] Loaded {len(markets)} markets on startup")

    while True:
        now = asyncio.get_event_loop().time()

        # ── Reset AI rate limiter every 60s ──────────────────────
        if now - ai_window_start >= 60:
            ai_calls_min    = 0
            ai_window_start = now

        # ── Tier 1: Price monitor (every 5s) ─────────────────────
        total_scans += 1
        spike_markets = []

        for market in markets:
            mid       = market["id"]
            cur_price = market["yes_price"]
            last      = price_cache.get(mid)

            if last is not None:
                move_pct = abs(cur_price - last) / last * 100
                if move_pct >= PRICE_SPIKE_PCT:
                    spike_markets.append((market, move_pct, "price_spike"))
                    log.info(f"[{BOT_NAME}] 🔥 SPIKE {move_pct:.1f}% on '{market['question'][:45]}'")

            price_cache[mid] = cur_price
            db.record_price(mid, cur_price)

        # ── Process spikes with AI (rate limited) ─────────────────
        for market, move_pct, trigger in spike_markets:
            if db.has_open_position(market["id"]):
                continue
            if ai_calls_min >= MAX_AI_PER_MIN:
                log.debug(f"[{BOT_NAME}] AI rate limit hit, skipping spike")
                break
            stats    = db.get_stats()
            analysis = await analyze_market(market, stats, learner.context, trigger="price_spike")
            ai_calls_min += 1
            if not analysis:
                continue
            conf = analysis.get("confidence", 0)
            rec  = analysis.get("recommendation","HOLD")
            log.info(f"[{BOT_NAME}] SPIKE → {rec} | {conf}% | {analysis.get('risk_level')}")
            if conf >= learner.threshold(market["category"]) and rec != "HOLD":
                await place_trade(market, analysis, db, trigger="price_spike")

        # ── Tier 2: Deep scan (every 30s) ─────────────────────────
        if now >= deep_scan_due:
            deep_scan_due = now + DEEP_SCAN_SECS
            markets = await fetch_markets()
            traded  = 0

            for market in markets:
                if db.has_open_position(market["id"]):
                    continue
                if ai_calls_min >= MAX_AI_PER_MIN:
                    break
                stats    = db.get_stats()
                analysis = await analyze_market(market, stats, learner.context, trigger="deep_scan")
                ai_calls_min += 1
                if not analysis:
                    continue
                conf = analysis.get("confidence", 0)
                rec  = analysis.get("recommendation","HOLD")
                log.info(
                    f"[{BOT_NAME}]   {market['question'][:50]}…\n"
                    f"             → {rec} | {conf}% | {analysis.get('risk_level')} risk"
                )
                if conf >= learner.threshold(market["category"]) and rec != "HOLD":
                    if await place_trade(market, analysis, db, trigger="deep_scan"):
                        traded += 1
                await asyncio.sleep(0.3)

            stats = db.get_stats()
            log.info(
                f"[{BOT_NAME}] Deep scan done — {traded} new trades\n"
                f"  Stats → Total:{stats['total_trades']} | "
                f"WR:{stats['win_rate']:.1f}% | "
                f"P&L:${stats['total_pnl']:.2f} | "
                f"Open:{stats['open_positions']} | "
                f"Spikes:{stats['spike_trades']} | "
                f"AI/min:{ai_calls_min}"
            )
            refine_cycle += 1

        # ── Resolve settled trades (every 5 min) ──────────────────
        if now >= resolve_due:
            resolve_due = now + 300
            settled = await resolve_settled(db)
            if settled:
                log.info(f"[{BOT_NAME}] Settled {len(settled)} trades")
                learner.update_from_outcomes(settled)

        # ── Refine strategy every 20 deep scans ───────────────────
        if refine_cycle >= 20:
            refine_cycle = 0
            stats = db.get_stats()
            await learner.refine(stats)

        await asyncio.sleep(PRICE_SCAN_SECS)


if __name__ == "__main__":
    asyncio.run(main())
