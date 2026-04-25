"""
PolyAgent - Autonomous Polymarket Trading Bot (Single File)
Scans markets every 5 min, analyzes with Claude AI, places trades, learns from outcomes.
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
        logging.FileHandler("logs/agent.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("polyagent")

# ── Config ────────────────────────────────────────────────────────
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
MARKETS_PER_SCAN = int(os.getenv("MARKETS_PER_SCAN", "20"))

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
    def __init__(self, path="data/polyagent.db"):
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
                    paper       INTEGER DEFAULT 1,
                    status      TEXT DEFAULT 'open',
                    outcome     REAL,
                    pnl         REAL,
                    placed_at   TEXT,
                    resolved_at TEXT,
                    order_id    TEXT
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
        log.info("Database ready")

    def record_trade(self, market, analysis, size, paper=True, order_id=None):
        side = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
        price = market["yes_price"] if side == "YES" else 1 - market["yes_price"]
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            cur = db.execute("""
                INSERT INTO trades
                (market_id, question, category, side, price, size, confidence,
                 fair_value, reasoning, paper, status, placed_at, order_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                market["id"], market["question"], market.get("category"),
                side, price, size, analysis.get("confidence"),
                analysis.get("fair_value"), analysis.get("reasoning"),
                1 if paper else 0, "open", now, order_id
            ))
            return cur.lastrowid

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
        resolved = won + lost
        return {
            "total_trades":   total,
            "open_positions": open_pos,
            "won": won, "lost": lost,
            "win_rate":  (won / resolved * 100) if resolved > 0 else 0.0,
            "total_pnl": pnl_row[0] or 0.0,
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

# ── Market Scanner ────────────────────────────────────────────────
async def fetch_markets():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{GAMMA_API}/markets",
                params={"active":"true","closed":"false","limit":50,"order":"volume","ascending":"false"},
                timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
    except Exception as e:
        log.error(f"Market fetch error: {e}")
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
            if yes_price < 0.05 or yes_price > 0.95:
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
    log.info(f"Scanner found {len(markets)} quality markets")
    return markets[:MARKETS_PER_SCAN]

# ── AI Analyst ────────────────────────────────────────────────────
async def analyze_market(market, stats, strategy_context=""):
    if not ANTHROPIC_KEY:
        log.error("No Anthropic API key")
        return None

    strategy_section = f"\n\nLearned strategy:\n{strategy_context}" if strategy_context else ""
    prompt = f"""You are an autonomous prediction market trading agent maximizing profit over time.

Market: "{market['question']}"
YES price: {market['yes_price']:.3f} ({market['yes_price']*100:.1f}% implied probability)
NO price:  {market['no_price']:.3f}
Volume:    ${market['volume']:,.0f}
Liquidity: ${market['liquidity']:,.0f}
Category:  {market['category']}
Ends:      {market.get('end_date','Unknown')}

Agent performance:
- Total trades: {stats['total_trades']}
- Win rate: {stats['win_rate']:.1f}%
- Total P&L: ${stats['total_pnl']:.2f}
{strategy_section}

Analyze for edge: price vs fair value, sentiment bias, liquidity risk, time horizon.

Respond ONLY with valid JSON, no markdown:
{{"recommendation":"BUY_YES","confidence":78,"edge":"reason","reasoning":"2-3 sentences.","risk_level":"MEDIUM","fair_value":0.72,"suggested_size_pct":0.5}}

recommendation: BUY_YES | BUY_NO | HOLD
confidence: 0-100
risk_level: LOW | MEDIUM | HIGH
fair_value: 0-1
suggested_size_pct: 0.25-1.0"""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ANTHROPIC_API,
                headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":600,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as resp:
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
        log.error(f"Analysis error: {e}")
        return None

# ── Trade Executor ────────────────────────────────────────────────
async def place_trade(market, analysis, db):
    side  = "YES" if analysis["recommendation"] == "BUY_YES" else "NO"
    price = market["yes_price"] if side == "YES" else market["no_price"]
    size  = round(MAX_TRADE * analysis.get("suggested_size_pct", 1.0), 2)

    stats = db.get_stats()
    if stats["open_positions"] >= MAX_POSITIONS:
        log.info("Max positions reached, skipping")
        return False

    if PAPER_MODE:
        trade_id = db.record_trade(market, analysis, size, paper=True)
        log.info(f"📄 PAPER #{trade_id}: {side} '{market['question'][:50]}' @ {price:.3f} for ${size}")
        return True

    if not POLY_API_KEY:
        log.error("No Polymarket API key for real trading")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CLOB_API}/order",
                headers={"Content-Type":"application/json",
                         "POLY_ADDRESS":WALLET,"POLY_API_KEY":POLY_API_KEY},
                json={"market":market["id"],"side":"BUY","outcome":side,
                      "price":round(price,4),"size":size,"orderType":"GTC"},
                timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("orderId"):
                    trade_id = db.record_trade(market, analysis, size, paper=False, order_id=data["orderId"])
                    log.info(f"💰 REAL #{trade_id}: {side} '{market['question'][:50]}' @ {price:.3f} for ${size}")
                    return True
                else:
                    log.warning(f"Trade rejected: {data}")
                    return False
    except Exception as e:
        log.error(f"Trade error: {e}")
        return False

async def resolve_settled_trades(db):
    open_trades = db.get_open_trades()
    if not open_trades:
        return []
    settled = []
    for trade in open_trades:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{GAMMA_API}/markets/{trade['market_id']}",
                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
DEFAULT_THRESHOLDS = {"Politics":75,"Sports":75,"Crypto":75,"World Events":75}

class LearningEngine:
    def __init__(self, db):
        self.db = db
        self.version    = 0
        self.thresholds = DEFAULT_THRESHOLDS.copy()
        self.context    = ""
        self.path       = Path("data/strategy.json")

    def load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                self.thresholds = data.get("thresholds", DEFAULT_THRESHOLDS)
                self.version    = data.get("version", 0)
                self.context    = data.get("context", "")
                log.info(f"Loaded strategy v{self.version}: {self.thresholds}")
                return
            except Exception:
                pass
        row = self.db.get_latest_strategy()
        if row:
            self.thresholds = json.loads(row["thresholds"])
            self.version    = row["version"]
            self.context    = row["notes"]
            log.info(f"Loaded strategy v{self.version} from DB")
        else:
            log.info("Using default strategy")

    def threshold(self, category):
        return self.thresholds.get(category, 75)

    def update_from_outcomes(self, settled):
        for trade in settled:
            cat     = trade.get("category","World Events")
            current = self.thresholds.get(cat, 75)
            if trade["won"]:
                self.thresholds[cat] = max(60, current - 1)
            else:
                self.thresholds[cat] = min(92, current + 2)
        self._save("Auto-updated from outcomes")
        log.info(f"Updated thresholds: {self.thresholds}")

    async def refine(self):
        resolved = self.db.get_resolved_trades(50)
        if len(resolved) < 5:
            log.info("Not enough resolved trades for refinement")
            return
        stats = self.db.get_stats()
        cat_stats = {}
        for cat in CATEGORY_KEYWORDS:
            trades = self.db.get_trades_by_category(cat)
            if not trades:
                cat_stats[cat] = {"trades":0,"win_rate":0,"pnl":0}
                continue
            won = sum(1 for t in trades if t["status"]=="won")
            pnl = sum(t.get("pnl") or 0 for t in trades)
            cat_stats[cat] = {"trades":len(trades),"win_rate":round(won/len(trades)*100,1),"pnl":round(pnl,2)}

        trade_lines = "\n".join(
            f"- [{t['status'].upper()}] {t['side']} '{t['question'][:50]}' "
            f"@ {t['price']:.2f} | conf={t['confidence']}% | P&L=${t.get('pnl') or 0:.2f}"
            for t in resolved[:20]
        )
        prompt = f"""Review this Polymarket trading agent's performance and suggest strategy improvements.

Overall: {stats['total_trades']} trades | {stats['win_rate']:.1f}% win rate | ${stats['total_pnl']:.2f} P&L
Category stats: {json.dumps(cat_stats)}
Current thresholds: {json.dumps(self.thresholds)}
Recent trades:
{trade_lines}

Respond ONLY with valid JSON:
{{"thresholds":{{"Politics":75,"Sports":80,"Crypto":70,"World Events":75}},"analysis":"what's working","key_insight":"main pattern","recommended_changes":"specific changes"}}"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(ANTHROPIC_API,
                    headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},
                    json={"model":"claude-sonnet-4-20250514","max_tokens":600,
                          "messages":[{"role":"user","content":prompt}]},
                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    data = await resp.json()
            text   = "".join(b.get("text","") for b in data.get("content",[]))
            result = json.loads(text.replace("```json","").replace("```","").strip())
            new_t  = result.get("thresholds", self.thresholds)
            self.thresholds = {k: max(55, min(95, int(v))) for k,v in new_t.items()}
            self.context = (
                f"v{self.version+1} — {result.get('analysis','')}\n"
                f"Insight: {result.get('key_insight','')}\n"
                f"Changes: {result.get('recommended_changes','')}"
            )
            self.version += 1
            self._save(self.context)
            self.db.log_strategy(self.version, self.context, self.thresholds, stats["win_rate"], stats["total_trades"])
            log.info(f"Strategy refined to v{self.version}: {self.thresholds}")
            log.info(f"Insight: {result.get('key_insight','')}")
        except Exception as e:
            log.error(f"Refinement error: {e}")

    def _save(self, context=""):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "version":self.version,"thresholds":self.thresholds,"context":context
        },indent=2))

# ── Main Loop ─────────────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info("PolyAgent starting up")
    log.info(f"Wallet:     {WALLET}")
    log.info(f"Paper mode: {PAPER_MODE}")
    log.info(f"Max trade:  ${MAX_TRADE}")
    log.info(f"Interval:   {SCAN_INTERVAL}s")
    log.info("=" * 60)

    db      = Database()
    learner = LearningEngine(db)
    learner.load()
    cycle = 0

    while True:
        try:
            cycle += 1
            log.info(f"\n── Cycle #{cycle} ──────────────────────────────")

            settled = await resolve_settled_trades(db)
            if settled:
                log.info(f"Settled {len(settled)} trades")
                learner.update_from_outcomes(settled)

            if cycle % 10 == 0:
                log.info("Refining strategy…")
                await learner.refine()

            markets = await fetch_markets()
            log.info(f"Analyzing {len(markets)} markets…")
            traded = 0

            for market in markets:
                if db.has_open_position(market["id"]):
                    continue
                stats    = db.get_stats()
                analysis = await analyze_market(market, stats, learner.context)
                if not analysis:
                    continue

                log.info(
                    f"  {market['question'][:55]}…\n"
                    f"  → {analysis['recommendation']} | {analysis['confidence']}% | {analysis['risk_level']} risk"
                )

                if analysis["confidence"] >= learner.threshold(market["category"]) \
                   and analysis["recommendation"] != "HOLD":
                    if await place_trade(market, analysis, db):
                        traded += 1

                await asyncio.sleep(1)

            stats = db.get_stats()
            log.info(
                f"Cycle #{cycle} done — {traded} trades placed\n"
                f"Stats → Total: {stats['total_trades']} | "
                f"Win rate: {stats['win_rate']:.1f}% | "
                f"P&L: ${stats['total_pnl']:.2f} | "
                f"Open: {stats['open_positions']}"
            )

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        log.info(f"Sleeping {SCAN_INTERVAL}s…")
        await asyncio.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
