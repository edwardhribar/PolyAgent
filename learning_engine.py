"""
SQLite database — stores trades, outcomes, and strategy history.
"""
import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("polyagent.db")


class Database:
    def __init__(self, path="data/polyagent.db"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
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
                    side        TEXT NOT NULL,       -- YES or NO
                    price       REAL NOT NULL,       -- entry price (0-1)
                    size        REAL NOT NULL,       -- $ amount
                    confidence  INTEGER,
                    fair_value  REAL,
                    reasoning   TEXT,
                    paper       INTEGER DEFAULT 1,   -- 1=paper, 0=real
                    status      TEXT DEFAULT 'open', -- open/won/lost/voided
                    outcome     REAL,               -- payout received
                    pnl         REAL,               -- profit/loss
                    placed_at   TEXT,
                    resolved_at TEXT,
                    order_id    TEXT
                );

                CREATE TABLE IF NOT EXISTS strategy_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp   TEXT,
                    version     INTEGER,
                    notes       TEXT,
                    thresholds  TEXT,   -- JSON
                    win_rate    REAL,
                    total_trades INTEGER
                );

                CREATE TABLE IF NOT EXISTS market_cache (
                    market_id   TEXT PRIMARY KEY,
                    question    TEXT,
                    category    TEXT,
                    data        TEXT,   -- JSON
                    cached_at   TEXT
                );
            """)
        log.info(f"Database ready at {self.path}")

    # ── Trades ────────────────────────────────────────────────────

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
            trade_id = cur.lastrowid
        log.info(f"Recorded {'paper' if paper else 'REAL'} trade #{trade_id}: {side} {market['question'][:40]}")
        return trade_id

    def get_open_trades(self):
        with self._conn() as db:
            rows = db.execute("SELECT * FROM trades WHERE status='open'").fetchall()
        return [dict(r) for r in rows]

    def resolve_trade(self, trade_id, won: bool, payout: float):
        pnl = payout - dict(self.get_trade(trade_id))["size"]
        now = datetime.now(timezone.utc).isoformat()
        status = "won" if won else "lost"
        with self._conn() as db:
            db.execute("""
                UPDATE trades SET status=?, outcome=?, pnl=?, resolved_at=?
                WHERE id=?
            """, (status, payout, pnl, now, trade_id))
        log.info(f"Trade #{trade_id} resolved: {status} | P&L: ${pnl:.2f}")

    def get_trade(self, trade_id):
        with self._conn() as db:
            row = db.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        return dict(row) if row else None

    def has_open_position(self, market_id):
        with self._conn() as db:
            row = db.execute(
                "SELECT id FROM trades WHERE market_id=? AND status='open'", (market_id,)
            ).fetchone()
        return row is not None

    def get_resolved_trades(self, limit=100):
        with self._conn() as db:
            rows = db.execute(
                "SELECT * FROM trades WHERE status IN ('won','lost') ORDER BY resolved_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_trades_by_category(self, category):
        with self._conn() as db:
            rows = db.execute(
                "SELECT * FROM trades WHERE category=? AND status IN ('won','lost')", (category,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Stats ─────────────────────────────────────────────────────

    def get_stats(self):
        with self._conn() as db:
            total = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            open_pos = db.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            won = db.execute("SELECT COUNT(*) FROM trades WHERE status='won'").fetchone()[0]
            lost = db.execute("SELECT COUNT(*) FROM trades WHERE status='lost'").fetchone()[0]
            pnl_row = db.execute("SELECT SUM(pnl) FROM trades WHERE pnl IS NOT NULL").fetchone()
            total_pnl = pnl_row[0] or 0.0
        resolved = won + lost
        win_rate = (won / resolved * 100) if resolved > 0 else 0.0
        return {
            "total_trades": total,
            "open_positions": open_pos,
            "won": won,
            "lost": lost,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
        }

    # ── Strategy log ──────────────────────────────────────────────

    def log_strategy(self, version, notes, thresholds, win_rate, total_trades):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            db.execute("""
                INSERT INTO strategy_log (timestamp, version, notes, thresholds, win_rate, total_trades)
                VALUES (?,?,?,?,?,?)
            """, (now, version, notes, json.dumps(thresholds), win_rate, total_trades))

    def get_latest_strategy(self):
        with self._conn() as db:
            row = db.execute(
                "SELECT * FROM strategy_log ORDER BY version DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None
