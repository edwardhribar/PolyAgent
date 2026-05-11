"""
Re-resolve script - fixes trades that were incorrectly marked as won/lost
due to the Over/Under outcome naming bug.
Run once: python fix_resolutions.py
"""

import asyncio
import aiohttp
import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("fix")

GAMMA_API = "https://gamma-api.polymarket.com"

DBS = [
    "data/polyagent.db",   # Conservative bot
    "data/hv.db",          # HV bot
]

def get_conn(path):
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c

async def check_market(mid, session):
    for params in [None, {"closed": "true"}, {"archived": "true"}]:
        try:
            url = f"{GAMMA_API}/markets/{mid}" if not params else f"{GAMMA_API}/markets"
            kwargs = {"params": {"id": mid, **params}} if params else {}
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), **kwargs) as r:
                if r.status != 200: continue
                data = await r.json()
                market = data[0] if isinstance(data, list) and data else data
                if isinstance(market, dict) and (market.get("resolved") or market.get("closed") or market.get("winningOutcome")):
                    return market
        except: continue
    return None

async def fix_db(db_path):
    if not Path(db_path).exists():
        log.info(f"Skipping {db_path} — not found")
        return

    conn = get_conn(db_path)
    trades = [dict(r) for r in conn.execute(
        "SELECT * FROM trades WHERE status IN ('won','lost')").fetchall()]
    log.info(f"{db_path}: checking {len(trades)} resolved trades")

    fixed = 0
    async with aiohttp.ClientSession() as session:
        for trade in trades:
            mid = trade["market_id"]
            try:
                market = await check_market(mid, session)
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

                # Correct outcome mapping
                outcomes = market.get("outcomes") or ["YES","NO"]
                winning_idx = None
                for i, o in enumerate(outcomes):
                    if str(o).upper().strip() == winning:
                        winning_idx = i
                        break

                our_side = trade["side"].upper()
                if winning_idx is not None:
                    should_have_won = (our_side == "YES" and winning_idx == 0) or \
                                     (our_side == "NO" and winning_idx == 1)
                else:
                    should_have_won = our_side == winning

                current_status = trade["status"]
                correct_status = "won" if should_have_won else "lost"

                if current_status != correct_status:
                    payout = trade["size"] / trade["price"] if should_have_won else 0.0
                    pnl = payout - trade["size"]
                    conn.execute(
                        "UPDATE trades SET status=?, outcome=?, pnl=? WHERE id=?",
                        (correct_status, payout, pnl, trade["id"]))
                    conn.commit()
                    log.info(f"FIXED #{trade['id']}: {current_status} → {correct_status} | P&L: ${pnl:.2f} | '{trade['question'][:50]}'")
                    log.info(f"  side={our_side} winning={winning} outcomes={outcomes} idx={winning_idx}")
                    fixed += 1
                else:
                    log.info(f"OK #{trade['id']}: {current_status} correct | '{trade['question'][:40]}'")

            except Exception as e:
                log.debug(f"Error checking {mid}: {e}")

    log.info(f"{db_path}: fixed {fixed} trades")
    conn.close()

async def main():
    log.info("Starting re-resolution fix...")
    for db in DBS:
        await fix_db(db)
    log.info("Done!")

if __name__ == "__main__":
    asyncio.run(main())
