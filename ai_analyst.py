"""
PolyAgent - Autonomous Polymarket Trading Bot
Scans markets, analyzes with Claude AI, places trades, learns from outcomes.
"""

import os
import json
import time
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from pathlib import Path

from config import Config
from market_scanner import MarketScanner
from ai_analyst import AIAnalyst
from trade_executor import TradeExecutor
from learning_engine import LearningEngine
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/agent.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("polyagent")


class PolyAgent:
    def __init__(self):
        self.config = Config()
        self.db = Database()
        self.scanner = MarketScanner(self.config)
        self.analyst = AIAnalyst(self.config, self.db)
        self.executor = TradeExecutor(self.config)
        self.learner = LearningEngine(self.db, self.analyst)
        self.cycle = 0

    async def run(self):
        log.info("=" * 60)
        log.info("PolyAgent starting up")
        log.info(f"Wallet: {self.config.WALLET_ADDRESS}")
        log.info(f"Paper mode: {self.config.PAPER_MODE}")
        log.info(f"Max trade size: ${self.config.MAX_TRADE_SIZE}")
        log.info(f"Scan interval: {self.config.SCAN_INTERVAL_SECS}s")
        log.info("=" * 60)

        # Initial learning load
        await self.learner.load_strategy()

        while True:
            try:
                await self.run_cycle()
            except Exception as e:
                log.error(f"Cycle error: {e}", exc_info=True)

            log.info(f"Sleeping {self.config.SCAN_INTERVAL_SECS}s until next cycle…")
            await asyncio.sleep(self.config.SCAN_INTERVAL_SECS)

    async def run_cycle(self):
        self.cycle += 1
        log.info(f"\n── Cycle #{self.cycle} ──────────────────────────────")

        # 1. Resolve any settled trades
        settled = await self.executor.resolve_settled_trades(self.db)
        if settled:
            log.info(f"Settled {len(settled)} trades, updating learning engine…")
            await self.learner.update_from_outcomes(settled)

        # 2. Re-learn every 10 cycles
        if self.cycle % 10 == 0:
            log.info("Running learning update…")
            await self.learner.refine_strategy()

        # 3. Scan markets
        markets = await self.scanner.get_markets()
        log.info(f"Found {len(markets)} markets to analyze")

        # 4. Analyze and trade
        traded = 0
        for market in markets:
            # Skip if already have open position
            if self.db.has_open_position(market["id"]):
                continue

            analysis = await self.analyst.analyze(market)
            if not analysis:
                continue

            log.info(
                f"Market: {market['question'][:60]}…\n"
                f"  → {analysis['recommendation']} | "
                f"Confidence: {analysis['confidence']}% | "
                f"Risk: {analysis['risk_level']}"
            )

            # Only trade if confidence exceeds dynamic threshold
            threshold = self.learner.get_confidence_threshold(market["category"])
            if analysis["confidence"] >= threshold and analysis["recommendation"] != "HOLD":
                success = await self.executor.place_trade(market, analysis, self.db)
                if success:
                    traded += 1
                    log.info(f"  ✓ Trade placed!")

            # Avoid hammering the AI API
            await asyncio.sleep(1)

        log.info(f"Cycle #{self.cycle} complete — {traded} trades placed")
        self._log_stats()

    def _log_stats(self):
        stats = self.db.get_stats()
        log.info(
            f"Stats → Total: {stats['total_trades']} | "
            f"Win rate: {stats['win_rate']:.1f}% | "
            f"P&L: ${stats['total_pnl']:.2f} | "
            f"Open: {stats['open_positions']}"
        )


if __name__ == "__main__":
    agent = PolyAgent()
    asyncio.run(agent.run())
