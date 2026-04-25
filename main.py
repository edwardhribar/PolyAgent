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
    "S
