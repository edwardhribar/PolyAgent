"""
Configuration — edit these values or set as environment variables.
"""
import os


class Config:
    # ── Wallet ────────────────────────────────────────────────────
    WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "0xd45996A1d51A0C478cb499b1bc24386734000C9f")
    POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
    POLYMARKET_SECRET = os.getenv("POLYMARKET_SECRET", "")  # if required by CLOB
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

    # ── Trading ───────────────────────────────────────────────────
    PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"  # Set false for real trades
    MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "10"))        # $ per trade
    MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))   # concurrent trades
    MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "5000"))        # skip illiquid markets
    MIN_VOLUME = float(os.getenv("MIN_VOLUME", "10000"))             # skip low-volume markets
    BASE_CONFIDENCE_THRESHOLD = int(os.getenv("BASE_CONFIDENCE", "75"))  # min AI confidence %

    # ── Scanning ──────────────────────────────────────────────────
    SCAN_INTERVAL_SECS = int(os.getenv("SCAN_INTERVAL_SECS", "300"))  # 5 minutes
    MARKETS_PER_SCAN = int(os.getenv("MARKETS_PER_SCAN", "20"))

    # ── Categories to trade ───────────────────────────────────────
    CATEGORIES = ["Politics", "Sports", "Crypto", "World Events"]

    # ── Notifications (optional) ──────────────────────────────────
    NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "")       # email for trade alerts
    SENDGRID_KEY = os.getenv("SENDGRID_KEY", "")       # SendGrid API key

    # ── Paths ─────────────────────────────────────────────────────
    DB_PATH = "data/polyagent.db"
    STRATEGY_PATH = "data/strategy.json"
