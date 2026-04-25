"""
Learning Engine — analyzes trade outcomes and refines strategy over time.
Uses Claude to review performance and adjust confidence thresholds per category.
"""
import json
import logging
import aiohttp
from pathlib import Path

log = logging.getLogger("polyagent.learner")

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"

DEFAULT_THRESHOLDS = {
    "Politics":     75,
    "Sports":       75,
    "Crypto":       75,
    "World Events": 75,
}


class LearningEngine:
    def __init__(self, db, analyst):
        self.db = db
        self.analyst = analyst
        self.strategy_version = 0
        self.thresholds = DEFAULT_THRESHOLDS.copy()
        self.strategy_path = Path("data/strategy.json")

    async def load_strategy(self):
        """Load saved strategy from disk or DB."""
        if self.strategy_path.exists():
            try:
                data = json.loads(self.strategy_path.read_text())
                self.thresholds = data.get("thresholds", DEFAULT_THRESHOLDS)
                self.strategy_version = data.get("version", 0)
                self.analyst.strategy_context = data.get("context", "")
                log.info(f"Loaded strategy v{self.strategy_version}: {self.thresholds}")
                return
            except Exception as e:
                log.warning(f"Failed to load strategy file: {e}")

        # Try DB fallback
        row = self.db.get_latest_strategy()
        if row:
            self.thresholds = json.loads(row["thresholds"])
            self.strategy_version = row["version"]
            self.analyst.strategy_context = row["notes"]
            log.info(f"Loaded strategy v{self.strategy_version} from DB")
        else:
            log.info("No saved strategy, using defaults")

    def get_confidence_threshold(self, category: str) -> int:
        return self.thresholds.get(category, DEFAULT_THRESHOLDS.get(category, 75))

    async def update_from_outcomes(self, settled_trades: list):
        """Quick update after trades settle — adjust thresholds based on recent outcomes."""
        for trade in settled_trades:
            category = trade.get("category", "World Events")
            current = self.thresholds.get(category, 75)

            if trade["won"]:
                # Slightly lower threshold (more aggressive) if winning
                self.thresholds[category] = max(60, current - 1)
            else:
                # Raise threshold (more conservative) if losing
                self.thresholds[category] = min(92, current + 2)

        self._save_strategy("Auto-updated from trade outcomes")
        log.info(f"Updated thresholds: {self.thresholds}")

    async def refine_strategy(self):
        """Full strategy review using Claude — runs every 10 cycles."""
        resolved = self.db.get_resolved_trades(limit=50)
        if len(resolved) < 5:
            log.info("Not enough resolved trades yet for strategy refinement")
            return

        stats = self.db.get_stats()
        category_stats = self._compute_category_stats()

        prompt = f"""You are reviewing the performance of an autonomous Polymarket trading agent.

Overall performance:
- Total trades: {stats['total_trades']}
- Win rate: {stats['win_rate']:.1f}%
- Total P&L: ${stats['total_pnl']:.2f}
- Open positions: {stats['open_positions']}

Performance by category:
{json.dumps(category_stats, indent=2)}

Recent trade outcomes (last 50):
{self._format_trades(resolved[:20])}

Current confidence thresholds by category:
{json.dumps(self.thresholds, indent=2)}

Based on this data:
1. Which categories are performing well vs poorly?
2. Should any confidence thresholds be adjusted?
3. Are there patterns in winning vs losing trades?
4. What strategy adjustments would improve P&L?

Respond ONLY with valid JSON:
{{
  "thresholds": {{"Politics": 75, "Sports": 80, "Crypto": 70, "World Events": 75}},
  "analysis": "2-3 sentences on what's working and what isn't",
  "key_insight": "The single most important pattern observed",
  "recommended_changes": "Specific changes to improve performance"
}}"""

        try:
            if not self.analyst.config.ANTHROPIC_API_KEY:
                return

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    ANTHROPIC_API,
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": self.analyst.config.ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 800,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    data = await resp.json()

            text = "".join(b.get("text", "") for b in data.get("content", []))
            clean = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean)

            # Apply new thresholds
            new_thresholds = result.get("thresholds", self.thresholds)
            self.thresholds = {k: max(55, min(95, int(v))) for k, v in new_thresholds.items()}

            # Update strategy context for future analyses
            context = (
                f"Strategy v{self.strategy_version + 1} insights:\n"
                f"{result.get('analysis', '')}\n"
                f"Key insight: {result.get('key_insight', '')}\n"
                f"Changes: {result.get('recommended_changes', '')}"
            )
            self.analyst.strategy_context = context
            self.strategy_version += 1

            self._save_strategy(context)
            self.db.log_strategy(
                self.strategy_version, context,
                self.thresholds, stats["win_rate"], stats["total_trades"]
            )

            log.info(f"Strategy refined to v{self.strategy_version}")
            log.info(f"New thresholds: {self.thresholds}")
            log.info(f"Insight: {result.get('key_insight', '')}")

        except Exception as e:
            log.error(f"Strategy refinement error: {e}")

    def _compute_category_stats(self):
        stats = {}
        for cat in ["Politics", "Sports", "Crypto", "World Events"]:
            trades = self.db.get_trades_by_category(cat)
            if not trades:
                stats[cat] = {"trades": 0, "win_rate": 0, "pnl": 0}
                continue
            won = sum(1 for t in trades if t["status"] == "won")
            pnl = sum(t.get("pnl") or 0 for t in trades)
            stats[cat] = {
                "trades": len(trades),
                "win_rate": round(won / len(trades) * 100, 1),
                "pnl": round(pnl, 2),
            }
        return stats

    def _format_trades(self, trades):
        lines = []
        for t in trades:
            lines.append(
                f"- [{t['status'].upper()}] {t['side']} '{t['question'][:50]}' "
                f"@ {t['price']:.2f} | conf={t['confidence']}% | P&L=${t.get('pnl') or 0:.2f}"
            )
        return "\n".join(lines)

    def _save_strategy(self, context=""):
        data = {
            "version": self.strategy_version,
            "thresholds": self.thresholds,
            "context": context,
        }
        self.strategy_path.parent.mkdir(parents=True, exist_ok=True)
        self.strategy_path.write_text(json.dumps(data, indent=2))
