"""
AI Analyst — uses Claude to analyze markets, informed by learned strategy.
"""
import json
import logging
import aiohttp

log = logging.getLogger("polyagent.analyst")

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"


class AIAnalyst:
    def __init__(self, config, db):
        self.config = config
        self.db = db
        self.strategy_context = ""  # injected by LearningEngine

    async def analyze(self, market: dict) -> dict | None:
        """Analyze a market and return a trading recommendation."""
        if not self.config.ANTHROPIC_API_KEY:
            log.error("No Anthropic API key set")
            return None

        stats = self.db.get_stats()
        prompt = self._build_prompt(market, stats)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    ANTHROPIC_API,
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": self.config.ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 600,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"Anthropic API error: {resp.status}")
                        return None
                    data = await resp.json()

            text = "".join(b.get("text", "") for b in data.get("content", []))
            clean = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean)

            # Validate required fields
            assert result.get("recommendation") in ("BUY_YES", "BUY_NO", "HOLD")
            assert 0 <= result.get("confidence", 0) <= 100
            result["category"] = market.get("category")
            return result

        except json.JSONDecodeError as e:
            log.warning(f"Failed to parse AI response: {e}")
            return None
        except Exception as e:
            log.error(f"AI analysis error: {e}")
            return None

    def _build_prompt(self, market: dict, stats: dict) -> str:
        strategy_section = f"\n\nCurrent learned strategy:\n{self.strategy_context}" if self.strategy_context else ""

        return f"""You are an autonomous prediction market trading agent. Your goal is to maximize profit over time.

Market to analyze:
- Question: "{market['question']}"
- YES price: {market['yes_price']:.3f} ({market['yes_price']*100:.1f}% implied probability)
- NO price: {market['no_price']:.3f}
- Volume: ${market['volume']:,.0f}
- Liquidity: ${market['liquidity']:,.0f}
- Category: {market.get('category', 'Unknown')}
- Ends: {market.get('end_date', 'Unknown')}

Agent performance so far:
- Total trades: {stats['total_trades']}
- Win rate: {stats['win_rate']:.1f}%
- Total P&L: ${stats['total_pnl']:.2f}
{strategy_section}

Analyze this market carefully. Look for:
1. Price vs fair value discrepancy (edge)
2. Market sentiment bias
3. Liquidity and exit risk
4. Time horizon risk

Respond ONLY with valid JSON, no markdown:
{{"recommendation":"BUY_YES","confidence":78,"edge":"Market underpricing X due to Y","reasoning":"2-3 sentence analysis of why this is a good or bad trade.","risk_level":"MEDIUM","fair_value":0.72,"suggested_size_pct":0.5}}

recommendation: BUY_YES | BUY_NO | HOLD
confidence: 0-100
risk_level: LOW | MEDIUM | HIGH
fair_value: estimated true probability 0-1
suggested_size_pct: 0.25 to 1.0 (fraction of max trade size)"""
