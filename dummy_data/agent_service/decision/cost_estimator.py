"""Cost estimator — OpenRouter free model, always returns $0.01."""

from __future__ import annotations

from agent_service.config import LLM_COST_PER_CALL


def estimate_cost(event: dict) -> float:
    """Returns estimated cost per LLM call — $0.01 per call."""
    return LLM_COST_PER_CALL
