"""LLM-assisted fix handler — two-call pattern via OpenRouter."""

from __future__ import annotations

import json
import logging
import re

import requests

from agent_service.config import (
    OPENROUTER_API_KEY,
    OPENROUTER_URL,
    OPENROUTER_MODEL,
    LLM_COST_PER_CALL,
)
from agent_service.memory.memory_store import MemoryStore, make_signature

logger = logging.getLogger(__name__)


def handle_llm_fix(event: dict, payload: dict, policy: dict) -> tuple[bool, float]:
    """
    Two-call OpenRouter pattern:
    1. Initial fix proposal with reasoning
    2. Self-review for confidence calibration
    Returns (success, cost_usd).
    """
    cost = LLM_COST_PER_CALL

    user_prompt = f"""Data quality event to fix:
- event_type: {event['event_type']}
- domain: {event['domain']}
- date: {event.get('date')}
- metric_name: {event.get('metric_name')}
- observed_value: {event.get('observed_value')}
- expected_value: {event.get('expected_value')}
- deviation_pct: {event.get('deviation_pct')}
- detail: {event.get('detail')}
- pipeline_stage: {event.get('pipeline_stage')}

Domain rules for {event.get('domain', 'unknown')}:
- ads: impressions >= clicks >= conversions, spend = clicks * cpc
- analytics: conversion_events <= sessions, funnel steps decrease monotonically
- crm: revenue only populated for closed_won status
- finance: roas = revenue/spend, profit = revenue-spend, cac = spend/conversions

Propose a deterministic remediation. Respond ONLY in this exact JSON format with no markdown fences:
{{
  "action": "description of what to fix",
  "fix_type": "schema_mapping | dtype_cast | data_correction | flag_only",
  "fix_payload": {{}},
  "explanation": "one sentence",
  "confidence": 0.0
}}"""

    # ── First call ──
    try:
        resp1 = requests.post(
            url=OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=60,
        )
        resp1.raise_for_status()
    except Exception as e:
        logger.error("[LLM] OpenRouter call 1 failed: %s", e)
        return False, cost

    resp1_json = resp1.json()
    try:
        assistant_msg = resp1_json["choices"][0]["message"]
        first_content = assistant_msg.get("content", "")
    except (KeyError, IndexError) as e:
        logger.error("[LLM] Bad response structure: %s", e)
        return False, cost

    # ── Second call — self-review ──
    messages = [
        {"role": "user", "content": user_prompt},
        {"role": "assistant", "content": first_content},
        {
            "role": "user",
            "content": (
                "Review your proposed fix. "
                "Is the confidence score accurate? "
                "Confirm or revise your JSON response."
            ),
        },
    ]

    cost += LLM_COST_PER_CALL  # second call
    try:
        resp2 = requests.post(
            url=OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": messages,
            },
            timeout=60,
        )
        resp2.raise_for_status()
        final_content = resp2.json()["choices"][0]["message"].get("content", "")
    except Exception as e:
        logger.warning("[LLM] Second call failed, using first response: %s", e)
        final_content = first_content

    # ── Parse JSON ──
    try:
        cleaned = re.sub(r"```json|```", "", final_content).strip()
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            raise ValueError("No JSON object found")
        result = json.loads(match.group())
    except Exception as e:
        logger.error("[LLM] Parse failed: %s | raw: %s", e, final_content[:300])
        return False, cost

    confidence = float(result.get("confidence", 0))
    logger.info(
        "[LLM] event_type=%s domain=%s confidence=%.2f fix_type=%s",
        event.get("event_type"), event.get("domain"), confidence, result.get("fix_type"),
    )

    if confidence < policy["llm_confidence_threshold"]:
        logger.info(
            "[LLM] Confidence %.2f below threshold %.2f — escalating",
            confidence, policy["llm_confidence_threshold"],
        )
        return False, cost

    # ── Store accepted fix in memory ──
    memory = MemoryStore()
    signature = make_signature(event)
    memory.update(
        signature=signature,
        domain=event["domain"],
        event_type=event["event_type"],
        action="llm_fix",
        payload=result,
        success=True,
        llm_cost=cost,
    )

    return True, cost
