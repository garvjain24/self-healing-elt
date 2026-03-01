"""
FastAPI Mock REST Server — exposes generated data with failure injection.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, File, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from fake_data_service.generators.ads_generator import AdsGenerator
from fake_data_service.generators.analytics_generator import AnalyticsGenerator
from fake_data_service.generators.crm_generator import CRMGenerator
from fake_data_service.generators.finance_generator import FinanceGenerator
from fake_data_service.generators.manual_generator import ManualGenerator
from fake_data_service.scenario_loader import ScenarioLoader
from fake_data_service.simulation_core.failure_engine import FailureEngine
from fake_data_service.simulation_core.rule_engine import RuleEngine
from fake_data_service.simulation_core.time_engine import TimeEngine

# ── App ──────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Fake Data Service — Controlled Reality Simulator",
    description="Generates scenario-driven marketing data with controlled error injection.",
    version="1.0.0",
)

# ── State ────────────────────────────────────────────────────────────────
_loader = ScenarioLoader()
_active_scenario: Optional[Dict[str, Any]] = None
_failure_engine = FailureEngine()
_rule_engine = RuleEngine()
_ads_gen = AdsGenerator()
_analytics_gen = AnalyticsGenerator()
_crm_gen = CRMGenerator()
_finance_gen = FinanceGenerator()
_manual_gen = ManualGenerator()

# ── Pydantic models ─────────────────────────────────────────────────────

class APIResponse(BaseModel):
    data: List[Dict[str, Any]]
    scenario_id: Optional[str] = None
    errors_injected: List[str] = []
    severity: Optional[str] = None
    requires_human_review: bool = False

class ActivateRequest(BaseModel):
    scenario_id: str

class HealthResponse(BaseModel):
    status: str
    active_scenario: Optional[str] = None

# ── Helpers ──────────────────────────────────────────────────────────────

def _load_scenario(scenario_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return the scenario config — prefer explicit param, fall back to active."""
    if scenario_id:
        try:
            return _loader.load(scenario_id)
        except (FileNotFoundError, ValueError):
            return None
    return _active_scenario


def _apply_failures(
    generator_name: str,
    df: pd.DataFrame,
    scenario: Optional[Dict[str, Any]],
    day_index: int = 0,
) -> tuple[pd.DataFrame, list[str], Optional[str], bool]:
    """
    Run the failure engine and return (df, errors_injected_labels,
    max_severity, requires_human_review).
    """
    if scenario is None:
        return df, [], None, False

    fe = FailureEngine(scenario.get("failure_config", []))
    df = fe.inject_failures(generator_name, df, day_index)
    log = fe.get_injected_log()
    errors = [
        f"{e['failure_type']}:{e.get('details', {}).get('target_field', '')}"
        for e in log
    ]
    severity = fe.get_max_severity()
    needs_review = severity == "CRITICAL"
    return df, errors, severity, needs_review


def _day_index_from_date(date_str: str, scenario: Optional[Dict[str, Any]]) -> int:
    if scenario is None:
        return 0
    start = scenario.get("start_date", date_str)
    try:
        te = TimeEngine(start)
        from datetime import date as _d
        target = _d.fromisoformat(date_str)
        return (target - te.get_current_date()).days
    except Exception:
        return 0

# ── Endpoints ────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(
        status="ok",
        active_scenario=_active_scenario.get("scenario_id") if _active_scenario else None,
    )


@app.get("/ads", response_model=APIResponse)
def get_ads(
    date: str = Query(..., description="YYYY-MM-DD"),
    scenario: Optional[str] = Query(None),
):
    sc = _load_scenario(scenario)
    df = _ads_gen.generate(date)
    day_idx = _day_index_from_date(date, sc)
    df, errors, severity, review = _apply_failures("ads", df, sc, day_idx)
    return APIResponse(
        data=df.to_dict(orient="records"),
        scenario_id=sc.get("scenario_id") if sc else None,
        errors_injected=errors,
        severity=severity,
        requires_human_review=review,
    )


@app.get("/analytics", response_model=APIResponse)
def get_analytics(
    date: str = Query(..., description="YYYY-MM-DD"),
    scenario: Optional[str] = Query(None),
):
    sc = _load_scenario(scenario)
    df = _analytics_gen.generate(date)
    day_idx = _day_index_from_date(date, sc)
    df, errors, severity, review = _apply_failures("analytics", df, sc, day_idx)
    return APIResponse(
        data=df.to_dict(orient="records"),
        scenario_id=sc.get("scenario_id") if sc else None,
        errors_injected=errors,
        severity=severity,
        requires_human_review=review,
    )


@app.get("/crm", response_model=APIResponse)
def get_crm(
    date: str = Query(..., description="YYYY-MM-DD"),
    scenario: Optional[str] = Query(None),
):
    sc = _load_scenario(scenario)
    df = _crm_gen.generate(date)
    day_idx = _day_index_from_date(date, sc)
    df, errors, severity, review = _apply_failures("crm", df, sc, day_idx)
    return APIResponse(
        data=df.to_dict(orient="records"),
        scenario_id=sc.get("scenario_id") if sc else None,
        errors_injected=errors,
        severity=severity,
        requires_human_review=review,
    )


@app.get("/finance", response_model=APIResponse)
def get_finance(
    date: str = Query(..., description="YYYY-MM-DD"),
    scenario: Optional[str] = Query(None),
):
    sc = _load_scenario(scenario)
    ads_df = _ads_gen.generate(date)
    df = _finance_gen.generate(date, ads_df=ads_df)
    day_idx = _day_index_from_date(date, sc)
    df, errors, severity, review = _apply_failures("finance", df, sc, day_idx)
    return APIResponse(
        data=df.to_dict(orient="records"),
        scenario_id=sc.get("scenario_id") if sc else None,
        errors_injected=errors,
        severity=severity,
        requires_human_review=review,
    )


@app.post("/manual/upload", response_model=APIResponse)
async def upload_manual(file: UploadFile = File(...)):
    """Accept a CSV upload, parse it, and return flagged data."""
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        return APIResponse(
            data=[],
            errors_injected=[f"parse_error:{str(e)}"],
            severity="HIGH",
            requires_human_review=True,
        )

    # Basic flagging: look for dollar signs, blank rows, non-numeric spend
    flags: List[str] = []
    for col in df.columns:
        if "$" in col:
            flags.append(f"suspicious_column_name:{col}")
        if df[col].dtype == object:
            has_dollar = df[col].astype(str).str.contains(r"\$", na=False).any()
            if has_dollar:
                flags.append(f"currency_strings_in:{col}")

    blank_rows = df.isnull().all(axis=1).sum()
    if blank_rows > 0:
        flags.append(f"blank_rows:{blank_rows}")

    return APIResponse(
        data=df.fillna("").to_dict(orient="records"),
        errors_injected=flags,
        severity="MEDIUM" if flags else None,
        requires_human_review=len(flags) > 2,
    )


@app.get("/scenario/list")
def list_scenarios():
    return _loader.list_scenarios()


@app.post("/scenario/activate")
def activate_scenario(req: ActivateRequest):
    global _active_scenario
    try:
        sc = _loader.load(req.scenario_id)
        _active_scenario = sc
        return {"status": "activated", "scenario_id": sc["scenario_id"]}
    except (FileNotFoundError, ValueError) as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
