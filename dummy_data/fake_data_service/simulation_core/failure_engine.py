"""
Failure Engine — reads scenario YAML and injects controlled errors into generator output.

Supported failure types
-----------------------
missing_rows      – randomly drop N% of rows
null_fields       – set specific fields to null
column_rename     – rename a column
duplicate_rows    – duplicate N% of rows
wrong_dtype       – convert numeric column to string
logic_break       – corrupt a calculated field
late_data         – shift date column forward by N days
schema_drift      – add or remove a column
"""

from __future__ import annotations

import copy
import random
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd


class FailureEngine:
    """Inject failures into a generator's DataFrame based on scenario config."""

    def __init__(self, failure_configs: Optional[List[Dict[str, Any]]] = None):
        """
        Args:
            failure_configs: list of failure dicts from scenario YAML, e.g.
                [{"generator": "finance", "failure_type": "logic_break", ...}]
        """
        self._configs = failure_configs or []
        self._injected_log: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def inject_failures(
        self,
        generator_name: str,
        df: pd.DataFrame,
        current_day_index: int,
    ) -> pd.DataFrame:
        """
        Apply any failures that match *generator_name* and *current_day_index*.

        Returns a (possibly mutated) copy of *df*.
        """
        self._injected_log.clear()
        df = df.copy()

        for cfg in self._configs:
            if cfg.get("generator") != generator_name:
                continue

            day_offset = cfg.get("day_offset")
            # If day_offset is specified, only inject on that day
            if day_offset is not None and day_offset != current_day_index:
                continue

            failure_type = cfg.get("failure_type", "")
            handler = self._FAILURE_HANDLERS.get(failure_type)
            if handler is None:
                continue

            df = handler(self, df, cfg)
            self._injected_log.append({
                "failure_type": failure_type,
                "generator": generator_name,
                "severity": cfg.get("severity", "LOW"),
                "day_index": current_day_index,
                "details": cfg,
            })

        return df

    def get_injected_log(self) -> List[Dict[str, Any]]:
        """Return the log of failures injected during the last call."""
        return list(self._injected_log)

    def get_max_severity(self) -> Optional[str]:
        """Return the highest severity among injected failures."""
        order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        if not self._injected_log:
            return None
        return max(
            (entry["severity"] for entry in self._injected_log),
            key=lambda s: order.get(s, -1),
        )

    # ------------------------------------------------------------------
    # Failure handlers
    # ------------------------------------------------------------------

    def _handle_missing_rows(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        pct = cfg.get("percentage", 0.1)
        n_drop = max(1, int(len(df) * pct))
        if n_drop >= len(df):
            return df.head(1)
        drop_idx = random.sample(list(df.index), n_drop)
        return df.drop(drop_idx).reset_index(drop=True)

    def _handle_null_fields(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        target = cfg.get("target_field")
        if target and target in df.columns:
            n_null = max(1, int(len(df) * cfg.get("percentage", 0.3)))
            idxs = random.sample(list(df.index), min(n_null, len(df)))
            df.loc[idxs, target] = None
        return df

    def _handle_column_rename(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        old = cfg.get("old_name") or cfg.get("target_field", "")
        new = cfg.get("new_name", f"{old}_renamed")
        if old in df.columns:
            df = df.rename(columns={old: new})
        return df

    def _handle_duplicate_rows(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        pct = cfg.get("percentage", 0.1)
        n_dup = max(1, int(len(df) * pct))
        dup_idx = random.choices(list(df.index), k=n_dup)
        dups = df.loc[dup_idx]
        return pd.concat([df, dups], ignore_index=True)

    def _handle_wrong_dtype(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        target = cfg.get("target_field")
        if target and target in df.columns:
            df[target] = df[target].astype(str)
        return df

    def _handle_logic_break(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        target = cfg.get("target_field")
        if target and target in df.columns:
            # Corrupt the field: negate, add large offset, etc.
            df[target] = df[target].apply(
                lambda x: -abs(float(x)) if isinstance(x, (int, float)) else x
            )
        return df

    def _handle_late_data(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        lag = cfg.get("lag_days", 3)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]) + timedelta(days=lag)
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        elif "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"]) + timedelta(days=lag)
            df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")
        return df

    def _handle_schema_drift(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        action = cfg.get("drift_action", "add")
        col = cfg.get("target_field", "extra_col")
        if action == "remove" and col in df.columns:
            df = df.drop(columns=[col])
        elif action == "add":
            df[col] = "UNKNOWN"
        return df

    # ------------------------------------------------------------------
    # Dispatch table
    # ------------------------------------------------------------------

    _FAILURE_HANDLERS = {
        "missing_rows": _handle_missing_rows,
        "null_fields": _handle_null_fields,
        "column_rename": _handle_column_rename,
        "duplicate_rows": _handle_duplicate_rows,
        "wrong_dtype": _handle_wrong_dtype,
        "logic_break": _handle_logic_break,
        "late_data": _handle_late_data,
        "schema_drift": _handle_schema_drift,
    }
