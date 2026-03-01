"""
Scenario Loader — reads and validates scenario YAML files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


_SCENARIOS_DIR = Path(__file__).resolve().parent / "scenarios"

_REQUIRED_KEYS = {"scenario_id", "description", "duration_days", "start_date", "generators"}


class ScenarioLoader:
    """Load, validate, and query scenario YAML files."""

    def __init__(self, scenarios_dir: Optional[str] = None):
        self._dir = Path(scenarios_dir) if scenarios_dir else _SCENARIOS_DIR
        self._cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, scenario_id: str) -> Dict[str, Any]:
        """
        Load a scenario by *scenario_id* (matches filename stem or the
        ``scenario_id`` field inside the YAML).

        Raises:
            FileNotFoundError – if no matching YAML exists.
            ValueError – if the YAML is missing required keys.
        """
        if scenario_id in self._cache:
            return self._cache[scenario_id]

        # Try filename first, then scan all files for matching scenario_id
        path = self._dir / f"{scenario_id}.yaml"
        if not path.exists():
            path = self._dir / f"{scenario_id}.yml"
        if not path.exists():
            path = self._find_by_id(scenario_id)

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        self._validate(data, path.name)
        self._cache[scenario_id] = data
        return data

    def list_scenarios(self) -> List[Dict[str, str]]:
        """Return a list of ``{scenario_id, description}`` for every YAML in the directory."""
        results: List[Dict[str, str]] = []
        for p in sorted(self._dir.glob("*.yaml")):
            with open(p) as f:
                data = yaml.safe_load(f)
            results.append(
                {
                    "scenario_id": data.get("scenario_id", p.stem),
                    "description": data.get("description", ""),
                }
            )
        for p in sorted(self._dir.glob("*.yml")):
            with open(p) as f:
                data = yaml.safe_load(f)
            results.append(
                {
                    "scenario_id": data.get("scenario_id", p.stem),
                    "description": data.get("description", ""),
                }
            )
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_by_id(self, scenario_id: str) -> Path:
        for ext in ("*.yaml", "*.yml"):
            for p in self._dir.glob(ext):
                with open(p) as f:
                    data = yaml.safe_load(f)
                if data.get("scenario_id") == scenario_id:
                    return p
        raise FileNotFoundError(
            f"No scenario YAML found for id '{scenario_id}' in {self._dir}"
        )

    @staticmethod
    def _validate(data: Dict[str, Any], filename: str) -> None:
        missing = _REQUIRED_KEYS - set(data.keys())
        if missing:
            raise ValueError(
                f"Scenario '{filename}' is missing required keys: {missing}"
            )
