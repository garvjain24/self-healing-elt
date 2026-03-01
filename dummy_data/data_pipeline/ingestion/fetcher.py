"""
HTTP client for the fake data service.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from data_pipeline.config import FAKE_DATA_SERVICE_URL

logger = logging.getLogger(__name__)


class PipelineIngestionError(Exception):
    """Raised when the fake data service is unreachable after retries."""


class DataServiceClient:
    """HTTP client with retry logic for the fake data service."""

    def __init__(self, base_url: str | None = None, max_retries: int = 3):
        self.base_url = (base_url or FAKE_DATA_SERVICE_URL).rstrip("/")
        self.max_retries = max_retries

    def fetch(self, domain: str, date: str) -> dict[str, Any]:
        """
        GET /{domain}?date={date} with exponential backoff.
        Returns the full response dict including envelope fields.
        """
        url = f"{self.base_url}/{domain}"
        params = {"date": date}

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=30.0) as client:
                    resp = client.get(url, params=params)
                    resp.raise_for_status()
                    return resp.json()
            except (httpx.HTTPError, httpx.ConnectError) as e:
                last_err = e
                wait = 2 ** attempt
                logger.warning(
                    "Fetch %s attempt %d/%d failed: %s. Retrying in %ds.",
                    url, attempt, self.max_retries, e, wait,
                )
                time.sleep(wait)

        raise PipelineIngestionError(
            f"Failed to fetch {url} after {self.max_retries} retries: {last_err}"
        )

    def activate_scenario(self, scenario_id: str) -> None:
        """POST /scenario/activate."""
        url = f"{self.base_url}/scenario/activate"
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, json={"scenario_id": scenario_id})
            resp.raise_for_status()

    def list_scenarios(self) -> list[dict]:
        """GET /scenario/list."""
        url = f"{self.base_url}/scenario/list"
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()

    def health_check(self) -> bool:
        """GET /health — returns True if service is up."""
        try:
            url = f"{self.base_url}/health"
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url)
                return resp.status_code == 200
        except Exception:
            return False
