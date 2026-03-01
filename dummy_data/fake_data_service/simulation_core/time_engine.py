"""
Time Engine — manages simulated date progression for the data pipeline simulator.
"""

from datetime import date, timedelta
from typing import List


class TimeEngine:
    """Simulates date progression. Generators query this for the current simulated date."""

    def __init__(self, start_date: str = "2024-01-01"):
        """
        Args:
            start_date: ISO-format date string (YYYY-MM-DD) for the simulation start.
        """
        self._start_date = date.fromisoformat(start_date)
        self._current_date = self._start_date
        self._day_offset = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_current_date(self) -> date:
        """Return the current simulated date."""
        return self._current_date

    def advance_day(self) -> date:
        """Move the clock forward by one day and return the new date."""
        self._day_offset += 1
        self._current_date = self._start_date + timedelta(days=self._day_offset)
        return self._current_date

    def get_date_range(self, n_days: int) -> List[date]:
        """Return a list of *n_days* dates starting from the current date."""
        return [self._current_date + timedelta(days=i) for i in range(n_days)]

    def get_lagged_date(self, lag_days: int = 0) -> date:
        """Return the current date minus *lag_days* — used for late-data simulation."""
        return self._current_date - timedelta(days=lag_days)

    def get_day_index(self) -> int:
        """Return 0-based index of how many days have elapsed since start."""
        return self._day_offset

    def reset(self) -> None:
        """Reset the clock back to the start date."""
        self._current_date = self._start_date
        self._day_offset = 0

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"TimeEngine(start={self._start_date.isoformat()}, "
            f"current={self._current_date.isoformat()}, day_index={self._day_offset})"
        )
