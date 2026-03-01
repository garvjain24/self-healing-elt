"""
Manual Generator — produces CSV and PDF files simulating human-uploaded reports.

Intentional human errors are injected directly (this is the one generator
where "errors" are part of normal output, not from the failure engine).
"""

from __future__ import annotations

import csv
import io
import os
import random
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet


class ManualGenerator:
    """Generate CSV and PDF files mimicking human-uploaded spend/revenue reports."""

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_csv(
        self,
        date_str: str,
        output_dir: str,
        inject_human_errors: bool = True,
        num_rows: int = 10,
    ) -> str:
        """
        Write a CSV to *output_dir* and return the file path.

        When *inject_human_errors* is True, the CSV will contain realistic
        human mistakes (wrong column names, bad date formats, numeric strings,
        extra blank rows, occasional typo zeros).
        """
        rows = self._build_rows(date_str, num_rows, inject_human_errors)
        headers = list(rows[0].keys()) if rows else []

        os.makedirs(output_dir, exist_ok=True)
        filename = f"manual_report_{date_str}.csv"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
                # Extra blank rows (human error)
                if inject_human_errors and random.random() < 0.15:
                    writer.writerow({h: "" for h in headers})

        return filepath

    def generate_pdf(
        self,
        date_str: str,
        output_dir: str,
        num_rows: int = 10,
    ) -> str:
        """
        Write a simple PDF report with a data table to *output_dir*.
        Returns the file path.
        """
        rows = self._build_rows(date_str, num_rows, inject_errors=False)
        os.makedirs(output_dir, exist_ok=True)
        filename = f"manual_report_{date_str}.pdf"
        filepath = os.path.join(output_dir, filename)

        doc = SimpleDocTemplate(filepath, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        # Title
        elements.append(Paragraph(f"Spend & Revenue Report — {date_str}", styles["Title"]))
        elements.append(Spacer(1, 0.3 * inch))

        # Table data
        if rows:
            headers = list(rows[0].keys())
            table_data = [headers] + [[str(row.get(h, "")) for h in headers] for row in rows]
            tbl = Table(table_data, repeatRows=1)
            tbl.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4472C4")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ]
                )
            )
            elements.append(tbl)

        doc.build(elements)
        return filepath

    def generate_dataframe(
        self,
        date_str: str,
        inject_human_errors: bool = True,
        num_rows: int = 10,
    ) -> pd.DataFrame:
        """Return a DataFrame (useful for the API server to return JSON)."""
        rows = self._build_rows(date_str, num_rows, inject_human_errors)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_rows(
        self,
        date_str: str,
        num_rows: int,
        inject_errors: bool,
    ) -> List[dict]:
        rows: List[dict] = []
        campaigns = [
            "Brand Search",
            "Retargeting",
            "Prospecting",
            "Lookalike",
            "Seasonal Promo",
        ]

        # Decide column-name variations ONCE per batch so every row has the
        # same keys (DictWriter requires this).
        if inject_errors:
            date_col = "date" if random.random() < 0.3 else "Date"
            spend_col = "Spend $" if random.random() < 0.4 else "spend"
            rev_col = "Revenue" if random.random() < 0.4 else "revenue"
            use_bad_date_fmt = random.random() < 0.5
        else:
            date_col = "Date"
            spend_col = "spend"
            rev_col = "revenue"
            use_bad_date_fmt = False

        for i in range(num_rows):
            campaign = campaigns[i % len(campaigns)]
            spend = round(random.uniform(100, 5000), 2)
            revenue = round(spend * random.uniform(1.2, 4.5), 2)

            row = self._format_row(
                date_str, campaign, spend, revenue,
                inject_errors, date_col, spend_col, rev_col, use_bad_date_fmt,
            )
            rows.append(row)

        return rows

    def _format_row(
        self,
        date_str: str,
        campaign: str,
        spend: float,
        revenue: float,
        inject_errors: bool,
        date_col: str,
        spend_col: str,
        rev_col: str,
        use_bad_date_fmt: bool,
    ) -> dict:
        """Build a single row, optionally injecting human-like errors."""
        # ---- Date formatting ----
        if inject_errors and use_bad_date_fmt:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date = dt.strftime("%d/%m/%Y")
        else:
            formatted_date = date_str

        # ---- Spend formatting ----
        if inject_errors and random.random() < 0.25:
            spend_val = f"${spend:,.2f}"  # e.g. "$1,200.00"
        elif inject_errors and random.random() < 0.10:
            # Typo: extra zero
            spend_val = spend * 10
        else:
            spend_val = spend

        # ---- Revenue formatting ----
        if inject_errors and random.random() < 0.20:
            revenue_val = f"${revenue:,.2f}"
        else:
            revenue_val = revenue

        return {
            date_col: formatted_date,
            "campaign": campaign,
            spend_col: spend_val,
            rev_col: revenue_val,
        }
