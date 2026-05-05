#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests
from bs4 import BeautifulSoup

INPUT_FILE = Path(os.getenv("TAX_MONITOR_INPUT", "theaters_tax_monitor_input_render_v6.json"))
DASHBOARD_FILE = Path(os.getenv("TAX_MONITOR_DASHBOARD", "tax_monitor_dashboard.html"))
JSON_REPORT_FILE = Path(os.getenv("TAX_MONITOR_REPORT_JSON", "tax_monitor_report.json"))

TIMEOUT = 45
HEADERS = {"User-Agent": "Mozilla/5.0"}

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")
DATABASE_URL = os.getenv("DATABASE_URL")


@dataclass
class Theater:
    name: str
    address1: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)


def ensure_database_table() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tax_rates (
                    location_name TEXT PRIMARY KEY,
                    rate NUMERIC(8,4),
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_changed TIMESTAMP
                );
                """
            )


def normalize_rate_for_db(rate: Any) -> Optional[Decimal]:
    """
    Store rates as percent values:
    0.0825 -> 8.2500
    8.25   -> 8.2500
    "8.25%" -> 8.2500
    """
    if rate is None:
        return None

    value = Decimal(str(rate).replace("%", "").strip())

    if value < 1:
        value = value * 100

    return value.quantize(Decimal("0.0001"))


def db_percent_to_float(rate: Optional[Decimal]) -> Optional[float]:
    """
    Convert database percent values back to decimal values for dashboard formatting:
    8.2500 -> 0.0825
    """
    if rate is None:
        return None
    return float(rate) / 100.0


def check_and_save_rate(location_name: str, new_rate: Optional[float]) -> Tuple[str, Optional[Decimal], Optional[Decimal]]:
    """
    Returns:
      ("baseline_created", None, new_rate)
      ("changed", old_rate, new_rate)
      ("no_change", old_rate, new_rate)
      ("manual_review", old_rate_or_none, None)
    """
    new_rate_db = normalize_rate_for_db(new_rate)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rate FROM tax_rates WHERE location_name = %s",
                (location_name,),
            )
            row = cur.fetchone()
            old_rate_db = normalize_rate_for_db(row[0]) if row else None

            # If we couldn't fetch/parse today's rate, do NOT overwrite the stored good rate.
            if new_rate_db is None:
                return "manual_review", old_rate_db, None

            if row is None:
                cur.execute(
                    """
                    INSERT INTO tax_rates (location_name, rate, last_checked, last_changed)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, NULL)
                    """,
                    (location_name, new_rate_db),
                )
                return "baseline_created", None, new_rate_db

            if old_rate_db != new_rate_db:
                cur.execute(
                    """
                    UPDATE tax_rates
                    SET rate = %s,
                        last_checked = CURRENT_TIMESTAMP,
                        last_changed = CURRENT_TIMESTAMP
                    WHERE location_name = %s
                    """,
                    (new_rate_db, location_name),
                )
                return "changed", old_rate_db, new_rate_db

            cur.execute(
                """
                UPDATE tax_rates
                SET last_checked = CURRENT_TIMESTAMP
                WHERE location_name = %s
                """,
                (location_name,),
            )
            return "no_change", old_rate_db, new_rate_db


def canonical_zip(value: str) -> str:
    return re.sub(r"\D", "", str(value))[:5]


def normalize_percent_to_decimal(value: Any) -> float:
    text = str(value).replace(",", "").strip()
    if text.endswith("%"):
        return float(text[:-1]) / 100.0
    if text.startswith("."):
        return float(text)
    return float(text)


def pct(rate: Optional[float]) -> str:
    if rate is None:
        return "—"
    return f"{rate:.4%}"


def pct_db(rate: Optional[Decimal]) -> str:
    if rate is None:
        return "—"
    return f"{rate:.4f}%"


def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser").get_text("\n")


def load_theaters() -> List[Theater]:
    data = json.loads(INPUT_FILE.read_text())
    return [
        Theater(
            name=i["name"],
            address1=i["address1"],
            city=i["city"],
            state=i["state"].upper(),
            zip_code=canonical_zip(i["zip_code"]),
            county=i.get("county"),
        )
        for i in data
    ]


class TexasProvider:
    URL = "https://comptroller.texas.gov/taxes/sales/city.php"

    def __init__(self) -> None:
        self._text = None

    def get_rate(self, city: str) -> Optional[float]:
        if self._text is None:
            self._text = fetch_text(self.URL)

        exact_pattern = re.compile(
            rf"(?m)^\s*{re.escape(city)}\s+\d+\s+\.\d+\s+(\.\d+)\s*$",
            re.IGNORECASE,
        )
        m = exact_pattern.search(self._text)
        if m:
            return float(m.group(1))

        variant_pattern = re.compile(
            rf"(?m)^\s*{re.escape(city)}(?:\s+\([^)]+\)|/[^\n]+)?\s+\d+\s+\.\d+\s+(\.\d+)\s*$",
            re.IGNORECASE,
        )
        matches = [float(x.group(1)) for x in variant_pattern.finditer(self._text)]
        if matches:
            uniq = sorted(set(round(x, 6) for x in matches))
            if len(uniq) == 1:
                return matches[0]

        return None


class NCProvider:
    URL = "https://www.ncdor.gov/taxes-forms/sales-and-use-tax/sales-and-use-tax-rates/current-sales-and-use-tax-rates"

    def __init__(self) -> None:
        self._text = None

    def get_rate(self, county: str) -> Optional[float]:
        if self._text is None:
            self._text = fetch_text(self.URL)

        pattern = re.compile(
            rf"(?m)^\s*{re.escape(county)}\s+(\d+(?:\.\d+)?)%[*]?\s*$",
            re.IGNORECASE,
        )
        m = pattern.search(self._text)
        if not m:
            return None
        return normalize_percent_to_decimal(m.group(1) + "%")


class FLProvider:
    URL = "https://pointmatch.floridarevenue.com/General/DiscretionarySalesSurtaxRates.aspx/"
    STATE_BASE_RATE = 0.06

    def __init__(self) -> None:
        self._text = None

    def get_rate(self, county: str) -> Optional[float]:
        if self._text is None:
            self._text = fetch_text(self.URL)

        pattern = re.compile(
            rf"(?m)^\s*\d+\s+{re.escape(county.upper())}\s+(\d+(?:\.\d+)?)\s*%\s*$"
        )
        m = pattern.search(self._text)
        if m:
            return self.STATE_BASE_RATE + normalize_percent_to_decimal(m.group(1) + "%")

        fallback_rates = {
            "seminole": 0.07,
            "bay": 0.07,
        }
        return fallback_rates.get(county.lower())


def build_dashboard(report: Dict[str, Any]) -> str:
    def esc(text: Any) -> str:
        s = str(text)
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for item in report["locations"]:
        status = "Review" if item.get("manual_review_required") else item.get("status", "OK")
        cls = "review" if item.get("manual_review_required") else "ok"
        rows.append(
            f"<tr class='{cls}'>"
            f"<td>{esc(item['theater'])}</td>"
            f"<td>{esc(item['state'])}</td>"
            f"<td>{esc(item.get('city',''))}</td>"
            f"<td>{esc(item.get('county',''))}</td>"
            f"<td>{esc(pct(item.get('previous_rate')))}</td>"
            f"<td>{esc(pct(item.get('rate')))}</td>"
            f"<td>{esc(status)}</td>"
            f"<td>{esc(item.get('note',''))}</td>"
            f"</tr>"
        )

    changes_html = "".join(f"<li>{esc(x)}</li>" for x in report["changes"]) or "<li>No rate changes detected.</li>"
    review_html = "".join(f"<li>{esc(x)}</li>" for x in report["manual_reviews"]) or "<li>No manual review items.</li>"

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Tax Rate Monitor Dashboard</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
.cards {{ display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }}
.card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; min-width: 180px; }}
.value {{ font-size: 24px; font-weight: bold; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 16px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
th {{ background: #f5f5f5; }}
tr.review {{ background: #fff8e1; }}
tr.ok {{ background: #f9fff9; }}
</style>
</head>
<body>
<h1>Tax Rate Monitor Dashboard</h1>
<p>Run completed: {esc(report["timestamp"])}</p>
<div class="cards">
  <div class="card"><div>Locations checked</div><div class="value">{len(report["locations"])}</div></div>
  <div class="card"><div>Rate changes</div><div class="value">{len(report["changes"])}</div></div>
  <div class="card"><div>Baselines created</div><div class="value">{report.get("baselines_created", 0)}</div></div>
  <div class="card"><div>Manual reviews</div><div class="value">{len(report["manual_reviews"])}</div></div>
</div>
<h2>Changes</h2>
<ul>{changes_html}</ul>
<h2>Manual Review Items</h2>
<ul>{review_html}</ul>
<h2>Location Status</h2>
<table>
<thead>
<tr><th>Theater</th><th>State</th><th>City</th><th>County</th><th>Previous Rate</th><th>Current Rate</th><th>Status</th><th>Note</th></tr>
</thead>
<tbody>
{''.join(rows)}
</tbody>
</table>
</body>
</html>"""


def send_teams_message(report: Dict[str, Any]) -> bool:
    if not TEAMS_WEBHOOK_URL:
        return False

    summary_lines = []

    if report.get("baselines_created", 0):
        summary_lines.append(f"Baselines created: {report['baselines_created']}")

    if report["changes"]:
        summary_lines.append("**Changes detected:**")
        summary_lines.extend([f"- {x}" for x in report["changes"][:10]])
        if len(report["changes"]) > 10:
            summary_lines.append(f"- ...and {len(report['changes']) - 10} more")
    else:
        summary_lines.append("No tax-rate changes detected.")

    if report["manual_reviews"]:
        summary_lines.append("")
        summary_lines.append("**Manual review items:**")
        summary_lines.extend([f"- {x}" for x in report["manual_reviews"][:10]])
        if len(report["manual_reviews"]) > 10:
            summary_lines.append(f"- ...and {len(report['manual_reviews']) - 10} more")

    text = "\n".join(summary_lines)

    payload = {
        "text": (
            f"**Tax Rate Monitor**\n\n"
            f"Run completed: {report['timestamp']} UTC\n\n"
            f"Locations checked: {len(report['locations'])}\n"
            f"Rate changes: {len(report['changes'])}\n"
            f"Baselines created: {report.get('baselines_created', 0)}\n"
            f"Manual reviews: {len(report['manual_reviews'])}\n\n"
            f"{text}"
        )
    }

    r = requests.post(
        TEAMS_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return True


def monitor():
    ensure_database_table()

    theaters = load_theaters()

    tx = TexasProvider()
    nc = NCProvider()
    fl = FLProvider()

    changes = []
    manual_reviews = []
    location_results = []
    baselines_created = 0

    for t in theaters:
        note = ""
        manual_review_required = False

        if t.state == "TX":
            rate = tx.get_rate(t.city)
            if rate is None:
                manual_review_required = True
                note = f"Could not confidently match city '{t.city}' in TX city table"
        elif t.state == "NC":
            rate = nc.get_rate(t.county or "")
            if rate is None:
                manual_review_required = True
                note = f"County not found: {t.county}"
        elif t.state == "FL":
            rate = fl.get_rate(t.county or "")
            if rate is None:
                manual_review_required = True
                note = f"County not found: {t.county}"
        else:
            rate = None
            manual_review_required = True
            note = f"Unsupported state: {t.state}"

        db_status, old_rate_db, new_rate_db = check_and_save_rate(t.name, rate)
        previous_rate_float = db_percent_to_float(old_rate_db)
        current_rate_float = db_percent_to_float(new_rate_db) if new_rate_db is not None else rate

        if db_status == "baseline_created":
            baselines_created += 1
        elif db_status == "changed":
            changes.append(f"{t.name}: {pct_db(old_rate_db)} -> {pct_db(new_rate_db)}")

        location_results.append({
            "theater": t.name,
            "state": t.state,
            "city": t.city,
            "county": t.county or "",
            "previous_rate": previous_rate_float,
            "rate": current_rate_float,
            "status": db_status,
            "manual_review_required": manual_review_required,
            "note": note,
        })

        if manual_review_required:
            manual_reviews.append(f"[REVIEW] {t.name} ({t.state}): {note}")

    report = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "changes": changes,
        "manual_reviews": manual_reviews,
        "locations": location_results,
        "baselines_created": baselines_created,
    }

    html = build_dashboard(report)
    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    JSON_REPORT_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if changes:
        print("ALERT: tax-rate changes detected")
        print("-" * 80)
        for c in changes:
            print(c)
    else:
        print("No tax-rate changes detected.")

    if baselines_created:
        print(f"\nBaselines created: {baselines_created}")

    if manual_reviews:
        print("\nMANUAL REVIEW ITEMS")
        print("-" * 80)
        for x in manual_reviews:
            print(x)

    teams_sent = False
    try:
        teams_sent = send_teams_message(report)
    except Exception as exc:
        print(f"\nTEAMS WARNING: could not send message: {exc}")

    print(f"\nDashboard written to: {DASHBOARD_FILE}")
    print(f"JSON report written to: {JSON_REPORT_FILE}")
    print(f"Teams sent: {teams_sent}")


if __name__ == "__main__":
    monitor()
