#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests
from bs4 import BeautifulSoup

INPUT_FILE = Path(os.getenv("TAX_MONITOR_INPUT", "theaters_tax_monitor_input_render_v5.json"))
BASELINE_FILE = Path(os.getenv("TAX_MONITOR_BASELINE", "tax_rate_baseline.json"))
DASHBOARD_FILE = Path(os.getenv("TAX_MONITOR_DASHBOARD", "tax_monitor_dashboard.html"))
JSON_REPORT_FILE = Path(os.getenv("TAX_MONITOR_REPORT_JSON", "tax_monitor_report.json"))

TIMEOUT = 45
HEADERS = {"User-Agent": "Mozilla/5.0"}

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")  # comma-separated
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "true").lower() in {"1", "true", "yes"}


@dataclass
class Theater:
    name: str
    address1: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None
    address2: Optional[str] = None


def canonical_zip(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))
    return digits[:5]


def normalize_percent_to_decimal(value: Any) -> float:
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        return float(text[:-1]) / 100.0
    if text.startswith("."):
        return float(text)
    return float(text)


def fetch_page_text(url: str) -> str:
    resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    return soup.get_text("\n")


def load_theaters() -> List[Theater]:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    raw = json.loads(INPUT_FILE.read_text())
    return [
        Theater(
            name=item["name"],
            address1=item["address1"],
            address2=item.get("address2"),
            city=item["city"],
            state=item["state"].upper(),
            zip_code=canonical_zip(item["zip_code"]),
            county=item.get("county"),
        )
        for item in raw
    ]


def load_baseline() -> Dict[str, Dict[str, object]]:
    if not BASELINE_FILE.exists():
        return {}
    return json.loads(BASELINE_FILE.read_text())


def save_baseline(data: Dict[str, Dict[str, object]]) -> None:
    BASELINE_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))


def pct(rate: Optional[float]) -> str:
    if rate is None:
        return "—"
    return f"{rate:.4%}"


class TexasProvider:
    URL = "https://comptroller.texas.gov/taxes/sales/city.php"

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        text = fetch_page_text(self.URL)
        rates: Dict[str, float] = {}

        matches = re.finditer(
            r"(?m)^\s*([A-Za-z0-9'()./&\- ]+?)\s+\d{7}\s+\.\d{6}\s+(\.\d{6})\s*$",
            text
        )

        for m in matches:
            city_name = m.group(1).strip()
            total_rate = normalize_percent_to_decimal(m.group(2))
            rates[city_name] = total_rate

        if not rates:
            raise RuntimeError("Could not parse TX city rate table")

        self._rates = rates
        return rates

    def _match_city_rate(self, city: str, rates: Dict[str, float]) -> Optional[float]:
        city_clean = city.strip().lower()

        for key, rate in rates.items():
            if key.strip().lower() == city_clean:
                return rate

        prefix_matches = []
        for key, rate in rates.items():
            key_clean = key.strip().lower()
            if key_clean.startswith(city_clean + " (") or key_clean.startswith(city_clean + "/"):
                prefix_matches.append(rate)

        if prefix_matches:
            unique_rates = {round(rate, 6) for rate in prefix_matches}
            if len(unique_rates) == 1:
                return prefix_matches[0]

        return None

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        rates = self.get_rates()
        rate = self._match_city_rate(theater.city, rates)

        if rate is None:
            return {
                "theater": theater.name,
                "state": "TX",
                "rate": None,
                "source": self.URL,
                "method": "TX city table",
                "manual_review_required": True,
                "note": f"Could not confidently match city '{theater.city}' in TX city table",
            }

        return {
            "theater": theater.name,
            "state": "TX",
            "rate": rate,
            "source": self.URL,
            "method": "TX city table",
            "manual_review_required": False,
        }


class NorthCarolinaProvider:
    URL = "https://www.ncdor.gov/taxes-forms/sales-and-use-tax/sales-and-use-tax-rates/current-sales-and-use-tax-rates"

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        text = fetch_page_text(self.URL)
        rates: Dict[str, float] = {}

        matches = re.finditer(
            r"(?m)^\s*([A-Za-z]+(?: [A-Za-z]+)*)\s+(\d+(?:\.\d+)?)%[*]?\s*$",
            text
        )

        for m in matches:
            county = m.group(1).strip().lower()
            rate = normalize_percent_to_decimal(m.group(2) + "%")
            rates[county] = rate

        if not rates:
            raise RuntimeError("Could not parse NC county rates")

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            return {
                "theater": theater.name,
                "state": "NC",
                "rate": None,
                "source": self.URL,
                "method": "NC county rate page",
                "manual_review_required": True,
                "note": "Missing county for NC lookup",
            }

        county_key = theater.county.strip().lower()
        rates = self.get_rates()
        if county_key not in rates:
            return {
                "theater": theater.name,
                "state": "NC",
                "rate": None,
                "source": self.URL,
                "method": "NC county rate page",
                "manual_review_required": True,
                "note": f"County not found: {theater.county}",
            }

        return {
            "theater": theater.name,
            "state": "NC",
            "rate": rates[county_key],
            "source": self.URL,
            "method": "NC county rate page",
            "manual_review_required": False,
        }


class FloridaProvider:
    URL = "https://pointmatch.floridarevenue.com/General/DiscretionarySalesSurtaxRates.aspx/"
    STATE_BASE_RATE = 0.06

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_surtax_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        text = fetch_page_text(self.URL)
        rates: Dict[str, float] = {}

        matches = re.finditer(
            r"(?m)^\s*\d+\s+([A-Z][A-Z ]+?)\s+(\d+(?:\.\d+)?)\s*%\s*$",
            text
        )

        for m in matches:
            county = m.group(1).strip().lower()
            surtax = normalize_percent_to_decimal(m.group(2) + "%")
            rates[county] = surtax

        # Fallbacks for counties not visible in first-page text output.
        # Review periodically against Florida DOR.
        fallback_rates = {
            "bay": 0.01,
            "seminole": 0.01,
        }
        for county, surtax in fallback_rates.items():
            rates.setdefault(county, surtax)

        if not rates:
            raise RuntimeError("Could not parse FL surtax rates")

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            return {
                "theater": theater.name,
                "state": "FL",
                "rate": None,
                "source": self.URL,
                "method": "FL surtax page",
                "manual_review_required": True,
                "note": "Missing county for FL lookup",
            }

        county_key = theater.county.strip().lower()
        surtax_rates = self.get_surtax_rates()
        if county_key not in surtax_rates:
            return {
                "theater": theater.name,
                "state": "FL",
                "rate": None,
                "source": self.URL,
                "method": "FL surtax page",
                "manual_review_required": True,
                "note": f"County not found: {theater.county}",
            }

        return {
            "theater": theater.name,
            "state": "FL",
            "rate": self.STATE_BASE_RATE + surtax_rates[county_key],
            "source": self.URL,
            "method": "FL surtax page",
            "manual_review_required": False,
        }


def provider_for_state(state: str):
    if state == "TX":
        return TexasProvider()
    if state == "NC":
        return NorthCarolinaProvider()
    if state == "FL":
        return FloridaProvider()
    raise ValueError(f"No provider configured for state {state}")


def build_dashboard(report: Dict[str, Any]) -> str:
    def esc(text: Any) -> str:
        s = str(text)
        return (
            s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
        )

    rows = []
    for item in report["locations"]:
        status = "Review" if item.get("manual_review_required") else "OK"
        row_class = "review" if item.get("manual_review_required") else "ok"
        rows.append(
            f"<tr class='{row_class}'>"
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

    changes_list = "".join(f"<li>{esc(x)}</li>" for x in report["changes"]) or "<li>No rate changes detected.</li>"
    reviews_list = "".join(f"<li>{esc(x)}</li>" for x in report["manual_reviews"]) or "<li>No manual review items.</li>"

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tax Rate Monitor Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .meta {{ color: #555; margin-bottom: 20px; }}
    .cards {{ display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; min-width: 180px; }}
    .value {{ font-size: 24px; font-weight: bold; margin-top: 4px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 16px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f5f5f5; }}
    tr.review {{ background: #fff8e1; }}
    tr.ok {{ background: #f9fff9; }}
    .section {{ margin-top: 28px; }}
  </style>
</head>
<body>
  <h1>Tax Rate Monitor Dashboard</h1>
  <div class="meta">Run completed: {esc(report["run_timestamp_utc"])} UTC</div>

  <div class="cards">
    <div class="card"><div>Locations checked</div><div class="value">{len(report["locations"])}</div></div>
    <div class="card"><div>Rate changes</div><div class="value">{len(report["changes"])}</div></div>
    <div class="card"><div>Manual reviews</div><div class="value">{len(report["manual_reviews"])}</div></div>
  </div>

  <div class="section">
    <h2>Changes</h2>
    <ul>{changes_list}</ul>
  </div>

  <div class="section">
    <h2>Manual Review Items</h2>
    <ul>{reviews_list}</ul>
  </div>

  <div class="section">
    <h2>Location Status</h2>
    <table>
      <thead>
        <tr>
          <th>Theater</th>
          <th>State</th>
          <th>City</th>
          <th>County</th>
          <th>Previous Rate</th>
          <th>Current Rate</th>
          <th>Status</th>
          <th>Note</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
  </div>
</body>
</html>"""


def maybe_send_email(report: Dict[str, Any], dashboard_html: str) -> bool:
    if not all([SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, EMAIL_FROM, EMAIL_TO]):
        return False

    msg = EmailMessage()
    msg["Subject"] = f"Tax Rate Monitor - {len(report['changes'])} changes, {len(report['manual_reviews'])} review items"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    body_lines = [
        f"Run completed: {report['run_timestamp_utc']} UTC",
        f"Locations checked: {len(report['locations'])}",
        f"Rate changes: {len(report['changes'])}",
        f"Manual reviews: {len(report['manual_reviews'])}",
        "",
        "Changes:",
    ]
    body_lines.extend(report["changes"] or ["No rate changes detected."])
    body_lines.append("")
    body_lines.append("Manual Review Items:")
    body_lines.extend(report["manual_reviews"] or ["No manual review items."])

    msg.set_content("\n".join(body_lines))
    msg.add_alternative(dashboard_html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        if EMAIL_USE_TLS:
            server.starttls()

    # Only login if credentials are provided
    if SMTP_USERNAME and SMTP_PASSWORD:
        server.login(SMTP_USERNAME, SMTP_PASSWORD)

    server.send_message(msg)

    return True


def monitor() -> None:
    theaters = load_theaters()
    baseline = load_baseline()
    new_snapshot: Dict[str, Dict[str, object]] = {}
    changes: List[str] = []
    manual_reviews: List[str] = []
    provider_cache: Dict[str, object] = {}
    location_results: List[Dict[str, Any]] = []

    for theater in theaters:
        provider = provider_cache.get(theater.state)
        if provider is None:
            provider = provider_for_state(theater.state)
            provider_cache[theater.state] = provider

        previous = baseline.get(theater.name, {})
        try:
            result = provider.get_rate_for_theater(theater)
        except Exception as exc:
            result = {
                "theater": theater.name,
                "state": theater.state,
                "rate": None,
                "manual_review_required": True,
                "note": f"Lookup error: {exc}",
            }

        key = theater.name
        new_snapshot[key] = result

        result_for_report = {
            **result,
            "city": theater.city,
            "county": theater.county or "",
            "previous_rate": previous.get("rate"),
        }
        location_results.append(result_for_report)

        if result.get("manual_review_required"):
            manual_reviews.append(
                f"[REVIEW] {key} ({theater.state}): {result.get('note', 'manual review required')}"
            )
            continue

        old = baseline.get(key)
        if old is None or old.get("rate") is None:
            changes.append(
                f"[NEW] {key}: {result['rate']:.4%} ({result['state']}) via {result.get('method', 'lookup')}"
            )
            continue

        old_rate = float(old["rate"])
        new_rate = float(result["rate"])
        if abs(old_rate - new_rate) > 1e-9:
            changes.append(
                f"[CHANGED] {key}: {old_rate:.4%} -> {new_rate:.4%} "
                f"({result['state']}) source={result.get('source', '')}"
            )

    report = {
        "run_timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "changes": changes,
        "manual_reviews": manual_reviews,
        "locations": location_results,
    }

    dashboard_html = build_dashboard(report)
    DASHBOARD_FILE.write_text(dashboard_html, encoding="utf-8")
    JSON_REPORT_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    save_baseline(new_snapshot)

    if changes:
        print("ALERT: tax-rate changes detected")
        print("-" * 80)
        for line in changes:
            print(line)
    else:
        print("No tax-rate changes detected.")

    if manual_reviews:
        print("\nMANUAL REVIEW ITEMS")
        print("-" * 80)
        for line in manual_reviews:
            print(line)

    email_sent = False
    try:
        email_sent = maybe_send_email(report, dashboard_html)
    except Exception as exc:
        print(f"\nEMAIL WARNING: could not send email: {exc}")

    print(f"\nDashboard written to: {DASHBOARD_FILE}")
    print(f"JSON report written to: {JSON_REPORT_FILE}")
    print(f"Email sent: {email_sent}")


if __name__ == "__main__":
    monitor()
