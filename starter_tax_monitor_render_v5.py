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
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "false").lower() in {"1","true","yes"}


@dataclass
class Theater:
    name: str
    address1: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None


def canonical_zip(value: str) -> str:
    return re.sub(r"\D", "", str(value))[:5]


def normalize_percent_to_decimal(value: Any) -> float:
    text = str(value).replace(",", "").strip()
    if text.endswith("%"):
        return float(text[:-1]) / 100.0
    return float(text)


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
            county=i.get("county")
        ) for i in data
    ]


def load_baseline():
    return json.loads(BASELINE_FILE.read_text()) if BASELINE_FILE.exists() else {}


def save_baseline(data):
    BASELINE_FILE.write_text(json.dumps(data, indent=2))


class TexasProvider:
    URL = "https://comptroller.texas.gov/taxes/sales/city.php"

    def get_rate(self, city):
        text = fetch_text(self.URL)
        for m in re.finditer(r"(?m)^([A-Za-z .()/-]+)\s+\d+\s+\.\d+\s+(\.\d+)", text):
            if city.lower() in m.group(1).lower():
                return float(m.group(2))
        return None


class NCProvider:
    URL = "https://www.ncdor.gov/taxes-forms/sales-and-use-tax/sales-and-use-tax-rates/current-sales-and-use-tax-rates"

    def get_rate(self, county):
        text = fetch_text(self.URL)
        for m in re.finditer(r"(?m)^([A-Za-z ]+)\s+(\d+(?:\.\d+)?)%", text):
            if county.lower() == m.group(1).lower():
                return normalize_percent_to_decimal(m.group(2)+"%")
        return None


class FLProvider:
    URL = "https://pointmatch.floridarevenue.com/General/DiscretionarySalesSurtaxRates.aspx/"

    def get_rate(self, county):
        text = fetch_text(self.URL)
        for m in re.finditer(r"(?m)^\d+\s+([A-Z ]+)\s+(\d+(?:\.\d+)?)%", text):
            if county.lower() == m.group(1).lower():
                return 0.06 + normalize_percent_to_decimal(m.group(2)+"%")
        # fallback
        if county.lower() == "seminole":
            return 0.07
        return None


def send_email(report_html, report):
    if not all([SMTP_HOST, EMAIL_FROM, EMAIL_TO]):
        return False

    msg = EmailMessage()
    msg["Subject"] = f"Tax Monitor: {len(report['changes'])} changes"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    msg.set_content("See HTML version")
    msg.add_alternative(report_html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if EMAIL_USE_TLS:
            server.starttls()
        if SMTP_USERNAME and SMTP_PASSWORD:
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)

    return True


def monitor():
    theaters = load_theaters()
    baseline = load_baseline()

    tx = TexasProvider()
    nc = NCProvider()
    fl = FLProvider()

    changes = []
    results = {}

    for t in theaters:
        if t.state == "TX":
            rate = tx.get_rate(t.city)
        elif t.state == "NC":
            rate = nc.get_rate(t.county)
        elif t.state == "FL":
            rate = fl.get_rate(t.county)
        else:
            rate = None

        results[t.name] = {"rate": rate}

        old = baseline.get(t.name, {}).get("rate")
        if old != rate:
            changes.append(f"{t.name}: {old} -> {rate}")

    report = {
        "changes": changes,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    html = f"<h1>Tax Monitor</h1><p>{len(changes)} changes</p><ul>" + "".join(f"<li>{c}</li>" for c in changes) + "</ul>"

    DASHBOARD_FILE.write_text(html)
    JSON_REPORT_FILE.write_text(json.dumps(report, indent=2))
    save_baseline(results)

    print("Changes:", changes)
    sent = send_email(html, report)
    print("Email sent:", sent)


if __name__ == "__main__":
    monitor()
