#!/usr/bin/env python3
"""
starter_tax_monitor.py

Starter sales-tax monitoring script for:
- Texas (TX)
- North Carolina (NC)
- Florida (FL)

What it does:
1. Loads theater locations from a JSON file
2. Pulls current official tax-rate data from state sources
3. Normalizes the rate for each theater
4. Compares to prior saved values
5. Prints change alerts and updates the baseline JSON

Files:
- theaters_tax_monitor_input.json  -> theater master input
- tax_rate_baseline.json          -> snapshot output from each run

Install:
    pip install requests beautifulsoup4 pandas lxml openpyxl

Run:
    python starter_tax_monitor.py
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

INPUT_FILE = Path("theaters_tax_monitor_input.json")
BASELINE_FILE = Path("tax_rate_baseline.json")
TIMEOUT = 30


@dataclass
class Theater:
    name: str
    address1: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None
    tx_city_key: Optional[str] = None


def load_theaters() -> List[Theater]:
    data = json.loads(INPUT_FILE.read_text())
    return [Theater(**item) for item in data["theaters"]]


def fetch_text(url: str) -> str:
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text


def normalize_percent_to_decimal(value: str) -> float:
    """
    Examples:
      "8.25%"   -> 0.0825
      ".082500" -> 0.0825
      "0.015"   -> 0.015
    """
    value = value.strip().replace(",", "")
    if value.endswith("%"):
        return float(value[:-1]) / 100.0
    if value.startswith("."):
        return float(value)
    return float(value)


class TexasProvider:
    """
    Starter TX provider:
    Uses official Texas city sales/use tax table page.
    In production, replace with address-based lookup from the TX rate locator.
    """

    URL = "https://comptroller.texas.gov/taxes/sales/city.php"

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        html = fetch_text(self.URL)
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n")
        rates: Dict[str, float] = {}

        pattern = re.compile(
            r"^([A-Za-z0-9'().\- /&]+?)\s+\d{7}\s+\.\d{6}\s+(\.\d{6})$"
        )

        for raw_line in text.splitlines():
            line = " ".join(raw_line.split())
            match = pattern.match(line)
            if not match:
                continue

            city_name = match.group(1).strip()
            total_rate = normalize_percent_to_decimal(match.group(2))
            rates.setdefault(city_name, total_rate)

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.tx_city_key:
            raise ValueError(f"{theater.name}: missing tx_city_key")

        rates = self.get_rates()
        if theater.tx_city_key not in rates:
            raise KeyError(
                f"TX rate not found for {theater.name} using city key '{theater.tx_city_key}'"
            )

        return {
            "theater": theater.name,
            "state": "TX",
            "rate": rates[theater.tx_city_key],
            "source": self.URL,
            "method": "TX city table (starter method)",
        }


class NorthCarolinaProvider:
    """
    Starter NC provider:
    Reads the current county rate table from NCDOR's current rates page.
    """

    URL = "https://www.ncdor.gov/taxes-forms/sales-and-use-tax/sales-and-use-tax-rates/current-sales-and-use-tax-rates"

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        tables = pd.read_html(self.URL)
        if not tables:
            raise RuntimeError("No tables found on NC current rates page")

        df = None
        for candidate in tables:
            cols = [str(c).strip().lower() for c in candidate.columns]
            if "county" in cols and "rate" in cols:
                df = candidate.copy()
                break

        if df is None:
            raise RuntimeError("Could not find NC county rate table")

        df.columns = [str(c).strip().lower() for c in df.columns]
        rates: Dict[str, float] = {}

        for _, row in df.iterrows():
            county = str(row["county"]).strip()
            rate = str(row["rate"]).strip().replace("*", "")
            rates[county] = normalize_percent_to_decimal(rate)

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            raise ValueError(f"{theater.name}: missing county")

        rates = self.get_rates()
        county = theater.county.strip()
        if county not in rates:
            raise KeyError(f"NC county rate not found for {theater.name}: {county}")

        return {
            "theater": theater.name,
            "state": "NC",
            "rate": rates[county],
            "source": self.URL,
            "method": "NC county current rate table",
        }


class FloridaProvider:
    """
    Starter FL provider:
    Uses Florida's discretionary surtax table by county and adds the statewide 6% base rate.
    """

    URL = "https://pointmatch.floridarevenue.com/General/DiscretionarySalesSurtaxRates.aspx/"
    STATE_BASE_RATE = 0.06

    def __init__(self) -> None:
        self._surtax_rates: Optional[Dict[str, float]] = None

    def get_surtax_rates(self) -> Dict[str, float]:
        if self._surtax_rates is not None:
            return self._surtax_rates

        tables = pd.read_html(self.URL)
        if not tables:
            raise RuntimeError("No tables found on FL surtax page")

        df = tables[0].copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        county_col = None
        surtax_col = None

        for c in df.columns:
            if "county" in c:
                county_col = c
            if "surtax" in c and "rate" in c:
                surtax_col = c

        if county_col is None or surtax_col is None:
            raise RuntimeError("Could not identify FL surtax table columns")

        rates: Dict[str, float] = {}
        for _, row in df.iterrows():
            county = str(row[county_col]).strip().upper()
            surtax = str(row[surtax_col]).strip()
            rates[county] = normalize_percent_to_decimal(surtax)

        self._surtax_rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            raise ValueError(f"{theater.name}: missing county")

        surtax_rates = self.get_surtax_rates()
        county = theater.county.strip().upper()
        if county not in surtax_rates:
            raise KeyError(f"FL county surtax not found for {theater.name}: {county}")

        total_rate = self.STATE_BASE_RATE + surtax_rates[county]

        return {
            "theater": theater.name,
            "state": "FL",
            "rate": total_rate,
            "source": self.URL,
            "method": "FL county surtax + 6% state base",
        }


def load_baseline() -> Dict[str, Dict[str, object]]:
    if not BASELINE_FILE.exists():
        return {}
    return json.loads(BASELINE_FILE.read_text())


def save_baseline(data: Dict[str, Dict[str, object]]) -> None:
    BASELINE_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))


def provider_for_state(state: str):
    if state == "TX":
        return TexasProvider()
    if state == "NC":
        return NorthCarolinaProvider()
    if state == "FL":
        return FloridaProvider()
    raise ValueError(f"No provider configured for state {state}")


def monitor() -> None:
    theaters = load_theaters()
    baseline = load_baseline()
    new_snapshot: Dict[str, Dict[str, object]] = {}
    changes: List[str] = []
    provider_cache: Dict[str, object] = {}

    for theater in theaters:
        provider = provider_cache.get(theater.state)
        if provider is None:
            provider = provider_for_state(theater.state)
            provider_cache[theater.state] = provider

        result = provider.get_rate_for_theater(theater)
        key = theater.name
        new_snapshot[key] = result
        old = baseline.get(key)

        if old is None:
            changes.append(
                f"[NEW] {key}: {result['rate']:.4%} ({result['state']}) via {result['method']}"
            )
            continue

        old_rate = float(old["rate"])
        new_rate = float(result["rate"])

        if abs(old_rate - new_rate) > 1e-9:
            changes.append(
                f"[CHANGED] {key}: {old_rate:.4%} -> {new_rate:.4%} "
                f"({result['state']}) source={result['source']}"
            )

    if changes:
        print("ALERT: tax-rate changes detected")
        print("-" * 80)
        for line in changes:
            print(line)
    else:
        print("No tax-rate changes detected.")

    save_baseline(new_snapshot)


if __name__ == "__main__":
    monitor()
