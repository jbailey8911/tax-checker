#!/usr/bin/env python3
"""
starter_tax_monitor.py

Address-level sales tax monitoring starter for:
- Texas (TX) via official Texas Comptroller Sales Tax Rate Locator
- North Carolina (NC) via official NCDOR county rate table
- Florida (FL) via official Florida county discretionary surtax table + 6% state base

What it does:
1. Loads theater locations from a JSON file
2. Looks up the current official tax rate for each TX / NC / FL location
3. Compares against a saved baseline JSON
4. Prints alerts when a rate changes
5. Updates the baseline snapshot

Install:
    pip install requests beautifulsoup4 pandas lxml

Run:
    python starter_tax_monitor.py

Optional env vars:
    TAX_MONITOR_INPUT=theaters_tax_monitor_input_render.json
    TAX_MONITOR_BASELINE=tax_rate_baseline.json
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import requests

INPUT_FILE = Path(os.getenv("TAX_MONITOR_INPUT", "theaters_tax_monitor_input_render.json"))
BASELINE_FILE = Path(os.getenv("TAX_MONITOR_BASELINE", "tax_rate_baseline.json"))
TIMEOUT = 45


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


def load_theaters() -> List[Theater]:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    raw = json.loads(INPUT_FILE.read_text())
    theaters: List[Theater] = []

    for item in raw:
        theaters.append(
            Theater(
                name=item["name"],
                address1=item["address1"],
                address2=item.get("address2"),
                city=item["city"],
                state=item["state"].upper(),
                zip_code=canonical_zip(item["zip_code"]),
                county=item.get("county"),
            )
        )

    return theaters


def load_baseline() -> Dict[str, Dict[str, object]]:
    if not BASELINE_FILE.exists():
        return {}
    return json.loads(BASELINE_FILE.read_text())


def save_baseline(data: Dict[str, Dict[str, object]]) -> None:
    BASELINE_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))


class TexasProvider:
    """
    Address-level TX provider.

    Uses the official Texas Comptroller Sales Tax Rate Locator POST endpoint.
    The web app is JavaScript-driven, but the backing service currently accepts a JSON
    payload with address components and returns the total rate and jurisdiction details.

    If the endpoint shape changes in the future, the script raises a clear error instead
    of silently returning the wrong rate.
    """

    SEARCH_URL = "https://gis.cpa.texas.gov/_api/search"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": "https://gis.cpa.texas.gov",
                "Referer": "https://gis.cpa.texas.gov/search/",
                "User-Agent": "Mozilla/5.0",
            }
        )

    def _payload(self, theater: Theater) -> Dict[str, object]:
        street = theater.address1
        if theater.address2:
            street = f"{street} {theater.address2}".strip()

        return {
            "query": {
                "searchText": street,
                "city": theater.city,
                "state": "TX",
                "zip": theater.zip_code,
            },
            "searchType": "singleAddress",
        }

    def _find_rate(self, obj: Any) -> Optional[float]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_lower = str(key).lower()

                if key_lower in {
                    "totalrate",
                    "combinedrate",
                    "totalsalestaxrate",
                    "taxrate",
                    "rate",
                }:
                    try:
                        rate = normalize_percent_to_decimal(value)
                        if 0.0 < rate <= 0.2:
                            return rate
                    except Exception:
                        pass

                found = self._find_rate(value)
                if found is not None:
                    return found

        elif isinstance(obj, list):
            for item in obj:
                found = self._find_rate(item)
                if found is not None:
                    return found

        return None

    def _find_timestamp(self, obj: Any) -> Optional[str]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_lower = str(key).lower()
                if key_lower in {"timestamp", "asof", "lastupdated", "searchtimestamp"}:
                    return str(value)
                found = self._find_timestamp(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._find_timestamp(item)
                if found:
                    return found
        return None

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        payload = self._payload(theater)
        resp = self.session.post(self.SEARCH_URL, json=payload, timeout=TIMEOUT)

        if resp.status_code >= 400:
            raise RuntimeError(
                f"TX locator request failed for {theater.name}: "
                f"{resp.status_code} {resp.text[:300]}"
            )

        data = resp.json()
        rate = self._find_rate(data)
        if rate is None:
            raise RuntimeError(
                f"TX locator returned no usable rate for {theater.name}. "
                f"Response sample: {json.dumps(data)[:500]}"
            )

        return {
            "theater": theater.name,
            "state": "TX",
            "rate": rate,
            "source": "https://gis.cpa.texas.gov/search/",
            "method": "TX address-level rate locator",
            "timestamp": self._find_timestamp(data),
        }


class NorthCarolinaProvider:
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
            rates[county.lower()] = normalize_percent_to_decimal(rate)

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            raise ValueError(f"{theater.name}: missing county for NC lookup")

        county_key = theater.county.strip().lower()
        rates = self.get_rates()
        if county_key not in rates:
            raise KeyError(f"NC county rate not found for {theater.name}: {theater.county}")

        return {
            "theater": theater.name,
            "state": "NC",
            "rate": rates[county_key],
            "source": self.URL,
            "method": "NC county current rate table",
        }


class FloridaProvider:
    URL = "https://pointmatch.floridarevenue.com/General/DiscretionarySalesSurtaxRates.aspx/"
    STATE_BASE_RATE = 0.06

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_surtax_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

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
            county = str(row[county_col]).strip().lower()
            surtax = str(row[surtax_col]).strip()
            rates[county] = normalize_percent_to_decimal(surtax)

        self._rates = rates
        return rates

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        if not theater.county:
            raise ValueError(f"{theater.name}: missing county for FL lookup")

        county_key = theater.county.strip().lower()
        surtax_rates = self.get_surtax_rates()

        if county_key not in surtax_rates:
            raise KeyError(f"FL county surtax not found for {theater.name}: {theater.county}")

        total_rate = self.STATE_BASE_RATE + surtax_rates[county_key]

        return {
            "theater": theater.name,
            "state": "FL",
            "rate": total_rate,
            "source": self.URL,
            "method": "FL county surtax + 6% state base",
        }


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
