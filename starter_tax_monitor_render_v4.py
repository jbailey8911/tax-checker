#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests
from bs4 import BeautifulSoup

INPUT_FILE = Path(os.getenv("TAX_MONITOR_INPUT", "theaters_tax_monitor_input_render_v4.json"))
BASELINE_FILE = Path(os.getenv("TAX_MONITOR_BASELINE", "tax_rate_baseline.json"))
TIMEOUT = 45
HEADERS = {"User-Agent": "Mozilla/5.0"}


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


class TexasProvider:
    URL = "https://comptroller.texas.gov/taxes/sales/city.php"

    def __init__(self) -> None:
        self._rates: Optional[Dict[str, float]] = None

    def get_rates(self) -> Dict[str, float]:
        if self._rates is not None:
            return self._rates

        text = fetch_page_text(self.URL)
        rates: Dict[str, float] = {}

        # Capture lines like:
        # San Antonio (Bexar Co) 2015004 .015000 .082500
        # Conroe 2043029 .020000 .082500
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

        # Capture lines like:
        # Guilford 6.75%
        # Forsyth 7%
        # Mecklenburg 7.25%*
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

        # Capture rows like:
        # 3 BAY 1.00 %
        # 48 SEMINOLE 1.00 %
        matches = re.finditer(
            r"(?m)^\s*\d+\s+([A-Z][A-Z ]+?)\s+(\d+(?:\.\d+)?)\s*%\s*$",
            text
        )

        for m in matches:
            county = m.group(1).strip().lower()
            surtax = normalize_percent_to_decimal(m.group(2) + "%")
            rates[county] = surtax

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


def monitor() -> None:
    theaters = load_theaters()
    baseline = load_baseline()
    new_snapshot: Dict[str, Dict[str, object]] = {}
    changes: List[str] = []
    manual_reviews: List[str] = []
    provider_cache: Dict[str, object] = {}

    for theater in theaters:
        provider = provider_cache.get(theater.state)
        if provider is None:
            provider = provider_for_state(theater.state)
            provider_cache[theater.state] = provider

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

    save_baseline(new_snapshot)


if __name__ == "__main__":
    monitor()
