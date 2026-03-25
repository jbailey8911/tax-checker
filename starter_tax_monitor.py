#!/usr/bin/env python3
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
TIMEOUT = 60


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
    Uses Playwright to automate the official Texas Sales Tax Rate Locator page.
    This avoids depending on a hidden JSON endpoint that may return HTML or change shape.
    """

    URL = "https://gis.cpa.texas.gov/search/"

    def _extract_rate_from_text(self, text: str) -> Optional[float]:
        # Prefer explicit labels if present
        label_patterns = [
            r"combined\s+rate\s*[:\s]+\s*(\d+\.\d+|\d+)\s*%",
            r"total\s+rate\s*[:\s]+\s*(\d+\.\d+|\d+)\s*%",
            r"sales\s+tax\s+rate\s*[:\s]+\s*(\d+\.\d+|\d+)\s*%",
        ]
        lowered = text.lower()

        for pattern in label_patterns:
            match = re.search(pattern, lowered, re.IGNORECASE)
            if match:
                return float(match.group(1)) / 100.0

        # Fallback: collect all percentages and choose the most plausible total rate
        candidates = []
        for match in re.finditer(r"(\d+(?:\.\d+)?)\s*%", text):
            try:
                pct = float(match.group(1))
                if 0.0 < pct <= 8.25:
                    candidates.append(pct / 100.0)
            except ValueError:
                continue

        if not candidates:
            return None

        # Total combined rate should be the highest plausible one
        return max(candidates)

    def get_rate_for_theater(self, theater: Theater) -> Dict[str, object]:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is required for TX address-level lookup. "
                "Install with: pip install playwright && python -m playwright install chromium"
            ) from exc

        street = theater.address1
        if theater.address2:
            street = f"{street} {theater.address2}".strip()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                page.goto(self.URL, wait_until="networkidle", timeout=TIMEOUT * 1000)

                # Try likely selectors without assuming exact markup
                address_candidates = [
                    'input[placeholder*="Address"]',
                    'input[aria-label*="Address"]',
                    'input[name*="address"]',
                    'input[id*="address"]',
                ]
                city_candidates = [
                    'input[placeholder*="City"]',
                    'input[aria-label*="City"]',
                    'input[name*="city"]',
                    'input[id*="city"]',
                ]
                zip_candidates = [
                    'input[placeholder*="Zip"]',
                    'input[aria-label*="Zip"]',
                    'input[name*="zip"]',
                    'input[id*="zip"]',
                ]

                def fill_first(candidates: List[str], value: str) -> bool:
                    for selector in candidates:
                        locator = page.locator(selector)
                        if locator.count() > 0:
                            locator.first.fill(value)
                            return True
                    return False

                ok_address = fill_first(address_candidates, street)
                ok_city = fill_first(city_candidates, theater.city)
                ok_zip = fill_first(zip_candidates, theater.zip_code)

                if not (ok_address and ok_city and ok_zip):
                    raise RuntimeError(
                        f"Could not find one or more TX locator input fields for {theater.name}"
                    )

                # Click Search or submit
                search_clicked = False
                for selector in [
                    'button:has-text("Search")',
                    'input[type="submit"]',
                    'button[type="submit"]',
                ]:
                    locator = page.locator(selector)
                    if locator.count() > 0:
                        locator.first.click()
                        search_clicked = True
                        break

                if not search_clicked:
                    page.keyboard.press("Enter")

                page.wait_for_timeout(4000)
                page.wait_for_load_state("networkidle", timeout=TIMEOUT * 1000)

                text = page.locator("body").inner_text(timeout=TIMEOUT * 1000)
                rate = self._extract_rate_from_text(text)

                if rate is None:
                    raise RuntimeError(
                        f"Could not extract TX rate for {theater.name}. "
                        f"Page text sample: {text[:800]}"
                    )

                return {
                    "theater": theater.name,
                    "state": "TX",
                    "rate": rate,
                    "source": self.URL,
                    "method": "TX address-level rate locator via browser automation",
                }

            except PlaywrightTimeout as exc:
                raise RuntimeError(f"TX locator timed out for {theater.name}") from exc
            finally:
                browser.close()


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
            county = str(row["county"]).strip().lower()
            rate = str(row["rate"]).strip().replace("*", "")
            rates[county] = normalize_percent_to_decimal(rate)

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
