print("REGISTRY_BUILDER_V3_LOADED", flush=True)

import csv
import requests
from io import StringIO

from core.db import upsert_asset, upsert_entity, upsert_source
from core.utils import now_utc


# =========================
# CONFIG
# =========================

OURAIRPORTS_URL = "https://ourairports.com/data/airports.csv"


# =========================
# GLOBAL AIRPORT INGESTION
# =========================

def ingest_all_airports_global():
    print("🌍 Starting global airport ingestion...", flush=True)

    response = requests.get(OURAIRPORTS_URL, timeout=60)
    response.raise_for_status()

    csv_data = StringIO(response.text)
    reader = csv.DictReader(csv_data)

    created = 0

    for row in reader:
        country_code = row.get("iso_country")
        name = row.get("name")

        if not country_code or not name:
            continue

        asset_type = row.get("type") or "airport"

        upsert_asset(
            country_code=country_code,
            asset_name=name,
            asset_type=asset_type,
            canonical_source_url=OURAIRPORTS_URL,
        )

        created += 1

        if created % 1000 == 0:
            print(f"✈️ Inserted {created} airports...", flush=True)

    print(f"✅ DONE: {created} airports loaded", flush=True)

    return created


# =========================
# COUNTRY-LEVEL INGESTION (fallback)
# =========================

def ingest_airports_for_country(country_code: str):
    response = requests.get(OURAIRPORTS_URL, timeout=60)
    response.raise_for_status()

    csv_data = StringIO(response.text)
    reader = csv.DictReader(csv_data)

    created = 0

    for row in reader:
        if row.get("iso_country") != country_code:
            continue

        name = row.get("name")
        if not name:
            continue

        asset_type = row.get("type") or "airport"

        asset_id = upsert_asset(
            country_code=country_code,
            asset_name=name,
            asset_type=asset_type,
            canonical_source_url=OURAIRPORTS_URL,
        )

        upsert_source(
            country_code=country_code,
            source_url=OURAIRPORTS_URL,
            source_type="ourairports_csv",
            asset_id=asset_id,
        )

        created += 1

    return created


# =========================
# OPERATOR DISCOVERY (STUB)
# =========================

def discover_operators(country_code: str):
    # ⚠️ Placeholder — next step буде реальний pipeline
    created = 0

    for i in range(3):
        name = f"{country_code} Airport Operator {i}"

        upsert_entity(
            country_code=country_code,
            entity_name=name,
            entity_type="operator",
        )

        created += 1

    return created


# =========================
# LINKING (STUB)
# =========================

def link_airports_to_operators(country_code: str):
    # ⚠️ Placeholder
    # Тут буде реальна логіка mapping
    return 1


# =========================
# MONITORING (STUB)
# =========================

def monitor_sources(country_code: str):
    # Поки просто placeholder
    return 0


# =========================
# MAIN ENTRY
# =========================

def crawl_country(country_code: str, mode: str = "bootstrap_airports"):
    print(f"🌐 crawl_country | {country_code} | mode={mode}", flush=True)

    summary = {
        "assets": 0,
        "entities": 0,
        "links": 0,
        "sources": 0,
    }

    # =========================
    # BOOTSTRAP AIRPORTS
    # =========================
    if mode == "bootstrap_airports":
        count = ingest_airports_for_country(country_code)

        summary["assets"] += count
        summary["sources"] += count

    # =========================
    # BOOTSTRAP OPERATORS
    # =========================
    elif mode == "bootstrap_operators":
        count = discover_operators(country_code)

        summary["entities"] += count

    # =========================
    # LINK AIRPORT ↔ OPERATOR
    # =========================
    elif mode == "link_airport_operator":
        count = link_airports_to_operators(country_code)

        summary["links"] += count

    # =========================
    # MONITOR
    # =========================
    elif mode == "monitor_sources":
        monitor_sources(country_code)

    # =========================
    # GLOBAL INGEST (manual only)
    # =========================
    elif mode == "bootstrap_airports_global":
        count = ingest_all_airports_global()

        summary["assets"] += count

    else:
        raise ValueError(f"Unknown mode: {mode}")

    return summary
