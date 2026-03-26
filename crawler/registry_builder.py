print("REGISTRY_BUILDER_V4_LOADED", flush=True)

import csv
from io import StringIO

import requests

from core.db import upsert_asset, upsert_entity, upsert_source

OURAIRPORTS_URL = "https://ourairports.com/data/airports.csv"


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

        asset_id = upsert_asset(
            country_code=country_code,
            country_name=country_code,  # later we can map code -> full country name
            name=name,
            asset_type=asset_type,
            municipality=row.get("municipality"),
            icao_code=row.get("gps_code") or row.get("ident"),
            iata_code=row.get("iata_code"),
            scheduled_service=row.get("scheduled_service"),
            home_link=row.get("home_link"),
            wikipedia_link=row.get("wikipedia_link"),
            canonical_source_url=OURAIRPORTS_URL,
            city=row.get("municipality"),
            region=row.get("iso_region"),
            status="active" if row.get("scheduled_service") == "yes" else "unknown",
        )

        upsert_source(
            country_code=country_code,
            source_url=OURAIRPORTS_URL,
            source_type="ourairports_csv",
            asset_id=asset_id,
        )

        created += 1

        if created % 1000 == 0:
            print(f"✈️ Inserted {created} airports...", flush=True)

    print(f"✅ DONE: {created} airports loaded", flush=True)
    return created


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
            country_name=country_code,
            name=name,
            asset_type=asset_type,
            municipality=row.get("municipality"),
            icao_code=row.get("gps_code") or row.get("ident"),
            iata_code=row.get("iata_code"),
            scheduled_service=row.get("scheduled_service"),
            home_link=row.get("home_link"),
            wikipedia_link=row.get("wikipedia_link"),
            canonical_source_url=OURAIRPORTS_URL,
            city=row.get("municipality"),
            region=row.get("iso_region"),
            status="active" if row.get("scheduled_service") == "yes" else "unknown",
        )

        upsert_source(
            country_code=country_code,
            source_url=OURAIRPORTS_URL,
            source_type="ourairports_csv",
            asset_id=asset_id,
        )

        created += 1

    return created


def discover_operators(country_code: str):
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


def link_airports_to_operators(country_code: str):
    return 1


def monitor_sources(country_code: str):
    return 0


def crawl_country(country_code: str, mode: str = "bootstrap_airports"):
    print(f"🌐 crawl_country | {country_code} | mode={mode}", flush=True)

    summary = {
        "assets": 0,
        "entities": 0,
        "links": 0,
        "sources": 0,
    }

    if mode == "bootstrap_airports":
        count = ingest_airports_for_country(country_code)
        summary["assets"] += count
        summary["sources"] += count

    elif mode == "bootstrap_operators":
        count = discover_operators(country_code)
        summary["entities"] += count

    elif mode == "link_airport_operator":
        count = link_airports_to_operators(country_code)
        summary["links"] += count

    elif mode == "monitor_sources":
        monitor_sources(country_code)

    elif mode == "bootstrap_airports_global":
        count = ingest_all_airports_global()
        summary["assets"] += count
        summary["sources"] += count

    else:
        raise ValueError(f"Unknown mode: {mode}")

    return summary
