from urllib.parse import urlparse

from bs4 import BeautifulSoup

from core.db import (
    record_error,
    touch_source_checked,
    upsert_asset,
    upsert_entity,
    upsert_source,
)
from core.http_client import FetchError, build_session, fetch_url
from core.utils import normalize_text


AIRPORT_KEYWORDS = [
    "airport",
    "airports",
    "aeroporto",
    "aeropuerto",
    "aerodrome",
    "aeródromo",
    "airstrip",
]

OPERATOR_KEYWORDS = [
    "operator",
    "airport operator",
    "operador",
    "concession",
    "concessionaire",
    "aena",
    "vinci",
    "fraport",
    "infraero",
]

MUNICIPALITY_KEYWORDS = [
    "municipality",
    "municipal",
    "prefeitura",
    "ayuntamiento",
    "gobierno local",
]

MINISTRY_KEYWORDS = [
    "ministry",
    "ministerio",
    "ministério",
    "aviation authority",
    "civil aviation",
]

MINING_KEYWORDS = [
    "mining",
    "mine",
    "mineracao",
    "mineração",
    "mineria",
]

MILITARY_KEYWORDS = [
    "air force",
    "military",
    "navy",
    "army",
    "base aérea",
    "fuerza aérea",
]


def guess_entity_type(text: str) -> str | None:
    t = text.lower()

    if any(k in t for k in OPERATOR_KEYWORDS):
        return "operator"
    if any(k in t for k in MUNICIPALITY_KEYWORDS):
        return "municipality"
    if any(k in t for k in MINISTRY_KEYWORDS):
        return "ministry"
    if any(k in t for k in MINING_KEYWORDS):
        return "mining_company"
    if any(k in t for k in MILITARY_KEYWORDS):
        return "military_authority"

    return None


def looks_like_airport_name(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in AIRPORT_KEYWORDS)


def extract_candidates_from_html(html: str) -> tuple[list[str], list[tuple[str, str]]]:
    soup = BeautifulSoup(html, "html.parser")

    airport_names = []
    entities = []

    seen_airports = set()
    seen_entities = set()

    # h1/h2/h3/li/a часто дають досить добрі сигнали
    tags = soup.find_all(["h1", "h2", "h3", "li", "a", "title"])

    for tag in tags:
        text = normalize_text(tag.get_text(" ", strip=True))
        if not text or len(text) < 4:
            continue

        if looks_like_airport_name(text):
            if text not in seen_airports:
                seen_airports.add(text)
                airport_names.append(text)

        entity_type = guess_entity_type(text)
        if entity_type:
            key = (text, entity_type)
            if key not in seen_entities:
                seen_entities.add(key)
                entities.append(key)

    return airport_names[:20], entities[:20]


def make_source_type(seed_name: str) -> str:
    s = seed_name.lower()

    if "operator" in s or "vinci" in s or "aena" in s or "infraero" in s or "fraport" in s:
        return "operator_site"
    if "ministry" in s or "minister" in s or "aviation" in s:
        return "authority_site"
    if "municip" in s or "prefeitura" in s:
        return "municipality_site"

    return "seed"


def crawl_country(country_code: str, seeds: list[dict]):
    session = build_session()

    assets_added_hint = 0
    entities_added_hint = 0
    links_added_hint = 0
    sources_added_hint = 0

    try:
        for seed in seeds:
            seed_name = seed.get("name", "unknown-seed")
            url = seed.get("url")
            seed_type = seed.get("type", "seed")

            if not url:
                continue

            try:
                html = fetch_url(url, session=session, timeout=20, retries=3)
                touch_source_checked(url)

                source_type = make_source_type(seed_name)
                upsert_source(
                    country_code=country_code,
                    source_url=url,
                    source_type=source_type,
                    priority=1,
                    active=1,
                )
                sources_added_hint += 1

                airport_names, entities = extract_candidates_from_html(html)

                for airport_name in airport_names:
                    upsert_asset(
                        country_code=country_code,
                        asset_name=airport_name,
                        asset_type="airport",
                        canonical_source_url=url,
                    )
                    assets_added_hint += 1

                for entity_name, entity_type in entities:
                    domain = urlparse(url).netloc
                    upsert_entity(
                        country_code=country_code,
                        entity_name=entity_name,
                        entity_type=entity_type,
                        official_domain=domain,
                        notes=f"Discovered from {seed_name}",
                    )
                    entities_added_hint += 1

            except FetchError as e:
                record_error(
                    task_id=None,
                    country_code=country_code,
                    seed_name=seed_name,
                    url=url,
                    stage="fetch",
                    error_text=str(e),
                )
            except Exception as e:
                record_error(
                    task_id=None,
                    country_code=country_code,
                    seed_name=seed_name,
                    url=url,
                    stage="parse",
                    error_text=str(e),
                )

    finally:
        session.close()

    return {
        "assets_added_hint": assets_added_hint,
        "entities_added_hint": entities_added_hint,
        "links_added_hint": links_added_hint,
        "sources_added_hint": sources_added_hint,
    }
