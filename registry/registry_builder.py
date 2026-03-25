from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from core.db import record_error, touch_source_checked, upsert_asset, upsert_entity, upsert_source

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AeroIntelRegistry/1.0)"}

def _extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""

def _fetch(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return response.text

def _extract_title_and_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    body = soup.get_text(" ", strip=True)
    if len(body) > 15000:
        body = body[:15000]
    return title, body

def _maybe_seed_entity(country_code: str, seed_name: str, seed_type: str, seed_url: str):
    n = (seed_name or "").lower()
    if "infraero" in n:
        entity_id = upsert_entity(country_code, "Infraero", "operator", official_domain=_extract_domain(seed_url))
        upsert_source(country_code, seed_url, "operator_page", entity_id=entity_id, priority=1)
    elif "anac" in n or "afac" in n or "aerocivil" in n or "casa" in n:
        entity_id = upsert_entity(country_code, seed_name, "regulator", official_domain=_extract_domain(seed_url))
        upsert_source(country_code, seed_url, "regulator_page", entity_id=entity_id, priority=1)
    elif "ministry" in n:
        entity_id = upsert_entity(country_code, seed_name, "ministry", official_domain=_extract_domain(seed_url))
        upsert_source(country_code, seed_url, "ministry_page", entity_id=entity_id, priority=1)

def crawl_seed(country_code: str, seed: dict, task_id=None):
    seed_name = seed.get("name", "")
    seed_type = seed.get("type", "seed")
    seed_url = seed.get("url", "")
    try:
        html = _fetch(seed_url)
        title, body = _extract_title_and_text(html)
        upsert_source(country_code, seed_url, seed_type, priority=1)
        touch_source_checked(seed_url)
        combined = f"{title} {body}".lower()
        if any(k in combined for k in ["airport", "aeroporto", "aeropuerto", "aerodrome", "aeródromo"]):
            asset_name = title.strip() or seed_name.strip() or seed_url
            asset_name = asset_name[:200]
            asset_id = upsert_asset(country_code, asset_name, "airport", canonical_source_url=seed_url)
            upsert_source(country_code, seed_url, "airport_page", asset_id=asset_id, priority=1)
        _maybe_seed_entity(country_code, seed_name, seed_type, seed_url)
        return {"assets_added_hint": 1 if any(k in combined for k in ["airport", "aeroporto", "aeropuerto", "aerodrome", "aeródromo"]) else 0,
                "entities_added_hint": 1, "links_added_hint": 0, "sources_added_hint": 1}
    except Exception as e:
        record_error(task_id=task_id, country_code=country_code, seed_name=seed_name, url=seed_url, stage="crawl_seed", error_text=e)
        return {"assets_added_hint": 0, "entities_added_hint": 0, "links_added_hint": 0, "sources_added_hint": 0}

def crawl_country(country_code: str, seeds: list[dict], task_id=None):
    summary = {"assets_added_hint": 0, "entities_added_hint": 0, "links_added_hint": 0, "sources_added_hint": 0}
    for seed in seeds:
        result = crawl_seed(country_code, seed, task_id=task_id)
        for key in summary:
            summary[key] += result.get(key, 0)
    return summary
