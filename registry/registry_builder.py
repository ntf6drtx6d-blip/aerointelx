import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from core.db import upsert_asset, upsert_entity, upsert_source, record_error
from core.utils import normalize_text

def extract_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

def crawl_country(country_code, seeds):
    assets_added = 0
    entities_added = 0
    sources_added = 0
    headers = {"User-Agent": "AeroIntel/1.0"}
    for seed in seeds:
        try:
            seed_name = seed["name"]
            url = seed["url"]
            source_type = seed.get("type", "seed")
            upsert_source(country_code, url, source_type)
            sources_added += 1
            response = requests.get(url, headers=headers, timeout=20)
            text = response.text
            soup = BeautifulSoup(text, "html.parser")
            page_text = normalize_text(soup.get_text(" ", strip=True))
            if "airport" in page_text.lower() or "aeroporto" in page_text.lower():
                upsert_asset(country_code, seed_name, "airport", url)
                assets_added += 1
            domain = extract_domain(url)
            if domain:
                upsert_entity(country_code, seed_name, source_type, domain, None)
                entities_added += 1
        except Exception as e:
            record_error(country_code=country_code, seed_name=seed.get("name"), url=seed.get("url"), stage="crawl_country", error_text=e)
    return {
        "assets_added_hint": assets_added,
        "entities_added_hint": entities_added,
        "links_added_hint": 0,
        "sources_added_hint": sources_added,
    }
