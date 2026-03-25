import random
import time
from typing import Optional

import requests


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}


class FetchError(Exception):
    pass


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def polite_sleep(base_min: float = 1.0, base_max: float = 2.5):
    time.sleep(random.uniform(base_min, base_max))


def fetch_url(
    url: str,
    session: Optional[requests.Session] = None,
    timeout: int = 20,
    retries: int = 3,
    backoff_base: float = 2.0,
) -> str:
    """
    Стабільний fetch:
    - retry
    - exponential backoff
    - random delay
    - fail only after N tries
    """
    own_session = False
    if session is None:
        session = build_session()
        own_session = True

    last_error = None

    try:
        for attempt in range(1, retries + 1):
            try:
                polite_sleep(0.8, 2.0)

                response = session.get(
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                )
                response.raise_for_status()

                # requests сам часто вгадає encoding, але іноді ні
                if not response.encoding:
                    response.encoding = response.apparent_encoding

                return response.text

            except requests.RequestException as e:
                last_error = e

                # остання спроба → кидаємо помилку
                if attempt == retries:
                    break

                sleep_seconds = backoff_base ** (attempt - 1) + random.uniform(0.5, 1.5)
                time.sleep(sleep_seconds)

        raise FetchError(f"Failed to fetch {url}: {last_error}")

    finally:
        if own_session:
            session.close()
