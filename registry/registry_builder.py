import requests
from core.db import get_conn, now


def crawl_country(country, seeds):
    conn = get_conn()
    cur = conn.cursor()

    for url in seeds:
        try:
            r = requests.get(url, timeout=10, headers={'User-Agent': 'AeroIntel/1.0'})
            text = r.text.lower()

            if 'aeroporto' in text or 'airport' in text:
                cur.execute(
                    '''
                    INSERT OR IGNORE INTO assets
                    (country_code, asset_name, asset_type, source_url, discovered_at)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (country, url, 'airport', url, now())
                )

            cur.execute(
                '''
                INSERT OR IGNORE INTO sources
                (country_code, source_url, source_type, created_at)
                VALUES (?, ?, 'seed', ?)
                ''',
                (country, url, now())
            )
            cur.execute(
                'UPDATE sources SET last_checked_at=? WHERE source_url=?',
                (now(), url)
            )

        except Exception as e:
            cur.execute(
                '''
                INSERT INTO crawler_errors (task_id, error_message, created_at)
                VALUES (?, ?, ?)
                ''',
                (None, f'{country} | {url} | {str(e)}', now())
            )

    conn.commit()
    conn.close()
