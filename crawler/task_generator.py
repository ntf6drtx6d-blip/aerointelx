from core.db import get_conn
from core.utils import now_utc, safe_json_loads


def generate_tasks_if_needed():
    conn = get_conn()
    cur = conn.cursor()

    # якщо вже є активні tasks — не створюємо нові
    cur.execute("""
    SELECT COUNT(*) FROM crawl_tasks
    WHERE status IN ('pending', 'running')
    """)
    active = cur.fetchone()[0]

    if active > 0:
        conn.close()
        return 0

    # беремо тільки enabled jobs
    cur.execute("""
    SELECT job_id, countries
    FROM crawl_jobs
    WHERE enabled = 1
    """)
    jobs = cur.fetchall()

    created = 0

    for job_id, countries in jobs:
        for country in countries.split(","):
            country_code = country.strip()
            if not country_code:
                continue

            cur.execute("""
            INSERT INTO crawl_tasks
            (job_id, country_code, status, retries, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                job_id,
                country_code,
                "pending",
                0,
                now_utc(),
                now_utc(),
            ))
            created += 1

    conn.commit()
    conn.close()
    return created
