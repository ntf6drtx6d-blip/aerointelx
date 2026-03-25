from core.db import db_cursor
from core.utils import now_utc, safe_json_loads

def generate_tasks_if_needed():
    conn = get_conn()
    cur = conn.cursor()

    # ❗ перевірка — чи вже є незавершені задачі
    cur.execute("""
    SELECT COUNT(*) FROM crawl_tasks
    WHERE status IN ('pending', 'running')
    """)
    active = cur.fetchone()[0]

    if active > 0:
        conn.close()
        return 0

    # тільки якщо все завершено → генеруємо нові
    cur.execute("""
    SELECT job_id, countries
    FROM crawl_jobs
    WHERE enabled = 1
    """)
    jobs = cur.fetchall()

    created = 0

    for job_id, countries in jobs:
        for country in countries.split(","):
            cur.execute("""
            INSERT INTO crawl_tasks (job_id, country_code, status)
            VALUES (?, ?, 'pending')
            """, (job_id, country.strip()))
            created += 1

    conn.commit()
    conn.close()
    return created
