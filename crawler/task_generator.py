from core.db import db_cursor
from core.utils import now_utc, safe_json_loads

def generate_tasks_if_needed():
    with db_cursor() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM crawl_tasks WHERE status IN ('pending', 'running')")
        if cur.fetchone()[0] > 0:
            return 0
        cur.execute("SELECT job_id, countries_json, max_tasks_per_run FROM crawl_jobs WHERE enabled=1 ORDER BY job_id ASC")
        jobs = cur.fetchall()
        created = 0
        for row in jobs:
            countries = [c.strip() for c in safe_json_loads(row["countries_json"]) if c.strip()]
            limit = row["max_tasks_per_run"] or len(countries)
            for country_code in countries[:limit]:
                cur.execute("""
                    INSERT INTO crawl_tasks (job_id, country_code, asset_type, entity_type, status, retries, created_at, updated_at)
                    VALUES (?, ?, NULL, NULL, 'pending', 0, ?, ?)
                """, (row["job_id"], country_code, now_utc(), now_utc()))
                created += 1
            cur.execute("UPDATE crawl_jobs SET last_run_at=?, updated_at=? WHERE job_id=?", (now_utc(), now_utc(), row["job_id"]))
        return created
