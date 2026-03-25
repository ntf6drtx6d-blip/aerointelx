print("TASK_GENERATOR_PG_LOADED", flush=True)

from core.db import get_conn
from core.utils import now_utc, safe_json_loads


def generate_tasks_if_needed():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT COUNT(*) FROM crawl_tasks
    WHERE status IN ('pending', 'running')
    """)
    active = cur.fetchone()[0]

    if active > 0:
        conn.close()
        return 0

    cur.execute("""
    SELECT job_id, countries_json, asset_types_json, entity_types_json, max_tasks_per_run
    FROM crawl_jobs
    WHERE enabled = 1
    """)
    jobs = cur.fetchall()

    created = 0

    for row in jobs:
        job_id = row[0]
        countries_json = row[1]
        asset_types_json = row[2]
        entity_types_json = row[3]
        max_tasks_per_run = row[4] or 10

        countries = safe_json_loads(countries_json) or []
        asset_types = safe_json_loads(asset_types_json) or []
        entity_types = safe_json_loads(entity_types_json) or []

        if not countries:
            continue

        task_count = 0

        for country_code in countries:
            if asset_types:
                for asset_type in asset_types:
                    if entity_types:
                        for entity_type in entity_types:
                            if task_count >= max_tasks_per_run:
                                break

                            cur.execute("""
                            INSERT INTO crawl_tasks (
                                job_id, country_code, asset_type, entity_type,
                                status, retries, created_at, updated_at
                            )
                            VALUES (%s, %s, %s, %s, 'pending', 0, %s, %s)
                            """, (
                                job_id,
                                country_code,
                                asset_type,
                                entity_type,
                                now_utc(),
                                now_utc(),
                            ))
                            created += 1
                            task_count += 1
                    else:
                        if task_count >= max_tasks_per_run:
                            break

                        cur.execute("""
                        INSERT INTO crawl_tasks (
                            job_id, country_code, asset_type,
                            status, retries, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, 'pending', 0, %s, %s)
                        """, (
                            job_id,
                            country_code,
                            asset_type,
                            now_utc(),
                            now_utc(),
                        ))
                        created += 1
                        task_count += 1
            else:
                if task_count >= max_tasks_per_run:
                    break

                cur.execute("""
                INSERT INTO crawl_tasks (
                    job_id, country_code,
                    status, retries, created_at, updated_at
                )
                VALUES (%s, %s, 'pending', 0, %s, %s)
                """, (
                    job_id,
                    country_code,
                    now_utc(),
                    now_utc(),
                ))
                created += 1
                task_count += 1

    conn.commit()
    conn.close()
    return created
