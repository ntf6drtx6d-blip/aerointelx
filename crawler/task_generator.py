print("TASK_GENERATOR_TASK_KIND_LOADED", flush=True)

from core.db import get_conn
from core.utils import now_utc, safe_json_loads


MIN_AIRPORTS_PER_COUNTRY = 20
MIN_OPERATORS_PER_COUNTRY = 3


def _task_exists(cur, job_id, country_code, task_kind, target_asset_id=None, target_entity_id=None):
    cur.execute("""
        SELECT COUNT(*)
        FROM crawl_tasks
        WHERE job_id = %s
          AND country_code = %s
          AND task_kind = %s
          AND status IN ('pending', 'running')
          AND COALESCE(target_asset_id, -1) = COALESCE(%s, -1)
          AND COALESCE(target_entity_id, -1) = COALESCE(%s, -1)
    """, (job_id, country_code, task_kind, target_asset_id, target_entity_id))
    return cur.fetchone()[0] > 0


def _insert_task(cur, job_id, country_code, task_kind, priority=5, target_url=None, target_asset_id=None, target_entity_id=None, notes=None):
    cur.execute("""
        INSERT INTO crawl_tasks (
            job_id,
            country_code,
            task_kind,
            target_url,
            target_asset_id,
            target_entity_id,
            priority,
            status,
            retries,
            created_at,
            updated_at,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', 0, %s, %s, %s)
    """, (
        job_id,
        country_code,
        task_kind,
        target_url,
        target_asset_id,
        target_entity_id,
        priority,
        now_utc(),
        now_utc(),
        notes,
    ))


def _country_stats(cur, country_code):
    cur.execute("""
        SELECT COUNT(*) FROM assets
        WHERE country_code = %s
    """, (country_code,))
    assets_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM entities
        WHERE country_code = %s
          AND entity_type = 'operator'
    """, (country_code,))
    operators_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM asset_entity_links l
        JOIN assets a ON a.asset_id = l.asset_id
        JOIN entities e ON e.entity_id = l.entity_id
        WHERE a.country_code = %s
          AND e.entity_type = 'operator'
    """, (country_code,))
    operator_links_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM sources
        WHERE country_code = %s
    """, (country_code,))
    sources_count = cur.fetchone()[0]

    return {
        "assets": assets_count,
        "operators": operators_count,
        "operator_links": operator_links_count,
        "sources": sources_count,
    }


def _generate_for_country(cur, job_id, country_code):
    created = 0
    stats = _country_stats(cur, country_code)

    # 1. Якщо мало аеропортів → качаємо master lists / airport discovery
    if stats["assets"] < MIN_AIRPORTS_PER_COUNTRY:
        if not _task_exists(cur, job_id, country_code, "bootstrap_airports"):
            _insert_task(
                cur,
                job_id,
                country_code,
                task_kind="bootstrap_airports",
                priority=1,
                notes="Need more airports in registry",
            )
            created += 1

    # 2. Якщо аеропорти є, але операторів мало → шукаємо операторів
    if stats["assets"] > 0 and stats["operators"] < MIN_OPERATORS_PER_COUNTRY:
        if not _task_exists(cur, job_id, country_code, "bootstrap_operators"):
            _insert_task(
                cur,
                job_id,
                country_code,
                task_kind="bootstrap_operators",
                priority=2,
                notes="Need more airport operators",
            )
            created += 1

    # 3. Якщо оператори є, але нема links → лінкуємо airport ↔ operator
    if stats["assets"] > 0 and stats["operators"] > 0 and stats["operator_links"] == 0:
        if not _task_exists(cur, job_id, country_code, "link_airport_operator"):
            _insert_task(
                cur,
                job_id,
                country_code,
                task_kind="link_airport_operator",
                priority=3,
                notes="Need airport-operator relationships",
            )
            created += 1

    # 4. Якщо база вже якось заповнена → monitoring
    if stats["assets"] >= MIN_AIRPORTS_PER_COUNTRY:
        if not _task_exists(cur, job_id, country_code, "monitor_sources"):
            _insert_task(
                cur,
                job_id,
                country_code,
                task_kind="monitor_sources",
                priority=5,
                notes="Monitoring known sources",
            )
            created += 1

    return created


def generate_tasks_if_needed():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # Якщо вже є pending/running задачі — не плодимо нові
        cur.execute("""
            SELECT COUNT(*)
            FROM crawl_tasks
            WHERE status IN ('pending', 'running')
        """)
        active = cur.fetchone()[0]

        if active > 0:
            conn.close()
            return 0

        cur.execute("""
            SELECT job_id, countries_json, enabled
            FROM crawl_jobs
            WHERE enabled = 1
            ORDER BY job_id ASC
        """)
        jobs = cur.fetchall()

        created = 0

        for row in jobs:
            job_id = row[0]
            countries_json = row[1]

            countries = safe_json_loads(countries_json) or []
            if not countries:
                continue

            for country_code in countries:
                created += _generate_for_country(cur, job_id, country_code)

        conn.commit()
        return created

    finally:
        conn.close()
