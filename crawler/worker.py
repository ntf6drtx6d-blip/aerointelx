import time

from core.db import get_conn, record_error
from core.utils import now_utc
from crawler.task_generator import generate_tasks_if_needed
from registry.registry_builder import crawl_country
from configs.seeds import SEEDS

WORKER_ID = "worker_1"


def heartbeat(task="idle"):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO worker_status (worker_id, last_heartbeat, current_task, processed_tasks, errors, updated_at)
            VALUES (%s, %s, %s, 0, 0, %s)
            ON CONFLICT (worker_id)
            DO UPDATE SET
                last_heartbeat = EXCLUDED.last_heartbeat,
                current_task = EXCLUDED.current_task,
                updated_at = EXCLUDED.updated_at
        """, (WORKER_ID, now_utc(), task, now_utc()))
        conn.commit()
    finally:
        conn.close()


def get_task():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT task_id, country_code
            FROM crawl_tasks
            WHERE status = 'pending'
            ORDER BY task_id ASC
            LIMIT 1
        """)
        row = cur.fetchone()

        if not row:
            return None

        task_id, country_code = row

        cur.execute("""
            UPDATE crawl_tasks
            SET status = 'running',
                started_at = %s,
                updated_at = %s
            WHERE task_id = %s
        """, (now_utc(), now_utc(), task_id))
        conn.commit()

        return task_id, country_code
    finally:
        conn.close()


def finish_task(task_id, summary=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE crawl_tasks
            SET status = 'done',
                finished_at = %s,
                updated_at = %s,
                notes = %s
            WHERE task_id = %s
        """, (now_utc(), now_utc(), str(summary or ""), task_id))
        conn.commit()
    finally:
        conn.close()


def fail_task(task_id, error_text):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE crawl_tasks
            SET status = 'failed',
                retries = retries + 1,
                updated_at = %s,
                notes = %s
            WHERE task_id = %s
        """, (now_utc(), str(error_text), task_id))
        conn.commit()
    finally:
        conn.close()


def worker_loop():
    print("🟢 Worker loop started", flush=True)

    while True:
        heartbeat("idle")

        created = generate_tasks_if_needed()
        if created:
            print(f"🧩 Generated {created} new tasks", flush=True)

        task = get_task()
        if not task:
            time.sleep(10)
            continue

        task_id, country_code = task
        heartbeat(f"processing {country_code}")

        seeds = SEEDS.get(country_code, [])
        print(f"🚧 Processing task {task_id} | country={country_code}", flush=True)
        print(f"🌐 Using {len(seeds)} seeds for {country_code}", flush=True)

        try:
            summary = crawl_country(country_code, seeds)
            finish_task(task_id, summary=summary)
            print(f"✅ Finished task {task_id} | country={country_code} | summary={summary}", flush=True)
        except Exception as e:
            fail_task(task_id, str(e))
            record_error(
                task_id=task_id,
                country_code=country_code,
                seed_name=None,
                url=None,
                stage="worker_loop",
                error_text=str(e),
            )
            print(f"❌ Failed task {task_id} | country={country_code} | error={e}", flush=True)

        time.sleep(2)
