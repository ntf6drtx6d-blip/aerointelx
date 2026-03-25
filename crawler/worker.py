import time
from configs.seeds import SEEDS
from core.db import db_cursor, now_utc, record_error
from crawler.task_generator import generate_tasks_if_needed
from registry.registry_builder import crawl_country

WORKER_ID = "worker_1"

def heartbeat(task="idle", processed_delta=0, errors_delta=0):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO worker_status (worker_id, last_heartbeat, current_task, processed_tasks, errors, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                last_heartbeat=excluded.last_heartbeat,
                current_task=excluded.current_task,
                processed_tasks=worker_status.processed_tasks + ?,
                errors=worker_status.errors + ?,
                updated_at=excluded.updated_at
        """, (WORKER_ID, now_utc(), task, 0, 0, now_utc(), processed_delta, errors_delta))

def get_next_task():
    with db_cursor() as (conn, cur):
        cur.execute("SELECT task_id, country_code FROM crawl_tasks WHERE status='pending' ORDER BY task_id ASC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("UPDATE crawl_tasks SET status='running', started_at=?, updated_at=? WHERE task_id=?", (now_utc(), now_utc(), row["task_id"]))
        return row["task_id"], row["country_code"]

def finish_task(task_id: int, notes=None):
    with db_cursor() as (conn, cur):
        cur.execute("UPDATE crawl_tasks SET status='done', finished_at=?, updated_at=?, notes=? WHERE task_id=?", (now_utc(), now_utc(), notes, task_id))

def fail_task(task_id: int, error_message: str):
    with db_cursor() as (conn, cur):
        cur.execute("UPDATE crawl_tasks SET status='failed', retries=retries+1, finished_at=?, updated_at=?, notes=? WHERE task_id=?", (now_utc(), now_utc(), str(error_message), task_id))
    record_error(task_id=task_id, stage="worker", error_text=error_message)

def worker_loop():
    print("🟢 Worker loop started", flush=True)
    while True:
        heartbeat("idle")
        created = generate_tasks_if_needed()
        if created:
            print(f"🧩 Generated {created} new tasks", flush=True)
        task = get_next_task()
        if not task:
            print("⏳ No tasks found. Sleeping 10s", flush=True)
            time.sleep(10)
            continue
        task_id, country_code = task
        print(f"🚧 Processing task {task_id} | country={country_code}", flush=True)
        heartbeat(f"processing {country_code}")
        try:
            seeds = SEEDS.get(country_code, [])
            print(f"🌐 Using {len(seeds)} seeds for {country_code}", flush=True)
            summary = crawl_country(country_code, seeds, task_id=task_id)
            finish_task(task_id, notes=str(summary))
            heartbeat("idle", processed_delta=1)
            print(f"✅ Finished task {task_id} | country={country_code} | summary={summary}", flush=True)
        except Exception as e:
            fail_task(task_id, str(e))
            heartbeat("idle", errors_delta=1)
            print(f"❌ Failed task {task_id} | country={country_code} | error={e}", flush=True)
        time.sleep(2)
