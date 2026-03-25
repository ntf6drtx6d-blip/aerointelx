from time import sleep
from core.db import get_conn, record_error
from core.utils import now_utc
from crawler.task_generator import generate_tasks_if_needed
from registry.registry_builder import crawl_country
from configs.seeds import SEEDS

WORKER_ID = "worker_1"

def heartbeat(task="idle"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO worker_status (worker_id, last_heartbeat, current_task, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (worker_id) DO UPDATE
        SET last_heartbeat = EXCLUDED.last_heartbeat,
            current_task = EXCLUDED.current_task,
            updated_at = EXCLUDED.updated_at
    """, (WORKER_ID, now_utc(), task, now_utc()))
    conn.commit()
    conn.close()

def get_next_task():
    conn = get_conn()
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
        conn.close()
        return None
    task_id, country_code = row[0], row[1]
    cur.execute("""
        UPDATE crawl_tasks
        SET status = 'running', started_at = %s, updated_at = %s
        WHERE task_id = %s
    """, (now_utc(), now_utc(), task_id))
    conn.commit()
    conn.close()
    return task_id, country_code

def finish_task(task_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE crawl_tasks
        SET status = 'done', finished_at = %s, updated_at = %s
        WHERE task_id = %s
    """, (now_utc(), now_utc(), task_id))
    conn.commit()
    conn.close()

def fail_task(task_id, error_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE crawl_tasks
        SET status = 'failed', retries = retries + 1, updated_at = %s
        WHERE task_id = %s
    """, (now_utc(), task_id))
    conn.commit()
    conn.close()
    record_error(task_id=task_id, stage="worker", error_text=str(error_text))

def worker_loop():
    print("🟢 Worker loop started", flush=True)
    while True:
        heartbeat("idle")
        created = generate_tasks_if_needed()
        if created:
            print(f"🧩 Generated {created} new tasks", flush=True)
        task = get_next_task()
        if not task:
            sleep(5)
            continue
        task_id, country_code = task
        heartbeat(f"processing {country_code}")
        print(f"🚧 Processing task {task_id} | country={country_code}", flush=True)
        try:
            seeds = SEEDS.get(country_code, [])
            print(f"🌐 Using {len(seeds)} seeds for {country_code}", flush=True)
            summary = crawl_country(country_code, seeds)
            finish_task(task_id)
            print(f"✅ Finished task {task_id} | country={country_code} | summary={summary}", flush=True)
        except Exception as e:
            fail_task(task_id, e)
            print(f"❌ Failed task {task_id} | country={country_code} | error={e}", flush=True)
