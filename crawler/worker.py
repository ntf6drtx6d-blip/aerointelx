import time
from core.db import get_conn, now
from registry.registry_builder import crawl_country
from configs.seeds import SEEDS

WORKER_ID = 'worker_1'


def heartbeat(task='idle'):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        '''
        INSERT INTO worker_status (worker_id, last_heartbeat, current_task, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
            last_heartbeat=excluded.last_heartbeat,
            current_task=excluded.current_task,
            updated_at=excluded.updated_at
        ''',
        (WORKER_ID, now(), task, now())
    )

    conn.commit()
    conn.close()


def get_task():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        '''
        SELECT task_id, country_code
        FROM crawl_tasks
        WHERE status='pending'
        ORDER BY task_id ASC
        LIMIT 1
        '''
    )

    row = cur.fetchone()

    if not row:
        conn.close()
        return None

    task_id, country = row

    cur.execute(
        '''
        UPDATE crawl_tasks
        SET status='running', started_at=?, updated_at=?
        WHERE task_id=?
        ''',
        (now(), now(), task_id)
    )

    conn.commit()
    conn.close()

    return task_id, country


def finish_task(task_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        '''
        UPDATE crawl_tasks
        SET status='done', finished_at=?, updated_at=?
        WHERE task_id=?
        ''',
        (now(), now(), task_id)
    )

    conn.commit()
    conn.close()


def fail_task(task_id, error_message):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        '''
        UPDATE crawl_tasks
        SET status='failed', retries=retries+1, updated_at=?
        WHERE task_id=?
        ''',
        (now(), task_id)
    )

    cur.execute(
        '''
        INSERT INTO crawler_errors (task_id, error_message, created_at)
        VALUES (?, ?, ?)
        ''',
        (task_id, error_message, now())
    )

    conn.commit()
    conn.close()


def worker_loop():
    while True:
        heartbeat('idle')

        task = get_task()

        if not task:
            time.sleep(5)
            continue

        task_id, country = task
        heartbeat(f'processing {country}')

        try:
            crawl_country(country, SEEDS.get(country, []))
            finish_task(task_id)
        except Exception as e:
            fail_task(task_id, str(e))

        heartbeat('done')
