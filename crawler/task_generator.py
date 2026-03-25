from core.db import get_conn, now


def generate_tasks():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute('SELECT job_id, countries FROM crawl_jobs WHERE enabled=1')
    jobs = cur.fetchall()

    for job_id, countries in jobs:
        for c in countries.split(','):
            cur.execute(
                '''
                INSERT INTO crawl_tasks
                (job_id, country_code, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
                ''',
                (job_id, c.strip(), now(), now())
            )

    conn.commit()
    conn.close()
