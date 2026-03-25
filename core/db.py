import os
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from core.utils import now_utc

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)

@contextmanager
def db_cursor():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield conn, cur
        conn.commit()
    finally:
        cur.close()
        conn.close()

def init_db():
    with db_cursor() as (conn, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            country_code TEXT PRIMARY KEY,
            country_name TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            asset_id SERIAL PRIMARY KEY,
            country_code TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            asset_type TEXT NOT NULL DEFAULT 'unknown',
            icao_code TEXT,
            iata_code TEXT,
            city TEXT,
            region TEXT,
            status TEXT DEFAULT 'unknown',
            canonical_source_url TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(country_code, asset_name)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            entity_id SERIAL PRIMARY KEY,
            country_code TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            official_domain TEXT,
            notes TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(country_code, entity_name, entity_type)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS asset_entity_links (
            link_id SERIAL PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            source_url TEXT,
            source_quote TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(asset_id, entity_id, role)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            source_id SERIAL PRIMARY KEY,
            country_code TEXT NOT NULL,
            source_url TEXT NOT NULL UNIQUE,
            source_type TEXT NOT NULL,
            asset_id INTEGER,
            entity_id INTEGER,
            priority INTEGER DEFAULT 2,
            active INTEGER DEFAULT 1,
            last_checked_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS crawl_jobs (
            job_id SERIAL PRIMARY KEY,
            job_name TEXT NOT NULL,
            countries_json TEXT NOT NULL,
            asset_types_json TEXT NOT NULL DEFAULT '[]',
            entity_types_json TEXT NOT NULL DEFAULT '[]',
            mode TEXT NOT NULL DEFAULT 'broad',
            enabled INTEGER DEFAULT 1,
            run_interval_minutes INTEGER DEFAULT 60,
            max_tasks_per_run INTEGER DEFAULT 10,
            requests_per_minute INTEGER DEFAULT 20,
            last_run_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS crawl_tasks (
            task_id SERIAL PRIMARY KEY,
            job_id INTEGER NOT NULL,
            country_code TEXT NOT NULL,
            asset_type TEXT,
            entity_type TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            retries INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT NOT NULL,
            notes TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS worker_status (
            worker_id TEXT PRIMARY KEY,
            last_heartbeat TEXT NOT NULL,
            current_task TEXT,
            processed_tasks INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS crawler_errors (
            error_id SERIAL PRIMARY KEY,
            task_id INTEGER,
            country_code TEXT,
            seed_name TEXT,
            url TEXT,
            stage TEXT,
            error_text TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS crawler_runs (
            run_id SERIAL PRIMARY KEY,
            job_id INTEGER,
            worker_id TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            tasks_processed INTEGER DEFAULT 0,
            assets_added INTEGER DEFAULT 0,
            entities_added INTEGER DEFAULT 0,
            links_added INTEGER DEFAULT 0,
            sources_added INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            notes TEXT
        )
        """)
        from configs.countries import COUNTRIES
        for code, name in COUNTRIES:
            cur.execute("""
                INSERT INTO countries (country_code, country_name, enabled, created_at, updated_at)
                VALUES (%s, %s, 1, %s, %s)
                ON CONFLICT (country_code) DO UPDATE
                SET country_name = EXCLUDED.country_name,
                    updated_at = EXCLUDED.updated_at
            """, (code, name, now_utc(), now_utc()))

def create_default_job_if_missing():
    with db_cursor() as (conn, cur):
        cur.execute("SELECT COUNT(*) AS cnt FROM crawl_jobs")
        row = cur.fetchone()
        cnt = row["cnt"] if isinstance(row, dict) else row[0]
        if cnt == 0:
            cur.execute("""
                INSERT INTO crawl_jobs (
                    job_name, countries_json, asset_types_json, entity_types_json,
                    mode, enabled, run_interval_minutes, max_tasks_per_run,
                    requests_per_minute, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                "main-registry",
                '["BR","MX","CO"]',
                '["airport","airstrip","military_base"]',
                '["operator","municipality","ministry","mining_company","military_authority"]',
                "broad", 1, 60, 10, 20, now_utc(), now_utc()
            ))

def upsert_asset(country_code, asset_name, asset_type="unknown", canonical_source_url=None):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO assets (country_code, asset_name, asset_type, canonical_source_url, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (country_code, asset_name) DO UPDATE
            SET asset_type = EXCLUDED.asset_type,
                canonical_source_url = COALESCE(EXCLUDED.canonical_source_url, assets.canonical_source_url),
                updated_at = EXCLUDED.updated_at
            RETURNING asset_id
        """, (country_code, asset_name, asset_type, canonical_source_url, now_utc(), now_utc()))
        row=cur.fetchone()
        return row["asset_id"] if isinstance(row, dict) else row[0]

def upsert_entity(country_code, entity_name, entity_type, official_domain=None, notes=None):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO entities (country_code, entity_name, entity_type, official_domain, notes, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (country_code, entity_name, entity_type) DO UPDATE
            SET official_domain = COALESCE(EXCLUDED.official_domain, entities.official_domain),
                notes = COALESCE(EXCLUDED.notes, entities.notes),
                updated_at = EXCLUDED.updated_at
            RETURNING entity_id
        """, (country_code, entity_name, entity_type, official_domain, notes, now_utc(), now_utc()))
        row=cur.fetchone()
        return row["entity_id"] if isinstance(row, dict) else row[0]

def upsert_source(country_code, source_url, source_type, asset_id=None, entity_id=None, priority=2, active=1):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO sources (country_code, source_url, source_type, asset_id, entity_id, priority, active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_url) DO UPDATE
            SET source_type = EXCLUDED.source_type,
                asset_id = COALESCE(EXCLUDED.asset_id, sources.asset_id),
                entity_id = COALESCE(EXCLUDED.entity_id, sources.entity_id),
                priority = EXCLUDED.priority,
                active = EXCLUDED.active,
                updated_at = EXCLUDED.updated_at
            RETURNING source_id
        """, (country_code, source_url, source_type, asset_id, entity_id, priority, active, now_utc(), now_utc()))
        row=cur.fetchone()
        return row["source_id"] if isinstance(row, dict) else row[0]

def record_error(task_id=None, country_code=None, seed_name=None, url=None, stage=None, error_text=""):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO crawler_errors (task_id, country_code, seed_name, url, stage, error_text, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (task_id, country_code, seed_name, url, stage, str(error_text), now_utc()))
