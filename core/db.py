import sqlite3
from contextlib import contextmanager
from pathlib import Path
from core.utils import now_utc

DB_PATH = Path("data/aerointel.db")

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

@contextmanager
def db_cursor():
    conn = get_conn()
    cur = conn.cursor()
    try:
        yield conn, cur
        conn.commit()
    finally:
        conn.close()

def init_db():
    with db_cursor() as (conn, cur):
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS countries (
            country_code TEXT PRIMARY KEY,
            country_name TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        );
        CREATE TABLE IF NOT EXISTS entities (
            entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            official_domain TEXT,
            notes TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(country_code, entity_name, entity_type)
        );
        CREATE TABLE IF NOT EXISTS asset_entity_links (
            link_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            source_url TEXT,
            source_quote TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(asset_id, entity_id, role),
            FOREIGN KEY(asset_id) REFERENCES assets(asset_id) ON DELETE CASCADE,
            FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            source_url TEXT NOT NULL UNIQUE,
            source_type TEXT NOT NULL,
            asset_id INTEGER,
            entity_id INTEGER,
            priority INTEGER DEFAULT 2,
            active INTEGER DEFAULT 1,
            last_checked_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(asset_id) REFERENCES assets(asset_id) ON DELETE SET NULL,
            FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS crawl_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        );
        CREATE TABLE IF NOT EXISTS crawl_tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES crawl_jobs(job_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS worker_status (
            worker_id TEXT PRIMARY KEY,
            last_heartbeat TEXT NOT NULL,
            current_task TEXT,
            processed_tasks INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS crawler_errors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            country_code TEXT,
            seed_name TEXT,
            url TEXT,
            stage TEXT,
            error_text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(task_id) REFERENCES crawl_tasks(task_id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS crawler_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES crawl_jobs(job_id) ON DELETE SET NULL
        );
        """)
        try:
            from configs.countries import COUNTRIES
            for code, name in COUNTRIES:
                cur.execute("""
                    INSERT INTO countries (country_code, country_name, enabled, created_at, updated_at)
                    VALUES (?, ?, 1, ?, ?)
                    ON CONFLICT(country_code) DO UPDATE SET
                        country_name=excluded.country_name,
                        updated_at=excluded.updated_at
                """, (code, name, now_utc(), now_utc()))
        except Exception:
            pass

def upsert_asset(country_code: str, asset_name: str, asset_type: str = "unknown", canonical_source_url=None) -> int:
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO assets (country_code, asset_name, asset_type, canonical_source_url, discovered_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(country_code, asset_name) DO UPDATE SET
                asset_type=COALESCE(excluded.asset_type, assets.asset_type),
                canonical_source_url=COALESCE(excluded.canonical_source_url, assets.canonical_source_url),
                updated_at=excluded.updated_at
        """, (country_code, asset_name, asset_type, canonical_source_url, now_utc(), now_utc()))
        cur.execute("SELECT asset_id FROM assets WHERE country_code=? AND asset_name=?", (country_code, asset_name))
        return cur.fetchone()[0]

def upsert_entity(country_code: str, entity_name: str, entity_type: str, official_domain=None, notes=None) -> int:
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO entities (country_code, entity_name, entity_type, official_domain, notes, discovered_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(country_code, entity_name, entity_type) DO UPDATE SET
                official_domain=COALESCE(excluded.official_domain, entities.official_domain),
                notes=COALESCE(excluded.notes, entities.notes),
                updated_at=excluded.updated_at
        """, (country_code, entity_name, entity_type, official_domain, notes, now_utc(), now_utc()))
        cur.execute("SELECT entity_id FROM entities WHERE country_code=? AND entity_name=? AND entity_type=?", (country_code, entity_name, entity_type))
        return cur.fetchone()[0]

def upsert_source(country_code: str, source_url: str, source_type: str, asset_id=None, entity_id=None, priority: int = 2, active: int = 1) -> int:
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO sources (country_code, source_url, source_type, asset_id, entity_id, priority, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_url) DO UPDATE SET
                source_type=excluded.source_type,
                asset_id=COALESCE(excluded.asset_id, sources.asset_id),
                entity_id=COALESCE(excluded.entity_id, sources.entity_id),
                priority=excluded.priority,
                active=excluded.active,
                updated_at=excluded.updated_at
        """, (country_code, source_url, source_type, asset_id, entity_id, priority, active, now_utc(), now_utc()))
        cur.execute("SELECT source_id FROM sources WHERE source_url=?", (source_url,))
        return cur.fetchone()[0]

def touch_source_checked(source_url: str):
    with db_cursor() as (conn, cur):
        cur.execute("UPDATE sources SET last_checked_at=?, updated_at=? WHERE source_url=?", (now_utc(), now_utc(), source_url))

def create_default_job_if_missing():
    with db_cursor() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM crawl_jobs")
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO crawl_jobs (
                    job_name, countries_json, asset_types_json, entity_types_json, mode,
                    enabled, run_interval_minutes, max_tasks_per_run, requests_per_minute,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                "main-registry",
                '["BR","MX","CO"]',
                '["airport","airstrip","military_base"]',
                '["operator","municipality","ministry","mining_company","military_authority"]',
                "broad", 1, 60, 10, 20, now_utc(), now_utc()
            ))

def record_error(task_id=None, country_code=None, seed_name=None, url=None, stage=None, error_text=""):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO crawler_errors (task_id, country_code, seed_name, url, stage, error_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (task_id, country_code, seed_name, url, stage, str(error_text), now_utc()))
