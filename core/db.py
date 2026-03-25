import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_DIR = BASE_DIR / 'data'
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / 'aerointel.db'


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA foreign_keys=ON;')
    return conn


def now():
    return datetime.utcnow().isoformat()


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.executescript(
        '''
        CREATE TABLE IF NOT EXISTS assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT,
            asset_name TEXT,
            asset_type TEXT,
            city TEXT,
            source_url TEXT,
            discovered_at TEXT,
            UNIQUE(asset_name, country_code)
        );

        CREATE TABLE IF NOT EXISTS entities (
            entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT,
            entity_name TEXT,
            entity_type TEXT,
            source_url TEXT,
            discovered_at TEXT,
            UNIQUE(entity_name, country_code, entity_type)
        );

        CREATE TABLE IF NOT EXISTS asset_entity_links (
            link_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            entity_id INTEGER,
            role TEXT,
            source_url TEXT,
            discovered_at TEXT,
            UNIQUE(asset_id, entity_id, role)
        );

        CREATE TABLE IF NOT EXISTS sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT,
            source_url TEXT UNIQUE,
            source_type TEXT,
            last_checked_at TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS crawl_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT,
            countries TEXT,
            enabled INTEGER DEFAULT 1,
            run_interval_minutes INTEGER DEFAULT 30,
            max_tasks INTEGER DEFAULT 10,
            last_run TEXT
        );

        CREATE TABLE IF NOT EXISTS crawl_tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            country_code TEXT,
            status TEXT,
            retries INTEGER DEFAULT 0,
            created_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS worker_status (
            worker_id TEXT PRIMARY KEY,
            last_heartbeat TEXT,
            current_task TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS crawler_errors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            error_message TEXT,
            created_at TEXT
        );
        '''
    )

    conn.commit()
    conn.close()
