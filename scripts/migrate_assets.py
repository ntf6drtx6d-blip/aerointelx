import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.db import execute_sql

print("🚀 START ASSETS MIGRATION", flush=True)

execute_sql("""
ALTER TABLE assets
ADD COLUMN IF NOT EXISTS country_name TEXT,
ADD COLUMN IF NOT EXISTS municipality TEXT,
ADD COLUMN IF NOT EXISTS icao_code TEXT,
ADD COLUMN IF NOT EXISTS scheduled_service TEXT,
ADD COLUMN IF NOT EXISTS home_link TEXT,
ADD COLUMN IF NOT EXISTS wikipedia_link TEXT,
ADD COLUMN IF NOT EXISTS last_updated TEXT;
""")

print("✅ ASSETS MIGRATION DONE", flush=True)
