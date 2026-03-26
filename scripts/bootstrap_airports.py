import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.db import init_db
from crawler.registry_builder import ingest_all_airports_global

print("🚀 GLOBAL AIRPORT BOOTSTRAP START", flush=True)

init_db()

count = ingest_all_airports_global()

print(f"🎯 TOTAL INSERTED: {count}", flush=True)
