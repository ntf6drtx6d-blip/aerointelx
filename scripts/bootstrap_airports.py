from core.db import init_db
from crawler.registry_builder import ingest_all_airports_global

print("🚀 GLOBAL AIRPORT BOOTSTRAP START")

init_db()

count = ingest_all_airports_global()

print(f"🎯 TOTAL INSERTED: {count}")
