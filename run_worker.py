from core.db import create_default_job_if_missing, init_db
from crawler.worker import worker_loop

print("🚀 AeroIntel registry worker booting...", flush=True)
init_db()
print("✅ Database initialized", flush=True)
create_default_job_if_missing()
print("🔥 Default job ensured", flush=True)
print("🔁 Starting worker loop...", flush=True)
worker_loop()
