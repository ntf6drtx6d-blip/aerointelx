# AeroIntel Phase 1

## What this package does
- Creates a registry database
- Lets you create crawl tasks
- Runs a worker that processes tasks continuously
- Shows worker status, tasks, assets, sources, and errors in Streamlit

## Folder structure
- `app/app.py` — Streamlit control panel
- `core/db.py` — SQLite schema and helpers
- `configs/seeds.py` — country seed URLs
- `registry/registry_builder.py` — simple country crawler
- `crawler/task_generator.py` — creates tasks from jobs
- `crawler/worker.py` — background worker loop
- `run_worker.py` — starts the worker

## First run
1. Install dependencies:
   pip install -r requirements.txt

2. Initialize DB:
   python -c "from core.db import init_db; init_db()"

3. Insert a starter job into SQLite. Example:
   INSERT INTO crawl_jobs (job_name, countries) VALUES ('main', 'Brazil,Mexico,Colombia');

4. Start worker:
   python run_worker.py

5. Start app:
   streamlit run app/app.py

6. In the app, click `Generate Tasks`.
