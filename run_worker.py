from core.db import init_db
from crawler.worker import worker_loop

init_db()
worker_loop()
