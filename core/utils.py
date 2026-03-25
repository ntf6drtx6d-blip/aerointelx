from datetime import datetime, timezone
import json

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)

def safe_json_loads(value: str):
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []
