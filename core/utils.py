import json
from datetime import datetime, timezone

print("UTILS_V2_LOADED", flush=True)

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def safe_json_loads(value, default=None):
    if default is None:
        default = []
    try:
        if value is None or value == "":
            return default
        if isinstance(value, (list, dict)):
            return value
        return json.loads(value)
    except Exception:
        return default

def safe_json_dumps(value):
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "[]"

def normalize_text(value: str) -> str:
    if not value:
        return ""
    return " ".join(value.strip().split())
