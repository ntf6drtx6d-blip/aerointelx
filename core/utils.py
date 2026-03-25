print("UTILS_V2_LOADED", flush=True)
import json
from datetime import datetime, timezone


# 🔹 Універсальний UTC timestamp (ISO формат)
def now_utc():
    return datetime.now(timezone.utc).isoformat()


# 🔹 Безпечний JSON loads
def safe_json_loads(value, default=None):
    """
    Безпечний парсер JSON:
    - None / "" → default
    - битий JSON → default
    """

    if default is None:
        default = []

    try:
        if value is None or value == "":
            return default

        # якщо вже список/обʼєкт → просто повернути
        if isinstance(value, (list, dict)):
            return value

        return json.loads(value)

    except Exception:
        return default


# 🔹 Безпечний JSON dumps
def safe_json_dumps(value):
    """
    Гарантує, що значення можна зберегти в базу як JSON string
    """
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "[]"


# 🔹 Нормалізація тексту
def normalize_text(value: str) -> str:
    if not value:
        return ""

    return " ".join(value.strip().split())


# 🔹 Простий safe lower
def safe_lower(value: str) -> str:
    if not value:
        return ""
    return value.lower()


# 🔹 Safe contains (без помилок)
def contains(text: str, keywords: list[str]) -> bool:
    if not text:
        return False

    text_lower = text.lower()
    return any(k.lower() in text_lower for k in keywords)


# 🔹 Безпечне отримання dict ключа
def safe_get(d: dict, key: str, default=None):
    try:
        return d.get(key, default)
    except Exception:
        return default
