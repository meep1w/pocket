import json
import os
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv

# путь к .env в корне проекта
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # /opt/pocketbot
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

def _get_list(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        # поддержим и JSON-массив, и "1,2,3"
        if raw.strip().startswith("["):
            return list(map(int, json.loads(raw)))
        return [int(x) for x in raw.replace(" ", "").split(",") if x]
    except Exception:
        return default

class Settings:
    project_name: str
    service_host: str
    miniapp_url: str
    vip_miniapp_url: Optional[str] = None
    timezone: str
    private_channel_id: int
    ga_admin_ids: List[int]
    parent_bot_token: str
    tenant_secret_mode: str
    global_postback_secret: str
    broadcast_rate_per_hour: int

    def __init__(self) -> None:
        self.project_name = os.getenv("PROJECT_NAME", "PocketBot")
        self.service_host = os.getenv("SERVICE_HOST", "https://localhost")
        self.miniapp_url = os.getenv("MINIAPP_URL", "https://meep1w.github.io/cortes-mini-app/")
        # === ВАЖНО: читаем VIP_MINIAPP_URL из .env ===
        self.vip_miniapp_url = os.getenv("VIP_MINIAPP_URL") or None

        self.timezone = os.getenv("TIMEZONE", "Europe/Moscow")
        self.private_channel_id = int(os.getenv("PRIVATE_CHANNEL_ID", "-1001234567890"))
        self.ga_admin_ids = _get_list("GA_ADMIN_IDS", [6677757907, 1189134876])
        self.parent_bot_token = os.getenv("PARENT_BOT_TOKEN", "")
        self.tenant_secret_mode = os.getenv("TENANT_SECRET_MODE", "enabled")
        self.global_postback_secret = os.getenv("GLOBAL_POSTBACK_SECRET", "REPLACE_ME")
        self.broadcast_rate_per_hour = int(os.getenv("BROADCAST_RATE_PER_HOUR", "40"))

settings = Settings()
