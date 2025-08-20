# /opt/pocketbot/scripts/add_vip_columns.py
from sqlalchemy import create_engine, text
import os

DB_URL = os.getenv("DATABASE_URL", "sqlite:////opt/pocketbot/pocketbot.db")
engine = create_engine(DB_URL)

def column_exists(table: str, column: str) -> bool:
    with engine.connect() as conn:
        res = conn.execute(text(f"PRAGMA table_info({table});")).mappings().all()
        return any(row["name"] == column for row in res)

def add_column(table: str, ddl: str):
    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl};"))

def main():
    changed = False
    # users.is_vip INTEGER (0/1)
    if not column_exists("users", "is_vip"):
        add_column("users", "is_vip INTEGER NOT NULL DEFAULT 0")
        changed = True

    # users.vip_miniapp_url TEXT (nullable)
    if not column_exists("users", "vip_miniapp_url"):
        add_column("users", "vip_miniapp_url TEXT")
        changed = True

    # users.vip_notified INTEGER (0/1) — присылали ли уведомление о VIP
    if not column_exists("users", "vip_notified"):
        add_column("users", "vip_notified INTEGER NOT NULL DEFAULT 0")
        changed = True

    if changed:
        print("✅ VIP columns added/ensured on 'users'")
    else:
        print("ℹ️ VIP columns already present, nothing to do")

if __name__ == "__main__":
    main()
