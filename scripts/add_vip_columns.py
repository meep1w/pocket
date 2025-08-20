#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL", "sqlite:////opt/pocketbot/pocketbot.db")
    return create_engine(db_url, future=True)


DDL = [
    # users
    ("users", "is_vip",          "ALTER TABLE users ADD COLUMN is_vip INTEGER NOT NULL DEFAULT 0"),
    ("users", "vip_miniapp_url", "ALTER TABLE users ADD COLUMN vip_miniapp_url TEXT"),
    ("users", "vip_notified",    "ALTER TABLE users ADD COLUMN vip_notified INTEGER NOT NULL DEFAULT 0"),

    # tenant_configs
    ("tenant_configs", "require_subscription",
     "ALTER TABLE tenant_configs ADD COLUMN require_subscription INTEGER NOT NULL DEFAULT 0"),
    ("tenant_configs", "vip_threshold",
     "ALTER TABLE tenant_configs ADD COLUMN vip_threshold INTEGER NOT NULL DEFAULT 500"),
]


def column_exists(engine: Engine, table: str, column: str) -> bool:
    q = text("PRAGMA table_info(%s)" % table)
    with engine.connect() as conn:
        rows = conn.execute(q).all()
    for r in rows:
        # r[1] - name
        if r[1] == column:
            return True
    return False


def main():
    eng = get_engine()
    with eng.begin() as conn:
        # ensure tables exist
        try:
            conn.execute(text("SELECT 1 FROM users LIMIT 1"))
            conn.execute(text("SELECT 1 FROM tenant_configs LIMIT 1"))
        except OperationalError as e:
            print(f"❌ DB not ready: {e}")
            return

    for table, col, ddl in DDL:
        try:
            if not column_exists(eng, table, col):
                with eng.begin() as conn:
                    conn.execute(text(ddl))
                print(f"✅ Added {table}.{col}")
            else:
                print(f"… {table}.{col} already exists")
        except Exception as e:
            print(f"⚠️ Skipped {table}.{col}: {e}")

    print("✅ VIP columns ensured.")


if __name__ == "__main__":
    main()
