# app/scripts/add_require_subscription_column.py
import sqlalchemy as sa
from sqlalchemy import text
from app.db import engine

def _is_sqlite(dialect_name: str) -> bool:
    return dialect_name.startswith("sqlite")

def main():
    insp = sa.inspect(engine)
    cols = {c["name"] for c in insp.get_columns("tenant_configs")}
    if "require_subscription" in cols:
        print("✅ tenant_configs.require_subscription уже существует — пропускаю")
        return

    dialect = engine.dialect.name
    with engine.begin() as conn:
        try:
            if _is_sqlite(dialect):
                # В SQLite BOOLEAN = INTEGER(0/1)
                conn.execute(text(
                    "ALTER TABLE tenant_configs "
                    "ADD COLUMN require_subscription BOOLEAN NOT NULL DEFAULT 0"
                ))
            else:
                # для PostgreSQL и прочих
                conn.execute(text(
                    "ALTER TABLE tenant_configs "
                    "ADD COLUMN require_subscription BOOLEAN NOT NULL DEFAULT FALSE"
                ))
            print("✅ Добавил tenant_configs.require_subscription (default: false)")
        except Exception as e:
            print(f"❌ Ошибка при добавлении require_subscription: {e}")

if __name__ == "__main__":
    main()
