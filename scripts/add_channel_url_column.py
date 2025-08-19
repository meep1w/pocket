# app/scripts/add_channel_url_column.py
import sqlalchemy as sa
from sqlalchemy import text
from app.db import engine

def main():
    insp = sa.inspect(engine)
    cols = {c["name"] for c in insp.get_columns("tenants")}
    if "channel_url" in cols:
        print("✅ tenants.channel_url уже существует — пропускаю")
        return

    # универсально: для SQLite это будет TEXT, для PG — VARCHAR
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE tenants ADD COLUMN channel_url VARCHAR"))
            print("✅ Добавил tenants.channel_url")
        except Exception as e:
            print(f"❌ Ошибка при добавлении channel_url: {e}")

if __name__ == "__main__":
    main()
