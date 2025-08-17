import os, sqlite3

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pocketbot.db"))

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# имя таблицы
tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
tbl = "tenants" if "tenants" in tables else ("tenant" if "tenant" in tables else None)
if not tbl:
    raise SystemExit("Не нашёл таблицу tenants/tenant")

cols = [r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()]
if "miniapp_url" not in cols:
    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN miniapp_url TEXT;")
    conn.commit()
    print("OK: колонка miniapp_url добавлена")
else:
    print("Колонка miniapp_url уже есть — пропускаю")

conn.close()
