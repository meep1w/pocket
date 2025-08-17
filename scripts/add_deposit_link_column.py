import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "pocketbot.db")
DB_PATH = os.path.abspath(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Узнаём реальное имя таблицы
tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
if "tenants" in tables:
    tbl = "tenants"
elif "tenant" in tables:
    tbl = "tenant"
else:
    raise SystemExit("Не нашёл таблицу tenants/tenant")

# Есть ли уже колонка?
cols = [r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()]
if "deposit_link" not in cols:
    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN deposit_link TEXT;")
    conn.commit()
    print("OK: колонка deposit_link добавлена")
else:
    print("Колонка deposit_link уже есть — пропускаю")

conn.close()
