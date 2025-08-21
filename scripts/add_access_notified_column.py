from sqlalchemy import create_engine, text
from app.settings import settings
from app.db import SQLALCHEMY_DATABASE_URL  # если у тебя есть, иначе сформируй строку как в проекте

engine = create_engine(SQLALCHEMY_DATABASE_URL)

with engine.begin() as conn:
    try:
        conn.execute(text("ALTER TABLE users ADD COLUMN access_notified BOOLEAN DEFAULT 0"))
    except Exception as e:
        print("Skip or already exists:", e)
