
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.db import SessionLocal
from app.models import Postback
import argparse


parser = argparse.ArgumentParser(description="Удаление постбэков для повторного прохождения шагов")
parser.add_argument("--tenant", type=int, required=True, help="ID тенанта (клиента)")
parser.add_argument("--click-id", required=True, help="click_id пользователя (обычно его Telegram ID)")
parser.add_argument("--events", choices=["registration", "deposit", "both"], default="both", help="Какие события удалить")
args = parser.parse_args()

db = SessionLocal()
try:
    q = db.query(Postback).filter(
        Postback.tenant_id == args.tenant,
        Postback.click_id == args.click_id,
    )
    if args.events != "both":
        q = q.filter(Postback.event == args.events)

    count = q.delete(synchronize_session=False)
    db.commit()
    print(f"Deleted {count} postbacks for tenant={args.tenant}, click_id={args.click_id}, events={args.events}")
finally:
    db.close()
