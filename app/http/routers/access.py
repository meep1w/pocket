from fastapi import APIRouter, HTTPException, Query
from app.db import SessionLocal
from app.models import User, UserStep

router = APIRouter()

@router.get("/miniapp/access")
def miniapp_access(tenant_id: int = Query(...), tg_user_id: int = Query(...)):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.tenant_id == tenant_id, User.tg_user_id == tg_user_id).first()
        if not user or user.step != UserStep.deposited:
            raise HTTPException(status_code=403, detail="forbidden")
        return {"ok": True}
    finally:
        db.close()
