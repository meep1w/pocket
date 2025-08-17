from fastapi import APIRouter, HTTPException, Query
from starlette.responses import RedirectResponse
from app.db import SessionLocal
from app.models import Tenant

router = APIRouter(prefix="/r", tags=["redirects"])

def _get_tenant(db, tenant_id: int) -> Tenant:
    t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="tenant not found")
    return t

@router.get("/reg")
def redirect_reg(tenant_id: int = Query(...), uid: int = Query(...)):
    """
    Редирект на реферальную ссылку регистрации клиента.
    """
    db = SessionLocal()
    try:
        t = _get_tenant(db, tenant_id)
        if not t.ref_link:
            raise HTTPException(status_code=400, detail="ref_link is not configured")
        # просто перенаправляем (click_id мы используем в постбэках)
        return RedirectResponse(url=t.ref_link)
    finally:
        db.close()

@router.get("/dep")
def redirect_dep(tenant_id: int = Query(...), uid: int = Query(...)):
    """
    Редирект на ссылку пополнения. Если deposit_link не задан — используем ref_link.
    """
    db = SessionLocal()
    try:
        t = _get_tenant(db, tenant_id)
        target = t.deposit_link or t.ref_link
        if not target:
            raise HTTPException(status_code=400, detail="deposit/ref link is not configured")
        return RedirectResponse(url=target)
    finally:
        db.close()
