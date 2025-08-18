from fastapi import APIRouter, HTTPException, Query
from starlette.responses import RedirectResponse
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from app.db import SessionLocal
from app.models import Tenant

router = APIRouter(prefix="/r", tags=["redirects"])


def _get_tenant(db, tenant_id: int) -> Tenant:
    t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="tenant not found")
    return t


def _with_param(url: str, key: str, value: str) -> str:
    """
    Добавляем/заменяем GET-параметр в URL.
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[key] = str(value)
    return urlunparse(u._replace(query=urlencode(q)))


@router.get("/reg")
def redirect_reg(tenant_id: int = Query(...), uid: str = Query(...)):
    """
    Редирект на реферальную ссылку регистрации клиента с подстановкой click_id.
    """
    db = SessionLocal()
    try:
        t = _get_tenant(db, tenant_id)
        if not t.ref_link:
            raise HTTPException(status_code=400, detail="ref_link is not configured")

        # добавляем click_id=uid
        url = _with_param(t.ref_link, "click_id", uid)
        return RedirectResponse(url=url)
    finally:
        db.close()


@router.get("/dep")
def redirect_dep(tenant_id: int = Query(...), uid: str = Query(...)):
    """
    Редирект на ссылку пополнения с подстановкой click_id.
    """
    db = SessionLocal()
    try:
        t = _get_tenant(db, tenant_id)
        target = t.deposit_link or t.ref_link
        if not target:
            raise HTTPException(status_code=400, detail="deposit/ref link is not configured")

        url = _with_param(target, "click_id", uid)
        return RedirectResponse(url=url)
    finally:
        db.close()
