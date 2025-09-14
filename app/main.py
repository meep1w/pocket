from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.db import init_db, Base
from app.http.routers.postback import router as postback_router
from app.http.routers.access import router as access_router
from app.http.routers.redirects import router as redirect_router
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import close_all_sessions
from app.db import engine

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup (если нужно) — yield разделяет блоки
    yield
    # shutdown: закрываем всё
    close_all_sessions()
    try:
        engine.dispose()  # закрыть пул соединений
    except Exception:
        pass

app = FastAPI(title="PocketBot HTTP")
init_db(Base)

app.include_router(postback_router)
app.include_router(access_router)
app.include_router(redirect_router)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/health")
def health():
    return {"ok": True}
