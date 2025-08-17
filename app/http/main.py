from fastapi import FastAPI
from app.db import init_db, Base
from .routers.postback import router as postback_router
from .routers.access import router as access_router
from .routers.redirects import router as redirect_router
from fastapi.staticfiles import StaticFiles


app = FastAPI(title="PocketBot HTTP")
init_db(Base)

app.include_router(postback_router)
app.include_router(access_router)
app.include_router(redirect_router)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/health")
def health():
    return {"ok": True}
