import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from pathlib import Path

# абсолютный путь к файлу БД рядом с проектом
DB_PATH = (Path(__file__).resolve().parent.parent / "pocketbot.db")
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,
    future=True,
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

def init_db(BaseModel):
    BaseModel.metadata.create_all(bind=engine)

def close_db() -> None:
    """
    Синхронное закрытие пулов/коннектов.
    Можно звать из асинхронного кода через to_thread.
    """
    # dispose() закрывает все соединения пула и освобождает ресурсы драйвера
    engine.dispose()

async def close_db_async() -> None:
    """
    Асинхронная обёртка для безопасного вызова dispose() в треде,
    чтобы не блокировать event loop.
    """
    await asyncio.to_thread(close_db)