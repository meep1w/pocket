from sqlalchemy import Column, Integer, String, DateTime, Enum, Boolean, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from .db import Base
import enum


class TenantStatus(str, enum.Enum):
    active = "active"
    paused = "paused"
    deleted = "deleted"


class UserStep(str, enum.Enum):
    new = "new"
    asked_reg = "asked_reg"
    registered = "registered"
    asked_deposit = "asked_deposit"
    deposited = "deposited"


class Tenant(Base):
    __tablename__ = "tenants"
    id = Column(Integer, primary_key=True)
    owner_tg_id = Column(Integer, index=True, nullable=False)
    child_bot_token = Column(String, nullable=False)
    child_bot_username = Column(String, nullable=False)
    status = Column(Enum(TenantStatus), default=TenantStatus.active, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    lang_default = Column(String, default="ru")
    support_url = Column(String, default="https://t.me/your_support")
    # NEW: ссылка на канал для проверки подписки
    channel_url = Column(String, nullable=True)
    ref_link = Column(String, default="https://pocketoption.com/ru/registration/")
    postback_secret = Column(String, default="")  # if per-tenant secrets are enabled
    ga_notes = Column(Text, default="")
    deposit_link = Column(String, nullable=True)
    miniapp_url = Column(String, nullable=True)

    texts = relationship("TenantText", back_populates="tenant", cascade="all, delete-orphan")
    users = relationship("User", back_populates="tenant", cascade="all, delete-orphan")


class TenantText(Base):
    __tablename__ = "tenant_texts"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False)
    locale = Column(String, nullable=False)  # ru|en
    # расширили список: main|guide|subscribe|step1|step2|unlocked
    key = Column(String, nullable=False)
    text = Column(Text, default="")
    image_file_id = Column(String, default=None)

    tenant = relationship("Tenant", back_populates="texts")
    __table_args__ = (UniqueConstraint("tenant_id", "locale", "key", name="uix_tenant_locale_key"),)


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=False, index=True)
    tg_user_id = Column(Integer, index=True, nullable=True)
    lang = Column(String, default=None)  # ru|en
    step = Column(Enum(UserStep), default=UserStep.new, nullable=False)
    click_id = Column(String, default=None)
    trader_id = Column(String, default=None)
    last_message_id = Column(Integer, default=None)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="users")


class Postback(Base):
    __tablename__ = "postbacks"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(Integer, index=True, nullable=False)
    event = Column(String, nullable=False)  # registration|deposit
    click_id = Column(String, default=None)
    trader_id = Column(String, default=None)
    sum = Column(Integer, default=0)
    token_ok = Column(Boolean, default=False)
    idempotency_key = Column(String, default=None)
    raw_query = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class Broadcast(Base):
    __tablename__ = "broadcasts"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(Integer, index=True, nullable=False)
    segment = Column(String, nullable=False)  # all|registered|deposited
    text = Column(Text, default="")
    media_file_id = Column(String, default=None)
    status = Column(String, default="queued")  # queued|running|done|paused
    created_at = Column(DateTime, default=datetime.utcnow)


class BroadcastJob(Base):
    __tablename__ = "broadcast_jobs"
    id = Column(Integer, primary_key=True)
    broadcast_id = Column(Integer, ForeignKey("broadcasts.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    state = Column(String, default="queued")  # queued|sent|failed
    error = Column(Text, default=None)
    sent_at = Column(DateTime, default=None)


class StatsDaily(Base):
    __tablename__ = "stats_daily"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(Integer, index=True, nullable=False)
    date = Column(String, nullable=False)  # YYYY-MM-DD
    users_total = Column(Integer, default=0)
    registered_total = Column(Integer, default=0)
    deposited_total = Column(Integer, default=0)


class TenantConfig(Base):
    __tablename__ = "tenant_configs"

    tenant_id = Column(Integer, ForeignKey("tenants.id"), primary_key=True)
    require_deposit = Column(Boolean, nullable=False, default=True)
    min_deposit = Column(Integer, nullable=False, default=50)
    # NEW: включить/выключить проверку подписки
    require_subscription = Column(Boolean, nullable=False, default=False)

    tenant = relationship("Tenant", backref="cfg", uselist=False)
