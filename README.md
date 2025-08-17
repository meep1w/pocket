# PocketBot — Multi-tenant Telegram Service (MVP)

## Состав
- Parent Bot (aiogram v3) — проверка членства в приватном канале, приём API-токена детского бота, меню /ga.
- Children Runner — поднимает/останавливает детские боты по данным из БД, автопауза/автовозобновление по членству владельца в приватном канале.
- HTTP (FastAPI) — /pb (постбэки регистрации/депозита), /miniapp/access (доступ к мини-аппу), /r/... (редиректы на реф-ссылки с прокидыванием click_id=uid).
- SQLite — однофайловая БД. Без Alembic в MVP.

## Быстрый старт (локально)
1. `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\Scripts\activate`)
2. `pip install -r requirements.txt`
3. Создайте `.env` в корне (см. пример ниже).
4. Инициализация БД произойдёт автоматически при первом старте любого компонента.
5. Запуск:
   - Parent Bot: `python -m app.bots.parent.main`
   - Children Runner: `python -m app.bots.child.runner`
   - HTTP API: `uvicorn app.http.main:app --host 0.0.0.0 --port 8000`

## .env пример
```
PROJECT_NAME=PocketBot
SERVICE_HOST=https://YOUR_HOST
MINIAPP_URL=https://meep1w.github.io/cortes-mini-app/
TIMEZONE=Europe/Moscow
PRIVATE_CHANNEL_ID=-1001234567890
GA_ADMIN_IDS=[6677757907,1189134876]
PARENT_BOT_TOKEN=0000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
TENANT_SECRET_MODE=enabled
GLOBAL_POSTBACK_SECRET=REPLACE_ME
BROADCAST_RATE_PER_HOUR=40
```

## systemd
В каталоге `systemd/` есть юниты. Отредактируйте пути к Python и рабочей директории:
- `parent-bot.service`
- `children-runner.service`
- `http-app.service`

## Примечания
- Детские боты **автоматически приостанавливаются**, если владелец (owner_tg_id) больше не является участником приватного канала. При возвращении — автоматически возобновляются.
- Рассылки ограничены 40 сообщений/час на бота (MVP).
