# BI Platform

Інтерактивний BI-застосунок з підтримкою A/Б тестування.

## Швидкий старт (Docker)

```bash
docker compose up --build
```

Бекенд доступний на: http://localhost:8000

## Endpoints

| URL | Опис |
|-----|------|
| `GET /api/v1/health/` | Healthcheck — стан сервера та БД |
| `GET /admin/` | Адмін-панель Django |

Перевірка healthcheck:

```bash
curl http://localhost:8000/api/v1/health/
# {"status":"ok","db":"ok"}
```

## Локальний запуск (без Docker)

1. Скопіювати `.env.example` → `.env` і заповнити значення:

```bash
cp backend/.env.example backend/.env
```

2. Застосувати міграції та запустити сервер:

```bash
cd backend
python manage.py migrate
python manage.py runserver
```

## PostgreSQL

| Параметр | Значення |
|----------|----------|
| Host | `localhost` (локально) / `db` (Docker) |
| Port | `5432` |
| Database | `bi` |
| User | `postgres` |
| Password | `password` |
