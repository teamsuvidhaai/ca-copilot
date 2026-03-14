#!/bin/sh
set -e

echo "🚀 Starting CA-Copilot Backend..."

# 1. Run migrations (non-fatal — env.py handles connection errors gracefully)
echo "⚙️  Running database migrations..."
cd /app/apps/api
alembic upgrade head || echo "⚠️  Alembic exited with non-zero — continuing startup"

# 2. Seed basic data (Kits)
echo "🌱 Seeding initial kits..."
python scripts/seed_data.py || echo "⚠️  Seeding skipped — data likely already exists"

# 3. Start Background Worker (Free Tier Optimization)
echo "👷 Starting Background Worker..."
python /app/apps/api/app/worker/main.py &

# 4. Start application
echo "📡 Launching Uvicorn..."
if [ "$APP_RELOAD" = "true" ]; then
    exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --reload --proxy-headers --forwarded-allow-ips='*'
else
    exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips='*'
fi
