#!/bin/bash
# Sentinet Quick Start Script

# Initialize database manually
echo "🗄️  Initializing TimescaleDB..."
docker exec sentinet-timescaledb bash -c "PGPASSWORD=sentinet_pass psql -U sentinet_user -d sentinet -f /docker-entrypoint-initdb.d/init.sql" 2>&1 | grep -v "ERROR"

echo "✅ Database initialized ready!"
echo ""
echo "Now run: PYTHONPATH=. python cli.py start"
