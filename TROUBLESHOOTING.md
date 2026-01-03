# Quick Start Fix Guide

If you encounter errors when starting Sentinet, follow these steps:

## 1. Python Path Issue

**Error**: `ModuleNotFoundError: No module named 'lib'`

**Fix**: Set PYTHONPATH before running commands:
```bash
PYTHONPATH=. python cli.py start
# Or add to your shell:
export PYTHONPATH=$PYTHONPATH:.
```

## 2. Missing Alpaca Credentials

**Error**: `ALPACA_API_KEY and ALPACA_API_SECRET must be set`

**Fix**: Edit `.env` file and add your Alpaca API credentials:
```bash
ALPACA_API_KEY=your_key_here
ALPACA_API_SECRET=your_secret_here
```

Sign up at https://alpaca.markets/ (free tier available)

## 3. Services Already Running

If you see "Container already running" warnings, that's normal - Docker Compose handles this gracefully.

## Recommended Startup Sequence

```bash
# 1. Ensure you're in the project directory
cd /Users/luckykatneni/Desktop/Sentinet

# 2. Set Python path
export PYTHONPATH=.

# 3. Verify Docker is running
docker ps

# 4. Start all services
python cli.py start
```

## Stopping Services

```bash
# Stop infrastructure only
./cli.py infra down

# Or Ctrl+C to stop the dashboard (which stops all services via CLI)
```

## Health Check

```bash
# Verify services are healthy
python cli.py health

# Check Docker containers
docker ps

# Check logs
docker logs sentinet-redpanda
docker logs sentinet-timescaledb
```
