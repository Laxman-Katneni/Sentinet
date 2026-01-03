#!/usr/bin/env python3
"""
Sentinet CLI - Command-line interface for orchestrating all services
"""

import argparse
import asyncio
import os
import subprocess
import sys
import signal
import time
from typing import Optional, List

# Process tracking
running_processes: List[subprocess.Popen] = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n🛑 Shutting down Sentinet...")
    cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def cleanup():
    """Kill all running processes."""
    for proc in running_processes:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except:
            proc.kill()


def run_command(cmd: List[str], cwd: Optional[str] = None, background: bool = False):
    """
    Run a command.
    
    Args:
        cmd: Command as list of strings
        cwd: Working directory
        background: If True, run in background
    """
    print(f"🚀 Running: {' '.join(cmd)}")
    
    if background:
        proc = subprocess.Popen(cmd, cwd=cwd)
        running_processes.append(proc)
        return proc
    else:
        result = subprocess.run(cmd, cwd=cwd)
        return result.returncode


def infra_up():
    """Start infrastructure (Docker Compose)."""
    print("🏗️  Starting Sentinet Infrastructure...")
    print("   - Redpanda (Kafka)")
    print("   - TimescaleDB (PostgreSQL)")
    
    returncode = run_command(['docker-compose', 'up', '-d'])
    
    if returncode == 0:
        print("✅ Infrastructure started successfully")
        print("🌐 Redpanda Admin UI: http://localhost:8080")
        print("🗄️  TimescaleDB: localhost:5432")
        print("\n⏳ Waiting for services to be ready...")
        time.sleep(10)
        print("✅ Services ready")
    else:
        print("❌ Failed to start infrastructure")
        sys.exit(1)


def infra_down():
    """Stop infrastructure."""
    print("🛑 Stopping Sentinet Infrastructure...")
    run_command(['docker-compose', 'down'])
    print("✅ Infrastructure stopped")


def infra_logs():
    """Show infrastructure logs."""
    run_command(['docker-compose', 'logs', '-f'])


def ingest_start(mode: str = 'live'):
    """
    Start ingestion service.
    
    Args:
        mode: 'live' or 'replay'
    """
    if mode == 'live':
        print("📡 Starting Live Ingestion Service...")
        run_command([sys.executable, 'services/ingestion/ingestion_service.py'], background=True)
        print("✅ Ingestion service started")
    
    elif mode == 'replay':
        print("🔁 Starting Replay Mode...")
        run_command([sys.executable, 'services/ingestion/replay_mode.py', 'replay'])
    
    else:
        print(f"❌ Unknown mode: {mode}")
        sys.exit(1)


def analytics_start():
    """Start analytics engine."""
    print("🧮 Starting Analytics Engine...")
    run_command([sys.executable, 'services/analytics/analytics_engine.py'], background=True)
    print("✅ Analytics engine started")


def dashboard_start():
    """Start Streamlit dashboard."""
    print("📊 Starting Sentinet Dashboard...")
    print("🌐 Opening in browser: http://localhost:8501")
    run_command(['streamlit', 'run', 'dashboard/app.py'])


def benchmark():
    """Run benchmark (record + replay)."""
    print("📊 Running Sentinet Benchmark")
    print("=" * 80)
    print("Step 1: Recording live data for 60 seconds...")
    run_command([sys.executable, 'services/ingestion/replay_mode.py', 'record', '60'])
    
    print("\nStep 2: Replaying at 100x speed...")
    run_command([sys.executable, 'services/ingestion/replay_mode.py', 'replay'])
    
    print("=" * 80)
    print("✅ Benchmark complete")


def health_check():
    """Check health of all services."""
    print("🏥 Sentinet Health Check")
    print("=" * 80)
    
    # Check Docker
    print("Checking Docker services...")
    result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True)
    print(result.stdout)
    
    # Check Redpanda
    print("\nChecking Redpanda...")
    result = subprocess.run(
        ['docker', 'exec', 'sentinet-redpanda', 'rpk', 'cluster', 'health'],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("✅ Redpanda: Healthy")
    else:
        print("❌ Redpanda: Unavailable")
    
    # Check TimescaleDB
    print("\nChecking TimescaleDB...")
    result = subprocess.run(
        ['docker', 'exec', 'sentinet-timescaledb', 'pg_isready', '-U', 'sentinet_user'],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("✅ TimescaleDB: Healthy")
    else:
        print("❌ TimescaleDB: Unavailable")
    
    print("=" * 80)


def start_all():
    """Start all services."""
    print("🚀 Starting All Sentinet Services...")
    
    # Start infrastructure
    infra_up()
    
    # Wait for infrastructure
    print("\n⏳ Waiting 5 seconds for infrastructure to stabilize...")
    time.sleep(5)
    
    # Start ingestion
    ingest_start(mode='live')
    
    # Wait a bit
    time.sleep(2)
    
    # Start analytics
    analytics_start()
    
    # Wait a bit
    time.sleep(2)
    
    # Start dashboard (blocking)
    print("\n📊 Launching Dashboard (Ctrl+C to stop all services)...")
    dashboard_start()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sentinet - Market Microstructure Surveillance Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start all services
  ./cli.py start

  # Start infrastructure only
  ./cli.py infra up
  
  # Start individual services
  ./cli.py ingest start
  ./cli.py analytics start
  ./cli.py dashboard
  
  # Run benchmark
  ./cli.py benchmark
  
  # Health check
  ./cli.py health
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Infrastructure commands
    infra_parser = subparsers.add_parser('infra', help='Manage infrastructure')
    infra_subparsers = infra_parser.add_subparsers(dest='subcommand')
    infra_subparsers.add_parser('up', help='Start infrastructure')
    infra_subparsers.add_parser('down', help='Stop infrastructure')
    infra_subparsers.add_parser('logs', help='Show logs')
    
    # Ingestion commands
    ingest_parser = subparsers.add_parser('ingest', help='Manage ingestion service')
    ingest_subparsers = ingest_parser.add_subparsers(dest='subcommand')
    start_ingest = ingest_subparsers.add_parser('start', help='Start ingestion')
    start_ingest.add_argument('--mode', choices=['live', 'replay'], default='live', help='Ingestion mode')
    
    # Analytics commands
    analytics_parser = subparsers.add_parser('analytics', help='Manage analytics engine')
    analytics_subparsers = analytics_parser.add_subparsers(dest='subcommand')
    analytics_subparsers.add_parser('start', help='Start analytics engine')
    
    # Dashboard
    subparsers.add_parser('dashboard', help='Start Streamlit dashboard')
    
    # Utilities
    subparsers.add_parser('benchmark', help='Run performance benchmark')
    subparsers.add_parser('health', help='Health check all services')
    subparsers.add_parser('start', help='Start all services')
    
    args = parser.parse_args()
    
    if args.command == 'infra':
        if args.subcommand == 'up':
            infra_up()
        elif args.subcommand == 'down':
            infra_down()
        elif args.subcommand == 'logs':
            infra_logs()
    
    elif args.command == 'ingest':
        if args.subcommand == 'start':
            ingest_start(mode=args.mode)
            # Keep running
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                cleanup()
    
    elif args.command == 'analytics':
        if args.subcommand == 'start':
            analytics_start()
            # Keep running
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                cleanup()
    
    elif args.command == 'dashboard':
        dashboard_start()
    
    elif args.command == 'benchmark':
        benchmark()
    
    elif args.command == 'health':
        health_check()
    
    elif args.command == 'start':
        start_all()
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
