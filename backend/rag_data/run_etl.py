#!/usr/bin/env python3
"""ETL runner helper for Football-Ai.

Usage:
  python run_etl.py --check-deps        # quick dependency check
  python run_etl.py --full              # run the full ETL (05_etl_to_postgres.py)
  python run_etl.py --step <script>     # run a single step via run_step.py
  python run_etl.py --container         # attempt to run ETL inside docker-compose (if available)

Notes:
- Prefer running ETL inside the `backend` container (docker-compose up --build db backend) so native bindings (polars, selenium) are satisfied.
- If running locally, ensure `pip install -r requirements.txt` inside `backend/` or in your venv.
"""
import os
import sys
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ETL = ROOT / '05_etl_to_postgres.py'
RUN_STEP = ROOT / 'run_step.py'

REQUIRED = ['polars', 'sqlalchemy']


def check_deps():
    missing = []
    for pkg in REQUIRED:
        try:
            __import__(pkg)
        except Exception:
            missing.append(pkg)
    return missing


def run_inside_container():
    # Prefer docker-compose command if available
    if shutil.which('docker-compose'):
        cmd = ['docker-compose', 'run', '--rm', 'backend', 'python3', f'rag_data/{ETL.name}']
        print('Running ETL inside docker-compose:', ' '.join(cmd))
        return subprocess.call(cmd)
    elif shutil.which('docker'):
        print('docker-compose not found; please use `docker-compose` or run inside your backend image manually.')
        return 2
    else:
        print('No docker/docker-compose CLI available in PATH. Please run ETL inside your backend container or install dependencies locally.')
        return 3


def run_local_full():
    # Run dependency check first
    missing = check_deps()
    if missing:
        print('Missing Python packages:', missing)
        print('Install with: pip install -r backend/requirements.txt')
        return 2
    if not ETL.exists():
        print(f'ETL script not found at {ETL}')
        return 1

    # Reduce parallelism to limit peak memory (especially on smaller VMs)
    os.environ.setdefault('POLARS_MAX_THREADS', '1')
    os.environ.setdefault('OMP_NUM_THREADS', '1')
    os.environ.setdefault('NUMEXPR_NUM_THREADS', '1')
    print('Running ETL with POLARS_MAX_THREADS=1 to reduce memory pressure')

    return subprocess.call([sys.executable, str(ETL)])


def run_step(script_name: str):
    if not RUN_STEP.exists():
        print('run_step.py not found; cannot run individual steps.')
        return 1
    return subprocess.call([sys.executable, str(RUN_STEP), script_name])


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--check-deps', action='store_true')
    p.add_argument('--container', action='store_true')
    p.add_argument('--full', action='store_true')
    p.add_argument('--step', type=str)
    args = p.parse_args()

    if args.check_deps:
        miss = check_deps()
        if miss:
            print('Missing packages:', miss)
            sys.exit(2)
        print('All required packages present.')
        sys.exit(0)

    if args.container:
        sys.exit(run_inside_container())

    if args.step:
        sys.exit(run_step(args.step))

    if args.full:
        sys.exit(run_local_full())

    print('No action specified. See --help for usage.')
    sys.exit(0)
