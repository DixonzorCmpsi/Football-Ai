import subprocess
import sys

def test_run_etl_check_deps():
    # The check-deps command returns 0 if all deps present, 2 if missing
    res = subprocess.run([sys.executable, 'rag_data/run_etl.py', '--check-deps'])
    assert res.returncode in (0, 2)
