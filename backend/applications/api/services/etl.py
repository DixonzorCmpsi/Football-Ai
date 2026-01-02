import asyncio
import sys
import os
from datetime import datetime
from ..config import logger, ETL_SCRIPT_PATH
from .data_loader import refresh_app_state, refresh_db_data

# --- Updated Non-Blocking ETL Function ---
async def run_daily_etl_async():
    """Executes the ETL script without blocking the main FastAPI event loop."""
    logger.info(f"Starting Daily ETL pipeline at {datetime.now()}...")
    if not os.path.exists(ETL_SCRIPT_PATH):
        logger.error(f"ETL script not found at: {ETL_SCRIPT_PATH}")
        return

    try:
        # Launch the process asynchronously
        process = await asyncio.create_subprocess_exec(
            sys.executable, ETL_SCRIPT_PATH,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("ETL process finished successfully")
            refresh_app_state()
            refresh_db_data()
        else:
            logger.error(f"ETL process exited with code {process.returncode}")
            if stderr: logger.error(f"STDERR: {stderr.decode()}")
    except Exception as e:
        logger.exception(f"Error during async ETL: {e}")

def etl_trigger_wrapper():
    """Bridge for APScheduler thread to async ETL."""
    try:
        asyncio.run(run_daily_etl_async())
    except Exception as e:
        logger.exception(f"Scheduler bridge error: {e}")
