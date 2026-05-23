import asyncio
import sys
import os
import signal
from datetime import datetime
from ..config import logger, ETL_SCRIPT_PATH
from .data_loader import refresh_app_state, refresh_db_data, load_depth_charts, load_historical_stats

def trigger_container_restart():
    """
    Triggers a graceful container restart by sending SIGTERM to the main process.
    Docker will automatically restart the container if restart policy is set.
    This ensures fresh data is loaded into memory after ETL completes.
    """
    logger.info("Triggering container restart to reload fresh data...")
    # Give a brief delay to allow logs to flush
    os.kill(os.getpid(), signal.SIGTERM)

# --- Updated Non-Blocking ETL Function ---
async def run_daily_etl_async(restart_after: bool = True):
    """
    Executes the ETL script without blocking the main FastAPI event loop.
    
    Args:
        restart_after: If True, triggers a container restart after successful ETL
                       to ensure all data is reloaded fresh. Defaults to True.
    """
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
            
            if restart_after and os.getenv('RESTART_AFTER_ETL', 'true').lower() in ('1', 'true', 'yes'):
                # Trigger container restart for clean data reload
                # Short delay to ensure logs are written
                await asyncio.sleep(2)
                trigger_container_restart()
            else:
                # Just refresh in-memory data without restart
                refresh_db_data()
                refresh_app_state()
                load_historical_stats()
                load_depth_charts(force=True)
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
