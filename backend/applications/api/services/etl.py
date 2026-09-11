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


# --- Injury-only refresh -------------------------------------------------
# Injury status comes from the Sleeper API and is the single most
# time-sensitive field in the app: a player ruled out on a Saturday night
# must not still render as "Active" on Sunday morning. The full ETL only
# runs once a day at 06:00, which left a ~24h window where a fresh injury
# was invisible. This runs just step 08 (fetch Sleeper -> write CSV) plus a
# DB sync, so it is cheap enough to schedule frequently.
#
# Cadence is env-tunable via INJURY_REFRESH_MINUTES. Sleeper asks callers to
# treat /v1/players/nfl as a heavy endpoint, so keep this at tens of minutes
# rather than seconds; 30m is the default.
INJURY_SCRIPT_NAME = "08_update_injuries.py"


def _injury_script_path() -> str:
    return os.path.join(os.path.dirname(ETL_SCRIPT_PATH), INJURY_SCRIPT_NAME)


async def run_injury_refresh_async() -> bool:
    """Re-pull injuries from Sleeper and reload them into memory.

    Returns True if the refresh completed and app state was reloaded.
    """
    script = _injury_script_path()
    if not os.path.exists(script):
        logger.error(f"Injury refresh script not found at: {script}")
        return False
    logger.info("Injury refresh: pulling latest statuses from Sleeper...")
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(
                "Injury refresh failed (code %s): %s",
                process.returncode,
                (stderr or b"").decode(errors="replace")[-800:],
            )
            return False

        # Push the refreshed CSV into Postgres, then rebuild the in-memory
        # injury map so live requests see the new statuses without a restart.
        try:
            refresh_db_data()
            refresh_app_state()
        except Exception:
            logger.exception("Injury refresh: reload after fetch failed")
            return False
        # Stamp the time so /health (and the UI) can state how fresh the injury
        # picture actually is instead of implying it is live.
        from datetime import datetime, timezone
        from ..state import model_data as _md
        _md["injuries_updated_at"] = (
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        logger.info("Injury refresh complete; injury map reloaded.")
        return True
    except Exception:
        logger.exception("Injury refresh raised")
        return False


def injury_refresh_wrapper():
    """Sync entry point for APScheduler."""
    try:
        asyncio.run(run_injury_refresh_async())
    except RuntimeError:
        # Already inside an event loop (rare under BackgroundScheduler).
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(run_injury_refresh_async())
        finally:
            loop.close()
