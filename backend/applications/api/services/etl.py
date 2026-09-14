import asyncio
import sys
import os
import signal
from datetime import datetime
from ..config import logger, ETL_SCRIPT_PATH
from .data_loader import refresh_app_state, refresh_db_data, load_depth_charts, load_historical_stats

def python_for_scripts() -> str:
    """The interpreter that has this app's packages, for running pipeline scripts.

    On Windows a venv's python.exe is a launcher that starts the base install, so
    inside the server `sys.executable` is the base interpreter. Spawning that ran
    every ETL step without the venv ("No module named 'nflreadpy'"): the
    half-hourly injury refresh and the daily ETL failed on every run. The venv's
    own launcher, found from sys.prefix, restores the environment.
    """
    for candidate in (
        os.path.join(sys.prefix, "Scripts", "python.exe"),
        os.path.join(sys.prefix, "bin", "python"),
    ):
        if sys.prefix != sys.base_prefix and os.path.exists(candidate):
            return candidate
    return sys.executable


_running_scripts: set = set()


async def run_script(script: str) -> tuple[int, bytes, bytes]:
    """Run a pipeline script and wait for it, without blocking the event loop.

    Not asyncio.create_subprocess_exec: on Windows, uvicorn's reload mode runs a
    SelectorEventLoop, which cannot start subprocesses (NotImplementedError), so
    the refresh "failed" in the server while the same code worked from a shell.

    Not asyncio.to_thread either: the default executor is joined when the loop
    shuts down, so a reload or Ctrl+C waited for the whole ETL, a browser-driven
    Bovada scrape included, and the server answered nothing for minutes. The wait
    happens on a daemon thread, and terminate_running_scripts() ends the children
    when the app stops.
    """
    import subprocess
    import threading

    loop = asyncio.get_running_loop()
    done: asyncio.Future = loop.create_future()
    process = subprocess.Popen([python_for_scripts(), script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _running_scripts.add(process)

    def _wait():
        try:
            out, err = process.communicate()
            result = (process.returncode, out or b"", err or b"")
        except Exception as exc:  # pragma: no cover - defensive
            result = (-1, b"", str(exc).encode())
        finally:
            _running_scripts.discard(process)
        if not loop.is_closed():
            loop.call_soon_threadsafe(lambda: done.done() or done.set_result(result))

    threading.Thread(target=_wait, name=f"script:{os.path.basename(script)}", daemon=True).start()
    return await done


def terminate_running_scripts() -> int:
    """End pipeline scripts this process started, so a stopping server doesn't leave them running."""
    stopped = 0
    for process in list(_running_scripts):
        if process.poll() is None:
            try:
                if sys.platform == "win32":
                    # The venv launcher starts a second python; end the whole tree.
                    import subprocess
                    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
                else:
                    process.terminate()
                stopped += 1
            except Exception:
                pass
    return stopped


# A successful full ETL is stamped here, so a restart (or every dev auto-reload)
# doesn't rerun a multi-minute scrape of data that is hours old at most.
ETL_STAMP = os.path.join(os.path.dirname(ETL_SCRIPT_PATH), ".etl_last_success")


def etl_ran_recently(hours: float | None = None) -> bool:
    hours = float(os.getenv("STARTUP_ETL_MIN_AGE_HOURS", "20")) if hours is None else hours
    try:
        return (datetime.now().timestamp() - os.path.getmtime(ETL_STAMP)) < hours * 3600
    except OSError:
        return False


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
        returncode, stdout, stderr = await run_script(ETL_SCRIPT_PATH)

        if returncode == 0:
            logger.info("ETL process finished successfully")
            try:
                with open(ETL_STAMP, "w") as stamp:
                    stamp.write(datetime.now().isoformat())
            except OSError:
                pass

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
            logger.error(f"ETL process exited with code {returncode}")
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


def sync_injuries_to_db(csv_path: str | None = None, uri: str | None = None) -> int:
    """Replace the database's injury rows for the weeks in the refreshed CSV.

    The refresh used to stop at the CSV and then "reload" from the database,
    which only the daily ETL wrote. So the half-hourly refresh never reached the
    app, and repeated loads had left up to 15 snapshots per player with
    conflicting statuses (54 players were both Active and Questionable), one of
    them picked arbitrarily per request.

    Delete-then-insert in one transaction: readers see the old week or the new
    one, never a mix. Returns rows written; 0 without a database.
    """
    import polars as pl
    import psycopg2
    from psycopg2.extras import execute_values

    from ..config import CURRENT_SEASON, DB_CONNECTION_STRING, RAG_DIR

    uri = uri or DB_CONNECTION_STRING
    csv_path = csv_path or os.path.join(RAG_DIR, f"weekly_injuries_{CURRENT_SEASON}.csv")
    if not uri or not os.path.exists(csv_path):
        return 0
    df = (
        pl.read_csv(csv_path, infer_schema_length=0)
        .select(["player_id", "player_name", "injury_status", "week"])
        .with_columns(pl.col("week").cast(pl.Int64))
        .unique(subset=["player_id", "week"], keep="last", maintain_order=True)
    )
    if df.is_empty():
        return 0
    table = f"weekly_injuries_{CURRENT_SEASON}"
    weeks = sorted(set(df["week"].to_list()))
    conn = psycopg2.connect(uri)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table} "
                "(player_id TEXT, player_name TEXT, injury_status TEXT, week BIGINT)"
            )
            cur.execute(f"DELETE FROM {table} WHERE week = ANY(%s)", (weeks,))
            execute_values(
                cur,
                f"INSERT INTO {table} (player_id, player_name, injury_status, week) VALUES %s",
                df.rows(),
                page_size=2000,
            )
    finally:
        conn.close()
    logger.info("Injury refresh: wrote %d rows for weeks %s to %s", df.height, weeks, table)
    return df.height


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
        returncode, stdout, stderr = await run_script(script)
        if returncode != 0:
            logger.error(
                "Injury refresh failed (code %s): %s",
                returncode,
                (stderr or b"").decode(errors="replace")[-800:],
            )
            return False

        # Push the refreshed CSV into Postgres, then rebuild the in-memory
        # injury map so live requests see the new statuses without a restart.
        try:
            await asyncio.to_thread(sync_injuries_to_db)
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
