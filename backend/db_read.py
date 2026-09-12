"""Read Postgres into polars without depending on pyarrow being loadable.

`pl.read_database_uri` is fast because it goes through connectorx and Arrow,
which means it needs pyarrow's native DLL. On 2026-09-12, after a reboot,
Windows Application Control started blocking that DLL. Every caller used
`read_database_uri` directly and swallowed the exception, so the API started
"healthy" with every table empty and the ETL would have failed the same way,
all without an error reaching anyone.

`read_db_uri` uses the Arrow path while it works. The first time pyarrow cannot
be imported, it switches, for the rest of the process, to a pure-Python path
(SQLAlchemy + psycopg2), logs that loudly once, and keeps the data live. Every
*other* error passes through unchanged, so a missing table still raises and
existing "relation does not exist" handling keeps working.

This module imports nothing from the app, so the API (`applications/api/db.py`)
and the ETL scripts in `rag_data/`, which run as separate processes, share it.
"""

from __future__ import annotations

import logging
import threading

import polars as pl

_log = logging.getLogger("football-ai")

# Postgres type OIDs -> polars dtypes. Only consulted for columns where every
# returned value is NULL: building a frame from rows cannot infer a type from no
# values and yields pl.Null, where the Arrow path reports the declared type.
# Anything unlisted becomes String. When this was measured, every such column
# (13 across 3 tables) was text.
_OID_DTYPES: dict[int, pl.DataType] = {
    16: pl.Boolean,
    20: pl.Int64, 21: pl.Int64, 23: pl.Int64,
    700: pl.Float64, 701: pl.Float64, 1700: pl.Float64,
    1082: pl.Date,
    1114: pl.Datetime, 1184: pl.Datetime,
}

FIX_HINT = (
    "Fix: allow pyarrow's DLL in Windows Security, or run "
    "`pip install --force-reinstall pyarrow` in backend/.venv."
)

_lock = threading.Lock()
_arrow_unavailable: str | None = None
_engines: dict[str, object] = {}


def is_arrow_import_failure(exc: BaseException) -> bool:
    """True when a read failed because pyarrow could not load.

    Covers a missing module and a blocked or broken DLL. polars re-wraps the
    ImportError with its own message, so this matches on the text as well as the type.
    """
    text = str(exc).lower()
    return isinstance(exc, ImportError) and (
        "pyarrow" in text or "dll load failed" in text or "application control" in text
    )


def _mark_arrow_unavailable(reason: str, where: str) -> None:
    global _arrow_unavailable
    with _lock:
        if _arrow_unavailable is not None:
            return
        _arrow_unavailable = reason
    _log.error(
        "%s: pyarrow cannot be loaded (%s). Database reads are using the SQLAlchemy "
        "path instead: data stays live, reads are slower. %s",
        where, reason, FIX_HINT,
    )


def _engine_for(uri: str):
    engine = _engines.get(uri)
    if engine is None:
        from sqlalchemy import create_engine

        # pool_pre_ping: the DB container restarts independently of this process
        # (Docker Desktop was also down after that reboot), so a pooled
        # connection may be dead when it is reused.
        engine = create_engine(uri, pool_pre_ping=True)
        _engines[uri] = engine
    return engine


def read_without_arrow(query: str, uri: str) -> pl.DataFrame:
    """The pyarrow-free path. Public so it can be tested directly."""
    raw = _engine_for(uri).raw_connection()
    try:
        cursor = raw.cursor()
        try:
            # No parameters are passed, so psycopg2 leaves a '%' in LIKE patterns alone.
            cursor.execute(query)
            description = cursor.description or []
            columns = [d[0] for d in description]
            oids = [d[1] for d in description]
            rows = cursor.fetchall()
        finally:
            cursor.close()
    finally:
        raw.close()

    if not columns:
        return pl.DataFrame()
    if not rows:
        return pl.DataFrame(schema={c: _OID_DTYPES.get(o, pl.String) for c, o in zip(columns, oids)})

    df = pl.DataFrame([tuple(r) for r in rows], schema=columns, orient="row", infer_schema_length=None)
    fixes = [
        pl.col(name).cast(_OID_DTYPES.get(oid, pl.String))
        for name, oid in zip(columns, oids)
        if df.schema[name] == pl.Null
    ]
    return df.with_columns(fixes) if fixes else df


def read_db_uri(query: str, uri: str) -> pl.DataFrame:
    """Drop-in for `pl.read_database_uri(query, uri)` that survives a blocked pyarrow."""
    if _arrow_unavailable is None:
        try:
            return pl.read_database_uri(query, uri)
        except Exception as exc:
            if not is_arrow_import_failure(exc):
                raise
            _mark_arrow_unavailable(str(exc).splitlines()[0][:300], "read_db_uri")
    return read_without_arrow(query, uri)


def probe_arrow(where: str = "startup check") -> bool:
    """Import pyarrow up front so a block is reported immediately, not discovered
    later as an empty table. Returns True when pyarrow loads."""
    try:
        import pyarrow  # noqa: F401
        return True
    except Exception as exc:
        _mark_arrow_unavailable(f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}", where)
        return False


def status() -> dict:
    """Which read path is live."""
    return {
        "db_read_path": "arrow" if _arrow_unavailable is None else "sqlalchemy-fallback",
        "arrow_unavailable_reason": _arrow_unavailable,
    }


def _reset_for_tests() -> None:
    global _arrow_unavailable
    with _lock:
        _arrow_unavailable = None
