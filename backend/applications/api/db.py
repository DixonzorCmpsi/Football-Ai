"""The one way the API reads Postgres into polars.

A thin binding of `backend/db_read.py` to this app's connection string. That
module holds the logic, and the reason for it: reads survive pyarrow being
blocked or missing, instead of quietly returning empty tables. It is shared with
the ETL scripts, which run as separate processes and cannot import this package.
"""

from __future__ import annotations

import polars as pl

import db_read

from .config import DB_CONNECTION_STRING


def read_db(query: str) -> pl.DataFrame:
    """Run a SQL query against DB_CONNECTION_STRING and return a polars frame."""
    if not DB_CONNECTION_STRING:
        raise RuntimeError("DB_CONNECTION_STRING is not set")
    return db_read.read_db_uri(query, DB_CONNECTION_STRING)


def probe_arrow() -> bool:
    return db_read.probe_arrow("API startup check")


def driver_status() -> dict:
    return db_read.status()
