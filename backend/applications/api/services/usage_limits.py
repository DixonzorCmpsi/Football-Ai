"""Daily limits for the free (house) assistant.

Three counters, all reset at midnight US Eastern (the NFL's clock, and where the
audience is):

* per browser: `AGENT_DAILY_QUESTIONS` questions (default 25)
* per IP: `AGENT_DAILY_QUESTIONS_PER_IP` (default 75). A browser id lives in
  localStorage and is reset by clearing site data; the IP cap stops that from
  being unlimited, while leaving room for a household or office behind one address.
* house upstream: `INFERENCE_HOUSE_DAILY_REQUEST_BUDGET` model calls (default 45).
  OpenRouter limits free models to 50 requests/day account-wide unless the account
  has bought at least $10 of credits (then 1000/day). One question is usually 2-4
  model calls, so without this guard one busy user exhausts the account and every
  other user gets a raw upstream 429. Raise it after buying credits.

The owner (a request carrying `AGENT_OWNER_TOKEN`) skips the per-browser and
per-IP limits, but not the upstream budget, which is a hard wall at the provider
anyway. Bring-your-own-key requests use none of these.

Counters live in Postgres (`agent_usage`) so every Cloud Run instance sees the
same numbers. Each increment is one conditional UPSERT, so two instances racing
for the last question cannot both win. Without a database, an in-process counter
is used and a warning logged: fine locally, wrong for more than one instance.
"""

from __future__ import annotations

import os
import threading
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from ..config import DB_CONNECTION_STRING, logger

RESET_TZ = ZoneInfo(os.getenv("AGENT_USAGE_TIMEZONE", "America/New_York"))


def _limit(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def questions_per_client() -> int:
    return _limit("AGENT_DAILY_QUESTIONS", 25)


def questions_per_ip() -> int:
    return _limit("AGENT_DAILY_QUESTIONS_PER_IP", 75)


def house_daily_budget() -> int:
    return _limit("INFERENCE_HOUSE_DAILY_REQUEST_BUDGET", 45)


def today() -> date:
    return datetime.now(RESET_TZ).date()


def resets_at() -> str:
    tomorrow = today() + timedelta(days=1)
    return datetime.combine(tomorrow, time.min, tzinfo=RESET_TZ).isoformat()


@dataclass
class Quota:
    allowed: bool
    used: int
    limit: int
    remaining: int
    resets_at: str
    owner: bool = False
    # "client" | "ip" | "house_budget" when not allowed.
    blocked_by: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


class _MemoryCounters:
    def __init__(self) -> None:
        self._counts: dict[tuple[date, str], int] = {}
        self._lock = threading.Lock()

    def take(self, subject: str, limit: int, day: date) -> tuple[bool, int]:
        with self._lock:
            # Drop earlier days so a long-lived process doesn't grow without bound.
            for key in [k for k in self._counts if k[0] < day]:
                del self._counts[key]
            used = self._counts.get((day, subject), 0)
            if used >= limit:
                return False, used
            self._counts[(day, subject)] = used + 1
            return True, used + 1

    def give_back(self, subject: str, day: date) -> None:
        with self._lock:
            key = (day, subject)
            if self._counts.get(key, 0) > 0:
                self._counts[key] -= 1

    def peek(self, subject: str, day: date) -> int:
        with self._lock:
            return self._counts.get((day, subject), 0)


class _PostgresCounters:
    CREATE = """
        CREATE TABLE IF NOT EXISTS agent_usage (
            day      date    NOT NULL,
            subject  text    NOT NULL,
            count    integer NOT NULL DEFAULT 0,
            PRIMARY KEY (day, subject)
        )
    """
    # Increments only while under the limit; returns no row when the limit is hit.
    TAKE = """
        INSERT INTO agent_usage (day, subject, count) VALUES (:day, :subject, 1)
        ON CONFLICT (day, subject) DO UPDATE SET count = agent_usage.count + 1
        WHERE agent_usage.count < :limit
        RETURNING count
    """

    def __init__(self, uri: str) -> None:
        from sqlalchemy import create_engine

        self._engine = create_engine(uri, pool_pre_ping=True)
        self._ready = False
        self._ready_lock = threading.Lock()

    def _ensure(self, conn=None) -> None:
        """Create the table once, in its own transaction.

        Concurrent `CREATE TABLE IF NOT EXISTS` is not safe in Postgres: two
        sessions can both pass the existence check, and the loser fails on
        pg_type's unique index. That failure used to drop the request into the
        in-memory fallback, which admitted a question over the limit (12 racing
        requests against a limit of 3 got 4 through). A process lock covers
        threads here; the retry below covers another instance winning the race.
        """
        if self._ready:
            return
        from sqlalchemy import exc as sa_exc
        from sqlalchemy import text

        with self._ready_lock:
            if self._ready:
                return
            try:
                with self._engine.begin() as ddl:
                    ddl.execute(text(self.CREATE))
            except (sa_exc.IntegrityError, sa_exc.ProgrammingError):
                # Another instance created it between the check and the create.
                with self._engine.begin() as check:
                    check.execute(text("SELECT 1 FROM agent_usage LIMIT 0"))
            self._ready = True

    def take(self, subject: str, limit: int, day: date) -> tuple[bool, int]:
        from sqlalchemy import text

        if limit <= 0:
            return False, self.peek(subject, day)
        self._ensure()
        with self._engine.begin() as conn:
            row = conn.execute(text(self.TAKE), {"day": day, "subject": subject, "limit": limit}).first()
        if row is None:
            return False, limit
        return True, int(row[0])

    def give_back(self, subject: str, day: date) -> None:
        from sqlalchemy import text

        self._ensure()
        with self._engine.begin() as conn:
            conn.execute(
                text("UPDATE agent_usage SET count = count - 1 WHERE day = :day AND subject = :subject AND count > 0"),
                {"day": day, "subject": subject},
            )

    def peek(self, subject: str, day: date) -> int:
        from sqlalchemy import text

        self._ensure()
        with self._engine.begin() as conn:
            row = conn.execute(
                text("SELECT count FROM agent_usage WHERE day = :day AND subject = :subject"),
                {"day": day, "subject": subject},
            ).first()
        return int(row[0]) if row else 0


class UsageLimiter:
    def __init__(self, uri: str | None = DB_CONNECTION_STRING) -> None:
        self._memory = _MemoryCounters()
        self._pg = _PostgresCounters(uri) if uri else None
        self._warned = False
        if not uri:
            logger.warning("Agent usage limits are in-process only (no database): correct for one instance.")

    def _call(self, method: str, *args):
        """Postgres when it works, memory when it doesn't: a database blip should
        slow abuse protection down, not take the assistant offline."""
        if self._pg is not None:
            try:
                return getattr(self._pg, method)(*args)
            except Exception as exc:
                if not self._warned:
                    logger.error("agent_usage unavailable (%s); limits are in-process until it recovers", exc)
                    self._warned = True
        return getattr(self._memory, method)(*args)

    def admit_question(self, client_id: str, ip: str | None, owner: bool) -> Quota:
        """Consume one house question for this browser, or report why not."""
        day = today()
        client_limit = questions_per_client()

        if owner:
            used = self._call("peek", f"client:{client_id}", day)
            return Quota(True, used, client_limit, client_limit, resets_at(), owner=True)

        ok, used = self._call("take", f"client:{client_id}", client_limit, day)
        if not ok:
            return Quota(False, used, client_limit, 0, resets_at(), blocked_by="client")

        if ip:
            ip_ok, _ = self._call("take", f"ip:{ip}", questions_per_ip(), day)
            if not ip_ok:
                self._call("give_back", f"client:{client_id}", day)
                return Quota(False, used - 1, client_limit, max(0, client_limit - used + 1), resets_at(), blocked_by="ip")

        return Quota(True, used, client_limit, max(0, client_limit - used), resets_at())

    def refund_question(self, client_id: str, ip: str | None) -> None:
        """Give a question back when the assistant failed before answering."""
        day = today()
        self._call("give_back", f"client:{client_id}", day)
        if ip:
            self._call("give_back", f"ip:{ip}", day)

    def quota(self, client_id: str, owner: bool) -> Quota:
        day = today()
        limit = questions_per_client()
        used = self._call("peek", f"client:{client_id}", day)
        return Quota(owner or used < limit, used, limit, limit if owner else max(0, limit - used), resets_at(), owner=owner)

    def take_house_call(self) -> bool:
        """Consume one upstream model call from the shared house budget."""
        ok, _ = self._call("take", "upstream:house", house_daily_budget(), today())
        return ok

    def house_budget_remaining(self) -> int:
        return max(0, house_daily_budget() - self._call("peek", "upstream:house", today()))


_limiter: UsageLimiter | None = None
_limiter_lock = threading.Lock()


def limiter() -> UsageLimiter:
    global _limiter
    with _limiter_lock:
        if _limiter is None:
            _limiter = UsageLimiter()
        return _limiter
