"""Two-tier persistent job store.

Architecture
------------
- **Hot tier (in-memory)**: jobs with `posted_at >= now - HOT_WINDOW` (7 days).
  Dict keyed by (company, url, title). Refilled at startup from disk; updated
  on every upsert. Read in O(N) for filters.

- **Cold tier (SQLite on disk)**: every job ever observed. Survives restarts.
  Periodically the hot tier evicts rows whose `posted_at` slipped past the
  7-day window; those rows remain in SQLite, so a `posted_days > 7` request
  can still surface them by querying the DB.

Public surface used by aggregator/main:
    store = get_store()
    store.upsert(jobs)                          # add/refresh from scrapers
    store.query(companies=None, max_age_days=N) # serve API
    store.evict_hot()                           # housekeeping; idempotent
    store.clear_all()                           # full purge (?refresh=true)

All SQLite calls are synchronous but tiny; we run them under a threading.Lock.
The data volume (~few thousand rows) means we don't need aiosqlite.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from backend.models import Job

log = logging.getLogger("store")

# 7-day hot window. Jobs with posted_at within this window live in RAM.
HOT_WINDOW = timedelta(days=7)

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "jobs.db"


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # SQLite stores ISO strings; allow both with and without tz.
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _job_key(j: Job) -> tuple[str, str, str]:
    return (j.company.lower(), (j.url or "").lower(), j.title.lower())


class JobStore:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self._path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                company    TEXT NOT NULL,
                title      TEXT NOT NULL,
                location   TEXT,
                url        TEXT,
                posted_at  TEXT,
                source     TEXT,
                seen_at    TEXT NOT NULL,
                PRIMARY KEY (company, url, title)
            )
            """
        )
        self._db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_posted ON jobs(posted_at)")
        self._db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_company ON jobs(company)")
        self._db.commit()

        # Hot tier: keyed by (company, url, title).
        self._hot: dict[tuple[str, str, str], Job] = {}
        self._load_hot()

    # ------------------------------------------------------------------
    # Hot tier maintenance
    # ------------------------------------------------------------------
    def _load_hot(self) -> None:
        cutoff = datetime.now(tz=timezone.utc) - HOT_WINDOW
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM jobs WHERE posted_at IS NOT NULL AND posted_at >= ?",
                (cutoff.isoformat(),),
            ).fetchall()
            self._hot = {_job_key(j): j for j in (_row_to_job(r) for r in rows)}
            # Also pull in jobs with unknown posted_at; we treat them as fresh
            # until proven otherwise (they'd otherwise never appear in the
            # 7-day view).
            rows_unknown = self._db.execute(
                "SELECT * FROM jobs WHERE posted_at IS NULL"
            ).fetchall()
            for r in rows_unknown:
                j = _row_to_job(r)
                self._hot[_job_key(j)] = j
        log.info("store: loaded %d hot rows from %s", len(self._hot), self._path)

    def evict_hot(self) -> int:
        """Drop jobs from hot tier whose posted_at slipped past 7 days.

        Cold tier (SQLite) is untouched. Returns count evicted.
        """
        cutoff = datetime.now(tz=timezone.utc) - HOT_WINDOW
        with self._lock:
            before = len(self._hot)
            self._hot = {
                k: j for k, j in self._hot.items()
                if j.posted_at is None or j.posted_at >= cutoff
            }
            after = len(self._hot)
        evicted = before - after
        if evicted:
            log.info("store: evicted %d rows from hot tier (%d remain)", evicted, after)
        return evicted

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------
    def upsert(self, jobs: Iterable[Job]) -> int:
        """Insert/refresh rows. Returns count written."""
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        cutoff = datetime.now(tz=timezone.utc) - HOT_WINDOW
        rows = []
        hot_updates: list[Job] = []
        for j in jobs:
            posted_iso = j.posted_at.isoformat() if j.posted_at else None
            rows.append((
                j.company, j.title, j.location, j.url or "",
                posted_iso, j.source, now_iso,
            ))
            if j.posted_at is None or j.posted_at >= cutoff:
                hot_updates.append(j)
        if not rows:
            return 0
        with self._lock:
            self._db.executemany(
                """
                INSERT INTO jobs (company, title, location, url, posted_at, source, seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company, url, title) DO UPDATE SET
                    location  = excluded.location,
                    posted_at = excluded.posted_at,
                    source    = excluded.source,
                    seen_at   = excluded.seen_at
                """,
                rows,
            )
            self._db.commit()
            for j in hot_updates:
                self._hot[_job_key(j)] = j
        return len(rows)

    def clear_all(self) -> None:
        """Full purge: wipe SQLite + hot tier. Called on ?refresh=true."""
        with self._lock:
            self._db.execute("DELETE FROM jobs")
            self._db.commit()
            self._hot.clear()
        log.info("store: cleared all (memory + disk)")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def query(
        self,
        companies: Optional[list[str]] = None,
        max_age_days: Optional[int] = None,
    ) -> list[Job]:
        """Return jobs filtered by company list and optional max age.

        - max_age_days <= 7 (or None and HOT-only requested) -> served from RAM.
        - max_age_days > 7 or None -> full union: RAM + SQLite for older rows.
        """
        wanted = {c for c in (companies or [])} or None

        # Hot tier always contributes
        out: list[Job] = []
        for j in list(self._hot.values()):
            if wanted and j.company not in wanted:
                continue
            out.append(j)

        # If user asked for <= HOT_WINDOW only, we're done.
        if max_age_days is not None and max_age_days <= HOT_WINDOW.days:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
            return [j for j in out if j.posted_at and j.posted_at >= cutoff]

        # Otherwise union with cold tier for rows OLDER than the hot window.
        cold_cutoff_lo = datetime.now(tz=timezone.utc) - timedelta(
            days=max_age_days
        ) if max_age_days else None
        cold_cutoff_hi = datetime.now(tz=timezone.utc) - HOT_WINDOW

        params: list = [cold_cutoff_hi.isoformat()]
        sql = "SELECT * FROM jobs WHERE posted_at IS NOT NULL AND posted_at < ?"
        if cold_cutoff_lo is not None:
            sql += " AND posted_at >= ?"
            params.append(cold_cutoff_lo.isoformat())
        if wanted:
            placeholders = ",".join("?" for _ in wanted)
            sql += f" AND company IN ({placeholders})"
            params.extend(sorted(wanted))

        with self._lock:
            rows = self._db.execute(sql, params).fetchall()

        seen = {_job_key(j) for j in out}
        for r in rows:
            j = _row_to_job(r)
            if _job_key(j) in seen:
                continue
            out.append(j)
        return out

    def stats(self) -> dict:
        with self._lock:
            total = self._db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        return {
            "hot": len(self._hot),
            "cold_total": total,
            "db_path": str(self._path),
            "hot_window_days": HOT_WINDOW.days,
        }


def _row_to_job(r: sqlite3.Row) -> Job:
    return Job(
        company=r["company"],
        title=r["title"],
        location=r["location"] or "",
        url=r["url"] or "",
        posted_at=_parse_dt(r["posted_at"]),
        source=r["source"] or "",
    )


_store: Optional[JobStore] = None
_store_lock = threading.Lock()


def get_store() -> JobStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = JobStore()
        return _store
