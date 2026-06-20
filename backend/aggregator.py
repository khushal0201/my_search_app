from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import httpx

from backend.models import Job
from backend.scrapers.companies import SCRAPERS, _matches_query

log = logging.getLogger("aggregator")

DEFAULT_ROLES: list[str] = [
    "data engineer",
    "ai engineer",
    "data analyst",
    "machine learning engineer",
    "forward deployed engineer",
]
DEFAULT_SKILLS: list[str] = [
    "spark",
    "pyspark",
    "scala",
    "langchain",
    "langgraph",
    "tensorflow",
    "keras",
    "hadoop",
    "scikit-learn",
    "crewai",
]

_CACHE: dict[str, tuple[float, list[Job]]] = {}
_CACHE_TTL_SECONDS = 5 * 60 * 60  # 5 hours; override per-request with ?refresh=true
_MAX_CONCURRENT = 10  # cap total in-flight HTTP requests across all scrapers

# Scrapers that are slow and/or return huge result sets. They are run only
# after every other company has finished, so the initial /api/jobs stream
# fills up quickly with the fast feeds. Keep in sync with HEAVY_COMPANIES in
# frontend/app.js.
HEAVY_COMPANIES: set[str] = {"Infosys", "PwC", "Accenture", "KPMG", "Bosch"}


async def _run_one(name: str, fn, client: httpx.AsyncClient, query: str,
                   sema: asyncio.Semaphore) -> list[Job]:
    async with sema:
        try:
            jobs = await fn(client, query)
            log.info("%s [%s] -> %d jobs", name, query, len(jobs))
            return jobs
        except Exception as e:  # pragma: no cover - scrapers also catch internally
            log.warning("%s [%s] raised: %s", name, query, e)
            return []


def _cache_key(roles: list[str], skills: list[str], companies: Optional[list[str]]) -> str:
    c = sorted(companies) if companies else None
    return json.dumps({"r": roles, "s": skills, "c": c}, sort_keys=True)


def _cache_get(key: str) -> Optional[list[Job]]:
    cached = _CACHE.get(key)
    if cached and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    return None


def _sort_jobs(jobs: list[Job]) -> None:
    jobs.sort(key=lambda j: (j.posted_at is None, -(j.posted_at.timestamp() if j.posted_at else 0)))


async def fetch_all(
    roles: Optional[list[str]] = None,
    skills: Optional[list[str]] = None,
    companies: Optional[list[str]] = None,
) -> list[Job]:
    roles = roles if roles is not None else DEFAULT_ROLES
    skills = skills if skills is not None else DEFAULT_SKILLS

    key = _cache_key(roles, skills, companies)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    merged: list[Job] = []
    async for _name, jobs in iter_all(roles=roles, skills=skills, companies=companies):
        merged.extend(jobs)

    _sort_jobs(merged)
    _CACHE[key] = (time.time(), merged)
    return merged


async def iter_all(
    roles: Optional[list[str]] = None,
    skills: Optional[list[str]] = None,
    companies: Optional[list[str]] = None,
):
    """Yield (company_name, jobs) tuples as each company's scraper batch finishes.

    Each company is run as a single sub-task that fans out across all queries,
    dedupes locally, and yields a single batch when complete. Cross-company
    dedup is the caller's responsibility.
    """
    roles = roles if roles is not None else DEFAULT_ROLES
    skills = skills if skills is not None else DEFAULT_SKILLS

    if companies:
        wanted = {c for c in companies if c in SCRAPERS}
        targets = {n: SCRAPERS[n] for n in wanted}
    else:
        targets = SCRAPERS
    queries: list[tuple[str, bool]] = [(r, True) for r in roles] + [(s, False) for s in skills]
    sema = asyncio.Semaphore(_MAX_CONCURRENT)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        async def run_company(name: str, fn) -> tuple[str, list[Job]]:
            sub = [_run_one(name, fn, client, q, sema) for q, _ in queries]
            results = await asyncio.gather(*sub)
            out: list[Job] = []
            seen: set[tuple[str, str, str]] = set()
            for jobs, (q, match_title) in zip(results, queries):
                for j in jobs:
                    k = (j.company.lower(), (j.url or "").lower(), j.title.lower())
                    if k in seen:
                        continue
                    if match_title and not _matches_query(j.title, q):
                        continue
                    seen.add(k)
                    out.append(j)
            return name, out

        tasks = [
            asyncio.create_task(run_company(n, f))
            for n, f in targets.items()
            if n not in HEAVY_COMPANIES
        ]
        for coro in asyncio.as_completed(tasks):
            yield await coro

        heavy_tasks = [
            asyncio.create_task(run_company(n, f))
            for n, f in targets.items()
            if n in HEAVY_COMPANIES
        ]
        for coro in asyncio.as_completed(heavy_tasks):
            yield await coro


async def stream_all(
    roles: Optional[list[str]] = None,
    skills: Optional[list[str]] = None,
    companies: Optional[list[str]] = None,
):
    """Like iter_all but also dedupes across companies and warms the cache when done.

    If a cached result is available, it is replayed immediately as a single
    per-company batch sequence — no scrapers are invoked.
    """
    roles = roles if roles is not None else DEFAULT_ROLES
    skills = skills if skills is not None else DEFAULT_SKILLS

    key = _cache_key(roles, skills, companies)
    cached = _cache_get(key)
    if cached is not None:
        by_company: dict[str, list[Job]] = {}
        for j in cached:
            by_company.setdefault(j.company, []).append(j)
        for name, jobs in by_company.items():
            yield name, jobs
        return

    cross_seen: set[tuple[str, str, str]] = set()
    merged: list[Job] = []
    async for name, jobs in iter_all(roles=roles, skills=skills, companies=companies):
        unique: list[Job] = []
        for j in jobs:
            k = (j.company.lower(), (j.url or "").lower(), j.title.lower())
            if k in cross_seen:
                continue
            cross_seen.add(k)
            unique.append(j)
        merged.extend(unique)
        yield name, unique

    _sort_jobs(merged)
    _CACHE[key] = (time.time(), merged)


def clear_cache() -> None:
    _CACHE.clear()


async def probe_sources(query: str = "data") -> list[dict]:
    """
    Run each scraper once with a benign query and report status. Used by /api/status
    so the UI can show which sources are currently returning data vs. failing.
    """
    sema = asyncio.Semaphore(_MAX_CONCURRENT)
    statuses: list[dict] = []

    async def _probe(name: str, fn) -> dict:
        async with sema:
            start = time.time()
            try:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    jobs = await fn(client, query)
                return {"company": name, "ok": True, "count": len(jobs), "ms": int((time.time() - start) * 1000), "error": None}
            except Exception as e:
                return {"company": name, "ok": False, "count": 0, "ms": int((time.time() - start) * 1000), "error": str(e)[:200]}

    statuses = await asyncio.gather(*[_probe(n, f) for n, f in SCRAPERS.items()])
    return sorted(statuses, key=lambda s: (-s["count"], s["company"]))
