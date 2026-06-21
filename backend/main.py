from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.aggregator import (
    DEFAULT_ROLES,
    DEFAULT_SKILLS,
    clear_cache,
    fetch_all,
    probe_sources,
    stream_all,
)
from backend.scrapers.companies import SCRAPERS
from backend.scrapers import spa_runner
from backend.store import HOT_WINDOW, get_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

ROOT = Path(__file__).resolve().parent.parent
FRONTEND = ROOT / "frontend"

app = FastAPI(title="India Data Jobs Aggregator", version="0.2.0")

_evict_task: asyncio.Task | None = None


async def _evict_loop() -> None:
    """Drop jobs from the hot tier whose posted_at slipped past HOT_WINDOW."""
    store = get_store()
    while True:
        try:
            store.evict_hot()
        except Exception as e:  # pragma: no cover
            logging.getLogger("store").exception("evict loop error: %s", e)
        await asyncio.sleep(3600)  # hourly


@app.on_event("startup")
async def _startup() -> None:
    # Touch the store so the SQLite schema is created and the hot tier is
    # loaded before the first request lands.
    get_store()
    global _evict_task
    _evict_task = asyncio.create_task(_evict_loop())
    await spa_runner.start_spa_refresher()


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _evict_task
    if _evict_task is not None:
        _evict_task.cancel()
        _evict_task = None
    await spa_runner.stop_spa_refresher()


def _split_csv(value: str | None) -> list[str] | None:
    """None -> None (use defaults); '' -> []; 'a,b' -> ['a', 'b']."""
    if value is None:
        return None
    return [s.strip() for s in value.split(",") if s.strip()]


@app.get("/api/companies")
async def list_companies() -> dict:
    return {"companies": sorted(SCRAPERS.keys())}


@app.get("/api/defaults")
async def list_defaults() -> dict:
    return {"roles": DEFAULT_ROLES, "skills": DEFAULT_SKILLS}


@app.get("/api/store/stats")
async def store_stats() -> dict:
    return get_store().stats()

@app.get("/api/status")
async def source_status(q: str = Query("data", description="Benign query to probe each scraper with")) -> dict:
    return {"query": q, "sources": await probe_sources(q)}


@app.get("/api/jobs")
async def get_jobs(
    roles: str | None = Query(None, description="Comma-separated role titles (matched in title). Omit to use defaults; empty string disables."),
    skills: str | None = Query(None, description="Comma-separated skills (matched via each site's full-text search)."),
    q: str | None = Query(None, description="Backward-compat: single role keyword. Overrides roles/skills."),
    company: str | None = Query(None, description="Restrict to one or more company names (comma-separated)."),
    posted_days: int | None = Query(None, description="Max age in days. <=7 serves hot tier only; >7 unions disk archive. None = any time."),
    refresh: bool = Query(False, description="Bypass server cache and purge the on-disk archive"),
) -> dict:
    if refresh:
        clear_cache(full=True)

    if q:
        role_list: list[str] | None = [q]
        skill_list: list[str] | None = []
    else:
        role_list = _split_csv(roles)
        skill_list = _split_csv(skills)

    company_list = _split_csv(company) or None
    jobs = await fetch_all(
        roles=role_list, skills=skill_list, companies=company_list,
        posted_days=posted_days,
    )
    return {
        "roles": role_list if role_list is not None else DEFAULT_ROLES,
        "skills": skill_list if skill_list is not None else DEFAULT_SKILLS,
        "companies": company_list,
        "posted_days": posted_days,
        "hot_window_days": HOT_WINDOW.days,
        "count": len(jobs),
        "jobs": [j.to_dict() for j in jobs],
    }


@app.get("/api/jobs/stream")
async def stream_jobs(
    roles: str | None = Query(None),
    skills: str | None = Query(None),
    q: str | None = Query(None),
    company: str | None = Query(None),
    posted_days: int | None = Query(None),
    refresh: bool = Query(False),
) -> StreamingResponse:
    if refresh:
        clear_cache(full=True)

    if q:
        role_list: list[str] | None = [q]
        skill_list: list[str] | None = []
    else:
        role_list = _split_csv(roles)
        skill_list = _split_csv(skills)

    company_list = _split_csv(company) or None
    valid = [c for c in (company_list or []) if c in SCRAPERS]
    target_names = sorted(SCRAPERS.keys()) if not valid else valid

    async def gen():
        yield json.dumps({"type": "start", "companies": target_names,
                          "posted_days": posted_days,
                          "hot_window_days": HOT_WINDOW.days}) + "\n"
        total = 0
        try:
            async for name, jobs in stream_all(
                roles=role_list, skills=skill_list,
                companies=valid or None, posted_days=posted_days,
            ):
                total += len(jobs)
                yield json.dumps({
                    "type": "batch",
                    "company": name,
                    "jobs": [j.to_dict() for j in jobs],
                }) + "\n"
        except Exception as e:  # pragma: no cover
            yield json.dumps({"type": "error", "error": str(e)[:300]}) + "\n"
        yield json.dumps({"type": "done", "total": total}) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")


# Serve the frontend (index.html + static assets) at "/"
if FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND), name="static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(FRONTEND / "index.html")
