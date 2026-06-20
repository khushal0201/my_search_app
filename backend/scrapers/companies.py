"""
Per-company scrapers. Each scraper is an async function:

    async def fetch(client: httpx.AsyncClient, query: str) -> list[Job]

It must:
- Return only India-based jobs.
- Catch its own errors and return [] on failure (never raise).
- Populate Job.posted_at as a timezone-aware UTC datetime when possible.

These endpoints are publicly reachable JSON/XHR calls that the companies'
career sites themselves use. They are best-effort — sites change schemas
without notice, so a scraper returning [] is expected, not fatal.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import httpx
from dateutil import parser as dateparser

from backend.models import Job

log = logging.getLogger("scrapers")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
DEFAULT_HEADERS = {"User-Agent": UA, "Accept": "application/json, text/plain, */*"}


def _looks_india(s: str | None) -> bool:
    if not s:
        return False
    s = s.lower()
    if "india" in s:
        return True
    cities = (
        "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "chennai",
        "gurgaon", "gurugram", "noida", "new delhi", "delhi", "kolkata",
        "ahmedabad", "jaipur",
    )
    return any(c in s for c in cities)


def _matches_query(title: str, query: str) -> bool:
    t = (title or "").lower()
    return all(tok in t for tok in query.lower().split())


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            # epoch seconds or ms
            v = float(value)
            if v > 1e12:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        dt = dateparser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _days_ago(text: str) -> Optional[datetime]:
    """Parse strings like '3 days ago', 'Posted Today', 'Yesterday'."""
    if not text:
        return None
    t = text.lower()
    now = datetime.now(timezone.utc)
    if "today" in t or "just posted" in t:
        return now
    if "yesterday" in t:
        return now - timedelta(days=1)
    m = re.search(r"(\d+)\s*\+?\s*day", t)
    if m:
        return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s*\+?\s*hour", t)
    if m:
        return now - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)\s*\+?\s*month", t)
    if m:
        return now - timedelta(days=30 * int(m.group(1)))
    return None


# ---------------------------------------------------------------------------
# Amazon
# ---------------------------------------------------------------------------
async def fetch_amazon(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.amazon.jobs/en/search.json"
    params = {
        "normalized_country_code[]": "IND",
        "radius": "24km",
        "industry_experience": "",
        "facets[]": ["normalized_country_code", "city"],
        "offset": 0,
        "result_limit": 50,
        "sort": "recent",
        "base_query": query,
    }
    try:
        r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("amazon failed: %s", e)
        return []
    out: list[Job] = []
    for j in data.get("jobs", []):
        loc = j.get("normalized_location") or j.get("location") or ""
        if not _looks_india(loc):
            continue
        title = j.get("title", "")
        out.append(Job(
            company="Amazon",
            title=title,
            location=loc,
            url="https://www.amazon.jobs" + (j.get("job_path") or ""),
            posted_at=_parse_dt(j.get("posted_date")) or _days_ago(j.get("posted_date", "")),
            source="amazon.jobs",
        ))
    return out


# ---------------------------------------------------------------------------
# Microsoft — now hosted on Eightfold at apply.careers.microsoft.com. Each
# search-result page SSRs every visible role as a JobPosting JSON-LD block.
# Legacy gcsservices.careers.microsoft.com endpoint is dead (301 -> 404).
# ---------------------------------------------------------------------------
async def fetch_microsoft(client: httpx.AsyncClient, query: str) -> list[Job]:
    # Microsoft uses the same Eightfold tenant pattern as Qualcomm; reuse helper.
    return await _fetch_eightfold_html(
        client, "Microsoft",
        "https://apply.careers.microsoft.com/careers",
        pid="1970393556872427", query=query, domain="microsoft.com",
        source="apply.careers.microsoft.com",
    )


# ---------------------------------------------------------------------------
# Google (scrape SSR HTML + AF_initDataCallback hydration blob)
# ---------------------------------------------------------------------------
async def fetch_google(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.google.com/about/careers/applications/jobs/results"
    params = {"location": "India", "q": query}
    try:
        r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        log.warning("google failed: %s", e)
        return []

    # The page ships an AF_initDataCallback({key:'ds:1', data:[...]}) blob holding
    # all jobs as JSON. Each record is a 21-element list:
    #   [0] jobId, [1] title, [2] applyUrl, [9] [[[locText,address,lat,lng,...]]],
    #   [12]/[13]/[14] [epochSeconds, nanos] timestamps.
    out: list[Job] = []
    blob_match = re.search(
        r"AF_initDataCallback\(\{[^{]*key:\s*'ds:1'[^{]*data:(\[.+?\]),\s*sideChannel",
        html, flags=re.S,
    )
    if blob_match:
        try:
            data = json.loads(blob_match.group(1))
        except Exception:
            data = None
        if data is not None:
            for rec in _walk_lists(data):
                if (
                    not isinstance(rec, list) or len(rec) < 15
                    or not isinstance(rec[0], str) or not rec[0].isdigit() or len(rec[0]) < 15
                    or not isinstance(rec[1], str)
                ):
                    continue
                jid = rec[0]
                title = rec[1]
                # Location
                loc = "India"
                try:
                    loc_cell = rec[9][0][0]
                    if isinstance(loc_cell, str) and loc_cell:
                        loc = loc_cell
                except Exception:
                    pass
                # Posted time: pick the earliest [secs, nanos] pair in the record.
                posted_at = None
                ts_candidates: list[int] = []
                for cell in rec[10:]:
                    if (isinstance(cell, list) and len(cell) == 2
                            and isinstance(cell[0], int) and 1_500_000_000 < cell[0] < 2_500_000_000):
                        ts_candidates.append(cell[0])
                if ts_candidates:
                    posted_at = datetime.fromtimestamp(min(ts_candidates), tz=timezone.utc)
                slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
                out.append(Job(
                    company="Google",
                    title=title,
                    location=loc,
                    url=f"https://www.google.com/about/careers/applications/jobs/results/{jid}-{slug}?location=India",
                    posted_at=posted_at,
                    source="google.com/careers",
                ))
        if out:
            return out

    # Fallback: parse anchor hrefs only (no dates).
    seen: set[str] = set()
    for m in re.finditer(
        r'href="(jobs/results/(\d+)-([a-z0-9\-]+)(?:\?[^"]*)?)"',
        html,
    ):
        href = m.group(1).replace("&amp;", "&")
        jid = m.group(2)
        slug = m.group(3)
        if jid in seen:
            continue
        seen.add(jid)
        title = slug.replace("-", " ").title()
        out.append(Job(
            company="Google",
            title=title,
            location="India",
            url=f"https://www.google.com/about/careers/applications/{href}",
            posted_at=None,
            source="google.com/careers",
        ))
    return out


def _walk_lists(node: Any):
    """Depth-first iterator over every list/dict node in a nested structure."""
    if isinstance(node, list):
        yield node
        for v in node:
            yield from _walk_lists(v)
    elif isinstance(node, dict):
        for v in node.values():
            yield from _walk_lists(v)


# ---------------------------------------------------------------------------
# Apple — SSR HTML carries a JSON island (<script id="__ACGH_DATA__">) with
# the full search results, so we can avoid Playwright entirely.
# Honors server-side India filter via location=india-INDC.
# ---------------------------------------------------------------------------
async def fetch_apple(client: httpx.AsyncClient, query: str) -> list[Job]:
    base = "https://jobs.apple.com/en-in/search"
    headers = {**DEFAULT_HEADERS, "Accept": "text/html,application/xhtml+xml"}
    out: list[Job] = []
    seen: set[str] = set()
    for page in range(1, 6):  # up to ~100 results
        params = {
            "search": query,
            "sort": "newest",
            "location": "india-INDC",
            "page": page,
        }
        try:
            r = await client.get(base, params=params, headers=headers, timeout=20)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            log.warning("apple page %d failed: %s", page, e)
            break
        # Apple SSRs the job list into window.__staticRouterHydrationData,
        # which is a JSON string passed to JSON.parse() — so it's double-encoded.
        m = re.search(
            r'window\.__staticRouterHydrationData\s*=\s*JSON\.parse\("(.*?)"\);',
            html, flags=re.S,
        )
        if not m:
            break
        try:
            inner = json.loads('"' + m.group(1) + '"')  # unescape JS string literal
            data = json.loads(inner)
        except Exception as e:
            log.warning("apple page %d parse failed: %s", page, e)
            break
        added = 0
        def _walk(node):
            if isinstance(node, dict):
                yield node
                for v in node.values():
                    yield from _walk(v)
            elif isinstance(node, list):
                for v in node:
                    yield from _walk(v)
        for rec in _walk(data):
            if not isinstance(rec, dict):
                continue
            pid = rec.get("positionId")
            title = rec.get("postingTitle")
            if not pid or not title or pid in seen:
                continue
            locs = rec.get("locations") or []
            loc_text = ", ".join(
                (l.get("name") or l.get("city") or "") if isinstance(l, dict) else str(l)
                for l in locs
            ).strip(", ")
            if not _looks_india(loc_text + " " + str(rec.get("countryID", ""))):
                continue
            seen.add(pid)
            added += 1
            slug = rec.get("transformedPostingTitle") or re.sub(r"[^a-z0-9]+", "-", str(title).lower()).strip("-")
            out.append(Job(
                company="Apple",
                title=title,
                location=loc_text or "India",
                url=f"https://jobs.apple.com/en-in/details/{pid}/{slug}",
                posted_at=_parse_dt(rec.get("postDateInGMT") or rec.get("postingDate")),
                source="jobs.apple.com",
            ))
        if added == 0:
            break
    return out


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
async def fetch_meta(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.metacareers.com/graphql"
    body = {
        "doc_id": "9114524511922157",
        "variables": {
            "search_input": {
                "q": query,
                "divisions": [],
                "offices": ["India"],
                "roles": [],
                "leadership_levels": [],
                "saved_jobs": [],
                "saved_searches": [],
                "sub_teams": [],
                "teams": [],
                "is_leadership": False,
                "is_remote_only": False,
                "is_in_page": False,
            },
        },
    }
    headers = {**DEFAULT_HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = await client.post(
            url,
            data={"variables": __import__("json").dumps(body["variables"]), "doc_id": body["doc_id"]},
            headers=headers,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("meta failed: %s", e)
        return []
    results = ((data.get("data") or {}).get("job_search")) or []
    out: list[Job] = []
    for j in results:
        locs = j.get("locations") or []
        loc = ", ".join(locs) if isinstance(locs, list) else str(locs)
        if not _looks_india(loc):
            continue
        title = j.get("title", "")
        jid = j.get("id") or ""
        out.append(Job(
            company="Meta",
            title=title,
            location=loc,
            url=f"https://www.metacareers.com/jobs/{jid}/",
            posted_at=None,  # Meta GraphQL doesn't expose a stable post date here
            source="metacareers.com",
        ))
    return out


# ---------------------------------------------------------------------------
# Generic Workday helper (used by several companies)
# ---------------------------------------------------------------------------
async def _fetch_workday(
    client: httpx.AsyncClient,
    company: str,
    base: str,
    site: str,
    tenant: str,
    query: str,
    india_location_facet: list[dict] | None = None,
    max_pages: int = 8,
) -> list[Job]:
    """
    Workday public CXS endpoint:
        POST {base}/wday/cxs/{tenant}/{site}/jobs
    Paginates up to ``max_pages`` × 20 results and keeps India hits only.
    """
    url = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    headers = {**DEFAULT_HEADERS, "Content-Type": "application/json"}
    out: list[Job] = []
    # Workday only reports `total` on the first page; subsequent pages return 0.
    # Remember it once so we don't bail out early.
    grand_total: Optional[int] = None
    for page in range(max_pages):
        payload: dict[str, Any] = {
            "appliedFacets": {},
            "limit": 20,
            "offset": page * 20,
            "searchText": query,
        }
        if india_location_facet:
            payload["appliedFacets"] = {"locationCountry": india_location_facet}
        try:
            r = await client.post(url, json=payload, headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("%s (workday p%d) failed: %s", company, page, e)
            break
        postings = data.get("jobPostings", []) or []
        if not postings:
            break
        for j in postings:
            loc = j.get("locationsText") or j.get("location") or ""
            if not loc:
                # Some tenants (e.g. Accenture) omit locationsText and put the
                # city in bulletFields[1]. Fall back to that.
                bf = j.get("bulletFields") or []
                if len(bf) >= 2 and isinstance(bf[1], str):
                    loc = bf[1]
            # When an India facet is applied, we trust the server-side filter
            # and don't require the location string to contain "India".
            if not india_location_facet and not _looks_india(loc):
                continue
            title = j.get("title", "")
            ext = j.get("externalPath") or ""
            full_url = f"{base}/en-US/{site}{ext}" if ext else f"{base}/en-US/{site}"
            out.append(Job(
                company=company,
                title=title,
                location=loc or "India",
                url=full_url,
                posted_at=_days_ago(j.get("postedOn", "")),
                source="workday",
            ))
        page_total = data.get("total") or 0
        if grand_total is None and page_total > 0:
            grand_total = page_total
        # Stop if we've fetched everything the first page promised, or got a short page.
        if grand_total and (page + 1) * 20 >= grand_total:
            break
        if len(postings) < 20:
            break
    return out


async def fetch_mastercard(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Mastercard",
        "https://mastercard.wd1.myworkdayjobs.com", "CorporateCareers", "mastercard", query,
    )


# ---------------------------------------------------------------------------
# Visa (Workday wd5 tenant)
# ---------------------------------------------------------------------------
async def fetch_visa(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Visa",
        "https://visa.wd5.myworkdayjobs.com", "Visa", "visa", query,
    )


# ---------------------------------------------------------------------------
# JPMorgan Chase (Oracle HCM Recruiting Cloud)
# ---------------------------------------------------------------------------
async def fetch_jpmorgan(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    finder = (
        "findReqs;siteNumber=CX_1001"
        f",keyword={query}"
        ",locationId=300000000471814"  # India location id used by JPMC site
        ",sortBy=POSTING_DATES_DESC"
    )
    params = {
        "onlyData": "true",
        "limit": 25,
        "expand": "requisitionList.secondaryLocations,requisitionList.requisitionFlexFields",
        "finder": finder,
    }
    try:
        r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("jpmorgan failed: %s", e)
        return []
    items = data.get("items") or []
    reqs = items[0].get("requisitionList", []) if items and isinstance(items[0], dict) else []
    out: list[Job] = []
    for j in reqs:
        loc = j.get("PrimaryLocation") or ""
        if not _looks_india(loc):
            continue
        jid = j.get("Id") or ""
        out.append(Job(
            company="JPMorgan Chase",
            title=j.get("Title", ""),
            location=loc,
            url=f"https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/{jid}/" if jid else "https://careers.jpmorganchase.com",
            posted_at=_parse_dt(j.get("PostedDate")),
            source="jpmc.fa.oraclecloud.com",
        ))
    return out


async def fetch_morgan_stanley(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Morgan Stanley",
        "https://ms.wd5.myworkdayjobs.com", "External", "ms", query,
    )


# ---------------------------------------------------------------------------
# Citi
# ---------------------------------------------------------------------------
async def fetch_citi(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://jobs.citi.com/search-jobs/results"
    params = {
        "ActiveFacetID": "india",
        "CurrentPage": 1,
        "RecordsPerPage": 25,
        "Distance": 50,
        "RadiusUnitType": 0,
        "Keywords": query,
        "Location": "India",
        "ShowRadius": "False",
        "IsPagination": "False",
        "CustomFacetName": "",
        "FacetTerm": "",
        "FacetType": 0,
        "SearchResultsModuleName": "Search Results",
        "SearchFiltersModuleName": "Search Filters",
        "SortCriteria": 1,  # most recent
        "SortDirection": 1,
    }
    html = ""
    try:
        r = await client.get(url, params=params, headers={**DEFAULT_HEADERS, "Accept": "application/json", "X-Requested-With": "XMLHttpRequest", "Referer": "https://jobs.citi.com/search-jobs/India"}, timeout=20)
        r.raise_for_status()
        data = r.json()
        html = data.get("results", "") if isinstance(data, dict) else ""
    except Exception as e:
        log.warning("citi JSON failed: %s", e)
    # Fallback: scrape the SSR HTML page directly when the JSON XHR is empty.
    if not html.strip():
        try:
            r = await client.get(
                f"https://jobs.citi.com/search-jobs/India",
                params={"Keywords": query},
                headers=DEFAULT_HEADERS,
                timeout=25,
            )
            r.raise_for_status()
            html = r.text
        except Exception as e:
            log.warning("citi HTML fallback failed: %s", e)
            return []
    out: list[Job] = []
    seen: set[str] = set()
    # Try the JSON-results card shape first (h2 + job-location).
    for m in re.finditer(
        r'<a href="(?P<href>[^"]+)"[^>]*>\s*<h2>(?P<title>[^<]+)</h2>.*?<span class="job-location">(?P<loc>[^<]+)</span>',
        html, flags=re.S,
    ):
        href, title, loc = m.group("href").strip(), m.group("title").strip(), m.group("loc").strip()
        if not _looks_india(loc) or href in seen:
            continue
        seen.add(href)
        if href.startswith("/"):
            href = "https://jobs.citi.com" + href
        out.append(Job(
            company="Citi", title=title, location=loc, url=href,
            posted_at=None, source="jobs.citi.com",
        ))
    if out:
        await _hydrate_citi_dates(client, out)
        return out
    # SSR HTML uses the sr-job-item__link shape instead.
    for m in re.finditer(
        r'<a class="sr-job-item__link"\s+href="(?P<href>[^"]+)"[^>]*>\s*(?P<title>.+?)\s*</a>(?P<rest>.{0,2000}?)<span class="sr-job-item__facet[^"]*sr-job-location"[^>]*>(?P<loc>[^<]+)</span>',
        html, flags=re.S,
    ):
        href = m.group("href").strip()
        title = re.sub(r"<[^>]+>", "", m.group("title")).strip()
        loc = m.group("loc").strip()
        if not _looks_india(loc) or href in seen:
            continue
        seen.add(href)
        if href.startswith("/"):
            href = "https://jobs.citi.com" + href
        out.append(Job(
            company="Citi", title=title, location=loc, url=href,
            posted_at=None, source="jobs.citi.com",
        ))
    # Fetch each detail page in parallel to recover datePosted from JSON-LD.
    await _hydrate_citi_dates(client, out)
    return out


async def _hydrate_citi_dates(client: httpx.AsyncClient, jobs: list[Job]) -> None:
    """Citi listing cards don't carry a date; each detail page does (JSON-LD JobPosting.datePosted)."""
    if not jobs:
        return
    async def _one(job: Job) -> None:
        try:
            r = await client.get(job.url, headers=DEFAULT_HEADERS, timeout=15)
            if r.status_code != 200:
                return
            m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', r.text)
            if m:
                job.posted_at = _parse_dt(m.group(1))
        except Exception:
            pass
    await asyncio.gather(*[_one(j) for j in jobs], return_exceptions=True)


# ---------------------------------------------------------------------------
# Goldman Sachs — higher.gs.com migrated to a client-rendered Apollo SPA.
# The __NEXT_DATA__ blob is now an empty skeleton, so this scraper has been
# moved to spa_runner.py (Playwright). Keep this comment so the registry
# lookup below clearly maps to the SPA wrapper.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# American Express (Oracle HCM at careers.americanexpress.com; Eightfold fallback)
# ---------------------------------------------------------------------------
async def fetch_amex(client: httpx.AsyncClient, query: str) -> list[Job]:
    # Primary: Oracle HCM Recruiting Cloud. The vanity host careers.americanexpress.com
    # returns "page not found" on /hcmRestApi/ — must hit the underlying Oracle pod.
    url = "https://egug.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    finder = f"findReqs;siteNumber=CX_1,keyword={query},sortBy=POSTING_DATES_DESC"
    params = {
        "onlyData": "true",
        "limit": 25,
        "expand": "requisitionList.secondaryLocations,requisitionList.requisitionFlexFields",
        "finder": finder,
    }
    out: list[Job] = []
    try:
        r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        reqs = items[0].get("requisitionList", []) if items and isinstance(items[0], dict) else []
        for j in reqs:
            loc = j.get("PrimaryLocation") or ""
            if not _looks_india(loc):
                continue
            jid = j.get("Id") or ""
            out.append(Job(
                company="American Express",
                title=j.get("Title", ""),
                location=loc,
                url=f"https://careers.americanexpress.com/en/sites/CX_1/job/{jid}/" if jid else "https://careers.americanexpress.com",
                posted_at=_parse_dt(j.get("PostedDate")),
                source="careers.americanexpress.com",
            ))
    except Exception as e:
        log.warning("amex (Oracle HCM) failed: %s", e)
    if out:
        return out
    # Fallback: Eightfold tenant (legacy, currently returns empty).
    try:
        r = await client.get(
            "https://aexp.eightfold.ai/api/apply/v2/jobs",
            params={"domain": "aexp.com", "query": query, "location": "India",
                    "sort_by": "timestamp", "start": 0, "num": 25},
            headers=DEFAULT_HEADERS, timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("amex (Eightfold fallback) failed: %s", e)
        return out
    for j in data.get("positions", []):
        loc = j.get("location") or ", ".join(j.get("locations") or [])
        if not _looks_india(loc):
            continue
        out.append(Job(
            company="American Express",
            title=j.get("name", ""),
            location=loc,
            url=j.get("canonicalPositionUrl") or f"https://aexp.eightfold.ai/careers?pid={j.get('id', '')}",
            posted_at=_parse_dt(j.get("t_create") or j.get("t_update")),
            source="aexp.eightfold.ai",
        ))
    return out


# ---------------------------------------------------------------------------
# Generic Greenhouse helper (boards-api.greenhouse.io)
# ---------------------------------------------------------------------------
async def _fetch_greenhouse(
    client: httpx.AsyncClient,
    company: str,
    slug: str,
    query: str,
) -> list[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        r = await client.get(url, params={"content": "true"}, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("%s (greenhouse) failed: %s", company, e)
        return []
    out: list[Job] = []
    for j in data.get("jobs", []):
        loc_name = ((j.get("location") or {}).get("name")) or ""
        offices = j.get("offices") or []
        office_names = " ".join((o.get("name") or "") for o in offices if isinstance(o, dict))
        if not _looks_india(loc_name + " " + office_names):
            continue
        title = j.get("title", "")
        if query and not _matches_query(title, query):
            continue
        out.append(Job(
            company=company,
            title=title,
            location=loc_name or "India",
            url=j.get("absolute_url") or f"https://boards.greenhouse.io/{slug}/jobs/{j.get('id', '')}",
            posted_at=_parse_dt(j.get("updated_at") or j.get("first_published")),
            source="greenhouse.io",
        ))
    return out


# ---------------------------------------------------------------------------
# Generic Lever helper (api.lever.co)
# ---------------------------------------------------------------------------
async def _fetch_lever(
    client: httpx.AsyncClient,
    company: str,
    slug: str,
    query: str,
) -> list[Job]:
    url = f"https://api.lever.co/v0/postings/{slug}"
    try:
        r = await client.get(url, params={"mode": "json", "limit": 200}, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("%s (lever) failed: %s", company, e)
        return []
    if not isinstance(data, list):
        return []
    out: list[Job] = []
    for j in data:
        cats = j.get("categories") or {}
        loc = cats.get("location") or ""
        all_locs = j.get("additional") or ""  # rarely populated
        if not _looks_india(f"{loc} {all_locs}"):
            continue
        title = j.get("text") or ""
        if query and not _matches_query(title, query):
            continue
        ts = j.get("createdAt")  # epoch ms
        out.append(Job(
            company=company,
            title=title,
            location=loc or "India",
            url=j.get("hostedUrl") or j.get("applyUrl") or "",
            posted_at=_parse_dt(ts) if ts else None,
            source="lever.co",
        ))
    return out


# ---------------------------------------------------------------------------
# Generic Ashby helper (api.ashbyhq.com)
# ---------------------------------------------------------------------------
async def _fetch_ashby(
    client: httpx.AsyncClient,
    company: str,
    slug: str,
    query: str,
) -> list[Job]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    try:
        r = await client.get(url, params={"includeCompensation": "true"}, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("%s (ashby) failed: %s", company, e)
        return []
    out: list[Job] = []
    for j in data.get("jobs", []):
        primary = j.get("locationName", "") or ""
        secondary = " ".join(
            (s.get("locationName", "") if isinstance(s, dict) else "")
            for s in (j.get("secondaryLocations") or [])
        )
        if not _looks_india(f"{primary} {secondary}"):
            continue
        title = j.get("title", "")
        if query and not _matches_query(title, query):
            continue
        out.append(Job(
            company=company,
            title=title,
            location=primary or "India",
            url=j.get("jobUrl") or f"https://jobs.ashbyhq.com/{slug}/{j.get('id', '')}",
            posted_at=_parse_dt(j.get("publishedDate") or j.get("updatedAt")),
            source="ashbyhq.com",
        ))
    return out


# ---------------------------------------------------------------------------
# Generic Phenom helper (public /api/jobs endpoint used by some Phenom sites)
# ---------------------------------------------------------------------------
async def _fetch_phenom(
    client: httpx.AsyncClient,
    company: str,
    base: str,
    query: str,
    page_size: int = 100,
    max_pages: int = 15,
) -> list[Job]:
    """Phenom's public /api/jobs paginates via ?page=N&limit=M (limit caps at 100)."""
    url = f"{base}/api/jobs"
    headers = {**DEFAULT_HEADERS, "Referer": base + "/"}
    out: list[Job] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        params = {"country": "India", "page": page, "limit": page_size}
        try:
            r = await client.get(url, params=params, headers=headers, timeout=25)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("%s (phenom p%d) failed: %s", company, page, e)
            break
        entries = data.get("jobs", []) or []
        if not entries:
            break
        for entry in entries:
            j = entry.get("data") if isinstance(entry, dict) else None
            if not isinstance(j, dict):
                continue
            loc = j.get("full_location") or ", ".join(
                p for p in [j.get("city"), j.get("state"), j.get("country")] if p
            )
            if not _looks_india(f"{loc} {j.get('country', '')}"):
                continue
            title = j.get("title", "")
            if query and not _matches_query(title, query):
                continue
            apply_url = j.get("apply_url") or ""
            key = apply_url or f"{title}|{loc}"
            if key in seen:
                continue
            seen.add(key)
            out.append(Job(
                company=company,
                title=title,
                location=loc or "India",
                url=apply_url,
                posted_at=_parse_dt(j.get("posted_date") or j.get("create_date")),
                source="phenom",
            ))
        total = data.get("totalCount", 0)
        if total and page * page_size >= total:
            break
        if len(entries) < page_size:
            break
    return out


# ---------------------------------------------------------------------------
# Newly added companies (verified working as of probe run)
# ---------------------------------------------------------------------------
async def fetch_mongodb(client, q):     return await _fetch_greenhouse(client, "MongoDB", "mongodb", q)
async def fetch_groww(client, q):       return await _fetch_greenhouse(client, "Groww", "groww", q)
async def fetch_airbnb(client, q):      return await _fetch_greenhouse(client, "Airbnb", "airbnb", q)
async def fetch_anthropic(client, q):   return await _fetch_greenhouse(client, "Anthropic", "anthropic", q)
async def fetch_cloudflare(client, q):  return await _fetch_greenhouse(client, "Cloudflare", "cloudflare", q)
async def fetch_reddit(client, q):      return await _fetch_greenhouse(client, "Reddit", "reddit", q)
async def fetch_discord(client, q):     return await _fetch_greenhouse(client, "Discord", "discord", q)
async def fetch_jane_street(client, q): return await _fetch_greenhouse(client, "Jane Street", "janestreet", q)

async def fetch_spotify(client, q):     return await _fetch_lever(client, "Spotify", "spotify", q)

async def fetch_philips(client, q):
    return await _fetch_workday(client, "Philips",
        "https://philips.wd3.myworkdayjobs.com", "jobs-and-careers", "philips", q)

async def fetch_deutsche_bank(client, q):
    return await _fetch_workday(client, "Deutsche Bank",
        "https://db.wd3.myworkdayjobs.com", "DBWebsite", "db", q)

async def fetch_autodesk(client, q):
    return await _fetch_workday(client, "Autodesk",
        "https://autodesk.wd1.myworkdayjobs.com", "Ext", "autodesk", q)

async def fetch_salesforce(client, q):
    return await _fetch_workday(client, "Salesforce",
        "https://salesforce.wd12.myworkdayjobs.com", "External_Career_Site", "salesforce", q)

async def fetch_blackrock(client, q):
    return await _fetch_workday(client, "BlackRock",
        "https://blackrock.wd1.myworkdayjobs.com", "BlackRock_Professional", "blackrock", q)

async def fetch_nvidia(client, q):
    return await _fetch_workday(client, "NVIDIA",
        "https://nvidia.wd5.myworkdayjobs.com", "NVIDIAExternalCareerSite", "nvidia", q)


# Round-2 additions (verified via probe2)
async def fetch_razorpay(client, q):
    return await _fetch_greenhouse(client, "Razorpay", "razorpaysoftwareprivatelimited", q)
async def fetch_paytm(client, q):       return await _fetch_lever(client, "Paytm", "paytm", q)
async def fetch_xai(client, q):         return await _fetch_greenhouse(client, "xAI", "xai", q)

async def fetch_openai(client, q):      return await _fetch_ashby(client, "OpenAI", "openai", q)
async def fetch_notion(client, q):      return await _fetch_ashby(client, "Notion", "notion", q)
async def fetch_confluent(client, q):   return await _fetch_ashby(client, "Confluent", "confluent", q)
async def fetch_snowflake(client, q):   return await _fetch_ashby(client, "Snowflake", "snowflake", q)

async def fetch_expedia(client, q):
    return await _fetch_workday(client, "Expedia",
        "https://expedia.wd108.myworkdayjobs.com", "search", "expedia", q)

async def fetch_zendesk(client, q):
    return await _fetch_workday(client, "Zendesk",
        "https://zendesk.wd1.myworkdayjobs.com", "zendesk", "zendesk", q)

async def fetch_broadcom(client, q):
    return await _fetch_workday(client, "Broadcom",
        "https://broadcom.wd1.myworkdayjobs.com", "External_Career", "broadcom", q)

async def fetch_paypal(client, q):
    return await _fetch_workday(client, "PayPal",
        "https://paypal.wd1.myworkdayjobs.com", "jobs", "paypal", q)

async def fetch_samsung(client, q):
    return await _fetch_workday(client, "Samsung",
        "https://sec.wd3.myworkdayjobs.com", "Samsung_Careers", "sec", q)


# Round-3 additions (verified via probe_ats run)
async def fetch_adobe(client, q):
    return await _fetch_workday(client, "Adobe",
        "https://adobe.wd5.myworkdayjobs.com", "external_experienced", "adobe", q)

async def fetch_disney(client, q):
    return await _fetch_workday(client, "Disney",
        "https://disney.wd5.myworkdayjobs.com", "disneycareer", "disney", q, max_pages=15)

async def fetch_warner_bros(client, q):
    return await _fetch_workday(client, "Warner Bros Discovery",
        "https://warnerbros.wd5.myworkdayjobs.com", "global", "warnerbros", q)

async def fetch_pwc(client, q):
    return await _fetch_workday(client, "PwC",
        "https://pwc.wd3.myworkdayjobs.com", "Global_Experienced_Careers", "pwc", q, max_pages=12)

async def fetch_zscaler(client, q):
    return await _fetch_greenhouse(client, "Zscaler", "zscaler", q)

async def fetch_meesho(client, q):
    return await _fetch_lever(client, "Meesho", "meesho", q)

async def fetch_pepsico(client, q):
    return await _fetch_phenom(client, "PepsiCo", "https://www.pepsicojobs.com", q)


# Walmart — WalmartExternal Workday tenant (wd504).
# Their main careers.walmart.com GraphQL dropped the country filter, but this
# Workday site has ~190 India jobs reliably filterable via locationCountry.
_WALMART_INDIA_FACET = ["c4f78be1a8f14da0ab49ce1162348a5e"]
async def fetch_walmart(client, q):
    return await _fetch_workday(
        client, "Walmart",
        "https://walmart.wd504.myworkdayjobs.com", "WalmartExternal", "walmart", q,
        india_location_facet=_WALMART_INDIA_FACET,
        max_pages=12,
    )


# Uber — www.uber.com/api/loadSearchJobsResults filters by country=IND directly.
async def fetch_uber(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.uber.com/api/loadSearchJobsResults"
    body = {
        "params": {"location": [{"country": "IND"}], "query": query or ""},
        "page": 0,
        "limit": 100,
    }
    try:
        r = await client.post(
            url, json=body,
            headers={**DEFAULT_HEADERS, "Content-Type": "application/json", "x-csrf-token": "x"},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("uber failed: %s", e)
        return []
    out: list[Job] = []
    for j in (data.get("data", {}).get("results") or []):
        locs = j.get("allLocations") or []
        loc_str = ", ".join(
            x.get("city") or x.get("region") or x.get("countryName") or ""
            for x in locs if isinstance(x, dict)
        ).strip(", ")
        if not _looks_india(loc_str):
            continue
        jid = j.get("id")
        out.append(Job(
            company="Uber",
            title=j.get("title", ""),
            location=loc_str,
            url=f"https://www.uber.com/global/en/careers/list/{jid}/" if jid else "https://www.uber.com/global/en/careers/",
            posted_at=_parse_dt(j.get("updatedDate") or j.get("creationDate")),
            source="uber.com",
        ))
    return out


# Accenture — wd103 tenant, AccentureCareers site. India returns ~2000 jobs.
# locationsText is omitted; helper falls back to bulletFields[1].
_INDIA_COUNTRY_ID = "c4f78be1a8f14da0ab49ce1162348a5e"
async def fetch_accenture(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Accenture",
        "https://accenture.wd103.myworkdayjobs.com", "AccentureCareers", "accenture", query,
        india_location_facet=[_INDIA_COUNTRY_ID],
        max_pages=50,  # India has ~2000 jobs ⇒ ~100 pages of 20; cap at 50 for speed
    )


# DBS Bank — wd3 tenant, DBS_Careers site. India ~450 jobs.
async def fetch_dbs(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "DBS",
        "https://dbs.wd3.myworkdayjobs.com", "DBS_Careers", "dbs", query,
        india_location_facet=[_INDIA_COUNTRY_ID],
        max_pages=25,
    )


# Slack — lives on Salesforce's Workday tenant under the "Slack" site.
# Small global postings (~16 worldwide) so we rely on _looks_india filter.
async def fetch_slack(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Slack",
        "https://salesforce.wd12.myworkdayjobs.com", "Slack", "salesforce", query,
        max_pages=3,
    )


# Nike — Workday tenant `nike` (host wd1), site `nke`. Worldwide ~657 jobs.
# No reliable per-tenant India facet ID, so use _looks_india client-side.
async def fetch_nike(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "Nike",
        "https://nike.wd1.myworkdayjobs.com", "nke", "nike", query,
        max_pages=15,
    )


# GoDaddy — Greenhouse board `godaddy`. ~10 India jobs at probe time.
async def fetch_godaddy(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_greenhouse(client, "GoDaddy", "godaddy", query)


# NoBroker — SmartRecruiters company slug `nobroker`.
# Public API: https://api.smartrecruiters.com/v1/companies/{slug}/postings
async def fetch_nobroker(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_smartrecruiters(client, "NoBroker", "nobroker", query)


# NTT Data — Workday tenant `nttlimited` (wd3), site `NTT_Careers`. ~939 jobs.
# Discovered after the SPA target on careers-inc.nttdata.com was returning 0.
async def fetch_ntt_data(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_workday(
        client, "NTT Data",
        "https://nttlimited.wd3.myworkdayjobs.com", "NTT_Careers", "nttlimited", query,
        max_pages=20,
    )


# EXL Service — Oracle HCM at fa-ewjt-saasfaprod1.fa.ocs.oraclecloud.com, site CX_2.
# Discovered via DDG research; the public landing exlservice.com/careers redirects here.
async def fetch_exl(client: httpx.AsyncClient, query: str) -> list[Job]:
    base = "https://fa-ewjt-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    out: list[Job] = []
    seen_ids: set[str] = set()
    PAGE = 100
    for page in range(15):  # cap at 15 pages = 1500 reqs
        finder = f"findReqs;siteNumber=CX_2,keyword={query},sortBy=POSTING_DATES_DESC"
        params = {
            "onlyData": "true",
            "limit": PAGE,
            "offset": page * PAGE,
            "expand": "requisitionList.secondaryLocations,requisitionList.requisitionFlexFields",
            "finder": finder,
        }
        try:
            r = await client.get(base, params=params, headers=DEFAULT_HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            items = data.get("items") or []
            reqs = items[0].get("requisitionList", []) if items and isinstance(items[0], dict) else []
            if not reqs:
                break
            new_in_page = 0
            for j in reqs:
                jid = j.get("Id") or ""
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)
                new_in_page += 1
                loc = j.get("PrimaryLocation") or ""
                if not _looks_india(loc):
                    continue
                out.append(Job(
                    company="EXL",
                    title=j.get("Title", ""),
                    location=loc,
                    url=f"https://fa-ewjt-saasfaprod1.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_2/job/{jid}/" if jid else "https://www.exlservice.com/careers",
                    posted_at=_parse_dt(j.get("PostedDate")),
                    source="exlservice.com/careers",
                ))
            if new_in_page < PAGE:
                break  # last page
        except Exception as e:
            log.warning("exl (Oracle HCM) page %d failed: %s", page, e)
            break
    return out


# Adidas — Workable XML feed at careers.adidas-group.com/jobs/feed.xml (~5MB, all jobs).
# We stream-parse, filter by India location, then keyword-match titles client-side.
async def fetch_adidas(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://careers.adidas-group.com/jobs/feed.xml"
    out: list[Job] = []
    try:
        r = await client.get(url, headers={**DEFAULT_HEADERS, "Accept": "application/xml,text/xml"}, timeout=60)
        if r.status_code != 200:
            return out
        # Parse XML using stdlib; the feed is a flat list of <job> elements.
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.text)
        for j in root.iter("job"):
            country = (j.findtext("country") or "").strip()
            city = (j.findtext("city") or "").strip()
            loc = ", ".join(x for x in (city, country) if x) or country
            if country.lower() != "india" and not _looks_india(loc):
                continue
            title = (j.findtext("title") or "").strip()
            if query and not _matches_query(title, query):
                continue
            link = (j.findtext("url") or j.findtext("link") or "").strip()
            date_raw = (j.findtext("publication_date") or j.findtext("date") or "").strip()
            out.append(Job(
                company="Adidas",
                title=title,
                location=loc,
                url=link or "https://careers.adidas-group.com/",
                posted_at=_parse_dt(date_raw),
                source="careers.adidas-group.com",
            ))
    except Exception as e:
        log.warning("adidas (Workable XML) failed: %s", e)
    return out


# Docker — Ashby HQ at jobs.ashbyhq.com/docker; public API returns all jobs.
async def fetch_docker(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_ashby(client, "Docker", "docker", query)


# KPMG India — Oracle HCM at ejgk.fa.em2.oraclecloud.com, 5 candidate sites
# CX_1=KI, CX_2=KGS, CX_3=KGS variant, CX_1001=KI variant, CX_3001=KDN India
# Merge results across all sites (dedupe by Job Id).
async def fetch_kpmg(client: httpx.AsyncClient, query: str) -> list[Job]:
    base = "https://ejgk.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    out: list[Job] = []
    seen_ids: set[str] = set()
    for site in ("CX_1", "CX_2", "CX_3", "CX_1001", "CX_3001"):
        for page in range(8):  # cap 8 pages per site = 800 reqs
            params = {
                "onlyData": "true",
                "limit": 100,
                "offset": page * 100,
                "expand": "requisitionList.secondaryLocations,requisitionList.requisitionFlexFields",
                "finder": f"findReqs;siteNumber={site},keyword={query},sortBy=POSTING_DATES_DESC",
            }
            try:
                r = await client.get(base, params=params, headers=DEFAULT_HEADERS, timeout=20)
                r.raise_for_status()
                data = r.json()
                items = data.get("items") or []
                reqs = items[0].get("requisitionList", []) if items and isinstance(items[0], dict) else []
                if not reqs:
                    break
                new_in_page = 0
                for j in reqs:
                    jid = j.get("Id") or ""
                    if jid in seen_ids:
                        continue
                    seen_ids.add(jid)
                    new_in_page += 1
                    loc = j.get("PrimaryLocation") or ""
                    if not _looks_india(loc):
                        continue
                    out.append(Job(
                        company="KPMG",
                        title=j.get("Title", ""),
                        location=loc,
                        url=f"https://ejgk.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{site}/job/{jid}/" if jid else "https://kpmg.com/in/en/careers.html",
                        posted_at=_parse_dt(j.get("PostedDate")),
                        source=f"kpmg.com (Oracle HCM/{site})",
                    ))
                if new_in_page < 100:
                    break  # likely last page
            except Exception as e:
                log.warning("kpmg %s page %d failed: %s", site, page, e)
                break
    return out


async def _fetch_smartrecruiters(
    client: httpx.AsyncClient,
    company: str,
    slug: str,
    query: str,
) -> list[Job]:
    """Generic SmartRecruiters public API helper.

    Endpoint returns up to 100 postings per page. We page through everything,
    then filter to India locations client-side.
    """
    base = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    items: list[dict] = []
    offset = 0
    try:
        for _ in range(20):  # cap pagination at 2000 postings per company
            r = await client.get(base, params={"limit": 100, "offset": offset},
                                 headers=DEFAULT_HEADERS, timeout=20)
            if r.status_code != 200:
                break
            data = r.json()
            batch = data.get("content", []) or []
            if not batch:
                break
            items.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
    except Exception as e:
        log.warning("%s (smartrecruiters) failed: %s", company, e)
        return []

    out: list[Job] = []
    for j in items:
        loc = j.get("location") or {}
        city = loc.get("city") or ""
        region = loc.get("region") or ""
        country = loc.get("country") or ""
        full_loc = ", ".join(x for x in (city, region, country) if x) or country or "India"
        # SmartRecruiters country codes are ISO alpha-2 lowercase; check for "in"
        # but also fall back to the textual matcher.
        if (country or "").lower() != "in" and not _looks_india(full_loc):
            continue
        title = j.get("name") or ""
        if query and not _matches_query(title, query):
            continue
        uuid = j.get("uuid") or j.get("id") or ""
        out.append(Job(
            company=company,
            title=title,
            location=full_loc,
            url=f"https://jobs.smartrecruiters.com/{slug}/{uuid}",
            posted_at=_parse_dt(j.get("releasedDate") or j.get("createdOn")),
            source="smartrecruiters.com",
        ))
    return out


# ---------------------------------------------------------------------------
# Netflix (Eightfold-style API at explore.jobs.netflix.net)
# ---------------------------------------------------------------------------
async def fetch_netflix(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://explore.jobs.netflix.net/api/apply/v2/jobs"
    params = {"domain": "netflix.com", "query": query, "location": "India",
              "start": 0, "num": 50, "sort_by": "relevance"}
    try:
        r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("netflix failed: %s", e)
        return []
    out: list[Job] = []
    for j in data.get("positions", []):
        loc = j.get("location") or ", ".join(j.get("locations") or [])
        if not _looks_india(str(loc)):
            continue
        out.append(Job(
            company="Netflix",
            title=j.get("name", ""),
            location=str(loc),
            url=j.get("canonicalPositionUrl") or f"https://explore.jobs.netflix.net/careers/job/{j.get('id', '')}",
            posted_at=_parse_dt(j.get("t_create") or j.get("t_update")),
            source="explore.jobs.netflix.net",
        ))
    return out


# ---------------------------------------------------------------------------
# Atlassian (custom JSON endpoint backed by iCIMS)
# ---------------------------------------------------------------------------
async def fetch_atlassian(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.atlassian.com/endpoint/careers/listings"
    try:
        r = await client.get(url, headers=DEFAULT_HEADERS, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("atlassian failed: %s", e)
        return []
    if not isinstance(data, list):
        return []
    out: list[Job] = []
    for j in data:
        if not isinstance(j, dict):
            continue
        locs = j.get("locations") or []
        loc_str = " | ".join(locs) if isinstance(locs, list) else str(locs)
        if not _looks_india(loc_str):
            continue
        title = j.get("title", "")
        if query and not _matches_query(title, query):
            continue
        portal = j.get("portalJobPost") or {}
        out.append(Job(
            company="Atlassian",
            title=title,
            location=next((x for x in (locs if isinstance(locs, list) else [loc_str]) if "india" in x.lower()), loc_str),
            url=portal.get("portalUrl") or j.get("applyUrl") or f"https://www.atlassian.com/company/careers/all-jobs?id={j.get('id','')}",
            posted_at=_parse_dt(portal.get("updatedDate")),
            source="atlassian.com/careers",
        ))
    return out


# ---------------------------------------------------------------------------
# Round 8 — Tier 1/Tier 2 product, semis, finance, GCC sweep
# Endpoints verified via probe_atss.py + probe_round2.py.
# ---------------------------------------------------------------------------

# Workday-backed (one-liner wrappers around _fetch_workday)
async def fetch_zoom(client, q):
    return await _fetch_workday(client, "Zoom",
        "https://zoom.wd5.myworkdayjobs.com", "Zoom", "zoom", q)

async def fetch_crowdstrike(client, q):
    return await _fetch_workday(client, "CrowdStrike",
        "https://crowdstrike.wd5.myworkdayjobs.com", "crowdstrikecareers", "crowdstrike", q)

async def fetch_workday_inc(client, q):
    return await _fetch_workday(client, "Workday",
        "https://workday.wd5.myworkdayjobs.com", "Workday", "workday", q)

async def fetch_target(client, q):
    return await _fetch_workday(client, "Target",
        "https://target.wd5.myworkdayjobs.com", "targetcareers", "target", q)

async def fetch_novartis(client, q):
    return await _fetch_workday(client, "Novartis",
        "https://novartis.wd3.myworkdayjobs.com", "Novartis_Careers", "novartis", q)

async def fetch_intel(client, q):
    return await _fetch_workday(client, "Intel",
        "https://intel.wd1.myworkdayjobs.com", "External", "intel", q)

async def fetch_cadence(client, q):
    return await _fetch_workday(client, "Cadence",
        "https://cadence.wd1.myworkdayjobs.com", "External_Careers", "cadence", q)

async def fetch_boeing(client, q):
    return await _fetch_workday(client, "Boeing",
        "https://boeing.wd1.myworkdayjobs.com", "External_Careers", "boeing", q)

async def fetch_astrazeneca(client, q):
    return await _fetch_workday(client, "AstraZeneca",
        "https://astrazeneca.wd3.myworkdayjobs.com", "Careers", "astrazeneca", q)

# Greenhouse-backed
async def fetch_databricks(client, q): return await _fetch_greenhouse(client, "Databricks", "databricks", q)
async def fetch_stripe(client, q):     return await _fetch_greenhouse(client, "Stripe",     "stripe",     q)
async def fetch_postman(client, q):    return await _fetch_greenhouse(client, "Postman",    "postman",    q)
async def fetch_okta(client, q):       return await _fetch_greenhouse(client, "Okta",       "okta",       q)
async def fetch_twilio(client, q):     return await _fetch_greenhouse(client, "Twilio",     "twilio",     q)
async def fetch_datadog(client, q):    return await _fetch_greenhouse(client, "Datadog",    "datadog",    q)
async def fetch_phonepe(client, q):    return await _fetch_greenhouse(client, "PhonePe",    "phonepe",    q)

# Lever-backed
async def fetch_cred(client, q):       return await _fetch_lever(client, "CRED", "cred", q)

# SmartRecruiters-backed (note Bosch's slug is `boschgroup`, not `bosch`)
async def fetch_freshworks(client, q): return await _fetch_smartrecruiters(client, "Freshworks", "freshworks", q)
async def fetch_bosch(client, q):      return await _fetch_smartrecruiters(client, "Bosch",      "boschgroup", q)


# GitHub — Phenom-style API at www.github.careers (GitHub.com is owned by
# Microsoft but the GitHub-branded careers site remains separate).
async def fetch_github(client: httpx.AsyncClient, query: str) -> list[Job]:
    url = "https://www.github.careers/api/jobs"
    out: list[Job] = []
    for page in range(8):  # ~80 results; totalCount India ~78
        params = {
            "stretchUnit": "MILES",
            "radius": 0,
            "location": "India",
            "page": page + 1,
            "limit": 10,
        }
        try:
            r = await client.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("github failed: %s", e)
            break
        jobs = data.get("jobs") or []
        if not jobs:
            break
        for wrapper in jobs:
            j = wrapper.get("data") if isinstance(wrapper, dict) and "data" in wrapper else wrapper
            if not isinstance(j, dict):
                continue
            loc = j.get("full_location") or j.get("location_name") or j.get("short_location") or j.get("country") or ""
            if not _looks_india(str(loc)):
                continue
            title = j.get("title") or ""
            if query and not _matches_query(title, query):
                continue
            slug = j.get("slug") or j.get("req_id") or ""
            apply_url = j.get("apply_url") or (f"https://www.github.careers/careers-home/jobs/{slug}" if slug else "https://www.github.careers/")
            out.append(Job(
                company="GitHub",
                title=title,
                location=str(loc) or "India",
                url=apply_url,
                posted_at=_parse_dt(j.get("posted_date") or j.get("create_date")),
                source="github.careers",
            ))
        if len(jobs) < 10:
            break
    return out


# Oracle — Oracle's own HCM at eeho.fa.us2.oraclecloud.com. They use a
# small set of siteNumbers; CX_1 is the global external site.
async def fetch_oracle(client: httpx.AsyncClient, query: str) -> list[Job]:
    base = "https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    out: list[Job] = []
    seen: set[str] = set()
    for site in ("CX_1", "CX_2"):
        for page in range(10):
            params = {
                "onlyData": "true",
                "limit": 100,
                "offset": page * 100,
                "expand": "requisitionList.secondaryLocations,requisitionList.requisitionFlexFields",
                "finder": f"findReqs;siteNumber={site},keyword={query},sortBy=POSTING_DATES_DESC",
            }
            try:
                r = await client.get(base, params=params, headers=DEFAULT_HEADERS, timeout=20)
                r.raise_for_status()
                data = r.json()
                items = data.get("items") or []
                reqs = items[0].get("requisitionList", []) if items and isinstance(items[0], dict) else []
                if not reqs:
                    break
                added = 0
                for j in reqs:
                    jid = j.get("Id") or ""
                    if jid in seen:
                        continue
                    seen.add(jid)
                    added += 1
                    loc = j.get("PrimaryLocation") or ""
                    if not _looks_india(loc):
                        continue
                    out.append(Job(
                        company="Oracle",
                        title=j.get("Title", ""),
                        location=loc,
                        url=f"https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{site}/job/{jid}/" if jid else "https://www.oracle.com/careers/",
                        posted_at=_parse_dt(j.get("PostedDate")),
                        source=f"oracle.com (Oracle HCM/{site})",
                    ))
                if added < 100:
                    break
            except Exception as e:
                log.warning("oracle %s page %d failed: %s", site, page, e)
                break
    return out


# ---------------------------------------------------------------------------
# Round 9 — replace Playwright SPA targets with HTTP scrapers where the
# careers site SSRs jobs into the HTML (TalentBrew, Eightfold JSON-LD, Phenom).
# ---------------------------------------------------------------------------

def _parse_jobposting_jsonld(html: str, company: str, source: str) -> list[Job]:
    """Extract every <script type='application/ld+json'>…JobPosting…</script> block."""
    out: list[Job] = []
    seen: set[str] = set()
    for m in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, flags=re.S,
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                continue
            title = item.get("title") or ""
            if not title:
                continue
            loc_obj = item.get("jobLocation") or {}
            loc_list = loc_obj if isinstance(loc_obj, list) else [loc_obj]
            loc_text = ""
            country_in_india = False
            for lo in loc_list:
                if not isinstance(lo, dict):
                    continue
                addr = lo.get("address") or {}
                if isinstance(addr, dict):
                    country = addr.get("addressCountry")
                    if isinstance(country, dict):
                        country = country.get("name") or ""
                    if str(country).upper() in ("IN", "INDIA"):
                        country_in_india = True
                    bits = [addr.get("addressLocality"), addr.get("addressRegion"), country]
                    text = ", ".join(b for b in bits if b)
                    if text and not loc_text:
                        loc_text = text
            if not (country_in_india or _looks_india(loc_text)):
                continue
            url = item.get("url") or ""
            if url in seen:
                continue
            seen.add(url)
            out.append(Job(
                company=company,
                title=title,
                location=loc_text or "India",
                url=url,
                posted_at=_parse_dt(item.get("datePosted")),
                source=source,
            ))
    return out


async def _fetch_eightfold_html(
    client: httpx.AsyncClient, company: str, base: str, pid: str, query: str,
    domain: str, source: str, max_pages: int = 1,
) -> list[Job]:
    """Eightfold-hosted SPAs only SSR the *spotlight* role as JobPosting JSON-LD
    — the full result list is hydrated client-side. Fetching page 0 reliably
    yields one canonical role per query; pagination returns the same spotlight
    on every page, so we don't iterate."""
    out: list[Job] = []
    seen_titles: set[str] = set()
    for page in range(max_pages):
        params = {
            "query": query, "start": page * 10, "location": "India",
            "pid": pid, "sort_by": "timestamp", "filter_include_remote": "1",
        }
        try:
            r = await client.get(base, params=params, headers=DEFAULT_HEADERS, timeout=25)
            r.raise_for_status()
        except Exception as e:
            log.warning("%s eightfold page %d failed: %s", company, page, e)
            break
        before = len(out)
        for j in _parse_jobposting_jsonld(r.text, company, source):
            if j.title in seen_titles:
                continue
            seen_titles.add(j.title)
            out.append(j)
        if len(out) == before:
            break
    return out


async def fetch_qualcomm(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_eightfold_html(
        client, "Qualcomm",
        "https://careers.qualcomm.com/careers",
        pid="446716678737", query=query, domain="qualcomm.com",
        source="careers.qualcomm.com",
    )


async def _fetch_talentbrew_html(
    client: httpx.AsyncClient, company: str, base: str, org_id: str,
    query: str, source: str, job_path_prefix: str = "/job/", max_pages: int = 5,
) -> list[Job]:
    """TalentBrew search pages SSR job anchors:
        <a class="...job-link" href="{job_path_prefix}{city}/{slug}/{org_id}/{job_id}"
           data-job-id="{job_id}">{title}</a>
        ...<span class="job-location"> {location} </span>...
    """
    out: list[Job] = []
    seen: set[str] = set()
    headers = {**DEFAULT_HEADERS, "Accept-Language": "en-US,en;q=0.9"}
    for page in range(1, max_pages + 1):
        params = {"k": query, "Keywords": query, "Location": "India", "p": page}
        url = base.rstrip("/") + ("/" if not base.endswith("/") else "")
        # TalentBrew search results live at /search-jobs (with optional /{kw}/{loc}/...)
        try:
            r = await client.get(
                base, params=params, headers=headers, timeout=25, follow_redirects=True,
            )
            r.raise_for_status()
        except Exception as e:
            log.warning("%s talentbrew page %d failed: %s", company, page, e)
            break
        html = r.text
        # Anchor pattern is class-agnostic across TalentBrew tenants: the link
        # always has both ``href="/job/..."`` (or ``/en/job/...``) and a
        # ``data-job-id="<digits>"``. Synopsys/Moody's wrap their title in
        # <h2>; Optum's same template uses <h2> too — just with a different
        # outer class (``brand-facet brand-facet__optum``).
        anchor_re = re.compile(
            r'<a[^>]+href="(?P<href>/(?:en/)?job/[^"]+/\d+)"[^>]+data-job-id="(?P<jid>\d+)"[^>]*>'
            r'(?P<inner>.*?)</a>',
            flags=re.S,
        )
        matches = list(anchor_re.finditer(html))
        if not matches:
            break
        added = 0
        for m in matches:
            jid = m.group("jid")
            if jid in seen:
                continue
            href = m.group("href")
            # Title is either inside <h2>…</h2> inside the anchor, or the anchor's text.
            inner = m.group("inner")
            title_m = re.search(r"<h\d[^>]*>(.*?)</h\d>", inner, flags=re.S)
            title_raw = title_m.group(1) if title_m else inner
            title = re.sub(r"<[^>]+>", "", title_raw).strip()
            title = re.sub(r"\s+", " ", title)
            if not title:
                continue
            # Location: the .job-location span sits INSIDE the anchor on Optum
            # (next to the <h2>) and AFTER the anchor on Synopsys/Moody's.
            loc = ""
            loc_inside = re.search(
                r'class="[^"]*job-location[^"]*"[^>]*>\s*([^<]+)<', inner,
            )
            if loc_inside:
                loc = loc_inside.group(1).strip()
            else:
                tail = html[m.end():m.end() + 1500]
                loc_m = re.search(
                    r'class="[^"]*(?:job-location|search-results-list__job-info\s+job-location)[^"]*"[^>]*>\s*([^<]+)<',
                    tail,
                )
                loc = loc_m.group(1).strip() if loc_m else ""
            loc = re.sub(r"\s+", " ", loc)
            # Honor server-side ?Location=India filter, but skip junk locs from
            # the "featured jobs" sidebar (Heredia etc. in MDY).
            if loc and not _looks_india(loc):
                continue
            seen.add(jid)
            added += 1
            full_url = href if href.startswith("http") else (
                base.split("/search-jobs")[0].rstrip("/") + href
            )
            out.append(Job(
                company=company, title=title,
                location=loc or "India",
                url=full_url, posted_at=None, source=source,
            ))
        if added == 0:
            break
    return out


async def fetch_synopsys(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_talentbrew_html(
        client, "Synopsys",
        base="https://careers.synopsys.com/search-jobs",
        org_id="44408", query=query,
        source="careers.synopsys.com",
    )


async def fetch_optum(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_talentbrew_html(
        client, "Optum",
        base="https://careers.unitedhealthgroup.com/search-jobs",
        org_id="34088", query=query,
        source="careers.unitedhealthgroup.com",
    )


async def fetch_moodys(client: httpx.AsyncClient, query: str) -> list[Job]:
    return await _fetch_talentbrew_html(
        client, "Moody's",
        base="https://careers.moodys.com/en/search-jobs",
        org_id="49841", query=query, job_path_prefix="/en/job/",
        source="careers.moodys.com",
    )


async def fetch_bcg(client: httpx.AsyncClient, query: str) -> list[Job]:
    """BCG (Phenom CMS) inlines results in eagerLoadRefineSearch.data.jobs[]."""
    url = "https://careers.bcg.com/global/en/search-results"
    params = {"keywords": query}
    headers = {**DEFAULT_HEADERS, "Accept-Language": "en-US,en;q=0.9"}
    try:
        r = await client.get(url, params=params, headers=headers, timeout=25)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        log.warning("bcg failed: %s", e)
        return []
    # The eagerLoadRefineSearch object contains the first page of jobs as a
    # raw JSON island. Find its opening brace and walk the balanced braces.
    key = '"eagerLoadRefineSearch"'
    key_at = html.find(key)
    if key_at == -1:
        return []
    obj_start = html.find('{', key_at)
    if obj_start == -1:
        return []
    depth = 0
    obj_end = -1
    i = obj_start
    while i < len(html):
        c = html[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                obj_end = i + 1
                break
        i += 1
    if obj_end == -1:
        return []
    try:
        block = json.loads(html[obj_start:obj_end])
    except Exception:
        return []
    jobs = ((block.get("data") or {}).get("jobs")) or []
    out: list[Job] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        country = j.get("country") or ""
        city = j.get("city") or j.get("location") or ""
        loc = ", ".join(x for x in (city, country) if x).strip(", ")
        if not _looks_india(loc + " " + country):
            continue
        jid = j.get("jobId") or j.get("jobSeqNo") or ""
        title = j.get("title") or j.get("jobTitle") or ""
        if not title:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        href = f"https://careers.bcg.com/global/en/job/{jid}/{slug}" if jid else "https://careers.bcg.com/global/en/search-results"
        out.append(Job(
            company="BCG",
            title=title,
            location=loc or "India",
            url=href,
            posted_at=_parse_dt(j.get("postedDate") or j.get("postingStartDate")),
            source="careers.bcg.com",
        ))
    return out



# Public registry consumed by the aggregator.
SCRAPERS = {
    "Amazon": fetch_amazon,
    "Apple": fetch_apple,
    "Microsoft": fetch_microsoft,
    "Google": fetch_google,
    "Meta": fetch_meta,
    "Mastercard": fetch_mastercard,
    "Visa": fetch_visa,
    "Citi": fetch_citi,
    "JPMorgan Chase": fetch_jpmorgan,
    # Goldman Sachs moved to Playwright SPA (higher.gs.com is now Apollo-only).
    "Morgan Stanley": fetch_morgan_stanley,
    "American Express": fetch_amex,
    # Round 1 additions
    "MongoDB": fetch_mongodb,
    "Groww": fetch_groww,
    "Airbnb": fetch_airbnb,
    "Anthropic": fetch_anthropic,
    "Cloudflare": fetch_cloudflare,
    "Reddit": fetch_reddit,
    "Discord": fetch_discord,
    "Jane Street": fetch_jane_street,
    "Spotify": fetch_spotify,
    "Philips": fetch_philips,
    "Deutsche Bank": fetch_deutsche_bank,
    "Autodesk": fetch_autodesk,
    "Salesforce": fetch_salesforce,
    "BlackRock": fetch_blackrock,
    "NVIDIA": fetch_nvidia,
    # Round 2 additions
    "Razorpay": fetch_razorpay,
    "Paytm": fetch_paytm,
    "xAI": fetch_xai,
    "OpenAI": fetch_openai,
    "Notion": fetch_notion,
    "Confluent": fetch_confluent,
    "Snowflake": fetch_snowflake,
    "Expedia": fetch_expedia,
    "Zendesk": fetch_zendesk,
    "Broadcom": fetch_broadcom,
    "PayPal": fetch_paypal,
    "Samsung": fetch_samsung,
    # Round 3 — custom JSON endpoints
    "Netflix": fetch_netflix,
    "Atlassian": fetch_atlassian,
    # Round 4 — verified via probe_ats
    "Adobe": fetch_adobe,
    "Disney": fetch_disney,
    "Warner Bros Discovery": fetch_warner_bros,
    "PwC": fetch_pwc,
    "Zscaler": fetch_zscaler,
    "Meesho": fetch_meesho,
    "PepsiCo": fetch_pepsico,
    "Walmart": fetch_walmart,
    "Uber": fetch_uber,
    "Accenture": fetch_accenture,
    "DBS": fetch_dbs,
    "Slack": fetch_slack,
    # Round 5 — wishlist sweep
    "Nike": fetch_nike,
    "GoDaddy": fetch_godaddy,
    "NoBroker": fetch_nobroker,
    # Round 6 — wishlist sweep, ATS discovered via Google research
    "NTT Data": fetch_ntt_data,
    "EXL": fetch_exl,
    "Adidas": fetch_adidas,
    # Round 7 — final wishlist gaps
    "Docker": fetch_docker,
    "KPMG": fetch_kpmg,
    # Round 9 — replaced Playwright targets with HTTP scrapers.
    "BCG": fetch_bcg,
    # Qualcomm reverted to Playwright in round 10: Eightfold SSR yields only
    # one spotlight role per query, whereas the rendered SPA exposes ~18.
    "Synopsys": fetch_synopsys,
    "Optum": fetch_optum,
    "Moody's": fetch_moodys,
    # Round 8 — Tier 1/Tier 2 product, semis, finance, GCC sweep
    # Workday
    "Zoom": fetch_zoom,
    "CrowdStrike": fetch_crowdstrike,
    "Workday": fetch_workday_inc,
    "Target": fetch_target,
    "Novartis": fetch_novartis,
    "Intel": fetch_intel,
    "Cadence": fetch_cadence,
    "Boeing": fetch_boeing,
    "AstraZeneca": fetch_astrazeneca,
    # Greenhouse
    "Databricks": fetch_databricks,
    "Stripe": fetch_stripe,
    "Postman": fetch_postman,
    "Okta": fetch_okta,
    "Twilio": fetch_twilio,
    "Datadog": fetch_datadog,
    "PhonePe": fetch_phonepe,
    # Lever
    "CRED": fetch_cred,
    # SmartRecruiters
    "Freshworks": fetch_freshworks,
    "Bosch": fetch_bosch,
    # Bespoke
    "GitHub": fetch_github,
    "Oracle": fetch_oracle,
}


# ---------------------------------------------------------------------------
# SPA scrapers — each is a thin wrapper around the Playwright background cache
# ---------------------------------------------------------------------------
def _make_spa_wrapper(company: str):
    from backend.scrapers import spa_runner

    async def _fn(client: httpx.AsyncClient, query: str) -> list[Job]:
        jobs = spa_runner.cached_jobs(company)
        if query:
            return [j for j in jobs if _matches_query(j.title, query)]
        return jobs
    _fn.__name__ = f"fetch_spa_{company.lower().replace(' ', '_')}"
    return _fn


# Register each Playwright-backed company. Importing here keeps it lazy
# enough that companies.py is still usable if Playwright is missing.
try:
    from backend.scrapers.spa_runner import SPA_COMPANIES as _SPA_COMPANIES
    for _name in _SPA_COMPANIES:
        SCRAPERS[_name] = _make_spa_wrapper(_name)
except Exception as _e:  # pragma: no cover
    log.warning("SPA scrapers not registered: %s", _e)
