"""
SPA scrapers using Playwright.

Architecture: a background task wakes every REFRESH_INTERVAL, opens each SPA
careers page in a headless Chromium, extracts JobPosting JSON-LD blocks and/or
runs a per-company JS extractor, then stores the result in _SPA_CACHE keyed
by company name. The /api/jobs scrapers in companies.py are simple wrappers
that read from _SPA_CACHE — so user requests never block on Playwright.

This module is fully optional. If Playwright isn't installed or Chromium fails
to launch, the cache stays empty and the corresponding pills show 0 — no error.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from dateutil import parser as dateparser

from backend.models import Job

log = logging.getLogger("spa")

REFRESH_INTERVAL_SECONDS = 15 * 60  # 15 min
PAGE_LOAD_TIMEOUT_MS = 30_000
PER_COMPANY_TIMEOUT_S = 45  # hard cap per company per refresh

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# company -> list[Job]
_SPA_CACHE: dict[str, list[Job]] = {}
# company -> last refresh epoch seconds (success or failure)
_SPA_LAST_RUN: dict[str, float] = {}

_INDIA_TOKENS = (
    "india", "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "chennai",
    "gurgaon", "gurugram", "noida", "new delhi", "delhi", "kolkata",
    "ahmedabad", "jaipur", "kochi",
)


def _looks_india(s: str | None) -> bool:
    if not s:
        return False
    s = s.lower()
    return any(t in s for t in _INDIA_TOKENS)


def _parse_dt(value) -> Optional[datetime]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
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


# ---------------------------------------------------------------------------
# Generic JSON-LD JobPosting extractor (works for any SPA that ships
# schema.org JobPosting blocks — surprisingly common: Apple, Walmart, MMT, etc.)
# ---------------------------------------------------------------------------
async def _extract_json_ld(page) -> list[dict]:
    """Return a list of raw JobPosting dicts found in any <script type=ld+json>."""
    scripts = await page.query_selector_all('script[type="application/ld+json"]')
    out: list[dict] = []
    for s in scripts:
        try:
            txt = await s.inner_text()
            data = json.loads(txt)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if item.get("@type") == "JobPosting":
                out.append(item)
            # ItemList wrapping JobPostings
            if item.get("@type") == "ItemList":
                for el in item.get("itemListElement") or []:
                    el_item = el.get("item") if isinstance(el, dict) else None
                    if isinstance(el_item, dict) and el_item.get("@type") == "JobPosting":
                        out.append(el_item)
    return out


def _job_from_ld(company: str, source: str, raw: dict) -> Optional[Job]:
    title = raw.get("title") or ""
    if not title:
        return None
    loc_obj = raw.get("jobLocation") or {}
    loc_text = ""
    if isinstance(loc_obj, list):
        loc_obj = loc_obj[0] if loc_obj else {}
    if isinstance(loc_obj, dict):
        addr = loc_obj.get("address") or {}
        if isinstance(addr, dict):
            loc_text = ", ".join(
                str(v) for v in (addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry"))
                if v
            )
        if not loc_text:
            loc_text = loc_obj.get("name") or ""
    if not _looks_india(loc_text):
        return None
    return Job(
        company=company,
        title=title,
        location=loc_text or "India",
        url=raw.get("url") or raw.get("hiringOrganization", {}).get("sameAs", "") or "",
        posted_at=_parse_dt(raw.get("datePosted") or raw.get("validThrough")),
        source=source,
    )


# ---------------------------------------------------------------------------
# Per-company SPA targets
#
# Each entry: (company_label, listing_url, custom_extractor or None)
# A custom_extractor is async (page) -> list[Job]. If None, generic JSON-LD is used.
# ---------------------------------------------------------------------------
async def _extract_swiggy(page) -> list[Job]:
    """Swiggy careers list jobs as <a class="job-card">. Walk DOM."""
    items = await page.evaluate("""
() => {
  const out = [];
  document.querySelectorAll('a[href*="/job/"]').forEach(a => {
    const t = a.textContent.replace(/\\s+/g,' ').trim();
    if (!t) return;
    const card = a.closest('article, li, div') || a;
    const loc = (card.querySelector('[class*=location], [class*=Location]') || {textContent:''}).textContent.trim();
    out.push({title: t, location: loc, url: a.href});
  });
  return out;
}
""")
    out: list[Job] = []
    seen: set[str] = set()
    for it in items:
        if not _looks_india(it.get("location", "") + " India"):  # Swiggy is India-only company; treat as India
            it_loc = it.get("location") or "India"
        else:
            it_loc = it.get("location") or "India"
        url = it.get("url", "")
        if url in seen:
            continue
        seen.add(url)
        out.append(Job(
            company="Swiggy", title=it.get("title", ""), location=it_loc,
            url=url, posted_at=None, source="careers.swiggy.com",
        ))
    return out


async def _extract_zomato(page) -> list[Job]:
    """Zomato (now Eternal) careers — anchors leading to /careers/job/<slug>."""
    items = await page.evaluate("""
() => {
  const out = [];
  document.querySelectorAll('a[href*="/job/"], a[href*="/careers/"]').forEach(a => {
    const t = a.textContent.replace(/\\s+/g,' ').trim();
    if (!t || t.length > 200) return;
    const card = a.closest('article, li, div') || a;
    const loc = (card.querySelector('[class*=location], [class*=Location]') || {textContent:''}).textContent.trim();
    out.push({title: t, location: loc, url: a.href});
  });
  return out;
}
""")
    out: list[Job] = []
    seen: set[str] = set()
    for it in items:
        url = it.get("url", "")
        if url in seen or "/job" not in url:
            continue
        loc = it.get("location") or "India"
        if not _looks_india(loc):
            continue
        seen.add(url)
        out.append(Job(
            company="Zomato", title=it.get("title", ""), location=loc,
            url=url, posted_at=None, source="eternal.com/careers",
        ))
    return out


async def _extract_apple(page) -> list[Job]:
    """Apple jobs list — they embed a JSON state in window.appData."""
    data = await page.evaluate("() => window.appData || window.__INITIAL_STATE__ || null")
    out: list[Job] = []
    if not data:
        return out
    # Walk for any list of role-like objects
    def walk(node):
        if isinstance(node, dict):
            if node.get("postingTitle") and node.get("locations"):
                yield node
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)
    seen: set[str] = set()
    for r in walk(data):
        title = r.get("postingTitle", "")
        locs = r.get("locations") or []
        loc_text = ", ".join(
            (l.get("name") or l.get("city") or "") if isinstance(l, dict) else str(l)
            for l in locs
        )
        if not _looks_india(loc_text):
            continue
        pid = r.get("positionId") or r.get("id") or ""
        if pid in seen:
            continue
        seen.add(pid)
        out.append(Job(
            company="Apple", title=title, location=loc_text,
            url=f"https://jobs.apple.com/en-us/details/{pid}" if pid else "https://jobs.apple.com",
            posted_at=_parse_dt(r.get("postDateInGMT") or r.get("postDate")),
            source="jobs.apple.com",
        ))
    return out


async def _extract_goldman(page) -> list[Job]:
    """higher.gs.com (Goldman Sachs careers) renders results client-side via
    Apollo. After hydration, role cards become anchors of the form
    ``/roles/<numeric-id>``. Pull title from the anchor text and the location
    from the nearest visible cell (the card text usually contains the city)."""
    try:
        await page.wait_for_selector('a[href*="/roles/"]', timeout=12000)
    except Exception:
        return []
    items = await page.evaluate(r"""
() => {
  const out = [];
  const seen = new Set();
  document.querySelectorAll('a[href*="/roles/"]').forEach(a => {
    const href = a.href;
    if (!href || seen.has(href)) return;
    if (!/\/roles\/\d+/.test(href)) return;
    const title = (a.textContent || '').replace(/\s+/g, ' ').trim();
    if (!title || title.length < 4) return;
    seen.add(href);
    const card = a.closest('tr, li, article, [role="row"], div') || a.parentElement;
    const ctx = card ? (card.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 400) : '';
    out.push({title, href, ctx});
  });
  return out.slice(0, 200);
}
""")
    out: list[Job] = []
    seen: set[str] = set()
    for it in items or []:
        href = it.get("href") or ""
        title = it.get("title") or ""
        ctx = it.get("ctx") or ""
        if not _looks_india(ctx) and not _looks_india(title):
            continue
        loc = "India"
        ctx_l = ctx.lower()
        for tok in _INDIA_TOKENS:
            if tok in ctx_l and tok != "india":
                loc = tok.title()
                break
        if href in seen:
            continue
        seen.add(href)
        out.append(Job(
            company="Goldman Sachs", title=title, location=loc,
            url=href, posted_at=None, source="higher.gs.com",
        ))
    return out


# Generic anchor-based fallback. Pulls anchors whose href looks like a job
# detail page (contains /jobs/<id>, /details/<id>, /position/<id>, etc.) and
# uses the anchor text as title. Location is read from nearby DOM if present;
# otherwise falls back to "India" only when the company is known India-only
# (whole-site India presence). We keep the company filter loose because many
# in-house SPAs render mixed locations and we just want anything with India
# tokens in title/anchor/parent.
async def _extract_anchor_jobs(page, company: str, source_url: str) -> list[Job]:
    try:
        items = await page.evaluate(r"""
() => {
  const sel = 'a[href*="/jobs/"], a[href*="/job/"], a[href*="/details/"],'
            + 'a[href*="/positions/"], a[href*="/position/"], a[href*="/openings/"],'
            + 'a[href*="/opening/"], a[href*="/career/"], a[href*="/role/"],'
            + 'a[href*="/vacancy/"]';
  const out = [];
  const seen = new Set();
  document.querySelectorAll(sel).forEach(a => {
    const href = a.href;
    if (!href || seen.has(href)) return;
    // Skip generic landing/list pages
    if (/\/(jobs|careers|openings|positions)\/?(\?|#|$)/i.test(href)) return;
    // Skip Apple-style helper anchors that aren't real job posts
    if (/\/locationPicker$|\/locationpicker$|\/teamPicker$|\/searchByTeam/i.test(href)) return;
    const title = (a.textContent || '').replace(/\s+/g, ' ').trim();
    if (!title || title.length < 4 || title.length > 200) return;
    // Heuristic: title should look job-like (contain a noun word, not just "Apply")
    if (/^(apply|view|see|read|details|more|where|see full role description)$/i.test(title)) return;
    if (/^see full role description$/i.test(title)) return;
    seen.add(href);
    const card = a.closest('article, li, tr, .job, [class*="job"], [class*="card"], div') || a.parentElement;
    const ctx = card ? (card.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 400) : '';
    out.push({title, href, ctx});
  });
  return out.slice(0, 200);
}
""")
    except Exception:
        return []
    if not items:
        return []
    out: list[Job] = []
    seen: set[str] = set()
    # Some SPAs (e.g. AngelOne, Cars24 etc.) are India-only sites — their
    # anchor parents don't repeat "India" but the whole page is implicitly
    # India. We also accept anchors whose href encodes an India locale
    # (e.g. jobs.apple.com/en-in/details/...).
    india_url_markers = ("/en-in/", "/en_in/", "/in/", "/india/", "-india/", "/india-")
    for it in items:
        href = it.get("href") or ""
        title = it.get("title") or ""
        ctx = (it.get("ctx") or "").lower()
        href_l = href.lower()
        is_india_url = any(m in href_l for m in india_url_markers)
        text_for_loc = (title + " " + ctx).lower()
        if not (is_india_url or _looks_india(text_for_loc)):
            continue
        # Try to pull a city out of the context
        loc = "India"
        for tok in _INDIA_TOKENS:
            if tok in ctx and tok != "india":
                loc = tok.title()
                break
        if href in seen:
            continue
        seen.add(href)
        out.append(Job(
            company=company, title=title, location=loc,
            url=href, posted_at=None,
            source=source_url.split("?")[0],
        ))
    return out


SPA_TARGETS: list[tuple[str, str, Optional[Callable]]] = [
    # Apple moved to HTTP scraper in companies.py (SSR HTML has __ACGH_DATA__ JSON island).
    ("Swiggy", "https://careers.swiggy.com/jobs", _extract_swiggy),
    ("Flipkart", "https://flipkart.turbohire.co/careerpage/4d757ba0-3d57-448a-b82c-238ed87ac90f", None),
    ("Cars24", "https://careers.cars24.com/", None),
    # Round 4 — in-house SPAs that produce anchors after JS hydration.
    ("Cisco", "https://jobs.cisco.com/jobs/SearchJobs/?listFilterMode=1&31813=%5B153%5D", None),
    # ServiceNow stays Playwright (httpx returns 403).
    # ServiceNow — country=India is the only filter that consistently
    # narrows results without hiding too many roles after hydration.
    ("ServiceNow", "https://careers.servicenow.com/jobs/?country=India&pagesize=50", None),
    ("GE Healthcare", "https://careers.gehealthcare.com/global/en/search-results?qcountry=India", None),
    # BCG moved to HTTP scraper (Phenom eagerLoadRefineSearch JSON island).
    ("Urban Company", "https://careers.urbancompany.com/", None),
    # Round 5 — Tier 3 SPA that yielded real jobs through the generic fallbacks.
    ("Infosys", "https://career.infosys.com/joblist?country=India", None),
    # Round 6 — wishlist sweep: HTML careers pages with no JSON ATS endpoint.
    # Many of these are "best effort" -- the generic anchor extractor may yield
    # 0 jobs for SuccessFactors / heavy SPAs, but the cost of trying is bounded
    # by PER_COMPANY_TIMEOUT_S so failures don't block the rest of the loop.
    # Tiger Analytics — the corporate page hides job titles outside any
    # heading/card we can reliably target, so we keep the sensehq mirror.
    ("Tiger Analytics", "https://tiger-analytics.sensehq.com/careers", None),
    ("Juspay", "https://juspay.io/careers", None),
    ("Nykaa", "https://careers.nykaa.com/", None),
    # Optum moved to HTTP scraper (TalentBrew SSR anchors).
    ("FedEx", "https://careers.fedex.com/fedex/jobs?location=India", None),
    ("Bain", "https://www.bain.com/careers/find-a-role/?filters=offices%28274%2C276%2C275%29%7C", None),
    ("Bank of America", "https://careers.bankofamerica.com/en-us/job-search?ref=country&country=India", None),
    ("Deloitte", "https://southasiacareers.deloitte.com/go/Deloitte-India/718244/", None),
    # Moody's moved to HTTP scraper (TalentBrew SSR anchors).
    # Round 7 — replaced broken HTTP scrapers (their JSON APIs moved/blocked).
    # Microsoft moved to HTTP Eightfold scraper (apply.careers.microsoft.com).
    # Meta blocks raw httpx (returns 1.5KB anti-bot shell) and rotates GraphQL
    # doc_ids; Playwright with a real Chromium UA gets the rendered page.
    ("Meta", "https://www.metacareers.com/jobs?offices[0]=India", None),
    # Round 8 — SPA fallbacks for companies whose HTTP probes returned no clean
    # JSON ATS (Darwinbox SPAs, custom in-house boards, Phenom anti-bot, etc.).
    # Each is best-effort: 45s timeout + generic anchor extractor; failures
    # silently return 0 and never block other companies.
    ("Zoho", "https://careers.zohocorp.com/jobs/Careers", None),
    ("Lenskart", "https://careers.lenskart.com/", None),
    ("DE Shaw", "https://www.deshawindia.com/careers", None),
    ("SAP", "https://jobs.sap.com/search-jobs/India/results", None),
    ("PharmEasy", "https://pharmeasy.in/careers", None),
    ("Ola Electric", "https://www.olaelectric.com/careers", None),
    ("Delhivery", "https://www.delhivery.com/careers", None),
    ("Dream11", "https://careers.dream11.com/", None),
    ("Honeywell", "https://careers.honeywell.com/global/en/search-results?qcountry=India", None),
    # Synopsys moved to HTTP scraper (TalentBrew SSR anchors).
    # Qualcomm — Eightfold SSR returns only one spotlight role; the rendered
    # SPA at this URL exposes per-role anchors with /careers/job/<id>.
    ("Qualcomm", "https://careers.qualcomm.com/careers?location=India&pid=446716678737&sort_by=timestamp", None),
    ("Wells Fargo", "https://www.wellsfargojobs.com/en/search-jobs/India", None),
    ("Fidelity", "https://jobs.fidelity.com/en/search-jobs/India", None),
    # Goldman Sachs — higher.gs.com is now a client-rendered Apollo SPA, so
    # the previous __NEXT_DATA__ scraper returns an empty skeleton. Use the
    # custom hydration-aware extractor to walk the rendered role anchors.
    ("Goldman Sachs", "https://higher.gs.com/results?sort=RELEVANCE&LOCATION_TAG=India", _extract_goldman),
    # Removed in Round 6 verification sweep:
    #   PaisaBazaar — no ATS at all (email-only: careers+tech@paisabazaar.com).
    #   Cleartrip — shares the Flipkart TurboHire instance, already covered by
    #     the Flipkart SPA target (https://flipkart.turbohire.co/...).
    #   NTT Data — moved to HTTP (Workday tenant nttlimited, ~939 jobs).
    #   Adidas — moved to HTTP (Workable XML feed, full job list).
    #   EXL — moved to HTTP (Oracle HCM siteNumber=CX_2).
    # Removed (returned 0 jobs after Playwright + 4-tier anchor/XHR extraction):
    #   Zomato (HTTP/2 anti-bot), MakeMyTrip (frame detach / heavy SPA),
    #   Myntra (empty 2KB shell),
    #   AngelOne (Open Positions empty until user clicks),
    #   Porter/Rapido/BigBasket (Darwinbox SPAs — Angular bundle, no anchors),
    #   KPMG India (Oracle HCM SPA — timeout),
    #   Capital One India / Barclays India (TalentBrew — render skipped),
    #   Cognizant / Wipro (SuccessFactors anti-bot or empty render),
    #   HSBC/Standard Chartered/Volvo/Agoda/LinkedIn
    #   (all need user interaction or session cookies before jobs render).
    #   Round-6 also-tried-and-failed-with-zero-anchors:
    #     BookMyShow (403 anti-bot), Deloitte (apply.deloitte.com SPA),
    #     EXL (custom hash router), Indeed (custom careers SPA),
    #     Domino's (403), PolicyBazaar (404), Kotak (custom hash router),
    #     Splunk (now part of Cisco; Cisco target already covers it),
    #     McKinsey (DNS timeout), KPMG (Oracle HCM).
]


# ---------------------------------------------------------------------------
# Browser singleton + refresh loop
# ---------------------------------------------------------------------------
_browser_lock = asyncio.Lock()
_browser = None
_pw = None
_refresh_task: Optional[asyncio.Task] = None


async def _ensure_browser():
    global _browser, _pw
    async with _browser_lock:
        if _browser is not None:
            return _browser
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            log.warning("playwright not installed; SPA scrapers will return [].")
            return None
        try:
            _pw = await async_playwright().start()
            _browser = await _pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            log.info("playwright chromium launched")
        except Exception as e:
            log.warning("playwright launch failed: %s", e)
            _browser = None
        return _browser


async def _refresh_one(browser, company: str, url: str, extractor) -> list[Job]:
    ctx = None
    page = None
    captured_json: list = []  # everything the page fetched as JSON

    async def _on_response(response):
        try:
            ct = (response.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            if response.status >= 400:
                return
            data = await response.json()
            captured_json.append((response.url, data))
        except Exception:
            pass

    try:
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()
        # Block heavy 3rd-party trackers/ads that often hang networkidle.
        _BLOCKED_HOSTS = (
            "google-analytics.com", "googletagmanager.com", "doubleclick.net",
            "facebook.net", "facebook.com", "datadoghq.com", "datadoghq.eu",
            "newrelic.com", "nr-data.net", "branch.io", "appsflyer.com",
            "appsflyersdk.com", "demdex.net", "everesttech.net",
            "adobedtm.com", "omtrdc.net", "hotjar.com", "segment.io",
            "segment.com", "mixpanel.com", "amplitude.com", "fullstory.com",
            "onelink.me", "creativecdn.com",
        )
        async def _route_block(route):
            try:
                u = route.request.url
                if any(h in u for h in _BLOCKED_HOSTS):
                    await route.abort()
                else:
                    await route.continue_()
            except Exception:
                try: await route.continue_()
                except Exception: pass
        try:
            await page.route("**/*", _route_block)
        except Exception:
            pass
        page.on("response", _on_response)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
        except Exception:
            # Some sites hang on domcontentloaded; fall back to commit (just nav).
            await page.goto(url, wait_until="commit", timeout=PAGE_LOAD_TIMEOUT_MS)
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        # Trigger lazy-loaded job lists by scrolling down a couple of times.
        try:
            for _ in range(3):
                await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
        except Exception:
            pass

        jobs: list[Job] = []
        if extractor is not None:
            try:
                jobs = await extractor(page)
            except Exception as e:
                log.warning("spa[%s] custom extractor failed: %s", company, e)

        # Always also try JSON-LD on the rendered DOM
        if not jobs:
            for raw in await _extract_json_ld(page):
                j = _job_from_ld(company, url, raw)
                if j is not None:
                    jobs.append(j)

        # Last resort: scan captured XHR JSON for India-tagged job objects
        if not jobs:
            jobs = _harvest_jobs_from_xhr(company, url, captured_json)

        # Final fallback: scrape <a href*="/jobs/"|"/details/"|...> anchors.
        # Useful when the page renders job links as plain HTML with no XHR.
        if not jobs:
            jobs = await _extract_anchor_jobs(page, company, url)

        return jobs
    finally:
        try:
            if page: await page.close()
        except Exception:
            pass
        try:
            if ctx: await ctx.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Generic XHR harvester — walks every JSON response captured during page load
# looking for arrays of objects that look like job postings.
# ---------------------------------------------------------------------------
_JOB_TITLE_KEYS = ("title", "jobtitle", "postingtitle", "name", "position", "role",
                   "reqtitle", "requisitiontitle")
_JOB_LOC_KEYS = ("location", "locations", "city", "office", "primarylocation",
                 "locationtext", "locationname", "locale", "country", "address",
                 "officelocationnames")
_JOB_URL_KEYS = ("url", "jobUrl", "applyurl", "absolute_url", "hostedurl",
                 "canonicalpositionurl", "permalink", "jobpath", "applylink")
_JOB_ID_KEYS = ("jobidobfuscated", "reqid", "jobid", "positionid", "postingid",
                "id", "jobcode", "jobreqid", "requisitionid")

# Per-company URL template applied when a job has an id but no url.
# {id} is substituted with the matched id value, {host} with the page host.
_URL_TEMPLATES: dict[str, str] = {
    "Swiggy": "https://careers.swiggy.com/#/jobs/{id}",
    "Flipkart": "https://flipkart.turbohire.co/job/{id}",
    "Myntra": "https://jobs.myntra.com/job/{id}",
    "Cars24": "https://careers.cars24.com/jobs/{id}",
}


def _unwrap(d: dict) -> dict:
    """Unwrap Elasticsearch-style envelopes ({_source: {...}}) and similar."""
    if not isinstance(d, dict):
        return d
    for wrapper_key in ("_source", "fields", "doc", "data"):
        inner = d.get(wrapper_key)
        if isinstance(inner, dict) and len(inner) > len(d) - 2:
            # Looks like a meaningful envelope. Merge: prefer envelope but
            # keep outer fields not in inner so we don't lose ids.
            merged = dict(inner)
            for k, v in d.items():
                if k != wrapper_key and k not in merged:
                    merged[k] = v
            return merged
    return d


def _coerce(v) -> Optional[str]:
    """Normalize a found value to a string. Returns None if empty.

    Handles strings, lists of strings/dicts, dicts. If the string looks
    like JSON (starts with [ or {), tries to parse and recurse.
    """
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Try to parse stringified JSON (e.g. Flipkart Location field)
        if s[0] in "[{":
            try:
                parsed = json.loads(s)
                got = _coerce(parsed)
                if got:
                    return got
            except Exception:
                pass
        return s
    if isinstance(v, list) and v:
        parts = []
        for x in v:
            if isinstance(x, dict):
                got = None
                for orig_k, val in x.items():
                    lk = orig_k.lower()
                    if lk in ("name", "addresslocality", "city", "country",
                              "label", "value", "address", "displayname",
                              "locationname"):
                        if isinstance(val, str) and val:
                            got = val
                            break
                if got:
                    parts.append(got)
            elif isinstance(x, str) and x:
                parts.append(x)
        return ", ".join(parts) or None
    if isinstance(v, dict):
        for orig_k, val in v.items():
            lk = orig_k.lower()
            if lk in ("name", "addresslocality", "city", "country",
                      "label", "value", "address", "displayname",
                      "locationname"):
                if isinstance(val, str) and val:
                    return val
    return None


def _kget(d: dict, keys) -> Optional[str]:
    """Case-insensitive key match — exact, suffix, or prefix.

    e.g. needle "title" matches keys "title", "jobTitle", "reqTitle", "titleText".
    """
    if not isinstance(d, dict):
        return None
    # Try exact first to be precise
    for orig_k, v in d.items():
        lk = orig_k.lower()
        for needle in keys:
            if lk == needle:
                s = _coerce(v)
                if s:
                    return s
    # Then suffix / prefix
    for orig_k, v in d.items():
        lk = orig_k.lower()
        for needle in keys:
            if lk.endswith(needle) or lk.startswith(needle):
                s = _coerce(v)
                if s:
                    return s
    return None


def _is_jobby(d: dict) -> bool:
    if not isinstance(d, dict):
        return False
    has_title = _kget(d, _JOB_TITLE_KEYS) is not None
    has_loc = _kget(d, _JOB_LOC_KEYS) is not None
    return has_title and has_loc


def _walk_for_arrays(node):
    if isinstance(node, list):
        if node and isinstance(node[0], dict):
            yield node
        for v in node:
            yield from _walk_for_arrays(v)
    elif isinstance(node, dict):
        for v in node.values():
            yield from _walk_for_arrays(v)


def _harvest_jobs_from_xhr(company: str, source_url: str,
                           captured: list[tuple[str, object]]) -> list[Job]:
    out: list[Job] = []
    seen: set[tuple[str, str]] = set()
    for resp_url, data in captured:
        for arr in _walk_for_arrays(data):
            unwrapped = [_unwrap(d) for d in arr if isinstance(d, dict)]
            jobby = [d for d in unwrapped if _is_jobby(d)]
            if len(jobby) < 2:  # avoid noise — need at least 2 to call it a job list
                continue
            for d in jobby:
                title = _kget(d, _JOB_TITLE_KEYS) or ""
                loc = _kget(d, _JOB_LOC_KEYS) or ""
                if not title or not _looks_india(loc):
                    continue
                url = _kget(d, _JOB_URL_KEYS)
                if not url:
                    # Fall back to per-company URL template + record id
                    rid = _kget(d, _JOB_ID_KEYS)
                    tmpl = _URL_TEMPLATES.get(company)
                    if rid and tmpl:
                        url = tmpl.format(id=rid)
                    else:
                        url = source_url
                if not url.startswith("http"):
                    # relative path — best-effort prefix with the response host
                    try:
                        from urllib.parse import urljoin
                        url = urljoin(resp_url, url)
                    except Exception:
                        pass
                key = (title.lower(), url.lower())
                if key in seen:
                    continue
                seen.add(key)
                # Try to find a posted-date field
                ts_val = None
                for ck in ("datePosted", "postedDate", "postedAt", "createdAt",
                           "publishedDate", "postdate", "postdateingmt", "updated_at"):
                    if isinstance(d.get(ck), (str, int, float)):
                        ts_val = d[ck]
                        break
                out.append(Job(
                    company=company,
                    title=title,
                    location=loc,
                    url=url,
                    posted_at=_parse_dt(ts_val),
                    source=resp_url.split("?")[0],
                ))
    return out


async def _refresh_all() -> None:
    browser = await _ensure_browser()
    if browser is None:
        return
    for company, url, extractor in SPA_TARGETS:
        start = time.time()
        try:
            jobs = await asyncio.wait_for(
                _refresh_one(browser, company, url, extractor),
                timeout=PER_COMPANY_TIMEOUT_S,
            )
            _SPA_CACHE[company] = jobs
            log.info("spa[%s] -> %d jobs in %.1fs", company, len(jobs), time.time() - start)
        except asyncio.TimeoutError:
            log.warning("spa[%s] timed out after %ds", company, PER_COMPANY_TIMEOUT_S)
            _SPA_CACHE.setdefault(company, [])
        except Exception as e:
            log.warning("spa[%s] error: %s", company, e)
            _SPA_CACHE.setdefault(company, [])
        finally:
            _SPA_LAST_RUN[company] = time.time()


async def _refresh_loop():
    while True:
        try:
            await _refresh_all()
        except Exception as e:
            log.exception("spa refresh loop error: %s", e)
        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)


async def start_spa_refresher() -> None:
    """Kick off the background refresher. Idempotent."""
    global _refresh_task
    if _refresh_task is not None and not _refresh_task.done():
        return
    _refresh_task = asyncio.create_task(_refresh_loop())
    log.info("spa refresher started (every %ds)", REFRESH_INTERVAL_SECONDS)


async def stop_spa_refresher() -> None:
    global _refresh_task, _browser, _pw
    if _refresh_task:
        _refresh_task.cancel()
        try:
            await _refresh_task
        except (asyncio.CancelledError, Exception):
            pass
        _refresh_task = None
    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None
    if _pw:
        try:
            await _pw.stop()
        except Exception:
            pass
        _pw = None


def cached_jobs(company: str) -> list[Job]:
    return list(_SPA_CACHE.get(company, []))


SPA_COMPANIES: list[str] = [c for c, _, _ in SPA_TARGETS]
