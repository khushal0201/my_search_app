# India Jobs Aggregator

Python (FastAPI) backend + lightweight HTML/JS frontend that pulls open roles
in **India** from the public career endpoints of **79 companies** — Big Tech,
fintech, consulting, retail, healthcare, and Indian unicorns. Results are
deduped, India-filtered, keyword-matched against role and skill lists, and
sorted by post date (newest first).

Scraping strategy is layered:

1. **HTTP JSON APIs** — Workday CXS, Greenhouse, Lever, SmartRecruiters,
   Ashby, Oracle HCM, Eightfold, Phenom, and bespoke endpoints (one async
   `fetch_*` per company in `backend/scrapers/companies.py`).
2. **Playwright SPA runner** — for single-page sites that ship no SSR job
   data, a background task opens each careers page in headless Chromium
   every 15 minutes, extracts JobPosting JSON-LD or anchor-based fallbacks,
   and caches results so user requests never block on a browser.

## Prerequisites

- **Python 3.10+** (3.12 recommended)
- **Git**
- Internet access — scrapers hit live career APIs

## Installation

```powershell
# 1. Clone
git clone https://github.com/khushal0201/my_search_app.git
cd my_search_app

# 2. Create + activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# (Linux/macOS: source .venv/bin/activate)

# 3. Install Python dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Install the Playwright Chromium browser (one-time, ~170 MB)
python -m playwright install chromium
```

On Linux you may also need OS libs for Chromium:

```bash
python -m playwright install-deps chromium
```

## Run

```powershell
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Open <http://localhost:8000> in your browser.

The Playwright SPA refresher kicks off automatically on startup. The first
full refresh cycle takes a few minutes (each SPA target has a 45-second cap);
HTTP-based scrapers respond immediately.

## API

- `GET /api/companies` — list of all 79 supported companies.
- `GET /api/defaults` — default role and skill lists used by the UI.
- `GET /api/jobs?roles=data+engineer,ai+engineer&skills=spark,langchain&company=&refresh=false`
  — aggregated jobs. `roles` are matched against the job title (all tokens
  must appear); `skills` are passed to each careers site's full-text search
  (matches JD content). `company` accepts a comma-separated subset of names.
  Pass `refresh=true` to bypass the 5-hour cache.
- `GET /api/jobs?q=engineer` — backward-compat single-keyword mode.
- `GET /api/status?q=data` — runs each scraper once and reports per-source
  health; used by the UI to render the colored source pills.

## Project layout

```
my_search_app/
  backend/
    main.py                # FastAPI app, serves API + frontend
    aggregator.py          # fan-out, 5-hour cache, heavy-company scheduling
    models.py              # Job dataclass
    scrapers/
      companies.py         # one async fetch_* per company + SCRAPERS registry
      spa_runner.py        # Playwright background refresher + SPA_TARGETS
  frontend/
    index.html
    app.js                 # bump ?v= when shipping JS changes
    style.css
  companies_wishlist.txt   # tracker of which companies are wired in
  requirements.txt
  README.md
```

## Notes

- Results are cached in-memory for **5 hours** per `(roles, skills, company)`
  triple. Restarting the server clears the cache.
- Heavy scrapers (Infosys, PwC, Accenture, KPMG) run last so the rest of the
  page populates quickly.
- Every scraper catches its own exceptions and returns `[]` on failure, so
  one broken source never breaks the aggregator.
- Some companies legitimately have zero India engineering openings on their
  public ATS (e.g. several US-only remote startups) — that is data, not a bug.
