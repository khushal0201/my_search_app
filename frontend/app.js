const $ = (id) => document.getElementById(id);
const tbody = document.querySelector('#jobs tbody');
const statusEl = $('status');

// Holds the companies the user has *actively* picked.
// Default after load = all companies except the heavy ones below (which are
// unchecked but still listed, so users can opt them in).
const selectedCompanies = new Set();
let allCompanies = [];
// Slow / very large feeds — unchecked by default and also deferred server-side
// so they don't block the initial refresh. Keep in sync with HEAVY_COMPANIES in
// backend/aggregator.py.
const HEAVY_COMPANIES = new Set(['Infosys', 'PwC', 'Accenture', 'KPMG', 'Bosch']);

async function loadCompanies() {
  try {
    const res = await fetch('/api/companies');
    const data = await res.json();
    allCompanies = data.companies || [];

    // Default state: everything selected except the heavy feeds.
    selectedCompanies.clear();
    for (const c of allCompanies) {
      if (!HEAVY_COMPANIES.has(c)) selectedCompanies.add(c);
    }

    // Seed the source pills from the company list so they show up immediately,
    // instead of waiting for the (expensive) /api/status probe. Each pill is
    // recolored by updatePillCounts() as stream batches arrive.
    const pills = $('sources');
    pills.innerHTML = allCompanies.map((c) =>
      `<span class="src empty" data-company="${escapeHtml(c)}">${escapeHtml(c)}<span class="n">0</span></span>`
    ).join('');

    // Build the multi-select checkbox list.
    const list = $('company-list');
    const allRow = `
      <label class="ms-item ms-all" data-name="">
        <input type="checkbox" id="company-all" />
        <span>Select all</span>
      </label>
    `;
    list.innerHTML = allRow + allCompanies.map((c) => {
      const checked = HEAVY_COMPANIES.has(c) ? '' : 'checked';
      return `
      <label class="ms-item" data-name="${escapeHtml(c.toLowerCase())}">
        <input type="checkbox" value="${escapeHtml(c)}" ${checked} />
        <span>${escapeHtml(c)}</span>
      </label>
    `;
    }).join('');
    list.querySelectorAll('input[type=checkbox][value]').forEach((cb) => {
      cb.addEventListener('change', () => {
        if (cb.checked) selectedCompanies.add(cb.value);
        else selectedCompanies.delete(cb.value);
        updateSelectAllState();
        updateCompanyToggleLabel();
        render(); // client-side filter, no API call
      });
    });
    $('company-all').addEventListener('change', () => {
      // Standard "Select all" semantics:
      //   checked   -> tick every company
      //   unchecked -> untick every company
      const sa = $('company-all');
      const wantAll = sa.checked;
      selectedCompanies.clear();
      for (const cb of list.querySelectorAll('input[type=checkbox][value]')) {
        cb.checked = wantAll;
        if (wantAll) selectedCompanies.add(cb.value);
      }
      updateSelectAllState();
      updateCompanyToggleLabel();
      render(); // client-side filter, no API call
    });
    updateCompanyToggleLabel();
    updateSelectAllState();
  } catch (e) {
    console.error(e);
  }
}

function updateCompanyToggleLabel() {
  const btn = $('company-toggle');
  if (!btn) return;
  const n = selectedCompanies.size;
  const total = allCompanies.length;
  if (n === 0) btn.textContent = 'No companies selected';
  else if (n === total) btn.textContent = 'All companies';
  else if (n === 1) btn.textContent = Array.from(selectedCompanies)[0];
  else btn.textContent = `${n} companies selected`;
}

function updateSelectAllState() {
  const sa = $('company-all');
  if (!sa) return;
  const n = selectedCompanies.size;
  const total = allCompanies.length;
  if (n === 0) {
    sa.checked = false;
    sa.indeterminate = false;
  } else if (n === total) {
    sa.checked = true;
    sa.indeterminate = false;
  } else {
    sa.checked = false;
    sa.indeterminate = true;
  }
}

function openCompanyPanel() {
  $('company-panel').hidden = false;
  $('company-filter').focus();
}
function closeCompanyPanel() {
  $('company-panel').hidden = true;
}
function toggleCompanyPanel() {
  if ($('company-panel').hidden) openCompanyPanel(); else closeCompanyPanel();
}

async function loadDefaults() {
  try {
    const res = await fetch('/api/defaults');
    const data = await res.json();
    if (!$('roles').value) $('roles').value = data.roles.join(', ');
    if (!$('skills').value) $('skills').value = data.skills.join(', ');
  } catch (e) {
    console.error(e);
  }
}

function updatePillCounts(jobs) {
  // Recount per-company from the actual table data so pills reflect the
  // user's current query, not the generic /api/status probe.
  const counts = {};
  for (const j of jobs) counts[j.company] = (counts[j.company] || 0) + 1;
  for (const pill of document.querySelectorAll('#sources .src[data-company]')) {
    if (pill.classList.contains('fail')) continue;
    const company = pill.dataset.company;
    const n = counts[company] || 0;
    const span = pill.querySelector('.n');
    if (span) span.textContent = String(n);
    pill.classList.remove('ok', 'empty');
    pill.classList.add(n > 0 ? 'ok' : 'empty');
  }
}

function fmtDate(iso) {
  if (!iso) return '<span class="muted">—</span>';
  const d = new Date(iso);
  if (isNaN(d)) return '<span class="muted">—</span>';
  return d.toISOString().slice(0, 10);
}

function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

async function loadJobs(refresh = false) {
  const roles = $('roles').value.trim();
  const skills = $('skills').value.trim();
  const postedDays = parseInt($('posted').value, 10) || 0;
  const params = new URLSearchParams();
  params.set('roles', roles);
  params.set('skills', skills);
  // Tell the server the user's "Posted within" window so it knows whether
  // to serve hot-tier only (<=7 days) or union the on-disk archive (>7).
  if (postedDays > 0) params.set('posted_days', String(postedDays));
  // Always fetch every company so the server can serve from one cache key.
  // The company dropdown filters client-side in render().
  if (refresh) params.set('refresh', 'true');

  // `refresh=true` is a one-shot action (purges memory + disk). Keep it out
  // of the URL so a subsequent browser reload doesn't accidentally trigger
  // a second full refresh while the first one is still in flight.
  const urlParams = new URLSearchParams(window.location.search);
  urlParams.delete('refresh');
  urlParams.delete('fresh');
  const qs = urlParams.toString();
  history.replaceState(null, '', qs ? `?${qs}` : window.location.pathname);

  allJobs = [];
  tbody.innerHTML = '';
  loadProgress = { done: 0, pending: 0 };
  statusEl.textContent = 'Loading…';

  try {
    const res = await fetch(`/api/jobs/stream?${params}`);
    if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { value, done: streamDone } = await reader.read();
      if (streamDone) break;
      buf += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        let msg;
        try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type === 'start') {
          loadProgress = { done: 0, pending: (msg.companies || []).length };
          render();
        } else if (msg.type === 'batch') {
          loadProgress.done += 1;
          if (msg.jobs && msg.jobs.length) {
            allJobs = allJobs.concat(msg.jobs);
            allJobs.sort((a, b) => {
              const ma = a.posted_at ? 0 : 1;
              const mb = b.posted_at ? 0 : 1;
              if (ma !== mb) return ma - mb;
              const ta = a.posted_at ? Date.parse(a.posted_at) : 0;
              const tb = b.posted_at ? Date.parse(b.posted_at) : 0;
              return tb - ta;
            });
          }
          render();
        } else if (msg.type === 'done') {
          loadProgress = null;
          loadedPostedDays = postedDays;
          render();
          // If the user widened the "Posted within" window while this load
          // was in flight, the data we just received doesn't cover it.
          // Kick a follow-up fetch so the archive rows show up.
          const liveDays = parseInt($('posted').value, 10) || 0;
          if (_effectivePostedDays(liveDays) > _effectivePostedDays(loadedPostedDays)) {
            loadJobs(false);
          }
        } else if (msg.type === 'error') {
          console.warn('stream error', msg.error);
        }
      }
    }
    loadProgress = null;
    render();
  } catch (e) {
    loadProgress = null;
    statusEl.textContent = 'Error loading jobs';
    console.error(e);
  }
}

let allJobs = [];
let loadProgress = null; // {done, pending} while a stream is in flight
// posted_days value the server was asked for in the *last completed* load.
// Used to decide whether a "Posted within" change needs another API hit.
// null  = nothing loaded yet
// 0     = "Any time" was requested (server returned everything)
// N>0   = last load fetched up to N days
let loadedPostedDays = null;

// Treat 0 ("Any time") as the widest window so comparisons are uniform.
function _effectivePostedDays(d) {
  return (!d || d <= 0) ? Infinity : d;
}

function filterByPosted(jobs) {
  const days = parseInt($('posted').value, 10) || 0;
  if (!days) return jobs;
  // Jobs without a known date are excluded when a date window is active —
  // we can't prove they're inside the window.
  const cutoff = Date.now() - days * 86400 * 1000;
  return jobs.filter((j) => {
    if (!j.posted_at) return false;
    const t = Date.parse(j.posted_at);
    return !isNaN(t) && t >= cutoff;
  });
}

function filterByCompany(jobs) {
  // When every company is selected (or none of the checkboxes have been built
  // yet) there's nothing to filter — show all rows.
  if (!allCompanies.length) return jobs;
  if (selectedCompanies.size === allCompanies.length) return jobs;
  if (selectedCompanies.size === 0) return [];
  return jobs.filter((j) => selectedCompanies.has(j.company));
}

function render() {
  const jobs = filterByPosted(filterByCompany(allJobs));
  const totalLabel = allJobs.length === jobs.length
    ? `${jobs.length} jobs`
    : `${jobs.length} of ${allJobs.length} jobs`;
  const progress = loadProgress
    ? ` · loading ${loadProgress.done} / ${loadProgress.pending} companies`
    : '';
  statusEl.textContent = totalLabel + progress;
  updatePillCounts(jobs);
  if (!jobs.length) {
    if (loadProgress) {
      tbody.innerHTML = '';
      return;
    }
    const msg = allJobs.length
      ? 'No jobs match the current "Posted within" window. Try a wider range.'
      : 'No jobs found. Try different keywords, or hit Refresh — some scrapers fail silently if the company changed their endpoint.';
    tbody.innerHTML = `<tr><td colspan="5" class="muted">${msg}</td></tr>`;
    return;
  }
  tbody.innerHTML = jobs.map((j) => `
    <tr>
      <td>${fmtDate(j.posted_at)}</td>
      <td>${escapeHtml(j.company)}</td>
      <td>${escapeHtml(j.title)}</td>
      <td>${escapeHtml(j.location)}</td>
      <td><a href="${escapeHtml(j.url)}" target="_blank" rel="noopener">Open</a></td>
    </tr>
  `).join('');
}

$('search').addEventListener('click', () => loadJobs(false));
$('refresh').addEventListener('click', () => loadJobs(true));
// Posted-within change strategy:
//   - load in flight                              -> re-render only; the
//                                                    in-flight stream will
//                                                    auto-refetch on `done`
//                                                    if we widened the window
//   - new window <= last-loaded window            -> re-render (subset of
//                                                    data we already have)
//   - new window  > last-loaded window            -> fetch (need archive rows)
$('posted').addEventListener('change', () => {
  const newDays = parseInt($('posted').value, 10) || 0;
  if (loadProgress) { render(); return; }
  if (loadedPostedDays !== null &&
      _effectivePostedDays(newDays) <= _effectivePostedDays(loadedPostedDays)) {
    render();
    return;
  }
  loadJobs(false);
});
for (const id of ['roles', 'skills']) {
  $(id).addEventListener('keydown', (e) => { if (e.key === 'Enter') loadJobs(); });
}

// Multi-select company panel
$('company-toggle').addEventListener('click', (e) => {
  e.stopPropagation();
  toggleCompanyPanel();
});
$('company-panel').addEventListener('click', (e) => e.stopPropagation());
document.addEventListener('click', () => closeCompanyPanel());
$('company-filter').addEventListener('input', (e) => {
  const q = e.target.value.trim().toLowerCase();
  for (const item of document.querySelectorAll('#company-list .ms-item')) {
    const name = item.dataset.name || '';
    item.classList.toggle('hidden', q && !name.includes(q));
  }
});
$('company-clear').addEventListener('click', () => {
  if (!selectedCompanies.size) return;
  selectedCompanies.clear();
  for (const cb of document.querySelectorAll('#company-list input[type=checkbox][value]')) {
    cb.checked = false;
  }
  updateSelectAllState();
  updateCompanyToggleLabel();
  render();
});

(async () => {
  await Promise.all([loadCompanies(), loadDefaults()]);
  // Allow URL like /?refresh=true (or ?fresh=1) to force a fresh fetch on load.
  const qs = new URLSearchParams(window.location.search);
  const wantFresh = qs.get('refresh') === 'true' || qs.get('fresh') === '1';
  loadJobs(wantFresh);
})();
