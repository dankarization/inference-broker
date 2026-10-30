# Queue dashboard: frontend updates without broker restart

## Boundaries

- `GET /dashboard` serves a file-backed HTML shell; observer unavailable still
  returns HTTP 503 with an explicit unavailable page, not zero counters.
- The browser fetches the unchanged, payload-free `GET /v1/dashboard` JSON.
  Sources, Active jobs, Forecast and initial History rows are rendered in
  `broker/static/dashboard.js` using DOM nodes and `textContent`.
- `GET /v1/history` supplies subsequent keyset pages and the same server-formatted
  History display values. Source controls use the existing POST endpoints.
- The 30-second page refresh, numeric Weight ordering, three-click table sorting
  cycle, source dispatch/admission controls, bulk confirmations and history
  scrolling are retained. No dispatcher, scheduler, policy or storage semantics
  are changed.
- A failed JSON fetch hides the live tables and reports unavailable; stale
  snapshots retain their explicit stale label. JavaScript is required for live
  rows; the unavailable HTML shell remains legible without it.

## Files and validators

The package includes:

- `broker/static/dashboard.html`: normal page structure and column headers.
- `broker/static/dashboard-unavailable.html`: initial HTTP 503 page.
- `broker/static/dashboard.css`: styling, including sortable headers.
- `broker/static/dashboard.js`: data loading, rendering and controls.
- `broker/table_sort.js`: existing shared sorting code, unchanged.

`broker/dashboard_assets.py` allows only these named assets under
`/dashboard/assets/<name>`; it is not a filesystem directory server. Every
request checks file mtime (nanoseconds) and size. Unchanged files reuse cached
bytes; inode and ctime also invalidate the cache after replacement or a
same-size copy with preserved mtime.

Asset ETags are content SHA-256 digests, with `Cache-Control: no-cache` and
conditional 304 responses. HTML dependency URLs include the content digest as
`?v=...`. The `/dashboard` validator combines the existing queue/policy/time
validator with all frontend digests: a CSS/JS-only edit invalidates the HTML
validator and changes the dependency URL on the next reload. The JSON endpoint's
validator and response shape are independent of frontend files and unchanged.
Unavailable HTML is `no-store`.

## Updating UI after this backend has been installed

1. Locate the active installation's `broker/static` directory. Edit/copy only the
   HTML/CSS/JS files there, retaining the version placeholders in the HTML.
   This applies equally to a source checkout and an unpacked installed wheel.
2. Prefer writing each new file beside its destination and atomically renaming it
   into place. Do not update a file in place while it is being read. For changes
   spanning files, keep old/new interfaces compatible during replacement.
3. Reload `/dashboard` (or wait for its 30-second refresh). Changed files are
   re-read by the existing HTTP server, including after conditional requests.
   Do not restart or drain the broker for a frontend-only update.
4. Roll back UI by restoring the previous asset bytes; reload the page again.

This implementation itself changes Python HTTP routing and needs a one-time
normal backend rollout before frontend-only hot updates are available.
Deploying or restarting production is outside this code change.

## Verification

```sh
python3 -m unittest discover -s tests
node tests/dashboard_dom.cjs
```

`tests/test_dashboard_assets.py` edits temporary HTML/CSS/JS while reusing one
HTTP server, checks mtime/size caching and ETag invalidation, verifies unchanged
JSON and queued/running job state, and keeps an actual dispatch attempt blocked
during a UI edit before letting that same attempt complete once.
`tests/dashboard_dom.cjs` executes the shipped HTML, dashboard script and shared
sorter against a small DOM shim (no npm dependency). It checks columns, sorting
and reset, initial/appended history, timestamps, policy writes, maintenance,
unavailable/stale states and fetch-failure recovery. The unittest wrapper skips
only this DOM check when Node is unavailable; Node is not a runtime requirement.
