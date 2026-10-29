"""Payload-free operational snapshot and HTML dashboard for the broker."""
from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .storage import PUBLIC_JOB_PREDICATE
from .profiles import PROFILES
from .table_sort import TABLE_SORT_CSS, TABLE_SORT_JS


LOCAL_TIMEZONE = ZoneInfo("Asia/Tbilisi")


def history_display(row: dict[str, Any]) -> dict[str, Any]:
    """Shared display values for initial HTML and paged history (no payload reads)."""
    finished = row.get("finished")
    started = row.get("started")
    duration = (
        float(finished) - float(started)
        if row.get("state") == "completed" and started is not None
        and finished is not None and finished >= started else None
    )
    profile = PROFILES.get(row.get("profile"))
    return dict(
        row,
        model=profile.model if profile else None,
        model_turn_seconds=duration,
        model_turn_display=f"{duration:.3f}s" if duration is not None else "—",
        finished_display=(
            datetime.fromtimestamp(float(finished), tz=timezone.utc)
            .astimezone(LOCAL_TIMEZONE).strftime("%d/%m/%Y, %H:%M:%S")
            if finished is not None else "—"
        ),
    )


def local_timestamp(value: float | None) -> str | None:
    """Format an epoch timestamp for the local-only operator dashboard."""
    if value is None:
        return None
    timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(LOCAL_TIMEZONE)
    offset = timestamp.strftime("%z")
    return (
        f"{timestamp:%Y-%m-%d %H:%M:%S} UTC{offset[:3]}:{offset[3:]} "
        "(Asia/Tbilisi)"
    )


def timestamp_title(value: float | None) -> str | None:
    """Return an operator-facing raw timestamp for a timestamp cell tooltip."""
    if value is None:
        return None
    utc_timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc)
    raw_iso = utc_timestamp.isoformat().replace("+00:00", "Z")
    return f"{raw_iso} (epoch {value})"


def snapshot(
    db: sqlite3.Connection, *, now: float, policy_snapshot: dict[str, Any] | None,
    include_producer_storage: bool = True,
) -> dict[str, Any]:
    """Return operational data only; request payloads, results and errors are excluded."""
    policy_sources = (policy_snapshot or {}).get("sources", {})
    policy_active = policy_snapshot is not None
    public_clause = "" if include_producer_storage else " AND " + PUBLIC_JOB_PREDICATE
    schedules = dict(db.execute("SELECT source,next_allowed FROM source_schedules"))
    # Do not discover sources by scanning all historical jobs.  `jobs` holds
    # payloads and can be multiple GiB, so even an index-only global scan can
    # exceed the observer deadline on a cold cache.  Policy/schedule sources
    # are known directly; active unconfigured sources remain visible through
    # a covering state/source index.
    source_names = set(policy_sources)
    source_names.update(schedules)
    source_names.update(
        row["source"]
        for row in db.execute(
            "SELECT DISTINCT source FROM jobs INDEXED BY jobs_state_source "
            "WHERE state IN ('queued','running','cancel_requested')" + public_clause
        )
    )
    sources: list[dict[str, Any]] = []
    overall = {name: 0 for name in ("queued", "running", "lease", "retry", "delayed", "completed", "failed", "dead", "cancelled")}

    for source in sorted(source_names):
        configured = policy_sources.get(source)
        state_counts = {
            row["state"]: row["count"]
            for row in db.execute(
                "SELECT state,count(*) AS count FROM jobs "
                "INDEXED BY jobs_source_state WHERE source=?" + public_clause +
                " GROUP BY state",
                (source,),
            )
        }
        queued = state_counts.get("queued", 0)
        next_allowed = schedules.get(source)
        delayed = queued if next_allowed is not None and next_allowed > now else 0
        # Completion audit records are committed with the terminal job update.
        # Their source/time index avoids reading every completed job payload to
        # derive the two recent windows.
        completed_windows = db.execute(
            "SELECT "
            "coalesce(sum(occurred>=?),0) AS completed_1h,"
            "coalesce(sum(occurred>=?),0) AS completed_24h "
            "FROM audit_events INDEXED BY audit_events_source_time "
            "WHERE source=? AND occurred>=? AND event_type='job.completed'" +
            ("" if include_producer_storage else " AND producer_storage=0"),
            (now - 3_600, now - 86_400, source, now - 86_400),
        ).fetchone()
        retry = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state_retry "
            "WHERE source=? AND state='queued' AND retry_count>0" + public_clause,
            (source,),
        ).fetchone()[0]
        lease = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state "
            "WHERE source=? AND state IN ('running','cancel_requested') "
            "AND lease_until IS NOT NULL" + public_clause,
            (source,),
        ).fetchone()[0]
        states = {
            "queued": queued,
            "running": state_counts.get("running", 0) + state_counts.get("cancel_requested", 0),
            "lease": lease,
            "retry": retry,
            "delayed": delayed,
            "completed": state_counts.get("completed", 0),
            "failed": state_counts.get("failed", 0),
            # The broker has no separate `dead` state: terminal jobs are failed.
            "dead": 0,
            "cancelled": state_counts.get("cancelled", 0),
        }
        for name, value in states.items():
            overall[name] += value
        sources.append({
            "source": source,
            "scheduler": {
                "enabled": configured.get("enabled") if isinstance(configured, dict) else (False if policy_active else None),
                "dispatch_paused": (not configured.get("enabled", True)) if isinstance(configured, dict) else (True if policy_active else None),
                "admission_allowed": configured.get("admission_allowed", True) if isinstance(configured, dict) else True,
                "weight": configured.get("weight") if isinstance(configured, dict) else None,
                "next_allowed": next_allowed,
            },
            "states": states,
            "completed_last_hour": completed_windows["completed_1h"],
            "completed_last_24_hours": completed_windows["completed_24h"],
        })

    active_jobs = [dict(row) for row in db.execute(
        "SELECT id,source,state,created,started,lease_until,attempt_count,retry_count "
        "FROM jobs WHERE state IN ('running','cancel_requested')" + public_clause +
        " ORDER BY started,created,id"
    )]
    return {
        "timestamp": now,
        "sources": sources,
        "active_jobs": active_jobs,
        "overall": {
            "states": overall,
            "completed_last_hour": sum(item["completed_last_hour"] for item in sources),
            "completed_last_24_hours": sum(item["completed_last_24_hours"] for item in sources),
        },
    }


def render(data: dict[str, Any]) -> bytes:
    """Render a self-contained, intentionally local-only dashboard."""
    observation = data.get("observation", {"state": "live"})

    def cell(value: Any) -> str:
        if value is None:
            return "—"
        if isinstance(value, bool):
            return "yes" if value else "no"
        return html.escape(str(value))

    def timestamp_cell(value: float | None, *, tag: str = "td") -> str:
        display = local_timestamp(value)
        sort_attr = (
            f' data-sort-value="{html.escape(str(value), quote=True)}"'
            if tag == "td" and value is not None else ""
        )
        if display is None:
            return f"<{tag}{sort_attr}>—</{tag}>"
        title = html.escape(timestamp_title(value) or "", quote=True)
        return f'<{tag}{sort_attr} title="{title}">{html.escape(display)}</{tag}>'

    def sort_th(label: str, key: str, extra_class: str = "") -> str:
        cls = f" class={extra_class}" if extra_class else ""
        return (
            f'<th data-sort-key="{html.escape(key)}"{cls}>'
            f'<button type=button class=sort-btn>{html.escape(label)}'
            f'<span class=sort-mark aria-hidden=true>↕</span></button></th>'
        )

    if observation["state"] == "unavailable":
        reason = html.escape(str(observation.get("reason", "dashboard data unavailable")))
        observed_at = timestamp_cell(observation.get("observed_at"), tag="code")
        return f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=30><title>Ollama broker queue unavailable</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}.error{{color:#a00}}</style>
<h1>Ollama inference broker queue</h1><p class=error>Dashboard data unavailable: {reason}.</p>
<p>Observed at: {observed_at} · refreshes every 30 seconds.</p>
</html>""".encode("utf-8")

    def weight_cell(source: str, value: float | None) -> str:
        if value is None:
            return "<td>—</td>"
        selected = int(value)
        options = "".join(
            f'<option value="{weight}"{" selected" if weight == selected else ""}>{weight}</option>'
            for weight in range(1, 11)
        )
        source_attribute = html.escape(source, quote=True)
        return (
            f'<td data-sort-value="{selected}"><form class="weight-form" data-source="'
            f'{source_attribute}"><select name="weight" aria-label="Weight for '
            f'{source_attribute}">{options}</select>'
            '<span class="weight-feedback" aria-live="polite"></span></form></td>'
        )

    def enabled_cell(source: str, value: bool | None) -> str:
        if value is None: return "<td>—</td>"
        source_attribute = html.escape(source, quote=True)
        return (f'<td data-sort-value="{1 if value else 0}"><form class="enabled-form" data-source="{source_attribute}">'
                f'<select name="enabled" aria-label="Dispatch enabled for {source_attribute}">'
                f'<option value="true"{" selected" if value else ""}>running</option>'
                f'<option value="false"{" selected" if not value else ""}>paused</option></select>'
                '<span class="enabled-feedback" aria-live="polite"></span></form></td>')

    def admission_cell(source: str, value: bool) -> str:
        source_attribute = html.escape(source, quote=True)
        return (f'<td data-sort-value="{1 if value else 0}"><form class="admission-form" data-source="{source_attribute}">'
                f'<select name="admission" aria-label="Admission for {source_attribute}">'
                f'<option value="true"{" selected" if value else ""}>allowed</option>'
                f'<option value="false"{" selected" if not value else ""}>blocked</option></select>'
                '<span class="admission-feedback" aria-live="polite"></span></form></td>')

    def bulk_cell(source: str, queued: int, failed: int) -> str:
        source_attribute = html.escape(source, quote=True)
        return (
            f'<td data-sort-value="{queued:05d},{failed:05d}"><button class="bulk-action" data-source="{source_attribute}" '
            f'data-action="queued/cancel" data-count="{queued}"'
            f'{" disabled" if not queued else ""}>Cancel queued ({queued})</button> '
            f'<button class="bulk-action" data-source="{source_attribute}" '
            f'data-action="failed/retry" data-count="{failed}"'
            f'{" disabled" if not failed else ""}>Retry failed ({failed})</button>'
            '<span class="bulk-feedback" aria-live="polite"></span></td>'
        )

    def source_order(item: dict[str, Any]) -> tuple[bool, float, str]:
        weight = item["scheduler"].get("weight")
        return (
            weight is None,
            float(weight) if weight is not None else float("inf"),
            str(item["source"]).casefold(),
        )

    rows = []
    for item in sorted(data["sources"], key=source_order):
        scheduler, states = item["scheduler"], item["states"]
        rows.append("<tr>" + "".join((
            f"<td>{cell(item['source'])}</td>",
            enabled_cell(item["source"], scheduler['enabled']),
            admission_cell(item["source"], scheduler["admission_allowed"]),
            weight_cell(item["source"], scheduler["weight"]),
            *(f"<td>{cell(value)}</td>" for value in (
            states["queued"], states["running"], states["lease"], states["retry"], states["delayed"],
            states["failed"], states["cancelled"], states["completed"],
            item["completed_last_hour"], item["completed_last_24_hours"],
            )),
            bulk_cell(item["source"], states["queued"], states["failed"]),
        )) + "</tr>")
    active = data["active_jobs"]
    active_rows = "".join(
        "<tr>" + "".join((
            f"<td class=compact>{cell(job['id'])}</td>",
            f"<td>{cell(job['source'])}</td>",
            f"<td>{cell(job['state'])}</td>",
            timestamp_cell(job["started"]),
            timestamp_cell(job["lease_until"]),
            f"<td class=compact>{cell(job['attempt_count'])}</td>",
            f"<td>{cell(job['retry_count'])}</td>",
        )) + "</tr>"
        for job in active
    ) or "<tr data-sort-ignore><td colspan=7>None</td></tr>"
    history_rows = "".join(
        f"<tr data-history-id='{cell(row['id'])}'><td class=compact>{cell(row['id'])}</td><td>{cell(row['source'])}</td><td>{cell(row['profile'])}</td><td>{cell(row['model'])}</td><td>{cell(row['state'])}</td><td>{cell(row['model_turn_display'])}</td><td class=compact>{cell(row['finished_display'])}</td><td class=compact>{cell(row['attempt_count'])}</td></tr>"
        for row in map(history_display, data.get("history", []))
    ) or "<tr data-sort-ignore><td colspan=8>None</td></tr>"
    forecast = data.get("forecast")
    forecast_html = ""
    if forecast:
        if forecast.get("unavailable"):
            forecast_html = (
                "<h2>Forecast</h2>"
                f'<p class=error>Forecast unavailable: {html.escape(str(forecast.get("reason", "observer read failed")))}.</p>'
            )
        else:
            current_model = cell(forecast.get("current_model")) or "none"
            selection_rows = "".join(
                "<tr>" + "".join((
                    f"<td>{cell(item['job_id'])}</td>",
                    f"<td>{cell(item['source'])}</td>",
                    f"<td>{cell(item.get('profile'))}</td>",
                    f"<td>{cell(item['model'])}</td>",
                    f"<td>{cell(item['weight'])}</td>",
                    f"<td>{cell(item['mode'])}</td>",
                    f"<td class=compact>{cell(item['reason'])}</td>",
                    f"<td>{cell(item['wait_seconds'])}s</td>",
                )) + "</tr>"
                for item in forecast.get("next_selections", [])
            ) or "<tr data-sort-ignore><td colspan=8>None queued</td></tr>"
            forecast_html = (
                "<h2>Forecast</h2>"
                f"<p>Current model: <b>{current_model}</b> · Lower Weight gets more GPU time (reciprocal shares); "
                "contiguous model batches, FIFO within a source. Active jobs are not interrupted.</p>"
                "<table><thead><tr>"
                + sort_th("Job", "job") + sort_th("Source", "source")
                + sort_th("Profile", "profile") + sort_th("Model", "model")
                + sort_th("Weight", "weight") + sort_th("Mode", "mode")
                + sort_th("Reason", "reason", "compact") + sort_th("Wait", "wait")
                + "</tr></thead>"
                f"<tbody>{selection_rows}</tbody></table>"
                f"<p><small>{html.escape(forecast.get('contingency', ''))}.</small></p>"
            )
    overall = data["overall"]
    document = f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta http-equiv=refresh content=30><title>Ollama broker queue</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem;color:#18212b}}table{{border-collapse:collapse;width:100%;margin:1rem 0}}th,td{{padding:.45rem;border:1px solid #ccd6df;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{background:#edf3f7}}.compact{{width:1%;white-space:nowrap}}code{{font-size:.9em}}.summary{{font-size:1.05rem}}.weight-form,.enabled-form,.admission-form{{display:flex;gap:.35rem;align-items:center;justify-content:flex-end}}.weight-feedback,.enabled-feedback,.admission-feedback,.bulk-feedback{{min-width:4rem;text-align:left}}.error{{color:#a00}}button{{margin:.15rem}}/* TABLE_SORT_CSS */</style>
<h1>Ollama inference broker queue</h1><p>Snapshot timestamp: {timestamp_cell(data['timestamp'], tag='code')} · refreshes every 30 seconds.</p>
{('<p class=error>Showing stale data: ' + html.escape(str(observation.get('reason', 'database observer read unavailable'))) + '.</p>') if observation['state'] == 'stale' else ''}
<p class=summary>Completed: <b>{overall['states']['completed']}</b> total · <b>{overall['completed_last_hour']}</b> last hour · <b>{overall['completed_last_24_hours']}</b> last 24 hours.</p>
<h2>Sources</h2><table><thead><tr>{sort_th('Source','source')}{sort_th('Dispatch','dispatch')}{sort_th('Admission','admission')}{sort_th('Weight','weight')}{sort_th('Queued','queued')}{sort_th('Running','running')}{sort_th('Lease','lease')}{sort_th('Retry','retry')}{sort_th('Delayed','delayed')}{sort_th('Failed','failed')}{sort_th('Cancelled','cancelled')}{sort_th('Completed total','completed')}{sort_th('Completed 1h','completed_1h')}{sort_th('Completed 24h','completed_24h')}{sort_th('Bulk actions','bulk')}</tr></thead><tbody>{''.join(rows) or '<tr data-sort-ignore><td colspan=15>None</td></tr>'}</tbody></table>
<p><small>Dispatch pause preserves queued jobs and lets a running job finish. Admission block rejects new jobs and does not change the queue. Delayed jobs are waiting for a source min-interval.</small></p>
<h2>Active jobs</h2><table><thead><tr>{sort_th('ID','id','compact')}{sort_th('Source','source')}{sort_th('State','state')}{sort_th('Started','started')}{sort_th('Lease until','lease_until')}{sort_th('Attempts','attempts','compact')}{sort_th('Retries','retries')}</tr></thead><tbody>{active_rows}</tbody></table>
{forecast_html}
<h2>History</h2><p><small>Finished: Asia/Tbilisi. Model turn: completed execution slot, including model preparation, excluding queue wait; unavailable for cancelled, failed or unstarted jobs.</small></p><table id=history-table><thead><tr>{sort_th('ID','id','compact')}{sort_th('Source','source')}{sort_th('Profile','profile')}{sort_th('Model','model')}{sort_th('State','state')}{sort_th('Model turn','model_turn')}{sort_th('Finished','finished','compact')}{sort_th('Attempts','attempts','compact')}</tr></thead><tbody id=history-body>{history_rows}</tbody></table><div id=history-sentinel data-cursor="{html.escape(str((data.get('history') or [{}])[-1].get('finished','')) + ':' + str((data.get('history') or [{}])[-1].get('id','')), quote=True)}"></div>
<script>
/* TABLE_SORT_JS */
document.querySelectorAll('table').forEach(function(table){{ TableSort.bind(table); }});
const bindPolicySelect=(form, field)=>{{ const select=form.elements[field], feedback=form.querySelector('.'+field+'-feedback'); let saved=select.value, desired=saved, saving=false; const flush=async()=>{{ if(saving)return; saving=true; while(desired!==saved){{ const value=desired; feedback.className=field+'-feedback'; feedback.textContent='Saving…'; try {{ const response=await fetch('/v1/sources/'+encodeURIComponent(form.dataset.source)+'/'+field,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{[field]:field==='weight'?Number(value):value==='true'}})}}); const result=await response.json(); if(!response.ok)throw new Error(result.error||'save failed'); saved=String(result[field]); if(desired===value){{ select.value=saved; feedback.textContent='Saved'; }} }} catch(error) {{ if(desired===value){{ select.value=saved; feedback.className=field+'-feedback error'; feedback.textContent=error.message||'Save failed'; break; }} }} }} saving=false; }}; select.addEventListener('change',()=>{{ desired=select.value; flush(); }}); }};
document.querySelectorAll('.weight-form').forEach(form=>bindPolicySelect(form,'weight'));
document.querySelectorAll('.enabled-form').forEach(form=>bindPolicySelect(form,'enabled'));
const bindAdmissionSelect=form=>{{ const select=form.elements.admission, feedback=form.querySelector('.admission-feedback'); let saved=select.value, desired=saved, saving=false; const flush=async()=>{{ if(saving)return; saving=true; while(desired!==saved){{ const value=desired; feedback.className='admission-feedback'; feedback.textContent='Saving…'; try {{ const response=await fetch('/v1/sources/'+encodeURIComponent(form.dataset.source)+'/admission',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{allowed:value==='true'}})}}); const result=await response.json(); if(!response.ok)throw new Error(result.error||'save failed'); saved=String(result.admission_allowed); if(desired===value){{ select.value=saved; feedback.textContent='Saved'; }} }} catch(error) {{ if(desired===value){{ select.value=saved; feedback.className='admission-feedback error'; feedback.textContent=error.message||'Save failed'; break; }} }} }} saving=false; }}; select.addEventListener('change',()=>{{desired=select.value;flush();}}); }};
document.querySelectorAll('.admission-form').forEach(bindAdmissionSelect);
document.querySelectorAll('.bulk-action').forEach(button=>button.addEventListener('click',async()=>{{ const count=Number(button.dataset.count), source=button.dataset.source, action=button.dataset.action; if(!window.confirm(action==='queued/cancel'?'Cancel '+count+' queued jobs for '+source+'?':'Retry '+count+' failed jobs for '+source+'?'))return; const feedback=button.parentElement.querySelector('.bulk-feedback'); button.parentElement.querySelectorAll('button').forEach(item=>item.disabled=true); try {{ const response=await fetch('/v1/sources/'+encodeURIComponent(source)+'/'+action,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{confirm:true}})}}); const result=await response.json(); if(!response.ok)throw new Error(result.error||'operation failed'); feedback.textContent=String(result.cancelled??result.retried)+' changed'; }} catch(error) {{ feedback.className='bulk-feedback error'; feedback.textContent=error.message||'Operation failed'; }} }}));
const sentinel=document.querySelector('#history-sentinel'); let loading=false; const historyBody=document.querySelector('#history-body'); new IntersectionObserver(async entries => {{ if(loading||!entries[0].isIntersecting||!sentinel.dataset.cursor) return; loading=true; const response=await fetch('/v1/history?limit=30&cursor='+encodeURIComponent(sentinel.dataset.cursor)); const page=await response.json(); (page.items||[]).forEach(row=>{{ const tr=document.createElement('tr'); tr.dataset.historyId=row.id; [row.id,row.source,row.profile,row.model,row.state,row.model_turn_display,row.finished_display,row.attempt_count].forEach((value,index)=>{{ const td=document.createElement('td'); td.textContent=value==null?'—':String(value); if([0,6,7].includes(index))td.className='compact'; tr.append(td); }}); historyBody.append(tr); const historySort=document.querySelector('#history-table'); if(historySort&&historySort._tableSort) historySort._tableSort.addRow(tr); }}); sentinel.dataset.cursor=page.next_cursor||''; loading=false; }}).observe(sentinel);
</script>
"""
    document = document.replace("/* TABLE_SORT_CSS */", TABLE_SORT_CSS).replace("/* TABLE_SORT_JS */", TABLE_SORT_JS)
    return document.encode("utf-8")
