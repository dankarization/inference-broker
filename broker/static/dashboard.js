"use strict";

const byId = id => document.getElementById(id);
const display = value => value == null ? '—' : String(value);

function cell(value, compact = false, sortValue = null) {
  const td = document.createElement('td');
  td.textContent = display(value);
  if (compact) td.className = 'compact';
  if (sortValue != null) td.setAttribute('data-sort-value', sortValue);
  return td;
}

function timestamp(element, value) {
  if (value == null) { element.textContent = '—'; return element; }
  const date = new Date(value * 1000);
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Tbilisi', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23', timeZoneName: 'longOffset'
  }).formatToParts(date).map(part => [part.type, part.value]));
  const offset = parts.timeZoneName.replace('GMT', 'UTC');
  element.textContent = `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} ${offset} (Asia/Tbilisi)`;
  element.title = `${date.toISOString().replace('.000Z', 'Z')} (epoch ${value})`;
  element.setAttribute('data-sort-value', value);
  return element;
}

function duration(value) {
  if (value == null) return '—';
  if (value < 60) return Number(value).toFixed(3) + 's';
  // Match Python format_duration's round-to-even at exact half seconds.
  const floor = Math.floor(value);
  const total = value - floor === 0.5 ? floor + floor % 2 : Math.round(value);
  const seconds = String(total % 60).padStart(2, '0');
  const minutes = Math.floor(total / 60);
  return minutes < 60 ? `${minutes}:${seconds}` :
    `${Math.floor(minutes / 60)}:${String(minutes % 60).padStart(2, '0')}:${seconds}`;
}

function row(cells) {
  const tr = document.createElement('tr');
  cells.forEach(td => tr.append(td));
  return tr;
}

function fillBody(id, rows, columns, empty = 'None') {
  const body = byId(id);
  if (!rows.length) {
    const td = cell(empty); td.colSpan = columns;
    const tr = row([td]); tr.setAttribute('data-sort-ignore', ''); rows = [tr];
  }
  body.replaceChildren(...rows);
}

function selectCell(source, field, value, options, label, disabled = false) {
  if (value == null) return cell(null);
  const td = cell(null, false, typeof value === 'boolean' ? Number(value) : value);
  td.textContent = '';
  const form = document.createElement('form');
  form.className = field + '-form'; form.dataset.source = source;
  const select = document.createElement('select'); select.name = field;
  select.setAttribute('aria-label', label + source); select.disabled = disabled;
  options.forEach(([key, text]) => {
    const option = document.createElement('option'); option.value = String(key);
    option.textContent = text; option.selected = String(key) === String(value);
    select.append(option);
  });
  select.value = String(value);
  const feedback = document.createElement('span'); feedback.className = field + '-feedback';
  feedback.setAttribute('aria-live', 'polite'); form.append(select, feedback); td.append(form);
  return td;
}

function bulkCell(source, states) {
  const sortValue = `${String(states.queued).padStart(5, '0')},${String(states.failed).padStart(5, '0')}`;
  const td = cell('', false, sortValue);
  [['queued/cancel', states.queued, 'Cancel queued'], ['failed/retry', states.failed, 'Retry failed']]
    .forEach(([action, count, label]) => {
      const button = document.createElement('button'); button.className = 'bulk-action';
      Object.assign(button.dataset, {source, action, count: String(count)});
      button.disabled = !count; button.textContent = `${label} (${count})`; td.append(button);
    });
  const feedback = document.createElement('span'); feedback.className = 'bulk-feedback';
  feedback.setAttribute('aria-live', 'polite'); td.append(feedback);
  return td;
}

function historyRow(item) {
  const tr = row([cell(item.id, true), cell(item.source), cell(item.weight),
    cell(item.profile), cell(item.model), cell(item.state),
    cell(item.model_turn_display, false, item.model_turn_seconds),
    cell(item.finished_display, true, item.finished), cell(item.attempt_count, true)]);
  tr.dataset.historyId = item.id;
  return tr;
}

function renderDashboard(data) {
  const observation = data.observation || {state: 'live'};
  const error = byId('dashboard-error'), live = byId('dashboard-live');
  timestamp(byId('snapshot-timestamp'), data.timestamp ?? observation.observed_at);
  if (observation.state === 'unavailable') {
    if (live) live.hidden = true;
    document.title = 'Ollama broker queue unavailable';
    error.hidden = false;
    error.textContent = 'Dashboard data unavailable: ' + (observation.reason || 'observer read failed') + '.';
    return false;
  }
  // An unavailable HTML shell can recover if the observer recovered between
  // the HTML and JSON requests. Reload once to obtain the full live shell.
  if (!live) { window.location.reload(); return false; }
  byId('maintenance').hidden = !data.maintenance;
  byId('stale').hidden = observation.state !== 'stale';
  byId('stale').textContent = 'Showing stale data: ' + (observation.reason || 'database observer read unavailable') + '.';
  const overall = data.overall;
  byId('summary').textContent = `Completed: ${overall.states.completed} total · ${overall.completed_last_hour} last hour · ${overall.completed_last_24_hours} last 24 hours.`;
  const sources = [...data.sources].sort((a, b) => {
    const aw = a.scheduler.weight, bw = b.scheduler.weight;
    return (aw == null) - (bw == null) || (aw ?? 0) - (bw ?? 0) ||
      a.source.localeCompare(b.source, undefined, {sensitivity: 'base'});
  });
  fillBody('sources-body', sources.map(item => {
    const s = item.scheduler, states = item.states;
    return row([cell(item.source),
      selectCell(item.source, 'enabled', s.enabled, [[true, 'running'], [false, 'paused']], 'Dispatch enabled for '),
      selectCell(item.source, 'admission', s.admission_allowed, [[true, 'allowed'], [false, 'blocked']], 'Admission for ', data.maintenance),
      selectCell(item.source, 'weight', s.weight, Array.from({length: 10}, (_, i) => [i + 1, String(i + 1)]), 'Weight for '),
      ...['queued', 'running', 'lease', 'retry', 'delayed', 'failed', 'cancelled', 'completed'].map(key => cell(states[key])),
      cell(item.completed_last_hour), cell(item.completed_last_24_hours), bulkCell(item.source, states)]);
  }), 15);
  fillBody('active-body', data.active_jobs.map(item => row([
    cell(item.id, true), cell(item.source), cell(item.weight), cell(item.state),
    timestamp(cell(null), item.started), timestamp(cell(null), item.lease_until),
    cell(item.attempt_count, true), cell(item.retry_count)])), 8);
  const forecast = data.forecast;
  byId('forecast').hidden = !forecast;
  if (forecast) {
    byId('forecast-error').hidden = !forecast.unavailable;
    byId('forecast-live').hidden = Boolean(forecast.unavailable);
    byId('forecast-error').textContent = 'Forecast unavailable: ' + (forecast.reason || 'observer read failed') + '.';
    byId('current-model').textContent = display(forecast.current_model);
    byId('contingency').textContent = (forecast.contingency || '') + '.';
    fillBody('forecast-body', (forecast.next_selections || []).map(item => row([
      cell(item.job_id), cell(item.source), cell(item.profile), cell(item.model), cell(item.weight),
      cell(item.mode), cell(item.reason, true), cell(duration(item.wait_seconds), false, item.wait_seconds)])), 8, 'None queued');
  }
  const history = data.history || [];
  fillBody('history-body', history.map(historyRow), 9);
  const last = history[history.length - 1];
  byId('history-sentinel').dataset.cursor = last ? `${last.finished}:${last.id}` : '';
  document.querySelectorAll('table').forEach(table => TableSort.bind(table));
  bindControls();
  live.hidden = false; error.hidden = true;
  return true;
}

function bindPolicySelect(form, field, requestField = field, resultField = field) {
  const select = form.elements[field], feedback = form.querySelector('.' + field + '-feedback');
  let saved = select.value, desired = saved, saving = false;
  const flush = async () => {
    if (saving) return;
    saving = true;
    while (desired !== saved) {
      const value = desired;
      feedback.className = field + '-feedback'; feedback.textContent = 'Saving…';
      try {
        const response = await fetch('/v1/sources/' + encodeURIComponent(form.dataset.source) + '/' + field, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({[requestField]: field === 'weight' ? Number(value) : value === 'true'})
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || 'save failed');
        saved = String(result[resultField]);
        if (desired === value) { select.value = saved; feedback.textContent = 'Saved'; }
      } catch (error) {
        if (desired === value) {
          select.value = saved; feedback.className = field + '-feedback error';
          feedback.textContent = error.message || 'Save failed'; break;
        }
      }
    }
    saving = false;
  };
  select.addEventListener('change', () => { desired = select.value; flush(); });
}

function bindControls() {
  document.querySelectorAll('.weight-form').forEach(form => bindPolicySelect(form, 'weight'));
  document.querySelectorAll('.enabled-form').forEach(form => bindPolicySelect(form, 'enabled'));
  document.querySelectorAll('.admission-form').forEach(form => bindPolicySelect(form, 'admission', 'allowed', 'admission_allowed'));
  document.querySelectorAll('.bulk-action').forEach(button => button.addEventListener('click', async () => {
    const count = Number(button.dataset.count), source = button.dataset.source, action = button.dataset.action;
    if (!window.confirm(action === 'queued/cancel' ? 'Cancel ' + count + ' queued jobs for ' + source + '?' :
      'Retry ' + count + ' failed jobs for ' + source + '?')) return;
    const feedback = button.parentElement.querySelector('.bulk-feedback');
    button.parentElement.querySelectorAll('button').forEach(item => { item.disabled = true; });
    try {
      const response = await fetch('/v1/sources/' + encodeURIComponent(source) + '/' + action, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({confirm: true})
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'operation failed');
      feedback.textContent = String(result.cancelled ?? result.retried) + ' changed';
    } catch (error) {
      feedback.className = 'bulk-feedback error'; feedback.textContent = error.message || 'Operation failed';
    }
  }));
}

function observeHistory() {
  const sentinel = byId('history-sentinel'), body = byId('history-body');
  let loading = false;
  const ids = new Set([...body.children].map(tr => tr.dataset.historyId));
  if (!('IntersectionObserver' in window)) return;
  new IntersectionObserver(async entries => {
    if (loading || !entries[0].isIntersecting || !sentinel.dataset.cursor) return;
    loading = true;
    try {
      const response = await fetch('/v1/history?limit=30&cursor=' + encodeURIComponent(sentinel.dataset.cursor));
      const page = await response.json();
      if (!response.ok) throw new Error(page.error || 'history read failed');
      (page.items || []).forEach(item => {
        if (ids.has(item.id)) return;
        ids.add(item.id);
        const tr = historyRow(item); body.append(tr);
        byId('history-table')._tableSort.addRow(tr);
      });
      sentinel.dataset.cursor = page.next_cursor || '';
      byId('history-error').textContent = '';
    } catch (error) {
      byId('history-error').textContent = error.message || 'History unavailable';
    } finally { loading = false; }
  }).observe(sentinel);
}

async function loadDashboard() {
  try {
    const response = await fetch('/v1/dashboard', {cache: 'no-cache'});
    const data = await response.json();
    if (!response.ok && data.observation?.state !== 'unavailable') throw new Error('dashboard read failed');
    if (renderDashboard(data)) observeHistory();
  } catch (error) {
    const live = byId('dashboard-live'); if (live) live.hidden = true;
    byId('dashboard-error').hidden = false;
    byId('dashboard-error').textContent = 'Dashboard data unavailable: ' + (error.message || 'request failed') + '.';
  }
}

const dashboardReady = loadDashboard();
