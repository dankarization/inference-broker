// Execute the shipped HTML and scripts against a small dependency-free DOM.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');

// A browser-like scroll reset is injected only by the scroll-preservation check,
// so the DOM shim stays passive everywhere else.
let disturbScroll = null;

class Element {
  constructor(tag) {
    this.tagName = tag.toUpperCase(); this.children = []; this.dataset = {};
    this.attrs = {}; this.events = {}; this.className = ''; this.hidden = false;
    this._text = ''; this.value = ''; this.disabled = false;
    this.scrollLeft = 0;
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(''); }
  append(...nodes) {
    for (const node of nodes) {
      if (node.parentElement) {
        const old = node.parentElement.children; old.splice(old.indexOf(node), 1);
      }
      this.children.push(node); node.parentElement = this;
    }
  }
  appendChild(node) { this.append(node); return node; }
  replaceChildren(...nodes) {
    this.children.forEach(node => { node.parentElement = null; });
    this.children = []; this._text = ''; this.append(...nodes);
    if (disturbScroll) disturbScroll(this);
  }
  setAttribute(key, value) {
    value = String(value); this.attrs[key] = value;
    if (key === 'class') this.className = value;
    if (key === 'hidden') this.hidden = true;
    if (['name', 'id', 'value'].includes(key)) this[key] = value;
    if (key.startsWith('data-')) this.dataset[key.slice(5).replace(/-([a-z])/g, (_, x) => x.toUpperCase())] = value;
  }
  getAttribute(key) { return this.attrs[key] ?? null; }
  hasAttribute(key) { return key in this.attrs; }
  get tBodies() { return this.children.filter(c => c.tagName === 'TBODY'); }
  get rows() { return this.children.filter(c => c.tagName === 'TR'); }
  get cells() { return this.children.filter(c => c.tagName === 'TD'); }
  get options() { return this.children; }
  get selectedIndex() { return this.children.findIndex(c => c.value === this.value); }
  get elements() {
    return Object.fromEntries(this.querySelectorAll('select').map(c => [c.name, c]));
  }
  matches(selector) {
    if (selector.startsWith('#')) return this.id === selector.slice(1);
    if (selector.startsWith('.')) return this.className.split(' ').includes(selector.slice(1));
    const [, tag, attr] = selector.match(/^(\w+)(?:\[([^\]]+)\])?$/) || [];
    return this.tagName === tag?.toUpperCase() && (!attr || this.hasAttribute(attr));
  }
  querySelectorAll(selector) {
    const parts = selector.split(' ');
    const all = this.children.flatMap(c => [c, ...c.querySelectorAll('*')]);
    if (selector === '*') return all;
    return all.filter(c => {
      if (!c.matches(parts[parts.length - 1])) return false;
      if (parts.length === 1) return true;
      for (let parent = c.parentElement; parent; parent = parent.parentElement)
        if (parent.matches(parts[0])) return true;
      return false;
    });
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  addEventListener(event, callback) { (this.events[event] ||= []).push(callback); }
  async fire(event) { for (const fn of this.events[event] || []) await fn(); }
}

function parseHTML(html) {
  const document = new Element('document'), stack = [document];
  document.hidden = false; document.scrollX = 0; document.scrollY = 0;
  for (const token of html.match(/<[^>]+>|[^<]+/g)) {
    if (token.startsWith('<!')) continue;
    if (token.startsWith('</')) { stack.pop(); continue; }
    if (!token.startsWith('<')) { stack[stack.length - 1]._text += token; continue; }
    const tag = token.match(/^<(\w+)/)[1], node = new Element(tag);
    const attrs = token.slice(tag.length + 1, -1);
    for (const attr of attrs.matchAll(/([\w-]+)(?:=(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g))
      node.setAttribute(attr[1], attr[2] ?? attr[3] ?? attr[4] ?? '');
    stack[stack.length - 1].append(node);
    if (!['meta', 'link', 'br', 'input'].includes(tag)) stack.push(node);
  }
  document.createElement = tag => new Element(tag);
  document.getElementById = id => document.querySelector('#' + id);
  return document;
}

const stamp = Date.now() / 1000;
const history = (id, turn = 12.25) => ({
  id, source: 'source', weight: 3, profile: 'interactive', model: 'test-model', state: turn == null ? 'cancelled' : 'completed',
  model_turn_display: turn == null ? '—' : '00:12', model_turn_seconds: turn,
  finished: stamp, finished_display: '28/10/2026, 17:52:17', attempt_count: 2
});
const source = (name, weight) => ({
  source: name, scheduler: {enabled: true, admission_allowed: false, weight},
  states: {queued: 2, running: 0, lease: 0, retry: 0, delayed: 0, failed: 1, cancelled: 0, completed: 0},
  completed_last_hour: 0, completed_last_24_hours: 0
});
const fixture = {
  timestamp: stamp, maintenance: true, observation: {state: 'stale', reason: '<locked>'},
  sources: [source('weight-10', 10), source('unknown', null), source('weight-1-b', 1),
    source('weight-2', 2), source('weight-1-a', 1)],
  active_jobs: [{id: '<script>job</script>', source: 'source', weight: 3, state: 'running',
    started: 1787992200, lease_until: null, attempt_count: 1, retry_count: 0}],
  overall: {states: {completed: 0}, completed_last_hour: 0, completed_last_24_hours: 0},
  history: [history('job-<one>')],
  forecast: {current_model: 'model', contingency: '<contingent>', next_selections: [
    {job_id: 'next', source: 'source', profile: 'interactive', model: 'test-model',
      weight: 1, mode: 'weight_priority', reason: 'lowest weight', wait_seconds: 150.2}
  ]}
};

async function boot(data, unavailableShell = false, fail = false, noObserver = false) {
  const document = parseHTML(fs.readFileSync(path.join(root, 'broker/static',
    unavailableShell ? 'dashboard-unavailable.html' : 'dashboard.html'), 'utf8'));
  const requests = [], observerCallbacks = [], timers = [], observations = [];
  const context = vm.createContext({
    document, Intl, Date, console, Set, Map,
    location: {reload() { context.reloaded = true; }},
    confirm: () => true,
    IntersectionObserver: class {
      constructor(callback) { observerCallbacks.push(callback); }
      observe() { observations.push(true); }
    },
    setInterval: (callback, ms) => {
      const timer = {id: timers.length + 1, callback, ms, active: true};
      timers.push(timer); return timer.id;
    },
    clearInterval: id => { const timer = timers.find(item => item.id === id); if (timer) timer.active = false; },
    fetch: async (url, options) => {
      requests.push({url, options});
      if (fail) throw new Error('offline');
      return {ok: data.observation.state !== 'unavailable', json: async () => data};
    }
  });
  context.window = context;
  if (noObserver) delete context.IntersectionObserver;
  context.scrollTo = (x, y) => { context.scrollX = x; context.scrollY = y; };
  vm.runInContext(fs.readFileSync(path.join(root, 'broker/table_sort.js'), 'utf8'), context);
  vm.runInContext(fs.readFileSync(path.join(root, 'broker/static/dashboard.js'), 'utf8'), context);
  await vm.runInContext('dashboardReady', context);
  return {context, document, requests, observerCallbacks, timers, observations,
    active: () => timers.filter(timer => timer.active),
    flush: async (turns = 8) => { for (let i = 0; i < turns; i++) await new Promise(resolve => setImmediate(resolve)); },
    id: id => document.getElementById(id)};
}

const durationCases = JSON.parse(
  fs.readFileSync(path.join(root, 'tests/duration_format_cases.json'), 'utf8')).cases;

(async () => {
  const page = await boot(fixture);
  const {context, id, document} = page;
  assert.equal(page.requests[0].url, '/v1/dashboard');
  assert.equal(id('dashboard-live').hidden, false);
  assert.equal(id('dashboard-error').hidden, true);
  assert.equal(document.querySelector('header #queue-pause'), id('queue-pause'));
  assert.equal(document.querySelectorAll('#queue-pause').length, 1);
  assert.match(document.querySelector('footer').textContent, /Data refreshes every 30 seconds/);
  assert.doesNotMatch(document.querySelector('.wrap').textContent, /Data refreshes every 30 seconds/);
  assert.doesNotMatch(document.querySelector('body').textContent, /Finished: Asia\/Tbilisi/);
  assert.equal(id('history-error').hidden, true);
  assert.equal(id('history-error').textContent, '');
  for (const tableId of ['active-table', 'forecast-table', 'history-table']) {
    assert.equal(id(tableId).querySelector('thead .compact'), null, tableId);
  }
  assert.equal(id('stale').hidden, false);
  assert.equal(id('stale').textContent, 'Showing stale data: <locked>.');
  assert.equal(id('maintenance').hidden, false);
  assert.match(id('summary').textContent, /^Completed: 0 total/);
  assert.deepEqual(id('sources-body').rows.map(tr => tr.cells[0].textContent),
    ['weight-1-a', 'weight-1-b', 'weight-2', 'weight-10', 'unknown']);
  assert.equal(id('sources-body').rows[0].cells.length, 15);
  assert.equal(document.querySelector('.admission-form').elements.admission.disabled, true);
  assert.equal(document.querySelector('.weight-form').elements.weight.options.length, 10);
  assert.equal(id('active-body').rows[0].cells[0].textContent, '<script>job</script>');
  assert.equal(id('active-body').rows[0].cells[0].children.length, 0);
  assert.equal(id('active-body').rows[0].cells[4].textContent, '2026-08-29 12:30:00 UTC+04:00 (Asia/Tbilisi)');
  assert.equal(id('active-body').rows[0].cells[4].title, '2026-08-29T08:30:00Z (epoch 1787992200)');
  assert.equal(id('active-body').rows[0].cells[5].textContent, '—');
  assert.equal(id('forecast-body').rows[0].cells[2].textContent, 'interactive');
  assert.equal(id('forecast-body').rows[0].cells[7].textContent, '02:30');
  assert.equal(id('forecast-body').rows[0].cells[7].getAttribute('data-sort-value'), '150.2');
  const initial = id('history-body').rows[0].cells.map(td => td.textContent);
  assert.deepEqual(initial, ['job-<one>', 'source', '3', 'interactive', 'test-model',
    'completed', '00:12', '28/10/2026, 17:52:17', '2']);
  assert.deepEqual(id('history-body').rows[0].cells.map((td, i) => td.className === 'compact' ? i : null)
    .filter(i => i != null), [0, 7, 8]);

  // (i) One duration format, shared with the Python helper through a fixture.
  for (const [value, expected] of durationCases)
    assert.equal(context.duration(value), expected, `duration(${value})`);
  assert.equal(context.duration(150.2), '02:30');

  // (ii) No meta refresh anywhere; the background timer replaces it.
  for (const name of ['dashboard.html', 'dashboard-unavailable.html']) {
    const html = fs.readFileSync(path.join(root, 'broker/static', name), 'utf8');
    assert.doesNotMatch(html, /http-equiv/i, name);
    assert.doesNotMatch(html, /content=30/i, name);
  }
  assert.equal(page.active().length, 1);
  assert.equal(page.active()[0].ms, 30000);
  assert.equal(context.reloaded, undefined);

  // Real sorting click cycle, including reset to the original numeric-weight order.
  const weightHead = id('sources-table').querySelectorAll('thead th[data-sort-key]')[3];
  await weightHead.querySelector('button').fire('click');
  assert.deepEqual(id('sources-body').rows.map(tr => tr.cells[0].textContent),
    ['weight-10', 'weight-2', 'weight-1-a', 'weight-1-b', 'unknown']);
  assert.equal(weightHead.getAttribute('aria-sort'), 'descending');
  await weightHead.querySelector('button').fire('click');
  assert.equal(weightHead.getAttribute('aria-sort'), 'ascending');
  await weightHead.querySelector('button').fire('click');
  assert.equal(id('sources-body').rows[0].cells[0].textContent, 'weight-1-a');

  // (iii) A background tick patches the DOM in place: no reload, and the chosen
  // sort plus the scroll positions survive the row replacement.
  await weightHead.querySelector('button').fire('click');
  assert.equal(weightHead.getAttribute('aria-sort'), 'descending');
  const box = document.querySelector('#sources-table').parentElement;
  assert.equal(box.className, 'table-scroll');
  assert.deepEqual(document.querySelectorAll('.table-scroll').map(node => node.tagName),
    ['DIV', 'DIV', 'DIV', 'DIV']);
  box.scrollLeft = 640; context.scrollY = 2100; context.scrollX = 12;
  const before = page.requests.length;
  disturbScroll = () => { box.scrollLeft = 0; context.scrollY = 0; context.scrollX = 0; };
  await page.active()[0].callback();
  disturbScroll = null;
  assert.equal(page.requests.length, before + 1);
  assert.equal(page.requests[page.requests.length - 1].url, '/v1/dashboard');
  assert.equal(context.reloaded, undefined);
  assert.deepEqual(id('sources-body').rows.map(tr => tr.cells[0].textContent),
    ['weight-10', 'weight-2', 'weight-1-a', 'weight-1-b', 'unknown']);
  assert.equal(weightHead.getAttribute('aria-sort'), 'descending');
  assert.equal(box.scrollLeft, 640);
  assert.equal(context.scrollY, 2100);
  assert.equal(context.scrollX, 12);
  // The sort handle is reused, so a click still cycles after the refresh.
  await weightHead.querySelector('button').fire('click');
  assert.equal(weightHead.getAttribute('aria-sort'), 'ascending');

  // (ii) A hidden tab stops polling; returning to the foreground resumes and
  // refreshes once, without reloading the page.
  document.hidden = true; await document.fire('visibilitychange');
  assert.equal(page.active().length, 0);
  await page.flush(); assert.equal(page.requests.length, before + 1);
  const hidden = page.requests.length;
  document.hidden = false; await document.fire('visibilitychange');
  assert.equal(page.active().length, 1);
  await page.flush();
  assert.equal(page.requests.length, hidden + 1);
  assert.equal(page.requests[page.requests.length - 1].url, '/v1/dashboard');
  assert.equal(context.reloaded, undefined);

  // Appended history shares the initial renderer, de-duplicates and recovers after failure.
  const beforeFailedPage = page.observations.length;
  context.fetch = async () => { throw new Error('history offline'); };
  await page.observerCallbacks[0]([{isIntersecting: true}]);
  assert.equal(page.observations.length, beforeFailedPage, 'failed history must not rearm a tight retry loop');
  assert.match(id('history-error').textContent, /history offline/);
  assert.equal(id('history-error').hidden, false);
  context.fetch = async () => ({ok: true, json: async () => ({
    items: [history('job-<one>'), history('next-history'), history('cancelled', null)], next_cursor: null
  })});
  await page.observerCallbacks[0]([{isIntersecting: true}]);
  assert.equal(id('history-body').rows.length, 3);
  assert.deepEqual(id('history-body').rows[1].cells.map(td => td.textContent).slice(1), initial.slice(1));
  assert.equal(id('history-body').rows[2].cells[6].textContent, '—');
  assert.equal(id('history-sentinel').dataset.cursor, '');
  assert.equal(id('history-error').textContent, '');
  assert.equal(id('history-error').hidden, true);

  // Hidden/overlapping reads cannot issue a second network request.
  const guarded = await boot(fixture);
  let finishRead, reads = 0;
  guarded.context.fetch = () => {
    reads++;
    return new Promise(resolve => { finishRead = resolve; });
  };
  guarded.document.hidden = true;
  await guarded.context.loadDashboard();
  await guarded.observerCallbacks[0]([{isIntersecting: true}]);
  assert.equal(reads, 0);
  guarded.document.hidden = false;
  const pendingRead = guarded.context.loadDashboard();
  await guarded.context.loadDashboard();
  await guarded.observerCallbacks[0]([{isIntersecting: true}]);
  assert.equal(reads, 1);
  finishRead({ok: true, json: async () => fixture});
  await pendingRead;
  let finishPage;
  guarded.context.fetch = () => {
    reads++;
    return new Promise(resolve => { finishPage = resolve; });
  };
  const pendingPage = guarded.observerCallbacks[0]([{isIntersecting: true}]);
  await guarded.observerCallbacks[0]([{isIntersecting: true}]);
  await guarded.context.loadDashboard();
  assert.equal(reads, 2);
  finishPage({ok: true, json: async () => ({items: [history('older')], next_cursor: 'old:cursor'})});
  await pendingPage;
  guarded.context.fetch = async () => ({ok: true, json: async () => fixture});
  await guarded.context.loadDashboard();
  assert.equal(guarded.id('history-body').rows.length, 2);
  assert.equal(guarded.id('history-sentinel').dataset.cursor, 'old:cursor');

  // Policy controls retain serialized latest-desired writes and failure recovery.
  const weightForm = document.querySelector('.weight-form'), select = weightForm.elements.weight;
  const sent = [], pending = [];
  context.fetch = (url, options) => {
    sent.push({url, body: JSON.parse(options.body)});
    return new Promise(resolve => pending.push(resolve));
  };
  const tick = () => new Promise(resolve => setImmediate(resolve));
  select.value = '2'; await select.fire('change'); select.value = '4'; await select.fire('change');
  assert.equal(sent.length, 1); assert.equal(sent[0].body.weight, 2);
  pending.shift()({ok: true, json: async () => ({weight: 2})}); await tick();
  assert.equal(sent.length, 2); assert.equal(sent[1].body.weight, 4);
  pending.shift()({ok: false, json: async () => ({error: 'denied'})}); await tick();
  assert.equal(select.value, '2');
  select.value = '7'; await select.fire('change');
  assert.equal(sent.length, 3);
  pending.shift()({ok: true, json: async () => ({weight: 7})}); await tick();
  assert.equal(weightForm.querySelector('.weight-feedback').textContent, 'Saved');

  const admission = document.querySelector('.admission-form').elements.admission;
  admission.value = 'true'; await admission.fire('change');
  assert.equal(sent[3].body.allowed, true);
  pending.shift()({ok: true, json: async () => ({admission_allowed: true})}); await tick();
  const enabled = document.querySelector('.enabled-form').elements.enabled;
  enabled.value = 'false'; await enabled.fire('change');
  assert.equal(sent[4].body.enabled, false);
  pending.shift()({ok: true, json: async () => ({enabled: false})}); await tick();

  const button = document.querySelector('.bulk-action'), clicking = button.fire('click');
  assert.deepEqual(sent[5].body, {confirm: true});
  assert.match(sent[5].url, /queued\/cancel$/);
  pending.shift()({ok: true, json: async () => ({cancelled: 2})}); await clicking;
  assert.equal(button.parentElement.querySelector('.bulk-feedback').textContent, '2 changed');

  const empty = await boot({...fixture, sources: [], active_jobs: [], history: [], forecast: {unavailable: true}});
  assert.equal(empty.id('sources-body').rows[0].cells[0].textContent, 'None');
  assert.equal(empty.id('sources-body').rows[0].cells[0].colSpan, 15);
  assert.equal(empty.id('history-sentinel').dataset.cursor, '');
  assert.equal(empty.id('forecast-live').hidden, true);
  assert.equal(empty.id('forecast-error').hidden, false);

  for (const unavailableShell of [false, true]) {
    const broken = await boot({observation: {state: 'unavailable', reason: '<locked>', observed_at: stamp}}, unavailableShell);
    assert.match(broken.id('dashboard-error').textContent, /Dashboard data unavailable: <locked>/);
    assert.equal(broken.observerCallbacks.length, 0);
    assert.equal(broken.active().length, 1);
    if (!unavailableShell) assert.equal(broken.id('dashboard-live').hidden, true);
  }
  const offline = await boot(fixture, false, true);
  assert.equal(offline.id('dashboard-live').hidden, true);
  assert.match(offline.id('dashboard-error').textContent, /offline/);
  const legacy = await boot(fixture, false, false, true);
  assert.equal(legacy.id('dashboard-error').hidden, true);
  assert.equal(legacy.observerCallbacks.length, 0);
  const recovered = await boot(fixture, true);
  assert.equal(recovered.context.reloaded, true);
  assert.equal(recovered.active().length, 0);
  const pausedQueue = await boot({...fixture, queue_paused: true});
  assert.equal(pausedQueue.id('queue-pause').textContent, 'Resume queue');
  assert.equal(pausedQueue.id('queue-pause').dataset.paused, 'true');
  assert.match(pausedQueue.id('queue-pause-status').textContent, /active job finishes/);
  assert.equal(id('queue-pause').textContent, 'Pause queue');
  console.log('dashboard frontend contract PASS');
})().catch(error => { console.error(error); process.exitCode = 1; });
