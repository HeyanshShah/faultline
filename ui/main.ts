import {
  BUGS,
  type BugId,
  causalSlice,
  decodeRepro,
  encodeRepro,
  makeScenario,
  procName,
  runScenario,
  type RunResult,
  type Scenario,
  type Story,
  tellStory,
  type TraceMsg,
  type TraceStep,
} from '../src/index';
import type { HuntEvent, HuntRequest } from './worker';

declare const __WORKER_SRC__: string;
declare const __DEMOS__: Record<string, { repro: string; seed: number; before: { faults: number; steps: number } }>;

type Filter = 'key' | 'causal' | 'all';
type Impl = BugId | 'correct';

const $ = <T extends Element = HTMLElement>(sel: string) => document.querySelector(sel) as unknown as T;
const SVGNS = 'http://www.w3.org/2000/svg';
const fmt = (n: number) => Math.round(n).toLocaleString('en-US');

function el<K extends keyof SVGElementTagNameMap>(tag: K, attrs: Record<string, string | number> = {}, text?: string): SVGElementTagNameMap[K] {
  const e = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
  if (text !== undefined) e.textContent = text;
  return e;
}

// ---- state --------------------------------------------------------------------

interface Loaded {
  scenario: Scenario;
  run: RunResult;
  steps: TraceStep[];
  msgs: TraceMsg[];
  slice: Set<number>;
  story: Story | null;
  endTime: number;
  origin: string;
  shrinkInfo: { before: { faults: number; steps: number }; after: { faults: number; steps: number }; runs?: number } | null;
}

const state = {
  impl: 'stale-read' as Impl,
  loaded: null as Loaded | null,
  filter: 'key' as Filter,
  idx: 0,
  t: 0,
  playing: false,
  speed: 60,
  worker: null as Worker | null,
};

const TIERS: Record<BugId, string> = {
  'stale-read': 'subtle',
  'forget-vote': 'rare',
  'no-uptodate-check': 'easy',
  'truncate-always': 'needs reordering',
};

// ---- loading a run ------------------------------------------------------------

function load(scenario: Scenario, origin: string, shrinkInfo: Loaded['shrinkInfo']): void {
  const run = runScenario(scenario, { record: true });
  const trace = run.trace!;
  const v = run.violation;
  const slice = v ? causalSlice(trace, v.step) : new Set(trace.steps.map((s) => s.step));
  const story = v ? tellStory(trace, v, run.linearizability) : null;
  const last = trace.steps[trace.steps.length - 1];
  state.loaded = { scenario, run, steps: trace.steps, msgs: trace.msgs, slice, story, endTime: last.time, origin, shrinkInfo };
  ($('#repro') as HTMLTextAreaElement).value = encodeRepro(scenario);
  renderVerdict();
  renderTimelineStatic();
  renderHistory();
  renderEventList();
  // Open on the moment of failure so the first frame shows the bug; Play replays from the start.
  if (v) seekStep(trace.steps.findIndex((s) => s.step === v.step));
  else seekStep(0);
}

function visible(s: TraceStep): boolean {
  const L = state.loaded!;
  const v = L.run.violation;
  if (state.filter === 'all' || !v) return true;
  if (s.step > v.step) return false;
  if (state.filter === 'causal') return L.slice.has(s.step);
  return L.slice.has(s.step) && L.story!.keySteps.has(s.step);
}

// ---- verdict ------------------------------------------------------------------

function renderVerdict(): void {
  const L = state.loaded!;
  const v = L.run.violation;
  const box = $('#verdict');
  box.classList.toggle('clean', !v);
  $('#v-pill').textContent = v ? v.kind.replace(/-/g, ' ') : 'no violation';
  $('#v-meta').textContent = `${L.origin} · seed ${L.scenario.seed} · ${L.scenario.nodes} servers, ${L.scenario.clients} clients`;
  $('#v-text').textContent = v
    ? L.story!.explanation
    : `This run finished cleanly: every safety invariant held and all ${fmt(L.run.stats.opsOk)} completed client operations have a legal linearization.`;
  const sh = $('#v-shrink');
  sh.innerHTML = '';
  const add = (html: string) => {
    const s = document.createElement('span');
    s.innerHTML = html;
    sh.append(s);
  };
  if (L.shrinkInfo) {
    add(`faults <b>${L.shrinkInfo.before.faults} → ${L.shrinkInfo.after.faults}</b>`);
    if (L.shrinkInfo.after.steps < L.shrinkInfo.before.steps) add(`events <b>${fmt(L.shrinkInfo.before.steps)} → ${fmt(L.shrinkInfo.after.steps)}</b>`);
    if (L.shrinkInfo.runs) add(`shrunk in <b>${L.shrinkInfo.runs}</b> re-runs`);
  }
  if (v) add(`<b>${L.slice.size}</b> of ${fmt(L.steps.length)} events causally upstream`);
  add(`${fmt(L.run.stats.messages)} messages · ${L.run.stats.elections} elections · ${fmt(L.run.stats.opsOk)} ops`);
}

// ---- cluster view -------------------------------------------------------------

const CX = 320;
const CY = 192;
const R = 140;

function serverPos(i: number, n: number): [number, number] {
  const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
  return [CX + R * Math.cos(a), CY + R * 0.92 * Math.sin(a)];
}

function clientPos(c: number, count: number): [number, number] {
  const span = Math.min(360, 110 * (count - 1));
  return [CX - span / 2 + (count === 1 ? 0 : (c * span) / (count - 1)), 412];
}

function pos(proc: number): [number, number] {
  const sc = state.loaded!.scenario;
  return proc < sc.nodes ? serverPos(proc, sc.nodes) : clientPos(proc - sc.nodes, sc.clients);
}

function connected(groups: number[], a: number, b: number): boolean {
  return groups[a] === groups[b] || groups[a] === -1 || groups[b] === -1;
}

function msgClass(m: TraceMsg): string {
  if (m.type === 'RequestVote' || m.type === 'RequestVoteReply') return 'vote';
  if (m.type === 'AppendEntries' || m.type === 'AppendEntriesReply') return 'append';
  return 'client';
}

/** Messages in flight at the current step and time. */
function inFlight(): { m: TraceMsg; p: number }[] {
  const L = state.loaded!;
  const cur = L.steps[state.idx];
  const t = state.t;
  const out: { m: TraceMsg; p: number }[] = [];
  let lo = 0;
  let hi = L.msgs.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (L.msgs[mid].sendStep <= cur.step) lo = mid + 1;
    else hi = mid;
  }
  for (let i = lo - 1; i >= 0; i--) {
    const m = L.msgs[i];
    if (m.sendTime < t - 300) break;
    const arrived = m.deliverStep !== null ? m.deliverStep <= cur.step : m.deliverTime <= t;
    if (arrived) continue;
    const span = Math.max(1, m.deliverTime - m.sendTime);
    out.push({ m, p: Math.min(0.97, Math.max(0, (t - m.sendTime) / span)) });
  }
  return out;
}

function renderCluster(): void {
  const L = state.loaded!;
  const svg = $<SVGSVGElement>('#cluster');
  const st = L.steps[state.idx];
  const n = L.scenario.nodes;
  const v = L.run.violation;
  const atViolation = v !== null && st.step === v.step;
  svg.replaceChildren();

  const links = el('g');
  for (let a = 0; a < n; a++)
    for (let b = a + 1; b < n; b++) {
      const [x1, y1] = serverPos(a, n);
      const [x2, y2] = serverPos(b, n);
      links.append(el('line', { x1, y1, x2, y2, class: connected(st.groups, a, b) ? 'link' : 'link cut' }));
    }
  svg.append(links);

  for (let c = 0; c < L.scenario.clients; c++) {
    const [x, y] = clientPos(c, L.scenario.clients);
    const g = el('g', { class: 'client' });
    g.append(el('rect', { x: x - 18, y: y - 12, width: 36, height: 24, rx: 4 }), el('text', { x, y: y + 4 }, procName(n + c, n)));
    svg.append(g);
  }

  for (const { m, p } of inFlight()) {
    const [x1, y1] = pos(m.from);
    const [x2, y2] = pos(m.to);
    const lost = m.fate === 'dropped' || m.fate === 'partitioned' || m.fate === 'dead';
    const c = el('circle', { cx: x1 + (x2 - x1) * p, cy: y1 + (y2 - y1) * p, r: 4.5, class: `msg ${lost ? 'lost' : msgClass(m)}` });
    c.append(el('title', {}, `${procName(m.from, n)} → ${procName(m.to, n)}: ${m.label}${lost ? ` (${m.fate})` : ''}`));
    svg.append(c);
  }

  st.nodes.forEach((node, i) => {
    const [x, y] = serverPos(i, n);
    const cls = !node.alive ? 'down' : node.role;
    const boom = atViolation && (v!.node === i || (v!.kind === 'election-safety' && node.role === 'leader' && node.term === v!.term));
    const g = el('g', { class: `node ${cls}${boom ? ' boom' : ''}` });
    if (boom) g.append(el('circle', { cx: x, cy: y, r: 38, class: 'ring' }));
    g.append(el('circle', { cx: x, cy: y, r: 29, class: 'body' }));
    g.append(el('text', { x, y: y + 5, class: 'name' }, `n${i}`));
    const info = node.alive ? `term ${node.term} · ${node.role}` : `down · term ${node.term} on disk`;
    g.append(el('text', { x, y: y + (y > CY + 40 ? 48 : -40), class: 'info' }, info));
    svg.append(g);
  });
}

// ---- logs ---------------------------------------------------------------------

function termColor(term: number): string {
  return `var(--t${term % 8})`;
}

function renderLogs(): void {
  const L = state.loaded!;
  const st = L.steps[state.idx];
  const v = L.run.violation;
  const hot = v && (v.kind === 'leader-completeness' || v.kind === 'state-machine-safety') ? v.index : undefined;
  const maxLen = Math.max(1, ...st.nodes.map((nd) => nd.log.length));
  const WINDOW = 30;
  const from = Math.max(1, Math.min(maxLen - WINDOW + 1, hot !== undefined ? Math.max(1, hot - WINDOW + 6) : maxLen));
  const box = $('#logs');
  box.replaceChildren();
  st.nodes.forEach((nd, i) => {
    const row = document.createElement('div');
    row.className = `logrow${nd.alive ? '' : ' down'}`;
    const who = document.createElement('div');
    who.className = 'who';
    who.innerHTML = `n${i}<small>${nd.alive ? `${nd.log.length} entries` : 'down'}</small>`;
    const cells = document.createElement('div');
    cells.className = 'cells';
    if (from > 1) {
      const off = document.createElement('span');
      off.className = 'offset';
      off.textContent = `#${from}`;
      cells.append(off);
    }
    for (let k = from; k < from + WINDOW && k <= maxLen; k++) {
      const c = document.createElement('div');
      const term = nd.log[k - 1];
      if (term === undefined) {
        c.className = `cell gap${k === hot ? ' hot' : ''}`;
      } else {
        c.className = `cell${k > nd.commit ? ' unc' : ''}${k === hot ? ' hot' : ''}`;
        c.style.background = termColor(term);
        c.title = `#${k} term ${term}${k <= nd.commit ? ', committed' : ''}`;
      }
      cells.append(c);
    }
    row.append(who, cells);
    box.append(row);
  });

  const now = $('#now');
  now.replaceChildren();
  const lbl = document.createElement('div');
  lbl.className = `lbl${st.kind === 'fault' ? ' fault' : ''}`;
  lbl.textContent = `${st.time}ms · ${st.label}`;
  now.append(lbl);
  for (const note of st.notes) {
    const d = document.createElement('div');
    d.className = 'note';
    d.textContent = `↳ ${note.text}`;
    now.append(d);
  }
  if (v && st.step === v.step) {
    const d = document.createElement('div');
    d.className = 'fault';
    d.textContent = `✕ ${v.message}`;
    now.append(d);
  }
}

// ---- timeline -----------------------------------------------------------------

const TL = { w: 1000, laneH: 14, top: 18, left: 34 };

function tx(t: number): number {
  const L = state.loaded!;
  return TL.left + ((TL.w - TL.left - 8) * t) / Math.max(1, L.endTime);
}

function renderTimelineStatic(): void {
  const L = state.loaded!;
  const n = L.scenario.nodes;
  const svg = $<SVGSVGElement>('#timeline');
  const h = TL.top + n * TL.laneH + 22;
  svg.setAttribute('viewBox', `0 0 ${TL.w} ${h}`);
  svg.style.height = `${Math.round(h * 1.05)}px`;
  svg.replaceChildren();

  // partition bands
  let bandStart: number | null = null;
  for (const s of L.steps) {
    const split = !s.groups.every((g) => g === s.groups[0]);
    if (split && bandStart === null) bandStart = s.time;
    if (!split && bandStart !== null) {
      svg.append(el('rect', { x: tx(bandStart), y: TL.top - 4, width: Math.max(1, tx(s.time) - tx(bandStart)), height: n * TL.laneH + 8, class: 'band' }));
      bandStart = null;
    }
  }
  if (bandStart !== null) svg.append(el('rect', { x: tx(bandStart), y: TL.top - 4, width: tx(L.endTime) - tx(bandStart), height: n * TL.laneH + 8, class: 'band' }));

  for (let i = 0; i < n; i++) {
    const y = TL.top + i * TL.laneH;
    svg.append(el('text', { x: 4, y: y + 10, class: 'lane-label' }, `n${i}`));
    let segStart = 0;
    let cur = 'follower';
    const flush = (end: number) => {
      svg.append(el('rect', { x: tx(segStart), y: y + 2, width: Math.max(0.8, tx(end) - tx(segStart)), height: TL.laneH - 4, class: `seg-${cur}` }));
    };
    for (const s of L.steps) {
      const nd = s.nodes[i];
      const role = nd.alive ? nd.role : 'down';
      if (role !== cur) {
        flush(s.time);
        segStart = s.time;
        cur = role;
      }
    }
    flush(L.endTime);
  }

  for (const s of L.steps) {
    if (s.kind !== 'fault') continue;
    const x = tx(s.time);
    svg.append(el('line', { x1: x, x2: x, y1: 2, y2: TL.top - 4, class: 'tick-fault' }));
  }
  svg.append(el('text', { x: 4, y: 11, class: 'lane-label' }, 'faults'));
  const axisY = TL.top + n * TL.laneH + 16;
  for (let k = 0; k <= 4; k++) {
    const t = (L.endTime * k) / 4;
    svg.append(el('text', { x: Math.min(TL.w - 40, tx(t)), y: axisY, class: 'lane-label' }, `${Math.round(t)}ms`));
  }
  const v = L.run.violation;
  if (v) {
    const x = tx(v.time);
    svg.append(el('path', { d: `M${x} ${TL.top - 12} l6 6 l-6 6 l-6 -6 z`, class: 'vmark' }));
  }
  const ph = el('line', { x1: 0, x2: 0, y1: 0, y2: TL.top + n * TL.laneH + 4, class: 'playhead', id: 'playhead' });
  svg.append(ph);
}

function renderPlayhead(): void {
  const x = tx(state.t);
  const ph = document.getElementById('playhead');
  ph?.setAttribute('x1', String(x));
  ph?.setAttribute('x2', String(x));
  const L = state.loaded!;
  $('#clock').textContent = `${Math.round(state.t)} / ${L.endTime} ms · event ${L.steps[state.idx].step}`;
}

// ---- events list ----------------------------------------------------------------

let eventRows: { step: number; idx: number; el: HTMLElement }[] = [];

function renderEventList(): void {
  const L = state.loaded!;
  const box = $('#events');
  box.replaceChildren();
  eventRows = [];
  const frag = document.createDocumentFragment();
  L.steps.forEach((s, idx) => {
    if (!visible(s)) return;
    const row = document.createElement('div');
    const isV = L.run.violation?.step === s.step;
    row.className = `ev${s.kind === 'fault' ? ' fault' : ''}${isV ? ' boom' : ''}`;
    const notes = s.notes.filter((n) => state.filter === 'all' || n.kind !== 'commit');
    row.innerHTML = `<span class="t">${s.time}ms</span><span class="l"></span>`;
    const l = row.querySelector('.l')!;
    l.textContent = s.label;
    for (const note of notes) {
      const span = document.createElement('span');
      span.className = 'n';
      span.textContent = `↳ ${note.text}`;
      l.append(span);
    }
    if (isV) {
      const span = document.createElement('span');
      span.className = 'n';
      span.textContent = `✕ ${L.run.violation!.kind}`;
      l.append(span);
    }
    row.addEventListener('click', () => {
      pause();
      seekStep(idx);
    });
    frag.append(row);
    eventRows.push({ step: s.step, idx, el: row });
  });
  box.append(frag);
  const names: Record<Filter, string> = { key: 'key moments', causal: 'causally upstream of the violation', all: 'all events' };
  $('#ev-count').textContent = `${eventRows.length} ${names[state.filter]}`;
  document.querySelectorAll<HTMLButtonElement>('.seg button').forEach((b) => b.setAttribute('aria-checked', String(b.dataset.filter === state.filter)));
}

let lastCur: HTMLElement | null = null;
function syncEventList(): void {
  const cur = state.loaded!.steps[state.idx].step;
  let best: (typeof eventRows)[number] | null = null;
  for (const r of eventRows) {
    if (r.step <= cur) best = r;
    else break;
  }
  if (lastCur && lastCur !== best?.el) lastCur.classList.remove('cur');
  if (best) {
    best.el.classList.add('cur');
    if (best.el !== lastCur) {
      const box = $('#events');
      const top = best.el.offsetTop - box.offsetTop;
      if (top < box.scrollTop || top > box.scrollTop + box.clientHeight - 30) box.scrollTop = top - box.clientHeight / 3;
    }
  }
  lastCur = best?.el ?? null;
}

// ---- client history (linearizability) -----------------------------------------

function renderHistory(): void {
  const L = state.loaded!;
  const lin = L.run.linearizability;
  const wrap = $('#history-wrap');
  const show = !!(lin && !lin.ok && lin.window && lin.culprit);
  wrap.hidden = !show;
  $('.lower').classList.toggle('solo', !show);
  if (!show) return;
  const culprit = lin!.culprit!;
  const source = lin!.window!.find((o) => o.kind === 'put' && o.value === culprit.value);
  const newer = lin!.window!.find(
    (o) => o.kind === 'put' && o !== source && o.retStep !== null && o.retStep < culprit.callStep && (!source || (source.retStep !== null && o.callStep > source.retStep)),
  );
  const t0 = Math.max(0, Math.min(source?.callTime ?? culprit.callTime, culprit.callTime - 400) - 20);
  const ops = lin!.window!.filter((o) => (o.retTime ?? Infinity) >= t0);
  const t1 = (culprit.retTime ?? culprit.callTime) + 20;
  const clients = [...new Set(ops.map((o) => o.client))].sort((a, b) => a - b);
  const W = 460;
  const laneH = 38;
  const left = 34;
  const H = clients.length * laneH + 30;
  const svg = $<SVGSVGElement>('#history');
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.replaceChildren();
  const x = (t: number) => left + ((W - left - 6) * (t - t0)) / (t1 - t0);
  $('#hist-sub').textContent = `key "${lin!.key}", ${Math.round(t0)}–${Math.round(t1)}ms`;
  clients.forEach((c, li) => {
    const y = 6 + li * laneH;
    svg.append(el('text', { x: 2, y: y + 18, class: 'lane-label' }, procName(c, L.scenario.nodes)));
    for (const o of ops.filter((op) => op.client === c)) {
      const end = o.retTime ?? t1;
      const cls = `op ${o.kind}${o === culprit ? ' cul' : ''}${o === source ? ' ref' : ''}${o === newer ? ' newer' : ''}${o.retTime === null ? ' pending' : ''}`;
      const g = el('g', { class: cls });
      const x0 = x(Math.max(o.callTime, t0));
      const w = Math.max(3, x(end) - x0);
      g.append(el('rect', { x: x0, y, width: w, height: 24, rx: 3 }));
      const label = o.kind === 'put' ? `w ${o.value}` : o.retTime === null ? 'r ?' : `r ${o.value ?? 'nil'}`;
      if (w > 34) g.append(el('text', { x: x0 + 4, y: y + 16 }, label));
      g.append(el('title', {}, `${procName(o.client, L.scenario.nodes)} ${label} · ${o.callTime}–${o.retTime ?? '…'}ms`));
      svg.append(g);
    }
  });
  svg.append(el('text', { x: left, y: H - 4, class: 'lane-label' }, 'red: impossible read · gold: value it returned · teal bold: newer acknowledged write'));
}

// ---- playback -----------------------------------------------------------------

function render(): void {
  renderCluster();
  renderLogs();
  renderPlayhead();
  syncEventList();
}

function seekStep(idx: number): void {
  const L = state.loaded!;
  state.idx = Math.max(0, Math.min(L.steps.length - 1, idx));
  state.t = L.steps[state.idx].time;
  render();
}

function seekTime(t: number): void {
  const L = state.loaded!;
  state.t = Math.max(0, Math.min(L.endTime, t));
  let lo = 0;
  let hi = L.steps.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (L.steps[mid].time <= state.t) lo = mid;
    else hi = mid - 1;
  }
  state.idx = lo;
  render();
}

function stepBy(dir: 1 | -1): void {
  const L = state.loaded!;
  for (let i = state.idx + dir; i >= 0 && i < L.steps.length; i += dir) {
    if (visible(L.steps[i])) return seekStep(i);
  }
}

let rafId = 0;
let lastFrame = 0;
function frame(now: number): void {
  if (!state.playing) return;
  const L = state.loaded!;
  const dt = Math.min(100, now - lastFrame);
  lastFrame = now;
  let t = state.t + (dt / 1000) * state.speed;
  const stop = L.run.violation && state.filter !== 'all' ? L.run.violation.time : L.endTime;
  if (state.filter !== 'all') {
    // Skip quiet stretches between interesting events.
    const next = L.steps.find((s, i) => i > state.idx && visible(s));
    if (next && next.time - t > 60) t = next.time - 30;
  }
  if (t >= stop) {
    seekTime(stop);
    if (L.run.violation) seekStep(L.steps.findIndex((s) => s.step === L.run.violation!.step));
    return pause();
  }
  seekTime(t);
  rafId = requestAnimationFrame(frame);
}

function play(): void {
  const L = state.loaded!;
  const stop = L.run.violation && state.filter !== 'all' ? L.run.violation.time : L.endTime;
  if (state.t >= stop - 1) seekStep(0);
  state.playing = true;
  $('#b-play').textContent = 'Pause';
  lastFrame = performance.now();
  rafId = requestAnimationFrame(frame);
}

function pause(): void {
  state.playing = false;
  cancelAnimationFrame(rafId);
  $('#b-play').textContent = 'Play';
}

// ---- implementation picker & hunting --------------------------------------------

function renderImpls(): void {
  const box = $('#impl-list');
  box.replaceChildren();
  const opts: { id: Impl; title: string; desc: string; tier?: string }[] = [
    { id: 'correct', title: 'Correct Raft', desc: 'The reference implementation. Hunting should come back clean.' },
    ...BUGS.map((b) => ({ id: b.id as Impl, title: b.title, desc: b.description, tier: TIERS[b.id] })),
  ];
  for (const o of opts) {
    const label = document.createElement('label');
    label.className = 'impl';
    label.dataset.checked = String(o.id === state.impl);
    label.title = o.desc;
    const input = document.createElement('input');
    input.type = 'radio';
    input.name = 'impl';
    input.id = `impl-${o.id}`;
    input.value = o.id;
    input.checked = o.id === state.impl;
    input.addEventListener('change', () => selectImpl(o.id));
    const b = document.createElement('b');
    b.textContent = o.title;
    if (o.tier) {
      const tier = document.createElement('span');
      tier.className = 'tier';
      tier.textContent = o.tier;
      b.append(tier);
    }
    const small = document.createElement('small');
    small.textContent = o.id === 'correct' ? o.desc : shortDesc(o.desc);
    label.append(input, b, small);
    box.append(label);
  }
}

function shortDesc(d: string): string {
  const first = d.split('. ')[0];
  return first.endsWith('.') ? first : `${first}.`;
}

function selectImpl(id: Impl): void {
  state.impl = id;
  renderImpls();
  pause();
  $('#hunt-hint').textContent =
    id === 'correct'
      ? 'Fuzz 2,000 fresh seeds against the reference implementation. A clean result is the point.'
      : 'Fuzz fresh seeds against this implementation, then shrink the first failure to a minimal repro.';
  if (id === 'correct') {
    const sc = makeScenario(7);
    sc.duration = 2500;
    load(sc, 'sample run', null);
  } else {
    const demo = __DEMOS__[id];
    load(decodeRepro(demo.repro), 'saved example', { before: demo.before, after: { faults: 0, steps: 0 } });
    // after-shape comes from the replay itself
    const L = state.loaded!;
    L.shrinkInfo = { before: demo.before, after: { faults: L.scenario.faults.length, steps: L.run.stats.steps } };
    renderVerdict();
  }
}

function setHuntStatus(seeds: string, events: string, rate: string, phase: string, pct: number): void {
  $('#hs-seeds').textContent = seeds;
  $('#hs-events').textContent = events;
  $('#hs-rate').textContent = rate;
  $('#hs-phase').textContent = phase;
  $('#hunt-bar').style.width = `${Math.max(0, Math.min(100, pct))}%`;
}

function hunt(): void {
  if (state.worker) {
    state.worker.terminate();
    state.worker = null;
    ($('#hunt') as HTMLButtonElement).textContent = 'Hunt for bugs';
    setHuntStatus($('#hs-seeds').textContent ?? '', $('#hs-events').textContent ?? '', '', 'Stopped', 0);
    return;
  }
  const bug = state.impl === 'correct' ? null : state.impl;
  const maxSeeds = bug ? 20000 : 2000;
  const start = 1 + Math.floor(Math.random() * 1_000_000);
  const blob = new Blob([__WORKER_SRC__], { type: 'text/javascript' });
  const w = new Worker(URL.createObjectURL(blob));
  state.worker = w;
  ($('#hunt') as HTMLButtonElement).textContent = 'Stop';
  setHuntStatus('0', '0', '–', 'Fuzzing', 0);
  let found: Extract<HuntEvent, { type: 'found' }> | null = null;
  w.onmessage = (ev: MessageEvent<HuntEvent>) => {
    const e = ev.data;
    if (e.type === 'progress') {
      setHuntStatus(fmt(e.seeds), fmt(e.steps), `${fmt((e.steps / e.elapsed) * 1000)}/s`, 'Fuzzing', (100 * e.seeds) / maxSeeds);
    } else if (e.type === 'found') {
      found = e;
      setHuntStatus(fmt(e.seeds), $('#hs-events').textContent ?? '', $('#hs-rate').textContent ?? '', `Found at seed ${e.seed}`, 100);
    } else if (e.type === 'shrink') {
      $('#hs-phase').textContent = `Shrinking: ${e.faults} faults, ${e.runs} runs`;
    } else if (e.type === 'done') {
      $('#hs-phase').textContent = `Shrunk ${e.before.faults} → ${e.after.faults} faults`;
      finishHunt();
      load(decodeRepro(e.repro), `found live after ${fmt(found!.seeds)} seed${found!.seeds === 1 ? '' : 's'} in ${(found!.elapsed / 1000).toFixed(2)}s`, {
        before: e.before,
        after: e.after,
        runs: e.runs,
      });
    } else if (e.type === 'clean') {
      setHuntStatus(fmt(e.seeds), fmt(e.steps), `${fmt((e.steps / e.elapsed) * 1000)}/s`, 'Clean', 100);
      finishHunt();
      const L = state.loaded!;
      $('#v-text').textContent = `No violations in ${fmt(e.seeds)} seeds: ${fmt(e.steps)} simulated events and ${fmt(e.ops)} client operations, all linearizable. Showing a sample run.`;
      L.origin = `${fmt(e.seeds)} seeds clean`;
      $('#v-meta').textContent = `${L.origin} · showing seed ${L.scenario.seed}`;
    }
  };
  const req: HuntRequest = { type: 'hunt', bug, start, maxSeeds };
  w.postMessage(req);
}

function finishHunt(): void {
  state.worker?.terminate();
  state.worker = null;
  ($('#hunt') as HTMLButtonElement).textContent = 'Hunt again';
}

// ---- wiring -------------------------------------------------------------------

function init(): void {
  renderImpls();
  selectImpl(state.impl);
  setHuntStatus('–', '–', '–', 'Idle', 0);

  $('#hunt').addEventListener('click', hunt);
  $('#b-play').addEventListener('click', () => (state.playing ? pause() : play()));
  $('#b-prev').addEventListener('click', () => (pause(), stepBy(-1)));
  $('#b-next').addEventListener('click', () => (pause(), stepBy(1)));
  $('#b-start').addEventListener('click', () => (pause(), seekStep(0)));
  $('#b-end').addEventListener('click', () => {
    pause();
    const L = state.loaded!;
    const v = L.run.violation;
    seekStep(v ? L.steps.findIndex((s) => s.step === v.step) : L.steps.length - 1);
  });
  document.querySelectorAll<HTMLButtonElement>('.seg button').forEach((b) =>
    b.addEventListener('click', () => {
      state.filter = b.dataset.filter as Filter;
      renderEventList();
      syncEventList();
    }),
  );
  $('#speed').addEventListener('change', (e) => (state.speed = Number((e.target as HTMLSelectElement).value)));

  const tl = $<SVGSVGElement>('#timeline');
  const scrub = (ev: PointerEvent) => {
    const r = tl.getBoundingClientRect();
    const vbx = ((ev.clientX - r.left) / r.width) * TL.w;
    const L = state.loaded!;
    seekTime(((vbx - TL.left) / (TL.w - TL.left - 8)) * L.endTime);
  };
  tl.addEventListener('pointerdown', (ev) => {
    pause();
    tl.setPointerCapture(ev.pointerId);
    scrub(ev);
    const move = (e: PointerEvent) => scrub(e);
    tl.addEventListener('pointermove', move);
    tl.addEventListener('pointerup', () => tl.removeEventListener('pointermove', move), { once: true });
  });

  document.addEventListener('keydown', (ev) => {
    const tag = (ev.target as HTMLElement).tagName;
    if (tag === 'TEXTAREA' || tag === 'SELECT' || tag === 'INPUT') return;
    if (ev.key === ' ') {
      ev.preventDefault();
      state.playing ? pause() : play();
    } else if (ev.key === 'ArrowRight') {
      pause();
      stepBy(1);
    } else if (ev.key === 'ArrowLeft') {
      pause();
      stepBy(-1);
    }
  });

  $('#copy').addEventListener('click', () => {
    const ta = $('#repro') as HTMLTextAreaElement;
    navigator.clipboard
      .writeText(ta.value)
      .then(() => ($('#repro-msg').textContent = 'Copied.'))
      .catch(() => {
        ta.select();
        $('#repro-msg').textContent = 'Selected. Press Ctrl+C or ⌘C to copy.';
      });
  });
  $('#load').addEventListener('click', () => {
    const ta = $('#repro') as HTMLTextAreaElement;
    try {
      const sc = decodeRepro(ta.value);
      pause();
      load(sc, 'pasted repro', null);
      $('#repro-msg').textContent = 'Replayed.';
    } catch {
      $('#repro-msg').textContent = 'That is not a valid repro string. Paste one copied from Faultline or printed by the CLI.';
    }
  });
}

init();
