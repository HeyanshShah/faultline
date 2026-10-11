import { Client } from './client';
import { Heap } from './heap';
import { InvariantChecker } from './invariants';
import { checkLinearizability, type LinearizabilityResult } from './linearizability';
import { RaftNode } from './raft';
import { deriveSeed, Rng } from './rng';
import type { Fault, HistoryOp, Msg, Observation, Persistent, Role, Scenario, Violation } from './types';

export interface NodeSnap {
  alive: boolean;
  role: Role;
  term: number;
  votedFor: number | null;
  commit: number;
  applied: number;
  leaderId: number | null;
  /** Term of each log entry. Shared between snapshots while the log is unchanged. */
  log: number[];
}

export interface Note {
  kind: 'election' | 'leader' | 'commit';
  node: number;
  term: number;
  /** For commits: highest committed index. */
  index?: number;
  text: string;
}

export type MsgFate = 'delivered' | 'dropped' | 'partitioned' | 'dead' | 'pending';

export interface TraceMsg {
  id: number;
  from: number;
  to: number;
  type: Msg['type'];
  label: string;
  sendStep: number;
  sendTime: number;
  deliverTime: number;
  fate: MsgFate;
  deliverStep: number | null;
}

export interface TraceStep {
  step: number;
  time: number;
  kind: 'start' | 'deliver' | 'timer' | 'fault';
  proc: number | null;
  label: string;
  notes: Note[];
  msg: number | null;
  /**
   * Causal parents: the step that sent the delivered message, and the last step that changed this process's
   * state. Steps that changed nothing (a heartbeat to an up-to-date follower) are not dependencies of later
   * steps, which keeps the causal slice to what actually mattered rather than everything Lamport-before.
   */
  parents: number[];
  nodes: NodeSnap[];
  /** Partition group of each server; -1 marks a bridge node that can reach every group. */
  groups: number[];
}

export interface Trace {
  steps: TraceStep[];
  msgs: TraceMsg[];
  servers: number;
  clients: number;
}

export interface RunStats {
  steps: number;
  messages: number;
  elections: number;
  maxTerm: number;
  committed: number;
  opsTotal: number;
  opsOk: number;
}

export interface RunResult {
  scenario: Scenario;
  violation: Violation | null;
  linearizability: LinearizabilityResult | null;
  history: HistoryOp[];
  stats: RunStats;
  trace: Trace | null;
}

export interface RunOptions {
  record?: boolean;
  checkLinearizability?: boolean;
  maxSteps?: number;
}

type QEvent =
  | { time: number; seq: number; kind: 'deliver'; msgId: number; from: number; to: number; body: Msg }
  | { time: number; seq: number; kind: 'timer'; proc: number; name: string; gen: number; life: number }
  | { time: number; seq: number; kind: 'fault'; fault: Fault };

export function procName(id: number, servers: number): string {
  return id < servers ? `n${id}` : `c${id - servers}`;
}

/** Runs one scenario to completion. Same scenario in, bit-identical execution out. */
export function runScenario(sc: Scenario, opts: RunOptions = {}): RunResult {
  return new Simulation(sc, opts).run();
}

class Simulation {
  private queue = new Heap<QEvent>((a, b) => a.time < b.time || (a.time === b.time && a.seq < b.seq));
  private seq = 0;
  private time = 0;
  private step = 0;
  private msgCount = 0;
  private readonly n: number;
  private readonly bugs: ReadonlySet<Scenario['bugs'][number]>;
  private readonly linkRng = new Map<number, Rng>();
  private readonly procRng: Rng[] = [];
  private readonly servers: (RaftNode | null)[] = [];
  private readonly storage: (Persistent | undefined)[] = [];
  private readonly life: number[] = [];
  private readonly timerGen: Map<string, number>[] = [];
  private readonly clients: Client[] = [];
  private groups: number[];
  private readonly history: HistoryOp[] = [];
  private readonly checker: InvariantChecker;
  private readonly stats: RunStats = { steps: 0, messages: 0, elections: 0, maxTerm: 0, committed: 0, opsTotal: 0, opsOk: 0 };

  // tracing
  private readonly trace: Trace | null;
  private notes: Note[] = [];
  private lastEffectOf: number[] = [];
  private stateBefore = '';
  private logCache: { version: number; life: number; terms: number[] }[] = [];

  constructor(
    private readonly sc: Scenario,
    private readonly opts: RunOptions,
  ) {
    this.n = sc.nodes;
    this.bugs = new Set(sc.bugs);
    this.groups = Array.from({ length: this.n }, () => 0);
    this.checker = new InvariantChecker(() => ({ step: this.step, time: this.time }));
    const procs = sc.nodes + sc.clients;
    for (let p = 0; p < procs; p++) {
      this.procRng.push(new Rng(deriveSeed(sc.seed, 2, p)));
      this.timerGen.push(new Map());
      this.life.push(0);
      this.lastEffectOf.push(0);
    }
    this.trace = opts.record ? { steps: [], msgs: [], servers: sc.nodes, clients: sc.clients } : null;
  }

  run(): RunResult {
    const sc = this.sc;
    for (let i = 0; i < this.n; i++) this.bootServer(i);
    const keys = Array.from({ length: sc.keys }, (_, k) => String.fromCharCode(120 + k)); // x, y, z, ...
    for (let c = 0; c < sc.clients; c++) {
      const id = this.n + c;
      const client = new Client(this.clientEnv(id), keys, sc.clientStop, this.history);
      this.clients.push(client);
      client.start();
    }
    for (const f of sc.faults) this.push({ time: f.at, seq: 0, kind: 'fault', fault: f });
    this.record('start', null, 'cluster boots', null, []);

    const maxSteps = this.opts.maxSteps ?? 2_000_000;
    while (this.queue.size > 0) {
      const ev = this.queue.peek()!;
      if (ev.time > sc.duration) break;
      this.queue.pop();
      this.time = ev.time;
      this.dispatch(ev);
      if (this.checker.violation || this.step >= maxSteps) break;
    }

    let violation = this.checker.violation;
    let lin: LinearizabilityResult | null = null;
    if (!violation && this.opts.checkLinearizability !== false) {
      lin = checkLinearizability(this.history);
      if (!lin.ok && lin.culprit) {
        const op = lin.culprit;
        violation = {
          kind: 'linearizability',
          message: `No legal order exists for key "${lin.key}": ${procName(op.client, this.n)} ${
            op.kind === 'get' ? `read ${op.value === null ? 'nothing' : `"${op.value}"`}` : `wrote "${op.value}"`
          } but no sequential execution consistent with real time produces that`,
          step: op.retStep!,
          time: op.retTime!,
          node: null,
        };
      }
    }

    this.stats.steps = this.step;
    this.stats.messages = this.msgCount;
    this.stats.opsTotal = this.history.length;
    this.stats.opsOk = this.history.filter((o) => o.retStep !== null).length;
    return { scenario: sc, violation, linearizability: lin, history: this.history, stats: this.stats, trace: this.trace };
  }

  // ---- event plumbing -------------------------------------------------------

  private push(ev: QEvent): void {
    ev.seq = this.seq++;
    this.queue.push(ev);
  }

  private dispatch(ev: QEvent): void {
    if (ev.kind === 'timer') {
      if (this.timerGen[ev.proc].get(ev.name) !== ev.gen || this.life[ev.proc] !== ev.life) return;
      if (ev.proc < this.n && !this.servers[ev.proc]) return;
      this.beginStep(ev.proc);
      const parents = [this.lastEffectOf[ev.proc]];
      if (ev.proc < this.n) this.servers[ev.proc]!.onTimer(ev.name);
      else this.clients[ev.proc - this.n].onTimer(ev.name);
      this.record('timer', ev.proc, timerLabel(ev.name, procName(ev.proc, this.n)), null, parents);
      return;
    }
    if (ev.kind === 'deliver') {
      const tm = this.trace?.msgs[ev.msgId];
      if (ev.to < this.n && !this.servers[ev.to]) {
        if (tm) tm.fate = 'dead';
        return;
      }
      if (ev.from < this.n && ev.to < this.n && !this.connected(ev.from, ev.to)) {
        if (tm) tm.fate = 'partitioned';
        return;
      }
      this.beginStep(ev.to);
      const parents = [this.lastEffectOf[ev.to]];
      if (tm) {
        tm.fate = 'delivered';
        tm.deliverStep = this.step;
        parents.push(tm.sendStep);
      }
      if (ev.to < this.n) this.servers[ev.to]!.onMessage(ev.from, ev.body);
      else this.clients[ev.to - this.n].onMessage(ev.from, ev.body);
      this.record(
        'deliver',
        ev.to,
        `${procName(ev.to, this.n)} ← ${procName(ev.from, this.n)}  ${msgLabel(ev.body, this.n)}`,
        ev.msgId,
        parents,
      );
      return;
    }
    this.applyFault(ev.fault);
  }

  private connected(a: number, b: number): boolean {
    const ga = this.groups[a];
    const gb = this.groups[b];
    return ga === gb || ga === -1 || gb === -1;
  }

  private beginStep(proc: number | null): void {
    this.step++;
    this.notes = [];
    if (this.trace && proc !== null) this.stateBefore = this.stateKey(proc);
  }

  private stateKey(proc: number): string {
    if (proc >= this.n) return this.clients[proc - this.n].stateKey();
    const s = this.servers[proc];
    return s ? `${this.life[proc]}|${s.role}|${s.term}|${s.votedFor}|${s.logVersion}|${s.commitIndex}` : `dead|${this.life[proc]}`;
  }

  private applyFault(f: Fault): void {
    const name = (i: number) => procName(i, this.n);
    switch (f.kind) {
      case 'crash': {
        if (!this.servers[f.node]) return;
        this.beginStep(f.node);
        const parents = [this.lastEffectOf[f.node]];
        this.servers[f.node] = null;
        this.life[f.node]++;
        this.record('fault', f.node, `CRASH ${name(f.node)}`, null, parents);
        return;
      }
      case 'restart': {
        if (this.servers[f.node]) return;
        this.beginStep(f.node);
        const parents = [this.lastEffectOf[f.node]];
        this.life[f.node]++;
        this.bootServer(f.node);
        this.record('fault', f.node, `RESTART ${name(f.node)} (term ${this.storage[f.node]?.term ?? 0} from disk)`, null, parents);
        return;
      }
      case 'partition': {
        this.beginStep(null);
        const groups = Array.from({ length: this.n }, () => 0);
        f.groups.forEach((g, gi) => g.forEach((node) => (groups[node] = gi)));
        if (f.bridge !== undefined) groups[f.bridge] = -1;
        this.groups = groups;
        const desc = f.groups.map((g) => `{${g.map(name).join(' ')}}`).join(f.bridge === undefined ? ' | ' : ` |${name(f.bridge)}| `);
        this.record('fault', null, `${f.bridge === undefined ? 'PARTITION' : 'BRIDGE PARTITION'} ${desc}`, null, []);
        return;
      }
      case 'heal': {
        if (this.groups.every((g) => g === this.groups[0])) return;
        this.beginStep(null);
        this.groups = this.groups.map(() => 0);
        this.record('fault', null, 'HEAL network', null, []);
        return;
      }
    }
  }

  private send(from: number, to: number, body: Msg): void {
    const msgId = this.msgCount++;
    const net = this.sc.net;
    // One stream per directed link: perturbing traffic on one link leaves every other link's dice untouched,
    // which keeps executions stable while the shrinker deletes faults.
    const link = from * 1024 + to;
    let rng = this.linkRng.get(link);
    if (!rng) {
      rng = new Rng(deriveSeed(this.sc.seed, 1, link));
      this.linkRng.set(link, rng);
    }
    const dropped = rng.chance(net.dropRate);
    const delay = rng.chance(net.spikeRate)
      ? rng.int(net.spikeMin, net.spikeMax + 1)
      : rng.int(net.minDelay, net.maxDelay + 1);
    if (this.trace) {
      this.trace.msgs.push({
        id: msgId,
        from,
        to,
        type: body.type,
        label: msgLabel(body, this.n),
        sendStep: this.step,
        sendTime: this.time,
        deliverTime: this.time + delay,
        fate: dropped ? 'dropped' : 'pending',
        deliverStep: null,
      });
    }
    if (dropped) return;
    this.push({ time: this.time + delay, seq: 0, kind: 'deliver', msgId, from, to, body });
  }

  private setTimer(proc: number, name: string, delay: number): void {
    const gen = (this.timerGen[proc].get(name) ?? 0) + 1;
    this.timerGen[proc].set(name, gen);
    this.push({ time: this.time + delay, seq: 0, kind: 'timer', proc, name, gen, life: this.life[proc] });
  }

  private bootServer(i: number): void {
    const env = {
      id: i,
      n: this.n,
      bugs: this.bugs,
      now: () => this.time,
      rand: () => this.procRng[i].next(),
      send: (to: number, msg: Msg) => this.send(i, to, msg),
      setTimer: (name: string, delay: number) => this.setTimer(i, name, delay),
      persist: (state: Persistent) => {
        this.storage[i] = state;
      },
      observe: (o: Observation) => this.observe(o),
    };
    const node = new RaftNode(env, this.storage[i]);
    this.servers[i] = node;
    node.start();
  }

  private clientEnv(id: number) {
    return {
      id,
      servers: this.n,
      now: () => this.time,
      step: () => this.step,
      rand: () => this.procRng[id].next(),
      send: (to: number, msg: Msg) => this.send(id, to, msg),
      setTimer: (name: string, delay: number) => this.setTimer(id, name, delay),
      clearTimer: (name: string) => {
        this.timerGen[id].set(name, (this.timerGen[id].get(name) ?? 0) + 1);
      },
    };
  }

  private observe(o: Observation): void {
    this.checker.observe(o);
    if (o.kind === 'leader') {
      this.stats.elections++;
      this.stats.maxTerm = Math.max(this.stats.maxTerm, o.term);
      if (this.trace) this.notes.push({ kind: 'leader', node: o.node, term: o.term, text: `n${o.node} becomes LEADER of term ${o.term}` });
    } else if (o.kind === 'commit') {
      this.stats.committed = Math.max(this.stats.committed, o.from + o.terms.length - 1);
      if (this.trace) {
        const index = o.from + o.terms.length - 1;
        this.notes.push({ kind: 'commit', node: o.node, term: o.term, index, text: `n${o.node} commits through #${index}` });
      }
    } else if (o.kind === 'role' && o.role === 'candidate' && this.trace) {
      this.notes.push({ kind: 'election', node: o.node, term: o.term, text: `n${o.node} starts election for term ${o.term}` });
    }
  }

  // ---- tracing ------------------------------------------------------------

  private record(kind: TraceStep['kind'], proc: number | null, label: string, msg: number | null, parents: number[]): void {
    if (!this.trace) return;
    if (proc !== null && (kind === 'fault' || this.stateKey(proc) !== this.stateBefore)) this.lastEffectOf[proc] = this.step;
    const nodes: NodeSnap[] = [];
    for (let i = 0; i < this.n; i++) {
      const s = this.servers[i];
      if (!s) {
        const disk = this.storage[i];
        nodes.push({
          alive: false,
          role: 'follower',
          term: disk?.term ?? 0,
          votedFor: disk?.votedFor ?? null,
          commit: 0,
          applied: 0,
          leaderId: null,
          log: disk ? disk.log.map((e) => e.term) : [],
        });
        continue;
      }
      let cache = this.logCache[i];
      if (!cache || cache.version !== s.logVersion || cache.life !== this.life[i]) {
        cache = { version: s.logVersion, life: this.life[i], terms: s.log.map((e) => e.term) };
        this.logCache[i] = cache;
      }
      nodes.push({
        alive: true,
        role: s.role,
        term: s.term,
        votedFor: s.votedFor,
        commit: s.commitIndex,
        applied: s.lastApplied,
        leaderId: s.leaderId,
        log: cache.terms,
      });
    }
    this.trace.steps.push({
      step: this.step,
      time: this.time,
      kind,
      proc,
      label,
      notes: this.notes,
      msg,
      parents: parents.filter((p) => p > 0 || this.step === 0),
      nodes,
      groups: this.groups,
    });
  }
}

function timerLabel(name: string, who: string): string {
  switch (name) {
    case 'election':
      return `${who} election timeout`;
    case 'heartbeat':
      return `${who} heartbeat`;
    case 'vote-retry':
      return `${who} re-requests votes`;
    case 'next':
      return `${who} issues next op`;
    case 'retry':
      return `${who} times out, retries elsewhere`;
    case 'resend':
      return `${who} follows redirect`;
    default:
      return `${who} timer ${name}`;
  }
}

export function msgLabel(m: Msg, servers: number): string {
  switch (m.type) {
    case 'RequestVote':
      return `RequestVote t${m.term} (log ${m.lastLogIndex}@t${m.lastLogTerm})`;
    case 'RequestVoteReply':
      return `Vote ${m.granted ? 'GRANTED' : 'denied'} t${m.term}`;
    case 'AppendEntries':
      return m.entries.length === 0
        ? `Heartbeat t${m.term} commit=${m.leaderCommit}`
        : `Append t${m.term} #${m.prevLogIndex + 1}..${m.prevLogIndex + m.entries.length} commit=${m.leaderCommit}`;
    case 'AppendEntriesReply':
      return m.success ? `AppendOK t${m.term} match=${m.matchIndex}` : `AppendReject t${m.term}`;
    case 'ClientRequest':
      return m.op.kind === 'put' ? `put ${m.op.key}="${m.op.value}"` : `get ${m.op.key}`;
    case 'ClientReply':
      return m.ok
        ? `ok ${m.value === null ? '(nil)' : `"${m.value}"`}`
        : `not leader${m.leaderHint === null ? '' : `, try ${procName(m.leaderHint, servers)}`}`;
  }
}
