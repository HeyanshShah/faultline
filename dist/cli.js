#!/usr/bin/env node

// src/types.ts
var BUGS = [
  {
    id: "stale-read",
    title: "Leader-local reads",
    description: "The leader answers reads from its own state machine without confirming it is still leader. A deposed leader on the wrong side of a partition serves stale data. Raft invariants all hold; only the linearizability checker can see it."
  },
  {
    id: "forget-vote",
    title: "votedFor not persisted",
    description: "currentTerm and the log are fsynced but votedFor lives only in memory. A node that crashes and restarts mid-election can vote twice in one term, electing two leaders."
  },
  {
    id: "no-uptodate-check",
    title: "Missing up-to-date vote check",
    description: "Voters grant votes without checking that the candidate log is at least as up to date as their own. A node missing committed entries can win and overwrite them."
  },
  {
    id: "truncate-always",
    title: "Truncate on every AppendEntries",
    description: "Followers truncate their log after prevLogIndex on every AppendEntries instead of only on conflict. A delayed, reordered message silently deletes acknowledged entries."
  }
];
var DEFAULT_NET = {
  minDelay: 2,
  maxDelay: 12,
  dropRate: 0.02,
  spikeRate: 0.04,
  spikeMin: 40,
  spikeMax: 250
};

// src/rng.ts
var Rng = class {
  state;
  constructor(seed) {
    this.state = mix32(seed >>> 0);
  }
  /** Uniform float in [0, 1). */
  next() {
    let t = this.state = this.state + 1831565813 >>> 0;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  }
  /** Uniform integer in [lo, hi). */
  int(lo, hi) {
    return lo + Math.floor(this.next() * (hi - lo));
  }
  chance(p) {
    return this.next() < p;
  }
  pick(items) {
    return items[this.int(0, items.length)];
  }
  shuffle(items) {
    const out = items.slice();
    for (let i = out.length - 1; i > 0; i--) {
      const j = this.int(0, i + 1);
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  }
};
function mix32(x) {
  x = x + 2654435769 >>> 0;
  x = Math.imul(x ^ x >>> 16, 2246822507) >>> 0;
  x = Math.imul(x ^ x >>> 13, 3266489909) >>> 0;
  return (x ^ x >>> 16) >>> 0;
}
function deriveSeed(root, ...labels) {
  let h = mix32(root);
  for (const l of labels) h = mix32(h ^ mix32(l + 19088743));
  return h;
}

// src/raft.ts
var ELECTION_MIN = 150;
var ELECTION_SPREAD = 150;
var HEARTBEAT = 50;
var MAX_BATCH = 16;
function cmdId(cmd) {
  return cmd.kind === "noop" ? "noop" : `${cmd.kind}:${cmd.client}:${cmd.seq}`;
}
var RaftNode = class {
  constructor(env, persisted) {
    this.env = env;
    this.term = persisted?.term ?? 0;
    this.votedFor = persisted?.votedFor ?? null;
    this.log = persisted ? persisted.log.slice() : [];
  }
  term;
  votedFor;
  log;
  role = "follower";
  leaderId = null;
  commitIndex = 0;
  lastApplied = 0;
  /** Bumped on every log mutation so observers can cheaply detect change. */
  logVersion = 0;
  votes = /* @__PURE__ */ new Set();
  nextIndex = [];
  matchIndex = [];
  kv = /* @__PURE__ */ new Map();
  sessions = /* @__PURE__ */ new Map();
  get id() {
    return this.env.id;
  }
  get lastIndex() {
    return this.log.length;
  }
  termAt(index) {
    return index === 0 ? 0 : this.log[index - 1].term;
  }
  start() {
    this.resetElectionTimer();
  }
  onTimer(name) {
    if (name === "election") {
      if (this.role !== "leader") this.startElection();
    } else if (name === "vote-retry") {
      if (this.role === "candidate") {
        this.requestVotes();
        this.env.setTimer("vote-retry", HEARTBEAT);
      }
    } else if (name === "heartbeat") {
      if (this.role === "leader") {
        this.broadcastAppend();
        this.env.setTimer("heartbeat", HEARTBEAT);
      }
    }
  }
  onMessage(from, m) {
    switch (m.type) {
      case "RequestVote":
        return this.onRequestVote(from, m);
      case "RequestVoteReply":
        return this.onVoteReply(from, m);
      case "AppendEntries":
        return this.onAppendEntries(from, m);
      case "AppendEntriesReply":
        return this.onAppendReply(from, m);
      case "ClientRequest":
        return this.onClientRequest(from, m);
      case "ClientReply":
        return;
    }
  }
  // ---- persistence & roles ------------------------------------------------
  persist() {
    this.env.persist({
      term: this.term,
      // BUG forget-vote: votedFor is never written to stable storage.
      votedFor: this.env.bugs.has("forget-vote") ? null : this.votedFor,
      log: this.log.slice()
    });
  }
  resetElectionTimer() {
    this.env.setTimer("election", ELECTION_MIN + Math.floor(this.env.rand() * ELECTION_SPREAD));
  }
  setRole(role) {
    if (role === this.role) return;
    this.role = role;
    this.env.observe({ kind: "role", node: this.id, role, term: this.term });
  }
  stepDown(term) {
    const wasLeader = this.role === "leader";
    if (term > this.term) {
      this.term = term;
      this.votedFor = null;
      this.leaderId = null;
    }
    this.setRole("follower");
    this.persist();
    if (wasLeader) this.resetElectionTimer();
  }
  // ---- elections ----------------------------------------------------------
  startElection() {
    this.term += 1;
    this.votedFor = this.id;
    this.leaderId = null;
    this.votes = /* @__PURE__ */ new Set([this.id]);
    this.role = "follower";
    this.setRole("candidate");
    this.persist();
    this.resetElectionTimer();
    this.requestVotes();
    this.env.setTimer("vote-retry", HEARTBEAT);
  }
  requestVotes() {
    for (let p = 0; p < this.env.n; p++) {
      if (this.votes.has(p)) continue;
      this.env.send(p, {
        type: "RequestVote",
        term: this.term,
        candidateId: this.id,
        lastLogIndex: this.lastIndex,
        lastLogTerm: this.termAt(this.lastIndex)
      });
    }
  }
  onRequestVote(from, m) {
    if (m.term > this.term) this.stepDown(m.term);
    const myLastTerm = this.termAt(this.lastIndex);
    const upToDate = m.lastLogTerm > myLastTerm || m.lastLogTerm === myLastTerm && m.lastLogIndex >= this.lastIndex;
    const granted = m.term === this.term && (this.votedFor === null || this.votedFor === m.candidateId) && // BUG no-uptodate-check: votes for anyone, regardless of how stale their log is.
    (upToDate || this.env.bugs.has("no-uptodate-check"));
    if (granted) {
      this.votedFor = m.candidateId;
      this.persist();
      this.resetElectionTimer();
    }
    this.env.send(from, { type: "RequestVoteReply", term: this.term, granted });
  }
  onVoteReply(from, m) {
    if (m.term > this.term) return this.stepDown(m.term);
    if (this.role !== "candidate" || m.term !== this.term || !m.granted) return;
    this.votes.add(from);
    if (this.votes.size * 2 > this.env.n) this.becomeLeader();
  }
  becomeLeader() {
    this.setRole("leader");
    this.leaderId = this.id;
    this.env.observe({ kind: "leader", node: this.id, term: this.term, logTerms: this.log.map((e) => e.term) });
    this.nextIndex = Array.from({ length: this.env.n }, () => this.lastIndex + 1);
    this.matchIndex = Array.from({ length: this.env.n }, () => 0);
    this.appendLocal({ kind: "noop" });
    this.broadcastAppend();
    this.env.setTimer("heartbeat", HEARTBEAT);
  }
  // ---- replication --------------------------------------------------------
  appendLocal(cmd) {
    this.log.push({ term: this.term, cmd });
    this.logVersion++;
    this.matchIndex[this.id] = this.lastIndex;
    this.persist();
  }
  broadcastAppend() {
    for (let p = 0; p < this.env.n; p++) if (p !== this.id) this.sendAppend(p);
  }
  sendAppend(p) {
    const prev = this.nextIndex[p] - 1;
    this.env.send(p, {
      type: "AppendEntries",
      term: this.term,
      leaderId: this.id,
      prevLogIndex: prev,
      prevLogTerm: this.termAt(prev),
      entries: this.log.slice(prev, prev + MAX_BATCH),
      leaderCommit: this.commitIndex
    });
  }
  onAppendEntries(from, m) {
    if (m.term < this.term) {
      this.env.send(from, { type: "AppendEntriesReply", term: this.term, success: false, matchIndex: 0 });
      return;
    }
    if (m.term > this.term || this.role !== "follower") this.stepDown(m.term);
    this.leaderId = m.leaderId;
    this.resetElectionTimer();
    if (m.prevLogIndex > this.lastIndex || this.termAt(m.prevLogIndex) !== m.prevLogTerm) {
      this.env.send(from, {
        type: "AppendEntriesReply",
        term: this.term,
        success: false,
        matchIndex: Math.min(this.lastIndex, m.prevLogIndex - 1)
      });
      return;
    }
    let changed = false;
    if (this.env.bugs.has("truncate-always")) {
      this.log = this.log.slice(0, m.prevLogIndex).concat(m.entries);
      changed = true;
    } else {
      for (let i = 0; i < m.entries.length; i++) {
        const idx = m.prevLogIndex + 1 + i;
        if (idx <= this.lastIndex && this.termAt(idx) === m.entries[i].term) continue;
        this.log = this.log.slice(0, idx - 1).concat(m.entries.slice(i));
        changed = true;
        break;
      }
    }
    if (changed) {
      this.logVersion++;
      this.persist();
    }
    const lastNew = m.prevLogIndex + m.entries.length;
    if (m.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(m.leaderCommit, lastNew);
      this.apply();
    }
    this.env.send(from, { type: "AppendEntriesReply", term: this.term, success: true, matchIndex: lastNew });
  }
  onAppendReply(from, m) {
    if (m.term > this.term) return this.stepDown(m.term);
    if (this.role !== "leader" || m.term !== this.term) return;
    if (m.success) {
      this.matchIndex[from] = Math.max(this.matchIndex[from], m.matchIndex);
      this.nextIndex[from] = this.matchIndex[from] + 1;
      this.advanceCommit();
      if (this.nextIndex[from] <= this.lastIndex) this.sendAppend(from);
    } else {
      this.nextIndex[from] = Math.max(1, Math.min(this.nextIndex[from] - 1, m.matchIndex + 1));
      this.sendAppend(from);
    }
  }
  advanceCommit() {
    for (let n = this.lastIndex; n > this.commitIndex; n--) {
      if (this.termAt(n) !== this.term) continue;
      let count = 1;
      for (let p = 0; p < this.env.n; p++) if (p !== this.id && this.matchIndex[p] >= n) count++;
      if (count * 2 > this.env.n) {
        const from = this.commitIndex + 1;
        this.commitIndex = n;
        this.env.observe({
          kind: "commit",
          node: this.id,
          term: this.term,
          from,
          terms: this.log.slice(from - 1, n).map((e) => e.term)
        });
        this.apply();
        return;
      }
    }
  }
  // ---- state machine ------------------------------------------------------
  apply() {
    while (this.lastApplied < this.commitIndex && this.lastApplied < this.lastIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied - 1];
      const cmd = entry.cmd;
      this.env.observe({ kind: "apply", node: this.id, index: this.lastApplied, term: entry.term, cmdId: cmdId(cmd) });
      if (cmd.kind === "noop") continue;
      let value;
      const session = this.sessions.get(cmd.client);
      if (session && cmd.seq <= session.seq) {
        value = cmd.seq === session.seq ? session.value : null;
      } else {
        if (cmd.kind === "put") {
          this.kv.set(cmd.key, cmd.value);
          value = cmd.value;
        } else {
          value = this.kv.get(cmd.key) ?? null;
        }
        this.sessions.set(cmd.client, { seq: cmd.seq, value });
      }
      if (this.role === "leader") {
        this.env.send(cmd.client, { type: "ClientReply", seq: cmd.seq, ok: true, value, leaderHint: this.id });
      }
    }
  }
  onClientRequest(from, m) {
    if (this.role !== "leader") {
      this.env.send(from, { type: "ClientReply", seq: m.seq, ok: false, value: null, leaderHint: this.leaderId });
      return;
    }
    if (m.op.kind === "get" && this.env.bugs.has("stale-read")) {
      const value = this.kv.get(m.op.key) ?? null;
      this.env.send(from, { type: "ClientReply", seq: m.seq, ok: true, value, leaderHint: this.id });
      return;
    }
    const session = this.sessions.get(m.client);
    if (session && m.seq <= session.seq) {
      if (m.seq === session.seq) {
        this.env.send(from, { type: "ClientReply", seq: m.seq, ok: true, value: session.value, leaderHint: this.id });
      }
      return;
    }
    for (let i = this.lastIndex; i > this.lastApplied; i--) {
      const c2 = this.log[i - 1].cmd;
      if (c2.kind !== "noop" && c2.client === m.client && c2.seq === m.seq) return;
    }
    const cmd = m.op.kind === "put" ? { kind: "put", key: m.op.key, value: m.op.value, client: m.client, seq: m.seq } : { kind: "get", key: m.op.key, client: m.client, seq: m.seq };
    this.appendLocal(cmd);
    this.broadcastAppend();
  }
};

// src/client.ts
var RETRY_AFTER = 200;
var REDIRECT_BACKOFF = 15;
var Client = class {
  constructor(env, keys, stopAt, history) {
    this.env = env;
    this.keys = keys;
    this.stopAt = stopAt;
    this.history = history;
    this.guess = Math.floor(env.rand() * env.servers);
  }
  seq = 0;
  current = null;
  guess;
  /** Changes whenever the client's observable state changes; used to build a tight causal graph. */
  stateKey() {
    return `${this.seq}|${this.current ? 1 : 0}|${this.guess}`;
  }
  start() {
    this.env.setTimer("next", 5 + Math.floor(this.env.rand() * 40));
  }
  onTimer(name) {
    if (name === "next") this.issue();
    else if (name === "retry" && this.current) {
      this.guess = Math.floor(this.env.rand() * this.env.servers);
      this.sendCurrent();
    } else if (name === "resend" && this.current) {
      this.sendCurrent();
    }
  }
  onMessage(_from, m) {
    if (m.type !== "ClientReply" || !this.current || m.seq !== this.current.seq) return;
    if (!m.ok) {
      this.guess = m.leaderHint ?? Math.floor(this.env.rand() * this.env.servers);
      this.env.setTimer("resend", REDIRECT_BACKOFF);
      return;
    }
    const rec = this.current.record;
    if (rec.kind === "get") rec.value = m.value;
    rec.retTime = this.env.now();
    rec.retStep = this.env.step();
    this.current = null;
    this.env.clearTimer("retry");
    this.env.setTimer("next", 5 + Math.floor(this.env.rand() * 60));
  }
  issue() {
    if (this.env.now() >= this.stopAt) return;
    this.seq++;
    const key = this.keys[Math.floor(this.env.rand() * this.keys.length)];
    const op = this.env.rand() < 0.5 ? { kind: "put", key, value: `c${this.env.id}.${this.seq}` } : { kind: "get", key };
    const record = {
      id: this.history.length,
      client: this.env.id,
      kind: op.kind,
      key,
      value: op.kind === "put" ? op.value : null,
      callTime: this.env.now(),
      retTime: null,
      callStep: this.env.step(),
      retStep: null
    };
    this.history.push(record);
    this.current = { seq: this.seq, op, record };
    this.sendCurrent();
  }
  sendCurrent() {
    if (!this.current) return;
    this.env.send(this.guess, { type: "ClientRequest", client: this.env.id, seq: this.current.seq, op: this.current.op });
    this.env.setTimer("retry", RETRY_AFTER);
  }
};

// src/heap.ts
var Heap = class {
  constructor(less) {
    this.less = less;
  }
  items = [];
  get size() {
    return this.items.length;
  }
  push(item) {
    const a = this.items;
    a.push(item);
    let i = a.length - 1;
    while (i > 0) {
      const p = i - 1 >> 1;
      if (!this.less(a[i], a[p])) break;
      [a[i], a[p]] = [a[p], a[i]];
      i = p;
    }
  }
  peek() {
    return this.items[0];
  }
  pop() {
    const a = this.items;
    if (a.length === 0) return void 0;
    const top = a[0];
    const last = a.pop();
    if (a.length > 0) {
      a[0] = last;
      let i = 0;
      for (; ; ) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < a.length && this.less(a[l], a[m])) m = l;
        if (r < a.length && this.less(a[r], a[m])) m = r;
        if (m === i) break;
        [a[i], a[m]] = [a[m], a[i]];
        i = m;
      }
    }
    return top;
  }
};

// src/invariants.ts
var InvariantChecker = class {
  constructor(clock) {
    this.clock = clock;
  }
  leaders = /* @__PURE__ */ new Map();
  /** committed[i] = term of the entry known committed at log index i+1. */
  committed = [];
  /** committedIn[i] = earliest leader term in which index i+1 was known committed. */
  committedIn = [];
  /** applied[i] = identity of the entry first applied at log index i+1. */
  applied = [];
  violation = null;
  observe(o) {
    if (this.violation) return;
    switch (o.kind) {
      case "leader": {
        const prev = this.leaders.get(o.term);
        if (prev !== void 0 && prev !== o.node) {
          return this.fail("election-safety", o.node, `Two leaders in term ${o.term}: n${prev} and n${o.node}`, { term: o.term });
        }
        this.leaders.set(o.term, o.node);
        for (let i = 0; i < this.committed.length; i++) {
          if (this.committedIn[i] < o.term && o.logTerms[i] !== this.committed[i]) {
            return this.fail(
              "leader-completeness",
              o.node,
              `n${o.node} became leader of term ${o.term} without committed entry #${i + 1} (term ${this.committed[i]}); its log has ${o.logTerms[i] === void 0 ? "nothing" : `term ${o.logTerms[i]}`} there`,
              { term: o.term, index: i + 1 }
            );
          }
        }
        return;
      }
      case "commit": {
        for (let k = 0; k < o.terms.length; k++) {
          const i = o.from - 1 + k;
          const known = this.committed[i];
          if (known !== void 0 && known !== o.terms[k]) {
            return this.fail(
              "state-machine-safety",
              o.node,
              `n${o.node} committed term ${o.terms[k]} at #${i + 1}, but term ${known} was already committed there`,
              { index: i + 1 }
            );
          }
          this.committed[i] = o.terms[k];
          this.committedIn[i] = Math.min(this.committedIn[i] ?? Infinity, o.term);
        }
        return;
      }
      case "apply": {
        const id = `${o.term}/${o.cmdId}`;
        const known = this.applied[o.index - 1];
        if (known !== void 0 && known !== id) {
          return this.fail(
            "state-machine-safety",
            o.node,
            `n${o.node} applied ${o.cmdId} (term ${o.term}) at #${o.index}, but another node applied ${known.split("/")[1]} (term ${known.split("/")[0]}) there`,
            { index: o.index }
          );
        }
        this.applied[o.index - 1] = id;
        return;
      }
      case "role":
        return;
    }
  }
  fail(kind, node, message, extra = {}) {
    const { step, time } = this.clock();
    this.violation = { kind, message, step, time, node, ...extra };
  }
};

// src/linearizability.ts
var BudgetExceeded = class extends Error {
};
function checkLinearizability(history, budget = 2e6) {
  const byKey = /* @__PURE__ */ new Map();
  for (const op of history) {
    if (!byKey.has(op.key)) byKey.set(op.key, []);
    byKey.get(op.key).push(op);
  }
  let inconclusive = false;
  for (const key of [...byKey.keys()].sort()) {
    const ops = byKey.get(key);
    let verdict;
    try {
      verdict = linearizableRegister(ops, Infinity, budget);
    } catch (e) {
      if (!(e instanceof BudgetExceeded)) throw e;
      inconclusive = true;
      continue;
    }
    if (verdict) continue;
    const rets = ops.filter((o) => o.retStep !== null).map((o) => o.retStep).sort((a, b) => a - b);
    let lo = 0;
    let hi = rets.length - 1;
    while (lo < hi) {
      const mid = lo + hi >> 1;
      let ok;
      try {
        ok = linearizableRegister(ops, rets[mid] + 1, budget);
      } catch {
        ok = true;
      }
      if (ok) lo = mid + 1;
      else hi = mid;
    }
    const cut = rets[lo];
    const culprit = ops.find((o) => o.retStep === cut);
    const window = ops.filter((o) => o.callStep <= cut).sort((a, b) => a.callStep - b.callStep);
    return { ok: false, inconclusive: false, key, culprit, window };
  }
  return { ok: true, inconclusive };
}
function linearizableRegister(all, cut, budget = 2e6) {
  const ops = all.filter((o) => o.callStep < cut).map((o) => ({ op: o, ret: o.retStep !== null && o.retStep < cut ? o.retStep : Infinity })).filter((o) => !(o.op.kind === "get" && o.ret === Infinity)).sort((a, b) => a.op.callStep - b.op.callStep);
  const n = ops.length;
  const done = new Uint8Array(n);
  const words = new Int32Array(Math.ceil(n / 32) || 1);
  let remaining = ops.filter((o) => o.ret !== Infinity).length;
  const seen = /* @__PURE__ */ new Set();
  let work = 0;
  const search = (value) => {
    if (remaining === 0) return true;
    if (++work > budget) throw new BudgetExceeded();
    let minRet = Infinity;
    for (let i = 0; i < n; i++) if (!done[i] && ops[i].ret < minRet) minRet = ops[i].ret;
    for (let i = 0; i < n; i++) {
      if (done[i]) continue;
      const { op, ret } = ops[i];
      if (op.callStep >= minRet) break;
      let next;
      if (op.kind === "get") {
        if (op.value !== value) continue;
        next = value;
      } else {
        next = op.value;
      }
      done[i] = 1;
      words[i >> 5] ^= 1 << (i & 31);
      const memoKey = `${words.join(",")}|${next}`;
      if (!seen.has(memoKey)) {
        seen.add(memoKey);
        if (ret !== Infinity) remaining--;
        const found = search(next);
        if (ret !== Infinity) remaining++;
        if (found) return true;
      }
      done[i] = 0;
      words[i >> 5] ^= 1 << (i & 31);
    }
    return false;
  };
  return search(null);
}

// src/sim.ts
function procName(id, servers) {
  return id < servers ? `n${id}` : `c${id - servers}`;
}
function runScenario(sc, opts = {}) {
  return new Simulation(sc, opts).run();
}
var Simulation = class {
  constructor(sc, opts) {
    this.sc = sc;
    this.opts = opts;
    this.n = sc.nodes;
    this.bugs = new Set(sc.bugs);
    this.groups = Array.from({ length: this.n }, () => 0);
    this.checker = new InvariantChecker(() => ({ step: this.step, time: this.time }));
    const procs = sc.nodes + sc.clients;
    for (let p = 0; p < procs; p++) {
      this.procRng.push(new Rng(deriveSeed(sc.seed, 2, p)));
      this.timerGen.push(/* @__PURE__ */ new Map());
      this.life.push(0);
      this.lastEffectOf.push(0);
    }
    this.trace = opts.record ? { steps: [], msgs: [], servers: sc.nodes, clients: sc.clients } : null;
  }
  queue = new Heap((a, b) => a.time < b.time || a.time === b.time && a.seq < b.seq);
  seq = 0;
  time = 0;
  step = 0;
  msgCount = 0;
  n;
  bugs;
  linkRng = /* @__PURE__ */ new Map();
  procRng = [];
  servers = [];
  storage = [];
  life = [];
  timerGen = [];
  clients = [];
  groups;
  history = [];
  checker;
  stats = { steps: 0, messages: 0, elections: 0, maxTerm: 0, committed: 0, opsTotal: 0, opsOk: 0 };
  // tracing
  trace;
  notes = [];
  lastEffectOf = [];
  stateBefore = "";
  logCache = [];
  run() {
    const sc = this.sc;
    for (let i = 0; i < this.n; i++) this.bootServer(i);
    const keys = Array.from({ length: sc.keys }, (_, k) => String.fromCharCode(120 + k));
    for (let c2 = 0; c2 < sc.clients; c2++) {
      const id = this.n + c2;
      const client = new Client(this.clientEnv(id), keys, sc.clientStop, this.history);
      this.clients.push(client);
      client.start();
    }
    for (const f of sc.faults) this.push({ time: f.at, seq: 0, kind: "fault", fault: f });
    this.record("start", null, "cluster boots", null, []);
    const maxSteps = this.opts.maxSteps ?? 2e6;
    while (this.queue.size > 0) {
      const ev = this.queue.peek();
      if (ev.time > sc.duration) break;
      this.queue.pop();
      this.time = ev.time;
      this.dispatch(ev);
      if (this.checker.violation || this.step >= maxSteps) break;
    }
    let violation = this.checker.violation;
    let lin = null;
    if (!violation && this.opts.checkLinearizability !== false) {
      lin = checkLinearizability(this.history);
      if (!lin.ok && lin.culprit) {
        const op = lin.culprit;
        violation = {
          kind: "linearizability",
          message: `No legal order exists for key "${lin.key}": ${procName(op.client, this.n)} ${op.kind === "get" ? `read ${op.value === null ? "nothing" : `"${op.value}"`}` : `wrote "${op.value}"`} but no sequential execution consistent with real time produces that`,
          step: op.retStep,
          time: op.retTime,
          node: null
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
  push(ev) {
    ev.seq = this.seq++;
    this.queue.push(ev);
  }
  dispatch(ev) {
    if (ev.kind === "timer") {
      if (this.timerGen[ev.proc].get(ev.name) !== ev.gen || this.life[ev.proc] !== ev.life) return;
      if (ev.proc < this.n && !this.servers[ev.proc]) return;
      this.beginStep(ev.proc);
      const parents = [this.lastEffectOf[ev.proc]];
      if (ev.proc < this.n) this.servers[ev.proc].onTimer(ev.name);
      else this.clients[ev.proc - this.n].onTimer(ev.name);
      this.record("timer", ev.proc, timerLabel(ev.name, procName(ev.proc, this.n)), null, parents);
      return;
    }
    if (ev.kind === "deliver") {
      const tm = this.trace?.msgs[ev.msgId];
      if (ev.to < this.n && !this.servers[ev.to]) {
        if (tm) tm.fate = "dead";
        return;
      }
      if (ev.from < this.n && ev.to < this.n && !this.connected(ev.from, ev.to)) {
        if (tm) tm.fate = "partitioned";
        return;
      }
      this.beginStep(ev.to);
      const parents = [this.lastEffectOf[ev.to]];
      if (tm) {
        tm.fate = "delivered";
        tm.deliverStep = this.step;
        parents.push(tm.sendStep);
      }
      if (ev.to < this.n) this.servers[ev.to].onMessage(ev.from, ev.body);
      else this.clients[ev.to - this.n].onMessage(ev.from, ev.body);
      this.record(
        "deliver",
        ev.to,
        `${procName(ev.to, this.n)} \u2190 ${procName(ev.from, this.n)}  ${msgLabel(ev.body, this.n)}`,
        ev.msgId,
        parents
      );
      return;
    }
    this.applyFault(ev.fault);
  }
  connected(a, b) {
    const ga = this.groups[a];
    const gb = this.groups[b];
    return ga === gb || ga === -1 || gb === -1;
  }
  beginStep(proc) {
    this.step++;
    this.notes = [];
    if (this.trace && proc !== null) this.stateBefore = this.stateKey(proc);
  }
  stateKey(proc) {
    if (proc >= this.n) return this.clients[proc - this.n].stateKey();
    const s = this.servers[proc];
    return s ? `${this.life[proc]}|${s.role}|${s.term}|${s.votedFor}|${s.logVersion}|${s.commitIndex}` : `dead|${this.life[proc]}`;
  }
  applyFault(f) {
    const name = (i) => procName(i, this.n);
    switch (f.kind) {
      case "crash": {
        if (!this.servers[f.node]) return;
        this.beginStep(f.node);
        const parents = [this.lastEffectOf[f.node]];
        this.servers[f.node] = null;
        this.life[f.node]++;
        this.record("fault", f.node, `CRASH ${name(f.node)}`, null, parents);
        return;
      }
      case "restart": {
        if (this.servers[f.node]) return;
        this.beginStep(f.node);
        const parents = [this.lastEffectOf[f.node]];
        this.life[f.node]++;
        this.bootServer(f.node);
        this.record("fault", f.node, `RESTART ${name(f.node)} (term ${this.storage[f.node]?.term ?? 0} from disk)`, null, parents);
        return;
      }
      case "partition": {
        this.beginStep(null);
        const groups = Array.from({ length: this.n }, () => 0);
        f.groups.forEach((g, gi) => g.forEach((node) => groups[node] = gi));
        if (f.bridge !== void 0) groups[f.bridge] = -1;
        this.groups = groups;
        const desc = f.groups.map((g) => `{${g.map(name).join(" ")}}`).join(f.bridge === void 0 ? " | " : ` |${name(f.bridge)}| `);
        this.record("fault", null, `${f.bridge === void 0 ? "PARTITION" : "BRIDGE PARTITION"} ${desc}`, null, []);
        return;
      }
      case "heal": {
        if (this.groups.every((g) => g === this.groups[0])) return;
        this.beginStep(null);
        this.groups = this.groups.map(() => 0);
        this.record("fault", null, "HEAL network", null, []);
        return;
      }
    }
  }
  send(from, to, body) {
    const msgId = this.msgCount++;
    const net = this.sc.net;
    const link = from * 1024 + to;
    let rng = this.linkRng.get(link);
    if (!rng) {
      rng = new Rng(deriveSeed(this.sc.seed, 1, link));
      this.linkRng.set(link, rng);
    }
    const dropped = rng.chance(net.dropRate);
    const delay = rng.chance(net.spikeRate) ? rng.int(net.spikeMin, net.spikeMax + 1) : rng.int(net.minDelay, net.maxDelay + 1);
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
        fate: dropped ? "dropped" : "pending",
        deliverStep: null
      });
    }
    if (dropped) return;
    this.push({ time: this.time + delay, seq: 0, kind: "deliver", msgId, from, to, body });
  }
  setTimer(proc, name, delay) {
    const gen = (this.timerGen[proc].get(name) ?? 0) + 1;
    this.timerGen[proc].set(name, gen);
    this.push({ time: this.time + delay, seq: 0, kind: "timer", proc, name, gen, life: this.life[proc] });
  }
  bootServer(i) {
    const env = {
      id: i,
      n: this.n,
      bugs: this.bugs,
      now: () => this.time,
      rand: () => this.procRng[i].next(),
      send: (to, msg) => this.send(i, to, msg),
      setTimer: (name, delay) => this.setTimer(i, name, delay),
      persist: (state) => {
        this.storage[i] = state;
      },
      observe: (o) => this.observe(o)
    };
    const node = new RaftNode(env, this.storage[i]);
    this.servers[i] = node;
    node.start();
  }
  clientEnv(id) {
    return {
      id,
      servers: this.n,
      now: () => this.time,
      step: () => this.step,
      rand: () => this.procRng[id].next(),
      send: (to, msg) => this.send(id, to, msg),
      setTimer: (name, delay) => this.setTimer(id, name, delay),
      clearTimer: (name) => {
        this.timerGen[id].set(name, (this.timerGen[id].get(name) ?? 0) + 1);
      }
    };
  }
  observe(o) {
    this.checker.observe(o);
    if (o.kind === "leader") {
      this.stats.elections++;
      this.stats.maxTerm = Math.max(this.stats.maxTerm, o.term);
      if (this.trace) this.notes.push({ kind: "leader", node: o.node, term: o.term, text: `n${o.node} becomes LEADER of term ${o.term}` });
    } else if (o.kind === "commit") {
      this.stats.committed = Math.max(this.stats.committed, o.from + o.terms.length - 1);
      if (this.trace) {
        const index = o.from + o.terms.length - 1;
        this.notes.push({ kind: "commit", node: o.node, term: o.term, index, text: `n${o.node} commits through #${index}` });
      }
    } else if (o.kind === "role" && o.role === "candidate" && this.trace) {
      this.notes.push({ kind: "election", node: o.node, term: o.term, text: `n${o.node} starts election for term ${o.term}` });
    }
  }
  // ---- tracing ------------------------------------------------------------
  record(kind, proc, label, msg, parents) {
    if (!this.trace) return;
    if (proc !== null && (kind === "fault" || this.stateKey(proc) !== this.stateBefore)) this.lastEffectOf[proc] = this.step;
    const nodes = [];
    for (let i = 0; i < this.n; i++) {
      const s = this.servers[i];
      if (!s) {
        const disk = this.storage[i];
        nodes.push({
          alive: false,
          role: "follower",
          term: disk?.term ?? 0,
          votedFor: disk?.votedFor ?? null,
          commit: 0,
          applied: 0,
          leaderId: null,
          log: disk ? disk.log.map((e) => e.term) : []
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
        log: cache.terms
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
      groups: this.groups
    });
  }
};
function timerLabel(name, who) {
  switch (name) {
    case "election":
      return `${who} election timeout`;
    case "heartbeat":
      return `${who} heartbeat`;
    case "vote-retry":
      return `${who} re-requests votes`;
    case "next":
      return `${who} issues next op`;
    case "retry":
      return `${who} times out, retries elsewhere`;
    case "resend":
      return `${who} follows redirect`;
    default:
      return `${who} timer ${name}`;
  }
}
function msgLabel(m, servers) {
  switch (m.type) {
    case "RequestVote":
      return `RequestVote t${m.term} (log ${m.lastLogIndex}@t${m.lastLogTerm})`;
    case "RequestVoteReply":
      return `Vote ${m.granted ? "GRANTED" : "denied"} t${m.term}`;
    case "AppendEntries":
      return m.entries.length === 0 ? `Heartbeat t${m.term} commit=${m.leaderCommit}` : `Append t${m.term} #${m.prevLogIndex + 1}..${m.prevLogIndex + m.entries.length} commit=${m.leaderCommit}`;
    case "AppendEntriesReply":
      return m.success ? `AppendOK t${m.term} match=${m.matchIndex}` : `AppendReject t${m.term}`;
    case "ClientRequest":
      return m.op.kind === "put" ? `put ${m.op.key}="${m.op.value}"` : `get ${m.op.key}`;
    case "ClientReply":
      return m.ok ? `ok ${m.value === null ? "(nil)" : `"${m.value}"`}` : `not leader${m.leaderHint === null ? "" : `, try ${procName(m.leaderHint, servers)}`}`;
  }
}

// src/faults.ts
function generateFaults(seed, nodes, duration) {
  const rng = new Rng(deriveSeed(seed, 3));
  const faults = [];
  let t = rng.int(100, 400);
  while (t < duration - 200) {
    const r = rng.next();
    if (r < 0.25) {
      const node = rng.int(0, nodes);
      faults.push({ at: t, kind: "crash", node });
      faults.push({ at: t + rng.int(2, 40), kind: "restart", node });
    } else if (r < 0.5) {
      const node = rng.int(0, nodes);
      faults.push({ at: t, kind: "crash", node });
      faults.push({ at: t + rng.int(30, 1200), kind: "restart", node });
    } else {
      const order = rng.shuffle(Array.from({ length: nodes }, (_, i) => i));
      if (rng.chance(0.3) && nodes >= 3) {
        const bridge = order.pop();
        const split = Math.max(1, Math.floor(order.length / 2));
        faults.push({ at: t, kind: "partition", groups: [sorted(order.slice(0, split)), sorted(order.slice(split))], bridge });
      } else {
        const split = rng.chance(0.4) ? 1 : rng.int(1, nodes);
        faults.push({ at: t, kind: "partition", groups: [sorted(order.slice(0, split)), sorted(order.slice(split))] });
      }
      faults.push({ at: t + rng.int(100, 1500), kind: "heal" });
    }
    t += rng.int(60, 500);
  }
  return faults.sort((a, b) => a.at - b.at);
}
function makeScenario(seed, o = {}) {
  const nodes = o.nodes ?? 5;
  const duration = o.duration ?? 6e3;
  return {
    seed,
    nodes,
    clients: o.clients ?? 3,
    keys: o.keys ?? 2,
    duration,
    clientStop: duration - 500,
    bugs: o.bugs ?? [],
    net: { ...DEFAULT_NET },
    faults: generateFaults(seed, nodes, duration)
  };
}
function sorted(xs) {
  return xs.sort((a, b) => a - b);
}

// src/shrink.ts
function shrink(original, failing, onProgress) {
  const kind = failing.violation.kind;
  let best = clone(original);
  let bestResult = failing;
  let runs = 0;
  const attempt = (candidate, phase) => {
    runs++;
    const r = runScenario(candidate);
    const ok = sameBug(r.violation, kind);
    if (ok) {
      best = candidate;
      bestResult = r;
    }
    onProgress?.({ phase, runs, faults: best.faults.length, steps: bestResult.stats.steps });
    return ok;
  };
  const trim = () => {
    const v = bestResult.violation;
    const cut = { ...best, duration: v.time + 1, faults: best.faults.filter((f) => f.at <= v.time) };
    if (cut.duration < best.duration || cut.faults.length < best.faults.length) attempt(cut, "trim");
  };
  trim();
  for (let round = 0; round < 3; round++) {
    const startFaults = best.faults.length;
    const startClients = best.clients;
    const startNet = JSON.stringify(best.net);
    best = ddmin(best, (faults) => attempt({ ...best, faults }, "faults"));
    trim();
    while (best.clients > 1 && attempt({ ...best, clients: best.clients - 1 }, "clients")) trim();
    if (best.net.dropRate > 0) attempt({ ...best, net: { ...best.net, dropRate: 0 } }, "network");
    if (best.net.spikeRate > 0) attempt({ ...best, net: { ...best.net, spikeRate: 0 } }, "network");
    trim();
    if (best.faults.length === startFaults && best.clients === startClients && JSON.stringify(best.net) === startNet) break;
  }
  const shape = (s, r) => ({
    faults: s.faults.length,
    steps: r.stats.steps,
    clients: s.clients,
    duration: s.duration
  });
  return { scenario: best, result: bestResult, runs, before: shape(original, failing), after: shape(best, bestResult) };
}
function ddmin(sc, test) {
  let items = sc.faults;
  if (items.length > 0 && test([])) return { ...sc, faults: [] };
  let n = 2;
  while (items.length >= 2) {
    const size = Math.ceil(items.length / n);
    const chunks = [];
    for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
    let reduced = false;
    for (const chunk of chunks) {
      if (test(chunk)) {
        items = chunk;
        n = 2;
        reduced = true;
        break;
      }
    }
    if (!reduced) {
      for (let i = 0; i < chunks.length; i++) {
        const complement = chunks.filter((_, j) => j !== i).flat();
        if (test(complement)) {
          items = complement;
          n = Math.max(n - 1, 2);
          reduced = true;
          break;
        }
      }
    }
    if (!reduced) {
      if (n >= items.length) break;
      n = Math.min(n * 2, items.length);
    }
  }
  if (items.length === 1 && test([])) items = [];
  return { ...sc, faults: items };
}
function sameBug(v, kind) {
  return v !== null && v.kind === "linearizability" === (kind === "linearizability");
}
function clone(sc) {
  return JSON.parse(JSON.stringify(sc));
}

// src/repro.ts
function encodeRepro(sc) {
  const json = JSON.stringify({ v: 1, ...sc });
  const bytes = new TextEncoder().encode(json);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function decodeRepro(code) {
  const b64 = code.trim().replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(b64 + "=".repeat((4 - b64.length % 4) % 4));
  const bytes = Uint8Array.from(bin, (c2) => c2.charCodeAt(0));
  const { v, ...sc } = JSON.parse(new TextDecoder().decode(bytes));
  if (v !== 1) throw new Error(`Unsupported repro version ${v}`);
  return sc;
}

// src/causal.ts
function causalSlice(trace, target) {
  const byStep = new Map(trace.steps.map((s) => [s.step, s]));
  const keep = /* @__PURE__ */ new Set();
  const stack = [target];
  while (stack.length) {
    const s = stack.pop();
    if (keep.has(s)) continue;
    keep.add(s);
    for (const p of byStep.get(s)?.parents ?? []) if (!keep.has(p)) stack.push(p);
  }
  for (const s of trace.steps) if (s.kind === "fault" && s.step <= target) keep.add(s.step);
  keep.add(0);
  return keep;
}

// src/story.ts
function tellStory(trace, v, lin) {
  const key = /* @__PURE__ */ new Set([0, v.step]);
  for (const s of trace.steps) {
    if (s.step > v.step) break;
    if (s.kind === "fault") key.add(s.step);
    if (s.notes.some((n2) => n2.kind === "leader" || n2.kind === "election")) key.add(s.step);
  }
  const n = trace.servers;
  let explanation = v.message + ".";
  if (v.kind === "election-safety" && v.term !== void 0) {
    const voters = /* @__PURE__ */ new Map();
    for (const m of trace.msgs) {
      if (m.type !== "RequestVoteReply" || m.fate !== "delivered" || !m.label.startsWith("Vote GRANTED") || !m.label.endsWith(`t${v.term}`)) continue;
      key.add(m.deliverStep);
      key.add(m.sendStep);
      if (!voters.has(m.from)) voters.set(m.from, []);
      voters.get(m.from).push(m.to);
    }
    const doubles = [...voters].filter(([, cands]) => new Set(cands).size > 1);
    explanation = `${v.message}. ` + (doubles.length ? doubles.map(([voter, cands]) => {
      const restarted = trace.steps.some((s) => s.step <= v.step && s.kind === "fault" && s.proc === voter && s.label.startsWith("RESTART"));
      return `${procName(voter, n)} granted its term-${v.term} vote to ${[...new Set(cands)].map((c2) => procName(c2, n)).join(" and ")}${restarted ? ", having forgotten its first vote across a crash and restart" : ""}`;
    }).join("; ") + ". Each candidate counted a majority, so both became leader for the same term." : "Two candidates each assembled a majority in the same term.");
  }
  if ((v.kind === "leader-completeness" || v.kind === "state-machine-safety") && v.index !== void 0) {
    const commit = trace.steps.find((s) => s.notes.some((x) => x.kind === "commit" && (x.index ?? 0) >= v.index));
    if (commit) key.add(commit.step);
    explanation = `${v.message}. Entry #${v.index} had already been committed${commit ? ` (at ${commit.time}ms)` : ""}, so a client may have been told it succeeded. A leader without it will overwrite it on every follower: acknowledged data is lost.`;
  }
  if (v.kind === "linearizability" && lin?.culprit && lin.window) {
    const read = lin.culprit;
    for (const step of [read.callStep, read.retStep]) if (step !== null) key.add(step);
    if (read.kind === "get") {
      const source = lin.window.find((o) => o.kind === "put" && o.value === read.value);
      const newer = lin.window.find(
        (o) => o.kind === "put" && o !== source && o.retStep !== null && o.retStep < read.callStep && (source === void 0 || source.retStep !== null && o.callStep > source.retStep)
      );
      for (const op of [source, newer]) if (op) {
        for (const step of [op.callStep, op.retStep]) if (step !== null) key.add(step);
      }
      if (newer) {
        explanation = `Stale read on "${lin.key}". ${describe(newer, n)} was acknowledged at ${newer.retTime}ms, ${source ? `after ${describe(source, n)} had finished, ` : ""}and then ${describe(read, n)} started at ${read.callTime}ms and still saw ${read.value === null ? "nothing" : `"${read.value}"`}. No order of these operations that respects real time can explain that.`;
      } else {
        explanation = `${describe(read, n)} returned ${read.value === null ? "nothing" : `"${read.value}"`}, which no legal ordering of the operations on "${lin.key}" can produce.`;
      }
    }
  }
  return { keySteps: key, explanation };
}
function describe(op, servers) {
  const who = procName(op.client, servers);
  return op.kind === "put" ? `${who}'s write of "${op.value}"` : `${who}'s read`;
}

// src/cli.ts
var c = {
  dim: (s) => `\x1B[2m${s}\x1B[0m`,
  bold: (s) => `\x1B[1m${s}\x1B[0m`,
  red: (s) => `\x1B[31m${s}\x1B[0m`,
  green: (s) => `\x1B[32m${s}\x1B[0m`,
  yellow: (s) => `\x1B[33m${s}\x1B[0m`,
  cyan: (s) => `\x1B[36m${s}\x1B[0m`
};
function flag(args, name, fallback) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
}
function usage() {
  console.log(`${c.bold("faultline")} \u2014 deterministic simulation testing for a Raft cluster

  faultline soak  [--seeds 2000] [--nodes 5]            fuzz the correct implementation
  faultline hunt  --bug <id> [--seeds 5000] [--start 1]  find, shrink and explain a planted bug
  faultline replay <repro> [--all]                       replay a repro string step by step
  faultline bugs                                         list planted bugs`);
}
function main(argv) {
  const [cmd, ...args] = argv;
  switch (cmd) {
    case "soak":
      return soak(Number(flag(args, "seeds", "2000")), Number(flag(args, "nodes", "5")));
    case "hunt": {
      const bug = flag(args, "bug");
      if (!bug || !BUGS.some((b) => b.id === bug)) {
        console.error(`--bug must be one of: ${BUGS.map((b) => b.id).join(", ")}`);
        return 2;
      }
      return huntCmd(bug, Number(flag(args, "seeds", "5000")), Number(flag(args, "start", "1")), Number(flag(args, "nodes", "5")));
    }
    case "replay":
      if (!args[0]) return usage(), 2;
      return replay(decodeRepro(args[0]), args.includes("--all"));
    case "bugs":
      for (const b of BUGS) console.log(`${c.bold(b.id.padEnd(18))} ${b.title}
${" ".repeat(19)}${c.dim(b.description)}
`);
      return 0;
    default:
      usage();
      return cmd ? 2 : 0;
  }
}
function soak(seeds, nodes) {
  const t0 = performance.now();
  let steps = 0;
  let ops = 0;
  let elections = 0;
  for (let seed = 1; seed <= seeds; seed++) {
    const r = runScenario(makeScenario(seed, { nodes }));
    steps += r.stats.steps;
    ops += r.stats.opsOk;
    elections += r.stats.elections;
    if (r.violation) {
      console.log(c.red(`
seed ${seed}: ${r.violation.kind}: ${r.violation.message}`));
      console.log(`repro: ${encodeRepro(r.scenario)}`);
      return 1;
    }
    if (seed % 100 === 0) process.stdout.write(`\r  ${seed}/${seeds} seeds clean`);
  }
  const secs = (performance.now() - t0) / 1e3;
  console.log(
    `\r${c.green("\u2713")} ${seeds} seeds, ${steps.toLocaleString()} simulated events, ${elections.toLocaleString()} elections, ${ops.toLocaleString()} client ops checked for linearizability`
  );
  console.log(c.dim(`  no violations \xB7 ${secs.toFixed(1)}s wall clock \xB7 ${Math.round(steps / secs).toLocaleString()} events/s`));
  return 0;
}
function huntCmd(bug, maxSeeds, start, nodes) {
  const info = BUGS.find((b) => b.id === bug);
  console.log(`${c.bold("Hunting")} ${info.title} ${c.dim(`(${bug})`)} across up to ${maxSeeds} seeds
`);
  const t0 = performance.now();
  for (let i = 0; i < maxSeeds; i++) {
    const seed = start + i;
    const sc = makeScenario(seed, { nodes, bugs: [bug] });
    const r = runScenario(sc);
    if (i % 25 === 0 && process.stdout.isTTY) process.stdout.write(`\r  seed ${seed}\u2026`);
    if (!r.violation) continue;
    const secs = ((performance.now() - t0) / 1e3).toFixed(2);
    console.log(`\r${c.red("\u2717")} seed ${seed} after ${i + 1} seeds (${secs}s): ${c.bold(r.violation.kind)}`);
    console.log(`  ${r.violation.message}
`);
    const tty = process.stdout.isTTY;
    if (tty) process.stdout.write(c.dim("  shrinking\u2026"));
    const s = shrink(sc, r, (p) => {
      if (tty) process.stdout.write(`\r${c.dim(`  shrinking (${p.phase}): ${p.runs} runs, ${p.faults} faults left`)}   `);
    });
    console.log(
      `\r  ${c.bold("shrunk")} in ${s.runs} runs: faults ${s.before.faults} \u2192 ${c.green(String(s.after.faults))}, events ${s.before.steps} \u2192 ${c.green(
        String(s.after.steps)
      )}, clients ${s.before.clients} \u2192 ${s.after.clients}                 
`
    );
    replay(s.scenario, false);
    return 1;
  }
  console.log(`\r${c.green("\u2713")} no violation in ${maxSeeds} seeds`);
  return 0;
}
function replay(sc, all) {
  const r = runScenario(sc, { record: true });
  const trace = r.trace;
  if (!r.violation) {
    console.log(c.green("No violation reproduced."));
    return 0;
  }
  const slice = causalSlice(trace, r.violation.step);
  const story = tellStory(trace, r.violation, r.linearizability);
  const shown = trace.steps.filter((s) => s.step <= r.violation.step && (all || slice.has(s.step) && story.keySteps.has(s.step)));
  console.log(
    c.bold(all ? "Full execution" : "Key moments") + c.dim(` \u2014 ${shown.length} shown \xB7 ${slice.size} of ${trace.steps.length} events are causally upstream of the violation`)
  );
  for (const st of shown) {
    const t = c.dim(`${String(st.time).padStart(5)}ms #${String(st.step).padEnd(5)}`);
    const label = st.kind === "fault" ? c.yellow(st.label) : st.label;
    const mark = st.step === r.violation.step ? c.red(" \u25C0 violation") : "";
    console.log(`  ${t} ${label}${mark}`);
    for (const note of st.notes) if (all || note.kind !== "commit") console.log(`  ${" ".repeat(15)}${c.cyan("\u21B3 " + note.text)}`);
  }
  console.log(`
${c.red(c.bold(r.violation.kind))}: ${story.explanation}`);
  if (r.linearizability?.window) {
    console.log(c.dim(`
history of key "${r.linearizability.key}" up to the failure:`));
    for (const op of r.linearizability.window) {
      const who = procName(op.client, sc.nodes);
      const what = op.kind === "put" ? `put "${op.value}"` : op.retStep === null ? "get \u2192 ?" : `get \u2192 ${op.value === null ? "nil" : `"${op.value}"`}`;
      const span = `${op.callTime}ms..${op.retTime === null ? "?" : op.retTime + "ms"}`;
      const mark = op === r.linearizability.culprit ? c.red("  \u25C0 impossible") : "";
      console.log(`  ${who} ${what.padEnd(18)} ${c.dim(span)}${mark}`);
    }
  }
  console.log(`
${c.dim("repro:")} ${encodeRepro(sc)}`);
  return 1;
}
process.exitCode = main(process.argv.slice(2));
