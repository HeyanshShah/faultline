import type { BugId, Cmd, Entry, Msg, Observation, Persistent, Role } from './types';

/** Everything a Raft node can do to the outside world. The simulator implements it; nothing else is reachable. */
export interface NodeEnv {
  readonly id: number;
  /** Number of servers; server ids are 0..n-1. */
  readonly n: number;
  readonly bugs: ReadonlySet<BugId>;
  now(): number;
  rand(): number;
  send(to: number, msg: Msg): void;
  /** Arms (or re-arms) the named timer; a re-arm cancels the previous one. */
  setTimer(name: string, delay: number): void;
  /** Durably stores state that survives a crash. */
  persist(state: Persistent): void;
  observe(o: Observation): void;
}

export const ELECTION_MIN = 150;
export const ELECTION_SPREAD = 150;
export const HEARTBEAT = 50;
const MAX_BATCH = 16;

export function cmdId(cmd: Cmd): string {
  return cmd.kind === 'noop' ? 'noop' : `${cmd.kind}:${cmd.client}:${cmd.seq}`;
}

/**
 * A Raft server (leader election, log replication, a deduplicating key-value state machine).
 * Correct by default; each entry in `env.bugs` swaps in one realistic, previously-shipped-style mistake.
 */
export class RaftNode {
  term: number;
  votedFor: number | null;
  log: Entry[];
  role: Role = 'follower';
  leaderId: number | null = null;
  commitIndex = 0;
  lastApplied = 0;
  /** Bumped on every log mutation so observers can cheaply detect change. */
  logVersion = 0;

  private votes = new Set<number>();
  private nextIndex: number[] = [];
  private matchIndex: number[] = [];
  readonly kv = new Map<string, string>();
  private sessions = new Map<number, { seq: number; value: string | null }>();

  constructor(
    private readonly env: NodeEnv,
    persisted?: Persistent,
  ) {
    this.term = persisted?.term ?? 0;
    this.votedFor = persisted?.votedFor ?? null;
    this.log = persisted ? persisted.log.slice() : [];
  }

  get id(): number {
    return this.env.id;
  }

  get lastIndex(): number {
    return this.log.length;
  }

  termAt(index: number): number {
    return index === 0 ? 0 : this.log[index - 1].term;
  }

  start(): void {
    this.resetElectionTimer();
  }

  onTimer(name: string): void {
    if (name === 'election') {
      if (this.role !== 'leader') this.startElection();
    } else if (name === 'vote-retry') {
      // Candidates retransmit RequestVote to anyone who has not granted yet (RPCs are retried, Raft §5.1).
      if (this.role === 'candidate') {
        this.requestVotes();
        this.env.setTimer('vote-retry', HEARTBEAT);
      }
    } else if (name === 'heartbeat') {
      if (this.role === 'leader') {
        this.broadcastAppend();
        this.env.setTimer('heartbeat', HEARTBEAT);
      }
    }
  }

  onMessage(from: number, m: Msg): void {
    switch (m.type) {
      case 'RequestVote':
        return this.onRequestVote(from, m);
      case 'RequestVoteReply':
        return this.onVoteReply(from, m);
      case 'AppendEntries':
        return this.onAppendEntries(from, m);
      case 'AppendEntriesReply':
        return this.onAppendReply(from, m);
      case 'ClientRequest':
        return this.onClientRequest(from, m);
      case 'ClientReply':
        return;
    }
  }

  // ---- persistence & roles ------------------------------------------------

  private persist(): void {
    this.env.persist({
      term: this.term,
      // BUG forget-vote: votedFor is never written to stable storage.
      votedFor: this.env.bugs.has('forget-vote') ? null : this.votedFor,
      log: this.log.slice(),
    });
  }

  private resetElectionTimer(): void {
    this.env.setTimer('election', ELECTION_MIN + Math.floor(this.env.rand() * ELECTION_SPREAD));
  }

  private setRole(role: Role): void {
    if (role === this.role) return;
    this.role = role;
    this.env.observe({ kind: 'role', node: this.id, role, term: this.term });
  }

  private stepDown(term: number): void {
    const wasLeader = this.role === 'leader';
    if (term > this.term) {
      this.term = term;
      this.votedFor = null;
      this.leaderId = null;
    }
    this.setRole('follower');
    this.persist();
    if (wasLeader) this.resetElectionTimer();
  }

  // ---- elections ----------------------------------------------------------

  private startElection(): void {
    this.term += 1;
    this.votedFor = this.id;
    this.leaderId = null;
    this.votes = new Set([this.id]);
    this.role = 'follower';
    this.setRole('candidate');
    this.persist();
    this.resetElectionTimer();
    this.requestVotes();
    this.env.setTimer('vote-retry', HEARTBEAT);
  }

  private requestVotes(): void {
    for (let p = 0; p < this.env.n; p++) {
      if (this.votes.has(p)) continue;
      this.env.send(p, {
        type: 'RequestVote',
        term: this.term,
        candidateId: this.id,
        lastLogIndex: this.lastIndex,
        lastLogTerm: this.termAt(this.lastIndex),
      });
    }
  }

  private onRequestVote(from: number, m: Extract<Msg, { type: 'RequestVote' }>): void {
    if (m.term > this.term) this.stepDown(m.term);
    const myLastTerm = this.termAt(this.lastIndex);
    const upToDate =
      m.lastLogTerm > myLastTerm || (m.lastLogTerm === myLastTerm && m.lastLogIndex >= this.lastIndex);
    const granted =
      m.term === this.term &&
      (this.votedFor === null || this.votedFor === m.candidateId) &&
      // BUG no-uptodate-check: votes for anyone, regardless of how stale their log is.
      (upToDate || this.env.bugs.has('no-uptodate-check'));
    if (granted) {
      this.votedFor = m.candidateId;
      this.persist();
      this.resetElectionTimer();
    }
    this.env.send(from, { type: 'RequestVoteReply', term: this.term, granted });
  }

  private onVoteReply(from: number, m: Extract<Msg, { type: 'RequestVoteReply' }>): void {
    if (m.term > this.term) return this.stepDown(m.term);
    if (this.role !== 'candidate' || m.term !== this.term || !m.granted) return;
    this.votes.add(from);
    if (this.votes.size * 2 > this.env.n) this.becomeLeader();
  }

  private becomeLeader(): void {
    this.setRole('leader');
    this.leaderId = this.id;
    this.env.observe({ kind: 'leader', node: this.id, term: this.term, logTerms: this.log.map((e) => e.term) });
    this.nextIndex = Array.from({ length: this.env.n }, () => this.lastIndex + 1);
    this.matchIndex = Array.from({ length: this.env.n }, () => 0);
    // A no-op in the new term lets the leader commit (and learn) everything before it.
    this.appendLocal({ kind: 'noop' });
    this.broadcastAppend();
    this.env.setTimer('heartbeat', HEARTBEAT);
  }

  // ---- replication --------------------------------------------------------

  private appendLocal(cmd: Cmd): void {
    this.log.push({ term: this.term, cmd });
    this.logVersion++;
    this.matchIndex[this.id] = this.lastIndex;
    this.persist();
  }

  private broadcastAppend(): void {
    for (let p = 0; p < this.env.n; p++) if (p !== this.id) this.sendAppend(p);
  }

  private sendAppend(p: number): void {
    const prev = this.nextIndex[p] - 1;
    this.env.send(p, {
      type: 'AppendEntries',
      term: this.term,
      leaderId: this.id,
      prevLogIndex: prev,
      prevLogTerm: this.termAt(prev),
      entries: this.log.slice(prev, prev + MAX_BATCH),
      leaderCommit: this.commitIndex,
    });
  }

  private onAppendEntries(from: number, m: Extract<Msg, { type: 'AppendEntries' }>): void {
    if (m.term < this.term) {
      this.env.send(from, { type: 'AppendEntriesReply', term: this.term, success: false, matchIndex: 0 });
      return;
    }
    if (m.term > this.term || this.role !== 'follower') this.stepDown(m.term);
    this.leaderId = m.leaderId;
    this.resetElectionTimer();

    if (m.prevLogIndex > this.lastIndex || this.termAt(m.prevLogIndex) !== m.prevLogTerm) {
      this.env.send(from, {
        type: 'AppendEntriesReply',
        term: this.term,
        success: false,
        matchIndex: Math.min(this.lastIndex, m.prevLogIndex - 1),
      });
      return;
    }

    let changed = false;
    if (this.env.bugs.has('truncate-always')) {
      // BUG truncate-always: blindly replace the suffix, even when a newer, longer message already landed.
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
    this.env.send(from, { type: 'AppendEntriesReply', term: this.term, success: true, matchIndex: lastNew });
  }

  private onAppendReply(from: number, m: Extract<Msg, { type: 'AppendEntriesReply' }>): void {
    if (m.term > this.term) return this.stepDown(m.term);
    if (this.role !== 'leader' || m.term !== this.term) return;
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

  private advanceCommit(): void {
    for (let n = this.lastIndex; n > this.commitIndex; n--) {
      // Only entries from the current term are committed by counting replicas (Raft §5.4.2).
      if (this.termAt(n) !== this.term) continue;
      let count = 1;
      for (let p = 0; p < this.env.n; p++) if (p !== this.id && this.matchIndex[p] >= n) count++;
      if (count * 2 > this.env.n) {
        const from = this.commitIndex + 1;
        this.commitIndex = n;
        this.env.observe({
          kind: 'commit',
          node: this.id,
          term: this.term,
          from,
          terms: this.log.slice(from - 1, n).map((e) => e.term),
        });
        this.apply();
        return;
      }
    }
  }

  // ---- state machine ------------------------------------------------------

  private apply(): void {
    while (this.lastApplied < this.commitIndex && this.lastApplied < this.lastIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied - 1];
      const cmd = entry.cmd;
      this.env.observe({ kind: 'apply', node: this.id, index: this.lastApplied, term: entry.term, cmdId: cmdId(cmd) });
      if (cmd.kind === 'noop') continue;
      let value: string | null;
      const session = this.sessions.get(cmd.client);
      if (session && cmd.seq <= session.seq) {
        value = cmd.seq === session.seq ? session.value : null;
      } else {
        if (cmd.kind === 'put') {
          this.kv.set(cmd.key, cmd.value);
          value = cmd.value;
        } else {
          value = this.kv.get(cmd.key) ?? null;
        }
        this.sessions.set(cmd.client, { seq: cmd.seq, value });
      }
      if (this.role === 'leader') {
        this.env.send(cmd.client, { type: 'ClientReply', seq: cmd.seq, ok: true, value, leaderHint: this.id });
      }
    }
  }

  private onClientRequest(from: number, m: Extract<Msg, { type: 'ClientRequest' }>): void {
    if (this.role !== 'leader') {
      this.env.send(from, { type: 'ClientReply', seq: m.seq, ok: false, value: null, leaderHint: this.leaderId });
      return;
    }
    if (m.op.kind === 'get' && this.env.bugs.has('stale-read')) {
      // BUG stale-read: serve reads locally, assuming we are still the leader.
      const value = this.kv.get(m.op.key) ?? null;
      this.env.send(from, { type: 'ClientReply', seq: m.seq, ok: true, value, leaderHint: this.id });
      return;
    }
    const session = this.sessions.get(m.client);
    if (session && m.seq <= session.seq) {
      if (m.seq === session.seq) {
        this.env.send(from, { type: 'ClientReply', seq: m.seq, ok: true, value: session.value, leaderHint: this.id });
      }
      return;
    }
    for (let i = this.lastIndex; i > this.lastApplied; i--) {
      const c = this.log[i - 1].cmd;
      if (c.kind !== 'noop' && c.client === m.client && c.seq === m.seq) return; // already in flight
    }
    const cmd: Cmd =
      m.op.kind === 'put'
        ? { kind: 'put', key: m.op.key, value: m.op.value, client: m.client, seq: m.seq }
        : { kind: 'get', key: m.op.key, client: m.client, seq: m.seq };
    this.appendLocal(cmd);
    this.broadcastAppend();
  }
}
