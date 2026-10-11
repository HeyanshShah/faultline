export type BugId = 'forget-vote' | 'no-uptodate-check' | 'stale-read' | 'truncate-always';

export interface BugInfo {
  id: BugId;
  title: string;
  description: string;
}

export const BUGS: readonly BugInfo[] = [
  {
    id: 'stale-read',
    title: 'Leader-local reads',
    description:
      'The leader answers reads from its own state machine without confirming it is still leader. A deposed leader on the wrong side of a partition serves stale data. Raft invariants all hold; only the linearizability checker can see it.',
  },
  {
    id: 'forget-vote',
    title: 'votedFor not persisted',
    description:
      'currentTerm and the log are fsynced but votedFor lives only in memory. A node that crashes and restarts mid-election can vote twice in one term, electing two leaders.',
  },
  {
    id: 'no-uptodate-check',
    title: 'Missing up-to-date vote check',
    description:
      'Voters grant votes without checking that the candidate log is at least as up to date as their own. A node missing committed entries can win and overwrite them.',
  },
  {
    id: 'truncate-always',
    title: 'Truncate on every AppendEntries',
    description:
      'Followers truncate their log after prevLogIndex on every AppendEntries instead of only on conflict. A delayed, reordered message silently deletes acknowledged entries.',
  },
];

export type Role = 'follower' | 'candidate' | 'leader';

export type Cmd =
  | { kind: 'noop' }
  | { kind: 'put'; key: string; value: string; client: number; seq: number }
  | { kind: 'get'; key: string; client: number; seq: number };

export interface Entry {
  term: number;
  cmd: Cmd;
}

export interface Persistent {
  term: number;
  votedFor: number | null;
  log: Entry[];
}

export type ClientOp = { kind: 'put'; key: string; value: string } | { kind: 'get'; key: string };

export type Msg =
  | { type: 'RequestVote'; term: number; candidateId: number; lastLogIndex: number; lastLogTerm: number }
  | { type: 'RequestVoteReply'; term: number; granted: boolean }
  | {
      type: 'AppendEntries';
      term: number;
      leaderId: number;
      prevLogIndex: number;
      prevLogTerm: number;
      entries: Entry[];
      leaderCommit: number;
    }
  | { type: 'AppendEntriesReply'; term: number; success: boolean; matchIndex: number }
  | { type: 'ClientRequest'; client: number; seq: number; op: ClientOp }
  | { type: 'ClientReply'; seq: number; ok: boolean; value: string | null; leaderHint: number | null };

export type Observation =
  | { kind: 'role'; node: number; role: Role; term: number }
  | { kind: 'leader'; node: number; term: number; logTerms: number[] }
  | { kind: 'commit'; node: number; term: number; from: number; terms: number[] }
  | { kind: 'apply'; node: number; index: number; term: number; cmdId: string };

export type Fault =
  | { at: number; kind: 'crash'; node: number }
  | { at: number; kind: 'restart'; node: number }
  /** Servers in different groups cannot talk; a bridge node (if any) can talk to everyone (Jepsen's "bridge" nemesis). */
  | { at: number; kind: 'partition'; groups: number[][]; bridge?: number }
  | { at: number; kind: 'heal' };

export interface NetConfig {
  minDelay: number;
  maxDelay: number;
  dropRate: number;
  spikeRate: number;
  spikeMin: number;
  spikeMax: number;
}

export interface Scenario {
  seed: number;
  nodes: number;
  clients: number;
  keys: number;
  /** Simulated milliseconds to run. */
  duration: number;
  /** Clients stop issuing new operations after this time. Kept separate from duration so trimming the run never changes behaviour before the cut. */
  clientStop: number;
  bugs: BugId[];
  net: NetConfig;
  faults: Fault[];
}

export type ViolationKind = 'election-safety' | 'leader-completeness' | 'state-machine-safety' | 'linearizability';

export interface Violation {
  kind: ViolationKind;
  message: string;
  step: number;
  time: number;
  node: number | null;
  /** Term (election safety) or log index (completeness/state machine safety) the violation is about. */
  term?: number;
  index?: number;
}

export interface HistoryOp {
  id: number;
  client: number;
  kind: 'put' | 'get';
  key: string;
  /** Argument for put, result for get. */
  value: string | null;
  callTime: number;
  retTime: number | null;
  callStep: number;
  retStep: number | null;
}

export const DEFAULT_NET: NetConfig = {
  minDelay: 2,
  maxDelay: 12,
  dropRate: 0.02,
  spikeRate: 0.04,
  spikeMin: 40,
  spikeMax: 250,
};
