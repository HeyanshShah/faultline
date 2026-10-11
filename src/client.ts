import type { ClientOp, HistoryOp, Msg } from './types';

export interface ClientEnv {
  readonly id: number;
  /** Number of servers; server ids are 0..n-1. */
  readonly servers: number;
  now(): number;
  step(): number;
  rand(): number;
  send(to: number, msg: Msg): void;
  setTimer(name: string, delay: number): void;
  clearTimer(name: string): void;
}

const RETRY_AFTER = 200;
const REDIRECT_BACKOFF = 15;

/**
 * A closed-loop client: one outstanding operation at a time, retried (with the same sequence number,
 * so the server can deduplicate) until a leader acknowledges it. Every call and return goes into the
 * shared history the linearizability checker reads.
 */
export class Client {
  private seq = 0;
  private current: { seq: number; op: ClientOp; record: HistoryOp } | null = null;
  private guess: number;

  constructor(
    private readonly env: ClientEnv,
    private readonly keys: readonly string[],
    private readonly stopAt: number,
    private readonly history: HistoryOp[],
  ) {
    this.guess = Math.floor(env.rand() * env.servers);
  }

  /** Changes whenever the client's observable state changes; used to build a tight causal graph. */
  stateKey(): string {
    return `${this.seq}|${this.current ? 1 : 0}|${this.guess}`;
  }

  start(): void {
    this.env.setTimer('next', 5 + Math.floor(this.env.rand() * 40));
  }

  onTimer(name: string): void {
    if (name === 'next') this.issue();
    else if (name === 'retry' && this.current) {
      this.guess = Math.floor(this.env.rand() * this.env.servers);
      this.sendCurrent();
    } else if (name === 'resend' && this.current) {
      this.sendCurrent();
    }
  }

  onMessage(_from: number, m: Msg): void {
    if (m.type !== 'ClientReply' || !this.current || m.seq !== this.current.seq) return;
    if (!m.ok) {
      this.guess = m.leaderHint ?? Math.floor(this.env.rand() * this.env.servers);
      this.env.setTimer('resend', REDIRECT_BACKOFF);
      return;
    }
    const rec = this.current.record;
    if (rec.kind === 'get') rec.value = m.value;
    rec.retTime = this.env.now();
    rec.retStep = this.env.step();
    this.current = null;
    this.env.clearTimer('retry');
    this.env.setTimer('next', 5 + Math.floor(this.env.rand() * 60));
  }

  private issue(): void {
    if (this.env.now() >= this.stopAt) return;
    this.seq++;
    const key = this.keys[Math.floor(this.env.rand() * this.keys.length)];
    const op: ClientOp =
      this.env.rand() < 0.5 ? { kind: 'put', key, value: `c${this.env.id}.${this.seq}` } : { kind: 'get', key };
    const record: HistoryOp = {
      id: this.history.length,
      client: this.env.id,
      kind: op.kind,
      key,
      value: op.kind === 'put' ? op.value : null,
      callTime: this.env.now(),
      retTime: null,
      callStep: this.env.step(),
      retStep: null,
    };
    this.history.push(record);
    this.current = { seq: this.seq, op, record };
    this.sendCurrent();
  }

  private sendCurrent(): void {
    if (!this.current) return;
    this.env.send(this.guess, { type: 'ClientRequest', client: this.env.id, seq: this.current.seq, op: this.current.op });
    this.env.setTimer('retry', RETRY_AFTER);
  }
}
