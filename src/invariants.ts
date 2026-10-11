import type { Observation, Violation } from './types';

/**
 * Online checker for Raft's safety properties (Raft paper, Figure 3). It sees only observations the
 * nodes emit, never their internals, so the same checker would work against any implementation.
 */
export class InvariantChecker {
  private leaders = new Map<number, number>();
  /** committed[i] = term of the entry known committed at log index i+1. */
  private committed: number[] = [];
  /** committedIn[i] = earliest leader term in which index i+1 was known committed. */
  private committedIn: number[] = [];
  /** applied[i] = identity of the entry first applied at log index i+1. */
  private applied: string[] = [];
  violation: Violation | null = null;

  constructor(private readonly clock: () => { step: number; time: number }) {}

  observe(o: Observation): void {
    if (this.violation) return;
    switch (o.kind) {
      case 'leader': {
        const prev = this.leaders.get(o.term);
        if (prev !== undefined && prev !== o.node) {
          return this.fail('election-safety', o.node, `Two leaders in term ${o.term}: n${prev} and n${o.node}`, { term: o.term });
        }
        this.leaders.set(o.term, o.node);
        // Leader Completeness (Raft §5.4): an entry committed in term T is in the log of every leader of a term > T.
        // A leader of an older, stale term may legitimately lack it, so only earlier commits are checked.
        for (let i = 0; i < this.committed.length; i++) {
          if (this.committedIn[i] < o.term && o.logTerms[i] !== this.committed[i]) {
            return this.fail(
              'leader-completeness',
              o.node,
              `n${o.node} became leader of term ${o.term} without committed entry #${i + 1} (term ${this.committed[i]}); its log has ${
                o.logTerms[i] === undefined ? 'nothing' : `term ${o.logTerms[i]}`
              } there`,
              { term: o.term, index: i + 1 },
            );
          }
        }
        return;
      }
      case 'commit': {
        for (let k = 0; k < o.terms.length; k++) {
          const i = o.from - 1 + k;
          const known = this.committed[i];
          if (known !== undefined && known !== o.terms[k]) {
            return this.fail(
              'state-machine-safety',
              o.node,
              `n${o.node} committed term ${o.terms[k]} at #${i + 1}, but term ${known} was already committed there`,
              { index: i + 1 },
            );
          }
          this.committed[i] = o.terms[k];
          this.committedIn[i] = Math.min(this.committedIn[i] ?? Infinity, o.term);
        }
        return;
      }
      case 'apply': {
        const id = `${o.term}/${o.cmdId}`;
        const known = this.applied[o.index - 1];
        if (known !== undefined && known !== id) {
          return this.fail(
            'state-machine-safety',
            o.node,
            `n${o.node} applied ${o.cmdId} (term ${o.term}) at #${o.index}, but another node applied ${known.split('/')[1]} (term ${known.split('/')[0]}) there`,
            { index: o.index },
          );
        }
        this.applied[o.index - 1] = id;
        return;
      }
      case 'role':
        return;
    }
  }

  private fail(kind: Violation['kind'], node: number, message: string, extra: { term?: number; index?: number } = {}): void {
    const { step, time } = this.clock();
    this.violation = { kind, message, step, time, node, ...extra };
  }
}
