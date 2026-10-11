import type { LinearizabilityResult } from './linearizability';
import type { Trace } from './sim';
import { procName } from './sim';
import type { HistoryOp, Violation } from './types';

export interface Story {
  /** Steps worth showing a human: faults, elections, the ops and messages the violation hinges on. */
  keySteps: Set<number>;
  /** One-paragraph plain-English account of why this is a bug. */
  explanation: string;
}

/** Picks out the handful of events that explain a violation and says what went wrong in words. */
export function tellStory(trace: Trace, v: Violation, lin: LinearizabilityResult | null): Story {
  const key = new Set<number>([0, v.step]);
  for (const s of trace.steps) {
    if (s.step > v.step) break;
    if (s.kind === 'fault') key.add(s.step);
    if (s.notes.some((n) => n.kind === 'leader' || n.kind === 'election')) key.add(s.step);
  }
  const n = trace.servers;
  let explanation = v.message + '.';

  if (v.kind === 'election-safety' && v.term !== undefined) {
    const voters = new Map<number, number[]>();
    for (const m of trace.msgs) {
      if (m.type !== 'RequestVoteReply' || m.fate !== 'delivered' || !m.label.startsWith('Vote GRANTED') || !m.label.endsWith(`t${v.term}`)) continue;
      key.add(m.deliverStep!);
      key.add(m.sendStep);
      if (!voters.has(m.from)) voters.set(m.from, []);
      voters.get(m.from)!.push(m.to);
    }
    const doubles = [...voters].filter(([, cands]) => new Set(cands).size > 1);
    explanation =
      `${v.message}. ` +
      (doubles.length
        ? doubles
            .map(([voter, cands]) => {
              const restarted = trace.steps.some((s) => s.step <= v.step && s.kind === 'fault' && s.proc === voter && s.label.startsWith('RESTART'));
              return `${procName(voter, n)} granted its term-${v.term} vote to ${[...new Set(cands)].map((c) => procName(c, n)).join(' and ')}${
                restarted ? ', having forgotten its first vote across a crash and restart' : ''
              }`;
            })
            .join('; ') + '. Each candidate counted a majority, so both became leader for the same term.'
        : 'Two candidates each assembled a majority in the same term.');
  }

  if ((v.kind === 'leader-completeness' || v.kind === 'state-machine-safety') && v.index !== undefined) {
    const commit = trace.steps.find((s) => s.notes.some((x) => x.kind === 'commit' && (x.index ?? 0) >= v.index!));
    if (commit) key.add(commit.step);
    explanation =
      `${v.message}. Entry #${v.index} had already been committed${commit ? ` (at ${commit.time}ms)` : ''}, ` +
      `so a client may have been told it succeeded. A leader without it will overwrite it on every follower: acknowledged data is lost.`;
  }

  if (v.kind === 'linearizability' && lin?.culprit && lin.window) {
    const read = lin.culprit;
    for (const step of [read.callStep, read.retStep]) if (step !== null) key.add(step);
    if (read.kind === 'get') {
      const source = lin.window.find((o) => o.kind === 'put' && o.value === read.value);
      const newer = lin.window.find(
        (o) =>
          o.kind === 'put' &&
          o !== source &&
          o.retStep !== null &&
          o.retStep < read.callStep &&
          (source === undefined || (source.retStep !== null && o.callStep > source.retStep)),
      );
      for (const op of [source, newer]) if (op) for (const step of [op.callStep, op.retStep]) if (step !== null) key.add(step);
      if (newer) {
        explanation =
          `Stale read on "${lin.key}". ${describe(newer, n)} was acknowledged at ${newer.retTime}ms, ` +
          `${source ? `after ${describe(source, n)} had finished, ` : ''}and then ${describe(read, n)} started at ${read.callTime}ms ` +
          `and still saw ${read.value === null ? 'nothing' : `"${read.value}"`}. No order of these operations that respects real time can explain that.`;
      } else {
        explanation = `${describe(read, n)} returned ${read.value === null ? 'nothing' : `"${read.value}"`}, which no legal ordering of the operations on "${lin.key}" can produce.`;
      }
    }
  }
  return { keySteps: key, explanation };
}

function describe(op: HistoryOp, servers: number): string {
  const who = procName(op.client, servers);
  return op.kind === 'put' ? `${who}'s write of "${op.value}"` : `${who}'s read`;
}
