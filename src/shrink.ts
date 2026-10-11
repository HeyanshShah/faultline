import { runScenario, type RunResult } from './sim';
import type { Scenario, Violation } from './types';

export interface ShrinkProgress {
  phase: string;
  runs: number;
  faults: number;
  steps: number;
}

export interface ShrinkResult {
  scenario: Scenario;
  result: RunResult;
  runs: number;
  before: { faults: number; steps: number; clients: number; duration: number };
  after: { faults: number; steps: number; clients: number; duration: number };
}

/**
 * Minimizes a failing scenario while it keeps failing with the same kind of violation:
 * trims the run to the violation, delta-debugs the fault schedule (Zeller's ddmin), removes clients,
 * and turns off random network chaos where possible.
 */
export function shrink(
  original: Scenario,
  failing: RunResult,
  onProgress?: (p: ShrinkProgress) => void,
): ShrinkResult {
  const kind = failing.violation!.kind;
  let best = clone(original);
  let bestResult = failing;
  let runs = 0;

  const attempt = (candidate: Scenario, phase: string): boolean => {
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
    const v = bestResult.violation!;
    const cut = { ...best, duration: v.time + 1, faults: best.faults.filter((f) => f.at <= v.time) };
    if (cut.duration < best.duration || cut.faults.length < best.faults.length) attempt(cut, 'trim');
  };

  trim();
  for (let round = 0; round < 3; round++) {
    const startFaults = best.faults.length;
    const startClients = best.clients;
    const startNet = JSON.stringify(best.net);

    best = ddmin(best, (faults) => attempt({ ...best, faults }, 'faults'));
    trim();

    while (best.clients > 1 && attempt({ ...best, clients: best.clients - 1 }, 'clients')) trim();
    if (best.net.dropRate > 0) attempt({ ...best, net: { ...best.net, dropRate: 0 } }, 'network');
    if (best.net.spikeRate > 0) attempt({ ...best, net: { ...best.net, spikeRate: 0 } }, 'network');
    trim();

    if (best.faults.length === startFaults && best.clients === startClients && JSON.stringify(best.net) === startNet) break;
  }

  const shape = (s: Scenario, r: RunResult) => ({
    faults: s.faults.length,
    steps: r.stats.steps,
    clients: s.clients,
    duration: s.duration,
  });
  return { scenario: best, result: bestResult, runs, before: shape(original, failing), after: shape(best, bestResult) };
}

function ddmin(sc: Scenario, test: (faults: Scenario['faults']) => boolean): Scenario {
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

function sameBug(v: Violation | null, kind: Violation['kind']): boolean {
  return v !== null && (v.kind === "linearizability") === (kind === "linearizability");
}

function clone(sc: Scenario): Scenario {
  return JSON.parse(JSON.stringify(sc));
}
