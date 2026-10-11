import { encodeRepro, makeScenario, runScenario, shrink, type BugId } from '../src/index';

export type HuntRequest = { type: 'hunt'; bug: BugId | null; start: number; maxSeeds: number };
export type HuntEvent =
  | { type: 'progress'; seeds: number; steps: number; ops: number; elapsed: number }
  | { type: 'found'; seed: number; seeds: number; kind: string; message: string; faults: number; steps: number; elapsed: number }
  | { type: 'shrink'; phase: string; runs: number; faults: number; steps: number }
  | { type: 'done'; repro: string; runs: number; before: { faults: number; steps: number }; after: { faults: number; steps: number } }
  | { type: 'clean'; seeds: number; steps: number; ops: number; elapsed: number };

const post = (e: HuntEvent) => (self as unknown as Worker).postMessage(e);

self.onmessage = (ev: MessageEvent<HuntRequest>) => {
  const { bug, start, maxSeeds } = ev.data;
  const t0 = performance.now();
  let steps = 0;
  let ops = 0;
  let lastPost = 0;
  for (let i = 0; i < maxSeeds; i++) {
    const seed = start + i;
    const sc = makeScenario(seed, { bugs: bug ? [bug] : [] });
    const r = runScenario(sc);
    steps += r.stats.steps;
    ops += r.stats.opsOk;
    const now = performance.now();
    if (now - lastPost > 60) {
      lastPost = now;
      post({ type: 'progress', seeds: i + 1, steps, ops, elapsed: now - t0 });
    }
    if (!r.violation) continue;
    post({ type: 'found', seed, seeds: i + 1, kind: r.violation.kind, message: r.violation.message, faults: sc.faults.length, steps: r.stats.steps, elapsed: now - t0 });
    let lastShrinkPost = 0;
    const s = shrink(sc, r, (p) => {
      const t = performance.now();
      if (t - lastShrinkPost > 40) {
        lastShrinkPost = t;
        post({ type: 'shrink', ...p });
      }
    });
    post({ type: 'done', repro: encodeRepro(s.scenario), runs: s.runs, before: s.before, after: s.after });
    return;
  }
  post({ type: 'clean', seeds: maxSeeds, steps, ops, elapsed: performance.now() - t0 });
};
