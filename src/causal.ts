import type { Trace } from './sim';

/** Every step that happens-before `target` (Lamport's relation), plus all faults before it. */
export function causalSlice(trace: Trace, target: number): Set<number> {
  const byStep = new Map(trace.steps.map((s) => [s.step, s]));
  const keep = new Set<number>();
  const stack = [target];
  while (stack.length) {
    const s = stack.pop()!;
    if (keep.has(s)) continue;
    keep.add(s);
    for (const p of byStep.get(s)?.parents ?? []) if (!keep.has(p)) stack.push(p);
  }
  for (const s of trace.steps) if (s.kind === 'fault' && s.step <= target) keep.add(s.step);
  keep.add(0);
  return keep;
}
