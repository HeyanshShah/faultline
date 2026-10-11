import type { HistoryOp } from './types';

export interface LinearizabilityResult {
  ok: boolean;
  /** True if the search budget ran out before an answer; treated as a pass. */
  inconclusive: boolean;
  key?: string;
  /** The operation whose completion first made the history non-linearizable. */
  culprit?: HistoryOp;
  /** Operations on the failing key up to the culprit, for display. */
  window?: HistoryOp[];
}

class BudgetExceeded extends Error {}

/**
 * Wing & Gong linearizability search with Lowe's memoization over (linearized set, register state),
 * checked per key (linearizability is compositional, so a multi-key register store is checked one key at a time).
 */
export function checkLinearizability(history: readonly HistoryOp[], budget = 2_000_000): LinearizabilityResult {
  const byKey = new Map<string, HistoryOp[]>();
  for (const op of history) {
    if (!byKey.has(op.key)) byKey.set(op.key, []);
    byKey.get(op.key)!.push(op);
  }
  let inconclusive = false;
  for (const key of [...byKey.keys()].sort()) {
    const ops = byKey.get(key)!;
    let verdict: boolean;
    try {
      verdict = linearizableRegister(ops, Infinity, budget);
    } catch (e) {
      if (!(e instanceof BudgetExceeded)) throw e;
      inconclusive = true;
      continue;
    }
    if (verdict) continue;
    // Linearizability is prefix-closed, so binary-search the earliest return that breaks it.
    const rets = ops.filter((o) => o.retStep !== null).map((o) => o.retStep!).sort((a, b) => a - b);
    let lo = 0;
    let hi = rets.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      let ok: boolean;
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

/** Is the single-register history, observed up to (not including) step `cut`, linearizable? */
export function linearizableRegister(all: readonly HistoryOp[], cut: number, budget = 2_000_000): boolean {
  // Ops invoked before the cut; ops returning at/after it are still pending. Pending reads have no effect.
  const ops = all
    .filter((o) => o.callStep < cut)
    .map((o) => ({ op: o, ret: o.retStep !== null && o.retStep < cut ? o.retStep : Infinity }))
    .filter((o) => !(o.op.kind === 'get' && o.ret === Infinity))
    .sort((a, b) => a.op.callStep - b.op.callStep);
  const n = ops.length;
  const done = new Uint8Array(n);
  const words = new Int32Array(Math.ceil(n / 32) || 1);
  let remaining = ops.filter((o) => o.ret !== Infinity).length;
  const seen = new Set<string>();
  let work = 0;

  const search = (value: string | null): boolean => {
    if (remaining === 0) return true;
    if (++work > budget) throw new BudgetExceeded();
    let minRet = Infinity;
    for (let i = 0; i < n; i++) if (!done[i] && ops[i].ret < minRet) minRet = ops[i].ret;
    for (let i = 0; i < n; i++) {
      if (done[i]) continue;
      const { op, ret } = ops[i];
      if (op.callStep >= minRet) break;
      let next: string | null;
      if (op.kind === 'get') {
        if (op.value !== value) continue;
        next = value;
      } else {
        next = op.value;
      }
      done[i] = 1;
      words[i >> 5] ^= 1 << (i & 31);
      const memoKey = `${words.join(',')}|${next}`;
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
