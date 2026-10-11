import { describe, expect, it } from 'vitest';
import { Heap } from '../src/heap';
import { Rng } from '../src/rng';
import { linearizableRegister, checkLinearizability } from '../src/linearizability';
import type { HistoryOp } from '../src/types';

describe('Rng', () => {
  it('is deterministic per seed and differs across seeds', () => {
    const a = new Rng(42);
    const b = new Rng(42);
    const c = new Rng(43);
    const xs = Array.from({ length: 5 }, () => a.next());
    expect(Array.from({ length: 5 }, () => b.next())).toEqual(xs);
    expect(Array.from({ length: 5 }, () => c.next())).not.toEqual(xs);
  });

  it('stays in range', () => {
    const r = new Rng(7);
    for (let i = 0; i < 10_000; i++) {
      const v = r.int(3, 9);
      expect(v).toBeGreaterThanOrEqual(3);
      expect(v).toBeLessThan(9);
    }
  });
});

describe('Heap', () => {
  it('pops in comparator order', () => {
    const h = new Heap<number>((a, b) => a < b);
    const r = new Rng(1);
    const xs = Array.from({ length: 500 }, () => r.int(0, 1000));
    xs.forEach((x) => h.push(x));
    const out: number[] = [];
    while (h.size) out.push(h.pop()!);
    expect(out).toEqual(xs.slice().sort((a, b) => a - b));
  });
});

let nextId = 0;
function op(kind: 'put' | 'get', value: string | null, callStep: number, retStep: number | null, client = 0): HistoryOp {
  return { id: nextId++, client, kind, key: 'x', value, callStep, retStep, callTime: callStep, retTime: retStep };
}

describe('linearizability checker', () => {
  it('accepts a sequential history', () => {
    expect(linearizableRegister([op('put', 'a', 1, 2), op('get', 'a', 3, 4), op('put', 'b', 5, 6), op('get', 'b', 7, 8)], Infinity)).toBe(true);
  });

  it('accepts concurrent ops in either order', () => {
    // put b overlaps both reads, so reading a then b is fine
    expect(linearizableRegister([op('put', 'a', 1, 2), op('put', 'b', 3, 10), op('get', 'a', 4, 5), op('get', 'b', 6, 7)], Infinity)).toBe(true);
  });

  it('rejects a stale read after an acknowledged write', () => {
    expect(linearizableRegister([op('put', 'a', 1, 2), op('put', 'b', 3, 4), op('get', 'a', 5, 6)], Infinity)).toBe(false);
  });

  it('rejects a read that flips back (new-old inversion)', () => {
    const h = [op('put', 'a', 1, 2), op('put', 'b', 3, 20), op('get', 'b', 4, 5), op('get', 'a', 6, 7)];
    expect(linearizableRegister(h, Infinity)).toBe(false);
  });

  it('lets a pending write take effect or not', () => {
    expect(linearizableRegister([op('put', 'a', 1, null), op('get', 'a', 2, 3)], Infinity)).toBe(true);
    expect(linearizableRegister([op('put', 'a', 1, null), op('get', null, 2, 3)], Infinity)).toBe(true);
    expect(linearizableRegister([op('put', 'a', 1, null), op('get', 'a', 2, 3), op('get', null, 4, 5)], Infinity)).toBe(false);
  });

  it('pinpoints the first operation that breaks it', () => {
    const stale = op('get', 'a', 5, 6);
    const r = checkLinearizability([op('put', 'a', 1, 2), op('put', 'b', 3, 4), stale, op('get', 'b', 7, 8)]);
    expect(r.ok).toBe(false);
    expect(r.culprit).toBe(stale);
  });
});
