/** Small, fast, fully deterministic PRNG (mulberry32) with stream forking. */
export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = mix32(seed >>> 0);
  }

  /** Uniform float in [0, 1). */
  next(): number {
    let t = (this.state = (this.state + 0x6d2b79f5) >>> 0);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** Uniform integer in [lo, hi). */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.next() * (hi - lo));
  }

  chance(p: number): boolean {
    return this.next() < p;
  }

  pick<T>(items: readonly T[]): T {
    return items[this.int(0, items.length)];
  }

  shuffle<T>(items: readonly T[]): T[] {
    const out = items.slice();
    for (let i = out.length - 1; i > 0; i--) {
      const j = this.int(0, i + 1);
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  }
}

/** splitmix-style 32-bit finalizer. */
export function mix32(x: number): number {
  x = (x + 0x9e3779b9) >>> 0;
  x = Math.imul(x ^ (x >>> 16), 0x85ebca6b) >>> 0;
  x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35) >>> 0;
  return (x ^ (x >>> 16)) >>> 0;
}

/** Derive an independent stream seed from a root seed and labels, so streams stay stable when unrelated ones change. */
export function deriveSeed(root: number, ...labels: number[]): number {
  let h = mix32(root);
  for (const l of labels) h = mix32(h ^ mix32(l + 0x1234567));
  return h;
}
