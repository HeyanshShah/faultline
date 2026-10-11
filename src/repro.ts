import type { Scenario } from './types';

/** A repro is the whole scenario, so anyone can replay the exact execution from a string. */
export function encodeRepro(sc: Scenario): string {
  const json = JSON.stringify({ v: 1, ...sc });
  const bytes = new TextEncoder().encode(json);
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function decodeRepro(code: string): Scenario {
  const b64 = code.trim().replace(/-/g, '+').replace(/_/g, '/');
  const bin = atob(b64 + '='.repeat((4 - (b64.length % 4)) % 4));
  const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  const { v, ...sc } = JSON.parse(new TextDecoder().decode(bytes));
  if (v !== 1) throw new Error(`Unsupported repro version ${v}`);
  return sc as Scenario;
}
