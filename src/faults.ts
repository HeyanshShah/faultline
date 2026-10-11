import { deriveSeed, Rng } from './rng';
import { type BugId, DEFAULT_NET, type Fault, type Scenario } from './types';

/** Generates a nemesis schedule: crash/restart pairs and partition/heal pairs at random times. */
export function generateFaults(seed: number, nodes: number, duration: number): Fault[] {
  const rng = new Rng(deriveSeed(seed, 3));
  const faults: Fault[] = [];
  let t = rng.int(100, 400);
  while (t < duration - 200) {
    const r = rng.next();
    if (r < 0.25) {
      // Process bounce: crash and come straight back, as after an OOM kill or a deploy.
      const node = rng.int(0, nodes);
      faults.push({ at: t, kind: 'crash', node });
      faults.push({ at: t + rng.int(2, 40), kind: 'restart', node });
    } else if (r < 0.5) {
      const node = rng.int(0, nodes);
      faults.push({ at: t, kind: 'crash', node });
      faults.push({ at: t + rng.int(30, 1200), kind: 'restart', node });
    } else {
      const order = rng.shuffle(Array.from({ length: nodes }, (_, i) => i));
      if (rng.chance(0.3) && nodes >= 3) {
        const bridge = order.pop()!;
        const split = Math.max(1, Math.floor(order.length / 2));
        faults.push({ at: t, kind: 'partition', groups: [sorted(order.slice(0, split)), sorted(order.slice(split))], bridge });
      } else {
        const split = rng.chance(0.4) ? 1 : rng.int(1, nodes);
        faults.push({ at: t, kind: 'partition', groups: [sorted(order.slice(0, split)), sorted(order.slice(split))] });
      }
      faults.push({ at: t + rng.int(100, 1500), kind: 'heal' });
    }
    t += rng.int(60, 500);
  }
  return faults.sort((a, b) => a.at - b.at);
}

export interface ScenarioOptions {
  nodes?: number;
  clients?: number;
  keys?: number;
  duration?: number;
  bugs?: BugId[];
}

export function makeScenario(seed: number, o: ScenarioOptions = {}): Scenario {
  const nodes = o.nodes ?? 5;
  const duration = o.duration ?? 6000;
  return {
    seed,
    nodes,
    clients: o.clients ?? 3,
    keys: o.keys ?? 2,
    duration,
    clientStop: duration - 500,
    bugs: o.bugs ?? [],
    net: { ...DEFAULT_NET },
    faults: generateFaults(seed, nodes, duration),
  };
}

function sorted(xs: number[]): number[] {
  return xs.sort((a, b) => a - b);
}
