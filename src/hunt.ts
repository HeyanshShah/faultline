import { makeScenario, type ScenarioOptions } from './faults';
import { runScenario, type RunResult } from './sim';
import type { Scenario } from './types';

export interface HuntFound {
  seed: number;
  seedsTried: number;
  scenario: Scenario;
  result: RunResult;
}

/** Iterates seeds until one produces a violation. Yields between seeds so callers can report progress or pause. */
export function* hunt(
  startSeed: number,
  maxSeeds: number,
  opts: ScenarioOptions,
): Generator<{ seed: number; result: RunResult }, HuntFound | null> {
  for (let i = 0; i < maxSeeds; i++) {
    const seed = startSeed + i;
    const scenario = makeScenario(seed, opts);
    const result = runScenario(scenario);
    if (result.violation) return { seed, seedsTried: i + 1, scenario, result };
    yield { seed, result };
  }
  return null;
}
