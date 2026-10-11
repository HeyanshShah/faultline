import { describe, expect, it } from 'vitest';
import {
  BUGS,
  type BugId,
  causalSlice,
  decodeRepro,
  encodeRepro,
  makeScenario,
  runScenario,
  shrink,
  tellStory,
} from '../src/index';

function firstFailure(bug: BugId, maxSeeds: number) {
  for (let seed = 1; seed <= maxSeeds; seed++) {
    const scenario = makeScenario(seed, { bugs: [bug] });
    const result = runScenario(scenario);
    if (result.violation) return { seed, scenario, result };
  }
  return null;
}

describe('determinism', () => {
  it('replays a scenario bit-for-bit', () => {
    const sc = makeScenario(1234);
    const a = runScenario(sc, { record: true });
    const b = runScenario(sc, { record: true });
    expect(b.stats).toEqual(a.stats);
    expect(b.history).toEqual(a.history);
    expect(b.trace!.steps.map((s) => s.label)).toEqual(a.trace!.steps.map((s) => s.label));
  });

  it('recording a trace does not perturb the execution', () => {
    const sc = makeScenario(99, { bugs: ['stale-read'] });
    expect(runScenario(sc, { record: true }).stats).toEqual(runScenario(sc).stats);
  });
});

describe('correct Raft', () => {
  it('survives 150 five-node and 100 three-node fault schedules with no violations', () => {
    for (let seed = 1; seed <= 150; seed++) {
      const r = runScenario(makeScenario(seed));
      expect(r.violation, `seed ${seed}`).toBeNull();
      expect(r.linearizability?.inconclusive).toBe(false);
    }
    for (let seed = 1; seed <= 100; seed++) {
      expect(runScenario(makeScenario(seed, { nodes: 3 })).violation, `3-node seed ${seed}`).toBeNull();
    }
  });

  it('makes progress: elects leaders and completes client operations', () => {
    const r = runScenario(makeScenario(5));
    expect(r.stats.elections).toBeGreaterThan(0);
    expect(r.stats.opsOk).toBeGreaterThan(50);
  });
});

describe('planted bugs', () => {
  const budget: Record<BugId, number> = { 'no-uptodate-check': 20, 'stale-read': 100, 'truncate-always': 100, 'forget-vote': 300 };
  const expected: Record<BugId, string[]> = {
    'no-uptodate-check': ['leader-completeness', 'state-machine-safety'],
    'stale-read': ['linearizability'],
    'truncate-always': ['leader-completeness', 'state-machine-safety'],
    'forget-vote': ['election-safety'],
  };
  for (const bug of BUGS) {
    it(`finds ${bug.id}`, () => {
      const found = firstFailure(bug.id, budget[bug.id]);
      expect(found, `no failure within ${budget[bug.id]} seeds`).not.toBeNull();
      expect(expected[bug.id]).toContain(found!.result.violation!.kind);
    });
  }
});

describe('shrinking and replay', () => {
  it('shrinks to a smaller scenario that still fails and replays from its repro string', () => {
    const found = firstFailure('no-uptodate-check', 20)!;
    const s = shrink(found.scenario, found.result);
    expect(s.result.violation).not.toBeNull();
    expect(s.after.faults).toBeLessThanOrEqual(s.before.faults);
    expect(s.after.steps).toBeLessThanOrEqual(s.before.steps);

    const replayed = runScenario(decodeRepro(encodeRepro(s.scenario)));
    expect(replayed.violation).toEqual(s.result.violation);
  });

  it('builds a causal slice and story around the violation', () => {
    const found = firstFailure('stale-read', 100)!;
    const r = runScenario(found.scenario, { record: true });
    const slice = causalSlice(r.trace!, r.violation!.step);
    expect(slice.has(r.violation!.step)).toBe(true);
    expect(slice.size).toBeLessThan(r.trace!.steps.length);
    const story = tellStory(r.trace!, r.violation!, r.linearizability);
    expect(story.explanation).toMatch(/read/i);
    expect(story.keySteps.has(r.violation!.step)).toBe(true);
  });
});

describe('regressions', () => {
  it('allows a leader of a stale term to lack entries committed in a later term (found by soak, seed 1373)', () => {
    // Raft's Leader Completeness only binds leaders of terms *after* the commit. An earlier checker
    // flagged this legal execution; the soak found it and the shrinker cut it from 40 faults to 17.
    const r = runScenario(decodeRepro('eyJ2IjoxLCJzZWVkIjoxMzczLCJub2RlcyI6NSwiY2xpZW50cyI6Mywia2V5cyI6MiwiZHVyYXRpb24iOjMxOTUsImNsaWVudFN0b3AiOjU1MDAsImJ1Z3MiOltdLCJuZXQiOnsibWluRGVsYXkiOjIsIm1heERlbGF5IjoxMiwiZHJvcFJhdGUiOjAuMDIsInNwaWtlUmF0ZSI6MC4wNCwic3Bpa2VNaW4iOjQwLCJzcGlrZU1heCI6MjUwfSwiZmF1bHRzIjpbeyJhdCI6MTMxLCJraW5kIjoicGFydGl0aW9uIiwiZ3JvdXBzIjpbWzBdLFsxLDIsMyw0XV19LHsiYXQiOjI4Miwia2luZCI6InBhcnRpdGlvbiIsImdyb3VwcyI6W1szXSxbMCwxLDIsNF1dfSx7ImF0Ijo2NjMsImtpbmQiOiJjcmFzaCIsIm5vZGUiOjJ9LHsiYXQiOjY4NCwia2luZCI6InJlc3RhcnQiLCJub2RlIjoyfSx7ImF0Ijo4MjMsImtpbmQiOiJoZWFsIn0seyJhdCI6MTExNSwia2luZCI6InBhcnRpdGlvbiIsImdyb3VwcyI6W1sxXSxbMCwyLDMsNF1dfSx7ImF0IjoxMzUwLCJraW5kIjoiaGVhbCJ9LHsiYXQiOjE2MDUsImtpbmQiOiJjcmFzaCIsIm5vZGUiOjF9LHsiYXQiOjE3ODgsImtpbmQiOiJyZXN0YXJ0Iiwibm9kZSI6MX0seyJhdCI6MjEwMSwia2luZCI6InBhcnRpdGlvbiIsImdyb3VwcyI6W1swLDNdLFsxLDIsNF1dfSx7ImF0IjoyMTc2LCJraW5kIjoiY3Jhc2giLCJub2RlIjowfSx7ImF0IjoyNTU0LCJraW5kIjoicGFydGl0aW9uIiwiZ3JvdXBzIjpbWzIsM10sWzAsNF1dLCJicmlkZ2UiOjF9LHsiYXQiOjI2MjYsImtpbmQiOiJjcmFzaCIsIm5vZGUiOjF9LHsiYXQiOjMwMjEsImtpbmQiOiJyZXN0YXJ0Iiwibm9kZSI6MX0seyJhdCI6MzA0MCwia2luZCI6ImhlYWwifSx7ImF0IjozMDYzLCJraW5kIjoicGFydGl0aW9uIiwiZ3JvdXBzIjpbWzEsMyw0XSxbMCwyXV19LHsiYXQiOjMxMjEsImtpbmQiOiJyZXN0YXJ0Iiwibm9kZSI6MH1dfQ'));
    expect(r.violation).toBeNull();
  });
});
