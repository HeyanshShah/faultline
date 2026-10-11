#!/usr/bin/env node
import {
  BUGS,
  type BugId,
  causalSlice,
  tellStory,
  decodeRepro,
  encodeRepro,
  makeScenario,
  procName,
  runScenario,
  shrink,
  type Scenario,
} from './index';

const c = {
  dim: (s: string) => `\x1b[2m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
  red: (s: string) => `\x1b[31m${s}\x1b[0m`,
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
};

function flag(args: string[], name: string, fallback?: string): string | undefined {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
}

function usage(): void {
  console.log(`${c.bold('faultline')} — deterministic simulation testing for a Raft cluster

  faultline soak  [--seeds 2000] [--nodes 5]            fuzz the correct implementation
  faultline hunt  --bug <id> [--seeds 5000] [--start 1]  find, shrink and explain a planted bug
  faultline replay <repro> [--all]                       replay a repro string step by step
  faultline bugs                                         list planted bugs`);
}

function main(argv: string[]): number {
  const [cmd, ...args] = argv;
  switch (cmd) {
    case 'soak':
      return soak(Number(flag(args, 'seeds', '2000')), Number(flag(args, 'nodes', '5')));
    case 'hunt': {
      const bug = flag(args, 'bug') as BugId | undefined;
      if (!bug || !BUGS.some((b) => b.id === bug)) {
        console.error(`--bug must be one of: ${BUGS.map((b) => b.id).join(', ')}`);
        return 2;
      }
      return huntCmd(bug, Number(flag(args, 'seeds', '5000')), Number(flag(args, 'start', '1')), Number(flag(args, 'nodes', '5')));
    }
    case 'replay':
      if (!args[0]) return usage(), 2;
      return replay(decodeRepro(args[0]), args.includes('--all'));
    case 'bugs':
      for (const b of BUGS) console.log(`${c.bold(b.id.padEnd(18))} ${b.title}\n${' '.repeat(19)}${c.dim(b.description)}\n`);
      return 0;
    default:
      usage();
      return cmd ? 2 : 0;
  }
}

function soak(seeds: number, nodes: number): number {
  const t0 = performance.now();
  let steps = 0;
  let ops = 0;
  let elections = 0;
  for (let seed = 1; seed <= seeds; seed++) {
    const r = runScenario(makeScenario(seed, { nodes }));
    steps += r.stats.steps;
    ops += r.stats.opsOk;
    elections += r.stats.elections;
    if (r.violation) {
      console.log(c.red(`\nseed ${seed}: ${r.violation.kind}: ${r.violation.message}`));
      console.log(`repro: ${encodeRepro(r.scenario)}`);
      return 1;
    }
    if (seed % 100 === 0) process.stdout.write(`\r  ${seed}/${seeds} seeds clean`);
  }
  const secs = (performance.now() - t0) / 1000;
  console.log(
    `\r${c.green('✓')} ${seeds} seeds, ${steps.toLocaleString()} simulated events, ${elections.toLocaleString()} elections, ${ops.toLocaleString()} client ops checked for linearizability`,
  );
  console.log(c.dim(`  no violations · ${secs.toFixed(1)}s wall clock · ${Math.round(steps / secs).toLocaleString()} events/s`));
  return 0;
}

function huntCmd(bug: BugId, maxSeeds: number, start: number, nodes: number): number {
  const info = BUGS.find((b) => b.id === bug)!;
  console.log(`${c.bold('Hunting')} ${info.title} ${c.dim(`(${bug})`)} across up to ${maxSeeds} seeds\n`);
  const t0 = performance.now();
  for (let i = 0; i < maxSeeds; i++) {
    const seed = start + i;
    const sc = makeScenario(seed, { nodes, bugs: [bug] });
    const r = runScenario(sc);
    if (i % 25 === 0 && process.stdout.isTTY) process.stdout.write(`\r  seed ${seed}…`);
    if (!r.violation) continue;
    const secs = ((performance.now() - t0) / 1000).toFixed(2);
    console.log(`\r${c.red('✗')} seed ${seed} after ${i + 1} seeds (${secs}s): ${c.bold(r.violation.kind)}`);
    console.log(`  ${r.violation.message}\n`);
    const tty = process.stdout.isTTY;
    if (tty) process.stdout.write(c.dim('  shrinking…'));
    const s = shrink(sc, r, (p) => {
      if (tty) process.stdout.write(`\r${c.dim(`  shrinking (${p.phase}): ${p.runs} runs, ${p.faults} faults left`)}   `);
    });
    console.log(
      `\r  ${c.bold('shrunk')} in ${s.runs} runs: faults ${s.before.faults} → ${c.green(String(s.after.faults))}, events ${s.before.steps} → ${c.green(
        String(s.after.steps),
      )}, clients ${s.before.clients} → ${s.after.clients}                 \n`,
    );
    replay(s.scenario, false);
    return 1;
  }
  console.log(`\r${c.green('✓')} no violation in ${maxSeeds} seeds`);
  return 0;
}

function replay(sc: Scenario, all: boolean): number {
  const r = runScenario(sc, { record: true });
  const trace = r.trace!;
  if (!r.violation) {
    console.log(c.green('No violation reproduced.'));
    return 0;
  }
  const slice = causalSlice(trace, r.violation.step);
  const story = tellStory(trace, r.violation, r.linearizability);
  const shown = trace.steps.filter((s) => s.step <= r.violation!.step && (all || (slice.has(s.step) && story.keySteps.has(s.step))));
  console.log(
    c.bold(all ? 'Full execution' : 'Key moments') +
      c.dim(` — ${shown.length} shown · ${slice.size} of ${trace.steps.length} events are causally upstream of the violation`),
  );
  for (const st of shown) {
    const t = c.dim(`${String(st.time).padStart(5)}ms #${String(st.step).padEnd(5)}`);
    const label = st.kind === 'fault' ? c.yellow(st.label) : st.label;
    const mark = st.step === r.violation.step ? c.red(' ◀ violation') : '';
    console.log(`  ${t} ${label}${mark}`);
    for (const note of st.notes) if (all || note.kind !== 'commit') console.log(`  ${' '.repeat(15)}${c.cyan('↳ ' + note.text)}`);
  }
  console.log(`\n${c.red(c.bold(r.violation.kind))}: ${story.explanation}`);
  if (r.linearizability?.window) {
    console.log(c.dim(`\nhistory of key "${r.linearizability.key}" up to the failure:`));
    for (const op of r.linearizability.window) {
      const who = procName(op.client, sc.nodes);
      const what = op.kind === 'put' ? `put "${op.value}"` : op.retStep === null ? 'get → ?' : `get → ${op.value === null ? 'nil' : `"${op.value}"`}`;
      const span = `${op.callTime}ms..${op.retTime === null ? '?' : op.retTime + 'ms'}`;
      const mark = op === r.linearizability.culprit ? c.red('  ◀ impossible') : '';
      console.log(`  ${who} ${what.padEnd(18)} ${c.dim(span)}${mark}`);
    }
  }
  console.log(`\n${c.dim('repro:')} ${encodeRepro(sc)}`);
  return 1;
}

process.exitCode = main(process.argv.slice(2));
