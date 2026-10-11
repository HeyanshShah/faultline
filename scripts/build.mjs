import { build } from 'esbuild';
import { readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

mkdirSync('dist', { recursive: true });

await build({
  entryPoints: ['src/cli.ts'],
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node20',
  outfile: 'dist/cli.js',
  logLevel: 'warning',
});

// Precompute one shrunk example per planted bug so the web app opens on a real failure instantly.
await build({ entryPoints: ['src/index.ts'], bundle: true, platform: 'node', format: 'esm', outfile: 'dist/.core.mjs', logLevel: 'warning' });
const core = await import(pathToFileURL('dist/.core.mjs').href);
rmSync('dist/.core.mjs');
const demos = {};
for (const bug of core.BUGS) {
  for (let seed = 1; seed <= 5000; seed++) {
    const sc = core.makeScenario(seed, { bugs: [bug.id] });
    const r = core.runScenario(sc);
    if (!r.violation) continue;
    const s = core.shrink(sc, r);
    demos[bug.id] = { repro: core.encodeRepro(s.scenario), seed, before: s.before };
    console.log(`example ${bug.id}: seed ${seed}, faults ${s.before.faults} -> ${s.after.faults}, events ${s.before.steps} -> ${s.after.steps}`);
    break;
  }
}

const worker = await build({ entryPoints: ['ui/worker.ts'], bundle: true, format: 'iife', target: 'es2020', minify: true, write: false, logLevel: 'warning' });
const ui = await build({
  entryPoints: ['ui/main.ts'],
  bundle: true,
  format: 'iife',
  target: 'es2020',
  minify: true,
  write: false,
  logLevel: 'warning',
  define: { __WORKER_SRC__: JSON.stringify(worker.outputFiles[0].text), __DEMOS__: JSON.stringify(demos) },
});

// The web app is one self-contained file: no server, no build step for viewers, works offline.
const fragment = readFileSync('ui/index.html', 'utf8')
  .replace('/*STYLE*/', () => readFileSync('ui/style.css', 'utf8'))
  .replace('/*SCRIPT*/', () => ui.outputFiles[0].text.replace(/<\/script/gi, '<\\/script'));
writeFileSync('dist/artifact.html', fragment);
writeFileSync(
  'dist/faultline.html',
  `<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n</head>\n<body>\n${fragment}\n</body>\n</html>\n`,
);
console.log(`built dist/cli.js and dist/faultline.html (${(fragment.length / 1024).toFixed(0)} KB)`);
