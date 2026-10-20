// Runs every showcase against pg_redis, one after another, and exits non-zero
// if any check failed. `node run.mjs job-queue` runs one.

import { failures } from "./lib.mjs";

const all = ["job-queue", "web-sessions", "rate-limit-realtime", "order-pipeline"];
const only = process.argv.slice(2);

for (const name of only.length ? only : all) {
	console.log(`\n▶ ${name}`);
	const started = Date.now();
	const { run } = await import(`./${name}/app.mjs`);
	await run();
	console.log(`  ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

const n = failures();
console.log(n === 0 ? "\nevery showcase passed" : `\n${n} check(s) failed`);
process.exit(n === 0 ? 0 : 1);
