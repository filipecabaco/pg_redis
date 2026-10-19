/**
 * Redis's own acceptance suite, run against pg_redis on both halves.
 *
 * Redis gates its releases on the Tcl suite under tests/unit. It can drive an
 * external server (`--host`/`--port`), which is all pg_redis is to it. Two
 * passes, one per storage half:
 *
 *   durable — the suite's default, `SELECT 9`: the SQL tables
 *   memory  — `--singledb`: the server's default database, which must be 0
 *             — the shared-memory tables — so run the server with
 *             redis.default_db=0, as CI does
 *
 * Each unit runs in its own process, so a unit that aborts takes only itself
 * down. Results are ratcheted against baseline.json, which records the tests
 * that pass today: the gate fails when one of them stops passing, and lists
 * the ones that started passing so the baseline can be moved forward.
 *
 *   bun run acceptance/run.ts                    # gate
 *   bun run acceptance/run.ts --update-baseline  # record what passes today
 *   bun run acceptance/run.ts --unit type/hash   # one unit, both halves
 */

const HOST = process.env.PG_REDIS_HOST ?? "localhost";
const PORT = process.env.PG_REDIS_PORT ?? "6379";
const UNIT_TIMEOUT_MS = Number(process.env.UNIT_TIMEOUT_MS ?? 900_000);
const HERE = new URL(".", import.meta.url).pathname;
const BASELINE = `${HERE}baseline.json`;
const SKIPFILE = `${HERE}skip.txt`;

/**
 * Every unit a server with Redis's full command set should pass when driven
 * from outside. Left out: the units Redis itself tags external:skip at the
 * top — they reconfigure, restart or replicate the server under test (acl,
 * aofrw, auth, client-eviction, limits, obuf-limits, pubsubshard, shutdown,
 * wait) — and the ones that measure Redis's own allocator or internals
 * rather than behaviour (memefficiency, oom-score-adj, querybuf,
 * replybufsize, violations, tls). maxmemory is the exception: it runs, in
 * part, through `REWRITTEN` below.
 */
const UNITS = [
	"unit/type/string",
	"unit/type/incr",
	"unit/type/hash",
	"unit/type/list",
	"unit/type/list-2",
	"unit/type/list-3",
	"unit/type/set",
	"unit/type/zset",
	"unit/type/stream",
	"unit/type/stream-cgroups",
	"unit/keyspace",
	"unit/expire",
	"unit/multi",
	"unit/pubsub",
	"unit/scan",
	"unit/protocol",
	"unit/info",
	"unit/info-command",
	"unit/introspection",
	"unit/introspection-2",
	"unit/bitops",
	"unit/bitfield",
	"unit/hyperloglog",
	"unit/geo",
	"unit/sort",
	"unit/dump",
	"unit/scripting",
	"unit/functions",
	"unit/slowlog",
	"unit/latency-monitor",
	"unit/tracking",
	"unit/pause",
	"unit/lazyfree",
	"unit/networking",
	"unit/other",
	"unit/auth",
	"unit/quit",
	"unit/maxmemory",
];

/**
 * Units Redis tags `external:skip` throughout that pg_redis can still run
 * most of. Each is copied with the tag removed and with the top-level blocks
 * that cannot run against an external server left out: the ones that start
 * a replica, read the replication stream, or measure client output buffers.
 * A block left in that fails is a failure in the report like any other.
 * Only on the halves listed: maxmemory applies to memory databases alone.
 */
const REWRITTEN: Record<string, { halves: Half[]; drop: RegExp[] }> = {
	"unit/maxmemory": {
		halves: ["memory"],
		drop: [/test_slave_buffers/, /attach_to_replication_stream/, /proc init_test/],
	},
};

/**
 * Tags whose tests cannot pass against any external server that is not Redis:
 * they need DEBUG, replication, or SAVE/BGSAVE to set up the case at all.
 * Denying them keeps the report about the command under test.
 */
const DENY_TAGS = ["needs:debug", "needs:repl", "needs:save", "needs:reset"];

/**
 * `needs:config-maxmemory` tests set a memory limit and expect eviction or
 * OOM from it, which only the memory half has: a logged database is bounded
 * by disk. Denied there, run on the memory half.
 */
const denyTags = (half: Half) =>
	half === "durable" ? [...DENY_TAGS, "needs:config-maxmemory"] : DENY_TAGS;

/**
 * The unit as the suite is to run it: the file itself, or the rewritten copy
 * `REWRITTEN` describes, written under tests/unit/pg_redis/.
 */
async function unitPath(unit: string): Promise<string> {
	const rw = REWRITTEN[unit];
	if (!rw) return unit;
	const source = await Bun.file(`tests/${unit}.tcl`).text();
	const chunks: string[][] = [];
	for (const line of source.split("\n")) {
		// A top-level block starts at the left margin; its closing brace and
		// the lines inside it do not.
		if (chunks.length === 0 || (/^\S/.test(line) && !/^}/.test(line))) chunks.push([]);
		chunks[chunks.length - 1].push(line);
	}
	const kept = chunks
		.map((c) => c.join("\n"))
		.filter((c) => !rw.drop.some((re) => re.test(c)))
		.join("\n")
		.replaceAll(/\s*"?external:skip"?/g, "");
	const name = `unit/pg_redis/${unit.replace(/^unit\//, "")}`;
	await Bun.write(`tests/${name}.tcl`, kept);
	return name;
}

const HALVES = {
	durable: [] as string[],
	memory: ["--singledb"],
} as const;
type Half = keyof typeof HALVES;

type Outcome = "ok" | "err" | "skip";
type UnitResult = {
	tests: Map<string, Outcome>;
	aborted: string | null;
};

const argv = process.argv.slice(2);
const updating = argv.includes("--update-baseline");
const onlyUnits = argv.flatMap((a, i) =>
	a === "--unit" ? [`unit/${argv[i + 1].replace(/^unit\//, "")}`] : [],
);
const units = onlyUnits.length > 0 ? onlyUnits : UNITS;

// biome-ignore lint/suspicious/noControlCharactersInRegex: ANSI colour codes
const ANSI = /\x1b\[[0-9;]*m/g;

/**
 * Every Redis test file starts a server of its own, so a CONFIG SET never
 * outlives the file that made it. Here one server serves every unit, and a
 * file that fails before restoring what it changed — introspection leaves
 * `maxmemory` at 10 MB when its rollback test fails — would cap every unit
 * after it. Put back what pg_redis acts on, as a fresh server would have it.
 */
async function resetConfig(): Promise<void> {
	const cmd = [
		"CONFIG",
		"SET",
		"maxmemory",
		"0",
		"maxmemory-policy",
		"noeviction",
		// The encoding thresholds, which the hash, set and zset files change
		// for a test and leave for the next file's fresh server to forget.
		"hash-max-listpack-entries",
		"512",
		"hash-max-listpack-value",
		"64",
		"set-max-intset-entries",
		"512",
		"set-max-listpack-entries",
		"128",
		"set-max-listpack-value",
		"64",
		"zset-max-listpack-entries",
		"128",
		"zset-max-listpack-value",
		"64",
		"list-max-listpack-size",
		"-2",
	];
	const wire = `*${cmd.length}\r\n${cmd.map((a) => `$${a.length}\r\n${a}\r\n`).join("")}`;
	await new Promise<void>((resolve) => {
		Bun.connect({
			hostname: HOST,
			port: Number(PORT),
			socket: {
				open: (s) => void s.write(wire),
				data: (s) => {
					s.end();
					resolve();
				},
				close: () => resolve(),
				error: () => resolve(),
				connectError: () => resolve(),
			},
		});
	});
}

async function runUnit(half: Half, unit: string): Promise<UnitResult> {
	await resetConfig();
	const args = [
		"tclsh",
		"tests/test_helper.tcl",
		"--host",
		HOST,
		"--port",
		PORT,
		"--single",
		await unitPath(unit),
		"--clients",
		"1",
		"--durable",
		"--ignore-encoding",
		"--ignore-digest",
		"--no-latency",
		// Seconds per test. The durable half's write-heavy tests (thousands of
		// PFADDs, each committed to the WAL) ran past 120 on CI's shared disks
		// while still passing; the limit catches a hang, not slowness.
		"--timeout",
		"300",
		"--skipfile",
		SKIPFILE,
		"--tags",
		denyTags(half)
			.map((t) => `-${t}`)
			.join(" "),
		...HALVES[half],
	];
	const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
	// The suite forks its test client with `exec ... &`, and that child holds
	// the pipe open after the parent dies: killing `proc` alone never ends the
	// read below. Take down every process of the run.
	let timedOut = false;
	const timer = setTimeout(() => {
		timedOut = true;
		Bun.spawnSync(["pkill", "-9", "-f", "tests/test_helper.tcl"]);
	}, UNIT_TIMEOUT_MS);
	const [out, err] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
	]);
	await proc.exited;
	clearTimeout(timer);

	const tests = new Map<string, Outcome>();
	let aborted: string | null = null;
	const lines = `${out}\n${err}`.replace(ANSI, "").split("\n");
	for (const [i, line] of lines.entries()) {
		let m = line.match(/^\[ok\]: (.*) \(\d+ ms\)$/);
		if (m) {
			// A name that appears twice keeps its worst outcome.
			if (!tests.has(m[1])) tests.set(m[1], "ok");
			continue;
		}
		m = line.match(/^\[err\]: (.*?)(?: in tests\/\S+)?$/);
		if (m) {
			tests.set(m[1], "err");
			if (process.env.VERBOSE) console.log(`    ${line}\n      ${lines[i + 1]}`);
			continue;
		}
		m = line.match(/^\[(?:skip|ignore)\]: (.*)$/);
		if (m) {
			if (!tests.has(m[1])) tests.set(m[1], "skip");
			continue;
		}
		if (/^\[(exception|TIMEOUT)\]/.test(line) || /^Killing still running/.test(line)) {
			aborted ??= [line, ...lines.slice(i + 1, i + 6)].join("\n      ");
		}
	}
	if (timedOut) aborted ??= `killed after ${UNIT_TIMEOUT_MS} ms`;
	return { tests, aborted };
}

type Baseline = Record<Half, Record<string, string[]>>;
const id = (half: Half, unit: string, test: string) => `${half} | ${unit} | ${test}`;

const results = new Map<string, UnitResult>();
const passing = new Set<string>();
const summary: string[] = [];

for (const half of Object.keys(HALVES) as Half[]) {
	console.log(`\n══ ${half} half ══════════════════════════════════════`);
	for (const unit of units) {
		if (REWRITTEN[unit] && !REWRITTEN[unit].halves.includes(half)) continue;
		const r = await runUnit(half, unit);
		results.set(`${half} | ${unit}`, r);
		const counts = { ok: 0, err: 0, skip: 0 };
		for (const [test, outcome] of r.tests) {
			counts[outcome]++;
			if (outcome === "ok") passing.add(id(half, unit, test));
		}
		const ran = counts.ok + counts.err;
		const line =
			`${unit.padEnd(24)} ${String(counts.ok).padStart(4)}/${String(ran).padEnd(4)} pass` +
			`  (${counts.skip} skipped)${r.aborted ? "  ABORTED" : ""}`;
		console.log(`  ${line}`);
		if (r.aborted) console.log(`      ${r.aborted}`);
		summary.push(`${half.padEnd(8)} ${line}`);
	}
}

const total = [...results.values()].reduce(
	(acc, r) => {
		for (const o of r.tests.values()) if (o !== "skip") acc.ran++;
		return acc;
	},
	{ ran: 0 },
);
console.log(`\n${passing.size}/${total.ran} Redis acceptance tests pass across both halves`);

if (updating) {
	const baseline: Baseline = { durable: {}, memory: {} };
	for (const key of [...passing].sort()) {
		const [half, unit, test] = key.split(" | ") as [Half, string, string];
		if (!baseline[half][unit]) baseline[half][unit] = [];
		baseline[half][unit].push(test);
	}
	// A partial run (--unit) only re-records the units it ran.
	if (onlyUnits.length > 0) {
		const previous: Baseline = await Bun.file(BASELINE).json();
		for (const half of Object.keys(HALVES) as Half[]) {
			for (const unit of units) {
				if (REWRITTEN[unit] && !REWRITTEN[unit].halves.includes(half)) continue;
				previous[half][unit] = baseline[half][unit] ?? [];
			}
		}
		Object.assign(baseline, previous);
	}
	await Bun.write(BASELINE, `${JSON.stringify(baseline, null, "\t")}\n`);
	console.log(`recorded ${passing.size} passing tests in ${BASELINE}`);
	process.exit(0);
}

const expected = new Set<string>();
try {
	const baseline: Baseline = await Bun.file(BASELINE).json();
	for (const half of Object.keys(HALVES) as Half[]) {
		for (const [unit, tests] of Object.entries(baseline[half] ?? {})) {
			if (!units.includes(unit)) continue;
			for (const test of tests) expected.add(id(half, unit, test));
		}
	}
} catch {
	console.error(`no baseline at ${BASELINE}; run with --update-baseline`);
}

const regressed = [...expected].filter((k) => !passing.has(k)).sort();
const improved = [...passing].filter((k) => !expected.has(k)).sort();

if (regressed.length > 0) {
	console.log(`\nREGRESSED — passed in the baseline, not now (${regressed.length}):`);
	for (const k of regressed) {
		const [half, unit, test] = k.split(" | ");
		const outcome = results.get(`${half} | ${unit}`)?.tests.get(test) ?? "did not run";
		console.log(`  ${k}   (${outcome})`);
	}
}
if (improved.length > 0) {
	console.log(`\nNewly passing — run with --update-baseline (${improved.length}):`);
	for (const k of improved) console.log(`  ${k}`);
}
process.exit(regressed.length > 0 ? 1 : 0);
