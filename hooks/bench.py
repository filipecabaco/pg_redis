"""Throughput figures for the site, from the report of the last benchmark run.

`mise run bench-site` runs docker/bench/suite.exs on the benchmark rig, every
server pinned to the same CPUs, and writes its report to docs/bench.json,
which is committed. This puts it in front of the pages: the home page's bars
read `config.extra.bench`, and a `<!-- bench:... -->` marker in a page becomes
a table, so no figure on the site is typed in. Without a report, the pages say
there is none rather than showing figures nobody measured.
"""

import json
import os
from datetime import datetime

REPORT = "bench.json"

# The bars on the home page: a few commands that between them write every type.
HOME = ["set", "get", "incr", "lpush", "sadd", "hset", "zadd"]

# The suite's order (docker/bench/suite.exs): a report's rows are a JSON object,
# which keeps none.
ORDER = ["set", "get", "incr", "lpush", "lpop", "rpush", "rpop", "sadd", "spop",
         "hset", "zadd", "zpopmin", "lrange_100", "mset"]

NONE = (
    '!!! note "Not measured yet"\n'
    "    These figures come from `mise run bench-site`, which writes\n"
    "    docs/bench.json; this build found none.\n"
)


def _load(config):
    path = os.path.join(config["docs_dir"], REPORT)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        report = json.load(f)
    rps = {}
    for result in report["results"]:
        for name, row in result["rows"].items():
            if row.get("rps") is not None:
                rps[(result["group"], name, result["target"])] = row["rps"]
    return {"report": report, "rps": rps}


def _k(value):
    """Thousands per second, as the bars label them: 6.4, 121."""
    k = value / 1000
    return round(k, 1) if k < 10 else round(k)


def _n(value):
    return "n/a" if value is None else f"{round(value):,}"


def _meta(report):
    meta = report["meta"]
    when = datetime.fromisoformat(meta["started_at"].replace("Z", "+00:00"))
    return {
        "host": meta["host_cpu"],
        "date": when.strftime("%-d %B %Y"),
        "commit": meta["pg_redis_commit"],
        "postgres": meta["postgres"].split()[0],
        "reps": meta["reps"],
        "server_cpus": meta["server_cpus"],
        "load_cpus": meta["load_cpus"],
    }


def on_config(config):
    data = _load(config)
    if data is None:
        config.extra["bench"] = None
        return config
    rps = data["rps"]
    rows = []
    for cmd in HOME:
        values = [rps.get(("commands", cmd, t)) for t in ("redis", "memory", "logged")]
        # A report from before the everysec target has none: its bar is left out.
        everysec = rps.get(("commands", cmd, "logged-everysec"))
        if None not in values:
            rows.append((cmd.upper(), *(_k(v) for v in values), everysec and _k(everysec)))
    top = max((v for r in rows for v in r[1:] if v is not None), default=1)
    config.extra["bench"] = {"rows": rows, "top": top, **_meta(data["report"])}
    config.extra["bench_data"] = data
    return config


def _commands(data):
    report, rps = data["report"], data["rps"]
    targets = report["targets"]
    head = "| Command | " + " | ".join(t["label"] for t in targets) + " |"
    rule = "|---|" + "---:|" * len(targets)
    names = [n for n in ORDER if any(k[:2] == ("commands", n) for k in rps)]
    for result in report["results"]:
        if result["group"] == "commands":
            names += sorted(n for n in result["rows"] if n not in names)
    lines = [head, rule]
    for name in names:
        cells = [_n(rps.get(("commands", name, t["id"]))) for t in targets]
        lines.append(f"| {name.upper()} | " + " | ".join(cells) + " |")
    for result in report["results"]:
        if result["group"] == "mixed":
            for name in result["rows"]:
                if not any(name in line for line in lines):
                    cells = [_n(rps.get(("mixed", name, t["id"]))) for t in targets]
                    lines.append(f"| {name} | " + " | ".join(cells) + " |")
    m = _meta(report)
    note = (
        f"Measured on {m['date']}: pg_redis `{m['commit']}` on PostgreSQL {m['postgres']}, "
        f"{m['host']}, the server under test {m['server_cpus']}, the load generator "
        f"{m['load_cpus']}; median of {m['reps']} run{'' if m['reps'] == 1 else 's'}. "
        f"The whole report is [bench.json](bench.json)."
    )
    return "\n".join(lines) + "\n\n" + note + "\n"


def on_page_markdown(markdown, page, config, files):
    if "<!-- bench:" not in markdown:
        return markdown
    data = config.extra.get("bench_data")
    return markdown.replace("<!-- bench:commands -->", _commands(data) if data else NONE)
