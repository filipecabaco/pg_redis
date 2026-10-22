# Benchmark pg_redis's four storage policies against Redis, like for like.
#
#   mise run bench-suite            # or: elixir docker/bench/suite.exs
#
# Each server runs alone on the same four pinned CPUs (docker/bench/compose.yml), the load
# generator on CPUs of its own. Every measurement is repeated and the median kept; every run
# is recorded. Results go to docker/bench/results/<timestamp>.{json,md}.
#
# Targets: Redis 7.2 with persistence off, Redis 7.2 with AOF everysec, and pg_redis databases
# 0, 1 and 8 with the policies memory, memory+snapshot and logged.
#
# Environment:
#   BENCH_REPS    repetitions per measurement (default 3)
#   BENCH_ONLY    comma-separated groups: commands,sizes,clients,pipeline,mixed (default all)
#   BENCH_SCALE   multiply every request count, e.g. 0.05 for a smoke run (default 1)
#   BENCH_KEEP    leave the rig running afterwards when set
#   BENCH_TARGETS comma-separated target ids to run (default all): redis, redis-aof, memory,
#                 memory+snapshot, logged
#   BENCH_OUT     where to write the report, without extension (default
#                 docker/bench/results/<timestamp>); CI writes the one the site reads
#   BENCH_HOST    what to call the machine in the report (default: its CPU)
#   BENCH_SERVER_CPUS, BENCH_LOAD_CPUS  the CPU pins (docker/bench/compose.yml)
#   BENCH_IMAGE   the pg_redis image to measure (default pg_redis:latest)
#   BENCH_COMMIT  the commit that image was built from, for the report (default HEAD)

defmodule Rig do
  @compose ["compose", "-f", "docker/bench/compose.yml", "-p", "pgrbench"]
  @servers ["pg_redis", "redis", "redis-aof"]

  def compose(args, opts \\ []) do
    {out, status} = System.cmd("docker", @compose ++ args, stderr_to_stdout: true)
    if status != 0 and Keyword.get(opts, :check, true) do
      raise "docker compose #{Enum.join(args, " ")} failed (#{status}):\n#{out}"
    end
    out
  end

  def exec(service, cmd, opts \\ []), do: compose(["exec", "-T", service | cmd], opts)

  def up_all do
    compose(["up", "-d", "--wait" | ["load", "memtier" | @servers]])
  end

  # Only the server under test runs, so an idle one never shares its CPUs
  def only(server) do
    others = @servers -- [server]
    compose(["stop" | others])
    compose(["start", server])
    compose(["up", "-d", "--wait", server])
  end

  def down, do: compose(["down", "-v"], check: false)

  def psql(sql), do: exec("pg_redis", ["psql", "-U", "postgres", "-qAtX", "-c", sql]) |> String.trim()

  def cli(t, args) do
    exec("load", ["redis-cli", "-h", t.host, "-a", "bench", "--no-auth-warning", "-n", "#{t.db}" | args])
  end
end

defmodule Bench do
  @scale System.get_env("BENCH_SCALE", "1") |> Float.parse() |> elem(0)
  @reps String.to_integer(System.get_env("BENCH_REPS", "3"))

  def reps, do: @reps
  def n(count), do: max(round(count * @scale), 200)

  def median(values) do
    sorted = Enum.sort(values)
    len = length(sorted)
    if rem(len, 2) == 1, do: Enum.at(sorted, div(len, 2)),
      else: (Enum.at(sorted, div(len, 2) - 1) + Enum.at(sorted, div(len, 2))) / 2
  end

  # One redis-benchmark run: rps and latency percentiles from its CSV line
  def redis_benchmark(t, test, opts) do
    args =
      ["redis-benchmark", "-h", t.host, "-a", "bench", "--dbnum", "#{t.db}", "--csv",
       "-t", test, "-n", "#{opts[:n]}", "-c", "#{opts[:c] || 50}", "-d", "#{opts[:d] || 64}",
       "-r", "#{opts[:r] || 100_000}", "-P", "#{opts[:p] || 1}"]

    out = Rig.exec("load", args, check: false)

    line =
      out
      |> String.split(["\n", "\r"], trim: true)
      |> Enum.find(fn l -> String.starts_with?(l, "\"") and not String.starts_with?(l, "\"test\"") end)

    case line && String.split(line, ",") |> Enum.map(&String.trim(&1, "\"")) do
      [_name, rps, _avg, _min, p50, _p95, p99 | _] ->
        %{rps: parse(rps), p50_ms: parse(p50), p99_ms: parse(p99)}

      _ ->
        %{error: String.slice(out, 0, 300)}
    end
  end

  # memtier_benchmark: populate, then a timed read-heavy run
  def memtier(t, opts) do
    base = ["memtier_benchmark", "-s", t.host, "-p", "6379", "-a", "bench", "--select-db=#{t.db}",
            "--protocol=redis", "--hide-histogram", "--key-maximum=#{opts[:keys]}",
            "--data-size=#{opts[:size]}", "--threads=4", "--clients=25"]

    Rig.exec("memtier", base ++ ["--ratio=1:0", "--key-pattern=P:P", "--requests=allkeys"], check: false)

    out = Rig.exec("memtier", base ++ ["--ratio=1:10", "--key-pattern=R:R",
                     "--test-time=#{opts[:seconds]}", "--json-out-file=/tmp/m.json"], check: false)
    json = Rig.exec("memtier", ["cat", "/tmp/m.json"], check: false)

    with {:ok, data} <- JSON.decode(json),
         %{"Totals" => totals} <- get_in(data, ["ALL STATS"]) do
      pct = totals["Percentile Latencies"] || %{}
      %{rps: totals["Ops/sec"], p50_ms: pct["p50.00"], p99_ms: pct["p99.00"]}
    else
      _ -> %{error: String.slice(out, -300, 300)}
    end
  end

  def repeat(fun) do
    runs = for _ <- 1..@reps, do: fun.()
    good = Enum.reject(runs, &Map.has_key?(&1, :error))

    if good == [] do
      %{runs: runs, error: hd(runs)[:error]}
    else
      %{runs: runs,
        rps: median(Enum.map(good, & &1.rps)),
        p50_ms: median(Enum.map(good, & &1.p50_ms)),
        p99_ms: median(Enum.map(good, & &1.p99_ms))}
    end
  end

  defp parse(s) do
    case Float.parse(s) do
      {f, _} -> f
      :error -> nil
    end
  end
end

# ── Targets ──────────────────────────────────────────────────────────────────

targets = [
  %{id: "redis", label: "Redis 7.2", host: "redis", db: 0, server: "redis", fast: true},
  %{id: "redis-aof", label: "Redis 7.2, AOF everysec", host: "redis-aof", db: 0, server: "redis-aof", fast: true},
  %{id: "memory", label: "pg_redis memory", host: "pg_redis", db: 0, server: "pg_redis", fast: true},
  %{id: "memory+snapshot", label: "pg_redis memory+snapshot", host: "pg_redis", db: 1, server: "pg_redis", fast: true},
  %{id: "logged", label: "pg_redis logged", host: "pg_redis", db: 8, server: "pg_redis", fast: false},
  # The same logged database with replies that do not wait for the WAL to reach disk:
  # Redis's appendfsync everysec, which the Redis column beside it uses too.
  %{id: "logged-everysec", label: "pg_redis logged, appendfsync everysec", host: "pg_redis", db: 9,
    server: "pg_redis", fast: false, fsync: "everysec"}
]

targets =
  case System.get_env("BENCH_TARGETS") do
    nil -> targets
    ids -> Enum.filter(targets, &(&1.id in String.split(ids, ",", trim: true)))
  end

groups =
  case System.get_env("BENCH_ONLY") do
    nil -> ~w(commands sizes clients pipeline mixed)
    s -> String.split(s, ",", trim: true)
  end

# Pops follow their producers so they have something to pop; everything else starts empty
commands = ~w(set get incr lpush lpop rpush rpop sadd spop hset zadd zpopmin lrange_100 mset)
follows = ~w(lpop rpop spop zpopmin get)

log = fn msg -> IO.puts(:stderr, "[#{Time.utc_now() |> Time.truncate(:second)}] #{msg}") end

# ── Run ──────────────────────────────────────────────────────────────────────

log.("starting the rig")
Rig.down()
Rig.up_all()

policies = "SELECT redis.set_policy(1, 'memory+snapshot')"
Rig.psql(policies)
policy_table = Rig.psql("SELECT string_agg(db || '=' || policy, ', ' ORDER BY db) FROM redis.policies() WHERE db IN (0, 1, 8)")
log.("pg_redis policies: #{policy_table}")

meta = %{
  started_at: DateTime.utc_now() |> DateTime.to_iso8601(),
  host_cpu:
    System.get_env("BENCH_HOST") ||
      case :os.type() do
        {:unix, :darwin} -> System.cmd("sysctl", ["-n", "machdep.cpu.brand_string"]) |> elem(0) |> String.trim()
        _ -> File.read!("/proc/cpuinfo") |> then(&Regex.run(~r/model name\s*:\s*(.+)/, &1)) |> Enum.at(1)
      end,
  docker_cpus: System.cmd("docker", ["info", "--format", "{{.NCPU}}"]) |> elem(0) |> String.trim(),
  pg_redis_commit:
    System.get_env("BENCH_COMMIT") ||
      System.cmd("git", ["rev-parse", "--short", "HEAD"]) |> elem(0) |> String.trim(),
  pg_redis_dirty:
    !System.get_env("BENCH_COMMIT") and
      System.cmd("git", ["status", "--porcelain"]) |> elem(0) |> String.trim() != "",
  postgres: Rig.psql("SHOW server_version"),
  redis: Rig.exec("redis", ["redis-server", "--version"]) |> String.trim(),
  policies: policy_table,
  reps: Bench.reps(),
  server_cpus: "pinned to cpuset #{System.get_env("BENCH_SERVER_CPUS", "0-3")}, 4 GB",
  load_cpus: "pinned to cpuset #{System.get_env("BENCH_LOAD_CPUS", "8-13")}",
  pg_settings: "redis.workers=4, redis.maxmemory=1024MB, shared_buffers=1GB, redis.snapshot_interval=60"
}

results =
  targets
  |> Enum.group_by(& &1.server)
  |> Enum.sort_by(fn {server, _} -> Enum.find_index(~w(redis redis-aof pg_redis), &(&1 == server)) end)
  |> Enum.flat_map(fn {server, ts} ->
    log.("server: #{server}")
    Rig.only(server)

    for t <- ts, group <- groups do
      # Each target runs with its own appendfsync, always unless it names another; the
      # workers apply a change from their next round.
      if t.server == "pg_redis" do
        Rig.cli(t, ["CONFIG", "SET", "appendfsync", Map.get(t, :fsync, "always")])
        Process.sleep(500)
      end

      flush = fn -> Rig.cli(t, ["FLUSHDB"]) end
      big = if t.fast, do: 1, else: 0.25

      rows =
        case group do
          "commands" ->
            for cmd <- commands do
              unless cmd in follows, do: flush.()
              log.("  #{t.id} #{cmd}")
              {cmd, Bench.repeat(fn -> Bench.redis_benchmark(t, cmd, n: Bench.n(200_000 * big)) end)}
            end

          "sizes" ->
            for d <- [64, 1024, 16_384, 131_072, 1_048_576], cmd <- ~w(set get) do
              if cmd == "set", do: flush.()
              # Keep the keyspace under ~200 MB at every size, well inside maxmemory
              keys = min(100_000, div(200_000_000, d))
              count = Bench.n(min(200_000, 20_000_000_000 / d / 100) * big)
              log.("  #{t.id} #{cmd} #{d}B")
              {"#{cmd} #{d}", Bench.repeat(fn -> Bench.redis_benchmark(t, cmd, n: round(count), d: d, r: keys) end)}
            end

          "clients" ->
            for c <- [1, 10, 50, 200], cmd <- ~w(set get) do
              if cmd == "set", do: flush.()
              count = Bench.n(if(c == 1, do: 20_000, else: 200_000) * big)
              log.("  #{t.id} #{cmd} c=#{c}")
              {"#{cmd} c=#{c}", Bench.repeat(fn -> Bench.redis_benchmark(t, cmd, n: count, c: c) end)}
            end

          "pipeline" ->
            for cmd <- ~w(set get) do
              if cmd == "set", do: flush.()
              log.("  #{t.id} #{cmd} P=16")
              {"#{cmd} P=16", Bench.repeat(fn -> Bench.redis_benchmark(t, cmd, n: Bench.n(1_000_000 * big), p: 16) end)}
            end

          "mixed" ->
            flush.()
            log.("  #{t.id} memtier 1:10")
            seconds = max(round(20 * Bench.n(1000) / 1000), 5)
            [{"memtier 1:10, 256B, 100k keys",
              Bench.repeat(fn -> flush.(); Bench.memtier(t, keys: 100_000, size: 256, seconds: seconds) end)}]
        end

      flush.()
      {t.id, group, Map.new(rows)}
    end
  end)

# ── Report ───────────────────────────────────────────────────────────────────

by = Enum.reduce(results, %{}, fn {tid, group, rows}, acc ->
  Map.update(acc, group, %{tid => rows}, &Map.put(&1, tid, rows))
end)

fmt = fn
  nil -> "n/a"
  n when is_number(n) and n >= 1000 -> n |> round() |> Integer.to_string() |> String.reverse() |> String.replace(~r/(\d{3})(?=\d)/, "\\1,") |> String.reverse()
  n when is_number(n) -> :erlang.float_to_binary(n / 1, decimals: 2)
end

cell = fn
  %{rps: rps} -> fmt.(rps)
  _ -> "error"
end

ids = Enum.map(targets, & &1.id)

table = fn group, title ->
  rows = by[group] || %{}
  keys = rows |> Map.values() |> Enum.flat_map(&Map.keys/1) |> Enum.uniq()
  order = case group do
    "commands" -> commands
    # "set 1024", "get c=50": by the number, then the command, so rows read small to large
    _ -> keys |> Enum.sort_by(fn k ->
           n = Regex.run(~r/\d+/, k) |> then(fn m -> if m, do: String.to_integer(hd(m)), else: 0 end)
           {n, k |> String.split() |> hd() |> then(&if(&1 == "set", do: 0, else: 1))}
         end)
  end
  head = "| #{title} | " <> Enum.map_join(targets, " | ", & &1.label) <> " |\n|---|" <> String.duplicate("---:|", length(targets))
  body = for k <- order, Enum.any?(ids, &get_in(rows, [&1, k])) do
    "| #{k} | " <> Enum.map_join(ids, " | ", &cell.(get_in(rows, [&1, k]))) <> " |"
  end
  Enum.join([head | body], "\n")
end

stamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d-%H%M%S")
out = System.get_env("BENCH_OUT", "docker/bench/results/#{stamp}")
File.mkdir_p!(Path.dirname(out))

md = """
# pg_redis benchmark #{meta.started_at}

Requests per second, median of #{meta.reps} runs, higher is better. Values are 64 bytes and
keys random over 100,000 unless the row says otherwise; 50 clients.

- Host: #{meta.host_cpu}, Docker with #{meta.docker_cpus} CPUs
- Servers: #{meta.server_cpus}, one running at a time. Load generator: #{meta.load_cpus}
- pg_redis #{meta.pg_redis_commit}#{if meta.pg_redis_dirty, do: " (uncommitted changes)", else: ""} on PostgreSQL #{meta.postgres}: #{meta.pg_settings}
- Policies: #{meta.policies}
- #{meta.redis}

#{if by["commands"], do: "## Commands\n\n" <> table.("commands", "command") <> "\n", else: ""}
#{if by["sizes"], do: "## Value sizes\n\n" <> table.("sizes", "command, bytes") <> "\n", else: ""}
#{if by["clients"], do: "## Clients\n\n" <> table.("clients", "command, clients") <> "\n", else: ""}
#{if by["pipeline"], do: "## Pipelining\n\n" <> table.("pipeline", "command") <> "\n", else: ""}
#{if by["mixed"], do: "## Mixed workload (memtier_benchmark, 1 write : 10 reads, 256 B, 4 threads × 25 clients)\n\n" <> table.("mixed", "workload") <> "\n", else: ""}
"""

File.write!("#{out}.json", JSON.encode!(%{meta: meta, targets: Enum.map(targets, &Map.take(&1, [:id, :label])), results: Enum.map(results, fn {t, g, r} -> %{target: t, group: g, rows: r} end)}))
File.write!("#{out}.md", md)
log.("wrote #{out}.{json,md}")
IO.puts(md)

unless System.get_env("BENCH_KEEP"), do: Rig.down()
