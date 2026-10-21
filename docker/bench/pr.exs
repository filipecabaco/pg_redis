# A pull request's throughput against main's, for CI to post on the pull request.
#
#   elixir docker/bench/pr.exs > bench.md
#
# Both servers and the reference Redis run on the same machine, started by
# `docker compose --profile bench --profile bench-main up`: pg_redis built from
# the pull request (`postgres`), from main (`postgres-main`), and Redis 7.2
# (`redis`). Each measurement runs on all three in turn, in a different order
# every round, and the median of the rounds is reported: a shared runner's
# noise then lands on all three alike, and what is left is the change.
#
#   BENCH_RUN  how to run redis-benchmark and redis-cli (default: in the redis container)
#   ROUNDS     rounds per measurement (default: 5)
#   MAIN_SHA, PR_SHA  the commits measured, for the heading

defmodule Bench do
  @password "testpass"
  # A change smaller than this is within what a shared runner moves by itself.
  @threshold 0.10

  @targets [main: "postgres-main", pr: "postgres", redis: "redis"]

  # {group, label, redis-benchmark test, database, value bytes, requests, compare with Redis}
  def measurements do
    memory =
      # PING first: no change to the data path reaches it, so it shows how far
      # the runner alone moved the figures.
      for t <- ~w(ping_mbulk set get incr lpush rpush lpop rpop sadd hset spop zadd zpopmin lrange_100 mset) do
        {:memory, t, t, 0, 3, 20_000, true}
      end

    sizes =
      for d <- [1024, 65_536], t <- ~w(set get) do
        {:sizes, "#{t} #{d} B", t, 0, d, if(d > 4096, do: 5_000, else: 20_000), true}
      end

    # The durable half, where a change to the SQL path shows. Fewer requests:
    # each write waits for a commit.
    logged =
      for t <- ~w(set get incr lpush rpush lpop rpop sadd hset spop zadd lrange_100) do
        {:logged, t, t, 8, 64, 4_000, false}
      end

    memory ++ sizes ++ logged
  end

  def run do
    rounds = String.to_integer(System.get_env("ROUNDS", "5"))
    ms = measurements()

    results =
      for round <- 1..rounds, reduce: %{} do
        acc ->
          for {m, i} <- Enum.with_index(ms), reduce: acc do
            acc ->
              {group, label, _, _, _, _, with_redis} = m
              targets = if with_redis, do: @targets, else: Keyword.delete(@targets, :redis)

              # A different target goes first each round and each measurement.
              targets
              |> rotate(round + i)
              |> Enum.reduce(acc, fn {name, host}, acc ->
                Map.update(acc, {group, label, name}, [rps(host, m)], &[rps(host, m) | &1])
              end)
          end
      end

    medians = Map.new(results, fn {k, v} -> {k, median(v)} end)
    IO.puts(report(ms, medians, rounds))
  end

  defp rotate(list, n), do: Enum.split(list, rem(n, length(list))) |> then(fn {a, b} -> b ++ a end)

  # The pushes and LRANGE start from an empty list, so every target measures
  # the same list; the pops drain what the pushes made.
  defp rps(host, {_, _, test, db, size, n, _}) do
    if test in ~w(lpush rpush lrange_100), do: cli(host, db, ["DEL", "mylist"])

    {out, _} =
      cmd(["redis-benchmark", "-h", host, "-a", @password, "--dbnum", "#{db}", "-n", "#{n}",
           "-c", "50", "-d", "#{size}", "-q", "-t", test])

    want = String.upcase(test)

    out
    |> String.split(["\r", "\n"])
    |> Enum.find_value(fn line ->
      with true <- String.starts_with?(line, want),
           [_, v] <- Regex.run(~r/: ([0-9.]+) requests per second/, line) do
        String.to_float(v)
      else
        _ -> nil
      end
    end)
  end

  defp cli(host, db, args),
    do: cmd(["redis-cli", "-h", host, "-a", @password, "--no-auth-warning", "-n", "#{db}" | args])

  defp cmd(args) do
    [exe | pre] = String.split(System.get_env("BENCH_RUN", "docker compose exec -T redis"))
    System.cmd(exe, pre ++ args, stderr_to_stdout: true)
  end

  defp median(values) do
    case values |> Enum.reject(&is_nil/1) |> Enum.sort() do
      [] -> nil
      sorted -> Enum.at(sorted, div(length(sorted), 2))
    end
  end

  defp change(nil, _), do: nil
  defp change(_, nil), do: nil
  defp change(main, pr), do: (pr - main) / main

  defp report(ms, med, rounds) do
    changes =
      for {g, l, _, _, _, _, _} <- ms, c = change(med[{g, l, :main}], med[{g, l, :pr}]), do: c

    faster = Enum.count(changes, &(&1 >= @threshold))
    slower = Enum.count(changes, &(&1 <= -@threshold))
    sha = fn var -> System.get_env(var, "") |> String.slice(0, 7) end

    [
      "<!-- pg_redis-bench -->",
      "## Benchmarks: this pull request against main",
      "",
      "`main` #{sha.("MAIN_SHA")} → this PR #{sha.("PR_SHA")} · " <>
        "**#{faster} faster, #{slower} slower** by #{round(@threshold * 100)}% or more",
      "",
      section(ms, med, :memory, "Memory (db 0, 3-byte values)", true),
      section(ms, med, :sizes, "Value sizes (db 0)", true),
      section(ms, med, :logged, "Logged (db 8, 64-byte values)", false),
      "<sub>Requests per second, higher is better; 50 clients; median of #{rounds} " <>
        "#{if rounds == 1, do: "round", else: "rounds"}, each " <>
        "running main, this PR and Redis 7.2 on the same runner in turn. A shared runner moves " <>
        "a figure by a few percent on its own: changes under #{round(@threshold * 100)}% are " <>
        "left unmarked. `docker/bench/pr.exs`.</sub>"
    ]
    |> Enum.join("\n")
  end

  defp section(ms, med, group, title, with_redis) do
    rows =
      for {^group, label, _, _, _, _, _} <- ms do
        main = med[{group, label, :main}]
        pr = med[{group, label, :pr}]
        cells = [label, fmt(main), fmt(pr), mark(change(main, pr))]
        cells = if with_redis, do: cells ++ [fmt(med[{group, label, :redis}])], else: cells
        "| " <> Enum.join(cells, " | ") <> " |"
      end

    head = ["command", "main", "this PR", "change"] ++ if(with_redis, do: ["Redis 7.2"], else: [])
    align = ["---", "---:", "---:", "---:"] ++ if(with_redis, do: ["---:"], else: [])

    Enum.join(
      ["### #{title}", "", "| " <> Enum.join(head, " | ") <> " |",
       "| " <> Enum.join(align, " | ") <> " |" | rows] ++ [""],
      "\n"
    )
  end

  defp mark(nil), do: "–"

  defp mark(c) do
    pct = "#{if c >= 0, do: "+", else: "−"}#{abs(round(c * 100))}%"

    cond do
      c >= @threshold -> "**▲ #{pct}**"
      c <= -@threshold -> "**▼ #{pct}**"
      true -> pct
    end
  end

  defp fmt(nil), do: "n/a"

  defp fmt(n) do
    n
    |> round()
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end
end

Bench.run()
