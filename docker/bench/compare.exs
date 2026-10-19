# Compare two bench-suite runs: before → after for each pg_redis policy, Redis alongside.
#
#   elixir docker/bench/compare.exs docker/bench/results/BEFORE.json docker/bench/results/AFTER.json
#
# Prints markdown: one table per group, a row per measurement, the change as a multiple.

[before_path, after_path] =
  case System.argv() do
    [a, b] -> [a, b]
    _ -> raise "usage: compare.exs BEFORE.json AFTER.json"
  end

load = fn path ->
  %{"results" => results} = path |> File.read!() |> JSON.decode!()

  for %{"target" => t, "group" => g, "rows" => rows} <- results, {name, row} <- rows, into: %{} do
    {{g, name, t}, row["rps"]}
  end
end

before = load.(before_path)
after_ = load.(after_path)

fmt = fn
  nil -> "–"
  n when n >= 1000 -> n |> round() |> Integer.to_string() |> String.reverse() |> String.replace(~r/(\d{3})(?=\d)/, "\\1,") |> String.reverse()
  n -> :erlang.float_to_binary(n / 1, decimals: 0)
end

ratio = fn
  nil, _ -> "–"
  _, nil -> "–"
  b, a when b > 0 -> :erlang.float_to_binary(a / b, decimals: 1) <> "×"
  _, _ -> "–"
end

order = %{
  "commands" => ~w(set get incr lpush lpop rpush rpop sadd spop hset zadd zpopmin lrange_100 mset)
}

groups = after_ |> Map.keys() |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort_by(&Enum.find_index(~w(commands sizes clients pipeline mixed), fn g -> g == &1 end))

for g <- groups do
  names = after_ |> Map.keys() |> Enum.filter(&(elem(&1, 0) == g)) |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
  names =
    case order[g] do
      nil -> Enum.sort_by(names, fn n -> {Regex.run(~r/\d+/, n) |> then(&if(&1, do: String.to_integer(hd(&1)), else: 0)), n} end)
      o -> Enum.filter(o, &(&1 in names))
    end

  IO.puts("\n### #{g}\n")
  IO.puts("| | Redis 7.2 | memory before | memory after | | logged before | logged after | |")
  IO.puts("|---|---:|---:|---:|---:|---:|---:|---:|")

  for n <- names do
    r = after_[{g, n, "redis"}]
    mb = before[{g, n, "memory"}]
    ma = after_[{g, n, "memory"}]
    lb = before[{g, n, "logged"}]
    la = after_[{g, n, "logged"}]
    IO.puts("| #{n} | #{fmt.(r)} | #{fmt.(mb)} | #{fmt.(ma)} | #{ratio.(mb, ma)} | #{fmt.(lb)} | #{fmt.(la)} | #{ratio.(lb, la)} |")
  end
end
