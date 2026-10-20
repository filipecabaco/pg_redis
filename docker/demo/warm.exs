# Warms the examples page's machine: runs every runnable block of docs/examples.md in a
# snowglobe run of the built demo, records the files the guest reads, and writes those the
# home page's warm pack does not hold into a second pack, system/warm-examples.pack, which only
# the examples page preloads. The home page keeps downloading just what its own demo needs.
#
#   elixir docker/demo/warm.exs [site dir]       (default docs/demo; needs snowglobe on PATH,
#                                                 or SNOWGLOBE)
#
# A pack is a little-endian u32 index length, the index as JSON ([name, _, length] per blob),
# then the blobs in index order.

[site | _] = System.argv() ++ ["docs/demo"]
snowglobe = System.get_env("SNOWGLOBE", "snowglobe")
name = "pg-redis-warm-#{System.os_time(:second)}"
reads = Path.join(System.tmp_dir!(), "#{name}-reads.json")

defmodule Warm do
  def sh(cmd, args, opts \\ []) do
    {out, status} = System.cmd(cmd, args, [stderr_to_stdout: true] ++ opts)
    if status != 0, do: raise("#{cmd} #{Enum.join(args, " ")} failed (#{status}):\n#{out}")
    out
  end

  # The page's runnable blocks, as the shell commands that do the same from /examples: a
  # redis-cli transcript's commands, a SQL block through psql, a console block's `$` lines.
  def commands(markdown) do
    ~r/^```(\w+)\n(.*?)^```/ms
    |> Regex.scan(markdown, capture: :all_but_first)
    |> Enum.flat_map(fn [lang, body] -> block(lang, String.split(body, "\n", trim: true)) end)
  end

  defp block(lang, lines) when lang in ["redis", "text"] do
    for line <- lines, m = Regex.run(~r/^127\.0\.0\.1:6379(?:\[(\d+)\])?> (.*)$/, line) do
      [_, db, cmd] = m
      db = if db == "", do: "0", else: db
      "redis-cli -n #{db} #{cmd}"
    end
  end

  defp block("sql", lines),
    do: ["psql -h /run/postgresql -U postgres -c #{shell_quote(Enum.join(lines, "\n"))}"]

  defp block("console", lines),
    do: for("$ " <> cmd <- lines, do: cmd)

  defp block(_, _), do: []

  defp shell_quote(s), do: "'" <> String.replace(s, "'", ~S('"'"')) <> "'"

  def read_pack(path) do
    <<size::little-32, rest::binary>> = File.read!(path)
    <<index::binary-size(size), blobs::binary>> = rest
    {entries, _} =
      Enum.map_reduce(:json.decode(index), 0, fn [n, _ | _] = e, at ->
        len = List.last(e)
        {{n, e, binary_part(blobs, at, len)}, at + len}
      end)
    entries
  end

  def write_pack(path, entries) do
    index = :json.encode(Enum.map(entries, fn {_, e, _} -> e end)) |> IO.iodata_to_binary()
    File.write!(path, [<<byte_size(index)::little-32>>, index | Enum.map(entries, &elem(&1, 2))])
  end
end

commands = Warm.commands(File.read!("docs/examples.md"))
IO.puts("#{length(commands)} commands from docs/examples.md")

Warm.sh(snowglobe, ["run", site, "--name", name, "--detach", "--record-reads", reads])

try do
  # Out of redis-cli, the top pane's shell takes over; everything runs from /examples.
  Warm.sh(snowglobe, ["exec", name, "exit"])
  Warm.sh(snowglobe, ["exec", name, "cd /examples"])
  for cmd <- commands do
    IO.puts("  $ #{String.slice(cmd, 0, 100)}")
    Warm.sh(snowglobe, ["exec", name, cmd])
  end
after
  Warm.sh(snowglobe, ["stop", name])
end

pack = Path.join([site, "system", "warm.pack"])
home = Warm.read_pack(pack)
File.cp!(pack, pack <> ".home")
File.cp!(Path.join(site, "run.json"), Path.join(site, "run.json.home"))
IO.write(Warm.sh(snowglobe, ["warm", site, reads]))

held = MapSet.new(home, &elem(&1, 0))
extra = Enum.reject(Warm.read_pack(pack), fn {n, _, _} -> MapSet.member?(held, n) end)
Warm.write_pack(Path.join([site, "system", "warm-examples.pack"]), extra)

# The home page's pack and the build's own record of it stay as they were.
File.rename!(pack <> ".home", pack)
File.rename!(Path.join(site, "run.json.home"), Path.join(site, "run.json"))
bytes = extra |> Enum.map(&byte_size(elem(&1, 2))) |> Enum.sum()
IO.puts("warm-examples.pack: #{length(extra)} files, #{Float.round(bytes / 1_048_576, 1)} MB")
