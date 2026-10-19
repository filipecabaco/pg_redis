# Redis's Lua libraries

The C sources of the libraries Redis 7.2.5 loads into its Lua scripting
environment, copied from `deps/lua/src` of the Redis tree:

| File | Library | Licence |
|---|---|---|
| `lua_cjson.c`, `strbuf.[ch]`, `fpconv.[ch]` | `cjson` (Mark Pulford, with Redis's fixes) | MIT |
| `lua_cmsgpack.c` | `cmsgpack` (Salvatore Sanfilippo) | MIT |
| `lua_struct.c` | `struct` (Roberto Ierusalimschy) | MIT |
| `lua_bit.c` | `bit` (Mike Pall, LuaBitOp) | MIT |

Each file keeps its copyright notice. `build.rs` compiles them against the
Lua 5.1 that `mlua` vendors.

One local change: `lua_cjson.c` no longer includes
`../../../src/solarisfixes.h`, a Redis-tree header that only matters on
Solaris.
