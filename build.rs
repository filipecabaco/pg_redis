//! Compiles the Lua libraries Redis ships to scripts — cjson, cmsgpack,
//! struct and bit — against the Lua that `mlua` vendors. They are Redis's own
//! C sources (see `vendor/redis-lua/README.md`), so what a script gets from
//! `cjson.encode` or `cmsgpack.pack` is byte for byte what Redis gives it.

fn main() {
    let lua_include = std::env::var("DEP_LUA_INCLUDE")
        .expect("mlua-sys exports its include directory as DEP_LUA_INCLUDE");
    let dir = std::path::Path::new("vendor/redis-lua");
    let sources = [
        "lua_cjson.c",
        "strbuf.c",
        "fpconv.c",
        "lua_cmsgpack.c",
        "lua_struct.c",
        "lua_bit.c",
    ];
    let mut build = cc::Build::new();
    build
        .include(&lua_include)
        .include(dir)
        .define("REDIS_STATIC", Some(""))
        .warnings(false)
        .opt_level(2);
    for s in sources {
        build.file(dir.join(s));
        println!("cargo:rerun-if-changed={}", dir.join(s).display());
    }
    build.compile("redis_lua_libs");
    println!("cargo:rerun-if-changed=build.rs");
}
