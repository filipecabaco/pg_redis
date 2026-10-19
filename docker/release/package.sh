#!/bin/sh
# Build and package the extension for one PostgreSQL major, inside a release builder image.
#
#   package.sh <pg major> <version> <pg_config>
#
# Writes /out (the files at their absolute install paths, for the ext image) and
# /dist/pg_redis.tar.gz (lib/ + extension/, for install.sh to place with pg_config's paths).
set -eu
major=$1 version=$2 pg_config=$3

# The release version replaces Cargo.toml's placeholder; pg_redis.control's default_version and
# the install script's file name both come from it
sed -i -E "1,/^version = /s/^version = \".*\"/version = \"${version}\"/" Cargo.toml

cargo pgrx init "--pg${major}" "$pg_config"
cargo pgrx package --pg-config "$pg_config" --no-default-features --features "pg${major}"

staged="target/release/pg_redis-pg${major}"
mkdir -p /out
cp -a "$staged/." /out/

pkglibdir=$("$pg_config" --pkglibdir)
sharedir=$("$pg_config" --sharedir)
tree=/tmp/pg_redis
mkdir -p "$tree/lib" "$tree/extension"
cp "$staged$pkglibdir"/pg_redis* "$tree/lib/"
cp "$staged$sharedir"/extension/pg_redis* "$tree/extension/"
printf 'version=%s\npg_major=%s\n' "$version" "$major" > "$tree/MANIFEST"
# The newest glibc symbol version the .so needs is the real floor for a glibc build (often
# below the builder's own glibc); install.sh checks the machine against it
if command -v objdump >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi glibc; then
  floor=$(objdump -T "$tree"/lib/pg_redis.so | grep -oE 'GLIBC_[0-9]+\.[0-9]+' |
    sed 's/GLIBC_//' | sort -t. -k1,1n -k2,2n -u | tail -1)
  printf 'glibc=%s\n' "$floor" >> "$tree/MANIFEST"
fi

mkdir -p /dist
tar -C /tmp -czf /dist/pg_redis.tar.gz pg_redis
tar -tzf /dist/pg_redis.tar.gz
