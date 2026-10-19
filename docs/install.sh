#!/bin/sh
# Install pg_redis into an existing PostgreSQL from a GitHub release.
#
#   curl -fsSL https://filipecabaco.github.io/pg_redis/install.sh | sh
#   curl -fsSL https://filipecabaco.github.io/pg_redis/install.sh | sh -s -- --configure
#
# It finds pg_config, picks the release tarball for that PostgreSQL major, this CPU and this
# libc, checks it against the release's SHA256SUMS, and copies the files into pg_config's
# pkglibdir and sharedir. It changes no configuration unless --configure is given, and then
# only appends pg_redis to shared_preload_libraries. Linux only for now.
#
# PG_REDIS_BASE_URL points it at a mirror of a release's files (tarballs and SHA256SUMS)
# instead of GitHub.
set -eu

REPO=${PG_REDIS_REPO:-filipecabaco/pg_redis}
VERSION=
PG_CONFIG_BIN=${PG_CONFIG:-}
FILE=
CONFIGURE=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

  --version vX.Y.Z   install this release (default: the latest)
  --pg-config PATH   the pg_config of the PostgreSQL to install into (default: $PG_CONFIG, then PATH)
  --file TARBALL     install a tarball already on disk instead of downloading one
  --configure        also add pg_redis to shared_preload_libraries (connects with psql)
  --dry-run          print what would happen and change nothing
  -h, --help         show this help
USAGE
}

say() { printf '%-11s %s\n' "$1" "$2"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case $1 in
    --version) VERSION=${2:?--version needs a value}; shift 2 ;;
    --version=*) VERSION=${1#*=}; shift ;;
    --pg-config) PG_CONFIG_BIN=${2:?--pg-config needs a value}; shift 2 ;;
    --pg-config=*) PG_CONFIG_BIN=${1#*=}; shift ;;
    --file) FILE=${2:?--file needs a value}; shift 2 ;;
    --file=*) FILE=${1#*=}; shift ;;
    --configure) CONFIGURE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

# ── Which PostgreSQL ─────────────────────────────────────────────────────────

if [ -z "$PG_CONFIG_BIN" ]; then
  PG_CONFIG_BIN=$(command -v pg_config 2>/dev/null || true)
fi
if [ -z "$PG_CONFIG_BIN" ]; then
  # Debian and Ubuntu keep each major's pg_config off PATH
  found=$(ls /usr/lib/postgresql/*/bin/pg_config 2>/dev/null || true)
  case $(printf '%s\n' "$found" | grep -c .) in
    1) PG_CONFIG_BIN=$found ;;
    0) die "pg_config not found. Install your PostgreSQL's development package, or pass --pg-config PATH." ;;
    *) die "several PostgreSQL versions found; choose one with --pg-config:
$found" ;;
  esac
fi
[ -x "$PG_CONFIG_BIN" ] || die "$PG_CONFIG_BIN is not executable"

pg_version=$("$PG_CONFIG_BIN" --version | awk '{print $2}')
major=${pg_version%%.*}
case $major in
  15|16|17|18) ;;
  *) die "PostgreSQL $pg_version is not supported (15 to 18 are)" ;;
esac

# ── Which build ──────────────────────────────────────────────────────────────

[ "$(uname -s)" = Linux ] || die "only Linux builds are published; on $(uname -s), build from source: https://filipecabaco.github.io/pg_redis/installation/"

case $(uname -m) in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) die "no build for $(uname -m); build from source" ;;
esac

if ls /lib/ld-musl-* >/dev/null 2>&1; then
  libc=musl
else
  libc=gnu
fi

asset="pg_redis-pg${major}-linux-${arch}-${libc}.tar.gz"
pkglibdir=$("$PG_CONFIG_BIN" --pkglibdir)
extdir="$("$PG_CONFIG_BIN" --sharedir)/extension"

say pg_config "$PG_CONFIG_BIN  (PostgreSQL $pg_version, $arch, $libc)"

# ── Fetch and verify ─────────────────────────────────────────────────────────

fetch() {
  if command -v curl >/dev/null 2>&1; then curl -fsSL -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then wget -q -O "$2" "$1"
  else die "needs curl or wget"
  fi
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
  else shasum -a 256 "$1" | awk '{print $1}'
  fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

if [ -n "$FILE" ]; then
  [ -f "$FILE" ] || die "$FILE does not exist"
  cp "$FILE" "$tmp/$asset"
  say tarball "$FILE  (not verified: no SHA256SUMS for a local file)"
else
  if [ -n "${PG_REDIS_BASE_URL:-}" ]; then
    base=${PG_REDIS_BASE_URL%/}   # a mirror holding the release's files
  elif [ -n "$VERSION" ]; then
    base="https://github.com/$REPO/releases/download/$VERSION"
  else
    base="https://github.com/$REPO/releases/latest/download"
  fi
  fetch "$base/$asset" "$tmp/$asset" || die "could not download $base/$asset"
  fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "could not download $base/SHA256SUMS"
  want=$(awk -v f="$asset" '$2 == f || $2 == "*" f {print $1}' "$tmp/SHA256SUMS")
  [ -n "$want" ] || die "$asset is not listed in SHA256SUMS"
  [ "$(sha256 "$tmp/$asset")" = "$want" ] || die "$asset does not match its SHA256SUMS entry"
  say download "$asset  ✓ sha256"
fi

tar -xzf "$tmp/$asset" -C "$tmp"
[ -f "$tmp/pg_redis/MANIFEST" ] || die "$asset is not a pg_redis release tarball"
built_for=$(sed -n 's/^pg_major=//p' "$tmp/pg_redis/MANIFEST")
[ "$built_for" = "$major" ] || die "$asset was built for PostgreSQL $built_for, not $major"
version=$(sed -n 's/^version=//p' "$tmp/pg_redis/MANIFEST")

# A glibc build records the newest glibc symbol version it links against
needs=$(sed -n 's/^glibc=//p' "$tmp/pg_redis/MANIFEST")
have=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')
if [ -n "$needs" ] && [ -n "$have" ] &&
   [ "$(printf '%s\n%s\n' "$have" "$needs" | sort -t. -k1,1n -k2,2n | head -1)" != "$needs" ]; then
  die "this build needs glibc $needs or newer, and this machine has $have; build from source"
fi

# ── Install ──────────────────────────────────────────────────────────────────

as_root=
if [ "$(id -u)" != 0 ] && { [ ! -w "$pkglibdir" ] || [ ! -w "$extdir" ]; }; then
  command -v sudo >/dev/null 2>&1 || die "$pkglibdir is not writable; run as root"
  as_root=sudo
fi

run() {
  if [ "$DRY_RUN" = 1 ]; then printf '  would run: %s\n' "$*"; else $as_root "$@"; fi
}

run install -m 755 "$tmp"/pg_redis/lib/pg_redis* "$pkglibdir/"
run install -m 644 "$tmp"/pg_redis/extension/pg_redis* "$extdir/"
say install "$pkglibdir/$(cd "$tmp/pg_redis/lib" && ls)"
for f in "$tmp"/pg_redis/extension/*; do say "" "$extdir/${f##*/}"; done
say version "$version"

# ── Configure ────────────────────────────────────────────────────────────────

psql_bin="$("$PG_CONFIG_BIN" --bindir)/psql"
[ -x "$psql_bin" ] || psql_bin=psql

sql() {
  # As the current user first (libpq's PG* variables apply); as root, fall back to the postgres
  # superuser over the local socket
  "$psql_bin" -X -qAt -c "$1" 2>/dev/null && return 0
  if [ "$(id -u)" = 0 ] && id postgres >/dev/null 2>&1; then
    su postgres -s /bin/sh -c "\"$psql_bin\" -X -qAt -c \"$1\"" 2>/dev/null && return 0
  fi
  return 1
}

configured=0
if [ "$CONFIGURE" = 1 ]; then
  # The value the next start will use: an earlier ALTER SYSTEM that hasn't been applied yet
  # (pg_file_settings), else the running one. It is a list, so each library is quoted on its
  # own; one quoted string 'a, b' would name a single library called "a, b".
  if current=$(sql "SELECT coalesce((SELECT setting FROM pg_file_settings WHERE name = 'shared_preload_libraries' ORDER BY seqno DESC LIMIT 1), current_setting('shared_preload_libraries'))"); then
    libs=$(printf '%s' "$current" | tr -d '"' | tr ',' '\n' | sed 's/^ *//; s/ *$//' | grep -v '^$' || true)
    if printf '%s\n' "$libs" | grep -qx pg_redis; then
      say configure "shared_preload_libraries already includes pg_redis"
      configured=1
    else
      list=$(printf '%s\npg_redis\n' "$libs" | grep -v '^$' | sed "s/.*/'&'/" | paste -sd, - | sed 's/,/, /g')
      if [ "$DRY_RUN" = 1 ]; then
        printf '  would run: ALTER SYSTEM SET shared_preload_libraries = %s\n' "$list"
        configured=1
      elif sql "ALTER SYSTEM SET shared_preload_libraries = $list" >/dev/null; then
        say configure "shared_preload_libraries = $list"
        configured=1
      fi
    fi
  fi
  [ "$configured" = 1 ] || say configure "could not connect with psql; set it by hand (below)"
fi

echo
if [ "$configured" = 0 ]; then
  echo "Next: add pg_redis to shared_preload_libraries, for example"
  echo "  ALTER SYSTEM SET shared_preload_libraries = 'pg_redis';   -- append if it already lists others"
fi
echo "Then restart PostgreSQL, and run:"
echo "  CREATE EXTENSION pg_redis;"
echo "  redis-cli -p 6379 PING"
