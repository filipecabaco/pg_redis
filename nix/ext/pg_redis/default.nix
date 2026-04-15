{
  lib,
  pkgs,
  stdenv,
  callPackages,
  postgresql,
  rust-bin,
  makeWrapper,
  latestOnly ? true,
}:
let
  pname = "pg_redis";
  version = "0.0.0";
  rustVersion = "1.88.0";
  pgrxVersion = "0.16.1";

  cargo = rust-bin.stable.${rustVersion}.default;
  mkPgrxExtension = callPackages ../../cargo-pgrx/mkPgrxExtension.nix {
    inherit rustVersion pgrxVersion;
  };

  # Build from the repo root. builtins.path avoids the git-tracking requirement
  # that a plain path reference (../../..) would impose in a flake context.
  src = builtins.path {
    path = ../../..;
    name = "pg_redis-source";
    filter = path: type:
      !(builtins.elem (baseNameOf path) [ ".git" "target" ".cargo" "result" ]);
  };
in
mkPgrxExtension {
  inherit pname version postgresql src;

  nativeBuildInputs = [ cargo ];
  buildInputs = [ postgresql ];

  cargoLock = {
    lockFile = "${src}/Cargo.lock";
    allowBuiltinFetchGit = false;
  };

  # pg_redis starts background TCP workers — disable them during the Nix build
  # by setting redis.workers = 0 in the pgrx-managed postgres instance.
  preBuild = ''
    echo "redis.workers = 0" >> "$PGDATA/postgresql.conf"
    echo "shared_preload_libraries = 'pg_redis'" >> "$PGDATA/postgresql.conf"
  '';

  # Tests require a live Redis-protocol listener; skip in the Nix sandbox.
  doCheck = false;

  env = lib.optionalAttrs stdenv.isDarwin {
    POSTGRES_LIB = "${postgresql}/lib";
    RUSTFLAGS = "-C link-arg=-undefined -C link-arg=dynamic_lookup";
    # Each extension needs a unique port to avoid conflicts during parallel builds.
    PGPORT = toString (
      5460
      + (if builtins.match ".*_.*" postgresql.version != null then 1 else 0)
      + ((builtins.fromJSON (builtins.substring 0 2 postgresql.version)) - 15) * 2
    );
  };

  CARGO = "${cargo}/bin/cargo";

  postInstall = ''
    mv $out/lib/${pname}${postgresql.dlSuffix} $out/lib/${pname}-${version}${postgresql.dlSuffix}

    sed -e "/^default_version =/d" \
        -e "s|^module_pathname = .*|module_pathname = '\$libdir/${pname}'|" \
      ${pname}.control > $out/share/postgresql/extension/${pname}--${version}.control
    rm -f $out/share/postgresql/extension/${pname}.control

    {
      echo "default_version = '${version}'"
      cat $out/share/postgresql/extension/${pname}--${version}.control
    } > $out/share/postgresql/extension/${pname}.control
    ln -sfn ${pname}-${version}${postgresql.dlSuffix} $out/lib/${pname}${postgresql.dlSuffix}
  '';

  meta = with lib; {
    description = "Redis protocol (RESP2) interface backed by PostgreSQL tables";
    homepage = "https://github.com/filipecabaco/pg_redis";
    platforms = postgresql.meta.platforms;
    license = licenses.postgresql;
  };
}
