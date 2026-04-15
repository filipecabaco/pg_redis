# preBuildAndTest and some small other bits
# taken from https://github.com/tcdi/pgrx/blob/v0.9.4/nix/extension.nix
# (but now heavily modified)
# which uses MIT License — see https://github.com/pgcentralfoundation/pgrx/blob/main/LICENSE
{
  lib,
  cargo-pgrx,
  pkg-config,
  rustPlatform,
  stdenv,
  writeShellScriptBin,
  defaultBindgenHook,
}:

{
  buildAndTestSubdir ? null,
  buildType ? "release",
  buildFeatures ? [ ],
  cargoBuildFlags ? [ ],
  postgresql,
  bindgenHook ? defaultBindgenHook,
  useFakeRustfmt ? true,
  usePgTestCheckFeature ? true,
  ...
}@args:
let
  rustfmtInNativeBuildInputs = lib.lists.any (dep: lib.getName dep == "rustfmt") (
    args.nativeBuildInputs or [ ]
  );
in

assert lib.asserts.assertMsg (
  (args.installPhase or "") == ""
) "buildPgrxExtensions overwrites the installPhase, so providing one does nothing";
assert lib.asserts.assertMsg (
  (args.buildPhase or "") == ""
) "buildPgrxExtensions overwrites the buildPhase, so providing one does nothing";
assert lib.asserts.assertMsg (useFakeRustfmt -> !rustfmtInNativeBuildInputs)
  "The parameter useFakeRustfmt is set to true, but rustfmt is included in nativeBuildInputs. Either set useFakeRustfmt to false or remove rustfmt from nativeBuildInputs.";
assert lib.asserts.assertMsg (!useFakeRustfmt -> rustfmtInNativeBuildInputs)
  "The parameter useFakeRustfmt is set to false, but rustfmt is not included in nativeBuildInputs. Either set useFakeRustfmt to true or add rustfmt from nativeBuildInputs.";

let
  fakeRustfmt = writeShellScriptBin "rustfmt" ''
    exit 0
  '';

  rustcWrapper = writeShellScriptBin "rustc" ''
    original_rustc="''${ORIGINAL_RUSTC:-rustc}"
    filtered_args=()
    for arg in "$@"; do
      if [[ -z "$arg" ]]; then
        continue
      fi
      if [[ "$arg" =~ postmaster_stub\.rs$ ]]; then
        if [[ ! -s "$arg" ]]; then
          continue
        fi
      fi
      filtered_args+=("$arg")
    done
    exec "$original_rustc" "''${filtered_args[@]}"
  '';
  maybeDebugFlag = lib.optionalString (buildType != "release") "--debug";
  maybeEnterBuildAndTestSubdir = lib.optionalString (buildAndTestSubdir != null) ''
    export CARGO_TARGET_DIR="$(pwd)/target"
    pushd "${buildAndTestSubdir}"
  '';
  maybeLeaveBuildAndTestSubdir = lib.optionalString (buildAndTestSubdir != null) "popd";
  pgrxBinaryName = if builtins.compareVersions "0.7.4" cargo-pgrx.version >= 0 then "pgx" else "pgrx";

  needsRustcWrapper = builtins.compareVersions cargo-pgrx.version "0.12.0" < 0;

  pgrxPostgresMajor = lib.versions.major postgresql.version;
  preBuildAndTest = ''
    export PGRX_HOME=$(mktemp -d)
    export PGX_HOME=$PGRX_HOME
    export PGDATA="$PGRX_HOME/data-${pgrxPostgresMajor}/"
    cargo-${pgrxBinaryName} ${pgrxBinaryName} init "--pg${pgrxPostgresMajor}" ${lib.getDev postgresql}/bin/pg_config

    export PGHOST="$(mktemp -d)"
    cat > "$PGDATA/postgresql.conf" <<EOF
    listen_addresses = '''
    unix_socket_directories = '$PGHOST'
    EOF

    export USER="$(whoami)"
    pg_ctl start
    createuser -h localhost --superuser --createdb "$USER" || true
    pg_ctl stop
  '';

  argsForBuildRustPackage = builtins.removeAttrs args [
    "postgresql"
    "useFakeRustfmt"
    "usePgTestCheckFeature"
  ];

  finalArgs = argsForBuildRustPackage // {
    buildInputs = (args.buildInputs or [ ]);

    nativeBuildInputs =
      (args.nativeBuildInputs or [ ])
      ++ [
        cargo-pgrx
        postgresql
        pkg-config
        bindgenHook
      ]
      ++ lib.optionals useFakeRustfmt [ fakeRustfmt ];

    buildPhase = ''
      runHook preBuild

      echo "Executing cargo-pgrx buildPhase"
      ${preBuildAndTest}
      ${maybeEnterBuildAndTestSubdir}

      export PGRX_BUILD_FLAGS="--frozen -j $NIX_BUILD_CORES ${builtins.concatStringsSep " " cargoBuildFlags}"
      export PGX_BUILD_FLAGS="$PGRX_BUILD_FLAGS"

      ${lib.optionalString needsRustcWrapper ''
        export ORIGINAL_RUSTC="$(command -v ${stdenv.cc.targetPrefix}rustc || command -v rustc)"
        export PATH="${rustcWrapper}/bin:$PATH"
        export RUSTC="${rustcWrapper}/bin/rustc"
      ''}

      ${lib.optionalString stdenv.hostPlatform.isDarwin ''RUSTFLAGS="''${RUSTFLAGS:+''${RUSTFLAGS} }-Clink-args=-Wl,-undefined,dynamic_lookup"''} \
      cargo ${pgrxBinaryName} package \
        --pg-config ${lib.getDev postgresql}/bin/pg_config \
        ${maybeDebugFlag} \
        --features "${builtins.concatStringsSep " " buildFeatures}" \
        --out-dir "$out"

      ${maybeLeaveBuildAndTestSubdir}

      runHook postBuild
    '';

    preCheck = preBuildAndTest + args.preCheck or "";

    installPhase = ''
      runHook preInstall

      echo "Executing buildPgrxExtension install"

      ${maybeEnterBuildAndTestSubdir}

      cargo-${pgrxBinaryName} ${pgrxBinaryName} stop all

      mv $out/${postgresql}/* $out
      mv $out/${postgresql.lib}/* $out
      rm -rf $out/nix

      ${maybeLeaveBuildAndTestSubdir}

      runHook postInstall
    '';

    PGRX_PG_SYS_SKIP_BINDING_REWRITE = "1";
    CARGO_BUILD_INCREMENTAL = "false";
    RUST_BACKTRACE = "full";

    checkNoDefaultFeatures = true;
    checkFeatures =
      (args.checkFeatures or [ ])
      ++ (lib.optionals usePgTestCheckFeature [ "pg_test" ])
      ++ [ "pg${pgrxPostgresMajor}" ];
  };
in
rustPlatform.buildRustPackage finalArgs
