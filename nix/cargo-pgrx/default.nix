{
  lib,
  fetchCrate,
  openssl,
  pkg-config,
  makeRustPlatform,
  stdenv,
  rust-bin,
  rustVersion ? "1.88.0",
}:
let
  rustPlatform = makeRustPlatform {
    cargo = rust-bin.stable.${rustVersion}.default;
    rustc = rust-bin.stable.${rustVersion}.default;
  };
  mkCargoPgrx =
    {
      version,
      hash,
      cargoHash,
    }:
    let
      pname = if builtins.compareVersions "0.7.4" version >= 0 then "cargo-pgx" else "cargo-pgrx";
    in
    rustPlatform.buildRustPackage rec {
      auditable = false;
      inherit pname;
      inherit version;
      src = fetchCrate { inherit version pname hash; };
      inherit cargoHash;
      nativeBuildInputs = lib.optionals stdenv.hostPlatform.isLinux [ pkg-config ];
      buildInputs = lib.optionals stdenv.hostPlatform.isLinux [ openssl ];

      OPENSSL_DIR = "${openssl.dev}";
      OPENSSL_INCLUDE_DIR = "${openssl.dev}/include";
      OPENSSL_LIB_DIR = "${openssl.out}/lib";
      PKG_CONFIG_PATH = "${openssl.dev}/lib/pkgconfig";
      preCheck = ''
        export PGRX_HOME=$(mktemp -d)
      '';
      checkFlags = [
        "--skip=command::schema::tests::test_parse_managed_postmasters"
      ];
      meta = with lib; {
        description = "Build Postgres Extensions with Rust";
        homepage = "https://github.com/pgcentralfoundation/pgrx";
        changelog = "https://github.com/pgcentralfoundation/pgrx/releases/tag/v${version}";
        license = licenses.mit;
        maintainers = with maintainers; [ happysalada ];
        mainProgram = "cargo-pgrx";
      };
    };
in
{
  cargo-pgrx_0_16_1 = mkCargoPgrx {
    version = "0.16.1";
    hash = "sha256-AjoBr+/sEPdzbD0wLUNVm2syCySkGaFOFQ70TST1U9w=";
    cargoHash = "sha256-95DHq5GLnAqb3bbKwwaeBeKEmkfRh81ZTRaJ7L59DAg=";
  };
  inherit mkCargoPgrx;
}
