{
  callPackage,
  rustVersion,
  pgrxVersion,
  makeRustPlatform,
  rust-bin,
  stdenv,
}:
let
  inherit ((callPackage ./default.nix { inherit rustVersion; })) mkCargoPgrx;

  rustPlatform = makeRustPlatform {
    cargo = rust-bin.stable.${rustVersion}.default;
    rustc = rust-bin.stable.${rustVersion}.default;
  };

  versions = builtins.fromJSON (builtins.readFile ./versions.json);

  cargo-pgrx =
    let
      pgrx =
        versions.${pgrxVersion}
          or (throw "Unsupported pgrx version ${pgrxVersion}. Available: ${builtins.toString (builtins.attrNames versions)}.");
      mapping = {
        inherit (pgrx) hash;
        cargoHash =
          pgrx.rust."${rustVersion}".cargoHash
            or (throw "Unsupported rust version ${rustVersion} for pgrx ${pgrxVersion}.");
      };
    in
    mkCargoPgrx {
      inherit (mapping) hash cargoHash;
      version = pgrxVersion;
    };

  bindgenHook =
    if (builtins.compareVersions "0.11.3" pgrxVersion > 0) then
      let
        nixos2211 = (
          import (builtins.fetchTarball {
            url = "https://channels.nixos.org/nixos-22.11/nixexprs.tar.xz";
            sha256 = "1j7h75a9hwkkm97jicky5rhvzkdwxsv5v46473rl6agvq2sj97y1";
          }) { inherit (stdenv.hostPlatform) system; }
        );
      in
      rustPlatform.bindgenHook.overrideAttrs {
        libclang = nixos2211.clang.cc.lib;
        clang = nixos2211.clang;
      }
    else
      rustPlatform.bindgenHook;
in
callPackage ./buildPgrxExtension.nix {
  inherit rustPlatform;
  inherit cargo-pgrx;
  defaultBindgenHook = bindgenHook;
}
