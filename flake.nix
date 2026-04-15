{
  description = "pg_redis — Redis protocol (RESP2) interface backed by PostgreSQL tables";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        buildFor =
          postgresql:
          pkgs.callPackage ./nix/ext/pg_redis/default.nix { inherit postgresql; };
      in
      {
        packages = {
          pg_redis-pg15 = buildFor pkgs.postgresql_15;
          pg_redis-pg16 = buildFor pkgs.postgresql_16;
          pg_redis-pg17 = buildFor pkgs.postgresql_17;
          default = buildFor pkgs.postgresql_17;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            (rust-bin.stable."1.88.0".default.override {
              extensions = [ "rust-src" "rust-analyzer" ];
            })
            postgresql_17
          ];
        };
      }
    );
}
