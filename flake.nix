{
  description = "Devshell for distributed service with go book";

  inputs = {
    nixpkgs = { url = "github:NixOS/nixpkgs/nixpkgs-unstable"; };
    flake-utils = { url = "github:numtide/flake-utils"; };
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit system;
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_19
            gopls
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
          ];
        };
        shellHook = ''
        if [[ $TERM_PROGRAM != "tmux" ]]; then
          tmux new -s prolog
        fi
        '';
    });
}
