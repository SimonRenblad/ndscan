{
  description = "ndscan for ARTIQ";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }:
  let
    pkgs = import nixpkgs { system = "x86_64-linux"; };
  in {
    devShells.x86_64-linux.default = pkgs.mkShell {
      name = "ndscan-dev-shell";
      buildInputs = [
        (pkgs.python3.withPackages (ps: with ps; [scipy numpy h5py]))
      ];
    };
  };
}
