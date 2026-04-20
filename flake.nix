{
  description = "ndscan for ARTIQ";

  inputs = {
    artiq = {
      url = git+https://git.m-labs.hk/M-Labs/artiq.git;
    };
    src-oitg = {
      url = "github:OxfordIonTrapGroup/oitg";
      flake = false;
    };
  };

  outputs =
    { self, src-oitg, artiq }:
    let
      pkgs = import artiq.inputs.nac3.inputs.nixpkgs { system = "x86_64-linux"; };
      oitg = pkgs.python3Packages.buildPythonPackage rec {
        pname = "oitg";
        version = "0.2";
        src = src-oitg;
        pyproject = true;
        build-system = [ pkgs.python3Packages.poetry-core ];
        propagatedBuildInputs = with pkgs.python3Packages; [
          poetry-dynamic-versioning
          numpy
          h5py
          scipy
          statsmodels
        ];
      };
      ndscan = pkgs.python3Packages.buildPythonPackage rec {
        pname = "ndscan";
        version = "0.3";
        src = self;       
        pyproject = true;
        build-system = [pkgs.python3Packages.hatchling];
        propagatedBuildInputs = [ oitg artiq.packages.x86_64-linux.artiq ];
        dontWrapQtApps = true;
      };
    in
    {
      devShells.x86_64-linux.default = pkgs.mkShell {
        name = "ndscan-dev-shell";
        buildInputs = [
          (pkgs.python3.withPackages (
            ps: with ps; [
              scipy
              numpy
              h5py
              ndscan
              oitg
            ]
          ))
        ];
      };
      packages.x86_64-linux.default = ndscan;
    };
}
