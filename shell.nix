let
  baseconfig = { allowUnfree = true; };
  pkgs = import <nixpkgs> { config = baseconfig; };
  unstable = import <nixos-unstable> { config = baseconfig; };

  shell = pkgs.mkShell {
    packages = [
        unstable.dotnetCorePackages.dotnet_11.sdk
        pkgs.jq
    ];

    shellHook = ''
        code .
    '';
    };
in shell
