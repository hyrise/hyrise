let
  nixPkgsUrl = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-23.11";
  pkgs = import nixPkgsUrl { config = {}; overlays = []; };
in 

pkgs.mkShellNoCC {
  packages = with pkgs; [
    autoconf
    boost
    clang
    gcc13
    coreutils
    cmake
    dos2unix
    gcovr
    lld
    parallel
    python311Packages.pexpect
    postgresql_16
    python3
    readline
    sqlite
    tbb
    valgrind
    ninja
  ];
}

