let
  nixPkgsUrl = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-23.11";
  pkgs = import nixPkgsUrl { config = {}; overlays = []; };
in 

pkgs.mkShellNoCC {
  packages = with pkgs; [
    autoconf
    boost
    llvmPackages_14.libcxxClang
    gcc13
    gcc11
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
    numactl
    ninja
  ];
}

