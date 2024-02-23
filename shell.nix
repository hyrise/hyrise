#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then run 
#     nix-shell    
# in the root of the repository to open a shell that is enabled to compile
# Hyrise. Even though the setup might work with other systems, the 
# following are recommended:
# - x86_64 Linux
# - x86_64 MacOS
# - ARM MacOS
#

let
  nixPkgsUrl = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-23.11";
  pkgs = import nixPkgsUrl { config = {}; overlays = []; };
in 

pkgs.mkShell {
  packages = with pkgs; [
    autoconf
    boost
    llvmPackages_16.libcxxClang
    libgcc
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
    ninja
  ];

  hardeningDisable = [ 
    "format"
    "fortify"
    "fortify3"
  ];
}

