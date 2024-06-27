#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then in the root of this repository, run 
#
#   nix-shell --pure`
#
# The `--pure` flag ensures that the shell does not take over environment
# variables from the host. Therefore, all software installed comes purely
# from Nix package manager. 
# For MacOS user: This also means the default Apple clang is not used ;)

let
  pkgs = import ./nixpkgs {};
in

pkgs.stdenv.mkDerivation {
  name = "hyrise";
  version = "0.0.1";

  # nativeBuildInput vs. buildInputs
  #     nativeBuildInputs are packages that are needed to be available during the runtime of the process
  #     buildInputs are that are needed during the build process, but not anymore during the runtime. 
  # As this is intended for usage with nix-shell, only nativeBuildInputs are needed.
  nativeBuildInputs = with pkgs; [
    clang
    autoconf
    cmake
    python3
    sqlite
    lld
    ninja
    parallel
    coreutils
    dos2unix
    gcovr
    python311Packages.pexpect
    tbb_2021_11
    readline
    boost
    postgresql_16
  ];

  hardeningDisable = [
    "format"
    "fortify"
    "fortify3"
  ];
}
