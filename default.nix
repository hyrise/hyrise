#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then in the root of this repository, run
#
#   nix-shell --pure
#
# You will now have an environment that allows building Hyrise with the standard
# build commands.
#
# The `--pure` flag ensures that the shell does not take over environment
# variables from the host. Therefore, all software installed comes purely
# from Nix package manager and local installations do not influence the build.

let
  pkgs = import ./nixpkgs {};
in

pkgs.stdenv.mkDerivation {
  name = "hyrise";
  version = "0.0.1";

  # nativeBuildInputs vs. buildInputs
  # - nativeBuildInputs are packages required during the runtime of the shell process.
  # - buildInputs are packages required during the build of the shell process, but not anymore during the shell's runtime.
  # As this is intended for usage with nix-shell (i.e., there are no build steps in this Nix derivation and users will execute processes only after the shell is already available), only nativeBuildInputs are needed.
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
