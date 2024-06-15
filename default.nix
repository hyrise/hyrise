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

#
# Why does this work?
#
# Scrolling down the file, you will not find any commands like 'cmake ..' or 'make'.
# Don't worry, we do not miss them and they are not written down anywhere else.
# Instead, Nix takes core of the build process for us. We only need to include
# 'cmake' in the nativeBuildInputs, adjust the cmakeFlags and we are good to go.
#

# Often you will find mkDerivation without a function argument, but that does not
# allow recursively calling the attributes of the derivation.
# One can either use rec { .. } or (finalAttrs: { .. }). The latter is recommended.
pkgs.mkShell {
  name = "hyrise";
  version = "0.0.1";

  # In simple Linux2Linux same-arch applications, buildInputs and nativeBuildInputs
  # do not make any significant difference. Therefore, Hyrise the differentiation
  # is not severly important.
  # Read more on the difference of buildInputs and nativeBuildInputs here: 
  # https://discourse.nixos.org/t/use-buildinputs-or-nativebuildinputs-for-nix-shell/8464

  buildInputs = with pkgs; [
    gcc
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
    tbb
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
