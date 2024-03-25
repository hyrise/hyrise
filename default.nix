#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then run 
#     nix-shell
# and proceed to build Hyrise, e.g., do the following:
#
#   mkdir cmake-build-debug && cd cmake-build-debug
#   cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
#   ninja
#
# Even though the setup might work with other systems, the 
# following are recommended:
# - x86_64 Linux
# - ARM MacOS
#

let
  nixPkgsUrl = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-23.11";
  pkgs = import nixPkgsUrl { config = {}; overlays = []; };
in 

# Often you will find mkDerivation without a function argument, but that does not 
# allow recursively calling the attributes of the derivation.
# One can either use rec { .. } or (finalAttrs: { .. }). The latter is recommended.
pkgs.stdenv.mkDerivation (finalAttrs: {
  name = "hyrise";
  version = "0.0.1";

  src = ./.;

  # In simple Linux2Linux same-arcj applications, buildInputs and nativeBuildInputs
  # do not make any significant difference. Therefore, Hyrise the differentiation
  # is not severly important.
  # Read more on the difference of buildInputs and nativeBuildInputs here: 
  # https://discourse.nixos.org/t/use-buildinputs-or-nativebuildinputs-for-nix-shell/8464
  
  # TODO(everyone): Figure out which packages should go to buildInputs and nativeBuildInputs
  # Usually only used for building.
  nativeBuildInputs = with pkgs; [
    gcc11
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
  ];
  
  # Usually things that we link on.
  buildInputs = with pkgs; [
    boost
    postgresql_16
    readline
    tbb
  ];

  hardeningDisable = [ 
    "format"
    "fortify"
    "fortify3"
  ];
})
