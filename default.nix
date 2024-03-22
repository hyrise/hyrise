#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then run 
#     nix-build
# to build Hyrise entirely.
# Even though the setup might work with other systems, the 
# following are recommended:
# - x86_64 Linux
# - x86_64 MacOS
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

  # Read more on the difference of buildInputs and nativeBuildInputs here: 
  # https://discourse.nixos.org/t/use-buildinputs-or-nativebuildinputs-for-nix-shell/8464
  
  # TODO(everyone): Figure out which packages should go to buildInputs and nativeBuildInputs
  # Usually only used for building.
  nativeBuildInputs = with pkgs; [
    gcc
    cmake
    python3
    ninja
    sqlite
    lld
  ];
  
  # Usually things that we link on.
  buildInputs = with pkgs; [
    autoconf
    boost
    coreutils
    dos2unix
    gcovr
    parallel
    python311Packages.pexpect
    postgresql_16
    readline
    tbb
  ];

  hardeningDisable = [ 
    "format"
    "fortify"
    "fortify3"
  ];
  
  cmakeFlags = [
    "DCMAKE_BUILD_TYPE=Debug"
    "DCMAKE_C_COMPILER=gcc"
    "DCMAKE_CXX_COMPILER=g++"
    "DCMAKE_UNITY_BUILD=Off"
    "DENABLE_ADDR_UB_LEAK_SANITIZATION=ON"
  ];

  prePatch = ''
    mkdir -p "$out/bin"
  '';
})
