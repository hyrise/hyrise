#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
#
# Then choose one of the following options:
# - Run `nix-shell`. You will be dropped into a fully working shell and can
#   proceed with your build commands as you did before.
#   Do this, if you develop the application and expect to compile frequently.
# - Run `nix-build` and wait for the whole project to be built. The results
#   will be located in /res. This does not require you to know any build 
#   commands, but it will rebuild everything from scratch every time you run 
#   it. Use it for measurement runs.

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
pkgs.stdenv.mkDerivation (finalAttrs: {
  name = "hyrise";
  version = "0.0.1";

  src = ./.;

  # In simple Linux2Linux same-arch applications, buildInputs and nativeBuildInputs
  # do not make any significant difference. Therefore, Hyrise the differentiation
  # is not severly important.
  # Read more on the difference of buildInputs and nativeBuildInputs here: 
  # https://discourse.nixos.org/t/use-buildinputs-or-nativebuildinputs-for-nix-shell/8464

  nativeBuildInputs = with pkgs; [
    gcc11
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
  ];

  # Usually things that we link on.
  buildInputs = with pkgs; [
    boost
    postgresql_16
  ];

  # Changes these as desired.
  cmakeFlags = [
    "-DCMAKE_BUILD_TYPE=Debug"
    "-DCMAKE_CXX_COMPILER=clang++"
    "-DCMAKE_C_COMPILER=clang"
  ];

  hardeningDisable = [
    "format"
    "fortify"
    "fortify3"
  ];

  fixupPhase = ''
    mkdir -p $out/bin
    cp -r $src/* $out/bin
  '';
})
