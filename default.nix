#
# Nix Setup for Hyrise
#
# First install Nix (see https://nixos.org/manual/nix/stable/installation/).
# Then in the Hyrise repository, run
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

pkgs.mkShell {
  name = "hyrise";
  version = "0.0.1";

  # nativeBuildInputs vs. buildInputs
  # - nativeBuildInputs are packages required during the runtime of the shell process.
  # - buildInputs are packages required during the build of the shell process, but not
  # anymore during the shell's runtime. As this is intended for usage with nix-shell
  # (i.e., there are no build steps in this Nix derivation and users will execute
  # processes only after the shell is already available), only nativeBuildInputs are
  # needed.
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

  # Running Hyrise executables requires the LD_LIBRARY_PATH variable set. This does not
  # happen by other hooks, therefore it happens manually here. Each of the derivations
  # listed here provides a dynamically linked library.
  # View the results by running `nix-shell --pure --run "echo $LD_LIBRARY_PATH"`.
  shellHook = ''
	export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [
		pkgs.lld
		pkgs.sqlite
		pkgs.tbb_2021_11
		pkgs.boost
		pkgs.libgccjit
		pkgs.stdenv.cc.cc.lib
	]}:$LD_LIBRARY_PATH"
  '';
}
