{
  description = "Development environment for Hyrise";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          name = "hyrise";

          # nativeBuildInputs vs. buildInputs
          #  - nativeBuildInputs are packages required during the runtime of the shell process.
          #  - buildInputs are packages required during the build of the shell process, but not anymore during the
          #    shell's runtime. As this is intended for usage with `nix develop` (i.e., there are no build steps in
          #    this flake and users will execute processes only after the shell is already available), only
          #    nativeBuildInputs are needed.
          nativeBuildInputs = with pkgs; [
            autoconf
            bc
            boost
            # gcc is listed before clang so that clang's setup hook runs last and remains the default compiler,
            # matching CMake's default. gcc stays on PATH as an explicit opt-in via -DCMAKE_C_COMPILER=gcc.
            gcc
            # Pinned to LLVM 20 (rather than the generic, ever-moving `clang`/`llvm`/`lld` attributes) because
            # this project is not warning-clean on newer clang majors, e.g. clang 21 rejects third_party/magic_enum
            # under -Werror=nrvo.
            llvmPackages_20.bintools
            llvmPackages_20.clang
            llvmPackages_20.clang-tools
            llvmPackages_20.libllvm
            llvmPackages_20.lld
            cmake
            coreutils
            dos2unix
            graphviz
            hwloc
            ncurses
            ninja
            parallel
            postgresql_16
            (python3.withPackages (ps: with ps; [
              black
              flake8
              matplotlib
              numpy
              pandas
              pexpect
              psutil
              pydriller
              scipy
              seaborn
              termcolor
              terminaltables
            ]))
            readline
            sqlite
            tbb_2022
            valgrind
          ] ++ lib.optionals stdenv.isLinux [
            numactl
          ];

          hardeningDisable = [
            "format"
            "fortify"
            "fortify3"
          ];

          # Running Hyrise executables requires the LD_LIBRARY_PATH variable set. This does not happen by other
          # hooks, therefore it happens manually here. Each of the packages listed here provides a dynamically linked
          # library. View the results by running `nix develop --command bash -c 'echo $LD_LIBRARY_PATH'`.
          shellHook = ''
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath ([
              pkgs.boost
              pkgs.hwloc
              pkgs.llvmPackages_20.lld
              pkgs.ncurses
              pkgs.postgresql_16
              pkgs.readline
              pkgs.sqlite
              pkgs.stdenv.cc.cc.lib
              pkgs.tbb_2022
            ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.numactl ])}:$LD_LIBRARY_PATH"
          '';
        };
      });
    };
}
