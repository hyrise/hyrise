[![Build Status](https://hyrise-ci.epic-hpi.de/buildStatus/icon?job=Hyrise/hyrise/master)](https://hyrise-ci.epic-hpi.de/blue/organizations/jenkins/Hyrise%2Fhyrise/activity/)
[![Coverage Status](https://hyrise-coverage-badge.herokuapp.com/coverage_badge.svg)](https://hyrise-ci.epic-hpi.de/job/Hyrise/job/hyrise/job/master/lastStableBuild/Llvm-cov_5fReport/)

# Welcome to Hyrise

This is the repository for the current Hyrise version, which has been rewritten from scratch. The new code base is easier to setup, to understand, and to contribute to. As of now, not all features of the old version are supported yet - we are working on that.

Papers that were published before October 2017 were based on the previous version of Hyrise, which can be found [here](https://github.com/hyrise/hyrise-v1).

# Getting started

*Have a look at our [contributor guidelines](CONTRIBUTING.md)*

You can find definitions of most of the terms and abbreviations used in the code in the [glossary](GLOSSARY.md). If you cannot find something that you are looking for, feel free to open an issue.

The [Step by Step Guide](https://github.com/hyrise/hyrise/wiki/Step-by-Step-Guide) is a good starting point to get to know Hyrise.

## Native Setup
You can install the dependencies on your own or use the install.sh script (**recommended**) which installs all of the therein listed dependencies and submodules.
The install script was tested under macOS High Sierra and Ubuntu 18.04 (apt-get).

See [dependencies](DEPENDENCIES.md) for a detailed list of dependencies to use with `brew install` or `apt-get install`, depending on your platform. As compilers, we generally use the most recent version of clang and gcc (Linux only). Please make sure that the system compiler points to the most recent version or use cmake (see below) accordingly.
Older versions may work, but are neither tested nor supported.

## Setup using Docker
To get all dependencies of Hyrise in a docker image, run
```
docker-compose build
```

You can start the container via
```
docker-compose run --rm hyrise
```

Inside of the container, run `./install.sh` to download the required submodules.
:whale:

## Building and Tooling
It is highly recommended to perform out-of-source builds, i.e., creating a separate directory for the build.
Advisable names for this directory would be `cmake-build-{debug,release}`, depending on the build type.
Within this directory call `cmake ..` to configure the build.
Subsequent calls to CMake, e.g., when adding files to the build will not be necessary, the generated Makefiles will take care of that.

### Compiler choice
CMake will default to your system's default compiler.
To use a different one, call like `cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ ..` in a clean build directory.

### Build
Simply call `make -j*`, where `*` denotes the number of threads to use.

Usually debug binaries are created.
To configure a build directory for a release build make sure it is empty and call CMake like `cmake -DCMAKE_BUILD_TYPE=Release`

### Lint
`./scripts/lint.sh` (Google's cpplint is used which needs python 2.7)

### Format
`./scripts/format.sh` (clang-format is used)

### Test
Calling `make hyriseTest` from the build directory builds all available tests.
The binary can be executed with `./<YourBuildDirectory>/hyriseTest`.
Note, that the tests/sanitizers/etc need to be executed from the project root in order for table files to be found.

### Coverage
`./scripts/coverage.sh` will print a summary to the command line and create detailed html reports at ./coverage/index.html

*Supports only clang on MacOS and only gcc on linux*

### Address/UndefinedBehavior Sanitizers
`cmake -DENABLE_ADDR_UB_SANITIZATION=ON` will generate Makefiles with AddressSanitizer and Undefined Behavior options.
Compile and run them as normal - if any issues are detected, they will be printed to the console.
It will fail on the first detected error and will print a summary.
To convert addresses to actual source code locations, make sure llvm-symbolizer is installed (included in the llvm package) and is available in `$PATH`.
To specify a custom location for the symbolizer, set `$ASAN_SYMBOLIZER_PATH` to the path of the executable.
This seems to work out of the box on macOS - If not, make sure to have llvm installed.
The binary can be executed with `LSAN_OPTIONS=suppressions=asan-ignore.txt ./<YourBuildDirectory>/hyriseTest`.

`cmake -DENABLE_THREAD_SANITIZATION=ON` will work as above but with the ThreadSanitizer. Some sanitizers are mutually exclusive, which is why we use two configurations for this.

### Compile Times
When trying to optimize the time spent building the project, it is often helpful to have an idea how much time is spent where.
`scripts/compile_time.sh` helps with that. Get usage instructions by running it without any arguments.

## Maintainers

- Jan Kossmann
- Markus Dreseler
- Martin Boissier
- Stefan Klauck


Contact: firstname.lastname@hpi.de

## Contributors

-	Yannick  Bäumer
-	Lawrence Benson
-	Timo     Djürken
-	Fabian   Dumke
-	Moritz   Eyssen
-	Martin   Fischer
-	Pedro    Flemming
-	Johannes Frohnhofen
-	Adrian   Holfter
-	Sven     Ihde
-	Michael  Janke
-	Max      Jendruk
-	Marvin   Keller
-	Sven     Lehmann
-	Jan      Mattfeld
-	Arne     Mayer
-	Torben   Meyer
-	Leander  Neiß
-	David    Schumann
-	Arthur   Silber
-	Daniel   Stolpe
-	Jonathan Striebel
-	Nils     Thamm
-	Carsten  Walther
-	Lukas    Wenzel
-	Fabian   Wiebe
-	Tim      Zimmermann
