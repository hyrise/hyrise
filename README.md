[![Build Status](https://ares.epic.hpi.uni-potsdam.de/jenkins/buildStatus/icon?job=Hyrise/zweirise/master)](https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/zweirise/)

# Hyrise v2 (Codename OpossumDB)

*Have a look at our [contributor guidelines](https://github.com/hyrise/zweirise/blob/master/CONTRIBUTING.md)*

The [wiki](https://github.com/hyrise/zweirise/wiki) is a good starting point to get to know Hyrise

## Easy start
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

## Dependencies
You can install the dependencies on your own or use the install.sh script (**recommended**) which installs all of the therein listed dependencies and submodules.
The install script was tested under macOS (brew) and Ubuntu 17.04/17.10 (apt-get).

See docs/dependencies.md for a detailled list of dependencies to use with `brew install` or `apt-get install`, depending on your platform. As compilers, we generally use the most recent version of gcc and clang.
Older versions may work, but are neither tested nor supported.

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
Note, that the tests/asan/etc need to be executed from the project root in order for table-files to be found.

### Coverage
`./scripts/coverage.sh <build dir>` will print a summary to the command line and create detailed html reports at ./coverage/index.html

*Supports only clang on MacOS and only gcc on linux*

### AddressSanitizer
`make hyriseAsan` will build Hyrise with enabled AddressSanitizer and Undefined Behavior options and execute all available tests.
It will fail on the first detected error and will print a summary.
To convert addresses to actual source code locations, make sure llvm-symbolizer is installed (included in the llvm package) and is available in `$PATH`.
To specify a custom location for the symbolizer, set `$ASAN_SYMBOLIZER_PATH` to the path of the executable.
This seems to work out of the box on macOS - If not, make sure to have llvm installed.
The binary can be executed with `LSAN_OPTIONS=suppressions=asan-ignore.txt ./<YourBuildDirectory>/hyriseAsan`.

### Compile Times
When trying to optimize the time spent building the project, it is often helpful to have an idea how much time is spent where.
`scripts/compile_time.sh` helps with that. Get usage instructions by running it without any arguments.

## Naming convention for gtest macros:

TEST(ModuleNameClassNameTest, TestName), e.g., TEST(OperatorsGetTableTest, RowCount)
same for fixtures Test_F()

If you want to test a single module, class or test you have to execute the test binary and use the `gtest_filter` option:

- Testing the storage module: `./build/test --gtest_filter="Storage*"`
- Testing the table class: `./build/test --gtest_filter="StorageTableTest*"`
- Testing the RowCount test: `./build/test --gtest_filter="StorageTableTest.RowCount"`

## Maintainers

- Jan Kossmann
- Markus Dreseler
- Martin Boissier
- Stefan Klauck


Contact: firstname.lastname@hpi.de

## Contributors

-	Yannick  Bäumer
-	Timo     Djürken
-	Fabian   Dumke
-	Moritz   Eyssen
-	Martin   Fischer
-	Pedro    Flemming
-	Sven     Ihde
-	Michael  Janke
-	Max      Jendruk
-	Marvin   Keller
-	Sven     Lehmann
-	Jan      Mattfeld
-	Arne     Mayer
-	Torben   Meyer
-	David    Schumann
-	Daniel   Stolpe
-	Jonathan Striebel
-	Nils     Thamm
-	Carsten  Walther
-	Fabian   Wiebe
-	Tim      Zimmermann
