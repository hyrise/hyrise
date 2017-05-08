[![Build Status](https://ares.epic.hpi.uni-potsdam.de/jenkins/buildStatus/icon?job=Hyrise/zweirise/master)](https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/zweirise/)

# opossum

*Have a look at our [contributor guidelines](https://github.com/hyrise/zweirise/blob/master/CONTRIBUTING.md)*

The [course material](https://hpi.de//plattner/teaching/winter-term-201617/build-your-own-database.html) is a good starting point to get to know Opossum

## Dependencies
You can install the dependencies on your own or use the install.sh script which installs most of the following packages.

The install script currently works with macOS (brew) and Ubuntu 16.10 (apt-get)

### premake4
install via homebrew / packet manager

### boost (version: >= 1.61.0)
install via homebrew / packet manager

### compiler
install recent versions of compilers (clang >= 3.5.0 and/or gcc >= 6.1) via homebrew / packet manager

### clang-format (version: >= 3.8)
install via homebrew / packet manager

### python (version: 2.7)
install via homebrew (python2.7 is standard on macOS) / packet manager

### gcovr (version: >= 3.2)
install via homebrew / packet manager

### googletest
get via `git submodule update --init`

### tbb
install via homebrew: brew install tbb
install via apt: apt-get install libtbb-dev

### development command line tools
install via `xcode-select --install` / `apt install build-essential`

### autoconf
install via homebrew / packet manager

### automake
install via homebrew / packet manager (installed as default by Ubuntu)

### libtool
install via homebrew / packet manager

### pkg-config
install via homebrew / packet manager (installed as default by Ubuntu)

### get and compile protoc and gRPC
get via `git submodule update --init --recursive`.

Compile via `CPPFLAGS="-Wno-deprecated-declarations" CFLAGS="-Wno-deprecated-declarations -Wno-implicit-function-declaration -Wno-shift-negative-value" make static --directory=third_party/grpc REQUIRE_CUSTOM_LIBRARIES_opt=true`.

installation guide on [github](https://github.com/grpc/grpc/blob/master/INSTALL.md#build-from-source)

### llvm (optional)
install via homebrew / packet manager
used for AddressSanitizer


## Building and Tooling

### compiler choice
You can specify the compiler via `premake4 --compiler=clang||gcc`

On linux you have to utilize make's `-R` flag if your choice does not equal your default compiler

### build
`premake4 && make -j`

Usually debug binaries are created. To activate release builds use `make config=release`

### lint (is also automatically triggerd before git commit)
`premake4 lint` (Google's cpplint is used which needs python 2.7)

### format (is also automatically triggered with make)
`premake4 format`

### testing (is also automatically triggered before git commit)
`make test` executes all available tests
The binary can be executed with `./build/test`

### coverage
`make -j coverage` will print a summary to the command line and create detailed html reports at ./coverage/index.html

*Supports only clang on MacOS and only gcc on linux*

### AddressSanitizer
`make -j asan` will build OpossumDB with enabled AddressSanitizer options and execute all available tests. It will fail on the first detected memory error and will print a summary. To convert addresses to actual source code locations, make sure llvm-symbolizer is installed (included in llvm package) and is available in `$PATH`. To specify a custom location for the symbolizer, set `$ASAN_SYMBOLIZER_PATH` to the path of the executable. This seems to work out of the box on macOS - If not, make sure to have llvm installed.

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

-	Yannick	Bäumer
-	Moritz	Eyssen
-	Martin	Fischer
-	Michael	Janke
-	Max	Jendruk
-	Marvin	Keller
-	Sven	Lehmann
-	Jan	Mattfeld
-	Arne	Mayer
-	Torben	Meyer
-	David	Schumann
-	Daniel	Stolpe
-	Nils	Thamm
-	Carsten	Walther
-	Fabian	Wiebe
-	Tim	Zimmermann
