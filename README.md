[![build status](https://gitlab.hpi.de/OpossumDB/OpossumDB/badges/master/build.svg)](https://gitlab.hpi.de/OpossumDB/OpossumDB/commits/master)
[![coverage report](https://gitlab.hpi.de/OpossumDB/OpossumDB/badges/master/coverage.svg)](https://gitlab.hpi.de/OpossumDB/OpossumDB/commits/master)
# opossum

*Have a look at docs/guidelines*

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

## Naming convention for gtest macros:

TEST(ModuleNameClassNameTest, TestName), e.g., TEST(OperatorsGetTableTest, RowCount)
same for fixtures Test_F()

If you want to test a single module, class or test you have to execute the test binary and use the `gtest_filter` option:

- Testing the storage module: `./build/test --gtest_filter="Storage*"`
- Testing the table class: `./build/test --gtest_filter="StorageTableTest*"`
- Testing the RowCount test: `./build/test --gtest_filter="StorageTableTest.RowCount"`

