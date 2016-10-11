# opossum

*Have a look at docs/guidelines*

## Dependencies
You can install the dependencies on your own or use the install.sh script which installs most of the following packages.

The install script currently works with macOS (brew) and Ubuntu 16.10 (apt-get)

### premake4
install via homebrew / packet manager

### boost (version: >= 1.61.0)
install via homebrew / packet manager

### boost hana
this is part of boost since version 1.61.0

In case you are using an older boost version, you have to build it from source:

http://www.boost.org/doc/libs/1_61_0/libs/hana/doc/html/index.html

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
`premake4 && make -j && clear && ./build/BinOpossum`

### lint (is also automatically triggerd before git commit)
`premake4 lint` (Google's cpplint is used which needs python 2.7)

### format (is also automatically triggered with make)
`premake4 format`

### testing (is also automatically triggered before git commit)
`premake4 test` executes all available tests

### coverage
`make -j coverage` will print a summary to the command line and create detailed html reports at ./coverage/index.html

*Supports only clang on MacOS and only gcc on linux*

## Naming convention for gtest macros:

TEST(module_name_class_name, test_case_description), e.g., TEST(operators_get_table, get_output_returns_correct_table)

If you want to test a single module, class or test case you have to execute the test binary and use the `gtest_filter` option:

- Testing the storage module: `./build/TestOpossum --gtest_filter="storage*"`
- Testing the table class: `./build/TestOpossum --gtest_filter="storage_table*"`
- Testing the has_one_chunk_after_creation case: `./build/TestOpossum --gtest_filter="storage_table.has_one_chunk_after_creation"`
