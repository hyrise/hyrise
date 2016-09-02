# opossum

*Have a look at docs/guidelines*

## build
premake4 gmake -&& make -j && clear && ./build/BinOpossum

## lint (is also automatically triggerd before git commit)
premake4 lint

## format (is also automatically triggered with make)
premake4 format

## testing (is also automatically triggered before git commit)
premake4 test executes all available tests

Naming convention for gtest macros:
TEST(module_name_class_name, test_case_description), e.g., TEST(operators_get_table, get_output_returns_correct_table)

If you want to test a single module, class or test case you have to execute the test binary and use the `gtest_filter`.
- Testing the storage module: `./build/TestOpossum --gtest_filter="storage*"`
- Testing the table class: `./build/TestOpossum --gtest_filter="storage_table*"`
- Testing the has_one_chunk_after_creation case: `./build/TestOpossum --gtest_filter="storage_table.has_one_chunk_after_creation"`

# dependencies

## boost
install via homebrew / packet manager

## Compiler
install recent versions of compilers (clang >= 3.5.0, gcc >= 6.1), required by hana. Using homebrew, you might have to deinstall gcc61 and install again.

## boost hana
build from source: http://www.boost.org/doc/libs/1_61_0/libs/hana/doc/html/index.html

## clang-format
install via homebrew / packet manager

## googletest
get via git submodule update --init