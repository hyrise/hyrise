# opossum

*Have a look at docs/guidelines*

## build
premake4 gmake -&& make -j && clear && ./build/Opossum

## lint (is also automatically triggerd before git commit)
premake4 lint

## format (is also automatically triggered before make)
premake4 format

# dependencies

## boost
install via homebrew / packet manager

## Compiler
install recent versions of compilers (clang >= 3.5.0, gcc >= 6.1), required by hana. Using homebrew, you might have to deinstall gcc61 and install again.

## boost hana
build from source: http://www.boost.org/doc/libs/1_61_0/libs/hana/doc/html/index.html

## clang-format
install via homebrew / packet manager
