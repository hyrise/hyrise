#!/bin/sh

find src -iname "*.cpp" -o -iname "*.hpp" | xargs -I{} sh -c "clang-format -i -style=file '{}'"