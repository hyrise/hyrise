#!/bin/sh

python2.7 cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 $(find src -iname "*.cpp" -o -iname "*.hpp")
