#!/bin/sh

python2.7 cpplint.py --verbose=3 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 src/**/*.{hpp,cpp}