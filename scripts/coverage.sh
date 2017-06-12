#!/bin/sh

./$1/opossumCoverage && rm -fr coverage; mkdir coverage && gcovr -s -r . --exclude="(.*types*.|.*test*.|.*\.pb\.|third_party)" --html --html-details -o coverage/index.html
