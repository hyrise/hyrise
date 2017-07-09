#!/bin/sh

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

./$1/tpccTableGenerator
python3 src/scripts/tpcc_request_generator.py test 150
python3 src/scripts/tpcc_sqlite_driver.py test
./$1/opossumTestTPCC