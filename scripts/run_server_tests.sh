#!/usr/bin/env bash

# This script requires psql to be pre-installed

SUCCEEDED=0
FAILED=0


RUN_TEST () {
    ERRORS=$(psql -h localhost -p 5432 -qAX -c "$2" 2>&1)
    # validate that result starts with header row
    if [[ $? -eq 0 ]]; then
        SUCCEEDED=$((SUCCEEDED+1))
    else
        FAILED=$((FAILED+1))
        echo -e "Test $1 failed due to: \n$ERRORS\n"
    fi
}

# start server

echo -e "Starting to run server tests...\n"

# TEST 1: create table
RUN_TEST "CREATE TABLE" "CREATE TABLE TEST (id char(5) PRIMARY KEY, name VARCHAR(10));"

# TEST 2: simple select statement
RUN_TEST "SIMPLE QUERY" "SELECT * FROM ITEM LIMIT 2"

# TEST 3: complex statement 1
# ...

# stop server

# print results
echo "Finished running server tests"
echo "$SUCCEEDED out of $((SUCCEEDED+FAILED)) tests finished successful..."