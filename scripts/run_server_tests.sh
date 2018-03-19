#!/usr/bin/env bash

# This test script can be used to test the Hyrise server interface with a real client. Testing only within C++ does not
# provide the full end2end-test-like experience :) This is basically just a check to see if the server starts correctly
# and can interact with clients. We cannot do this in our regular testing framework because it requires us to mock the
# actual network interface.
#
# usage: ./scripts/run_server_test.sh
#
# This script requires psql to be pre-installed

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

./$1/hyriseServer &
SERVER_PID="$!"

sleep 5

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

# TEST 1: load table
RUN_TEST "LOAD TABLE" "LOAD src/test/tables/int.tbl foo;"

# TEST 2: simple select statement
RUN_TEST "SIMPLE QUERY" "SELECT * FROM foo LIMIT 2;"

# TEST 3: complex statement
# ...

# stop server
kill $SERVER_PID

# print results
echo "Finished running server tests"
echo "$SUCCEEDED out of $((SUCCEEDED+FAILED)) tests finished successful..."