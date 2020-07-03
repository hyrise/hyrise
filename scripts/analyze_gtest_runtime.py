#!/usr/bin/env python3

# Takes the JSON output of googletest and prints information about the longest running tests and test suites

import json
import sys
from terminaltables import AsciiTable

if len(sys.argv) != 2:
    print('Usage: (1) Run googletest with --gtest_output="json:output.json"')
    print('       (2) " + sys.argv[0] + " output.json')
    sys.exit(1)

with open(sys.argv[1]) as f:
    data = json.load(f)

testsuites = {}
tests = {}

for testsuite in data["testsuites"]:
    testsuites[testsuite["name"]] = float(testsuite["time"].replace("s", ""))
    for test in testsuite["testsuite"]:
        tests[testsuite["name"] + "." + test["name"]] = float(test["time"].replace("s", ""))


testsuites_sorted = list({k: v for k, v in sorted(testsuites.items(), key=lambda item: -item[1])}.items())
tests_sorted = list({k: v for k, v in sorted(tests.items(), key=lambda item: -item[1])}.items())

ENTRIES_SHOWN = 20
table = []
table += [[str(ENTRIES_SHOWN) + " most expensive test suites", "s", str(ENTRIES_SHOWN) + " most expensive tests", "s"]]

for i in range(ENTRIES_SHOWN):
    table += [[testsuites_sorted[i][0], testsuites_sorted[i][1], tests_sorted[i][0], tests_sorted[i][1]]]

print(AsciiTable(table).table)
