#!/usr/bin/env python3

# Takes a FileIO benchmark output JSON and plots ...
#

import json
import sys

if len(sys.argv) != 2:
    sys.exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as json_file:
    data_json = json.load(json_file)
    print(data_json)