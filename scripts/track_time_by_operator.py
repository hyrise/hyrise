#!/usr/bin/env python

import sys
import subprocess
import os

if(len(sys.argv) == 1):
    print("Example usage: " + sys.argv[0] + " hyriseBenchmarkTPCH -r 10 -s 1")
    exit()

# We need to store some values in associative arrays. Those cannot be initialized dynamically, due to memory limitations.
# Choosing a high threshold may result a high memory allocation time at session startup. Script will crash if limit exceeded.
MAXMAPENTRIES = os.environ.get("MAXMAPENTRIES")
if not MAXMAPENTRIES:
    print("MAXMAPENTRIES not set, using default of 10000")
    print("run \"export MAXMAPENTRIES=<VALUE>\" to increase the maximum map size (maximum is 2^32)")
    MAXMAPENTRIES = 10000

# Start a SystemTap session of the query analyzer script. Therefore, the specified binary will be
# executed considering the given parameters.
command = "sudo stap tracer_scripts/query_analyzer.stp -DMAXMAPENTRIES={} -DMAXACTION=100000000 -G debug=0 -c \"{}\" {}".format(MAXMAPENTRIES, ' '.join(sys.argv[1:]), sys.argv[1])
process = subprocess.Popen(command, shell=True)

process.communicate()
