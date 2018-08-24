#!/usr/bin/env python

import sys
import subprocess
import os

MAXMAPENTRIES = os.environ.get("MAXMAPENTRIES")
if not MAXMAPENTRIES:
    print("MAXMAPENTRIES not set, using default of 10000")
    print("run \"export MAXMAPENTRIES=<VALUE>\" to increase the maximum map size (maximum is 2^32)")
    MAXMAPENTRIES = 10000


command = "sudo stap tracer_scripts/query_analyzer.stp -DMAXMAPENTRIES={} -DMAXACTION=100000000 -G debug=0 -c \"{}\" {}".format(MAXMAPENTRIES, ' '.join(sys.argv[1:]), sys.argv[1])
process = subprocess.Popen(command, shell=True)

process.communicate()
