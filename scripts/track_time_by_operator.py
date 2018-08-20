#!/usr/bin/env python

import sys
import subprocess

command = "sudo stap tracer_scripts/query_analyzer.stp -DMAXMAPENTRIES=10000 -DMAXACTION=100000000 -G debug=0 -c \"{}\" {}".format(' '.join(sys.argv[1:]), sys.argv[1])
process = subprocess.Popen(command, shell=True)

process.communicate()
