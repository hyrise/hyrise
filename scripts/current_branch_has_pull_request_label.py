#!/usr/bin/env python

# This is used by Jenkins - see Jenkinsfile

import json
import subprocess
import sys
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

if (len(sys.argv) != 2):
	print("Usage: %s label" % (sys.argv[0]))
	print("Returns whether the current commit belongs to a Github Pull Request that has a certain label")
	sys.exit(-1)

prs = json.loads(urlopen('https://api.github.com/repos/hyrise/hyrise/pulls').read())

process = subprocess.Popen(['git', 'rev-parse', 'HEAD'], shell=False, stdout=subprocess.PIPE)
git_head_hash = process.communicate()[0].strip()

for pr in prs:
	if (pr['head']['sha'] != git_head_hash.decode("utf-8")):
		continue

	pr_details = json.loads(urlopen('https://api.github.com/repos/hyrise/hyrise/pulls/%i' % (pr['number'])).read())

	for label in pr_details['labels']:
		if(label['name'] == sys.argv[1]):
			print("true")
			sys.exit(0)
	else:
		print("false")
		sys.exit(0)

else:
	print("No PR for current commit %s found" % (git_head_hash))