#!/usr/bin/env python3

# For a build folder that was compiled with ccache and the environment variable CCACHE_DEBUG=1, this prints the number
# of compiled files and the ccache miss ratio. This helps in verifying that ccache is working as expected, especially
# when using unity builds and/or precompiled headers.

from pathlib import Path
import os
import re
import sys

files = {}
for filename in Path('src').rglob('*.ccache-log'):
  with open(filename, 'r') as file:
    try:
      for line in file:
        source_file_match = re.findall(r'Source file: (.*)', line)
        if source_file_match:
          source_file = source_file_match[0]
        result_match = re.findall(r'Result: cache (.*)', line)
        if result_match:
          result = result_match[0]
          files[source_file] = result
          break
    except UnicodeDecodeError:
      print("UnicodeDecodeError in %s" % (filename))
      continue

if len(files) == 0:
  print("No *.ccache-log files found. Did you compile with ccache and the environment variable CCACHE_DEBUG=1?")
  sys.exit(1)

common_path_prefix = os.path.commonprefix(list(files.keys()))
files_shortened = {}

misses = 0
for file in files:
  shortened = file.replace(common_path_prefix, '')
  if files[file] == 'miss':
    misses += 1
    print("ccache miss: %s" % (shortened))
  else:
    print("ccache hit: %s" % (shortened))

print("\n=== %i files, %i cache misses (%f %%) ===\n" % (len(files), misses, float(misses) / len(files) * 100))
