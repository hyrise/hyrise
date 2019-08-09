#!/usr/bin/python3

# This script is meant to be called by hyriseBenchmarkJoinOrder, but nothing stops you from calling it yourself.
# It downloads the IMDB used by the JoinOrderBenchmark and unzips it. We do this in Python and not in C++ because
# downloading and unzipping is straight forward in Python.

import hashlib
import os
import sys
import urllib.request
import zipfile


def clean_up(including_table_dir=False):
    if os.path.exists(FILE_NAME):
        os.remove(FILE_NAME)

    if including_table_dir and os.path.exists(table_dir):
        for file in os.listdir(table_dir):
            os.remove("./%s/%s" % (table_dir, file))
        os.rmdir(table_dir)


def is_setup():
    for table_name in TABLE_NAMES:
        if not os.path.exists(os.path.join(table_dir, table_name + ".csv")):
            return False
        if not os.path.exists(os.path.join(table_dir, table_name + ".csv.json")):
            return False

    return True


# [cmd, table_dir]
assert len(sys.argv) == 2
table_dir = sys.argv[1]

LOCATION = "https://www.dropbox.com/s/ckh4nyqpol70ri3/imdb.zip?dl=1"
FILE_NAME = "imdb.zip"
TABLE_NAMES = ["aka_name", "aka_title", "cast_info", "char_name", "company_name", "company_type", "comp_cast_type", "complete_cast", "info_type",
                    "keyword", "kind_type", "link_type", "movie_companies", "movie_info", "movie_info_idx", "movie_keyword", "movie_link", "name",
                    "person_info", "role_type", "title"]

print("- Retrieving the IMDB dataset.")

if is_setup():
    print("- IMDB setup already complete, no setup action required")
    sys.exit(0)

# We are going to calculate the md5 hash later, on-the-fly while downloading
hash_md5 = hashlib.md5()

url = urllib.request.urlopen(LOCATION)
meta = url.info()
file_size = int(meta['Content-Length'])

file = open(FILE_NAME, 'wb')

print("- Downloading: %s (%.2f GB)" % (FILE_NAME, file_size / 1000 / 1000 / 1000))

already_retrieved = 0
block_size = 8192
try:
    while True:
        buffer = url.read(block_size)
        if not buffer:
            break

        hash_md5.update(buffer)

        already_retrieved += len(buffer)
        file.write(buffer)
        status = r"- Retrieved %3.2f%% of the data" % (already_retrieved * 100. / file_size)
        status = status + chr(8) * (len(status) + 1)
        print(status, end='\r')
except:
    print("- Aborting. Something went wrong during the download. Cleaning up.")
    clean_up()
    sys.exit(1)

file.close()
print()
print("- Validating integrity...")

hash_dl = hash_md5.hexdigest()

if hash_dl != "79e4c71f8ec0dae17d6aa9182fdab835":
    print("  Aborting. MD5 checksum mismatch. Cleaning up.")
    clean_up()
    sys.exit(2)

print("- Downloaded file is valid.")
print("- Unzipping the file...")

try:
    zip = zipfile.ZipFile("imdb.zip", "r")
    zip.extractall(table_dir)
    zip.close()
except:
    print("- Aborting. Something went wrong during unzipping. Cleaning up.")
    clean_up(including_table_dir=True)
    sys.exit(3)

print("- Deleting the archive file.")
clean_up()
print("- imdb_setup.py ran sucessfully.")