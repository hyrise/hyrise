#!/usr/bin/env python3

import os
import re
import sys
import shutil

OUTPUT_FOLDER_NAME = 'query_log_output'

filename = sys.argv[1]

# we always drop and recreate the output folder
shutil.rmtree(OUTPUT_FOLDER_NAME, ignore_errors=True)
os.makedirs(OUTPUT_FOLDER_NAME)

with open(filename) as file:
    # let's not think about efficiency here. Logs should not be huge.
    content = file.read()

    # tiny bit of clean up
    content = content[content.find('$$$$$$QUERYNAME:'):]
    content = content[:content.rfind('####/SQL_QUERY####') + len('####/SQL_QUERY####')]
    
    for query in content.split('$$$$$$QUERYNAME:'):
        # print(query)

        query_name = query[:query.find('\n')]
        query_regex = re.search('####SQL_QUERY####(.*)####/SQL_QUERY####', query, re.DOTALL)
        if query_regex:
            query = query_regex.group(1).strip()

            query_file = open(f'{OUTPUT_FOLDER_NAME}/{query_name}.sql', 'a')
            if query[-1] != ';':
                query += ';' # append semicolon if not present in query
            query_file.write(query + '\n')

        # last_char_is_semicolon = query[-1] == ';'
