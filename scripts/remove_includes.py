#!/usr/local/bin/python3

# TODO: Comments
# TODO: Add dry run (check if it loops)
# TODO: Verify dummy.cpp added
# TODO: Verify that running with NUMA and JIT

import fileinput
import multiprocessing
import os
import re
import subprocess
import sys

def test_build():
    # First, try if the modified file itself still works
    build = subprocess.Popen("cd build; ninja src/lib/CMakeFiles/hyrise.dir/dummy.cpp.o", shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    build.communicate()
    if build.returncode != 0:
        return False

    build = subprocess.Popen("cd build; ninja -j %i hyrise" % (multiprocessing.cpu_count() - 1), shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    build.communicate()
    return build.returncode == 0

def file_change_include(path, include, action):
    for line in fileinput.input(path, inplace=1):
        if '#include' in line and include in line:
            if 'CHECKINCLUDE' in line:
                if action == 'remove':
                    continue
                elif action == 'putback':
                    print(line[16:-1] + ' // NEEDEDINCLUDE\n', end='')
                else:
                    assert(False)
            else:
                assert(action == 'comment')
                print('// CHECKINCLUDE ' + line, end='')
        else:
            print(line, end='')

print("Check initial build")
assert test_build()

cleaned_user_files = set()

files = []
for (dirpath, dirnames, filenames) in os.walk('src/lib'):
    for filename in filenames:
        path = os.path.join(dirpath, filename)
        user_includes = set()
        user_includes_full = set()
        system_includes = set()
        with open(path) as file:
            for line in file:
                if 'CHECKINCLUDE' in line:
                    print("Remaining CHECKINCLUDE from last found in file %s, aborting" % (path))
                    sys.exit(1)
                if line.startswith('#include "') and not 'NEEDEDINCLUDE' in line:
                    include = line.split(line[9])[1]
                    if include.endswith('h') or include.split('/')[0] in ['llvm', 'sql-parser', 'boost', 'json', 'cqf', 'uninitialized_vector.hpp', 'Expr.h', 'SQLStatement.h', 'json.hpp', 'SQLParser.h', 'SQLParserResult.h', 'tbb', 'provider.hpp', 'bytell_hash_map.hpp', 'SelectStatement.h'] or include[0:3] == 'cqf':
                        system_includes.add(line[10:-2])
                        continue

                    user_includes.add(include.split('/')[-1])
                    user_includes_full.add(include)
                if line.startswith('#include <' and not 'NEEDEDINCLUDE' in line):
                    include = line[10:line.find('>')]
                    system_includes.add(include)
        files += [{'path' : path, 'user_includes' : user_includes, 'user_includes_full' : user_includes_full, 'system_includes' : system_includes}]
        if not user_includes:
            cleaned_user_files.add(path)

files.sort(key = lambda file: len(file['user_includes']))

while len(files) > 0:
    file = files[0]

    unchecked = file['user_includes'] - cleaned_user_files
    if (unchecked):
        print ("Skipping %s because %s has not been checked yet" % (file['path'], next(iter(unchecked))))
        files += [file]
        files.pop(0)
        cleaned_user_files.add(file['path'].split('/')[-1])
        continue
    
    for include in list(file['user_includes']) + list(file['system_includes']):
        print("Trying to remove %s from %s" % (include, file['path']))
        file_change_include(file['path'], include, 'comment')

        with open('src/lib/dummy.cpp', 'w') as f: 
            f.write('#include "%s"' % file['path'].replace('src/lib/', ''))

        if test_build():
            print("\twas not needed - removed")
            file_change_include(file['path'], include, 'remove')
        else:
            print("\twas needed - put back")
            file_change_include(file['path'], include, 'putback')

    cleaned_user_files.add(file['path'].split('/')[-1])
    files.pop(0)