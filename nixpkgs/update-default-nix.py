import sys

f = open("default.nix", "r")
lines = f.readlines()

hash = sys.argv[1]
lines[11] = 'import (fetchCommit "' + hash + '") {\n'
f.close()

fw = open("default.nix", "w")
fw.writelines(lines)
