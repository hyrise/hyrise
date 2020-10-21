#!/bin/bash

set -e

SCRIPTS_DIR=${BASH_SOURCE[0]%/*}

# DBGEN=/Users/markus/Projekte/Opossum/Git/third_party/jcch-dbgen
# SCALE=0.1
# ln -s ${DBGEN}/dists.dss || true

# Generate tables
# mkdir -p sf-${SCALE}/tables
# cd sf-${SCALE}/tables

# ${DBGEN}/dbgen -k -s ${SCALE}

# mv customer.tbl customer.csv
# mv lineitem.tbl lineitem.csv
# mv nation.tbl nation.csv
# mv orders.tbl orders.csv
# mv part.tbl part.csv
# mv partsupp.tbl partsupp.csv
# mv region.tbl region.csv
# mv supplier.tbl supplier.csv

# ln -s ../../customer.csv.json
# ln -s ../../lineitem.csv.json
# ln -s ../../nation.csv.json
# ln -s ../../orders.csv.json
# ln -s ../../part.csv.json
# ln -s ../../partsupp.csv.json
# ln -s ../../region.csv.json
# ln -s ../../supplier.csv.json

# sed -e 's/\|$//' -i '' customer.csv
# sed -e 's/\|$//' -i '' lineitem.csv
# sed -e 's/\|$//' -i '' nation.csv
# sed -e 's/\|$//' -i '' orders.csv
# sed -e 's/\|$//' -i '' part.csv
# sed -e 's/\|$//' -i '' partsupp.csv
# sed -e 's/\|$//' -i '' region.csv
# sed -e 's/\|$//' -i '' supplier.csv

# cd -

# # Generate queries
# mkdir -p sf-${SCALE}/queries
# cd sf-${SCALE}/queries

# ln -s ${DBGEN}/queries/*.sql .

# for n in {1..1}
# do
# 	${DBGEN}/qgen -k -s ${SCALE} -b ../../dists.dss -r $n -l params_unsorted # > /dev/null
# done

# sort params_unsorted | uniq > params
# rm params_unsorted

# rm *.sql

# cd -

# rm dists.dss
