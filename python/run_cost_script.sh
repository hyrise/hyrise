#!/bin/bash

python3 cost_models.py -train data/data/train/TableScan.csv data/data/train/table_meta.csv data/data/train/column_meta.csv data/data/train/segment_meta.csv --test data/data/test/TableScan.csv data/data/test/table_meta.csv data/data/test/column_meta.csv data/data/test/segment_meta.csv
