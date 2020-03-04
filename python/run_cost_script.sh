#!/bin/bash

python3 cost_models.py -train ../data/train/TableScan.csv ../data/train/table_meta.csv ../data/train/column_meta.csv ../data/train/segment_meta.csv --test ../data/test/TableScan.csv ../data/test/table_meta.csv ../data/test/column_meta.csv ../data/test/segment_meta.csv
