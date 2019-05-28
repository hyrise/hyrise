#!/bin/bash

# This script creates a very small sample from the imdb database downloaded during the setup of hyriseBenchmarkJoinOrder
# to avoid downloading it for every new test environment and allowing fast testing
rm -rf imdb_sample
mkdir imdb_sample

# These rows are picked to allow a verification of query 3b in the hyriseBenchmarkJoinOrder_test.py
tail -n 1                             imdb_data/aka_name.csv          > imdb_sample/aka_name.csv
tail -n 1                             imdb_data/aka_title.csv         > imdb_sample/aka_title.csv
tail -n 1                             imdb_data/cast_info.csv         > imdb_sample/cast_info.csv
tail -n 1                             imdb_data/char_name.csv         > imdb_sample/char_name.csv
tail -n 1                             imdb_data/company_name.csv      > imdb_sample/company_name.csv
tail -n 1                             imdb_data/company_type.csv      > imdb_sample/company_type.csv
tail -n 1                             imdb_data/comp_cast_type.csv    > imdb_sample/comp_cast_type.csv
tail -n 1                             imdb_data/complete_cast.csv     > imdb_sample/complete_cast.csv
tail -n 1                             imdb_data/info_type.csv         > imdb_sample/info_type.csv
grep -w 1136                          imdb_data/keyword.csv           > imdb_sample/keyword.csv
tail -n 1                             imdb_data/kind_type.csv         > imdb_sample/kind_type.csv
tail -n 1                             imdb_data/link_type.csv         > imdb_sample/link_type.csv
tail -n 1                             imdb_data/movie_companies.csv   > imdb_sample/movie_companies.csv
grep -r -m 1 3292787.*\"Bulgaria\"    imdb_data/movie_info.csv        > imdb_sample/movie_info.csv
tail -n 1                             imdb_data/movie_info_idx.csv    > imdb_sample/movie_info_idx.csv
grep -w 3292787,1136                  imdb_data/movie_keyword.csv     > imdb_sample/movie_keyword.csv
tail -n 1                             imdb_data/movie_link.csv        > imdb_sample/movie_link.csv
tail -n 1                             imdb_data/name.csv              > imdb_sample/name.csv
tail -n 1                             imdb_data/person_info.csv       > imdb_sample/person_info.csv
tail -n 1                             imdb_data/role_type.csv         > imdb_sample/role_type.csv
grep -w 3292787                       imdb_data/title.csv             > imdb_sample/title.csv

cp imdb_data/*.csv.json          			imdb_sample/
