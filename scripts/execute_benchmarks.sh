#!/bin/bash

# Sequential: Measure
mkdir -p sequential
./hyriseBenchmarkTPCH --encoding BitCompression_config.json --output sequential/BitCompression_result_sequential.json  -r 100 --dont_cache_binary_tables
./hyriseBenchmarkTPCH --encoding TurboPFOR_encoding.json --output sequential/TurboPFOR_result_sequential.json  -r 100 --dont_cache_binary_tables
./hyriseBenchmarkTPCH --encoding Dictionary_encoding.json --output sequential/Dictionary_result_sequential.json -r 100 --dont_cache_binary_tables

# Sequential: Visualize
mkdir -p sequential/vis_turbo
mkdir -p sequential/vis_dict
mkdir -p sequential/vis_bitcomp
./hyriseBenchmarkTPCH --encoding TurboPFOR_encoding.json --visualize --dont_cache_binary_tables
mv *.svg sequential/vis_turbo/
./hyriseBenchmarkTPCH --encoding Dictionary_encoding.json --visualize --dont_cache_binary_tables
mv *.svg sequential/vis_dict
./hyriseBenchmarkTPCH --encoding BitCompression_config.json --visualize --dont_cache_binary_tables
mv *.svg sequential/vis_bitcomp

exec bash