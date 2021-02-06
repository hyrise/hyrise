#!/bin/bash

# Sequential: Measure
mkdir -p sequential
./hyriseBenchmarkTPCH --encoding ../encoding_bitcompression.json --output sequential/bitcompression_result_sequential.json  -r 100 --dont_cache_binary_tables
£

# Sequential: Visualize
mkdir -p sequential/vis_dict
mkdir -p sequential/vis_bitcomp
./hyriseBenchmarkTPCH --encoding ../encoding_bitcompression.json --visualize --dont_cache_binary_tables
mv *.svg sequential/vis_dict
./hyriseBenchmarkTPCH --encoding ../encoding_dictionary.json --visualize --dont_cache_binary_tables
mv *.svg sequential/vis_bitcomp

exec bash