#!/bin/bash

set -e

if [ $# -ne 2 ]
then
  echo 'This script is used to evaluate the quality of cardinality estimations of the current build.'
  echo 'It creates a cardinality estimation csv for each benchmark evaluating each node of the query with the following structure:'
  echo 'Benchmark Item ID ; LQP Node ; PQP Node ; LQP Row Count ; PQP Row Count'
  echo 'As the evaluation depends on the assumption that lqp and pqp will usually have the same order of nodes, additional debug information is printed to identify and manually correct cases where this is not the case.'
  echo 'For this a file with the number of nodes in LQP and PQP for each benchmark item, as well as the visualized plans of LQP and PQP are created and stored, too.'
  echo 'Typical call from the root folder: ./scripts/cardinality_estimations_all.sh  ./path/to/build_folder ./path/to/cardinality_information_folder'
  exit 1
fi

build_folder=$1
output_folder=$2

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder'

# Run benchmarks and store results and debug information in specified output folder
mkdir "${output_folder}" 2>/dev/null || true
for benchmark in $benchmarks
  do
      echo "Running cardinality estimation evaluation for $benchmark"
      ( "${build_folder}"/"$benchmark" -r 1 --visualize )

      mv "./benchmark_cardinality_estimation.csv" "${benchmark}_benchmark_cardinality_estimation.csv"
      mv "./benchmark_cardinality_estimation_debug_info.csv" "${benchmark}_benchmark_cardinality_estimation_debug_info.csv"

      mkdir "${output_folder}/$benchmark/" "${output_folder}/$benchmark/visualized_plans/"
      mv "${benchmark}_benchmark_cardinality_estimation.csv" "${benchmark}_benchmark_cardinality_estimation_debug_info.csv" "${output_folder}/$benchmark/"
      mv ./*.svg "${output_folder}/$benchmark/visualized_plans/" 
  done