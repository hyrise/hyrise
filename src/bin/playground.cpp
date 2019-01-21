#include <iostream>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "visualization/lqp_visualizer.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;

int main() { return 0; }
