#include <iostream>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "types.hpp"
#include "visualization/lqp_visualizer.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;

int main() {
  const auto histogram = GenericHistogram<int32_t>{{0}, {29}, {5}, {5}};

  std::cout.precision(std::numeric_limits<float>::max_digits10);

  ////  std::cout << histogram.estimate_cardinality(PredicateCondition::Between, 10, 15).cardinality << std::endl;
  //  std::cout << histogram.estimate_distinct_count(PredicateCondition::Between, 10, 15) << std::endl;
  std::cout << histogram.estimate_distinct_count(PredicateCondition::LessThan, 16) << std::endl;
  //  std::cout << histogram.estimate_distinct_count(PredicateCondition::LessThan, 10) << std::endl;

  return 0;
}
