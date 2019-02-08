#include <iostream>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;

int main() {
  const auto table = load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl");

  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{10});

  StringHistogramDomain domain{" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 6u};

  const auto histogram = EqualDistinctCountHistogram<std::string>::from_segment(segment, 6u, domain);

  std::cout << histogram->description(true) << std::endl;

}
