#include <iostream>

#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

int main() {
  TpchTableGenerator{0.02f, 500}.generate_and_store();

  auto statement = SQLPipelineBuilder{"SELECT l_orderkey, l_quantity FROM lineitem;"}.dont_cleanup_temporaries().create_pipeline_statement();
  statement.get_result_table();

  const auto pqp = statement.get_physical_plan();

  PQPVisualizer{}.visualize({pqp}, "test.dot", "test.png");

  return 0;
}
