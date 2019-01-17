#include <iostream>

#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/plan_to_cpp.hpp"

using namespace opossum;  // NOLINT

int main() {
  TpchTableGenerator{0.01f}.generate_and_store();
  auto query = TPCHQueryGenerator{false, 0.01f}.build_deterministic_query(QueryID{6});
  query = "SELECT * FROM lineitem, supplier WHERE s_suppkey = l_suppkey AND l_shipdate > 'than_ever'";

  auto statement = SQLPipelineBuilder{query}.create_pipeline_statement();

  statement.get_optimized_logical_plan()->print();

  std::cout << lqp_to_cpp(statement.get_optimized_logical_plan()) << std::endl;

  return 0;
}
