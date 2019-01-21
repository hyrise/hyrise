#include <iostream>

#include "types.hpp"
#include "expression/expression_functional.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/plan_to_cpp.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT

int main() {
  TpchTableGenerator{0.01f}.generate_and_store();
  auto query = TPCHQueryGenerator{false, 0.01f}.build_deterministic_query(QueryID{6});
  query = "SELECT (SELECT MIN(o_totalprice) FROM orders WHERE l_orderkey = o_orderkey) FROM lineitem;";

  auto statement = SQLPipelineBuilder{query}.create_pipeline_statement();

  statement.get_optimized_logical_plan()->print();

  std::cout << lqp_to_cpp(statement.get_optimized_logical_plan()) << std::endl;

  return 0;
}
