#include "sql/sql_planner.hpp"

#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_translator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

SQLQueryPlan SQLPlanner::plan(const hsql::SQLParserResult& result, bool validate) {
  // Translate to LQP
  auto result_nodes = SQLTranslator{validate}.translate_parse_result(result);

  SQLQueryPlan plan{};

  for (const auto& node : result_nodes) {
    auto optimized = Optimizer::create_default_optimizer()->optimize(node);
    auto op = LQPTranslator{}.translate_node(optimized);

    plan.add_tree_by_root(op);
  }

  return plan;
}
}  // namespace opossum
