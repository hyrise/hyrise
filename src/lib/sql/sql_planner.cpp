#include "sql/sql_planner.hpp"

#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

SQLQueryPlan SQLPlanner::plan(const hsql::SQLParserResult& result) {
  // Translate to AST
  auto result_nodes = SQLToASTTranslator::get().translate_parse_result(result);

  SQLQueryPlan plan;

  for (const auto& node : result_nodes) {
    // Call optimizer
    auto optimized = Optimizer::optimize(node);

    // Translate to operators
    auto op = ASTToOperatorTranslator::get().translate_node(optimized);

    plan.add_tree_by_root(op);
  }

  return plan;
}
}  // namespace opossum
