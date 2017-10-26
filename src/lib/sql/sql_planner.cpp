#include "sql/sql_planner.hpp"

#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

SQLQueryPlan SQLPlanner::plan(const hsql::SQLParserResult& result, bool validate) {
  // Translate to AST
  auto result_nodes = SQLToASTTranslator{validate}.translate_parse_result(result);

  SQLQueryPlan plan{};

  for (const auto& node : result_nodes) {
    auto optimized = Optimizer::get().optimize(node);
    auto op = ASTToOperatorTranslator{}.translate_node(optimized);

    plan.add_tree_by_root(op);
  }

  return plan;
}
}  // namespace opossum
