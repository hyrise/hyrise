#include "sql/sql_planner.hpp"

#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

SQLQueryPlan SQLPlanner::plan(const hsql::SQLParserResult& result,
                              const std::shared_ptr<TransactionContext>& transaction_context) {
  // Translate to AST
  auto result_nodes = SQLToASTTranslator::get().translate_parse_result(result);

  ASTToOperatorTranslator translator{transaction_context};
  SQLQueryPlan plan{};

  for (const auto& node : result_nodes) {
    auto optimized = Optimizer::optimize(node);
    auto op = translator.translate_node(optimized);

    plan.add_tree_by_root(op);
  }

  return plan;
}
}  // namespace opossum
