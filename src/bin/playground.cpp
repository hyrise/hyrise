
#include <iostream>

#include "expression/correlated_parameter_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto tables = TpchTableGenerator{0.01f}.generate();
  for (auto [name, info] : tables) {
    StorageManager::get().add_table(name, info.table);
  }

  const auto sql =
      "SELECT * FROM customer WHERE c_custkey IN (SELECT * FROM orders WHERE EXISTS (SELECT s_suppkey FROM supplier "
      "WHERE s_suppkey = c_custkey))";
  const auto lqps = SQLPipelineBuilder{sql}.create_pipeline().get_unoptimized_logical_plans();

  lqps[0]->print();

  const auto in_predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqps[0]);
  const auto in_predicate = in_predicate_node->predicate();
  std::shared_ptr<LQPSubqueryExpression> lqp_subquery_expression;
  visit_expression(in_predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::LQPSubquery) {
      lqp_subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(expression);
    }
    return ExpressionVisitation::VisitArguments;
  });

  const auto exists_predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp_subquery_expression->lqp);
  const auto exists_predicate = exists_predicate_node->predicate();
  visit_expression(exists_predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::CorrelatedParameter) {
      const auto correlated_parameter_expression = std::static_pointer_cast<CorrelatedParameterExpression>(expression);
      std::cout << "Found correlated parameter expression with parameter id: "
                << correlated_parameter_expression->parameter_id << '\n';
    }
    return ExpressionVisitation::VisitArguments;
  });

  return 0;
}