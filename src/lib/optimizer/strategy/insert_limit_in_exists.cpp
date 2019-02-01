#include "insert_limit_in_exists.hpp"

#include "expression/exists_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string InsertLimitInExistsRule::name() const { return "Insert Limit in Exists Expression Rule"; }

void InsertLimitInExistsRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  visit_lqp(node, [&](const auto& sub_node) {
    // Iterate over all expressions of a lqp node
    for (const auto& expression : sub_node->node_expressions) {
      // Recursively iterate over each nested expression
      visit_expression(expression, [&](const auto& sub_expression) {
        // Apply rule for every subquery
        if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression)) {
          apply_to(subquery_expression->lqp);
        }

        // Add limit to exists subquery
        if (const auto exists_expression = std::dynamic_pointer_cast<ExistsExpression>(sub_expression)) {
          const auto subquery_expression =
              std::dynamic_pointer_cast<LQPSubqueryExpression>(exists_expression->subquery());
          const auto& lqp = subquery_expression->lqp;
          if (lqp->type != LQPNodeType::Limit) {
            const auto num_rows_expression = std::make_shared<ValueExpression>(int64_t{1});
            subquery_expression->lqp = LimitNode::make(num_rows_expression, lqp);
          }
        }

        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
