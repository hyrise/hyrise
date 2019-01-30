#include "insert_limit_in_exists.hpp"

#include "expression/exists_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string InsertLimitInExistsRule::name() const { return "Insert Limit in Exists Expression Rule"; }

void InsertLimitInExistsRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  visit_lqp(node, [&](const auto sub_node) {
    // Iterate over all expressions of a lqp node
    for (auto expression : sub_node->node_expressions) {
      visit_expression(expression, [&](const auto sub_expression) {
        if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression)) {
          this->apply_to(select_expression->lqp);
        }
        return ExpressionVisitation::VisitArguments;
      });

      if (auto exists_node = std::dynamic_pointer_cast<ExistsExpression>(expression)) {
        // Add limit to exists subquery
        auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(exists_node->select());
        const auto lqp = select_expression->lqp;
        if (lqp->type != LQPNodeType::Limit) {
          int64_t num_rows = 1;
          const auto num_rows_expression = std::make_shared<ValueExpression>(num_rows);
          select_expression->lqp = LimitNode::make(num_rows_expression, lqp);
        }
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
