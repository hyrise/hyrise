#include "insert_limit_in_exists.hpp"

#include "expression/exists_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

std::string InsertLimitInExistsRule::name() const { return "Insert Limit in Exists Rule"; }

void InsertLimitInExistsRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
    _apply_to_expressions({predicate_node->predicate()});
  } else if (const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node)) {
    _apply_to_expressions(projection_node->column_expressions());
  }
  _apply_to_inputs(node);
}

void InsertLimitInExistsRule::_apply_to_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const {
  for (auto& expression : expressions) {
    if (auto exists_node = std::dynamic_pointer_cast<ExistsExpression>(expression)) {
      auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(exists_node->select());
      const auto lqp = select_expression->lqp;
      if (lqp->type != LQPNodeType::Limit) {
        int64_t num_rows = 1;
        const auto num_rows_expression = std::make_shared<ValueExpression>(num_rows);
        select_expression->lqp = LimitNode::make(num_rows_expression, lqp);
      }
    } else if (auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(expression)) {
      apply_to(select_expression->lqp);
    }
    _apply_to_expressions(expression->arguments);
  }
}

}  // namespace opossum
