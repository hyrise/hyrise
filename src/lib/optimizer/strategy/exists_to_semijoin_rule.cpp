#include "exists_to_semijoin_rule.hpp"

#include <unordered_map>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ExistsToSemijoinRule::name() const { return "Exists to Semijoin Rule"; }

bool ExistsToSemijoinRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // find a predicate
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  DebugAssert(predicate_node->predicate->type == ExpressionType::Predicate,
              "Expected predicate node to have predicate expression as predicate");

  const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate);
  if (predicate_expression->predicate_condition != PredicateCondition::Equals ||
      predicate_expression->arguments[0]->type != ExpressionType::Exists ||
      *predicate_expression->arguments[1] != ValueExpression{0}) {  // TODO: also deal with exists?
    return _apply_to_inputs(node);
  }

  const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_expression->arguments[0]);
  const auto subselect = std::static_pointer_cast<LQPSelectExpression>(exists_expression->select());

  if (subselect->parameter_count() == 0) {
    // We don't care about uncorrelated subselects
    return _apply_to_inputs(node);
  }

  // Find the projection that executes the subselect
  // We assume that the projection node is right below the predicate. This might change in the future.
  if (predicate_node->left_input()->type != LQPNodeType::Projection) {
    return _apply_to_inputs(node);
  }
  const auto projection_node = std::static_pointer_cast<ProjectionNode>(predicate_node->left_input());

  auto join_predicate = std::shared_ptr<AbstractExpression>();
  visit_lqp(subselect->lqp, [&join_predicate, &subselect](const auto& subselect_node) {
    // Play it safe. We do not know how to handle complicated LQPs here
    if (subselect_node->type != LQPNodeType::Predicate && subselect_node->type != LQPNodeType::Validate &&
        subselect_node->type != LQPNodeType::StoredTable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (subselect_node->type == LQPNodeType::Predicate) {
      const auto subselect_predicate_node = std::static_pointer_cast<PredicateNode>(subselect_node);
      const auto subselect_predicate_expression =
          std::static_pointer_cast<AbstractPredicateExpression>(subselect_predicate_node->predicate);

      if (subselect_predicate_expression->predicate_condition != PredicateCondition::Equals) {
        // Semi/Anti Joins are currently only implemented by the hash join, which only supports equal predicates
        return LQPVisitation::VisitInputs;
      }

      // Now check if one side of the predicate is a column "outside" the subselect and the other inside
      auto inner_column_expression = std::shared_ptr<LQPColumnExpression>();
      auto parameter_expression = std::shared_ptr<ParameterExpression>();

      if (subselect_predicate_expression->arguments[0]->type == ExpressionType::LQPColumn &&
          subselect_predicate_expression->arguments[1]->type == ExpressionType::Parameter) {
        inner_column_expression =
            std::static_pointer_cast<LQPColumnExpression>(subselect_predicate_expression->arguments[0]);
        parameter_expression =
            std::static_pointer_cast<ParameterExpression>(subselect_predicate_expression->arguments[1]);
      } else if (subselect_predicate_expression->arguments[0]->type == ExpressionType::Parameter &&
                 subselect_predicate_expression->arguments[1]->type == ExpressionType::LQPColumn) {
        parameter_expression =
            std::static_pointer_cast<ParameterExpression>(subselect_predicate_expression->arguments[0]);
        inner_column_expression =
            std::static_pointer_cast<LQPColumnExpression>(subselect_predicate_expression->arguments[1]);
      } else {
        return LQPVisitation::VisitInputs;
      }

      // We found the condition.
      DebugAssert(subselect->arguments[parameter_expression->parameter_id]->type == ExpressionType::LQPColumn,
                  "Expected LQP Column");
      const auto outer_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(subselect->arguments[parameter_expression->parameter_id]);
      join_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, outer_column_expression,
                                                                   inner_column_expression);

      if(subselect->lqp == subselect_predicate_node) {
        subselect->lqp = subselect_predicate_node->left_input();
      } else {
        lqp_remove_node(subselect_predicate_node);
      }

      // TODO: once found, make sure that there is no second parameter left in LQP
    }

    return LQPVisitation::VisitInputs;
  });

  const auto join_node = std::make_shared<JoinNode>(JoinMode::Anti, join_predicate);
  lqp_replace_node(predicate_node, join_node);
  join_node->set_right_input(subselect->lqp);

  // Finally, remove the subselect from the projection
  std::cout << "VORDAS:" << std::endl;
  for(auto e : projection_node->expressions) std::cout << " - " << e->as_column_name() << std::endl;
  projection_node->print();
  projection_node->expressions.erase(std::remove(projection_node->expressions.begin(), projection_node->expressions.end(), exists_expression), projection_node->expressions.end());
  std::cout << "NACHDAS:" << std::endl;
  for(auto e : projection_node->expressions) std::cout << " - " << e->as_column_name() << std::endl;
  projection_node->print();

  return true;
}

}  // namespace opossum
