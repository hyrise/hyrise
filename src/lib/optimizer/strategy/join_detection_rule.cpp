#include "join_detection_rule.hpp"

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/binary_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string JoinDetectionRule::name() const { return "Join Detection Rule"; }

bool JoinDetectionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type == LQPNodeType::Join) {
    // ... "potential"_cross_join_node until this if below
    auto cross_join_node = std::dynamic_pointer_cast<JoinNode>(node);
    if (cross_join_node->join_mode == JoinMode::Cross) {
      /**
       * If we find a predicate with a condition that operates on the cross-joined tables,
       * replace the cross join and the predicate with a conditional inner join
       */
      const auto predicate_node = _find_predicate_for_cross_join(cross_join_node);
      if (predicate_node) {
        const auto new_join_node = JoinNode::make(JoinMode::Inner, predicate_node->predicate);

        /**
         * Place the conditional join where the cross join was and remove the predicate node
         */
        lqp_replace_node(cross_join_node, new_join_node);
        lqp_remove_node(predicate_node);

        return true;
      }
    }
  }

  return _apply_to_inputs(node);
}

std::shared_ptr<PredicateNode> JoinDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<JoinNode>& cross_join) const {
  Assert(cross_join->left_input() && cross_join->right_input(), "Cross Join must have two inputs");

  // Go up in LQP to find corresponding PredicateNode
  std::shared_ptr<AbstractLQPNode> node = cross_join;
  while (true) {
    const auto outputs = node->outputs();

    /**
     * Can't deal with the outputs.size() > 0 case - would need to check that a potential predicate exists in all
     * outputs and this is too much work considering the JoinDetectionRule will be removed soon-ish (TM)
     */
    if (outputs.empty() || outputs.size() > 1) {
      break;
    }

    node = outputs[0];

    /**
     * TODO(anyone)
     * Right now we only support traversing past nodes that do not change the column order and to be 100% safe
     * we make this explicit by only traversing past Joins, Projection and Predicates
     *
     * Detecting Join Conditions across other node types may be possible by applying 'Predicate Pushdown' first.
     */
    if (node->type != LQPNodeType::Join && node->type != LQPNodeType::Predicate &&
        node->type != LQPNodeType::Projection) {
      return nullptr;
    }

    if (node->type == LQPNodeType::Predicate) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate);

      if (!binary_predicate) continue;

      const auto left_operand = binary_predicate->left_operand();
      const auto right_operand = binary_predicate->right_operand();

      /**
       * We have a (Cross)JoinNode and PredicateNode located further up in the tree. Now we have to determine whether
       * and how they can be merged to a normal Join.
       * More precisely, we have to determine which columns of the cross joins input tables correspond to the columns
       * used in the predicate.
       */
      const auto left_in_left = cross_join->left_input()->find_column_id(*left_operand);
      const auto right_in_right = cross_join->right_input()->find_column_id(*right_operand);

      if (left_in_left && right_in_right) {
        return predicate_node;
      }

      const auto left_in_right = cross_join->right_input()->find_column_id(*left_operand);
      const auto right_in_left = cross_join->left_input()->find_column_id(*right_operand);

      if (right_in_left && left_in_right) {
        return predicate_node;
      }
    }
  }

  return nullptr;
}

}  // namespace opossum
