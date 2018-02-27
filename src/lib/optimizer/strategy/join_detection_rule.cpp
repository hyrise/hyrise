#include "join_detection_rule.hpp"

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string JoinDetectionRule::name() const { return "Join Detection Rule"; }

bool JoinDetectionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() == LQPNodeType::Join) {
    // ... "potential"_cross_join_node until this if below
    auto cross_join_node = std::dynamic_pointer_cast<JoinNode>(node);
    if (cross_join_node->join_mode() == JoinMode::Cross) {
      /**
       * If we find a predicate with a condition that operates on the cross-joined tables,
       * replace the cross join and the predicate with a conditional inner join
       */
      auto join_condition = _find_predicate_for_cross_join(cross_join_node);
      if (join_condition) {
        LQPColumnReferencePair join_column_ids(join_condition->left_column_reference,
                                               join_condition->right_column_reference);

        auto predicate_node = join_condition->predicate_node;
        const auto new_join_node =
            JoinNode::make(JoinMode::Inner, join_column_ids, predicate_node->predicate_condition());

        /**
         * Place the conditional join where the cross join was and remove the predicate node
         */
        cross_join_node->replace_with(new_join_node);
        predicate_node->remove_from_tree();

        return true;
      }
    }
  }

  return _apply_to_inputs(node);
}

std::optional<JoinDetectionRule::JoinCondition> JoinDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<JoinNode>& cross_join) {
  Assert(cross_join->left_input() && cross_join->right_input(), "Cross Join must have two inputs");

  // Everytime we traverse a node which we're the right input of, the ColumnIDs a predicate needs to reference become
  // offset
  auto column_id_offset = 0;

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

    if (node->get_input_side(outputs[0]) == LQPInputSide::Right) {
      column_id_offset += outputs[0]->left_input()->output_column_count();
    }

    node = outputs[0];

    /**
     * TODO(anyone)
     * Right now we only support traversing past nodes that do not change the column order and to be 100% safe
     * we make this explicit by only traversing past Joins and Predicates
     *
     * Detecting Join Conditions across other node types may be possible by applying 'Predicate Pushdown' first.
     */
    if (node->type() != LQPNodeType::Join && node->type() != LQPNodeType::Predicate) {
      return std::nullopt;
    }

    if (node->type() == LQPNodeType::Predicate) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

      if (!is_lqp_column_reference(predicate_node->value())) {
        continue;
      }

      /**
       * We have a (Cross)JoinNode and PredicateNode located further up in the tree. Now we have to determine whether
       * and how they can be merged to a normal Join.
       * More precisely, we have to determine which columns of the cross joins input tables correspond to the columns
       * used in the predicate.
       */
      auto predicate_left_column_reference = predicate_node->column_reference();
      auto predicate_right_column_reference = boost::get<LQPColumnReference>(predicate_node->value());

      const auto left_in_left = cross_join->left_input()->find_output_column_id(predicate_left_column_reference);
      const auto right_in_right = cross_join->right_input()->find_output_column_id(predicate_right_column_reference);

      if (left_in_left && right_in_right) {
        return JoinCondition{predicate_node, predicate_left_column_reference, predicate_right_column_reference};
      }

      const auto left_in_right = cross_join->right_input()->find_output_column_id(predicate_left_column_reference);
      const auto right_in_left = cross_join->left_input()->find_output_column_id(predicate_right_column_reference);

      if (right_in_left && left_in_right) {
        return JoinCondition{predicate_node, predicate_right_column_reference, predicate_left_column_reference};
      }
    }
  }

  return std::nullopt;
}

}  // namespace opossum
