#include "join_detection_rule.hpp"

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_logical_query_plan_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string JoinDetectionRule::name() const { return "Join Detection Rule"; }

bool JoinDetectionRule::apply_to(const std::shared_ptr<AbstractLogicalQueryPlanNode>& node) {
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
        std::pair<ColumnID, ColumnID> column_ids(join_condition->left_column_id, join_condition->right_column_id);

        auto predicate_node = join_condition->predicate_node;
        const auto new_join_node = std::make_shared<JoinNode>(JoinMode::Inner, column_ids, predicate_node->scan_type());

        /**
         * Place the conditional join where the cross join was and remove the predicate node
         */
        cross_join_node->replace_with(new_join_node);
        predicate_node->remove_from_tree();

        return true;
      }
    }
  }

  return _apply_to_children(node);
}

std::optional<JoinDetectionRule::JoinCondition> JoinDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<JoinNode>& cross_join) {
  Assert(cross_join->left_child() && cross_join->right_child(), "Cross Join must have two children");

  // Everytime we traverse a node which we're the right child of, the ColumnIDs a predicate needs to reference become
  // offsetted
  auto column_id_offset = 0;

  // Go up in AST to find corresponding PredicateNode
  std::shared_ptr<AbstractLogicalQueryPlanNode> node = cross_join;
  while (true) {
    const auto parents = node->parents();

    /**
     * Can't deal with the parents.size() > 0 case - would need to check that a potential predicate exists in all
     * parents and this is too much work considering the JoinDetectionRule will be removed soon-ish (TM)
     */
    if (parents.empty() || parents.size() > 1) {
      break;
    }

    if (node->get_child_side(parents[0]) == LQPChildSide::Right) {
      column_id_offset += parents[0]->left_child()->output_column_count();
    }

    node = parents[0];

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

      if (predicate_node->value().type() != typeid(ColumnID)) {
        continue;
      }

      /**
       * We have a (Cross)JoinNode and PredicateNode located further up in the tree. Now we have to determine whether
       * and how they can be merged to a normal Join.
       * More precisely, we have to determine which columns of the cross joins input tables correspond to the columns
       * used
       * in the predicate.
       * In order to do this, check whether the left column of the Predicate refers to the left input table of the
       * CrossJoin and the right column of the Predicate to the right input table of the CrossJoin - OR whether there
       * is left-to-right and right-to_left match.
       *
       * IMPORTANT: Since we only traversed nodes that do not change the column order when looking for the Predicate,
       * we can do this by the simple range check in _is_join_condition()
       */
      auto predicate_left_column_id = predicate_node->column_id();
      auto predicate_right_column_id = boost::get<ColumnID>(predicate_node->value());

      // The Predicate refers to a subtree left of the subtree the Cross Join came from
      if (predicate_left_column_id < column_id_offset || predicate_right_column_id < column_id_offset) {
        continue;
      }

      // Calculate column ids relative to the leftmost output column of the CrossJoin subtree.
      predicate_left_column_id -= column_id_offset;
      predicate_right_column_id -= column_id_offset;

      const auto cross_left_num_cols = cross_join->left_child()->output_column_count();
      const auto cross_right_num_cols = cross_join->right_child()->output_column_count();

      if (_is_join_condition(predicate_left_column_id, predicate_right_column_id, cross_left_num_cols,
                             cross_right_num_cols)) {
        return JoinCondition{predicate_node, predicate_left_column_id,
                             ColumnID{(ColumnID::base_type)(predicate_right_column_id - cross_left_num_cols)}};
      }
      if (_is_join_condition(predicate_right_column_id, predicate_left_column_id, cross_left_num_cols,
                             cross_right_num_cols)) {
        return JoinCondition{predicate_node, predicate_right_column_id,
                             ColumnID{(ColumnID::base_type)(predicate_left_column_id - cross_left_num_cols)}};
      }

      // If we make it here, the predicate was no fit to be merged with the cross join
    }
  }

  return std::nullopt;
}

bool JoinDetectionRule::_is_join_condition(ColumnID left, ColumnID right, size_t left_num_cols,
                                           size_t right_num_cols) const {
  auto left_value = static_cast<ColumnID::base_type>(left);
  auto right_value = static_cast<ColumnID::base_type>(right);

  return left_value < left_num_cols && right_value >= left_num_cols && right_value < left_num_cols + right_num_cols;
}

}  // namespace opossum
