#include "join_detection_rule.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

bool JoinConditionDetectionRule::apply_to(const std::shared_ptr<AbstractASTNode> &node) {
  if (node->type() == ASTNodeType::Join) {
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
        new_join_node->replace_in_tree(cross_join_node);
        predicate_node->remove_from_tree();

        return true;
      }
    }
  }

  return _apply_to_children(node);
}

optional<JoinConditionDetectionRule::JoinCondition> JoinConditionDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<JoinNode> &cross_join) {
  Assert(cross_join->left_child() && cross_join->right_child(), "Cross Join must have two children");

  // Go up in AST to find corresponding PredicateNode
  std::shared_ptr<AbstractASTNode> node = cross_join;
  while (node->parent() != nullptr) {
    node = node->parent();

    /**
     * TODO(anyone)
     * Right now we only support traversing past nodes that do not change the column order and to be 100% safe
     * we make this explicit by only traversing past Joins and Predicates
     *
     * Detecting Join Conditions across other node types may be possible by applying 'Predicate Pushdown' first.
     */
    if (node->type() != ASTNodeType::Join && node->type() != ASTNodeType::Predicate) {
      return nullopt;
    }

    if (node->type() == ASTNodeType::Predicate) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

      if (predicate_node->value().type() != typeid(ColumnID)) {
        continue;
      }

      /**
       * We have a (Cross)JoinNode and PredicateNode located further up in the tree. Now we have to determine whether
       * and how they can be merged to a normal Join.
       * In order to do this, check whether the left column of the Predicate refers to the left input table of the
       * CrossJoin and the right column of the Predicate to the right input table of the CrossJoin - OR whether there
       * is left-to-right and right-to_left match.
       *
       * IMPORTANT: Since we only traversed nodes that do not change the column order when looking for the Predicate,
       * we can do this by the simple range check in _is_join_condition()
       */
      const auto predicate_left_column_id = predicate_node->column_id();
      const auto predicate_right_column_id = boost::get<ColumnID>(predicate_node->value());
      const auto cross_left_num_cols = cross_join->left_child()->output_col_count();
      const auto cross_right_num_cols = cross_join->right_child()->output_col_count();

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
    }
  }

  return nullopt;
}

bool JoinConditionDetectionRule::_is_join_condition(ColumnID left, ColumnID right, size_t left_num_cols,
                                                    size_t right_num_cols) const {
  auto left_value = static_cast<ColumnID::base_type>(left);
  auto right_value = static_cast<ColumnID::base_type>(right);

  return left_value < left_num_cols && right_value >= left_num_cols && right_value < left_num_cols + right_num_cols;
}

}  // namespace opossum
