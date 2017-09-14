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

const std::shared_ptr<AbstractASTNode> JoinConditionDetectionRule::apply_to(
    const std::shared_ptr<AbstractASTNode> & node) {
  if (node->type() == ASTNodeType::Join) {
    auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

    // Only search join condition for Cross Joins
    if (join_node->join_mode() != JoinMode::Cross) {
      return node;
    }

    auto predicate_node = _find_predicate_for_cross_join(join_node);

    /**
     * When we found a valid Join Condition, we have to rewrite the JoinNode.
     */
    if (predicate_node) {
      std::pair<ColumnID, ColumnID> column_ids(predicate_node->column_id(),
                                               boost::get<ColumnID>(predicate_node->value()));

      const auto new_join_node = std::make_shared<JoinNode>(JoinMode::Inner, column_ids, predicate_node->scan_type());
      // TODO(Sven): what if parent is JoinNode with right child

      new_join_node->set_left_child(join_node->left_child());
      new_join_node->set_right_child(join_node->right_child());

      if (join_node->parent()) {
        join_node->parent()->set_left_child(new_join_node);
      }

      /**
       * Apply rule recursively
       */
      apply_to_children(new_join_node);

      return new_join_node;
    }
  }

  /**
   * Apply rule recursively
   */
  if (node->left_child()) {
    auto left_node = apply_to(node->left_child());
    // Check whether node was removed from AST, specifically if node was PredicateNode
    if (!node->left_child()) {
      return left_node;
    }
  }
  if (node->right_child()) {
    apply_to(node->right_child());
  }

  return node;
}

const std::shared_ptr<PredicateNode> JoinConditionDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<JoinNode> &cross_join) {
  Assert(cross_join->left_child() && cross_join->right_child(), "Cross Join must have two children");

  // Go up in AST to find corresponding PredicateNode
  std::shared_ptr<AbstractASTNode> node = cross_join;
  while (node->parent() != nullptr) {
    node = node->parent();

    if (node->type() == ASTNodeType::Predicate) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

      if (predicate_node->value().type() != typeid(ColumnID)) {
        continue;
      }

      const auto left_column_id = predicate_node->column_id();
      const auto right_column_id = boost::get<ColumnID>(predicate_node->value());
      const auto left_size = cross_join->left_child()->output_col_count();

      if (_is_join_condition(left_column_id, right_column_id, left_size)) {
        // remove predicate from AST
        if (predicate_node->parent()) {
          predicate_node->parent()->set_left_child(predicate_node->left_child());
        } else {
          predicate_node->left_child()->clear_parent();
        }

        return predicate_node;
      }
    }
  }

  return nullptr;
}

bool JoinConditionDetectionRule::_is_join_condition(ColumnID left, ColumnID right, size_t number_columns_left) {
  return (left < number_columns_left && !(right < number_columns_left)) ||
         (!(left < number_columns_left) && right < number_columns_left);
}

}  // namespace opossum
