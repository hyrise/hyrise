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
#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::shared_ptr<AbstractASTNode> JoinConditionDetectionRule::apply_to(
    const std::shared_ptr<AbstractASTNode> node) {
  if (node->type() == ASTNodeType::Join) {
    auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

    // Only search join condition for Cross Joins
    if (join_node->join_mode() != JoinMode::Cross) {
      return node;
    }

    auto predicate_node = _find_predicate_for_cross_join(node);

    if (predicate_node) {
      std::pair<ColumnID, ColumnID> column_ids(predicate_node->column_id(), boost::get<ColumnID>(predicate_node->value()));

      auto scan_type = predicate_node->scan_type();
      auto join_mode = JoinMode::Inner;

      const auto new_join_node = std::make_shared<JoinNode>(join_mode, column_ids, scan_type);
      // TODO(Sven): what if parent is JoinNode with right child

      new_join_node->set_left_child(join_node->left_child());
      new_join_node->set_right_child(join_node->right_child());

      if (join_node->parent()) {
        join_node->parent()->set_left_child(new_join_node);
      }
      return new_join_node;
    }
  }

  // TODO(Sven): explain
  if (node->left_child()) {
    auto left_node = apply_to(node->left_child());
    if (!node->left_child()) {
      return left_node;
    }
  }
  if (node->right_child()) {
    auto right_node = apply_to(node->right_child());
  }

  return node;
}

const std::shared_ptr<PredicateNode> JoinConditionDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<AbstractASTNode> &cross_join) {
  // TODO(mp): Assume for now that we only have products of two tables. Subselect might need special handling
  DebugAssert(cross_join->left_child() && cross_join->right_child(), "Cross Join must have two children");

  // TODO(Sven): Fix with ColumnIDs, need to traverse recursively to find table names
  const auto left_input = cross_join->left_child();
  const auto right_input = cross_join->right_child();

  // Go up in AST to find corresponding PredicateNode
  auto temp_node = cross_join;
  while (temp_node->parent() != nullptr) {
    temp_node = temp_node->parent();

    if (temp_node->type() == ASTNodeType::Predicate) {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(temp_node);

      // TODO(Sven): check type of AllParameterVariant
      const auto left_column_id = predicate_node->column_id();
      const auto right_column_id = boost::get<ColumnID>(predicate_node->value());

      const auto left_size = cross_join->left_child()->output_col_count();

      if ((left_column_id < left_size && !(right_column_id < left_size)) || (!(left_column_id < left_size) && right_column_id < left_size)) {
        std::cout << "found join conition" << std::endl;

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

}  // namespace opossum