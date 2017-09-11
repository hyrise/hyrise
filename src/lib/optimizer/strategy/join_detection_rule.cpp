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

    auto join_condition = _find_predicate_for_cross_join(node);

    if (join_condition) {
      // TODO(Sven): implement
      std::pair<std::string, std::string> column_names(join_condition->left_child()->name(),
                                                       join_condition->right_child()->name());
      auto scan_type = ScanType::OpEquals;
      auto join_mode = JoinMode::Inner;

      join_node->set_join_column_names(column_names);
      join_node->set_join_mode(join_mode);
      join_node->set_scan_type(scan_type);

      return join_node;
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

const std::shared_ptr<ExpressionNode> JoinConditionDetectionRule::_find_predicate_for_cross_join(
    const std::shared_ptr<AbstractASTNode> &cross_join) {
  // TODO(mp): Assume for now that we only have products of two tables. Subselect might need special handling
  DebugAssert(cross_join->left_child() && cross_join->right_child(), "Cross Join must have two children");
  DebugAssert(cross_join->left_child()->type() == ASTNodeType::StoredTable,
              "For now children of Cross Join must be StoredTables, left_child is not.");
  DebugAssert(cross_join->right_child()->type() == ASTNodeType::StoredTable,
              "For now children of Cross Join must be StoredTables, right_child is not.");

  // TODO(Sven): Fix with ColumnIDs
  auto left_table = std::dynamic_pointer_cast<StoredTableNode>(cross_join->left_child());
  auto right_table = std::dynamic_pointer_cast<StoredTableNode>(cross_join->right_child());

  auto temp_node = cross_join;

  while (temp_node->parent() != nullptr) {
    temp_node = temp_node->parent();

    if (temp_node->type() == ASTNodeType::Predicate) {
      auto predicate = std::dynamic_pointer_cast<PredicateNode>(temp_node);
      auto expression = predicate->predicate();

      auto join_condition = _find_join_condition(left_table, right_table, predicate, expression);
      if (join_condition != nullptr) {
        // remove predicate from AST
        if (!predicate->predicate()) {
          if (predicate->parent()) {
            predicate->parent()->set_left_child(predicate->left_child());
          } else {
            predicate->left_child()->clear_parent();
          }
        } else {
          // remove condition from predicate
        }

        return join_condition;
      }
    }
  }

  return nullptr;
}

const std::shared_ptr<ExpressionNode> JoinConditionDetectionRule::_find_join_condition(
    const std::shared_ptr<StoredTableNode> &left_table, const std::shared_ptr<StoredTableNode> &right_table,
    const std::shared_ptr<PredicateNode> &predicate, const std::shared_ptr<ExpressionNode> &expression) {
  if (expression->left_child() && expression->right_child()) {
    if (expression->left_child()->type() == ExpressionType::ColumnIdentifier &&
        expression->right_child()->type() == ExpressionType::ColumnIdentifier) {
      auto &left_column_reference = expression->left_child();
      auto &right_column_reference = expression->right_child();

      if (_is_join_condition(left_table->table_name(), right_table->table_name(), left_column_reference->table_name(),
                             right_column_reference->table_name())) {
        // remove expression from expression tree
        if (expression->parent()) {
          expression->clear_parent();
        } else {
          predicate->set_predicate(nullptr);
        }

        return expression;
      }
    }

    // Check children of expression for join condition
    auto potential = _find_join_condition(left_table, right_table, predicate, expression->left_child());
    if (potential) {
      return potential;
    }

    return _find_join_condition(left_table, right_table, predicate, expression->right_child());
  }

  return nullptr;
}

bool JoinConditionDetectionRule::_is_join_condition(const std::string &expected_left_table_name,
                                                    const std::string &expected_right_table_name,
                                                    const std::string &actual_left_table_name,
                                                    const std::string &actual_right_table_name) {
  return (expected_left_table_name == actual_left_table_name && expected_right_table_name == actual_right_table_name) ||
         (expected_right_table_name == actual_left_table_name && expected_left_table_name == actual_right_table_name);
}

}  // namespace opossum