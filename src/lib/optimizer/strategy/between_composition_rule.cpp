#include "between_composition_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string BetweenCompositionRule::name() const { return "Between Composition Rule"; }

const ColumnBoundary BetweenCompositionRule::_get_boundary(const std::shared_ptr<BinaryPredicateExpression>& expression,
                                                           const std::shared_ptr<PredicateNode>& node) const {
  const auto left_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->left_operand());
  const auto right_value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->right_operand());
  auto type = ColumnBoundaryType::None;

  if (left_column_expression != nullptr && right_value_expression != nullptr) {
    if (expression->predicate_condition == PredicateCondition::LessThanEquals) {
      type = ColumnBoundaryType::UpperBoundaryInclusive;
    } else if (expression->predicate_condition == PredicateCondition::GreaterThanEquals) {
      type = ColumnBoundaryType::LowerBoundaryInclusive;
    } else if (expression->predicate_condition == PredicateCondition::LessThan) {
      type = ColumnBoundaryType::UpperBoundaryExclusive;
    } else if (expression->predicate_condition == PredicateCondition::GreaterThan) {
      type = ColumnBoundaryType::LowerBoundaryExclusive;
    }
    return {
        node,
        left_column_expression,
        right_value_expression,
        type,
    };
  }

  const auto left_value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->left_operand());
  const auto right_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->right_operand());

  if (left_value_expression != nullptr && right_column_expression != nullptr) {
    if (expression->predicate_condition == PredicateCondition::GreaterThanEquals) {
      type = ColumnBoundaryType::UpperBoundaryInclusive;
    } else if (expression->predicate_condition == PredicateCondition::LessThanEquals) {
      type = ColumnBoundaryType::LowerBoundaryInclusive;
    } else if (expression->predicate_condition == PredicateCondition::GreaterThan) {
      type = ColumnBoundaryType::UpperBoundaryExclusive;
    } else if (expression->predicate_condition == PredicateCondition::LessThan) {
      type = ColumnBoundaryType::LowerBoundaryExclusive;
    }
    return {
        node,
        right_column_expression,
        left_value_expression,
        type,
    };
  }

  return {nullptr, nullptr, nullptr, type};
}

bool _column_boundary_comparator(const ColumnBoundary& a, const ColumnBoundary& b) {
  return a.column_expression->column_reference.original_column_id() <
         b.column_expression->column_reference.original_column_id();
}

void BetweenCompositionRule::_replace_predicates(std::vector<std::shared_ptr<AbstractLQPNode>>& predicates) const {
  // Store original input and output
  auto input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  auto between_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  auto predicate_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  auto boundaries = std::vector<ColumnBoundary>();

  // Untie predicates from LQP, so we can freely retie them
  for (auto& predicate : predicates) {
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(predicate);
    const auto expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
    if (expression != nullptr) {
      const auto boundary = _get_boundary(expression, predicate_node);
      if (boundary.type != ColumnBoundaryType::None) {
        boundaries.push_back(boundary);
      } else {
        predicate_nodes.push_back(predicate);
      }
    } else {
      predicate_nodes.push_back(predicate);
    }
    lqp_remove_node(predicate);
  }

  std::sort(boundaries.begin(), boundaries.end(), _column_boundary_comparator);

  std::shared_ptr<opossum::LQPColumnExpression> current_column_expression = nullptr;
  std::shared_ptr<opossum::ValueExpression> lower_bound_value_expression = nullptr;
  std::shared_ptr<opossum::ValueExpression> upper_bound_value_expression = nullptr;
  auto left_inclusive = true;
  auto right_inclusive = true;
  auto node_scope = std::vector<std::shared_ptr<AbstractLQPNode>>();

  for (auto boundary : boundaries) {
    if (current_column_expression == nullptr || current_column_expression->column_reference.original_column_id() !=
                                                    boundary.column_expression->column_reference.original_column_id()) {
      if (lower_bound_value_expression != nullptr && upper_bound_value_expression != nullptr) {
        const auto between_node = PredicateNode::make(
            std::make_shared<BetweenExpression>(current_column_expression, lower_bound_value_expression,
                                                upper_bound_value_expression, left_inclusive, right_inclusive));
        between_nodes.push_back(between_node);
      } else {
        predicate_nodes.insert(std::end(predicate_nodes), std::begin(node_scope), std::end(node_scope));
      }
      upper_bound_value_expression = nullptr;
      lower_bound_value_expression = nullptr;
      left_inclusive = true;
      right_inclusive = true;
      node_scope = std::vector<std::shared_ptr<AbstractLQPNode>>();
      current_column_expression = boundary.column_expression;
    }

    if (boundary.type == ColumnBoundaryType::UpperBoundaryInclusive) {
      if (upper_bound_value_expression == nullptr ||
          upper_bound_value_expression->value > boundary.value_expression->value) {
        upper_bound_value_expression = boundary.value_expression;
        right_inclusive = true;
        node_scope.push_back(boundary.node);
      }
    } else if (boundary.type == ColumnBoundaryType::LowerBoundaryInclusive) {
      if (lower_bound_value_expression == nullptr ||
          lower_bound_value_expression->value < boundary.value_expression->value) {
        lower_bound_value_expression = boundary.value_expression;
        left_inclusive = true;
        node_scope.push_back(boundary.node);
      }
    } else if (boundary.type == ColumnBoundaryType::UpperBoundaryExclusive) {
      if (upper_bound_value_expression == nullptr ||
          upper_bound_value_expression->value <= boundary.value_expression->value) {
        upper_bound_value_expression = boundary.value_expression;
        right_inclusive = false;
        node_scope.push_back(boundary.node);
      }
    } else if (boundary.type == ColumnBoundaryType::LowerBoundaryExclusive) {
      if (lower_bound_value_expression == nullptr ||
          lower_bound_value_expression->value <= boundary.value_expression->value) {
        lower_bound_value_expression = boundary.value_expression;
        left_inclusive = false;
        node_scope.push_back(boundary.node);
      }
    }
  }

  // Apply boundary check again for the last column
  if (lower_bound_value_expression != nullptr && upper_bound_value_expression != nullptr) {
    const auto between_node = PredicateNode::make(
        std::make_shared<BetweenExpression>(current_column_expression, lower_bound_value_expression,
                                            upper_bound_value_expression, left_inclusive, right_inclusive));
    between_nodes.push_back(between_node);
  } else {
    predicate_nodes.insert(std::end(predicate_nodes), std::cbegin(node_scope), std::cend(node_scope));
  }

  predicate_nodes.insert(std::end(predicate_nodes), std::cbegin(between_nodes), std::cend(between_nodes));

  // Insert predicate nodes to LQP
  // Connect last predicate to chain input
  predicate_nodes.back()->set_left_input(input);

  // Connect predicates
  for (size_t predicate_index = 0; predicate_index < predicate_nodes.size() - 1; predicate_index++) {
    predicate_nodes[predicate_index]->set_left_input(predicate_nodes[predicate_index + 1]);
  }

  // Connect first predicates to chain output
  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], predicate_nodes.front());
  }
}

void BetweenCompositionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type == LQPNodeType::Predicate) {
    std::vector<std::shared_ptr<AbstractLQPNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type == LQPNodeType::Predicate) {
      // Once a node has multiple outputs, we're not talking about a predicate chain anymore
      if (current_node->outputs().size() > 1 || current_node->right_input() != nullptr) {
        break;
      }

      predicate_nodes.emplace_back(current_node);

      if (current_node->left_input() == nullptr) {
        break;
      }
      current_node = current_node->left_input();
    }

    if (predicate_nodes.size() > 1) {
      // A chain of predicates was found. Continue rule with last input
      _replace_predicates(predicate_nodes);
      _apply_to_inputs(predicate_nodes.back());
      return;
    }
  }

  _apply_to_inputs(node);
}

}  // namespace opossum
