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

/**
 * _get_boundary takes a BinaryPredicateExpression and the corresponding PredicateNode
 * as its input and returns a normalized ColumnBoundary. This function checks where the
 * LQPColumnExpression and where the ValueExpression is stored in the BinaryPredicateExpression.
 * The expressions are transferred to a normalized ColumnBoundary format
 * and are labled with a ColumnBoundaryType, that depends on their positions and the predicate condition
 * of the BinaryPredicateExpression
 *
 **/
const ColumnBoundary BetweenCompositionRule::_get_boundary(const std::shared_ptr<BinaryPredicateExpression>& expression,
                                                           const std::shared_ptr<PredicateNode>& node) const {
  auto type = ColumnBoundaryType::None;
  auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->left_operand());
  auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->right_operand());

  // Case: "ColumnExpression [CONDITION] ValueExpression" will be checked
  // Boundary type will be set according to this order
  if (column_expression != nullptr && value_expression != nullptr) {
    switch (expression->predicate_condition) {
      case PredicateCondition::LessThanEquals:
        type = ColumnBoundaryType::UpperBoundaryInclusive;
        break;
      case PredicateCondition::GreaterThanEquals:
        type = ColumnBoundaryType::LowerBoundaryInclusive;
        break;
      case PredicateCondition::LessThan:
        type = ColumnBoundaryType::UpperBoundaryExclusive;
        break;
      case PredicateCondition::GreaterThan:
        type = ColumnBoundaryType::LowerBoundaryExclusive;
        break;
      default:
        break;
    }
  } else {
    value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->left_operand());
    column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->right_operand());

    // Case: "ValueExpression [CONDITION] ColumnExpression" will be checked
    // Boundary type will be set according to this order
    if (value_expression != nullptr && column_expression != nullptr) {
      switch (expression->predicate_condition) {
        case PredicateCondition::GreaterThanEquals:
          type = ColumnBoundaryType::UpperBoundaryInclusive;
          break;
        case PredicateCondition::LessThanEquals:
          type = ColumnBoundaryType::LowerBoundaryInclusive;
          break;
        case PredicateCondition::GreaterThan:
          type = ColumnBoundaryType::UpperBoundaryExclusive;
          break;
        case PredicateCondition::LessThan:
          type = ColumnBoundaryType::LowerBoundaryExclusive;
          break;
        default:
          break;
      }
    }
  }

  return {
      node,
      column_expression,
      value_expression,
      type,
  };
}
/**
 * _replace_predicates gets a vector of AbstractLQPNodes as input and
 * substitutes suitable BinaryPredicateExpressions with BetweenExpressions.
 * Furthermore BinaryPredicateExpressions which are obsolete after the substitution
 * are removed.
**/
void BetweenCompositionRule::_replace_predicates(std::vector<std::shared_ptr<AbstractLQPNode>>& predicates) const {
  // Store original input and output
  auto input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  auto between_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  auto predicate_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  std::unordered_map<LQPColumnReference, std::vector<ColumnBoundary>> column_boundaries;

  // Filter predicates with a boundary to the boundaries vector
  for (auto& predicate : predicates) {
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(predicate);
    const auto expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
    if (expression != nullptr) {
      const auto boundary = _get_boundary(expression, predicate_node);
      if (boundary.type != ColumnBoundaryType::None) {
        if (column_boundaries.find(boundary.column_expression->column_reference) == column_boundaries.end()) {
          column_boundaries[boundary.column_expression->column_reference] = std::vector<ColumnBoundary>();
        }
        column_boundaries[boundary.column_expression->column_reference].push_back(boundary);
      } else {
        predicate_nodes.push_back(predicate);
      }
    } else {
      predicate_nodes.push_back(predicate);
    }
    // Remove node from lqp in order to rearrange them later
    lqp_remove_node(predicate);
  }
  // Store the highest lower bound and the lowest upper bound for a column in order to get an optimal BetweenExpression
  std::shared_ptr<ValueExpression> lower_bound_value_expression;
  std::shared_ptr<ValueExpression> upper_bound_value_expression;
  bool left_inclusive;
  bool right_inclusive;

  for (const auto boundaries : column_boundaries) {
    for (const auto boundary : boundaries.second) {
      switch (boundary.type) {
        case ColumnBoundaryType::UpperBoundaryInclusive:
          if (!upper_bound_value_expression || upper_bound_value_expression->value > boundary.value_expression->value) {
            upper_bound_value_expression = boundary.value_expression;
            right_inclusive = true;
          }
          break;
        case ColumnBoundaryType::LowerBoundaryInclusive:
          if (!lower_bound_value_expression || lower_bound_value_expression->value < boundary.value_expression->value) {
            lower_bound_value_expression = boundary.value_expression;
            left_inclusive = true;
          }
          break;
        case ColumnBoundaryType::UpperBoundaryExclusive:
          if (!upper_bound_value_expression ||
              upper_bound_value_expression->value >= boundary.value_expression->value) {
            upper_bound_value_expression = boundary.value_expression;
            right_inclusive = false;
          }
          break;
        case ColumnBoundaryType::LowerBoundaryExclusive:
          if (!lower_bound_value_expression ||
              lower_bound_value_expression->value <= boundary.value_expression->value) {
            lower_bound_value_expression = boundary.value_expression;
            left_inclusive = false;
          }
          break;
        default:
          break;
      }
    }

    if (lower_bound_value_expression != nullptr && upper_bound_value_expression != nullptr) {
      const auto between_node = PredicateNode::make(
          std::make_shared<BetweenExpression>(boundaries.second[0].column_expression, lower_bound_value_expression,
                                              upper_bound_value_expression, left_inclusive, right_inclusive));
      between_nodes.push_back(between_node);
    } else {
      // If no substitution was possible, all nodes referring to this column have to be inserted into the LQP again later
      for (const auto& boundary : boundaries.second) {
        predicate_nodes.push_back(boundary.node);
      }
    }
    lower_bound_value_expression = nullptr;
    upper_bound_value_expression = nullptr;
  }

  // Append between nodes to predicate nodes to get the complete chain of all necessary LQP nodes
  predicate_nodes.insert(predicate_nodes.cend(), between_nodes.cbegin(), between_nodes.cend());

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
