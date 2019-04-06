#include "between_composition_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
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
 * as its input and returns a standardized ColumnBoundary. This function checks where the
 * LQPColumnExpression and the ValueExpression are stored in the BinaryPredicateExpression.
 * The expressions are transferred to a standardized ColumnBoundary format
 * and labelled with a ColumnBoundaryType that depends on their positions and the predicate condition
 * of the BinaryPredicateExpression
 *
 **/
const BetweenCompositionRule::ColumnBoundary BetweenCompositionRule::_get_boundary(
    const std::shared_ptr<BinaryPredicateExpression>& expression, const uint8_t id) const {
  auto type = ColumnBoundaryType::None;
  const auto left_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->left_operand());
  auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->right_operand());

  // Case: "ColumnExpression [CONDITION] ValueExpression" will be checked
  // Boundary type will be set according to this order
  if (left_column_expression && value_expression) {
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
    return {
        left_column_expression, value_expression, type, false, id,
    };
  } else {
    value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->left_operand());
    const auto right_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->right_operand());

    // Case: "ValueExpression [CONDITION] ColumnExpression" will be checked
    // Boundary type will be set according to this order
    if (value_expression && right_column_expression) {
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
      return {
          right_column_expression, value_expression, type, false, id,
      };
    } else if (left_column_expression && right_column_expression) {
      // Case: "ColumnExpression [CONDITION] ColumnExpression" will be checked
      // Boundary type will be set according to this order
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
      return {
          left_column_expression, right_column_expression, type, true, id,
      };
    }
  }
  return {
      nullptr, nullptr, ColumnBoundaryType::None, false, id,
  };
}

const BetweenCompositionRule::ColumnBoundary BetweenCompositionRule::_create_inverse_boundary(
    const std::shared_ptr<ColumnBoundary> column_boundary) const {
  auto type = ColumnBoundaryType::None;
  switch (column_boundary->type) {
    case ColumnBoundaryType::UpperBoundaryInclusive:
      type = ColumnBoundaryType::LowerBoundaryInclusive;
      break;
    case ColumnBoundaryType::LowerBoundaryInclusive:
      type = ColumnBoundaryType::UpperBoundaryInclusive;
      break;
    case ColumnBoundaryType::UpperBoundaryExclusive:
      type = ColumnBoundaryType::LowerBoundaryExclusive;
      break;
    case ColumnBoundaryType::LowerBoundaryExclusive:
      type = ColumnBoundaryType::UpperBoundaryExclusive;
      break;
    default:
      break;
  }

  return {
      std::static_pointer_cast<LQPColumnExpression>(column_boundary->border_expression),
      std::static_pointer_cast<AbstractExpression>(column_boundary->column_expression),
      type,
      true,
      column_boundary->id,
  };
}

static PredicateCondition get_between_predicate_condition(bool left_inclusive, bool right_inclusive) {
  if (left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenInclusive;
  } else if (left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenUpperExclusive;
  } else if (!left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenLowerExclusive;
  } else if (!left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenExclusive;
  }
  Fail("Unreachable Case");
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
  std::unordered_map<LQPColumnReference, std::vector<std::shared_ptr<ColumnBoundary>>> column_boundaries;

  int16_t id_counter = 0;

  // Filter predicates with a boundary to the boundaries vector
  for (auto& predicate : predicates) {
    // A logical expression can contain multiple binary predicate expressions
    std::vector<std::shared_ptr<BinaryPredicateExpression>> expressions;
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(predicate);
    const auto binary_predicate_expression =
        std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
    if (binary_predicate_expression) {
      expressions.push_back(binary_predicate_expression);
    } else {
      const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(predicate_node->predicate());
      if (logical_expression && logical_expression->logical_operator == LogicalOperator::And) {
        const auto flattened_expressions = flatten_logical_expressions(logical_expression, LogicalOperator::And);
        for (const auto& flattened_expression : flattened_expressions) {
          const auto flattened_binary_predicate_expression =
              std::dynamic_pointer_cast<BinaryPredicateExpression>(flattened_expression);
          if (flattened_binary_predicate_expression) {
            expressions.push_back(flattened_binary_predicate_expression);
          } else {
            predicate_nodes.push_back(PredicateNode::make(flattened_expression));
          }
        }
      } else {
        predicate_nodes.push_back(predicate);
      }
    }

    for (const auto& expression : expressions) {
      const auto boundary = std::make_shared<ColumnBoundary>(_get_boundary(expression, id_counter));
      id_counter++;
      if (boundary->type != ColumnBoundaryType::None) {
        if (boundary->boundary_is_column_expression) {
          const auto inverse_boundary = std::make_shared<ColumnBoundary>(_create_inverse_boundary(boundary));
          if (column_boundaries.find(inverse_boundary->column_expression->column_reference) ==
              column_boundaries.end()) {
            column_boundaries[inverse_boundary->column_expression->column_reference] =
                std::vector<std::shared_ptr<ColumnBoundary>>();
          }
          column_boundaries[inverse_boundary->column_expression->column_reference].push_back(inverse_boundary);
        }
        if (column_boundaries.find(boundary->column_expression->column_reference) == column_boundaries.end()) {
          column_boundaries[boundary->column_expression->column_reference] =
              std::vector<std::shared_ptr<ColumnBoundary>>();
        }
        column_boundaries[boundary->column_expression->column_reference].push_back(boundary);
      } else {
        predicate_nodes.push_back(predicate);
      }
    }
    // Remove node from lqp in order to rearrange the nodes soon
    lqp_remove_node(predicate);
  }
  // Store the highest lower bound and the lowest upper bound for a column in order to get an optimal BetweenExpression
  std::shared_ptr<ColumnBoundary> lower_bound_value_expression;
  std::shared_ptr<ColumnBoundary> upper_bound_value_expression;
  std::shared_ptr<ColumnBoundary> upper_bound_column_expression;
  std::shared_ptr<ColumnBoundary> lower_bound_column_expression;
  auto column_boundary_ids = std::vector<uint8_t>();
  bool value_lower_inclusive = false;
  bool value_upper_inclusive = false;
  bool column_upper_inclusive = false;
  bool column_lower_inclusive = false;

  for (const auto& boundaries : column_boundaries) {
    for (auto& boundary : boundaries.second) {
      if (!boundary->boundary_is_column_expression) {
        const auto boundary_border_expression = std::static_pointer_cast<ValueExpression>(boundary->border_expression);
        switch (boundary->type) {
          case ColumnBoundaryType::UpperBoundaryInclusive:
            if (!upper_bound_value_expression ||
                std::static_pointer_cast<ValueExpression>(upper_bound_value_expression->border_expression)->value >
                    boundary_border_expression->value) {
              upper_bound_value_expression = boundary;
              value_upper_inclusive = true;
            }
            break;
          case ColumnBoundaryType::LowerBoundaryInclusive:
            if (!lower_bound_value_expression ||
                std::static_pointer_cast<ValueExpression>(lower_bound_value_expression->border_expression)->value <
                    boundary_border_expression->value) {
              lower_bound_value_expression = boundary;
              value_lower_inclusive = true;
            }
            break;
          case ColumnBoundaryType::UpperBoundaryExclusive:
            if (!upper_bound_value_expression ||
                std::static_pointer_cast<ValueExpression>(upper_bound_value_expression->border_expression)->value >=
                    boundary_border_expression->value) {
              upper_bound_value_expression = boundary;
              value_upper_inclusive = false;
            }
            break;
          case ColumnBoundaryType::LowerBoundaryExclusive:
            if (!lower_bound_value_expression ||
                std::static_pointer_cast<ValueExpression>(lower_bound_value_expression->border_expression)->value <=
                    boundary_border_expression->value) {
              lower_bound_value_expression = boundary;
              value_lower_inclusive = false;
            }
            break;
          case ColumnBoundaryType::None:
            break;
        }
      } else {
        switch (boundary->type) {
          case ColumnBoundaryType::UpperBoundaryInclusive:
            if (upper_bound_column_expression == nullptr) {
              upper_bound_column_expression = boundary;
              column_upper_inclusive = true;
            }
            break;
          case ColumnBoundaryType::LowerBoundaryInclusive:
            if (lower_bound_column_expression == nullptr) {
              lower_bound_column_expression = boundary;
              column_lower_inclusive = true;
            }
            break;
          case ColumnBoundaryType::UpperBoundaryExclusive:
            if (upper_bound_column_expression == nullptr) {
              upper_bound_column_expression = boundary;
              column_upper_inclusive = false;
            }
            break;
          case ColumnBoundaryType::LowerBoundaryExclusive:
            if (lower_bound_column_expression == nullptr) {
              lower_bound_column_expression = boundary;
              column_lower_inclusive = false;
            }
            break;
          case ColumnBoundaryType::None:
            break;
        }
      }
    }

    if (lower_bound_value_expression && upper_bound_value_expression) {
      const auto lower_value_expression =
          std::static_pointer_cast<ValueExpression>(lower_bound_value_expression->border_expression);
      const auto upper_value_expression =
          std::static_pointer_cast<ValueExpression>(upper_bound_value_expression->border_expression);
      const auto between_node = PredicateNode::make(std::make_shared<BetweenExpression>(
          get_between_predicate_condition(value_lower_inclusive, value_upper_inclusive),
          boundaries.second[0]->column_expression, lower_value_expression, upper_value_expression));
      between_nodes.push_back(between_node);
      column_boundary_ids.push_back(lower_bound_value_expression->id);
      column_boundary_ids.push_back(upper_bound_value_expression->id);
      // Remove unnecessary value boundaries for this column
      for (const auto value_boundary : boundaries.second) {
        if (!value_boundary->boundary_is_column_expression) {
          column_boundary_ids.push_back(value_boundary->id);
        }
      }
    }

    if (lower_bound_column_expression && upper_bound_column_expression) {
      const auto lower_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(lower_bound_column_expression->border_expression);
      const auto upper_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(upper_bound_column_expression->border_expression);
      const auto between_node = PredicateNode::make(std::make_shared<BetweenExpression>(
          get_between_predicate_condition(column_lower_inclusive, column_upper_inclusive),
          boundaries.second[0]->column_expression, lower_column_expression, upper_column_expression));
      between_nodes.push_back(between_node);
      column_boundary_ids.push_back(lower_bound_column_expression->id);
      column_boundary_ids.push_back(upper_bound_column_expression->id);
    }

    lower_bound_value_expression = nullptr;
    upper_bound_value_expression = nullptr;
    lower_bound_column_expression = nullptr;
    upper_bound_column_expression = nullptr;
  }

  // If no substitution was possible, all nodes referring to this column have to be inserted into the LQP again
  // later. Therefore we create a semantically equal predicate node.
  for (const auto& boundaries : column_boundaries) {
    for (auto& boundary : boundaries.second) {
      if (std::find(column_boundary_ids.begin(), column_boundary_ids.end(), boundary->id) ==
          column_boundary_ids.end()) {
        if (!boundary->boundary_is_column_expression) {
          const auto boundary_border_value_expression =
              std::static_pointer_cast<ValueExpression>(boundary->border_expression);
          switch (boundary->type) {
            case ColumnBoundaryType::UpperBoundaryInclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::LessThanEquals, boundary->column_expression, boundary_border_value_expression)));
              break;
            case ColumnBoundaryType::LowerBoundaryInclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::GreaterThanEquals, boundary->column_expression,
                  boundary_border_value_expression)));
              break;
            case ColumnBoundaryType::UpperBoundaryExclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::LessThan, boundary->column_expression, boundary_border_value_expression)));
              break;
            case ColumnBoundaryType::LowerBoundaryExclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::GreaterThan, boundary->column_expression,
                  boundary_border_value_expression)));
              break;
            default:
              break;
          }
        } else {
          const auto boundary_border_column_expression =
              std::static_pointer_cast<LQPColumnExpression>(boundary->border_expression);
          switch (boundary->type) {
            case ColumnBoundaryType::UpperBoundaryInclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::LessThanEquals, boundary->column_expression, boundary_border_column_expression)));
              break;
            case ColumnBoundaryType::LowerBoundaryInclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::GreaterThanEquals, boundary->column_expression,
                  boundary_border_column_expression)));
              break;
            case ColumnBoundaryType::UpperBoundaryExclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::LessThan, boundary->column_expression, boundary_border_column_expression)));
              break;
            case ColumnBoundaryType::LowerBoundaryExclusive:
              predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                  PredicateCondition::GreaterThan, boundary->column_expression, boundary_border_column_expression)));
              break;
            default:
              break;
          }
        }
      }
    }
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
  for (size_t output_index = 0; output_index < outputs.size(); ++output_index) {
    outputs[output_index]->set_input(input_sides[output_index], predicate_nodes.front());
  }
}

void BetweenCompositionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type == LQPNodeType::Predicate) {
    std::vector<std::shared_ptr<AbstractLQPNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type == LQPNodeType::Predicate) {
      // Once a node has multiple outputs, we're not talking about a predicate chain anymore
      if (current_node->outputs().size() > 1) {
        break;
      }

      predicate_nodes.emplace_back(current_node);

      current_node = current_node->left_input();
    }

    // A substitution is also possible with only 1 predicate_node, if it is a LogicalExpression with
    // the LogicalOperator::AND
    if (!predicate_nodes.empty()) {
      // A chain of predicates was found. Continue rule with last input
      _replace_predicates(predicate_nodes);
      _apply_to_inputs(predicate_nodes.back());
      return;
    }
  }

  _apply_to_inputs(node);
}

}  // namespace opossum
