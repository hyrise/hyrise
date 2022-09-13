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

using namespace hyrise::expression_functional;  // NOLINT

namespace {
using namespace hyrise;  // NOLINT

PredicateCondition get_between_predicate_condition(bool left_inclusive, bool right_inclusive) {
  if (left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenInclusive;
  }

  if (left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenUpperExclusive;
  }

  if (!left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenLowerExclusive;
  }

  if (!left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenExclusive;
  }
  Fail("Unreachable Case");
}

}  // namespace

namespace hyrise {

std::string BetweenCompositionRule::name() const {
  static const auto name = std::string{"BetweenCompositionRule"};
  return name;
}

/**
 * Distinction from the ChunkPruningRule:
 *  Both rules search for predicate chains, but of different types:
 *   a) The ChunkPruningRule searches for chains of predicates that relate to specific StoredTableNodes. Predicate
 *      pruning chains always start at a StoredTableNode and continue as long as a StoredTableNode's columns remain
 *      present and become filtered.
 *   b) The BetweenCompositionRule searches for arbitrary PredicateNodes that are directly linked. Therefore,
 *      predicate chains can start and end in the midst of LQPs.
 */
void BetweenCompositionRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> visited_nodes;
  std::vector<PredicateChain> predicate_chains;

  // (1) Gather PredicateNodes and group them to predicate chains when there are no other nodes in between.
  // (1.1) Collect all leaf nodes and add them to a visitation queue
  auto node_queue = std::queue<std::shared_ptr<AbstractLQPNode>>();
  for (const auto& leaf_node : lqp_find_leaves(lqp_root)) {
    node_queue.push(leaf_node);
  }
  // (1.2) Visit the whole LQP bottom-up from the leaf nodes
  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();
    /**
     * Find the next predicate chain by collecting the next set of PredicateNodes that are directly linked with each
     * other. For this purpose, visit the LQP upwards until
     *    - there is a node that has already been visited
     *    - there is a non-PredicateNode that follows a PredicateNode
     *    - the LQP branches
     */
    auto current_predicate_chain = PredicateChain();
    visit_lqp_upwards(node, [&](const auto& current_node) {
      if (visited_nodes.contains(current_node)) {
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }
      visited_nodes.insert(current_node);

      // Add to current predicate chain or finalize it when a non-PredicateNode follows
      if (current_node->type == LQPNodeType::Predicate) {
        auto current_predicate_node = std::static_pointer_cast<PredicateNode>(current_node);
        current_predicate_chain.push_back(current_predicate_node);
      } else if (!current_predicate_chain.empty()) {
        for (const auto& output_node : current_node->outputs()) {
          if (!visited_nodes.contains(output_node)) {
            node_queue.push(output_node);
          }
        }
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      // Check whether LQP branches
      if (current_node->outputs().size() > 1) {
        for (const auto& output_node : current_node->outputs()) {
          // Prepare the next iteration of visit_lqp_upwards
          if (!visited_nodes.contains(output_node)) {
            node_queue.push(output_node);
          }
        }
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      return LQPUpwardVisitation::VisitOutputs;
    });

    if (!current_predicate_chain.empty()) {
      // In some cases, Between-substitutions are also possible for a single PredicateNode. Think of predicates with
      // LogicalExpression and LogicalOperator::AND, for example.
      predicate_chains.emplace_back(std::move(current_predicate_chain));
    }
  }

  // (2) Substitute predicates with BetweenExpressions, if possible
  for (const auto& predicate_chain : predicate_chains) {
    _substitute_predicates_with_between_expressions(predicate_chain);
  }
}

/**
 * Looks for BinaryPredicateExpressions in @param predicate_chain, a vector of directly connected PredicateNodes and
 * replaces them with BetweenExpressions, if possible.
 * After the substitution, obsolete BinaryPredicateExpressions are removed.
 */
void BetweenCompositionRule::_substitute_predicates_with_between_expressions(const PredicateChain& predicate_chain) {
  // Store input and output nodes of the predicate chain
  auto predicate_chain_input_node = predicate_chain.front()->left_input();
  auto predicate_chain_output_nodes = predicate_chain.back()->outputs();
  const auto predicate_chain_output_input_sides = predicate_chain.back()->get_input_sides();

  auto between_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  auto predicate_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>();
  ExpressionUnorderedMap<std::vector<std::shared_ptr<ColumnBoundary>>> column_boundaries;

  auto id_counter = size_t{0};

  // Filter predicates with a boundary to the boundaries vector
  for (const auto& predicate_node : predicate_chain) {
    // A logical expression can contain multiple binary predicate expressions
    std::vector<std::shared_ptr<BinaryPredicateExpression>> expressions;
    const auto binary_predicate_expression =
        std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
    if (binary_predicate_expression) {
      expressions.push_back(binary_predicate_expression);
    } else {
      const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(predicate_node->predicate());
      Assert(!logical_expression || logical_expression->logical_operator != LogicalOperator::And,
             "Conjunctions should already have been split up");
      predicate_nodes.push_back(predicate_node);
    }

    for (const auto& expression : expressions) {
      const auto boundary = std::make_shared<ColumnBoundary>(_get_boundary(expression, id_counter));
      ++id_counter;
      if (boundary->type != ColumnBoundaryType::None) {
        if (boundary->boundary_is_column_expression) {
          const auto inverse_boundary = std::make_shared<ColumnBoundary>(_create_inverse_boundary(boundary));
          if (column_boundaries.find(inverse_boundary->column_expression) == column_boundaries.end()) {
            column_boundaries[inverse_boundary->column_expression] = std::vector<std::shared_ptr<ColumnBoundary>>();
          }
          column_boundaries[inverse_boundary->column_expression].push_back(inverse_boundary);
        }
        if (column_boundaries.find(boundary->column_expression) == column_boundaries.end()) {
          column_boundaries[boundary->column_expression] = std::vector<std::shared_ptr<ColumnBoundary>>();
        }
        column_boundaries[boundary->column_expression].push_back(boundary);
      } else {
        predicate_nodes.push_back(predicate_node);
      }
    }
    // Remove node from lqp in order to rearrange the nodes soon
    lqp_remove_node(predicate_node);
  }

  // Store the highest lower bound and the lowest upper bound for a column in order to get an optimal BetweenExpression
  std::shared_ptr<ColumnBoundary> lower_bound_value_expression;
  std::shared_ptr<ColumnBoundary> upper_bound_value_expression;
  auto consumed_boundary_ids = std::vector<size_t>();
  bool value_lower_inclusive = false;
  bool value_upper_inclusive = false;

  // libc++ and libstdc++ have different orderings for unordered_map. This results in nodes being inserted into the LQP
  // in arbitrary order. While these will be sorted by a different rule later, it can cause tests to fail.
  auto column_boundaries_sorted = std::vector<std::vector<std::shared_ptr<ColumnBoundary>>>{};
  column_boundaries_sorted.reserve(column_boundaries.size());
  for (auto& [column_expression, boundaries] : column_boundaries) {
    column_boundaries_sorted.emplace_back(std::move(boundaries));
  }
  column_boundaries.clear();
  std::sort(column_boundaries_sorted.begin(), column_boundaries_sorted.end(),
            [](const auto& left, const auto& right) { return left[0]->id < right[0]->id; });

  for (const auto& boundaries : column_boundaries_sorted) {
    for (const auto& boundary : boundaries) {
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
      }
    }

    if (lower_bound_value_expression && upper_bound_value_expression) {
      const auto lower_value_expression =
          std::static_pointer_cast<ValueExpression>(lower_bound_value_expression->border_expression);
      const auto upper_value_expression =
          std::static_pointer_cast<ValueExpression>(upper_bound_value_expression->border_expression);
      const auto between_node = PredicateNode::make(std::make_shared<BetweenExpression>(
          get_between_predicate_condition(value_lower_inclusive, value_upper_inclusive),
          boundaries[0]->column_expression, lower_value_expression, upper_value_expression));
      between_nodes.push_back(between_node);
      consumed_boundary_ids.push_back(lower_bound_value_expression->id);
      consumed_boundary_ids.push_back(upper_bound_value_expression->id);
      // Remove unnecessary value boundaries for this column
      for (const auto& value_boundary : boundaries) {
        if (!value_boundary->boundary_is_column_expression) {
          consumed_boundary_ids.push_back(value_boundary->id);
        }
      }
    }

    lower_bound_value_expression = nullptr;
    upper_bound_value_expression = nullptr;

    // Here, we could also generate BETWEEN expressions for when the lower and upper bounds are LQPColumnExpressions.
    // As the table scan does not yet support that and will revert to the ExpressionEvaluator, we don't do this and
    // use two separate predicates instead.
  }

  // If no substitution was possible, all nodes referring to this column have to be inserted into the LQP again
  // later. Therefore we create a semantically equal PredicateNode.
  for (const auto& boundaries : column_boundaries_sorted) {
    for (const auto& boundary : boundaries) {
      if (std::find(consumed_boundary_ids.begin(), consumed_boundary_ids.end(), boundary->id) ==
          consumed_boundary_ids.end()) {
        switch (boundary->type) {
          case ColumnBoundaryType::UpperBoundaryInclusive:
            predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                PredicateCondition::LessThanEquals, boundary->column_expression, boundary->border_expression)));
            break;
          case ColumnBoundaryType::LowerBoundaryInclusive:
            predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                PredicateCondition::GreaterThanEquals, boundary->column_expression, boundary->border_expression)));
            break;
          case ColumnBoundaryType::UpperBoundaryExclusive:
            predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                PredicateCondition::LessThan, boundary->column_expression, boundary->border_expression)));
            break;
          case ColumnBoundaryType::LowerBoundaryExclusive:
            predicate_nodes.push_back(PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
                PredicateCondition::GreaterThan, boundary->column_expression, boundary->border_expression)));
            break;
          default:
            break;
        }

        // Add the current boundary to consumed_boundary_ids so that we do not also add the inverse
        consumed_boundary_ids.emplace_back(boundary->id);
      }
    }
  }

  // Append between nodes to PredicateNodes to get the complete chain of all necessary LQP nodes
  predicate_nodes.insert(predicate_nodes.cend(), between_nodes.cbegin(), between_nodes.cend());

  // Insert PredicateNodes into the LQP
  //  Connect first PredicateNode to chain input
  predicate_nodes.front()->set_left_input(predicate_chain_input_node);

  //  Connect PredicateNodes among each other
  for (auto predicate_index = size_t{1}; predicate_index < predicate_nodes.size(); ++predicate_index) {
    predicate_nodes[predicate_index]->set_left_input(predicate_nodes[predicate_index - 1]);
  }

  //  Connect last PredicateNode with all output nodes of the chain
  for (size_t output_index = 0; output_index < predicate_chain_output_nodes.size(); ++output_index) {
    predicate_chain_output_nodes[output_index]->set_input(predicate_chain_output_input_sides[output_index],
                                                          predicate_nodes.back());
  }
}

/**
 * _get_boundary takes a BinaryPredicateExpression and the corresponding PredicateNode
 * as its input and returns a standardized ColumnBoundary. This function checks where the
 * LQPColumnExpression and the ValueExpression are stored in the BinaryPredicateExpression.
 * The expressions are transferred to a standardized ColumnBoundary format
 * and labelled with a ColumnBoundaryType that depends on their positions and the predicate condition
 * of the BinaryPredicateExpression
 *
 **/
BetweenCompositionRule::ColumnBoundary BetweenCompositionRule::_get_boundary(
    const std::shared_ptr<BinaryPredicateExpression>& expression, const size_t expression_id) {
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
    return {left_column_expression, value_expression, type, false, expression_id};
  }

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
    return {right_column_expression, value_expression, type, false, expression_id};
  }

  if (left_column_expression && right_column_expression) {
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
    return {left_column_expression, right_column_expression, type, true, expression_id};
  }

  return {nullptr, nullptr, ColumnBoundaryType::None, false, expression_id};
}

BetweenCompositionRule::ColumnBoundary BetweenCompositionRule::_create_inverse_boundary(
    const std::shared_ptr<ColumnBoundary>& column_boundary) {
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

}  // namespace hyrise
