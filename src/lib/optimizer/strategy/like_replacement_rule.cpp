#include "like_replacement_rule.hpp"

#include <algorithm>
#include <iostream>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string LikeReplacementRule::name() const { return "Like Replacement Rule"; }

void LikeReplacementRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  // Continue only if predicte_node is defined
  if (predicate_node == nullptr) {
    _apply_to_inputs(node);
    return;
  }
  const auto expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
  // Continue only if expressions predicate condition is of type PredicateCondition::Like
  if (expression == nullptr || expression->predicate_condition != PredicateCondition::Like) {
    _apply_to_inputs(node);
    return;
  }
  const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression->left_operand());
  const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression->right_operand());
  // Continue only if left expressions is column and right expression is value (column LIKE "asdf%")
  if (column_expression == nullptr || value_expression == nullptr) {
    _apply_to_inputs(node);
    return;
  }
  const auto value = boost::get<std::string>(value_expression->value);
  const auto offset = value.find_first_of("%");

  // Continue only if the string has a % wildcard and ends with the % wildcard ("asdf%")
  if (offset == std::string::npos || offset <= 0 || value.length() != offset + 1) {
    _apply_to_inputs(node);
    return;
  }
  // Calculate lower and upper bound of the string
  const auto lower_bound = value.substr(0, offset);
  const auto current_character_value = static_cast<int>(lower_bound.at(lower_bound.length() - 1));
  // Find next value according to ASCII-table
  if (current_character_value > 0 && current_character_value < 127) {
    const auto next_character = static_cast<char>(current_character_value + 1);
    const auto upper_bound = lower_bound.substr(0, offset - 1) + next_character;
    const auto lower_bound_node = PredicateNode::make(
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, column_expression,
                                                    std::make_shared<ValueExpression>(lower_bound)));
    const auto upper_bound_node = PredicateNode::make(std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::LessThan, column_expression, std::make_shared<ValueExpression>(upper_bound)));

    // Store the input and outputs of the node
    auto input = predicate_node->left_input();
    const auto outputs = predicate_node->outputs();
    const auto input_sides = predicate_node->get_input_sides();

    lqp_remove_node(predicate_node);

    // Connect the boundary nodes with the input and outputs of the replaced like node
    lower_bound_node->set_left_input(input);
    upper_bound_node->set_left_input(lower_bound_node);
    for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
      outputs[output_idx]->set_input(input_sides[output_idx], upper_bound_node);
    }
  }
  _apply_to_inputs(node);
}

}  // namespace opossum
