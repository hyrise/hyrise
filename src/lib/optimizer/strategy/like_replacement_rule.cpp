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
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string LikeReplacementRule::name() const { return "Like Replacement Rule"; }

constexpr int MAX_ASCII_VALUE = 127;

void LikeReplacementRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  // Continue only on predicate nodes
  if (!predicate_node) {
    _apply_to_inputs(node);
    return;
  }

  // Continue only if expressions predicate condition is of type PredicateCondition::Like
  const auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
  if (!predicate || predicate->predicate_condition != PredicateCondition::Like) {
    _apply_to_inputs(node);
    return;
  }

  // Continue only if right predicate is value (expr LIKE "asdf%")
  const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->right_operand());
  if (!value_expression) {
    _apply_to_inputs(node);
    return;
  }

  // Filter strings containing "_", as they cannot be reformulated using this approach
  const auto value = boost::get<pmr_string>(value_expression->value);
  if (value.find_first_of('_') != pmr_string::npos) {
    _apply_to_inputs(node);
    return;
  }

  // Continue only if the string has a % wildcard and ends with the % wildcard ("asdf%") and has a
  // non-empty prefix before the wildcard
  const auto offset = value.find_first_of('%');
  if (offset == std::string::npos || offset <= 0 || value.length() != offset + 1) {
    _apply_to_inputs(node);
    return;
  }

  // Calculate lower and upper bound of the search pattern
  const auto lower_bound = value.substr(0, offset);
  const auto current_character_value = static_cast<int>(lower_bound.at(lower_bound.length() - 1));

  // Find next value according to ASCII-table
  if (current_character_value < MAX_ASCII_VALUE) {
    // TODO(anyone): Replace with right inclusive between
    const auto next_character = static_cast<char>(current_character_value + 1);
    const auto upper_bound = lower_bound.substr(0, offset - 1) + next_character;
    const auto lower_bound_node = PredicateNode::make(greater_than_equals_(predicate->left_operand(), lower_bound));
    const auto upper_bound_node = PredicateNode::make(less_than_(predicate->left_operand(), upper_bound));

    // Store the input and outputs of the like predicate node
    auto input = predicate_node->left_input();
    const auto outputs = predicate_node->outputs();
    const auto input_sides = predicate_node->get_input_sides();

    // Connect the lower_bound and upper_bound nodes with the input and outputs of the replaced like node
    lower_bound_node->set_left_input(input);
    upper_bound_node->set_left_input(lower_bound_node);
    for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
      outputs[output_idx]->set_input(input_sides[output_idx], upper_bound_node);
    }
    _apply_to_inputs(node);
    lqp_remove_node(predicate_node);
  }

  _apply_to_inputs(node);
}

}  // namespace opossum
