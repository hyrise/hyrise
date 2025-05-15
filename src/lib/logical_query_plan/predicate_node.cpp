#include "predicate_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "expression/expression_utils.hpp"
#include "expression/is_null_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/inclusion_dependency.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "types.hpp"

namespace hyrise {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractLQPNode(LQPNodeType::Predicate, {predicate}) {}

std::string PredicateNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  auto stream = std::stringstream{};
  stream << "[Predicate] " << predicate()->description(expression_mode);
  return stream.str();
}

UniqueColumnCombinations PredicateNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies PredicateNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

InclusionDependencies PredicateNode::inclusion_dependencies() const {
  auto inclusion_dependencies = InclusionDependencies{};
  const auto& is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate());

  // Only forward IND if the predicate is IS NOT NULL on a single-column IND's expression.
  if (is_null_expression && is_null_expression->predicate_condition == PredicateCondition::IsNotNull) {
    const auto input_inclusion_dependencies = left_input()->inclusion_dependencies();
    const auto& operand = is_null_expression->operand();

    for (const auto& input_inclusion_dependency : input_inclusion_dependencies) {
      if (input_inclusion_dependency.expressions.size() > 1) {
        continue;
      }

      if (*operand != *input_inclusion_dependency.expressions.front()) {
        continue;
      }

      inclusion_dependencies.emplace(input_inclusion_dependency);
    }
  }

  return inclusion_dependencies;
}

std::shared_ptr<AbstractExpression> PredicateNode::predicate() const {
  return node_expressions[0];
}

size_t PredicateNode::_on_shallow_hash() const {
  return std::hash<ScanType>{}(scan_type);
}

std::shared_ptr<AbstractLQPNode> PredicateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate(), node_mapping));
}

bool PredicateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  const auto equal =
      expression_equal_to_expression_in_different_lqp(*predicate(), *predicate_node.predicate(), node_mapping);

  return equal;
}

}  // namespace hyrise
