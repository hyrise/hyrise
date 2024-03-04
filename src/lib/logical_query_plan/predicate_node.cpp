#include "predicate_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "operators/operator_scan_predicate.hpp"

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
