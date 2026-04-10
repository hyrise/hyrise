#include "probe_node.hpp"

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"

namespace hyrise {

ProbeNode::ProbeNode(const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractLQPNode{LQPNodeType::Probe, {predicate}} {}

std::string ProbeNode::description(const DescriptionMode mode) const {
  return "[Probe] " + predicate()->description(_expression_description_mode(mode));
}

std::vector<std::shared_ptr<AbstractExpression>> ProbeNode::output_expressions() const {
  return left_input()->output_expressions();
}

UniqueColumnCombinations ProbeNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies ProbeNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

std::shared_ptr<AbstractExpression> ProbeNode::predicate() const {
  return node_expressions[0];
}

bool ProbeNode::is_column_nullable(const ColumnID column_id) const {
  return left_input()->is_column_nullable(column_id);
}

FunctionalDependencies ProbeNode::non_trivial_functional_dependencies() const {
  return left_input()->non_trivial_functional_dependencies();
}

std::shared_ptr<AbstractLQPNode> ProbeNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ProbeNode::make(expression_copy_and_adapt_to_different_lqp(*predicate(), node_mapping));
}

bool ProbeNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& probe_node = static_cast<const ProbeNode&>(rhs);
  return expression_equal_to_expression_in_different_lqp(*predicate(), *probe_node.predicate(), node_mapping);
}

}  // namespace hyrise
