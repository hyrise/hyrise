#include "build_node.hpp"

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace hyrise {

BuildNode::BuildNode(const std::shared_ptr<AbstractExpression>& build_column, const DataType init_probe_data_type)
    : AbstractLQPNode{LQPNodeType::Build, {build_column}}, probe_data_type{init_probe_data_type} {}

std::string BuildNode::description(const DescriptionMode mode) const {
  return "[Build] " + build_column()->description(_expression_description_mode(mode));
}

UniqueColumnCombinations BuildNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies BuildNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

std::shared_ptr<AbstractExpression> BuildNode::build_column() const {
  return node_expressions[0];
}

size_t BuildNode::_on_shallow_hash() const {
  return boost::hash_value(probe_data_type);
}

std::shared_ptr<AbstractLQPNode> BuildNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return BuildNode::make(expression_copy_and_adapt_to_different_lqp(*build_column(), node_mapping), probe_data_type);
}

bool BuildNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& build_node = static_cast<const BuildNode&>(rhs);
  return probe_data_type == build_node.probe_data_type &&
         expression_equal_to_expression_in_different_lqp(*build_column(), *build_node.build_column(), node_mapping);
}

}  // namespace hyrise
