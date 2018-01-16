#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const LQPColumnReference& column_reference, const OrderByMode order_by_mode)
    : column_reference(column_reference), order_by_mode(order_by_mode) {}

SortNode::SortNode(const OrderByDefinitions& order_by_definitions)
    : AbstractLQPNode(LQPNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::shared_ptr<AbstractLQPNode> SortNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  OrderByDefinitions order_by_definitions;
  order_by_definitions.reserve(_order_by_definitions.size());

  for (const auto& order_by_definition : _order_by_definitions) {
    const auto column_reference =
        adapt_column_reference_to_different_lqp(order_by_definition.column_reference, left_child(), copied_left_child);
    order_by_definitions.emplace_back(column_reference, order_by_definition.order_by_mode);
  }

  return std::make_shared<SortNode>(order_by_definitions);
}

std::string SortNode::description() const {
  std::ostringstream s;

  s << "[Sort] ";

  auto stream_aggregate = [&](const OrderByDefinition& definition) {
    s << definition.column_reference.description();
    s << " (" << order_by_mode_to_string.at(definition.order_by_mode) + ")";
  };

  auto it = _order_by_definitions.begin();
  if (it != _order_by_definitions.end()) {
    stream_aggregate(*it);
    ++it;
  }

  for (; it != _order_by_definitions.end(); ++it) {
    s << ", ";
    stream_aggregate(*it);
  }

  return s.str();
}

const OrderByDefinitions& SortNode::order_by_definitions() const { return _order_by_definitions; }

}  // namespace opossum
