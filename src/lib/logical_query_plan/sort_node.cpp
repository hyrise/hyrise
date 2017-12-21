#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const ColumnOrigin& column_origin, const OrderByMode order_by_mode)
    : column_origin(column_origin), order_by_mode(order_by_mode) {}

SortNode::SortNode(const std::vector<OrderByDefinition>& order_by_definitions)
    : AbstractLQPNode(LQPNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::shared_ptr<AbstractLQPNode> SortNode::_deep_copy_impl(const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  return std::make_shared<SortNode>(_order_by_definitions);
}

std::string SortNode::description() const {
  std::ostringstream s;

  s << "[Sort] ";

  auto stream_aggregate = [&](const OrderByDefinition& definition) {
    s << definition.column_origin.get_verbose_name();
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

const std::vector<OrderByDefinition>& SortNode::order_by_definitions() const { return _order_by_definitions; }

}  // namespace opossum
