#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const std::string &column_name, const OrderByMode order_by_mode)
    : column_name(column_name), order_by_mode(order_by_mode) {}

SortNode::SortNode(const std::vector<OrderByDefinition> &order_by_definitions)
    : AbstractASTNode(ASTNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::string SortNode::description() const {
  std::ostringstream s;

  auto stream_aggregate = [&](const OrderByDefinition &definition) {
    s << definition.column_name << " (" << order_by_mode_to_string.at(definition.order_by_mode) + ")";
  };

  s << "Sort: ";

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

const std::vector<OrderByDefinition> &SortNode::order_by_definitions() const { return _order_by_definitions; }

}  // namespace opossum
