#include "sort_node.hpp"

#include <boost/lexical_cast.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"

#include "types.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const ColumnID column_id, const OrderByMode order_by_mode)
    : column_id(column_id), order_by_mode(order_by_mode) {}

SortNode::SortNode(const std::vector<OrderByDefinition> &order_by_definitions)
    : AbstractASTNode(ASTNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::string SortNode::description() const {
  std::ostringstream s;

  auto stream_aggregate = [&](const OrderByDefinition &definition) {
    s << boost::lexical_cast<std::string>(definition.column_id);
    s << " (" << order_by_mode_to_string.at(definition.order_by_mode) + ")";
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
