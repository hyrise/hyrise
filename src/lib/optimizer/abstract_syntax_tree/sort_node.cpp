#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const ColumnID column_id, const OrderByMode order_by_mode)
    : column_id(column_id), order_by_mode(order_by_mode) {}

SortNode::SortNode(const OrderByDefinitions& order_by_definitions)
    : AbstractASTNode(ASTNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::string SortNode::description() const {
  std::ostringstream s;

  s << "[Sort] ";

  auto stream_aggregate = [&](const OrderByDefinition& definition) {
    s << get_verbose_column_name(definition.column_id);
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

void SortNode::map_column_ids(const ColumnIDMapping& column_id_mapping,
                              ASTChildSide caller_child_side) {
  DebugAssert(left_child(), "Input needs to be set to perform this operation. Mostly because we can't validate the size of column_id_mapping otherwise.");
  DebugAssert(column_id_mapping.size() == left_child()->output_col_count(), "Invalid column_id_mapping");

  for (auto& order_by_definition : _order_by_definitions) {
    order_by_definition.column_id = column_id_mapping[order_by_definition.column_id];
  }

  _propagate_column_id_mapping_to_parent(column_id_mapping);
}

}  // namespace opossum
