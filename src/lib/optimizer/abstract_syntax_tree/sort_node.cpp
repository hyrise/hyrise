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

std::string SortNode::description(DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Sort] ";
  if (mode == DescriptionMode::MultiLine) {
    desc << "\n";
  }

  for (size_t definition_idx = 0; definition_idx < _order_by_definitions.size(); ++definition_idx) {
    const auto& definition = _order_by_definitions[definition_idx];

    desc << get_qualified_column_name(definition.column_id);
    desc << " (" << order_by_mode_to_string.at(definition.order_by_mode) + ")";

    if (definition_idx + 1 < _order_by_definitions.size()) {
      desc << (mode == DescriptionMode::SingleLine ? ", " : "\n");
    }
  }

  return desc.str();
}

const OrderByDefinitions& SortNode::order_by_definitions() const { return _order_by_definitions; }

void SortNode::map_column_ids(const ColumnIDMapping& column_id_mapping, ASTChildSide caller_child_side) {
  DebugAssert(left_child(),
              "Input needs to be set to perform this operation. Mostly because we can't validate the size of "
              "column_id_mapping otherwise.");
  DebugAssert(column_id_mapping.size() == left_child()->output_column_count(), "Invalid column_id_mapping");

  for (auto& order_by_definition : _order_by_definitions) {
    order_by_definition.column_id = column_id_mapping[order_by_definition.column_id];
  }

  _propagate_column_id_mapping_to_parents(column_id_mapping);
}

}  // namespace opossum
