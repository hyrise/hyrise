#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

OrderByDefinition::OrderByDefinition(const ColumnID column_id, const OrderByMode order_by_mode)
    : column_id(column_id), order_by_mode(order_by_mode) {}

SortNode::SortNode(const std::vector<OrderByDefinition>& order_by_definitions)
    : AbstractASTNode(ASTNodeType::Sort), _order_by_definitions(order_by_definitions) {}

std::string SortNode::description(DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Sort] ";
  if (mode == DescriptionMode::MultiLine) {
    desc << "\n";
  }

  for (size_t definition_idx = 0; definition_idx < _order_by_definitions.size(); ++definition_idx) {
    const auto& definition = _order_by_definitions[definition_idx];

    desc << get_verbose_column_name(definition.column_id);
    desc << " (" << order_by_mode_to_string.at(definition.order_by_mode) + ")";

    if (definition_idx + 1 < _order_by_definitions.size()) {
      if (mode == DescriptionMode::SingleLine) {
        desc << ", ";
      } else {
        desc << "\n";
      }
    }
  }

  return desc.str();
}

const std::vector<OrderByDefinition>& SortNode::order_by_definitions() const { return _order_by_definitions; }

}  // namespace opossum
