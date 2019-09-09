#include "static_table_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

StaticTableNode::StaticTableNode(const std::shared_ptr<Table>& table)
    : BaseNonQueryNode(LQPNodeType::StaticTable), table(table) {}

std::string StaticTableNode::description() const {
  std::ostringstream stream;

  stream << "[StaticTable]:"
         << " (";
  for (auto column_id = ColumnID{0}; column_id < table->column_definitions().size(); ++column_id) {
    const auto& column_definition = table->column_definitions()[column_id];
    stream << column_definition;

    if (column_id + 1u < table->column_definitions().size()) {
      stream << ", ";
    }
  }
  stream << ")";

  return stream.str();
}

size_t StaticTableNode::_shallow_hash() const {
  size_t hash{0};
  for (const auto& column_definition : table->column_definitions()) {
    boost::hash_combine(hash, column_definition.hash());
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> StaticTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return StaticTableNode::make(table);
}

bool StaticTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& static_table_node = static_cast<const StaticTableNode&>(rhs);
  return table->column_definitions() == static_table_node.table->column_definitions();
}

}  // namespace opossum
