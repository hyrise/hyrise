#include "create_table_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

CreateTableNode::CreateTableNode(const std::string& table_name, const TableColumnDefinitions& column_definitions,
                                 const bool if_not_exists)
    : BaseNonQueryNode(LQPNodeType::CreateTable),
      table_name(table_name),
      column_definitions(column_definitions),
      if_not_exists(if_not_exists) {}

std::string CreateTableNode::description() const {
  std::ostringstream stream;

  stream << "[CreateTable] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: '" << table_name << "' (";
  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    const auto& column_definition = column_definitions[column_id];

    stream << "'" << column_definition.name << "' " << column_definition.data_type << " ";
    if (column_definition.nullable) {
      stream << "NULL";
    } else {
      stream << "NOT NULL";
    }

    if (column_id + 1u < column_definitions.size()) {
      stream << ", ";
    }
  }
  stream << ")";

  return stream.str();
}

std::shared_ptr<AbstractLQPNode> CreateTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateTableNode::make(table_name, column_definitions, if_not_exists);
}

bool CreateTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_table_node = static_cast<const CreateTableNode&>(rhs);
  return table_name == create_table_node.table_name && column_definitions == create_table_node.column_definitions &&
         if_not_exists == create_table_node.if_not_exists;
}

}  // namespace opossum
