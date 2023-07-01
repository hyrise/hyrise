#include "table_column_definition.hpp"

namespace hyrise {

TableColumnDefinition::TableColumnDefinition(const std::string& init_name, const DataType init_data_type,
                                             const bool init_nullable)
    : name(init_name), data_type(init_data_type), nullable(init_nullable) {}

bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
  return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
}

size_t TableColumnDefinition::hash() const {
  auto hash = boost::hash_value(name);
  boost::hash_combine(hash, data_type);
  boost::hash_combine(hash, nullable);
  return hash;
}
}  // namespace hyrise
