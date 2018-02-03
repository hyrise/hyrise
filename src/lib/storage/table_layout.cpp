#include "table_layout.hpp"

#include "types.hpp"

namespace opossum {

TableColumnDefinition::TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable): name(name), data_type(data_type), nullable(nullable) {
  Assert((name.size() < std::numeric_limits<ColumnNameLength>::max()), "Cannot add column. Column name is too long.");
}

}  // namespace opossum
