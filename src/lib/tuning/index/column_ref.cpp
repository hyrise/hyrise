#include "column_ref.hpp"

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

ColumnRef::ColumnRef(std::string table_name, ColumnID column_id) : table_name{table_name}, column_ids{column_id} {}

ColumnRef::ColumnRef(std::string table_name, const std::vector<ColumnID>& column_ids)
    : table_name{table_name}, column_ids{column_ids} {}

bool ColumnRef::operator<(const ColumnRef& other) const {
  return (table_name == other.table_name) ? column_ids < other.column_ids : table_name < other.table_name;
}
bool ColumnRef::operator>(const ColumnRef& other) const {
  return (table_name == other.table_name) ? column_ids > other.column_ids : table_name > other.table_name;
}

std::ostream& operator<<(std::ostream& output, const ColumnRef& column_ref) {
  auto table_ptr = StorageManager::get().get_table(column_ref.table_name);
  output << column_ref.table_name << ".(";
  for (auto i_column = 0u; i_column < column_ref.column_ids.size(); ++i_column) {
    auto& column_name = table_ptr->column_name(column_ref.column_ids[i_column]);
    if (i_column != 0u) {
      output << ", ";
    }
    output << column_name;
  }
  return output << ")";
}

bool ColumnRef::operator==(const ColumnRef& other) const {
  return other.table_name == table_name && other.column_ids == column_ids;
}

}  // namespace opossum
