#include "indexable_column_set.hpp"

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

IndexableColumnSet::IndexableColumnSet(std::string table_name, ColumnID column_id)
    : table_name{table_name}, column_ids{column_id} {}

IndexableColumnSet::IndexableColumnSet(std::string table_name, const std::vector<ColumnID>& column_ids)
    : table_name{table_name}, column_ids{column_ids} {}

bool IndexableColumnSet::operator<(const IndexableColumnSet& other) const {
  return (table_name == other.table_name) ? column_ids < other.column_ids : table_name < other.table_name;
}

bool IndexableColumnSet::operator==(const IndexableColumnSet& other) const {
  return other.table_name == table_name && other.column_ids == column_ids;
}

std::ostream& operator<<(std::ostream& output, const IndexableColumnSet& indexable_column_set) {
  auto table_ptr = StorageManager::get().get_table(indexable_column_set.table_name);
  output << indexable_column_set.table_name << ".(";
  for (auto i_column = 0u; i_column < indexable_column_set.column_ids.size(); ++i_column) {
    auto& column_name = table_ptr->column_name(indexable_column_set.column_ids[i_column]);
    if (i_column != 0u) {
      output << ", ";
    }
    output << column_name;
  }
  return output << ")";
}

}  // namespace opossum
