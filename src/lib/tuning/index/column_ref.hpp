#pragma once

#include <string>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * A ColumnRef is used to reference one specific column by the name of its table
 * and its index in that table
 */
struct ColumnRef {
  ColumnRef(std::string table_name, ColumnID column_id) : table_name{table_name}, column_id{column_id} {}

  std::string table_name;
  ColumnID column_id;

  bool operator<(const ColumnRef& other) const {
    return (table_name == other.table_name) ? column_id < other.column_id : table_name < other.table_name;
  }
  bool operator>(const ColumnRef& other) const {
    return (table_name == other.table_name) ? column_id > other.column_id : table_name > other.table_name;
  }

  /**
   * Operator for printing them (debugging)
   */
  friend std::ostream& operator<<(std::ostream& output, const ColumnRef& column_ref) {
    auto table_ptr = StorageManager::get().get_table(column_ref.table_name);
    auto& column_name = table_ptr->column_name(column_ref.column_id);
    return output << column_ref.table_name << "." << column_name;
  }
};

}  // namespace opossum
