#pragma once

#include <string>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Encapsulates a creation or deletion operation of an index
 */
class IndexOperation {
 public:
  IndexOperation(const std::string& table_name, ColumnID column_id, bool create)
      : table_name{table_name}, column_id{column_id}, create{create} {}

  /**
   * The column the this operation refers to
   */
  std::string table_name;
  ColumnID column_id;

  /**
   * true: create index, false: remove index
   */
  bool create;

  /**
   * Operator for printing them (debugging)
   */
  friend std::ostream& operator<<(std::ostream& output, const IndexOperation& operation) {
    auto table_ptr = StorageManager::get().get_table(operation.table_name);
    auto& column_name = table_ptr->column_name(operation.column_id);

    std::string operation_string = operation.create ? "create" : "remove";

    return output << "IndexOperation " << operation_string << " " << operation.table_name << "." << column_name;
  }
};

}  // namespace opossum
