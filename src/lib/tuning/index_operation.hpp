#pragma once

#include <string>

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
};

}  // namespace opossum
