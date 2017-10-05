#pragma once

#include <sqlite3.h>

#include <memory>

#include "storage/table.hpp"

namespace opossum {

class SqliteWrapper {
 public:
  SqliteWrapper();
  ~SqliteWrapper();

  void create_table_from_tbl(const std::string& file, const std::string& tablename);
  std::shared_ptr<Table> execute_query(const std::string& sql_query);

 protected:
  void _create_columns(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count);
  void _add_row(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count);

  sqlite3* _db;
};

}  // namespace opossum
