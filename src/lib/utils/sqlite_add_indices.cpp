#include "sqlite_add_indices.hpp"

#include <fstream>
#include <iostream>
#include <string>

#include "operators/print.hpp"

#include "hyrise.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/sqlite_wrapper.hpp"
#include "utils/timer.hpp"

namespace opossum {

void add_indices_to_sqlite(const std::string& schema_file_path, const std::string& create_indices_file_path,
                           std::shared_ptr<SQLiteWrapper>& sqlite_wrapper) {
  Assert(sqlite_wrapper, "sqlite_wrapper should be set");

  std::cout << "- Adding indexes to SQLite" << std::endl;
  Timer timer;

  // SQLite does not support adding primary keys to non-empty tables, so we rename the table, create an empty one from
  // the provided schema and copy the data.
  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    // SQLite doesn't like an unescaped "ORDER" as a table name, thus we escape it. No need to escape the
    // "..._unindexed" name.
    const auto escaped_table_name = std::string{"\""} + table_name + "\"";

    sqlite_wrapper->main_connection.raw_execute_query(std::string{"ALTER TABLE "}
                                                          .append(escaped_table_name)
                                                          .append(" RENAME TO ")
                                                          .append(table_name)
                                                          .append("_unindexed"));
  }

  // Recreate tables using the passed schema sql file
  std::ifstream schema_file(schema_file_path);
  Assert(schema_file.good(), std::string{"Schema file "} + schema_file_path +
                                 " not found - try running the binary from the Hyrise root or the build directory");
  std::string schema_sql((std::istreambuf_iterator<char>(schema_file)), std::istreambuf_iterator<char>());
  sqlite_wrapper->main_connection.raw_execute_query(schema_sql);

  // If indices are not part of the schema file, add them here
  if (!create_indices_file_path.empty()) {
    std::ifstream create_indices_file(create_indices_file_path);
    Assert(create_indices_file.good(),
           std::string{"Index file "} + create_indices_file_path +
               " not found - try running the binary from the Hyrise root or the build directory");
    std::string create_indices_sql((std::istreambuf_iterator<char>(create_indices_file)),
                                   std::istreambuf_iterator<char>());
    sqlite_wrapper->main_connection.raw_execute_query(create_indices_sql);
  }

  // Copy over data
  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    Timer per_table_time;
    std::cout << "-  Adding indexes to SQLite table " << table_name << std::flush;

    const auto escaped_table_name = std::string{"\""} + table_name + "\"";

    sqlite_wrapper->main_connection.raw_execute_query(std::string{"INSERT INTO "}
                                                          .append(escaped_table_name)
                                                          .append(" SELECT * FROM ")
                                                          .append(table_name)
                                                          .append("_unindexed"));

    std::cout << " (" << per_table_time.lap_formatted() << ")" << std::endl;
  }

  std::cout << "- Added indexes to SQLite (" << timer.lap_formatted() << ")" << std::endl;
}

}  // namespace opossum
