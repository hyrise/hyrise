#pragma once

#include <memory>
#include <string>

namespace opossum {

class SQLiteWrapper;

/**
 * Add indices to SQLite for existing data tables. This is used for the SQLite verification.
 * Without this, SQLite would take an enormous amount of time to execute certain queries.
 *
 * First, the existing tables are renamed so that tables with the original table names can be created again.
 * The schema file (sql) is then used to create the specified table schema.
 * Afterwards, indices are created with the create indices file (sql).
 * Finally, the data from the corresponding renamed tables is copied over to the newly created indexed tables.
 * 
 *
 * @param schema_file_path              the path to an SQL file which creates the data schema
 * @param create_indices_file_path      the path to an SQL file which creates indices (if separate, else "")
 * @param sqlite_wrapper                the used sqlite_wrapper
 */
void add_indices_to_sqlite(const std::string& schema_file_path, const std::string& create_indices_file_path,
                           std::shared_ptr<SQLiteWrapper>& sqlite_wrapper);

}  // namespace opossum
