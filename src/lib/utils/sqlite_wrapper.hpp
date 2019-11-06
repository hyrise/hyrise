#pragma once

#include <sqlite3.h>

#include <memory>
#include <string>

#include "storage/table.hpp"

namespace opossum {

/*
 * This class wraps the sqlite3 library for opossum. It creates an in-memory sqlite database on construction.
 * When executing a sql query, the wrapper converts the result into an opossum Table.
 */
class SQLiteWrapper final {
 public:
  SQLiteWrapper();

  /*
   * Recreates a table given another table to copy from.
   *
   * @param command SQL command string
   */
  void reset_table_from_copy(const std::string& table_name_to_reset, const std::string& table_name_to_copy_from) const;

  /*
   * Creates a table in the sqlite database from a given opossum Table
   *
   * @param table      The table to load into sqlite
   * @param tablename  The desired table name
   */
  void create_sqlite_table(const Table& table, const std::string& table_name);

  class Connection final {
   public:
    explicit Connection(const std::string& uri);

    Connection(const Connection&) = delete;
    Connection(Connection&&) noexcept;
    Connection& operator=(const Connection&) = delete;
    Connection& operator=(Connection&&) = delete;
    ~Connection();

    sqlite3* db{nullptr};

    /*
     * Executes a sql query in the sqlite database context and returns a Hyrise table.
     *
     * @param sql_query Query to be executed
     * @returns An opossum Table containing the results of the executed query
     */
    std::shared_ptr<Table> execute_query(const std::string& sql) const;

    /**
     * Execute an SQL statement on the wrapped sqlite db without invoking any Hyrise parts
     */
    void raw_execute_query(const std::string& sql, const bool allow_failure = false) const;
  };

  /**
   * Creates an additional connection to the same database
   */
  Connection new_connection() const;

 protected:
  std::string _uri;

 public:
  /*
   * Users can create their own connections with new_connection. Those connections can have their own transactions and
   * may run in parallel. If only a single connection is used (for example when importing data), the main connection is
   * used.
   */
  Connection main_connection;
};

}  // namespace opossum
