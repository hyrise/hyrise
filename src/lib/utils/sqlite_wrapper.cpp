#include "sqlite_wrapper.hpp"

#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "constant_mappings.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/string_utils.hpp"

namespace {

using namespace opossum;  // NOLINT
/*
 * Creates columns in given opossum table according to an sqlite intermediate statement (one result row).
 */
std::shared_ptr<Table> create_hyrise_table_from_result(sqlite3_stmt* sqlite_statement, int column_count) {
  std::vector<bool> column_nullable(column_count, false);
  std::vector<std::string> column_types(column_count, "");
  std::vector<std::string> column_names(column_count, "");

  bool no_result = true;
  int rc;
  while ((rc = sqlite3_step(sqlite_statement)) == SQLITE_ROW) {
    for (int column_id = 0; column_id < column_count; ++column_id) {
      if (no_result) {
        column_names[column_id] = sqlite3_column_name(sqlite_statement, column_id);
      }

      switch (sqlite3_column_type(sqlite_statement, column_id)) {
        case SQLITE_INTEGER: {
          column_types[column_id] = "int";
          break;
        }

        case SQLITE_FLOAT: {
          column_types[column_id] = "double";
          break;
        }

        case SQLITE_TEXT: {
          column_types[column_id] = "string";
          break;
        }

        case SQLITE_NULL: {
          column_nullable[column_id] = true;
          break;
        }

        case SQLITE_BLOB:
        default: {
          sqlite3_finalize(sqlite_statement);
          Fail("Column type not supported.");
        }
      }
    }
    no_result = false;

    // We cannot break here, as we still need to check whether the columns contain NULL values or not
  }

  // If this fails, this is likely because of a transaction conflict within sqlite. This means that the Hyrise
  // and the sqlite states have diverged. When using the SQLite wrapper, the caller must ensure that parallel
  // operations do not conflict.
  Assert(rc == SQLITE_ROW || rc == SQLITE_DONE, "Unexpected sqlite state");

  if (!no_result) {
    TableColumnDefinitions column_definitions;
    for (int column_id = 0; column_id < column_count; ++column_id) {
      if (column_types[column_id].empty()) {
        // Hyrise does not have explicit NULL columns
        column_types[column_id] = "int";
      }

      const auto data_type = data_type_to_string.right.at(column_types[column_id]);
      column_definitions.emplace_back(column_names[column_id], data_type, column_nullable[column_id]);
    }

    return std::make_shared<Table>(column_definitions, TableType::Data);
  }

  return nullptr;
}

/*
 * Adds a single row to given opossum table according to an sqlite intermediate statement (one result row).
 */
void copy_row_from_sqlite_to_hyrise(const std::shared_ptr<Table>& table, sqlite3_stmt* sqlite_statement,
                                    int column_count) {
  std::vector<AllTypeVariant> row;

  for (int column_id = 0; column_id < column_count; ++column_id) {
    switch (sqlite3_column_type(sqlite_statement, column_id)) {
      case SQLITE_INTEGER: {
        row.emplace_back(AllTypeVariant{sqlite3_column_int(sqlite_statement, column_id)});
        break;
      }

      case SQLITE_FLOAT: {
        row.emplace_back(AllTypeVariant{sqlite3_column_double(sqlite_statement, column_id)});
        break;
      }

      case SQLITE_TEXT: {
        row.emplace_back(AllTypeVariant{
            pmr_string(reinterpret_cast<const char*>(sqlite3_column_text(sqlite_statement, column_id)))});
        break;
      }

      case SQLITE_NULL: {
        row.emplace_back(NULL_VALUE);
        break;
      }

      case SQLITE_BLOB:
      default: {
        sqlite3_finalize(sqlite_statement);
        Fail("Column type not supported.");
      }
    }
  }

  table->append(row);
}

}  // namespace

namespace opossum {

// We include the address of this wrapper in the URI for the sqlite db so that two wrappers don't interfere
// See also https://www.sqlite.org/inmemorydb.html, starting at "If two or more distinct but shareable
// in-memory databases"
SQLiteWrapper::SQLiteWrapper()
    : _uri{std::string{"file:sqlitewrapper_"} + std::to_string(reinterpret_cast<uintptr_t>(this)) +  // NOLINT
           "?mode=memory&cache=shared"},
      main_connection{_uri} {
  Assert(sqlite3_threadsafe(), "Expected sqlite to be compiled with thread-safety");
}

SQLiteWrapper::Connection::Connection(const std::string& uri) {
  // Explicity set parallel mode. On Linux it seems to be the default, on Mac, it seems to make a difference.
  const auto ret =
      sqlite3_open_v2(uri.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,  // NOLINT
                      nullptr);
  if (ret != SQLITE_OK) {
    sqlite3_close(db);
    Fail("Cannot open database: " + std::string(sqlite3_errmsg(db)));
  }

  // Make LIKE case sensitive, just like in Hyrise
  raw_execute_query("PRAGMA case_sensitive_like = true;");
}

SQLiteWrapper::Connection::Connection(Connection&& other) noexcept : db(other.db) { other.db = nullptr; }

SQLiteWrapper::Connection::~Connection() {
  if (db) {
    sqlite3_close(db);
  }
}

SQLiteWrapper::Connection SQLiteWrapper::new_connection() const { return Connection{_uri}; }

std::shared_ptr<Table> SQLiteWrapper::Connection::execute_query(const std::string& sql) const {
  sqlite3_stmt* sqlite_statement;

  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  const auto& queries = sql_pipeline.get_sql_per_statement();

  // We need to split the queries such that we only create columns/add rows from the final SELECT query
  std::vector<std::string> queries_before_select(queries.begin(), queries.end() - 1);
  std::string select_query = queries.back();

  int rc;
  for (const auto& query : queries_before_select) {
    rc = sqlite3_prepare_v2(db, query.c_str(), -1, &sqlite_statement, nullptr);

    if (rc != SQLITE_OK) {
      sqlite3_finalize(sqlite_statement);
      Fail("Failed to execute query \"" + query + "\": " + std::string(sqlite3_errmsg(db)) + "\n");
    }

    while ((rc = sqlite3_step(sqlite_statement)) != SQLITE_DONE) {
    }
  }

  rc = sqlite3_prepare_v2(db, select_query.c_str(), -1, &sqlite_statement, nullptr);

  if (rc != SQLITE_OK) {
    auto error_message = "Failed to execute query \"" + select_query + "\": " + std::string(sqlite3_errmsg(db));
    sqlite3_finalize(sqlite_statement);
    Fail(error_message);
  }

  auto result_table = create_hyrise_table_from_result(sqlite_statement, sqlite3_column_count(sqlite_statement));

  if (!sqlite3_stmt_readonly(sqlite_statement)) {
    // We need to make sure that we do not call sqlite3_reset below on a modifying statement - otherwise, we would
    // re-execute the modification
    Assert(!result_table, "Modifying statement was expected to return empty table");
    sqlite3_finalize(sqlite_statement);
    return nullptr;
  }

  sqlite3_reset(sqlite_statement);

  while ((rc = sqlite3_step(sqlite_statement)) == SQLITE_ROW) {
    copy_row_from_sqlite_to_hyrise(result_table, sqlite_statement, sqlite3_column_count(sqlite_statement));
  }

  Assert(rc == SQLITE_DONE, "Unexpected sqlite state");

  sqlite3_finalize(sqlite_statement);
  return result_table;
}

void SQLiteWrapper::Connection::raw_execute_query(const std::string& sql, const bool allow_failure) const {
  char* err_msg;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg);

  if (rc != SQLITE_OK) {
    auto msg = std::string(err_msg);
    sqlite3_free(err_msg);
    Fail("Failed to execute query (" + sql + "). SQL error: " + msg + "\n");
  }
}

void SQLiteWrapper::create_sqlite_table(const Table& table, const std::string& table_name) {
  std::vector<std::string> column_types;

  for (const auto& column_definition : table.column_definitions()) {
    switch (column_definition.data_type) {
      case DataType::Int:
      case DataType::Long:
        column_types.emplace_back("INT");
        break;
      case DataType::Float:
      case DataType::Double:
        column_types.emplace_back("REAL");
        break;
      case DataType::String:
        column_types.emplace_back("TEXT");
        break;
      case DataType::Null:
        Fail("SQLiteWrapper: column type not supported.");
        break;
    }
  }

  // SQLite doesn't like an unescaped "ORDER" as a table name, thus we escape all table names.
  const auto escaped_table_name = std::string{"\""} + table_name + "\"";

  std::stringstream create_table_query;
  create_table_query << "CREATE TABLE " << escaped_table_name << "(";
  for (auto column_id = ColumnID{0}; column_id < table.column_definitions().size(); column_id++) {
    create_table_query << table.column_definitions()[column_id].name << " " << column_types[column_id];

    if (column_id + 1u < table.column_definitions().size()) {
      create_table_query << ", ";
    }
  }
  create_table_query << ");";
  main_connection.raw_execute_query(create_table_query.str());

  // Prepare `INSERT INTO <table_name> VALUES (?, ?, ...)` statement
  std::stringstream insert_into_stream;
  insert_into_stream << "INSERT INTO " << escaped_table_name << " VALUES (";
  for (auto column_id = ColumnID{0}; column_id < table.column_count(); column_id++) {
    insert_into_stream << "?";
    if (static_cast<ColumnCount>(column_id + 1u) < table.column_count()) {
      insert_into_stream << ", ";
    }
  }
  insert_into_stream << ");";
  const auto insert_into_str = insert_into_stream.str();

  sqlite3_stmt* insert_into_statement;
  const auto sqlite3_prepare_return_code =
      sqlite3_prepare_v2(main_connection.db, insert_into_str.c_str(), static_cast<int>(insert_into_str.size() + 1),
                         &insert_into_statement, nullptr);
  Assert(sqlite3_prepare_return_code == SQLITE_OK,
         "Failed to prepare statement: " + std::string(sqlite3_errmsg(main_connection.db)));

  // Insert values row-by-row
  const auto chunk_count = table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    if (!chunk) continue;

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
      for (auto column_id = ColumnID{0}; column_id < table.column_count(); column_id++) {
        const auto segment = chunk->get_segment(column_id);
        // SQLite's parameter indices are 1-based.
        const auto sqlite_column_id = static_cast<int>(column_id) + 1;
        const auto value = (*segment)[chunk_offset];

        auto sqlite3_bind_return_code = int{};

        if (variant_is_null(value)) {
          sqlite3_bind_return_code = sqlite3_bind_null(insert_into_statement, sqlite_column_id);
        } else {
          switch (table.column_data_type(column_id)) {
            case DataType::Int: {
              sqlite3_bind_return_code =
                  sqlite3_bind_int(insert_into_statement, sqlite_column_id, boost::get<int32_t>(value));
            } break;
            case DataType::Long:
              sqlite3_bind_return_code =
                  sqlite3_bind_int64(insert_into_statement, sqlite_column_id, boost::get<int64_t>(value));
              break;
            case DataType::Float:
              sqlite3_bind_return_code =
                  sqlite3_bind_double(insert_into_statement, sqlite_column_id, boost::get<float>(value));
              break;
            case DataType::Double:
              sqlite3_bind_return_code =
                  sqlite3_bind_double(insert_into_statement, sqlite_column_id, boost::get<double>(value));
              break;
            case DataType::String: {
              const auto& string_value = boost::get<pmr_string>(value);
              // clang-tidy doesn't like SQLITE_TRANSIENT
              // clang-format off
              sqlite3_bind_return_code = sqlite3_bind_text(insert_into_statement, sqlite_column_id, string_value.c_str(), static_cast<int>(string_value.size()), SQLITE_TRANSIENT);  // NOLINT
              // clang-format on
            } break;
            case DataType::Null:
              Fail("SQLiteWrapper: column type not supported.");
              break;
          }
        }

        Assert(sqlite3_bind_return_code == SQLITE_OK,
               "Failed to bind value: " + std::string(sqlite3_errmsg(main_connection.db)));
      }

      const auto sqlite3_step_return_code = sqlite3_step(insert_into_statement);
      Assert(sqlite3_step_return_code == SQLITE_DONE,
             "Failed to step INSERT: " + std::string(sqlite3_errmsg(main_connection.db)));

      const auto sqlite3_reset_return_code = sqlite3_reset(insert_into_statement);
      Assert(sqlite3_reset_return_code == SQLITE_OK,
             "Failed to reset statement: " + std::string(sqlite3_errmsg(main_connection.db)));
    }
  }

  sqlite3_finalize(insert_into_statement);
}

void SQLiteWrapper::reset_table_from_copy(const std::string& table_name_to_reset,
                                          const std::string& table_name_to_copy_from) const {
  std::stringstream command_ss;
  command_ss << "DROP TABLE IF EXISTS " << table_name_to_reset << ";";
  command_ss << "CREATE TABLE " << table_name_to_reset << " AS SELECT * FROM " << table_name_to_copy_from << ";";

  main_connection.raw_execute_query(command_ss.str());
}

}  // namespace opossum
