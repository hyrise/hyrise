#include "sqlite_wrapper.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/string_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

}  // namespace

namespace opossum {

SQLiteWrapper::SQLiteWrapper() {
  const auto r = sqlite3_open(":memory:", &_db);
  if (r != SQLITE_OK) {
    sqlite3_close(_db);
    Fail("Cannot open database: " + std::string(sqlite3_errmsg(_db)));
  }
}

SQLiteWrapper::~SQLiteWrapper() { sqlite3_close(_db); }

void SQLiteWrapper::create_table(const Table& table, const std::string& table_name) {
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
      case DataType::Bool:
        Fail("SQLiteWrapper: column type not supported.");
        break;
    }
  }

  std::stringstream create_table_query;
  create_table_query << "CREATE TABLE " << table_name << "(";
  for (auto column_id = ColumnID{0}; column_id < table.column_definitions().size(); column_id++) {
    create_table_query << table.column_definitions()[column_id].name << " " << column_types[column_id];

    if (column_id + 1u < table.column_definitions().size()) {
      create_table_query << ", ";
    }
  }
  create_table_query << ");";
  _exec_sql(create_table_query.str());

  // Prepare `INSERT INTO <table_name> VALUES (?, ?, ...)` statement
  std::stringstream insert_into_stream;
  insert_into_stream << "INSERT INTO " << table_name << " VALUES (";
  for (auto column_id = ColumnID{0}; column_id < table.column_count(); column_id++) {
    insert_into_stream << "?";
    if (column_id + 1u < table.column_count()) {
      insert_into_stream << ", ";
    }
  }
  insert_into_stream << ");";
  const auto insert_into_str = insert_into_stream.str();

  sqlite3_stmt* insert_into_statement;
  const auto sqlite3_prepare_return_code = sqlite3_prepare_v2(
      _db, insert_into_str.c_str(), static_cast<int>(insert_into_str.size() + 1), &insert_into_statement, nullptr);
  Assert(sqlite3_prepare_return_code == SQLITE_OK, "Failed to prepare statement: " + std::string(sqlite3_errmsg(_db)));

  // Insert
  for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);

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
              const auto& string_value = boost::get<std::string>(value);
              // clang-tidy doesn't like SQLITE_TRANSIENT
              // clang-format off
              sqlite3_bind_return_code = sqlite3_bind_text(insert_into_statement, sqlite_column_id, string_value.c_str(), static_cast<int>(string_value.size()), SQLITE_TRANSIENT);  // NOLINT
              // clang-format on
            } break;
            case DataType::Null:
            case DataType::Bool:
              Fail("SQLiteWrapper: column type not supported.");
              break;
          }
        }

        Assert(sqlite3_bind_return_code == SQLITE_OK, "Failed to bind value: " + std::string(sqlite3_errmsg(_db)));
      }

      const auto sqlite3_step_return_code = sqlite3_step(insert_into_statement);
      Assert(sqlite3_step_return_code == SQLITE_DONE, "Failed to step INSERT: " + std::string(sqlite3_errmsg(_db)));

      const auto sqlite3_reset_return_code = sqlite3_reset(insert_into_statement);
      Assert(sqlite3_reset_return_code == SQLITE_OK, "Failed to reset statement: " + std::string(sqlite3_errmsg(_db)));
    }
  }

  sqlite3_finalize(insert_into_statement);
}

std::shared_ptr<Table> SQLiteWrapper::execute_query(const std::string& sql_query) {
  sqlite3_stmt* result_row;

  auto sql_pipeline = SQLPipelineBuilder{sql_query}.create_pipeline();
  const auto& queries = sql_pipeline.get_sql_strings();

  // We need to split the queries such that we only create columns/add rows from the final SELECT query
  std::vector<std::string> queries_before_select(queries.begin(), queries.end() - 1);
  std::string select_query = queries.back();

  int rc;
  for (const auto& query : queries_before_select) {
    rc = sqlite3_prepare_v2(_db, query.c_str(), -1, &result_row, nullptr);

    if (rc != SQLITE_OK) {
      sqlite3_finalize(result_row);
      sqlite3_close(_db);
      throw std::runtime_error("Failed to execute query \"" + query + "\": " + std::string(sqlite3_errmsg(_db)) + "\n");
    }

    while ((rc = sqlite3_step(result_row)) != SQLITE_DONE) {
    }
  }

  rc = sqlite3_prepare_v2(_db, select_query.c_str(), -1, &result_row, nullptr);

  if (rc != SQLITE_OK) {
    auto error_message = "Failed to execute query \"" + select_query + "\": " + std::string(sqlite3_errmsg(_db));
    sqlite3_finalize(result_row);
    sqlite3_close(_db);
    throw std::runtime_error(error_message);
  }

  auto result_table = _create_table(result_row, sqlite3_column_count(result_row));

  sqlite3_reset(result_row);

  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    _copy_row_from_sqlite_to_hyrise(result_table, result_row, sqlite3_column_count(result_row));
  }

  sqlite3_finalize(result_row);
  return result_table;
}

std::shared_ptr<Table> SQLiteWrapper::_create_table(sqlite3_stmt* result_row, int column_count) {
  std::vector<bool> column_nullable(column_count, false);
  std::vector<std::string> column_types(column_count, "");
  std::vector<std::string> column_names(column_count, "");

  bool no_result = true;
  int rc;
  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    for (int i = 0; i < column_count; ++i) {
      if (no_result) {
        column_names[i] = sqlite3_column_name(result_row, i);
      }

      switch (sqlite3_column_type(result_row, i)) {
        case SQLITE_INTEGER: {
          column_types[i] = "int";
          break;
        }

        case SQLITE_FLOAT: {
          column_types[i] = "double";
          break;
        }

        case SQLITE_TEXT: {
          column_types[i] = "string";
          break;
        }

        case SQLITE_NULL: {
          column_nullable[i] = true;
          break;
        }

        case SQLITE_BLOB:
        default: {
          sqlite3_finalize(result_row);
          sqlite3_close(_db);
          throw std::runtime_error("Column type not supported.");
        }
      }
    }
    no_result = false;
  }

  if (!no_result) {
    TableColumnDefinitions column_definitions;
    for (int i = 0; i < column_count; ++i) {
      if (column_types[i].empty()) {
        // Hyrise does not have explicit NULL columns
        column_types[i] = "int";
      }

      const auto data_type = data_type_to_string.right.at(column_types[i]);
      column_definitions.emplace_back(column_names[i], data_type, column_nullable[i]);
    }

    return std::make_shared<Table>(column_definitions, TableType::Data);
  }

  return nullptr;
}

void SQLiteWrapper::reset_table_from_copy(const std::string& table_name_to_reset,
                                          const std::string& table_name_to_copy_from) const {
  std::stringstream command_ss;
  command_ss << "DROP TABLE IF EXISTS " << table_name_to_reset << ";";
  command_ss << "CREATE TABLE " << table_name_to_reset << " AS SELECT * FROM " << table_name_to_copy_from << ";";

  _exec_sql(command_ss.str());
}

void SQLiteWrapper::_copy_row_from_sqlite_to_hyrise(const std::shared_ptr<Table>& table, sqlite3_stmt* result_row,
                                                    int column_count) {
  std::vector<AllTypeVariant> row;

  for (int i = 0; i < column_count; ++i) {
    switch (sqlite3_column_type(result_row, i)) {
      case SQLITE_INTEGER: {
        row.emplace_back(AllTypeVariant{sqlite3_column_int(result_row, i)});
        break;
      }

      case SQLITE_FLOAT: {
        row.emplace_back(AllTypeVariant{sqlite3_column_double(result_row, i)});
        break;
      }

      case SQLITE_TEXT: {
        row.emplace_back(
            AllTypeVariant{std::string(reinterpret_cast<const char*>(sqlite3_column_text(result_row, i)))});
        break;
      }

      case SQLITE_NULL: {
        row.emplace_back(NULL_VALUE);
        break;
      }

      case SQLITE_BLOB:
      default: {
        sqlite3_finalize(result_row);
        sqlite3_close(_db);
        throw std::runtime_error("Column type not supported.");
      }
    }
  }

  table->append(row);
}

void SQLiteWrapper::_exec_sql(const std::string& sql) const {
  char* err_msg;
  auto rc = sqlite3_exec(_db, sql.c_str(), nullptr, nullptr, &err_msg);

  if (rc != SQLITE_OK) {
    auto msg = std::string(err_msg);
    sqlite3_free(err_msg);
    sqlite3_close(_db);
    Fail("Failed to create table. SQL error: " + msg + "\n");
  }
}

}  // namespace opossum
