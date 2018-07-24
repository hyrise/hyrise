#include "sqlite_wrapper.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/load_table.hpp"

namespace opossum {

SQLiteWrapper::SQLiteWrapper() {
  int rc = sqlite3_open(":memory:", &_db);

  if (rc != SQLITE_OK) {
    sqlite3_close(_db);
    throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(_db)) + "\n");
  }
}

SQLiteWrapper::~SQLiteWrapper() { sqlite3_close(_db); }

void SQLiteWrapper::create_table_from_tbl(const std::string& file, const std::string& table_name) {
  char* err_msg;
  std::ifstream infile(file);
  Assert(infile.is_open(), "SQLiteWrapper: Could not find file " + file);

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> col_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types;

  for (const std::string& type : _split<std::string>(line, '|')) {
    std::string actual_type = _split<std::string>(type, '_')[0];
    if (actual_type == "int" || actual_type == "long") {
      col_types.push_back("INT");
    } else if (actual_type == "float" || actual_type == "double") {
      col_types.push_back("REAL");
    } else if (actual_type == "string") {
      col_types.push_back("TEXT");
    } else {
      DebugAssert(false, "SQLiteWrapper: column type " + type + " not supported.");
    }
  }

  std::stringstream query;
  query << "CREATE TABLE " << table_name << "(";
  for (size_t i = 0; i < col_names.size(); i++) {
    query << col_names[i] << " " << col_types[i];

    if ((i + 1) < col_names.size()) {
      query << ", ";
    }
  }
  query << ");";

  while (std::getline(infile, line)) {
    query << "INSERT INTO " << table_name << " VALUES (";
    std::vector<std::string> values = _split<std::string>(line, '|');
    for (size_t i = 0; i < values.size(); i++) {
      if (col_types[i] == "TEXT" && values[i] != "null") {
        query << "'" << values[i] << "'";
      } else {
        query << values[i];
      }

      if ((i + 1) < values.size()) {
        query << ", ";
      }
    }
    query << ");";
  }

  int rc = sqlite3_exec(_db, query.str().c_str(), 0, 0, &err_msg);

  if (rc != SQLITE_OK) {
    auto msg = std::string(err_msg);
    sqlite3_free(err_msg);
    sqlite3_close(_db);
    throw std::runtime_error("Failed to create table. SQL error: " + msg + "\n");
  }
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
    rc = sqlite3_prepare_v2(_db, query.c_str(), -1, &result_row, 0);

    if (rc != SQLITE_OK) {
      sqlite3_finalize(result_row);
      sqlite3_close(_db);
      throw std::runtime_error("Failed to execute query \"" + query + "\": " + std::string(sqlite3_errmsg(_db)) + "\n");
    }

    while ((rc = sqlite3_step(result_row)) != SQLITE_DONE) {
    }
  }

  rc = sqlite3_prepare_v2(_db, select_query.c_str(), -1, &result_row, 0);

  if (rc != SQLITE_OK) {
    auto error_message = "Failed to execute query \"" + select_query + "\": " + std::string(sqlite3_errmsg(_db));
    sqlite3_finalize(result_row);
    sqlite3_close(_db);
    throw std::runtime_error(error_message);
  }

  auto result_table = _create_table(result_row, sqlite3_column_count(result_row));

  sqlite3_reset(result_row);

  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    _add_row(result_table, result_row, sqlite3_column_count(result_row));
  }

  sqlite3_finalize(result_row);
  return result_table;
}

std::shared_ptr<Table> SQLiteWrapper::_create_table(sqlite3_stmt* result_row, int column_count) {
  std::vector<bool> col_nullable(column_count, false);
  std::vector<std::string> col_types(column_count, "");
  std::vector<std::string> col_names(column_count, "");

  bool no_result = true;
  int rc;
  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    for (int i = 0; i < column_count; ++i) {
      if (no_result) {
        col_names[i] = sqlite3_column_name(result_row, i);
      }

      switch (sqlite3_column_type(result_row, i)) {
        case SQLITE_INTEGER: {
          col_types[i] = "int";
          break;
        }

        case SQLITE_FLOAT: {
          col_types[i] = "double";
          break;
        }

        case SQLITE_TEXT: {
          col_types[i] = "string";
          break;
        }

        case SQLITE_NULL: {
          col_nullable[i] = true;
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
      if (col_types[i].empty()) {
        // Hyrise does not have explicit NULL columns
        col_types[i] = "int";
      }

      const auto data_type = data_type_to_string.right.at(col_types[i]);
      column_definitions.emplace_back(col_names[i], data_type, col_nullable[i]);
    }

    return std::make_shared<Table>(column_definitions, TableType::Data);
  }

  return nullptr;
}

void SQLiteWrapper::_add_row(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count) {
  std::vector<AllTypeVariant> row;

  for (int i = 0; i < column_count; ++i) {
    switch (sqlite3_column_type(result_row, i)) {
      case SQLITE_INTEGER: {
        row.push_back(AllTypeVariant{sqlite3_column_int(result_row, i)});
        break;
      }

      case SQLITE_FLOAT: {
        row.push_back(AllTypeVariant{sqlite3_column_double(result_row, i)});
        break;
      }

      case SQLITE_TEXT: {
        row.push_back(AllTypeVariant{std::string(reinterpret_cast<const char*>(sqlite3_column_text(result_row, i)))});
        break;
      }

      case SQLITE_NULL: {
        row.push_back(NULL_VALUE);
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

}  // namespace opossum
