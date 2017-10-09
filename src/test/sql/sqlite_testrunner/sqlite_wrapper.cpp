#include "sqlite_wrapper.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "utils/load_table.hpp"

namespace opossum {

SqliteWrapper::SqliteWrapper() {
  int rc = sqlite3_open(":memory:", &_db);

  if (rc != SQLITE_OK) {
    sqlite3_close(_db);
    throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(_db)) + "\n");
  }
}

SqliteWrapper::~SqliteWrapper() { sqlite3_close(_db); }

void SqliteWrapper::create_table_from_tbl(const std::string& file, const std::string& table_name) {
  char* err_msg;
  std::ifstream infile(file);
  Assert(infile.is_open(), "SqliteWrapper: Could not find file " + file);

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> col_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types;

  for (std::string type : _split<std::string>(line, '|')) {
    if (type == "int" || type == "long") {
      col_types.push_back("INT");
    } else if (type == "float" || type == "double") {
      col_types.push_back("REAL");
    } else if (type == "string") {
      col_types.push_back("TEXT");
    } else {
      DebugAssert(false, "SqliteWrapper: column type " + type + " not supported.");
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
      if (col_types[i] == "TEXT") {
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
    sqlite3_free(err_msg);
    throw std::runtime_error("Failed to create table. SQL error: " + std::string(err_msg) + "\n");
  }
}

std::shared_ptr<Table> SqliteWrapper::execute_query(const std::string& sql_query) {
  sqlite3_stmt* result_row;

  auto result_table = std::make_shared<Table>();

  int rc = sqlite3_prepare_v2(_db, sql_query.c_str(), -1, &result_row, 0);

  if (rc != SQLITE_OK) {
    throw std::runtime_error("Failed to execute query \"" + sql_query + "\": " + std::string(sqlite3_errmsg(_db)) +
                             "\n");
  }

  if ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    int column_count = sqlite3_column_count(result_row);
    _create_columns(result_table, result_row, column_count);

    do {
      _add_row(result_table, result_row, column_count);
    } while ((rc = sqlite3_step(result_row)) == SQLITE_ROW);
  }

  sqlite3_finalize(result_row);
  return result_table;
}

void SqliteWrapper::_create_columns(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count) {
  for (int i = 0; i < column_count; ++i) {
    std::string col_name = sqlite3_column_name(result_row, i);
    std::string col_type;
    switch (sqlite3_column_type(result_row, i)) {
      case SQLITE_INTEGER: {
        col_type = "int";
        break;
      }

      case SQLITE_FLOAT: {
        col_type = "float";
        break;
      }

      case SQLITE_TEXT: {
        col_type = "string";
        break;
      }

      case SQLITE_NULL:
      case SQLITE_BLOB:
      default: { throw std::runtime_error("Column type not supported."); }
    }
    table->add_column(col_name, col_type);
  }
}

void SqliteWrapper::_add_row(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count) {
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

      case SQLITE_NULL:
      case SQLITE_BLOB:
      default: { throw std::runtime_error("Column type not supported."); }
    }
  }

  table->append(row);
}

}  // namespace opossum
