#include "sqlite_wrapper.hpp"

namespace opossum {

SqliteWrapper::SqliteWrapper() {
  int rc = sqlite3_open(":memory:", &_db);

  if (rc != SQLITE_OK) {
    std::cerr << "Cannot open database: " << sqlite3_errmsg(_db) << std::endl;
    sqlite3_close(_db);
  }
}

SqliteWrapper::~SqliteWrapper() {
  sqlite3_close(_db);
}

void SqliteWrapper::create_table_from_tbl(const std::string & file, const std::string & tablename) {
  // char *err_msg;
  // char const *sql = "CREATE TABLE table_a(a INT, b REAL);"
  //   "INSERT INTO table_a VALUES (12345, 458.7);"
  //   "INSERT INTO table_a VALUES (123, 456.7);"
  //   "INSERT INTO table_a VALUES (1234, 457.7);";

  // int rc = sqlite3_exec(_db, sql, 0, 0, &err_msg);

  // if (rc != SQLITE_OK ) {
  //   fprintf(stderr, "Failed to create table\n");
  //   fprintf(stderr, "SQL error: %s\n", err_msg);
  //   sqlite3_free(err_msg);
  // } else {
  //   fprintf(stdout, "Table table_a created successfully\n");
  // }
}

std::shared_ptr<Table> SqliteWrapper::execute_query(const std::string & sql_query) {
  sqlite3_stmt *result_row;

  auto result_table = std::make_shared<Table>();

  int rc = sqlite3_prepare_v2(_db, sql_query.c_str(), -1, &result_row, 0);

  if (rc != SQLITE_OK) {
    std::cerr << "Failed to execute query \"" << sql_query << "\": " << sqlite3_errmsg(_db) << std::endl;

    return result_table;
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

void SqliteWrapper::_create_columns(std::shared_ptr<Table> table, sqlite3_stmt * result_row, int column_count) {
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
      default: {
        col_type = "";
      }
    }
    table->add_column(col_name, col_type);
  }
}

void SqliteWrapper::_add_row(std::shared_ptr<Table> table, sqlite3_stmt * result_row, int column_count) {
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
      default: {
        row.push_back(AllTypeVariant{});
      }
    }
  }

  table->append(row);
}

} // namespace opossum
