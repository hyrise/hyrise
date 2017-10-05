
#include <sqlite3.h>
#include <memory>
#include <string>
#include <fstream>
#include <iostream>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_planner.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLSQLiteRunnerTest : public BaseTest {
 protected:
  int add_sqlite_table() {
    char *err_msg;
    char const *sql = "CREATE TABLE table_a(a INT, b REAL);"
      "INSERT INTO table_a VALUES (12345, 458.7);"
      "INSERT INTO table_a VALUES (123, 456.7);"
      "INSERT INTO table_a VALUES (1234, 457.7);";

    int rc = sqlite3_exec(_db, sql, 0, 0, &err_msg);

    if (rc != SQLITE_OK ) {
      fprintf(stderr, "Failed to create table\n");
      fprintf(stderr, "SQL error: %s\n", err_msg);
      sqlite3_free(err_msg);
    } else {
      fprintf(stdout, "Table table_a created successfully\n");
    }
    return 0;
  }

  int add_sqlite_table2() {
    char *err_msg;
    char const *sql = "CREATE TABLE table_b(a INT, b REAL);"
      "INSERT INTO table_b VALUES (12345, 456.7);"
      "INSERT INTO table_b VALUES (12345, 457.7);"
      "INSERT INTO table_b VALUES (123, 458.7);"
      "INSERT INTO table_b VALUES (12, 350.7);";

    int rc = sqlite3_exec(_db, sql, 0, 0, &err_msg);

    if (rc != SQLITE_OK ) {
      fprintf(stderr, "Failed to create table\n");
      fprintf(stderr, "SQL error: %s\n", err_msg);
      sqlite3_free(err_msg);
    } else {
      fprintf(stdout, "Table table_b created successfully\n");
    }
    return 0;
  }

  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    int rc = sqlite3_open(":memory:", &_db);

    if (rc == SQLITE_OK) {

    } else {
      std::cerr << "Cannot open database: " << sqlite3_errmsg(_db) << std::endl;
      sqlite3_close(_db);
    }

    add_sqlite_table();
    add_sqlite_table2();
  }

  void TearDown() override {
    sqlite3_close(_db);
  }

  sqlite3 *_db;
};

void create_columns(std::shared_ptr<Table> table, sqlite3_stmt * result_row, int column_count) {
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

void add_row(std::shared_ptr<Table> table, sqlite3_stmt * result_row, int column_count) {
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

std::shared_ptr<Table> execute_query_sqlite(const std::string & sql_query, sqlite3 *db) {
  sqlite3_stmt *result_row;

  auto result_table = std::make_shared<Table>();

  int rc = sqlite3_prepare_v2(db, sql_query.c_str(), -1, &result_row, 0);

  if (rc != SQLITE_OK) {
    std::cerr << "Failed to execute query \"" << sql_query << "\": " << sqlite3_errmsg(db) << std::endl;

    return result_table;
  }

  if ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    int column_count = sqlite3_column_count(result_row);
    create_columns(result_table, result_row, column_count);

    do {
      add_row(result_table, result_row, column_count);
    } while ((rc = sqlite3_step(result_row)) == SQLITE_ROW);
  }

  sqlite3_finalize(result_row);
  return result_table;
}

TEST_F(SQLSQLiteRunnerTest, CompareToSQLiteTestRunner) {
  std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);

  std::ifstream file("src/test/sql/sql_sqlite_runner_test.testqueries");
  std::string query;
  while (std::getline(file, query))
  {
    if (query.empty()) { continue; }
    std::cout << query << std::endl;

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    // Expect the query to be a single statement.
    auto plan = SQLPlanner::plan(parse_result);
    auto result_operator = plan.tree_roots().front();

    auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);

    auto result_table = tasks.back()->get_operator()->get_output();
    // Print::print(result_table, 0);

    auto sqlite_result_table = execute_query_sqlite(query, _db);

    EXPECT_TABLE_EQ(result_table, sqlite_result_table, true);
  }
}

}  // namespace opossum
