#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

using hsql::SQLParserResult;
using hsql::SQLParser;

namespace opossum {

// The fixture for testing class GetTable.
class SQLParseTreeCacheTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    parse_tree_cache_hits = 0;
  }

  void TearDown() override {
    CurrentScheduler::set(nullptr);  // Make sure there is no Scheduler anymore
  }

  std::shared_ptr<OperatorTask> execute_query_task(const std::string& query) {
    auto op = std::make_shared<SQLQueryOperator>(query);
    auto task = std::make_shared<OperatorTask>(op);
    task->execute();

    if (op->hit_parse_tree_cache()) {
      parse_tree_cache_hits++;
    }
    return task;
  }

  void schedule_query_task(const std::string& query) {
    auto op = std::make_shared<SQLQueryOperator>(query);
    auto task = std::make_shared<OperatorTask>(op);
    task->schedule();
  }

  size_t parse_tree_cache_hits;
};

TEST_F(SQLParseTreeCacheTest, SQLParseTreeCacheTest) {
  SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>> cache(2);
  std::string query1 = "SELECT * FROM test;";
  std::string query2 = "SELECT * FROM test2;";

  std::shared_ptr<SQLParserResult> result;

  EXPECT_FALSE(cache.has(query1));
  EXPECT_FALSE(cache.has(query2));

  result.reset(SQLParser::parseSQLString(query1.c_str()));

  cache.set(query1, result);

  EXPECT_FALSE(cache.has(query2));
  EXPECT_TRUE(cache.has(query1));

  std::shared_ptr<SQLParserResult> cached = cache.get(query1);
  EXPECT_EQ(cached, result);
  EXPECT_EQ(cached->size(), 1u);
}

TEST_F(SQLParseTreeCacheTest, PrepareAndExecuteSimpleTest) {
  const std::string prep_query = "PREPARE query FROM 'SELECT * FROM table_a';";
  const std::string exec_query = "EXECUTE query;";

  auto prep_task = execute_query_task(prep_query);

  auto exec_task = execute_query_task(exec_query);
  const std::shared_ptr<SQLQueryOperator>& exec_op =
      (const std::shared_ptr<SQLQueryOperator>&)exec_task->get_operator();
  auto exec_result = exec_op->get_result_task();

  auto expected_result = load_table("src/test/tables/int_float.tbl", 2);
  EXPECT_TABLE_EQ(exec_result->get_operator()->get_output(), expected_result);
}

TEST_F(SQLParseTreeCacheTest, QueryOperatorParseTreeCache) {
  SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>>& cache = SQLQueryOperator::get_parse_tree_cache();
  cache.clear_and_resize(2);

  const std::string q1 = "SELECT * FROM table_a;";
  const std::string q2 = "SELECT * FROM table_b;";
  const std::string q3 = "SELECT * FROM table_a WHERE a > 1;";

  // Execute the queries in arbitrary order.
  execute_query_task(q1);
  execute_query_task(q2);
  execute_query_task(q1);  // Hit.
  execute_query_task(q3);
  execute_query_task(q3);  // Hit.
  execute_query_task(q1);  // Hit.
  execute_query_task(q2);
  execute_query_task(q1);  // Hit.
  execute_query_task(q3);
  execute_query_task(q1);  // Hit.

  EXPECT_TRUE(cache.has(q1));
  EXPECT_FALSE(cache.has(q2));
  EXPECT_TRUE(cache.has(q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, parse_tree_cache_hits);
}

}  // namespace opossum
