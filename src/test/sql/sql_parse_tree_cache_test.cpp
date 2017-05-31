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
#include "sql/sql_parse_tree_cache.hpp"
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
  }

  void TearDown() override {
    CurrentScheduler::set(nullptr);  // Make sure there is no Scheduler anymore
  }
};

TEST_F(SQLParseTreeCacheTest, BasicLRUCacheTest) {
  LRUCache<int, int> cache(2);

  EXPECT_FALSE(cache.has(1));
  EXPECT_FALSE(cache.has(2));
  EXPECT_FALSE(cache.has(3));

  cache.set(1, 2);

  EXPECT_TRUE(cache.has(1));
  EXPECT_FALSE(cache.has(2));
  EXPECT_FALSE(cache.has(3));

  EXPECT_EQ(cache.get(1), 2);

  cache.set(1, 2);
  cache.set(2, 4);
  cache.set(3, 6);

  EXPECT_FALSE(cache.has(1));
  EXPECT_TRUE(cache.has(2));
  EXPECT_TRUE(cache.has(3));
  EXPECT_EQ(cache.get(2), 4);
  EXPECT_EQ(cache.get(3), 6);

  cache.get(2);
  cache.set(1, 5);

  EXPECT_TRUE(cache.has(1));
  EXPECT_TRUE(cache.has(2));
  EXPECT_FALSE(cache.has(3));
  EXPECT_EQ(cache.get(1), 5);
  EXPECT_EQ(cache.get(2), 4);
}

TEST_F(SQLParseTreeCacheTest, BasicSQLCacheTest) {
  SQLParseTreeCache cache(2);
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

TEST_F(SQLParseTreeCacheTest, BasicPrepareAndExecuteTest) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  const std::string prep_query = "PREPARE query FROM 'SELECT * FROM table_a';";
  auto prep_op = std::make_shared<SQLQueryOperator>(prep_query);
  auto prep_task = std::make_shared<OperatorTask>(prep_op);

  const std::string exec_query = "EXECUTE query;";
  auto exec_op = std::make_shared<SQLQueryOperator>(exec_query);
  auto exec_task = std::make_shared<OperatorTask>(exec_op);
  auto exec_result = exec_op->get_result_task();

  prep_task->set_as_predecessor_of(exec_task);

  prep_task->schedule();
  exec_task->schedule();

  CurrentScheduler::get()->finish();

  auto expected_result = load_table("src/test/tables/int_float.tbl", 2);
  EXPECT_TABLE_EQ(exec_result->get_operator()->get_output(), expected_result);
}

}  // namespace opossum