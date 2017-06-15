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

  void schedule_query(const std::string& query) {
    auto op = std::make_shared<SQLQueryOperator>(query);
    auto task = std::make_shared<OperatorTask>(op);
    task->schedule();
  }
};

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

TEST_F(SQLParseTreeCacheTest, BasicAutoCacheTest) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  // Reset operator statistics.
  SQLQueryOperator::num_executed = 0;
  SQLQueryOperator::parse_tree_cache_hits = 0;
  SQLQueryOperator::parse_tree_cache_misses = 0;

  SQLParseTreeCache& cache = SQLQueryOperator::get_parse_tree_cache();
  cache.reset(16);

  const std::string q1 = "SELECT * FROM table_a;";
  const std::string q2 = "SELECT * FROM table_b;";
  const std::string q3 = "SELECT * FROM table_a WHERE a > 1;";

  // Execute each query once to cache them.
  schedule_query(q1);
  schedule_query(q2);
  schedule_query(q3);

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  schedule_query(q3);
  schedule_query(q1);
  schedule_query(q1);
  schedule_query(q2);
  schedule_query(q1);
  schedule_query(q3);
  schedule_query(q1);

  CurrentScheduler::get()->finish();

  EXPECT_TRUE(cache.has(q1));
  EXPECT_TRUE(cache.has(q2));
  EXPECT_TRUE(cache.has(q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check operator runtime statistics.
  EXPECT_EQ(10u, SQLQueryOperator::num_executed);
  EXPECT_EQ(7u, SQLQueryOperator::parse_tree_cache_hits);
  EXPECT_EQ(3u, SQLQueryOperator::parse_tree_cache_misses);
  EXPECT_EQ(3u, cache.size());
}

}  // namespace opossum
