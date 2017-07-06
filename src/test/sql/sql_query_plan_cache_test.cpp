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

namespace opossum {

// The fixture for testing class GetTable.
class SQLQueryPlanCacheTest : public BaseTest {
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

    if (op->parse_tree_cache_hit()) {
      parse_tree_cache_hits++;
    }

    if (op->query_plan_cache_hit()) {
      query_plan_cache_hits++;
    }
    return task;
  }

  void schedule_query_task(const std::string& query) {
    auto op = std::make_shared<SQLQueryOperator>(query);
    auto task = std::make_shared<OperatorTask>(op);
    task->schedule();
  }

  size_t parse_tree_cache_hits;
  size_t query_plan_cache_hits;
};

TEST_F(SQLQueryPlanCacheTest, SQLQueryPlanCacheTest) {
  SQLQueryCache<SQLQueryPlan> cache(2);
  std::string query1 = "SELECT * FROM table_a;";
  std::string query2 = "SELECT * FROM table_b;";

  EXPECT_FALSE(cache.has(query1));
  EXPECT_FALSE(cache.has(query2));

  SQLQueryOperator op(query1, false);
  op.execute();

  // Cache the query plan.
  cache.set(query1, op.get_query_plan());

  EXPECT_FALSE(cache.has(query2));
  EXPECT_TRUE(cache.has(query1));

  const SQLQueryPlan cached_plan = cache.get(query1);

  auto task_list1 = cached_plan.recreate().tasks();
  auto task_list2 = cached_plan.recreate().tasks();

  for (auto task : task_list1) {
    task->execute();
  }

  for (auto task : task_list2) {
    task->execute();
  }

  EXPECT_TABLE_EQ(task_list1.back()->get_operator()->get_output(), task_list2.back()->get_operator()->get_output());
}

TEST_F(SQLQueryPlanCacheTest, QueryOperatorParseTreeCache) {
  query_plan_cache_hits = 0;

  SQLQueryCache<SQLQueryPlan>& cache = SQLQueryOperator::get_query_plan_cache();
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
  EXPECT_EQ(5u, query_plan_cache_hits);
}

}  // namespace opossum
