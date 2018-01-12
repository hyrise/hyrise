#include <memory>
#include <string>
#include <utility>

#include "sql_base_test.hpp"

#include "sql/gdfs_cache.hpp"
#include "sql/lru_cache.hpp"
#include "sql/lru_k_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// The fixture for testing class GetTable.
class SQLQueryPlanCacheTest : public SQLBaseTest {
 protected:
  void SetUp() override {
    // Load tables.
    auto table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));
    auto table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    parse_tree_cache_hits = 0;
    query_plan_cache_hits = 0;
  }

  void TearDown() override {
    SQLQueryOperator::get_parse_tree_cache().resize(0);
    SQLQueryOperator::get_query_plan_cache().resize(0);
    SQLQueryOperator::get_prepared_statement_cache().clear();
    SQLQueryOperator::get_prepared_statement_cache().resize(1024);
  }
};

TEST_F(SQLQueryPlanCacheTest, SQLQueryPlanCacheTest) {
  SQLQueryCache<SQLQueryPlan> cache(2);

  EXPECT_FALSE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));

  // Execute a query and cache its plan.
  SQLQueryOperator op(Q1, false, false);
  op.execute();
  cache.set(Q1, op.get_query_plan());

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));

  // Retrieve and execute the cached plan.
  const SQLQueryPlan cached_plan = cache.get(Q1);
  auto task_list1 = cached_plan.recreate().create_tasks();
  auto task_list2 = cached_plan.recreate().create_tasks();

  for (auto task : task_list1) task->execute();
  for (auto task : task_list2) task->execute();

  EXPECT_TABLE_EQ_UNORDERED(task_list1.back()->get_operator()->get_output(),
                            task_list2.back()->get_operator()->get_output());
}

// Test query plan cache with LRU implementation.
TEST_F(SQLQueryPlanCacheTest, AutomaticQueryOperatorCacheLRU) {
  SQLQueryCache<SQLQueryPlan>& cache = SQLQueryOperator::get_query_plan_cache();
  cache.replace_cache_impl<LRUCache<std::string, SQLQueryPlan>>(2);

  // Execute the queries in arbitrary order.
  execute_query_task(Q1, false);  // Miss.
  execute_query_task(Q2, false);  // Miss.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);  // Miss, evict Q2.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q2, false);  // Miss, evict Q3.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);  // Miss, evict Q2.
  execute_query_task(Q1, false);  // Hit.

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));
  EXPECT_TRUE(cache.has(Q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, query_plan_cache_hits);
}

// Test query plan cache with GDFS implementation.
TEST_F(SQLQueryPlanCacheTest, AutomaticQueryOperatorCacheGDFS) {
  SQLQueryCache<SQLQueryPlan>& cache = SQLQueryOperator::get_query_plan_cache();
  cache.replace_cache_impl<GDFSCache<std::string, SQLQueryPlan>>(2);

  // Execute the queries in arbitrary order.
  execute_query_task(Q1, false);  // Miss.
  execute_query_task(Q2, false);  // Miss.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);  // Miss, evict Q2.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q2, false);  // Miss, evict Q1.
  execute_query_task(Q1, false);  // Miss, evict Q2.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));
  EXPECT_TRUE(cache.has(Q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(9u, query_plan_cache_hits);
}

// Test query plan cache with GDFS implementation.
TEST_F(SQLQueryPlanCacheTest, AutomaticQueryOperatorCacheLRUK2) {
  SQLQueryCache<SQLQueryPlan>& cache = SQLQueryOperator::get_query_plan_cache();
  cache.replace_cache_impl<LRUKCache<2, std::string, SQLQueryPlan>>(2);

  // Execute the queries in arbitrary order.
  execute_query_task(Q1, false);  // Miss.
  execute_query_task(Q2, false);  // Miss.
  execute_query_task(Q2, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);  // Miss, evict Q1.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Miss, evict Q2.
  execute_query_task(Q2, false);  // Miss, evict Q1.
  execute_query_task(Q1, false);  // Miss, evict Q2.
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));
  EXPECT_TRUE(cache.has(Q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, query_plan_cache_hits);
}

}  // namespace opossum
