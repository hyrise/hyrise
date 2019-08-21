#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "cache/gdfs_cache.hpp"
#include "cache/lru_cache.hpp"
#include "cache/lru_k_cache.hpp"
#include "hyrise.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"

namespace opossum {

class QueryPlanCacheTest : public BaseTest {
 protected:
  void SetUp() override {
    // Load tables.
    auto table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", std::move(table_a));
    auto table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_b", std::move(table_b));

    _query_plan_cache_hits = 0;

    cache = std::make_shared<SQLPhysicalPlanCache>();
  }

  void execute_query(const std::string& query) {
    auto pipeline_statement = SQLPipelineBuilder{query}.with_pqp_cache(cache).create_pipeline_statement();
    pipeline_statement.get_result_table();

    if (pipeline_statement.metrics()->query_plan_cache_hit) {
      _query_plan_cache_hits++;
    }
  }

  const std::string Q1 = "SELECT * FROM table_a;";
  const std::string Q2 = "SELECT * FROM table_b;";
  const std::string Q3 = "SELECT * FROM table_a WHERE a > 1;";

  size_t _query_plan_cache_hits;

  std::shared_ptr<SQLPhysicalPlanCache> cache;
};

TEST_F(QueryPlanCacheTest, QueryPlanCacheTest) {
  EXPECT_FALSE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));

  // Execute a query and cache its plan.
  auto pipeline_statement = SQLPipelineBuilder{Q1}.disable_mvcc().create_pipeline_statement();
  pipeline_statement.get_result_table();
  cache->set(Q1, pipeline_statement.get_physical_plan());

  EXPECT_TRUE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));

  // Retrieve and execute the cached plan.
  const auto cached_plan = cache->get_entry(Q1);
  EXPECT_EQ(cached_plan, pipeline_statement.get_physical_plan());
}

// Test query plan cache with LRU implementation.
TEST_F(QueryPlanCacheTest, AutomaticQueryOperatorCacheLRU) {
  cache->replace_cache_impl<LRUCache<std::string, std::shared_ptr<AbstractOperator>>>(2);

  // Execute the queries in arbitrary order.
  execute_query(Q1);  // Miss.
  execute_query(Q2);  // Miss.
  execute_query(Q1);  // Hit.
  execute_query(Q3);  // Miss, evict Q2.
  execute_query(Q3);  // Hit.
  execute_query(Q1);  // Hit.
  execute_query(Q2);  // Miss, evict Q3.
  execute_query(Q1);  // Hit.
  execute_query(Q3);  // Miss, evict Q2.
  execute_query(Q1);  // Hit.

  EXPECT_TRUE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));
  EXPECT_TRUE(cache->has(Q3));
  EXPECT_FALSE(cache->has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, _query_plan_cache_hits);
}

// Test query plan cache with GDFS implementation.
TEST_F(QueryPlanCacheTest, AutomaticQueryOperatorCacheGDFS) {
  cache->replace_cache_impl<GDFSCache<std::string, std::shared_ptr<AbstractOperator>>>(2);

  // Execute the queries in arbitrary order.
  execute_query(Q1);  // Miss.
  execute_query(Q2);  // Miss.
  execute_query(Q1);  // Hit.
  execute_query(Q3);  // Miss, evict Q2.
  execute_query(Q3);  // Hit.
  execute_query(Q3);  // Hit.
  execute_query(Q3);  // Hit.
  execute_query(Q3);  // Hit.
  execute_query(Q3);  // Hit.
  execute_query(Q1);  // Hit.
  execute_query(Q2);  // Miss, evict Q1.
  execute_query(Q1);  // Miss, evict Q2.
  execute_query(Q3);  // Hit.
  execute_query(Q1);  // Hit.

  EXPECT_TRUE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));
  EXPECT_TRUE(cache->has(Q3));
  EXPECT_FALSE(cache->has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(9u, _query_plan_cache_hits);
}

// Test query plan cache with LRUK implementation.
TEST_F(QueryPlanCacheTest, AutomaticQueryOperatorCacheLRUK2) {
  cache->replace_cache_impl<LRUKCache<2, std::string, std::shared_ptr<AbstractOperator>>>(2);

  // Execute the queries in arbitrary order.
  execute_query(Q1);  // Miss.
  execute_query(Q2);  // Miss.
  execute_query(Q2);  // Hit.
  execute_query(Q1);  // Hit.
  execute_query(Q3);  // Miss, evict Q1.
  execute_query(Q3);  // Hit.
  execute_query(Q1);  // Miss, evict Q2.
  execute_query(Q2);  // Miss, evict Q1.
  execute_query(Q1);  // Miss, evict Q2.
  execute_query(Q3);  // Hit.
  execute_query(Q1);  // Hit.

  EXPECT_TRUE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));
  EXPECT_TRUE(cache->has(Q3));
  EXPECT_FALSE(cache->has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, _query_plan_cache_hits);
}

}  // namespace opossum
