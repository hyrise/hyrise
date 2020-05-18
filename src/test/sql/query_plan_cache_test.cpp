#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

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
    auto pipeline = SQLPipelineBuilder{query}.with_pqp_cache(cache).create_pipeline();
    pipeline.get_result_table();

    if (pipeline.metrics().statement_metrics.at(0)->query_plan_cache_hit) {
      _query_plan_cache_hits++;
    }
  }

  size_t query_frequency(const std::string& key) const { return (*(cache->_map.find(key)->second)).frequency; }

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
  auto pipeline = SQLPipelineBuilder{Q1}.disable_mvcc().create_pipeline();
  pipeline.get_result_table();
  cache->set(Q1, pipeline.get_physical_plans().at(0));

  EXPECT_TRUE(cache->has(Q1));
  EXPECT_FALSE(cache->has(Q2));

  // Retrieve and execute the cached plan.
  const auto cached_plan = cache->try_get(Q1);
  EXPECT_EQ(cached_plan, pipeline.get_physical_plans().at(0));
}

// Test query plan cache with GDFS implementation.
TEST_F(QueryPlanCacheTest, AutomaticQueryOperatorCacheGDFS) {
  cache = std::make_shared<SQLPhysicalPlanCache>(2);

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

// Check access to PQP cache. When set, check the underlying cache implementation, and verify that it is a GDFS cache
// that supports retrieving the cache frequency count.
TEST_F(QueryPlanCacheTest, CachedPQPFrequencyCount) {
  // Create pipeline and pass pqp cache. Verify that this does not change default_pqp_cache.
  auto sql_pipeline = SQLPipelineBuilder{Q1}.with_pqp_cache(cache).create_pipeline();
  EXPECT_EQ(Hyrise::get().default_pqp_cache, nullptr);

  // Setting default_pqp_cache and verify it's set.
  Hyrise::get().default_pqp_cache = cache;
  EXPECT_NE(Hyrise::get().default_pqp_cache, nullptr);

  // Create new pipeline, without setting a cache (default cache set previously). Execute pipeline and check if
  // frequency of query is as expected.
  auto new_sql_pipeline = SQLPipelineBuilder{Q1}.create_pipeline();
  new_sql_pipeline.get_result_table();
  EXPECT_EQ(1, query_frequency(Q1));
}

}  // namespace opossum
