#include <memory>
#include <string>
#include <utility>

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "sql_base_test.hpp"

#include "sql/lru_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// The fixture for testing class GetTable.
class SQLParseTreeCacheTest : public SQLBaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    parse_tree_cache_hits = 0;
    query_plan_cache_hits = 0;
  }

  void TearDown() override {}
};

TEST_F(SQLParseTreeCacheTest, SQLParseTreeCacheTest) {
  SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>> cache(2);

  EXPECT_FALSE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));

  // Parse the query and cache the parse tree.
  auto result = std::make_shared<hsql::SQLParserResult>();
  hsql::SQLParser::parseSQLString(Q1.c_str(), result.get());
  cache.set(Q1, result);

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));

  auto cached = cache.get(Q1);
  EXPECT_EQ(cached, result);
  EXPECT_EQ(cached->size(), 1u);
}

// Test query plan cache with LRU implementation.
TEST_F(SQLParseTreeCacheTest, AutomaticQueryOperatorCacheLRU) {
  auto& cache = SQLQueryOperator::get_parse_tree_cache();
  cache.replace_cache_impl<LRUCache<std::string, std::shared_ptr<hsql::SQLParserResult>>>(2);

  // Execute the queries in arbitrary order.
  execute_query_task(Q1, false);
  execute_query_task(Q2, false);
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);
  execute_query_task(Q3, false);  // Hit.
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q2, false);
  execute_query_task(Q1, false);  // Hit.
  execute_query_task(Q3, false);
  execute_query_task(Q1, false);  // Hit.

  EXPECT_TRUE(cache.has(Q1));
  EXPECT_FALSE(cache.has(Q2));
  EXPECT_TRUE(cache.has(Q3));
  EXPECT_FALSE(cache.has("SELECT * FROM test;"));

  // Check for the expected number of hits.
  EXPECT_EQ(5u, parse_tree_cache_hits);
}

}  // namespace opossum
