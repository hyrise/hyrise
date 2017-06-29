
#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "sql/lru_cache.hpp"

namespace opossum {

// Test for the different cache implementations in lib/sql.
// Not using SQL types in this test, only testing cache eviction.
class SQLBasicCacheTest : public BaseTest {};

TEST_F(SQLBasicCacheTest, BasicLRUCacheTest) {
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

}  // namespace opossum
