#include "base_test.hpp"

#include "cache/cache.hpp"
#include "cache/gdfs_cache.hpp"
#include "cache/gds_cache.hpp"
#include "cache/lru_cache.hpp"
#include "cache/lru_k_cache.hpp"
#include "cache/random_cache.hpp"

namespace opossum {

// Test for the different cache implementations in lib/sql.
// Not using SQL types in this test, only testing cache eviction.
class CachePolicyTest : public BaseTest {};

// LRU Strategy
TEST(CachePolicyTest, LRUCacheTest) {
  LRUCache<int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert.

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.get(1));  // Hit.

  cache.set(1, 2);  // Hit.
  cache.set(2, 4);  // Miss, insert.
  cache.set(3, 6);  // Miss, evict 1.

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
  ASSERT_EQ(4, cache.get(2));  // Hit.
  ASSERT_EQ(6, cache.get(3));  // Hit.

  cache.get(2);     // Hit.
  cache.set(1, 5);  // Miss, evict 3.

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_FALSE(cache.has(3));
  ASSERT_EQ(5, cache.get(1));  // Hit.
  ASSERT_EQ(4, cache.get(2));  // Hit.
}

// LRU-K (K = 2)
TEST(CachePolicyTest, LRU2CacheTest) {
  LRUKCache<2, int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert.

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(2, 4);  // Miss, insert.
  cache.set(3, 6);  // Miss, evict 1.

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(4, cache.get(2));  // Hit.
  ASSERT_EQ(6, cache.get(3));  // Hit.

  cache.get(3);     // Hit.
  cache.get(2);     // Hit.
  cache.set(1, 5);  // Miss, evict 2.

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(5, cache.get(1));  // Hit.
  ASSERT_EQ(6, cache.get(3));  // Hit.
}

// GDS Strategy
TEST(CachePolicyTest, GDSCacheTest) {
  GDSCache<int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2, 5);  // Miss, insert, L=0, C=5, P=5
  ASSERT_EQ(5.0, cache.priority(1));

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.get(1));  // Hit.
  ASSERT_EQ(5.0, cache.priority(1));

  cache.set(1, 2, 5);  // Hit.
  ASSERT_EQ(5.0, cache.priority(1));

  cache.set(2, 4, 4);  // Miss, insert, L=0, C=4, P=4
  ASSERT_EQ(4.0, cache.priority(2));

  cache.set(3, 6, 2);  // Miss, evict 2, L=4, C=2, P=6
  ASSERT_EQ(6.0, cache.priority(3));
  ASSERT_EQ(4.0, cache.inflation());

  ASSERT_EQ(5.0, cache.queue().top().priority);

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(6, cache.get(3));  // Hit.
  ASSERT_EQ(6.0, cache.priority(3));
  ASSERT_EQ(5.0, cache.queue().top().priority);

  ASSERT_EQ(6, cache.get(3));  // Hit.
  ASSERT_EQ(6.0, cache.priority(3));
  ASSERT_EQ(5.0, cache.priority(1));
  ASSERT_EQ(5.0, cache.queue().top().priority);

  cache.set(2, 5, 4);  // Miss, evict 1, L=5, C=4, P=9
  ASSERT_EQ(5.0, cache.inflation());

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
}

// GDFS Strategy
TEST(CachePolicyTest, GDFSCacheTest) {
  GDFSCache<int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, cache.priority(1));

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.get(1));  // Hit, L=0, Fr=2
  ASSERT_EQ(2.0, cache.priority(1));

  cache.set(1, 2);  // Hit, L=0, Fr=3
  ASSERT_EQ(3.0, cache.priority(1));

  cache.set(2, 4);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, cache.priority(2));

  cache.set(3, 6);  // Miss, evict 2, L=1, Fr=1
  ASSERT_EQ(2.0, cache.priority(3));
  ASSERT_EQ(1.0, cache.inflation());

  ASSERT_EQ(2.0, cache.queue().top().priority);

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(6, cache.get(3));  // Hit, L=1, Fr=2
  ASSERT_EQ(3.0, cache.priority(3));
  ASSERT_EQ(3.0, cache.queue().top().priority);

  ASSERT_EQ(6, cache.get(3));  // Hit, L=1, Fr=3
  ASSERT_EQ(4.0, cache.priority(3));
  ASSERT_EQ(3.0, cache.priority(1));
  ASSERT_EQ(3.0, cache.queue().top().priority);

  cache.set(2, 5);  // Miss, evict 1, L=1, Fr=3
  ASSERT_EQ(3.0, cache.inflation());

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
}

// Random Replacement Strategy
TEST(CachePolicyTest, RandomCacheTest) {
  RandomCache<int, int> cache(3);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert 1.

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.get(1));  // Hit.

  cache.set(1, 2);  // Hit.
  cache.set(2, 4);  // Miss, insert 2.
  cache.set(3, 6);  // Miss, insert 3.

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
  ASSERT_EQ(2, cache.get(1));  // Hit.
  ASSERT_EQ(4, cache.get(2));  // Hit.
  ASSERT_EQ(6, cache.get(3));  // Hit.
  ASSERT_EQ(4, cache.get(2));  // Hit.

  cache.set(1, 5);  // Hit.

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
  ASSERT_EQ(5, cache.get(1));  // Hit.
  ASSERT_EQ(4, cache.get(2));  // Hit.

  cache.set(4, 51);  // Miss, evict ?
  cache.set(5, 52);  // Miss, evict ?
  cache.set(6, 53);  // Miss, evict ?
  // We can only expect the most recent insertion to be in the cache.
  ASSERT_TRUE(cache.has(6));
  ASSERT_EQ(53, cache.get(6));  // Hit.
}

// Test the default cache (uses GDFS).
TEST(CachePolicyTest, Iterators) {
  Cache<int, int> cache(2);

  cache.set(0, 100);
  cache.set(1, 100);
  cache.set(2, 100);
  cache.set(3, 100);

  auto element_count = size_t{0};
  auto value_sum = size_t{0};

  for (const auto& [key, value] : cache) {
    ++element_count;
    value_sum += value;
    ASSERT_EQ(value, 100);
  }

  ASSERT_EQ(element_count, 2);
  ASSERT_EQ(value_sum, 200);
}

template <typename T>
class CacheTest : public BaseTest {};

// Here, all cache types are defined.
using CacheTypes = ::testing::Types<LRUCache<int, int>, LRUKCache<2, int, int>, GDSCache<int, int>, GDFSCache<int, int>,
                                    RandomCache<int, int>>;
TYPED_TEST_SUITE(CacheTest, CacheTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(CacheTest, Size) {
  TypeParam cache(3);

  cache.set(1, 2);
  cache.set(2, 4);

  ASSERT_EQ(cache.size(), 2u);
}

TYPED_TEST(CacheTest, Clear) {
  TypeParam cache(3);

  cache.set(1, 2);
  cache.set(2, 4);

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));

  cache.clear();

  ASSERT_EQ(cache.capacity(), 3u);
  ASSERT_EQ(cache.size(), 0u);
  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
}

TYPED_TEST(CacheTest, ResizeGrow) {
  TypeParam cache(3);

  ASSERT_EQ(cache.capacity(), 3u);

  cache.set(1, 2);
  cache.set(2, 4);

  cache.resize(5);

  ASSERT_EQ(cache.capacity(), 5u);
  ASSERT_EQ(cache.size(), 2u);

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
}

TYPED_TEST(CacheTest, ResizeShrink) {
  TypeParam cache(3);

  ASSERT_EQ(cache.capacity(), 3u);

  cache.set(1, 2);
  cache.set(2, 4);
  cache.set(3, 6);

  cache.resize(1);

  ASSERT_EQ(cache.capacity(), 1u);
  ASSERT_EQ(cache.size(), 1u);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));
  ASSERT_EQ(cache.get(3), 6);
}

// Cache Iterator
TYPED_TEST(CacheTest, CacheIteratorsRangeBasedForLoop) {
  TypeParam cache(2);

  cache.set(0, 100);
  cache.set(1, 100);
  cache.set(2, 100);
  cache.set(3, 100);

  auto element_count = size_t{0};
  auto value_sum = size_t{0};

  for (const auto& [key, value] : cache) {
    ++element_count;
    value_sum += value;
    ASSERT_EQ(value, 100);
  }

  ASSERT_EQ(element_count, 2);
  ASSERT_EQ(value_sum, 200);
}

TYPED_TEST(CacheTest, CacheIteratorsBeginEndLoop) {
  TypeParam cache(2);

  cache.set(0, 100);
  cache.set(1, 100);
  cache.set(2, 100);
  cache.set(3, 100);

  auto element_count = size_t{0};
  auto value_sum = size_t{0};

  for (auto iter = cache.begin(), end = cache.end(); iter != end; ++iter) {
    const auto value = (*iter).second;
    ++element_count;
    value_sum += value;
    ASSERT_EQ(value, 100);
  }

  ASSERT_EQ(element_count, 2);
  ASSERT_EQ(value_sum, 200);
}

}  // namespace opossum
