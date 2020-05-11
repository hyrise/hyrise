#include "base_test.hpp"

namespace opossum {

// Test for the cache implementation in lib/cache.
// Not using SQL types in this test, only testing cache eviction.
class CachePolicyTest : public BaseTest {};

// GDFS Strategy
TEST_F(CachePolicyTest, GDFSCacheTest) {
  GDFSCache<int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, cache.priority(1));

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.get_entry(1));  // Hit, L=0, Fr=2
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

  ASSERT_EQ(6, cache.get_entry(3));  // Hit, L=1, Fr=2
  ASSERT_EQ(3.0, cache.priority(3));
  ASSERT_EQ(3.0, cache.queue().top().priority);

  ASSERT_EQ(6, cache.get_entry(3));  // Hit, L=1, Fr=3
  ASSERT_EQ(4.0, cache.priority(3));
  ASSERT_EQ(3.0, cache.priority(1));
  ASSERT_EQ(3.0, cache.queue().top().priority);

  cache.set(2, 5);  // Miss, evict 1, L=1, Fr=3
  ASSERT_EQ(3.0, cache.inflation());

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(cache.frequency(3), 3);
  ASSERT_EQ(cache.frequency(100), 0);
}

class CacheTest : public BaseTest {};

TEST_F(CacheTest, Size) {
  GDFSCache<int, int> cache(3);

  cache.set(1, 2);
  cache.set(2, 4);

  ASSERT_EQ(cache.size(), 2u);
}

TEST_F(CacheTest, Clear) {
  GDFSCache<int, int> cache(3);

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

TEST_F(CacheTest, ResizeGrow) {
  GDFSCache<int, int> cache(3);

  ASSERT_EQ(cache.capacity(), 3u);

  cache.set(1, 2);
  cache.set(2, 4);

  cache.resize(5);

  ASSERT_EQ(cache.capacity(), 5u);
  ASSERT_EQ(cache.size(), 2u);

  ASSERT_TRUE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
}

TEST_F(CacheTest, ResizeShrink) {
  GDFSCache<int, int> cache(3);

  ASSERT_EQ(cache.capacity(), 3u);

  cache.set(1, 2);
  cache.set(2, 4);
  cache.set(3, 6);

  cache.resize(1);

  ASSERT_EQ(cache.capacity(), 1u);
  ASSERT_EQ(cache.size(), 1u);

  ASSERT_FALSE(cache.try_get(1));
  ASSERT_FALSE(cache.try_get(2));
  ASSERT_TRUE(cache.try_get(3));
  ASSERT_EQ(cache.try_get(3), 6);
}

// Cache Iterator
TEST_F(CacheTest, CacheIteratorsRangeBasedForLoop) {
  GDFSCache<int, int> cache(2);

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

TEST_F(CacheTest, CacheIteratorsBeginEndLoop) {
  GDFSCache<int, int> cache(2);

  cache.set(0, 100);
  cache.set(1, 100);
  cache.set(2, 100);
  cache.set(3, 100);

  auto element_count = size_t{0};
  auto value_sum = size_t{0};

  for (auto iter = cache.unsafe_begin(), end = cache.unsafe_end(); iter != end; ++iter) {
    const auto value = (*iter).second;
    ++element_count;
    value_sum += value;
    ASSERT_EQ(value, 100);
  }

  ASSERT_EQ(element_count, 2);
  ASSERT_EQ(value_sum, 200);
}

}  // namespace opossum
