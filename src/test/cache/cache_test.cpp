#include "base_test.hpp"

namespace opossum {

// Test for the cache implementation in lib/cache.
// Not using SQL types in this test, only testing cache eviction.
class CachePolicyTest : public BaseTest {
 protected:
  template <typename Key, typename Value>
  double inflation(GDFSCache<Key, Value>& cache) const {
    return cache._inflation;
  }

  template <typename Key, typename Value>
  const boost::heap::fibonacci_heap<typename GDFSCache<Key, Value>::GDFSCacheEntry>& queue(
      GDFSCache<Key, Value>& cache) const {
    return cache._queue;
  }
};

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

  ASSERT_EQ(2, cache.get(1));  // Hit, L=0, Fr=2
  ASSERT_EQ(2.0, cache.priority(1));

  cache.set(1, 2);  // Hit, L=0, Fr=3
  ASSERT_EQ(3.0, cache.priority(1));

  cache.set(2, 4);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, cache.priority(2));

  cache.set(3, 6);  // Miss, evict 2, L=1, Fr=1
  ASSERT_EQ(2.0, cache.priority(3));
  ASSERT_EQ(1.0, inflation(cache));

  ASSERT_EQ(2.0, queue(cache).top().priority);

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(6, cache.get(3));  // Hit, L=1, Fr=2
  ASSERT_EQ(3.0, cache.priority(3));
  ASSERT_EQ(3.0, queue(cache).top().priority);

  ASSERT_EQ(6, cache.get(3));  // Hit, L=1, Fr=3
  ASSERT_EQ(4.0, cache.priority(3));
  ASSERT_EQ(3.0, cache.priority(1));
  ASSERT_EQ(3.0, queue(cache).top().priority);

  cache.set(2, 5);  // Miss, evict 1, L=1, Fr=3
  ASSERT_EQ(3.0, inflation(cache));

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(cache.frequency(3), 3);
  ASSERT_EQ(cache.frequency(100), 0);
}

TEST_F(CachePolicyTest, GDFSSnapshotTest) {
  GDFSCache<int, int> cache(5);
  const auto values = {1, 2, 3, 4, 5};
  for (const auto value : values) {
    cache.set(value, value);
  }

  const auto snapshot = cache.snapshot();
  for (const auto value : values) {
    const auto it = snapshot.find(value);
    EXPECT_NE(it, snapshot.end());

    const auto entry = it->second;
    EXPECT_EQ(entry.value, value);
    EXPECT_EQ(entry.frequency, 1);
  }
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

}  // namespace opossum
