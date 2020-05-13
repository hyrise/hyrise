#include "base_test.hpp"

namespace opossum {

// Test for the cache implementation in lib/cache.
// Not using SQL types in this test, only testing cache eviction.
class CachePolicyTest : public BaseTest {
 protected:
  template <typename Key, typename Value>
  double inflation(const GDFSCache<Key, Value>& cache) const {
    return cache._inflation;
  }

  template <typename Key, typename Value>
  const boost::heap::fibonacci_heap<typename GDFSCache<Key, Value>::GDFSCacheEntry>& queue(
      const GDFSCache<Key, Value>& cache) const {
    return cache._queue;
  }

  template <typename Key, typename Value>
  const typename GDFSCache<Key, Value>::GDFSCacheEntry get_full_entry(const GDFSCache<Key, Value>& cache,
                                                                      const Key& key) const {
    return *(cache._map.find(key)->second);
  }
};

// GDFS Strategy
TEST_F(CachePolicyTest, GDFSCacheTest) {
  GDFSCache<int, int> cache(2);

  ASSERT_FALSE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  cache.set(1, 2);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, get_full_entry(cache, 1).priority);
  ASSERT_EQ(1, get_full_entry(cache, 1).frequency);

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_FALSE(cache.has(3));

  ASSERT_EQ(2, cache.try_get(1));  // Hit, L=0, Fr=2
  ASSERT_EQ(2.0, get_full_entry(cache, 1).priority);
  ASSERT_EQ(2, get_full_entry(cache, 1).frequency);

  cache.set(1, 2);  // Hit, L=0, Fr=3
  ASSERT_EQ(3.0, get_full_entry(cache, 1).priority);
  ASSERT_EQ(3, get_full_entry(cache, 1).frequency);

  cache.set(2, 4);  // Miss, insert, L=0, Fr=1
  ASSERT_EQ(1.0, get_full_entry(cache, 2).priority);
  ASSERT_EQ(1, get_full_entry(cache, 2).frequency);

  cache.set(3, 6);  // Miss, evict 2, L=1, Fr=1
  ASSERT_EQ(2.0, get_full_entry(cache, 3).priority);
  ASSERT_EQ(1.0, inflation(cache));
  ASSERT_EQ(1, get_full_entry(cache, 3).frequency);

  ASSERT_EQ(2.0, queue(cache).top().priority);

  ASSERT_TRUE(cache.has(1));
  ASSERT_FALSE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(6, cache.try_get(3));  // Hit, L=1, Fr=2
  ASSERT_EQ(3.0, get_full_entry(cache, 3).priority);
  ASSERT_EQ(3.0, queue(cache).top().priority);
  ASSERT_EQ(2, get_full_entry(cache, 3).frequency);

  ASSERT_EQ(6, cache.try_get(3));  // Hit, L=1, Fr=3
  ASSERT_EQ(4.0, get_full_entry(cache, 3).priority);
  ASSERT_EQ(3.0, get_full_entry(cache, 1).priority);
  ASSERT_EQ(3.0, queue(cache).top().priority);
  ASSERT_EQ(3, get_full_entry(cache, 3).frequency);

  cache.set(2, 5);  // Miss, evict 1, L=1, Fr=3
  ASSERT_EQ(3.0, inflation(cache));
  ASSERT_EQ(1, get_full_entry(cache, 2).frequency);

  ASSERT_FALSE(cache.has(1));
  ASSERT_TRUE(cache.has(2));
  ASSERT_TRUE(cache.has(3));

  ASSERT_EQ(3, get_full_entry(cache, 3).frequency);
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

TEST_F(CacheTest, NoGrowthOverCapacity) {
  GDFSCache<int, int> cache(3);

  cache.set(1, 2);
  cache.set(2, 4);
  cache.set(4, 6);
  cache.set(6, 8);

  ASSERT_EQ(cache.size(), 3u);
}

TEST_F(CacheTest, TryGet) {
  {
    GDFSCache<int, int> cache(0);
    cache.set(1, 2);
    ASSERT_EQ(cache.try_get(1), std::nullopt);
  }

  GDFSCache<int, int> cache(3);
  cache.set(1, 2);
  ASSERT_EQ(cache.try_get(2), std::nullopt);
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

TEST_F(CacheTest, Snapshot) {
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

}  // namespace opossum
