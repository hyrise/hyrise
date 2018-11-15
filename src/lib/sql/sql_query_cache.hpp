#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gdfs_cache.hpp"

#include "SQLParserResult.h"
#include "utils/singleton.hpp"

namespace opossum {

inline constexpr size_t DefaultCacheCapacity = 1024;

// Cache that stores instances of SQLParserResult.
// Per-default, uses the GDFS cache as underlying storage.
template <typename Value, typename Key = std::string>
class SQLQueryCache : public Singleton<SQLQueryCache<Value, Key>> {
 public:
  explicit SQLQueryCache(size_t capacity = DefaultCacheCapacity)
      : _cache(std::move(std::make_unique<GDFSCache<Key, Value>>(capacity))) {}

  virtual ~SQLQueryCache() {}

  // Adds or refreshes the cache entry [query, value].
  void set(const Key& query, const Value& value) {
    if (_cache->capacity() == 0) return;

    std::lock_guard<std::mutex> lock(_mutex);
    _cache->set(query, value);
  }

  // Tries to fetch the cache entry for the query into the result object.
  // Returns true if the entry was found, false otherwise.
  std::optional<Value> try_get(const Key& query) {
    if (_cache->capacity() == 0) return {};

    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache->has(query)) {
      return {};
    }
    return _cache->get(query);
  }

  // Checks whether an entry for the query exists.
  bool has(const Key& query) const { return _cache->has(query); }

  // Returns and refreshes the cache entry for the given query.
  // Causes undefined behavior if the query is not in the cache.
  Value get_entry(const Key& query) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _cache->get(query);
  }

  // Purges all entries from the cache.
  void clear() { _cache->clear(); }

  void resize(size_t capacity) { _cache->resize(capacity); }

  size_t size() const { return _cache->size(); }

  // Returns a reference to the underlying cache.
  AbstractCache<Key, Value>& cache() { return *_cache; }
  const AbstractCache<Key, Value>& cache() const { return *_cache; }

  // Replaces the underlying cache by creating a new object
  // of the given cache type.
  template <class cache_t>
  void replace_cache_impl(size_t capacity) {
    _cache = std::make_unique<cache_t>(capacity);
  }

 protected:
  // Underlying cache strategy.
  std::unique_ptr<AbstractCache<Key, Value>> _cache;

  std::mutex _mutex;
};

}  // namespace opossum
