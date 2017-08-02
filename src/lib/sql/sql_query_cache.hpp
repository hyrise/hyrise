#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "gdfs_cache.hpp"

#include "SQLParserResult.h"

namespace opossum {

// Cache that stores instances of SQLParserResult.
// Per-default, uses the GDFS cache as underlying storage.
template <typename val_t, typename key_t = std::string>
class SQLQueryCache {
 public:
  explicit SQLQueryCache(size_t capacity) : _cache(std::move(std::make_unique<GDFSCache<key_t, val_t>>(capacity))) {}

  virtual ~SQLQueryCache() {}

  // Adds or refreshes the cache entry [query, value].
  void set(const key_t& query, const val_t& value) {
    if (_cache->capacity() == 0) return;

    std::lock_guard<std::mutex> lock(_mutex);
    _cache->set(query, value);
  }

  // Tries to fetch the cache entry for the query into the result object.
  // Returns true if the entry was found, false otherwise.
  optional<val_t> try_get(const key_t& query) {
    if (_cache->capacity() == 0) return {};

    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache->has(query)) {
      return {};
    }
    return _cache->get(query);
  }

  // Checks whether an entry for the query exists.
  bool has(const key_t& query) const { return _cache->has(query); }

  // Returns and refreshes the cache entry for the given query.
  // Causes undefined behavior if the query is not in the cache.
  val_t get(const key_t& query) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _cache->get(query);
  }

  // Purges all entries from the cache and reinitializes it with the given capacity.
  void clear_and_resize(size_t capacity) { _cache->clear_and_resize(capacity); }

  // Purges all entries from the cache.
  void clear() { _cache->clear(); }

  size_t size() const { return _cache->size(); }

  // Returns a reference to the underlying cache.
  AbstractCache<key_t, val_t>& cache() { return *_cache; }

  // Replaces the underlying cache by creating a new object
  // of the given cache type.
  template <class cache_t>
  void replace_cache_impl(size_t capacity) {
    _cache = std::make_unique<cache_t>(capacity);
  }

 protected:
  // Underlying cache strategy.
  std::unique_ptr<AbstractCache<key_t, val_t>> _cache;

  std::mutex _mutex;
};

}  // namespace opossum
