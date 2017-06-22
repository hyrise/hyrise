#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "lru_cache.hpp"

#include "SQLParserResult.h"

namespace opossum {

// Cache that stores instances of SQLParserResult.
// Uses the least-recently-used cache as underlying storage.
template <typename val_t, typename key_t = std::string>
class SQLQueryCache {
 public:
  explicit SQLQueryCache(size_t capacity) : _cache(capacity) {}

  virtual ~SQLQueryCache() {}

  // Adds or refreshes the cache entry [query, value].
  void set(const key_t& query, val_t value) {
    if (_cache.capacity() == 0) return;

    std::lock_guard<std::mutex> lock(_mutex);
    _cache.set(query, value);
  }

  // Tries to fetch the cache entry for the query into the result object.
  // Returns true if the entry was found, false otherwise.
  bool try_get(const key_t& query, val_t* result) {
    if (_cache.capacity() == 0) return false;

    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache.has(query)) {
      return false;
    }
    *result = _cache.get(query);
    return true;
  }

  // Checks whether an entry for the query exists.
  bool has(const key_t& query) const { return _cache.has(query); }

  // Returns and refreshes the cache entry for the given query.
  // Causes undefined behavior if the query is not in the cache.
  val_t get(const key_t& query) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _cache.get(query);
  }

  // Purges all entries from the cache and reinitializes it with the given capacity.
  void clear_and_resize(size_t capacity) { _cache = LRUCache<key_t, val_t>(capacity); }

  // Purges all entries from the cache.
  void clear() {
    size_t capacity = _cache.capacity();
    _cache = LRUCache<key_t, val_t>(capacity);
  }

  inline size_t size() const { return _cache.size(); }

  inline LRUCache<key_t, val_t>& cache() { return _cache; }

 protected:
  LRUCache<key_t, val_t> _cache;

  std::mutex _mutex;
};

}  // namespace opossum
