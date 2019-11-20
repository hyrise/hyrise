#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gdfs_cache.hpp"

#include "utils/singleton.hpp"

namespace opossum {

inline constexpr size_t DefaultCacheCapacity = 1024;

// Per-default, uses the GDFS cache as underlying storage.
template <typename Value, typename Key = std::string>
class Cache {
 public:
  using Iterator = typename AbstractCacheImpl<Key, Value>::ErasedIterator;

  explicit Cache(size_t capacity = DefaultCacheCapacity)
      : _impl(std::move(std::make_unique<GDFSCache<Key, Value>>(capacity))) {}

  virtual ~Cache() {}

  // Adds or refreshes the cache entry [query, value].
  void set(const Key& query, const Value& value) {
    if (_impl->capacity() == 0) return;

    std::lock_guard<std::mutex> lock(_mutex);
    _impl->set(query, value);
  }

  // Tries to fetch the cache entry for the query into the result object.
  // Returns true if the entry was found, false otherwise.
  std::optional<Value> try_get(const Key& query) {
    if (_impl->capacity() == 0) return {};

    std::lock_guard<std::mutex> lock(_mutex);
    if (!_impl->has(query)) {
      return {};
    }
    return _impl->get(query);
  }

  // Checks whether an entry for the query exists.
  bool has(const Key& query) const { return _impl->has(query); }

  // Returns and refreshes the cache entry for the given query.
  // Causes undefined behavior if the query is not in the cache.
  Value get_entry(const Key& query) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _impl->get(query);
  }

  // Purges all entries from the cache.
  void clear() { _impl->clear(); }

  void resize(size_t capacity) { _impl->resize(capacity); }

  // Returns the access frequency of a cached item (=1 for set(), +1 for each get()).
  // Returns 0 for keys not being cache-resident.
  size_t size() const { return _impl->size(); }

  // Returns a reference to the underlying cache.
  AbstractCacheImpl<Key, Value>& cache() { return *_impl; }
  const AbstractCacheImpl<Key, Value>& cache() const { return *_impl; }

  // Replaces the underlying cache by creating a new object
  // of the given cache type.
  template <class cache_t>
  void replace_cache_impl(size_t capacity) {
    _impl = std::make_unique<cache_t>(capacity);
  }

  Iterator begin() { return _impl->begin(); }

  Iterator end() { return _impl->end(); }

  size_t frequency(const Key& key) { return _impl->frequency(key); }

 protected:
  // Underlying cache eviction strategy.
  std::unique_ptr<AbstractCacheImpl<Key, Value>> _impl;

  std::mutex _mutex;
};

}  // namespace opossum
