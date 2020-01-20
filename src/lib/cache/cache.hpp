#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
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

  // Adds or refreshes the cache entry [query, value].
  void set(const Key& query, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (_impl->capacity() == 0) return;

    _impl->set(query, value);
  }

  // Tries to fetch the cache entry for the query into the result object. Returns true if the entry was found, false
  // otherwise. This needs a write lock to be acquired as most implementation update some type of access count when
  // retrieving an entry.
  std::optional<Value> try_get(const Key& query) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (_impl->capacity() == 0) return {};

    if (!_impl->has(query)) {
      return {};
    }
    return _impl->get(query);
  }

  // Checks whether an entry for the query exists.
  bool has(const Key& query) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _impl->has(query);
  }

  // Returns and refreshes the cache entry for the given query. Causes undefined behavior if the query is not in the
  // cache. This needs a write lock to be acquired as most implementation update some type of access count when
  // retrieving an entry.
  Value get_entry(const Key& query) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    return _impl->get(query);
  }

  // Purges all entries from the cache.
  void clear() {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _impl->clear();
  }

  void resize(size_t capacity) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _impl->resize(capacity);
  }

  size_t size() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _impl->size();
  }

  // Replaces the underlying cache by creating a new object of the given cache type.
  template <class cache_t>
  void replace_cache_impl(size_t capacity) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _impl = std::make_unique<cache_t>(capacity);
  }

  // These methods are named "unsafe_" (similar to tbb's naming) because iterator does not hold a mutex. As such,
  // modifications to the cache invalidate the iterators. While this is also true for begin()/end() in other data
  // structures, the Cache class usually deals with concurrency.
  Iterator unsafe_begin() { return _impl->begin(); }
  Iterator unsafe_end() { return _impl->end(); }

  // Returns the access frequency of a cached item (=1 for set(), +1 for each get()). Returns 0 for keys not being
  // cache-resident.
  size_t frequency(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _impl->frequency(key);
  }

 protected:
  // Underlying cache eviction strategy.
  std::unique_ptr<AbstractCacheImpl<Key, Value>> _impl;

  mutable std::shared_mutex _mutex;
};

}  // namespace opossum
