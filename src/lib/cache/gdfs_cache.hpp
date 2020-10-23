#pragma once

#include <shared_mutex>

#include "abstract_cache.hpp"
#include "boost/heap/fibonacci_heap.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Generic cache implementation using the GDFS policy.
 * To iterate over the cache in a thread-safe manner, use the copy provided by snapshot().
 * Different cache implementations existed in the past, but were retired with PR 2129.
 */
template <typename Key, typename Value>
class GDFSCache : public AbstractCache<Key, Value> {
 public:
  // Entries within the GDFS cache.
  struct GDFSCacheEntry {
    Key key;
    Value value;
    size_t frequency;
    double size;
    double priority;

    // The underlying heap is a max-heap.
    // To have the item with lowest priority at the top, we invert the comparison.
    bool operator<(const GDFSCacheEntry& other) const { return priority > other.priority; }
  };

  using Handle = typename boost::heap::fibonacci_heap<GDFSCacheEntry>::handle_type;
  using CacheMap = typename std::unordered_map<Key, Handle>;
  using SnapshotEntry = typename AbstractCache<Key, Value>::SnapshotEntry;

  explicit GDFSCache(size_t capacity = DEFAULT_CACHE_CAPACITY) : AbstractCache<Key, Value>(capacity), _inflation(0.0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) final {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    if (this->_capacity == 0) return;
    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update priority.
      Handle handle = it->second;

      GDFSCacheEntry& entry = (*handle);
      entry.value = value;
      entry.size = size;
      entry.frequency++;
      entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
      _queue.update(handle);

      return;
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    if (_queue.size() >= this->_capacity) {
      _evict();
    }

    // Insert new item in cache.
    GDFSCacheEntry entry{key, value, 1, size, 0.0};
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    Handle handle = _queue.push(entry);
    _map[key] = handle;
  }

  std::optional<Value> try_get(const Key& query) final {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    auto it = _map.find(query);
    if (it == _map.end()) return std::nullopt;

    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const Key& key) const final {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _map.contains(key);
  }

  size_t size() const final {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _map.size();
  }

  void clear() final {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _map.clear();
    _queue.clear();
  }

  void resize(size_t capacity) final {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    while (_queue.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

  std::unordered_map<Key, SnapshotEntry> snapshot() const final {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    std::unordered_map<Key, SnapshotEntry> map_copy(_map.size());
    for (const auto& [key, entry] : _map) {
      map_copy[key] = SnapshotEntry{(*entry).value, (*entry).frequency};
    }
    return map_copy;
  }

 protected:
  friend class CachePolicyTest;
  friend class QueryPlanCacheTest;

  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<GDFSCacheEntry> _queue;

  // Map to point towards element in the list.
  CacheMap _map;

  /**
   * Locking this data structure is easier than using a concurrent data structure (e.g. TBB) as
   * (1) both the queue and the map would have to be concurrent data structures,
   * (2) their modifications would have to be synchronized, and
   * (3) TBB's concurrent_unordered_map does not provide safe deletion and concurrent_hash_map is difficult to use.
   */
  mutable std::shared_mutex _mutex;

  // Inflation value that will be updated whenever an item is evicted.
  double _inflation;

  void _evict() final {
    auto top = _queue.top();

    _inflation = top.priority;
    _map.erase(top.key);
    _queue.pop();
  }
};

}  // namespace opossum
