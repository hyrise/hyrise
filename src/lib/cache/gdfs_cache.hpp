#pragma once

#include <list>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"
#include "boost/heap/fibonacci_heap.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
 * Generic cache implementation using the GDFS policy.
 * To iterate over a cache copy, use snapshot().
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

  struct SnapshotEntry {
    Value value;
    size_t frequency;
  };

  using Handle = typename boost::heap::fibonacci_heap<GDFSCacheEntry>::handle_type;
  using CacheMap = typename std::unordered_map<Key, Handle>;

  explicit GDFSCache(size_t capacity = DEFAULT_CACHE_CAPACITY) : AbstractCache<Key, Value>(capacity), _inflation(0.0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
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

  std::optional<Value> try_get(const Key& query) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    if (this->_capacity == 0) return std::nullopt;

    auto it = _map.find(query);
    if (it == _map.end()) return std::nullopt;

    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  Value& get(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    auto it = _map.find(key);
    DebugAssert(it != _map.end(), "Key not present.");
    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _map.find(key) != _map.end();
  }

  size_t size() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _map.size();
  }

  void clear() {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _map.clear();
    _queue.clear();
  }

  void resize(size_t capacity) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    while (_queue.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

  double priority(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    auto it = _map.find(key);
    return (*it->second).priority;
  }

  size_t frequency(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    const auto it = _map.find(key);
    if (it == _map.end()) {
      return size_t{0};
    }
    return (*it->second).frequency;
  }

  std::unordered_map<Key, SnapshotEntry> snapshot() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    std::unordered_map<Key, SnapshotEntry> map_copy;
    for (auto it = _map.begin(); it != _map.end(); it++) {
      const auto [key, entry] = *it;
      map_copy[key] = SnapshotEntry{(*entry).value, (*entry).frequency};
    }
    return map_copy;
  }

 protected:
  friend class CachePolicyTest;

  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<GDFSCacheEntry> _queue;

  // Map to point towards element in the list.
  // We use a locked map here since TBB's concurrent maps are not safe for erasing.
  CacheMap _map;

  mutable std::shared_mutex _mutex;

  // Inflation value that will be updated whenever an item is evicted.
  double _inflation;

  void _evict() {
    auto top = _queue.top();

    _inflation = top.priority;
    _map.erase(top.key);
    _queue.pop();
  }
};

}  // namespace opossum
