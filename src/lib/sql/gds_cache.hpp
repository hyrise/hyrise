#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"
#include "boost/heap/fibonacci_heap.hpp"

namespace opossum {

// Generic cache implementation using the GDS policy.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class GDSCache : public AbstractCache<Key, Value> {
 public:
  // Entries within the GDS cache.
  struct GDSCacheEntry {
    Key key;
    Value value;
    double cost;
    double size;
    double priority;

    // The underlying heap is a max-heap.
    // To have the item with lowest priority at the top, we invert the comparison.
    bool operator<(const GDSCacheEntry& other) const { return priority > other.priority; }
  };

  typedef GDSCacheEntry entry_t;
  typedef typename boost::heap::fibonacci_heap<entry_t>::handle_type handle_t;

  explicit GDSCache(size_t capacity) : AbstractCache<Key, Value>(capacity), _inflation(0.0) {}

  std::optional<Key> set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update priority.
      handle_t handle = it->second;

      entry_t& entry = (*handle);
      entry.value = value;
      entry.size = size;
      entry.cost = cost;
      entry.priority = _inflation + entry.cost / entry.size;
      _queue.update(handle);

      return {};
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    std::optional<Key> evicted;
    if (_queue.size() >= this->_capacity) {
      auto top = _queue.top();

      evicted = std::make_optional(top.key);

      _inflation = top.priority;
      _map.erase(top.key);
      _queue.pop();
    }

    // Insert new item in cache.
    entry_t entry{key, value, cost, size, 0.0};
    entry.priority = _inflation + entry.cost / entry.size;
    handle_t handle = _queue.push(entry);
    _map[key] = handle;

    return evicted;
  }

  Value& get(const Key& key) {
    auto it = _map.find(key);
    handle_t handle = it->second;
    entry_t& entry = (*handle);
    entry.priority = _inflation + entry.cost / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const Key& key) const { return _map.find(key) != _map.end(); }

  size_t size() const { return _map.size(); }

  void clear() {
    _map.clear();
    _queue.clear();
  }

  void clear_and_resize(size_t capacity) {
    clear();
    this->_capacity = capacity;
  }

  const boost::heap::fibonacci_heap<entry_t>& queue() const { return _queue; }

  double inflation() const { return _inflation; }

  double priority(const Key& key) const {
    auto it = _map.find(key);
    return (*it->second).priority;
  }

  void resize(size_t capacity) {
    if (_queue.size() > capacity) {
      for (size_t difference = 0; difference < _queue.size() - capacity; ++difference) {
        auto top = _queue.top();

        _inflation = top.priority;
        _map.erase(top.key);
        _queue.pop();
      }
    }
    this->_capacity = capacity;
  }

  std::vector<Key> dump_cache() {
    std::vector<Key> cache;
    for (auto& entry : _queue) {
      cache.push_back(entry.key);
    }
    return cache;
  }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<entry_t> _queue;

  // Map to point towards element in the list.
  std::unordered_map<Key, handle_t> _map;

  // Inflation value that will be updated whenever an item is evicted.
  double _inflation;
};

}  // namespace opossum
