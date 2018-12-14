#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"
#include "boost/function.hpp"
#include "boost/heap/fibonacci_heap.hpp"
#include "boost/iterator/transform_iterator.hpp"

namespace opossum {

// Generic cache implementation using the GDFS policy.
// Note: This implementation is not thread-safe.
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

  explicit GDFSCache(size_t capacity) : AbstractCache<Key, Value>(capacity), _inflation(0.0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update priority.
      Handle handle = it->second;

      GDFSCacheEntry& entry = (*handle);
      entry.value = value;
      entry.size = size;
      entry.frequency++;
      entry.priority = _inflation + entry.frequency / entry.size;
      _queue.update(handle);

      return;
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    if (_queue.size() >= this->_capacity) {
      auto top = _queue.top();
      _inflation = top.priority;
      _map.erase(top.key);
      _queue.pop();
    }

    // Insert new item in cache.
    GDFSCacheEntry entry{key, value, 1, size, 0.0};
    entry.priority = _inflation + entry.frequency / entry.size;
    Handle handle = _queue.push(entry);
    _map[key] = handle;
  }

  Value& get(const Key& key) {
    auto it = _map.find(key);
    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + entry.frequency / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const Key& key) const { return _map.find(key) != _map.end(); }

  size_t size() const { return _map.size(); }

  void clear() {
    _map.clear();
    _queue.clear();
  }

  void resize(size_t capacity) {
    while (_queue.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

  const boost::heap::fibonacci_heap<GDFSCacheEntry>& queue() const { return _queue; }

  double inflation() const { return _inflation; }

  double priority(const Key& key) const {
    auto it = _map.find(key);
    return (*it->second).priority;
  }

  static const std::pair<Key, Value> get_value(std::pair<Key, Handle> const& p)  {
    const Handle handle = p.second;
    const GDFSCacheEntry& entry = (*handle);
    return std::make_pair(p.first, entry.value);
  }

  using CacheIterator = typename std::unordered_map<Key, Handle>::const_iterator;

  typedef boost::function<const std::pair<Key, Value> (const std::pair<Key, Handle>&)> F;
  typedef boost::transform_iterator<F, CacheIterator> transform_iterator;

  transform_iterator begin() {
    return boost::make_transform_iterator(_map.begin(), &get_value);
  }

  transform_iterator end() {
    return boost::make_transform_iterator(_map.end(), &get_value);
  }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<GDFSCacheEntry> _queue;

  // Map to point towards element in the list.
  CacheMap _map;

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
