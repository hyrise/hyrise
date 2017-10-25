#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"
#include "boost/heap/fibonacci_heap.hpp"

namespace opossum {

// Generic cache implementation using the GDFS policy.
// Note: This implementation is not thread-safe.
template <typename key_t, typename val_t>
class GDFSCache : public AbstractCache<key_t, val_t> {
 public:
  // Entries within the GDFS cache.
  struct GDFSCacheEntry {
    key_t key;
    val_t value;
    size_t frequency;
    double size;
    double priority;

    // The underlying heap is a max-heap.
    // To have the item with lowest priority at the top, we invert the comparison.
    bool operator<(const GDFSCacheEntry& rhs) const { return priority > rhs.priority; }
  };

  typedef GDFSCacheEntry entry_t;
  typedef typename boost::heap::fibonacci_heap<entry_t>::handle_type handle_t;

  explicit GDFSCache(size_t capacity) : AbstractCache<key_t, val_t>(capacity), _inflation(0.0) {}

  void set(const key_t& key, const val_t& value, double cost = 1.0, double size = 1.0) {
    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update priority.
      handle_t handle = it->second;

      entry_t& entry = (*handle);
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
    entry_t entry{key, value, 1, size, 0.0};
    entry.priority = _inflation + entry.frequency / entry.size;
    handle_t handle = _queue.push(entry);
    _map[key] = handle;
  }

  val_t& get(const key_t& key) {
    auto it = _map.find(key);
    handle_t handle = it->second;
    entry_t& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + entry.frequency / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const key_t& key) const { return _map.find(key) != _map.end(); }

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

  double priority(const key_t& key) const {
    auto it = _map.find(key);
    return (*it->second).priority;
  }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<entry_t> _queue;

  // Map to point towards element in the list.
  std::unordered_map<key_t, handle_t> _map;

  // Inflation value that will be updated whenever an item is evicted.
  double _inflation;
};

}  // namespace opossum
