#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache_impl.hpp"
#include "boost/heap/fibonacci_heap.hpp"

namespace opossum {

// Generic cache implementation using the GDS policy.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class GDSCache : public AbstractCacheImpl<Key, Value> {
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

  using Handle = typename boost::heap::fibonacci_heap<GDSCacheEntry>::handle_type;

  using typename AbstractCacheImpl<Key, Value>::KeyValuePair;
  using typename AbstractCacheImpl<Key, Value>::AbstractIterator;
  using typename AbstractCacheImpl<Key, Value>::ErasedIterator;

  class Iterator : public AbstractIterator {
   public:
    using IteratorType = typename std::unordered_map<Key, Handle>::iterator;
    explicit Iterator(IteratorType p) : _wrapped_iterator(p) {}

   private:
    friend class boost::iterator_core_access;
    friend class AbstractCacheImpl<Key, Value>::ErasedIterator;

    IteratorType _wrapped_iterator;
    mutable KeyValuePair _tmp_return_value;

    void increment() { ++_wrapped_iterator; }

    bool equal(const AbstractIterator& other) const {
      return _wrapped_iterator == static_cast<const Iterator&>(other)._wrapped_iterator;
    }

    const KeyValuePair& dereference() const {
      const auto iter_value = *_wrapped_iterator;
      _tmp_return_value = {iter_value.first, (*iter_value.second).value};
      return _tmp_return_value;
    }
  };

  explicit GDSCache(size_t capacity) : AbstractCacheImpl<Key, Value>(capacity), _inflation(0.0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update priority.
      Handle handle = it->second;

      GDSCacheEntry& entry = (*handle);
      entry.value = value;
      entry.size = size;
      entry.cost = cost;
      entry.priority = _inflation + entry.cost / entry.size;
      _queue.update(handle);

      return;
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    if (_queue.size() >= this->_capacity) {
      _evict();
    }

    // Insert new item in cache.
    GDSCacheEntry entry{key, value, cost, size, 0.0};
    entry.priority = _inflation + entry.cost / entry.size;
    Handle handle = _queue.push(entry);
    _map[key] = handle;
  }

  Value& get(const Key& key) {
    auto it = _map.find(key);
    DebugAssert(it != _map.end(), "key not present");
    Handle handle = it->second;
    GDSCacheEntry& entry = (*handle);
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

  void resize(size_t capacity) {
    while (_queue.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

  const boost::heap::fibonacci_heap<GDSCacheEntry>& queue() const { return _queue; }

  double inflation() const { return _inflation; }

  double priority(const Key& key) const {
    auto it = _map.find(key);
    return (*it->second).priority;
  }

  ErasedIterator begin() { return ErasedIterator{std::make_unique<Iterator>(_map.begin())}; }

  ErasedIterator end() { return ErasedIterator{std::make_unique<Iterator>(_map.end())}; }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<GDSCacheEntry> _queue;

  // Map to point towards element in the list.
  std::unordered_map<Key, Handle> _map;

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
