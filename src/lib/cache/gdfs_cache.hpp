#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache_impl.hpp"
#include "boost/heap/fibonacci_heap.hpp"
#include "utils/assert.hpp"

namespace opossum {

// Generic cache implementation using the GDFS policy.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class GDFSCache : public AbstractCacheImpl<Key, Value> {
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

  using typename AbstractCacheImpl<Key, Value>::KeyValuePair;
  using typename AbstractCacheImpl<Key, Value>::AbstractIterator;
  using typename AbstractCacheImpl<Key, Value>::ErasedIterator;

  class Iterator : public AbstractIterator {
   public:
    using IteratorType = typename CacheMap::iterator;
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

  explicit GDFSCache(size_t capacity) : AbstractCacheImpl<Key, Value>(capacity), _inflation(0.0) {}

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
    DebugAssert(it != _map.end(), "key not present");
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

  ErasedIterator begin() { return ErasedIterator{std::make_unique<Iterator>(_map.begin())}; }

  ErasedIterator end() { return ErasedIterator{std::make_unique<Iterator>(_map.end())}; }

  size_t frequency(const Key& key) {
    const auto it = _map.find(key);
    if (it == _map.end()) {
      return size_t{0};
    }

    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    return entry.frequency;
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
