#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"
#include "boost/heap/fibonacci_heap.hpp"
#include "utils/assert.hpp"

namespace opossum {

// Generic cache implementation using the GDFS policy.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class GDFSCacheLock : public AbstractCache<Key, Value> {
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
  using CacheSnapshot = typename std::unordered_map<Key, GDFSCacheEntry>;

  using typename AbstractCache<Key, Value>::KeyValuePair;
  using typename AbstractCache<Key, Value>::AbstractIterator;
  using typename AbstractCache<Key, Value>::ErasedIterator;

  class Iterator : public AbstractIterator {
   public:
    using IteratorType = typename CacheMap::iterator;
    explicit Iterator(IteratorType p) : _wrapped_iterator(p) {}

   private:
    friend class boost::iterator_core_access;
    friend class AbstractCache<Key, Value>::ErasedIterator;

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

  explicit GDFSCacheLock(size_t capacity = 1024) : AbstractCache<Key, Value>(capacity), _inflation(0.0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    {
      std::shared_lock<std::shared_mutex> map_lock(_map_mutex);
      std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
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
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    {
      std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
      std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
      while (_queue.size() >= this->_capacity) {
        _evict();
      }
    }

    // Insert new item in cache.
    GDFSCacheEntry entry{key, value, 1, size, 0.0};
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
    std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
    Handle handle = _queue.push(entry);
    _map[key] = handle;
  }

  std::optional<Value> try_get(const Key& query) {
    std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
    std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
    auto it = _map.find(query);
    if (it == _map.end()) return {};
    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  Value& get(const Key& key) {
    std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
    std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
    auto it = _map.find(key);
    DebugAssert(it != _map.end(), "key not present");
    Handle handle = it->second;
    GDFSCacheEntry& entry = (*handle);
    entry.frequency++;
    entry.priority = _inflation + static_cast<double>(entry.frequency) / entry.size;
    _queue.update(handle);
    return entry.value;
  }

  bool has(const Key& key) const {
    std::shared_lock<std::shared_mutex> map_lock(_map_mutex);
    return _map.find(key) != _map.end();
  }

  size_t size() const {
    std::shared_lock<std::shared_mutex> map_lock(_map_mutex);
    return _map.size();
  }

  void clear() {
    std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
    std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
    _map.clear();
    _queue.clear();
  }

  void resize(size_t capacity) {
    std::unique_lock<std::shared_mutex> map_lock(_map_mutex);
    std::unique_lock<std::shared_mutex> queue_lock(_queue_mutex);
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

  CacheSnapshot snapshot() const {
    std::shared_lock<std::shared_mutex> map_lock(_map_mutex);
    std::shared_lock<std::shared_mutex> queue_lock(_queue_mutex);
    CacheSnapshot _map_copy;
    for (auto it = _map.begin(); it != _map.end(); it++) {
      const auto [key, entry] = *it;
      _map_copy[key] = *entry;
    }
    return _map_copy;
  }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<GDFSCacheEntry> _queue;

  // Map to point towards element in the list.
  CacheMap _map;

  //mutable std::shared_mutex _mutex;
  mutable std::shared_mutex _map_mutex;
  mutable std::shared_mutex _queue_mutex;

  // Inflation value that will be updated whenever an item is evicted.
  std::atomic<double> _inflation;

  void _evict() {
    auto top = _queue.top();
    _inflation = top.priority;
    _map.erase(top.key);
    _queue.pop();
  }
};

}  // namespace opossum
