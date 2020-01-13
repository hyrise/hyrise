#pragma once

#include <list>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_cache_impl.hpp"
#include "boost/heap/fibonacci_heap.hpp"

namespace opossum {

// Generic cache implementation using the LRU-K policy.
// When an item should be evicted the item with the largest backward k-distance is evicted.
// This is the item whose k-th most recent access is the furthest in the past.
// Note: This implementation is not thread-safe.
template <size_t K, typename Key, typename Value>
class LRUKCache : public AbstractCacheImpl<Key, Value> {
 public:
  // Entries within the LRU-K cache.
  // They keep a reference history of the K last accesses.
  class LRUKCacheEntry {
   public:
    Key key;
    Value value;

    // Reference history, ordered with the least-recent access at the beginning.
    std::vector<size_t> history;

    // Adds an entry to the history list of the cache entry.
    // If the history has K elements, all items are shifted left
    // and the new entry is added at the end.
    void add_history_entry(size_t current_access_counter) {
      if (history.size() < K) {
        history.push_back(current_access_counter);
        return;
      }

      for (size_t i = K - 1; i > 0; --i) {
        history[i - 1] = history[i];
      }
      history[K - 1] = current_access_counter;
    }

    // The underlying heap in the LRU-K cache is a max-heap.
    // The item with the largest backward k-distance should be at the top.
    // This is the item with the lowest k-th most recent access number.
    // If the history of both items is not equally long, the item with the shorter history is evicted.
    bool operator<(const LRUKCacheEntry& other) const {
      if (history.size() != other.history.size()) {
        return history.size() > other.history.size();
      }
      return history[0] > other.history[0];
    }
  };

  using Handle = typename boost::heap::fibonacci_heap<LRUKCacheEntry>::handle_type;

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

  explicit LRUKCache(size_t capacity) : AbstractCacheImpl<Key, Value>(capacity), _access_counter(0) {}

  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    ++_access_counter;

    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update entry.
      Handle handle = it->second;

      LRUKCacheEntry& entry = (*handle);
      entry.value = value;
      entry.add_history_entry(_access_counter);
      _queue.update(handle);
      return;
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    if (_queue.size() >= this->_capacity) {
      _evict();
    }

    // Insert new item in cache.
    LRUKCacheEntry entry{key, value, {_access_counter}};
    Handle handle = _queue.push(entry);
    _map[key] = handle;
  }

  Value& get(const Key& key) {
    ++_access_counter;

    auto it = _map.find(key);
    DebugAssert(it != _map.end(), "key not present");

    Handle handle = it->second;
    LRUKCacheEntry& entry = (*handle);
    entry.add_history_entry(_access_counter);
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

  const boost::heap::fibonacci_heap<LRUKCacheEntry>& queue() const { return _queue; }

  ErasedIterator begin() { return ErasedIterator{std::make_unique<Iterator>(_map.begin())}; }

  ErasedIterator end() { return ErasedIterator{std::make_unique<Iterator>(_map.end())}; }

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<LRUKCacheEntry> _queue;

  // Map to point towards element in the list.
  std::unordered_map<Key, Handle> _map;

  // Running counter to keep track of the reference history.
  size_t _access_counter;

  void _evict() {
    auto top = _queue.top();
    _map.erase(top.key);
    _queue.pop();
  }
};

}  // namespace opossum
