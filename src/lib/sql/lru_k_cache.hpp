#pragma once

#include <list>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/heap/fibonacci_heap.hpp"

#include "abstract_cache.hpp"

namespace opossum {

// Generic cache implementation using the LRU-K policy.
// When an item should be evicted the item with the largest backward k-distance is evicted.
// This is the item whose k-th most recent access is the furthest in the past.
// Note: This implementation is not thread-safe.
template <size_t K, typename key_t, typename val_t>
class LRUKCache : public AbstractCache<key_t, val_t> {
 public:
  // Entries within the LRU-K cache.
  // They keep a reference history of the K last accesses.
  class LRUKCacheEntry {
   public:
    key_t key;
    val_t value;

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
    bool operator<(const LRUKCacheEntry& rhs) const {
      if (history.size() != rhs.history.size()) {
        return history.size() > rhs.history.size();
      }
      return history[0] > rhs.history[0];
    }
  };

  typedef LRUKCacheEntry entry_t;
  typedef typename boost::heap::fibonacci_heap<entry_t>::handle_type handle_t;

  explicit LRUKCache(size_t capacity) : AbstractCache<key_t, val_t>(capacity), _access_counter(0) {}

  void set(const key_t& key, const val_t& value, double cost = 1.0, double size = 1.0) {
    ++_access_counter;

    auto it = _map.find(key);
    if (it != _map.end()) {
      // Update entry.
      handle_t handle = it->second;

      entry_t& entry = (*handle);
      entry.value = value;
      entry.add_history_entry(_access_counter);
      _queue.update(handle);
      return;
    }

    // If the cache is full, erase the item at the top of the heap
    // so that we can insert the new item.
    if (_queue.size() >= this->_capacity) {
      auto top = _queue.top();
      _map.erase(top.key);
      _queue.pop();
    }

    // Insert new item in cache.
    entry_t entry{key, value, {_access_counter}};
    handle_t handle = _queue.push(entry);
    _map[key] = handle;
  }

  val_t& get(const key_t& key) {
    ++_access_counter;

    auto it = _map.find(key);
    handle_t handle = it->second;
    entry_t& entry = (*handle);
    entry.add_history_entry(_access_counter);
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

 protected:
  // Priority queue to hold all elements. Implemented as max-heap.
  boost::heap::fibonacci_heap<entry_t> _queue;

  // Map to point towards element in the list.
  std::unordered_map<key_t, handle_t> _map;

  // Running counter to keep track of the reference history.
  size_t _access_counter;
};

}  // namespace opossum
