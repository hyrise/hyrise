#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache.hpp"

namespace opossum {

// Generic implementation of a least-recently-used cache.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class LRUCache : public AbstractCache<Key, Value> {
 public:
  typedef typename std::pair<Key, Value> KeyValuePair;

  explicit LRUCache(size_t capacity) : AbstractCache<Key, Value>(capacity) {}

  // Sets the value to be cached at the given key.
  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    _list.push_front(KeyValuePair(key, std::move(value)));

    // Override old element at that key, if it exists.
    auto it = _map.find(key);
    if (it != _map.end()) {
      _list.erase(it->second);
      _map.erase(it);
    }
    _map[key] = _list.begin();

    // Delete the last one, if capacity is exceeded.
    if (_map.size() > this->_capacity) {
      _evict();
    }
  }

  // Retrieves the value cached at the key.
  // Causes undefined behavior if the key is not in the cache.
  Value& get(const Key& key) {
    auto it = _map.find(key);
    _list.splice(_list.begin(), _list, it->second);
    return it->second->second;
  }

  bool has(const Key& key) const { return _map.find(key) != _map.end(); }

  // Returns the underlying list of all elements in the cache.
  std::list<KeyValuePair>& list() { return _list; }

  size_t size() const { return _map.size(); }

  void clear() {
    _list.clear();
    _map.clear();
  }

  void resize(size_t capacity) {
    while (_map.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

 protected:
  // Doubly-linked list to hold all elements.
  std::list<KeyValuePair> _list;

  // Map to point towards element in the list.
  std::unordered_map<Key, typename std::list<KeyValuePair>::iterator> _map;

  void _evict() {
    auto last = _list.end();
    last--;

    _map.erase(last->first);
    _list.pop_back();
  }
};

}  // namespace opossum
