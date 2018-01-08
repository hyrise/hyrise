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
  std::optional<Key> set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
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
      auto last = _list.end();
      last--;

      auto evicted = std::make_optional(last->first);

      _map.erase(last->first);
      _list.pop_back();

      return evicted;
    }

    return {};
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

  void clear_and_resize(size_t capacity) {
    clear();
    this->_capacity = capacity;
  }

  void resize(size_t capacity) {
    if (_map.size() > capacity) {
      for (size_t difference = 0; difference < _map.size() - capacity; ++difference) {
        auto last = _list.end();
        last--;

        _map.erase(last->first);
        _list.pop_back();
      }
    }
    this->_capacity = capacity;
  }

  std::vector<Key> dump_cache() {
    std::vector<Key> cache;
    cache.reserve(_list.size());
    for (auto &kv : _list) {
      cache.push_back(kv.first);
    }
    return cache;
  }

 protected:
  // Doubly-linked list to hold all elements.
  std::list<KeyValuePair> _list;

  // Map to point towards element in the list.
  std::unordered_map<Key, typename std::list<KeyValuePair>::iterator> _map;
};

}  // namespace opossum
